import asyncio
from concurrent.futures import ThreadPoolExecutor

import httpx
from loguru import logger
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio

from .census_fetcher import fetch_census_fips
from .fetcher import fetch_page, get_total_rows_from_api
from .uploader import (
    count_snowflake_rows,
    get_latest_value,
    upload_dataframe,
)


# ---------------------------
# Census Pipeline
# ---------------------------
async def run_census_pipeline(
    table_name: str = "US_CENSUS_2023",
):
    with tqdm(
        total=2,
        desc="🧮 Census Pipeline",
        ncols=100,
    ) as progress:
        progress.set_postfix_str("fetch")
        df = fetch_census_fips()
        progress.update(1)

        progress.set_postfix_str("upload")
        rows = upload_dataframe(df, table_name)
        logger.info(f"Uploaded {rows} rows into {table_name}")
        progress.update(1)


# ---------------------------
# CDC Pipeline (async + parallel)
# ---------------------------
async def run_pipeline(
    endpoint: str,
    table_name: str,
    config: dict,
    unique_key_cols: list,
    select_columns: list | None = None,
):
    limit = int(config.get("LIMIT", 10000))
    concurrent_requests = int(config.get("CONCURRENT_REQUESTS", 10))
    concurrent_uploads = int(config.get("CONCURRENT_UPLOADS", 5))

    latest_value = get_latest_value(table_name, unique_key_cols)
    logger.info(f"🟢 Latest value in {table_name}: {latest_value}")

    semaphore = asyncio.Semaphore(concurrent_requests)
    upload_semaphore = asyncio.Semaphore(concurrent_uploads)
    executor = ThreadPoolExecutor(max_workers=concurrent_uploads)

    total_uploaded = 0

    def upload_writer(df, _batch_id):
        nonlocal total_uploaded
        rows = upload_dataframe(df, table_name)
        total_uploaded += rows

    # Build server-side $where filter so the API returns only new rows.
    where_clause: str | None = None
    if latest_value and unique_key_cols:
        date_col = unique_key_cols[0]
        date_val = latest_value.split("|")[0]
        where_clause = f"{date_col} > '{date_val}'"
        logger.info(f"Server-side filter active: $where={where_clause}")

    api_total = get_total_rows_from_api(endpoint)
    label = "📥 Fetching (new rows)" if where_clause else "📥 Fetching"
    fetch_bar = tqdm_asyncio(
        total=api_total if api_total > 0 else None,
        desc=label,
        ncols=100,
    )
    upload_bar = tqdm_asyncio(desc="⬆️  Uploading", ncols=100)

    offset = 0
    batch_id = 0
    has_more = True

    async with httpx.AsyncClient(timeout=60.0) as client:

        while has_more:

            async def task(offset, batch_id):
                async with semaphore:
                    return await fetch_page(
                        endpoint=endpoint,
                        offset=offset,
                        batch_id=batch_id,
                        latest_value=latest_value,
                        stage_semaphore=upload_semaphore,
                        executor=executor,
                        dataset_name=table_name,
                        unique_key_cols=unique_key_cols,
                        select_columns=select_columns,
                        fetch_bar=fetch_bar,
                        stage_bar=upload_bar,
                        limit=limit,
                        stage_writer=upload_writer,
                        client=client,
                        where_clause=where_clause,
                    )

            tasks = [
                task(offset + i * limit, batch_id + i)
                for i in range(concurrent_requests)
            ]

            results = await asyncio.gather(*tasks)

            offset += concurrent_requests * limit
            batch_id += concurrent_requests

            if all(r is False or r is None for r in results):
                has_more = False

    fetch_bar.close()
    upload_bar.close()
    executor.shutdown(wait=True)

    snowflake_rows = count_snowflake_rows(table_name)
    logger.info(
        f"✅ Pipeline complete. "
        f"Uploaded ~{total_uploaded:,} new rows. "
        f"Total in Snowflake: {snowflake_rows:,}"
    )
