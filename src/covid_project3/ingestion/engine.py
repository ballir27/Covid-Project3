# src/covid_project3/ingestion/engine.py
import asyncio
from concurrent.futures import ThreadPoolExecutor
from loguru import logger
from .fetcher import fetch_page
from .uploader import get_latest_value
from tqdm import tqdm

async def run_pipeline(endpoint: str, table_name: str, config: dict,
                       unique_key_cols: list, select_columns: list = None):
    """
    Main pipeline to fetch, filter, and upload API data to Snowflake.
    """
    LIMIT = int(config.get("LIMIT", 1000))
    MAX_PAGES = int(config.get("MAX_PAGES", 100))

    latest_value = get_latest_value(table_name, unique_key_cols)
    logger.info(f"🟢 Latest value in {table_name}: {latest_value}")

    semaphore = asyncio.Semaphore(int(config.get("CONCURRENT_REQUESTS", 5)))
    upload_semaphore = asyncio.Semaphore(int(config.get("CONCURRENT_UPLOADS", 3)))
    executor = ThreadPoolExecutor(max_workers=int(config.get("CONCURRENT_UPLOADS", 3)))

    progress_bar = tqdm(total=MAX_PAGES, desc="📊 Progress", ncols=100)

    async def task(offset: int, batch_id: int):
        async with semaphore:
            await fetch_page(
                endpoint=endpoint,
                offset=offset,
                batch_id=batch_id,
                latest_value=latest_value,
                upload_semaphore=upload_semaphore,
                executor=executor,
                table_name=table_name,
                unique_key_cols=unique_key_cols,
                select_columns=select_columns,
                progress_bar=progress_bar,
                limit=LIMIT
            )

    tasks = [task(page * LIMIT, page) for page in range(MAX_PAGES)]
    await asyncio.gather(*tasks)
    progress_bar.close()