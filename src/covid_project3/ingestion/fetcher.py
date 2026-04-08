
import asyncio
import time
from functools import partial

import httpx
import polars as pl
from loguru import logger

MAX_RETRIES = 5


def generate_unique_key(
    df: pl.DataFrame,
    unique_key_cols: list[str],
) -> pl.DataFrame:
    return df.with_columns(
        pl.concat_str(
            [pl.col(c).cast(pl.Utf8) for c in unique_key_cols],
            separator="|",
        ).alias("unique_key")
    )


async def fetch_page(
    endpoint,
    offset,
    batch_id,
    latest_value,
    stage_semaphore,
    executor,
    dataset_name,
    unique_key_cols,
    select_columns,
    fetch_bar,
    stage_bar,
    limit,
    stage_writer,
    client,
    where_clause: str | None = None,
):
    """
    Fetch one page from API -> transform -> stage parquet batch.

    where_clause: optional Socrata $where expression applied server-side,
    e.g. "case_month > '2024-06'" so only new rows are returned.

    Returns:
        True  = data processed
        False = no more data / stop condition
    """

    base_url = f"{endpoint}?$limit={limit}&$offset={offset}"
    url = f"{base_url}&$where={where_clause}" if where_clause else base_url
    retries = 0

    while retries < MAX_RETRIES:
        try:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()

            if not data:
                fetch_bar.update(limit)
                return False

            df = pl.DataFrame(data)
            raw_count = df.height
            if select_columns:
                df = df.select([c for c in select_columns if c in df.columns])

            # Incremental filtering
            if latest_value and all(c in df.columns for c in unique_key_cols):
                mask = pl.concat_str(
                    [pl.col(c).cast(pl.Utf8) for c in unique_key_cols],
                    separator="|",
                )
                df = df.filter(mask > latest_value)

            # If nothing left after filtering → stop
            if df.height == 0:
                fetch_bar.update(raw_count)
                return False

            df = generate_unique_key(df, unique_key_cols)

            async with stage_semaphore:
                loop = asyncio.get_running_loop()
                start = time.time()

                await loop.run_in_executor(
                    executor,
                    partial(stage_writer, df, batch_id),
                )

                duration = time.time() - start
                stage_bar.update(1)
                stage_bar.set_description(f"💾 Staging: {duration:.2f}s")

            fetch_bar.update(raw_count)

            return True

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                wait = 2 ** retries
                logger.warning(
                    f"Rate limited for {dataset_name} batch {batch_id}. "
                    f"Retrying in {wait}s..."
                )
                await asyncio.sleep(wait)
                retries += 1
            else:
                logger.error(
                    "HTTP error for dataset "
                    f"{dataset_name} batch {batch_id}: {e}"
                )
                fetch_bar.update(1)
                return False

        except (httpx.RequestError, httpx.TimeoutException):
            wait = 2 ** retries
            logger.warning(
                f"Network error for dataset {dataset_name} batch {batch_id}. "
                f"Retrying in {wait}s..."
            )
            await asyncio.sleep(wait)
            retries += 1

        except (ValueError, TypeError, KeyError) as e:
            logger.error(
                "Error while staging dataset "
                f"{dataset_name} batch {batch_id}: {e}"
            )
            fetch_bar.update(1)
            return False

    logger.error(f"Max retries exceeded for batch {batch_id}")
    fetch_bar.update(1)
    return False


# Optional helper (not very reliable for CDC, but kept for completeness)
def get_total_rows_from_api(endpoint: str) -> int:
    """
    Try to get total rows from API (may not work for CDC/Socrata APIs)
    """
    try:
        resp = httpx.get(f"{endpoint}?$select=count(*)")
        resp.raise_for_status()
        data = resp.json()
        return int(data[0]["count"]) if data else -1
    except (httpx.HTTPError, KeyError, TypeError, ValueError, IndexError):
        return -1
