
# src/covid_project3/ingestion/fetcher.py
import asyncio
import time
from functools import partial
import httpx
import polars as pl
from loguru import logger
from .uploader import upload_to_snowflake, generate_unique_key

MAX_RETRIES = 5


async def fetch_page(
    endpoint,
    offset,
    batch_id,
    latest_value,
    upload_semaphore,
    executor,
    table_name,
    unique_key_cols,
    select_columns,
    fetch_bar,
    upload_bar,
    limit,
    client,  # ✅ shared client
):
    """
    Fetch one page from API → transform → upload to Snowflake

    Returns:
        True  = data processed
        False = no more data / stop condition
    """

    url = f"{endpoint}?$limit={limit}&$offset={offset}"
    retries = 0

    while retries < MAX_RETRIES:
        try:
            # ✅ reuse shared HTTP client
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()

            # ✅ Stop condition: no more data
            if not data:
                fetch_bar.update(1)
                return False

            df = pl.DataFrame(data)

            # Select only required columns
            if select_columns:
                df = df.select([c for c in select_columns if c in df.columns])

            # Incremental filtering
            if latest_value and all(c in df.columns for c in unique_key_cols):
                mask = pl.concat_str(
                    [pl.col(c).cast(pl.Utf8) for c in unique_key_cols],
                    separator="|"
                )
                df = df.filter(mask > latest_value)

            # If nothing left after filtering → stop
            if df.height == 0:
                fetch_bar.update(1)
                return False

            # Generate unique key
            df = generate_unique_key(df, unique_key_cols)

            # ✅ Upload concurrently (thread pool)
            async with upload_semaphore:
                loop = asyncio.get_running_loop()  # ✅ modern fix
                start = time.time()

                await loop.run_in_executor(
                    executor,
                    partial(upload_to_snowflake, df, batch_id, table_name)
                )

                duration = time.time() - start
                upload_bar.update(1)
                upload_bar.set_description(f"📤 Upload: {duration:.2f}s")

            # ✅ Progress = batches (NOT rows)
            fetch_bar.update(1)

            return True

        # ✅ Rate limit retry
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                wait = 2 ** retries
                logger.warning(f"Rate limited. Retrying in {wait}s...")
                await asyncio.sleep(wait)
                retries += 1
            else:
                logger.error(f"HTTP error batch {batch_id}: {e}")
                fetch_bar.update(1)
                return False

        # ✅ Network retry
        except (httpx.RequestError, httpx.TimeoutException) as e:
            wait = 2 ** retries
            logger.warning(f"Network error batch {batch_id}. Retrying in {wait}s...")
            await asyncio.sleep(wait)
            retries += 1

        # ❌ Other errors → skip batch
        except Exception as e:
            logger.error(f"❌ Error in batch {batch_id}: {e}")
            fetch_bar.update(1)
            return False

    logger.error(f"❌ Max retries exceeded for batch {batch_id}")
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
    except Exception:
        return -1