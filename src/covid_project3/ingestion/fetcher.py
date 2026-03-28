# src/covid_project3/ingestion/fetcher.py
import asyncio
import time
from functools import partial
import httpx
import polars as pl
from loguru import logger
from .uploader import upload_to_snowflake, generate_unique_key

MAX_RETRIES = 5

async def fetch_page(endpoint, offset, batch_id, latest_value,
                     upload_semaphore, executor, table_name,
                     unique_key_cols, select_columns, progress_bar, limit):
    """
    Fetch a page from the API, filter by latest value, generate unique key, and upload.
    """
    url = f"{endpoint}?$limit={limit}&$offset={offset}"
    retries = 0

    while retries < MAX_RETRIES:
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(url, timeout=30.0)
                resp.raise_for_status()
                data = resp.json()

            if not data:
                progress_bar.update(1)
                return

            df = pl.DataFrame(data)

            # Select only specified columns
            if select_columns:
                df = df.select([col for col in select_columns if col in df.columns])

            # Filter rows greater than latest composite key
            if latest_value and all(col in df.columns for col in unique_key_cols):
                mask = pl.concat_str([pl.col(col).cast(pl.Utf8) for col in unique_key_cols], separator="|")
                df = df.filter(mask > latest_value)

            if df.shape[0] == 0:
                progress_bar.update(1)
                return

            # Generate composite unique key column
            df = generate_unique_key(df, unique_key_cols)

            async with upload_semaphore:
                loop = asyncio.get_event_loop()
                start = time.time()

                await loop.run_in_executor(
                    executor,
                    partial(upload_to_snowflake, df, batch_id, table_name)
                )

                duration = time.time() - start
                progress_bar.set_description(f"📊 Last batch: {duration:.2f}s")
                progress_bar.update(1)

            return

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                wait = 2 ** retries
                logger.warning(f"Rate limited. Retrying in {wait}s...")
                await asyncio.sleep(wait)
                retries += 1
            else:
                logger.error(f"HTTP error batch {batch_id}: {e}")
                progress_bar.update(1)
                return
        except Exception as e:
            logger.error(f"❌ Error in batch {batch_id}: {e}")
            progress_bar.update(1)
            return