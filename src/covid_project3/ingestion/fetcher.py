# # src/covid_project3/ingestion/fetcher.py
# import asyncio
# import time
# from functools import partial
# import os
# import httpx
# import polars as pl
# from loguru import logger
# from .uploader import upload_to_snowflake, generate_unique_key

# MAX_RETRIES = 5
# TMP_DIR = "tmp"
# os.makedirs(TMP_DIR, exist_ok=True)

# async def fetch_page(
#     endpoint,
#     offset,
#     batch_id,
#     latest_value,
#     upload_semaphore,
#     executor,
#     table_name,
#     unique_key_cols,
#     select_columns,
#     progress_bar,
#     limit,
# ):
#     """
#     Fetch API page → save Parquet locally → bulk upload to Snowflake
#     """
#     url = f"{endpoint}?$limit={limit}&$offset={offset}"
#     retries = 0

#     while retries < MAX_RETRIES:
#         try:
#             async with httpx.AsyncClient(timeout=60.0) as client:
#                 resp = await client.get(url)
#                 resp.raise_for_status()
#                 data = resp.json()

#             if not data:
#                 progress_bar.update(1)
#                 return

#             df = pl.DataFrame(data)

#             # Only select requested columns
#             if select_columns:
#                 df = df.select([c for c in select_columns if c in df.columns])

#             # Filter rows greater than latest composite key
#             if latest_value and all(c in df.columns for c in unique_key_cols):
#                 mask = pl.concat_str([pl.col(c).cast(pl.Utf8) for c in unique_key_cols], separator="|")
#                 df = df.filter(mask > latest_value)

#             if df.shape[0] == 0:
#                 progress_bar.update(1)
#                 return

#             # Generate unique key
#             df = generate_unique_key(df, unique_key_cols)

#             # Save locally as Parquet
#             parquet_path = os.path.join(TMP_DIR, f"cdc_batch_{batch_id}.parquet")
#             df.write_parquet(parquet_path)

#             # Bulk upload to Snowflake concurrently
#             async with upload_semaphore:
#                 loop = asyncio.get_event_loop()
#                 start = time.time()
#                 await loop.run_in_executor(
#                     executor,
#                     partial(upload_to_snowflake, df, batch_id, table_name)
#                 )
#                 duration = time.time() - start
#                 progress_bar.set_description(f"📊 Last batch: {duration:.2f}s")
#                 progress_bar.update(1)

#             return

#         except httpx.HTTPStatusError as e:
#             if e.response.status_code == 429:
#                 wait = 2 ** retries
#                 logger.warning(f"Rate limited. Retrying in {wait}s...")
#                 await asyncio.sleep(wait)
#                 retries += 1
#             else:
#                 logger.error(f"HTTP error batch {batch_id}: {e}")
#                 progress_bar.update(1)
#                 return
#         except Exception as e:
#             logger.error(f"❌ Error in batch {batch_id}: {e}")
#             progress_bar.update(1)
#             return
        
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
):
    url = f"{endpoint}?$limit={limit}&$offset={offset}"
    retries = 0

    while retries < MAX_RETRIES:
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(url, timeout=60.0)
                resp.raise_for_status()
                data = resp.json()

            if not data:
                if fetch_bar.total == 0:
                    fetch_bar.update(1)
                return False

            df = pl.DataFrame(data)

            if select_columns:
                df = df.select([c for c in select_columns if c in df.columns])

            if latest_value and all(c in df.columns for c in unique_key_cols):
                mask = pl.concat_str([pl.col(c).cast(pl.Utf8) for c in unique_key_cols], separator="|")
                df = df.filter(mask > latest_value)

            if df.shape[0] == 0:
                if fetch_bar.total == 0:
                    fetch_bar.update(1)
                return False

            df = generate_unique_key(df, unique_key_cols)

            # Save and upload concurrently
            async with upload_semaphore:
                loop = asyncio.get_event_loop()
                start = time.time()
                await loop.run_in_executor(
                    executor,
                    partial(upload_to_snowflake, df, batch_id, table_name)
                )
                duration = time.time() - start
                upload_bar.update(1)
                upload_bar.set_description(f"📤 Last batch: {duration:.2f}s")

            fetch_bar.update(df.shape[0])
            return True

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
        except Exception as e:
            logger.error(f"❌ Error in batch {batch_id}: {e}")
            fetch_bar.update(1)
            return False


def get_total_rows_from_api(endpoint: str) -> int:
    """Attempt to fetch total row count from API. Returns -1 if unavailable."""
    try:
        resp = httpx.get(f"{endpoint}?$limit=1")
        resp.raise_for_status()
        total = resp.json().get("total", -1)
        return total if isinstance(total, int) else -1
    except Exception:
        return -1