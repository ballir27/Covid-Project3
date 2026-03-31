# # src/covid_project3/ingestion/engine.py
import asyncio
from concurrent.futures import ThreadPoolExecutor
from loguru import logger
from tqdm.asyncio import tqdm

from .fetcher import fetch_page
from .uploader import get_latest_value

async def run_pipeline(
    endpoint: str,
    table_name: str,
    config: dict,
    unique_key_cols: list,
    select_columns: list = None,
):
    LIMIT = int(config.get("LIMIT", 10000))
    MAX_PAGES = int(config.get("MAX_PAGES", 11000))
    CONCURRENT_REQUESTS = int(config.get("CONCURRENT_REQUESTS", 10))
    CONCURRENT_UPLOADS = int(config.get("CONCURRENT_UPLOADS", 5))

    latest_value = get_latest_value(table_name, unique_key_cols)
    logger.info(f"🟢 Latest value in {table_name}: {latest_value}")

    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    upload_semaphore = asyncio.Semaphore(CONCURRENT_UPLOADS)
    executor = ThreadPoolExecutor(max_workers=CONCURRENT_UPLOADS)

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
                limit=LIMIT,
            )

    tasks = [task(page * LIMIT, page) for page in range(MAX_PAGES)]
    await asyncio.gather(*tasks)
    progress_bar.close()


# if __name__ == "__main__":
#     import os
#     from covid_project3.config import config

#     endpoint = config["API_ENDPOINT"]
#     table_name = "CDC_RAW_CASES_1"
#     unique_keys = ["case_id", "report_date"]  # replace with your keys

#     asyncio.run(run_pipeline(endpoint, table_name, config, unique_keys))

# src/covid_project3/ingestion/engine.py
# src/covid_project3/ingestion/engine.py
# src/covid_project3/ingestion/engine.py
# import asyncio
# from concurrent.futures import ThreadPoolExecutor
# from loguru import logger
# from tqdm.asyncio import tqdm_asyncio
# from .fetcher import fetch_page, get_total_rows_from_api
# from .uploader import get_latest_value, count_snowflake_rows

# from .census_fetcher import fetch_census_fips
# from .uploader import upload_census_to_snowflake


# async def run_census_pipeline():
#     df = fetch_census_fips()
#     upload_census_to_snowflake(df)
    
    
# async def run_pipeline(
#     endpoint: str,
#     table_name: str,
#     config: dict,
#     unique_key_cols: list,
#     select_columns: list = None,
# ):
#     LIMIT = int(config.get("LIMIT", 10000))
#     CONCURRENT_REQUESTS = int(config.get("CONCURRENT_REQUESTS", 10))
#     CONCURRENT_UPLOADS = int(config.get("CONCURRENT_UPLOADS", 5))

#     latest_value = get_latest_value(table_name, unique_key_cols)
#     logger.info(f"🟢 Latest value in {table_name}: {latest_value}")

#     # Semaphores for concurrent fetches and uploads
#     semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
#     upload_semaphore = asyncio.Semaphore(CONCURRENT_UPLOADS)
#     executor = ThreadPoolExecutor(max_workers=CONCURRENT_UPLOADS)

#     # Fetch total rows from API if possible
#     api_total = get_total_rows_from_api(endpoint)
#     if api_total > 0:
#         fetch_bar = tqdm_asyncio(total=api_total, desc="📥 Fetching", ncols=100)
#     else:
#         fetch_bar = tqdm_asyncio(desc="📥 Fetching", ncols=100, total=0)

#     upload_bar = tqdm_asyncio(desc="📤 Uploading", ncols=100, total=0)

#     offset = 0
#     batch_id = 0
#     has_more = True

#     while has_more:
#         async def task(offset, batch_id):
#             async with semaphore:
#                 return await fetch_page(
#                     endpoint=endpoint,
#                     offset=offset,
#                     batch_id=batch_id,
#                     latest_value=latest_value,
#                     upload_semaphore=upload_semaphore,
#                     executor=executor,
#                     table_name=table_name,
#                     unique_key_cols=unique_key_cols,
#                     select_columns=select_columns,
#                     fetch_bar=fetch_bar,
#                     upload_bar=upload_bar,
#                     limit=LIMIT,
#                 )

#         tasks = [task(offset + i*LIMIT, batch_id + i) for i in range(CONCURRENT_REQUESTS)]
#         results = await asyncio.gather(*tasks)

#         offset += CONCURRENT_REQUESTS * LIMIT
#         batch_id += CONCURRENT_REQUESTS

#         # If all returned False/None, no more data
#         if all(r is False or r is None for r in results):
#             has_more = False

#     await fetch_bar.close()
#     await upload_bar.close()

    # Verification
    # snowflake_rows = count_snowflake_rows(table_name)
    # logger.info(f"🔎 Verification result: API total={api_total}, Snowflake rows={snowflake_rows}, "
    #             f"Status={'✅ OK' if api_total == snowflake_rows else '⚠️ Cannot verify total rows from API'}")

    # logger.info("✅ Pipeline completed successfully!")