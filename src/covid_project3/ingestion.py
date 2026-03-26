import asyncio
import os
import time

import httpx
import polars as pl
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

async def fetch_batch(client, url, headers, offset, limit):
    """Worker function to fetch a single offset."""
    params = {"$select": "*", "$offset": offset, "$limit": limit}
    response = await client.get(url, params=params, headers=headers, timeout=60.0)
    response.raise_for_status()
    return response.json()

async def cdc_covid_case_surveillance():
    endpoint = os.getenv("CDC_COVID_API_ENDPOINT")
    headers = {"X-App-Token": os.getenv("CDC_COVID_API_APP_TOKEN")}

    limit = 10000
    batch_size = 5
    current_offset = 0
    all_rows = []

    logger.info(f"Starting async batch fetch from: {endpoint}")
    async with httpx.AsyncClient() as client:
        start = time.time()
        #TODO: Can I get rid of "while True" and replace with "while not finished" or "while more data" logic?
        while True:
            # 1. Create a batch of offsets
            offsets = [current_offset + (i * limit) for i in range(batch_size)]

            # 2. Run the batch concurrently
            logger.info(f"Fetching offsets {offsets[0]} to {offsets[-1]}...")
            tasks = [fetch_batch(client, endpoint, headers, off, limit) for off in offsets]
            results = await asyncio.gather(*tasks)

            # 3. Process results and check for the end of data
            finished = False
            for data in results:
                if not data:
                    finished = True
                    break
                all_rows.extend(data)

            if finished:
                logger.info("Reached the end of the dataset.")
                break

            current_offset += (limit * batch_size)
            logger.info(f"Total records collected so far: {len(all_rows)}")

    end = time.time()
    logger.info(f"Total time taken: {end - start:.2f} seconds")

    return pl.DataFrame(all_rows)
