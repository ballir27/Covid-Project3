import os

import httpx
import polars as pl
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

def cdc_covid_case_surveillance():

    cdc_covid_endpoint = os.getenv("CDC_COVID_API_ENDPOINT")
    headers = {"X-App-Token": os.getenv("CDC_COVID_API_APP_TOKEN")}
    params = {
        "$select": "*",
        "$offset": 0,
        "$limit": 10000
    }

    logger.info(f"Fetching data from CDC COVID Case Surveillance: {cdc_covid_endpoint}")
    try:
        response = httpx.get(cdc_covid_endpoint, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        cdc_covid_polars = pl.DataFrame(data)
        logger.info("Successfully fetched and loaded CDC COVID Case Surveillance data.")
        return cdc_covid_polars

    except httpx.HTTPError as e:
        logger.error(f"HTTP error occurred while fetching CDC COVID Case Surveillance data: {e}")
        raise e

    except Exception as e:
        logger.error(f"An error occurred while processing CDC COVID Case Surveillance data: {e}")
        raise e
