# src/covid_project3/ingestion/census_fetcher.py
import os
from urllib.parse import quote, urlencode
import polars as pl
import requests
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

def fetch_census_fips():
    """Fetch FIPS population data from Census API"""
    census_api_url = os.getenv("CENSUS_API_URL")
    census_api_key = os.getenv("CENSUS_API_KEY")

    params = {
        "get": (
            "DP05_0001E,DP05_0002E,DP05_0003E,DP05_0005E,DP05_0006E,"
            "DP05_0007E,DP05_0008E,DP05_0009E,DP05_0010E,DP05_0011E,"
            "DP05_0012E,DP05_0013E,DP05_0014E,DP05_0015E,DP05_0016E,"
            "DP05_0017E,DP05_0068E,DP05_0069E,DP05_0070E,DP05_0071E,"
            "DP05_0072E,DP05_0073E"
        ),
        "for": "county:*",
        "key": census_api_key,
    }

    query_string = urlencode(params, quote_via=quote).replace('%2A', '*').replace('%3A', ':')
    full_url = f"{census_api_url}?{query_string}"

    try:
        resp = requests.get(full_url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        census_fips_df = pl.DataFrame(data[1:], schema=data[0], orient="row")
        census_fips_df = census_fips_df.rename({
            "DP05_0001E": "Total_Population",
            "DP05_0002E": "Male_Population",
            "DP05_0003E": "Female_Population",
            "DP05_0005E": "Under_5_Population",
            "DP05_0006E": "5_To_9_Population",
            "DP05_0007E": "10_To_14_Population",
            "DP05_0008E": "15_To_19_Population",
            "DP05_0009E": "20_To_24_Population",
            "DP05_0010E": "25_To_34_Population",
            "DP05_0011E": "35_To_44_Population",
            "DP05_0012E": "45_To_54_Population",
            "DP05_0013E": "55_To_59_Population",
            "DP05_0014E": "60_To_64_Population",
            "DP05_0015E": "65_To_74_Population",
            "DP05_0016E": "75_To_84_Population",
            "DP05_0017E": "85_Plus_Population",
            "DP05_0068E": "White_Population",
            "DP05_0069E": "Black_Or_African_American_Population",
            "DP05_0070E": "American_Indian_And_Alaska_Native_Population",
            "DP05_0071E": "Asian_Population",
            "DP05_0072E": "Native_Hawaiian_And_Other_Pacific_Islander_Population",
            "DP05_0073E": "Some_Other_Race_Population",
        })
        census_fips_df = census_fips_df.with_columns(
            (pl.col("state") + pl.col("county")).alias("fips_code")
        )
        logger.info(f"Fetched {census_fips_df.height} rows from Census API")
        return census_fips_df
    except Exception as e:
        logger.error(f"Failed to fetch Census FIPS data: {e}")
        raise