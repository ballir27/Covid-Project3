import os
from urllib.parse import quote, urlencode

import polars as pl
import pyarrow as pa
import requests
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

def fips_pop_from_api():
    census_api_url = os.getenv("CENSUS_API_URL")
    census_api_key = os.getenv("CENSUS_API_KEY")

    base_url = census_api_url
    params = {
        "get": (
            "DP05_0001E,DP05_0002E,DP05_0003E,DP05_0005E,DP05_0006E,"
            "DP05_0007E,DP05_0008E,DP05_0009E,DP05_0010E,DP05_0011E,"
            "DP05_0012E,DP05_0013E,DP05_0014E,DP05_0015E,DP05_0016E,"
            "DP05_0017E,DP05_0068E,DP05_0069E,DP05_0070E,DP05_0071E,"
            "DP05_0072E,DP05_0073E"
        ),
        "for": "county:*",
        "key": census_api_key}
    query_string = urlencode(params, quote_via=quote)
    query_string = query_string.replace('%2A', '*')
    query_string = query_string.replace('%3A', ':')
    full_url = f"{base_url}?{query_string}"

    try:
        response = requests.get(full_url, timeout=10)
        response.raise_for_status()
        data = response.json()
        fips_pop = pl.DataFrame(data[1:], schema=data[0], orient = "row")
        fips_pop = fips_pop.rename({
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
            "DP05_0073E": "Some_Other_Race_Population"
        })
        fips_pop = fips_pop.with_columns((pl.col("state") + pl.col("county")).alias("fips_code"))
        logger.debug(f"FIPS population data: {fips_pop.head()}")
        logger.info("FIPS population data extracted successfully from API")
        return fips_pop
    except Exception as e:
        logger.error(f"Error in fips_pop_from_api: {e}")
        raise e


def fips_pop_snowflake_upload(census_polars, adbc_conn, snow_cursor):
    logger.info("Uploading FIPS population data to Snowflake...")
        # Ensure compatibility with Snowflake's file path format

    try:
        #Stage and upload the files to Snowflake
        snow_cursor.execute("USE DATABASE COVID19_DB")
        snow_cursor.execute("USE SCHEMA BRONZE")
        snow_cursor.execute("CREATE OR REPLACE STAGE CENSUS_STAGE")

        logger.info("Uploading 2023 US Census to Snowflake...")
        snow_cursor.execute("""
            CREATE OR REPLACE TABLE US_CENSUS_2023 (
                Total_Population INT,
                Male_Population INT,
                Female_Population INT,
                Age_Under_5_Population INT,
                Age_5_To_9_Population INT,
                Age_10_To_14_Population INT,
                Age_15_To_19_Population INT,
                Age_20_To_24_Population INT,
                Age_25_To_34_Population INT,
                Age_35_To_44_Population INT,
                Age_45_To_54_Population INT,
                Age_55_To_59_Population INT,
                Age_60_To_64_Population INT,
                Age_65_To_74_Population INT,
                Age_75_To_84_Population INT,
                Age_85_Plus_Population INT,
                White_Population INT,
                Black_Or_African_American_Population INT,
                American_Indian_And_Alaska_Native_Population INT,
                Asian_Population INT,
                Native_Hawaiian_And_Other_Pacific_Islander_Population INT,
                Some_Other_Race_Population INT,
                State STRING,
                County STRING
            )
        """)

        #To ensure all string columns are treated as strings in Snowflake (Snowflake doesn't play nice with Polars)
        arrow_table = census_polars.to_arrow()

        with adbc_conn.cursor() as adbc_cursor:
            adbc_cursor.adbc_ingest(
                table_name="US_CENSUS_2023",
                data=arrow_table,
                mode="replace"
            )

        adbc_conn.commit()
        logger.info("FIPS population data uploaded successfully to Snowflake")

    except Exception as e:
        logger.error(f"Error uploading 2023 US Census data to Snowflake: {e}")
        raise e
