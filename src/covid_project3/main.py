import asyncio

import census_ingestion
import config
import ingestion
from loguru import logger


async def main():
    try:
        snow_conn = config.snow_config()
        snow_cursor = snow_conn.cursor()
        adbc_conn = config.adbc_snowflake_config()

        # cdc_covid_data = await ingestion.cdc_covid_case_surveillance()
        # print(cdc_covid_data.head())

        fips_pop_data = census_ingestion.fips_pop_from_api()
        logger.info(fips_pop_data.head())
        census_ingestion.fips_pop_snowflake_upload(fips_pop_data, adbc_conn, snow_cursor)

    except Exception as e:
        logger.error(f"Error uploading to Snowflake: {e}")
        raise e

    finally:
        snow_cursor.close()
        snow_conn.close()

if __name__ == "__main__":
    asyncio.run(main())
