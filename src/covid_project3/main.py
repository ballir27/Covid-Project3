import asyncio

import config
import ingestion
from loguru import logger


async def main():
    try:
        snow_conn = config.snow_config()
        snow_cursor = snow_conn.cursor()

        cdc_covid_data = await ingestion.cdc_covid_case_surveillance()
        print(cdc_covid_data.head())

    except Exception as e:
        logger.error(f"Error uploading to Snowflake: {e}")
        raise e

    finally:
        snow_cursor.close()
        snow_conn.close()

if __name__ == "__main__":
    asyncio.run(main())
