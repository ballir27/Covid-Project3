import config
from loguru import logger


def main():
    try:
        snow_conn = config.snow_config()
        snow_cursor = snow_conn.cursor()

    except Exception as e:
        logger.error(f"Error uploading to Snowflake: {e}")
        raise e

    finally:
        snow_cursor.close()
        snow_conn.close()

if __name__ == "__main__":
    main()
