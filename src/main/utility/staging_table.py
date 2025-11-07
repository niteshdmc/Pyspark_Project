import datetime
from loguru import logger
from resources.dev import config
from src.main.utility.mysql_session import MySqlSession

mysql_session_obj = MySqlSession()
connection = mysql_session_obj.get_mysql_connection()
cursor = connection.cursor()


def add_to_staging_table(s3_bucket,s3_prefix, file_name):
    file_location = f"s3://{s3_bucket}/{s3_prefix}/{file_name}"
    created_date = datetime.datetime.now().strftime("%Y_%m_%d")
    updated_date = datetime.datetime.now().strftime("%Y_%m_%d")
    add_to_staging_query = f""" INSERT INTO {config.database_name}.{config.product_staging_table}
                                (file_name, file_location, created_date, updated_date, status)
                                VALUES (%s, %s, %s , %s , %s)
                            """
    try:
        cursor.execute(add_to_staging_query, (file_name, file_location, created_date, updated_date, "U"))
        connection.commit()
        logger.info(f"Added {file_name} to staging table")
        cursor.close()
        connection.close()
    except Exception as e:
        logger.error(f"Error inserting into staging table: {e}")
        connection.rollback()
        cursor.close()
        connection.close()

def update_staging_status():
    pass
