from resources.dev.config import bucket_name
from src.main.utility.logging_config import *


def write_dataframe(df,file_path):
    try:
        df.write.format("csv") \
            .option("header", "true") \
            .mode("overwrite") \
            .option("path", file_path)\
            .save()
    except Exception as e:
        logger.error(f"Error writing the data : {str(e)}")
        raise e