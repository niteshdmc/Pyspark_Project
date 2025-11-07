from resources.dev.config import bucket_name
from src.main.utility.logging_config import *


def write_dataframe(df):
    try:
        df.write.option("header", True)\
            .mode("overwrite")\
            .csv("Z:\\Projects\\relmart-project\\spark_data\\customer_datamart\\")
        #df.write.format("csv") \
        #    .option("header", "true") \
        #    .mode("overwrite") \
        #    .option("path", file_path)\
        #    .save()
    except Exception as e:
        logger.error(f"Error writing the data : {str(e)}")
        raise e