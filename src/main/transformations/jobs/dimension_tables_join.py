from pyspark.sql.functions import *
from src.main.utility.logging_config import *
#enriching the data from different table

def dimensions_table_join(final_df_to_process,
                         customer_df,store_df,sales_team_df):

    logger.info("Joining the final_df_to_process with customer_df ")
    s3_customer_joined_df = final_df_to_process.alias("s3_data")\
        .join(customer_df.alias("ct"),col("s3_data.customer_id") == col("ct.customer_id"),"left")\
        .withColumn("customer_name", when(col("customer_name").isNull(), concat_ws(" ", col("ct.first_name"), col("ct.last_name")))\
        .otherwise(col("customer_name")))\
        .withColumn("customer_address", when(col("customer_address").isNull(), concat_ws(" ", col("address"), col("pincode"))) \
        .otherwise(col("customer_address")))\
        .select("s3_data.customer_id", "customer_name", "store_id", "product_name", "sales_date", "sales_person_id", "price", "quantity","total_cost", "customer_address", "phone_number", "sales_person_name","product_id", "extra_data")


    logger.info("Joining the s3_customer_joined_df with store_df ")
    s3_customer_store_joined_df = (
        s3_customer_joined_df.alias("s3_customer_joined")
        .join(store_df.alias("st"),col("s3_customer_joined.store_id") == col("st.id"),"left")
        .withColumnRenamed("address", "store_address")
        .drop("id", "store_opening_date", "reviews"))



    logger.info("Joining the s3_customer_store_joined_df with sales_team_df ")
    s3_customer_store_sales_joined_df = s3_customer_store_joined_df.alias("s3_customer_store_joined")\
        .join(sales_team_df.alias("st"),col("st.id") == col("s3_customer_store_joined.sales_person_id"),"left")\
        .withColumn("sales_person_name", when(col("sales_person_name").isNull(), concat_ws ( " ", col("first_name"), col("last_name")))\
        .otherwise(col("sales_person_name")))\
        .withColumnRenamed("address", "sales_person_address")\
        .drop("id","first_name","last_name","manager_id","is_manager","pincode","joining_date")

    return s3_customer_store_sales_joined_df

