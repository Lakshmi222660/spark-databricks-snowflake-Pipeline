# SNOW.py
# Reads user data from Snowflake and saves it to Databricks for the join.

import snowflake.connector
import pandas as pd

# Password is entered here at run time
dbutils.widgets.text("snowflake_password", "")
snowflake_password = dbutils.widgets.get("snowflake_password").strip()

conn = snowflake.connector.connect(
    account="SAORQXH-NO09703",
    user="LAKSHMI09",
    password=snowflake_password,
    database="zeyodb",
    schema="zeyoschema",
    warehouse="COMPUTE_WH",
    role="ACCOUNTADMIN"
)

pandas_df = pd.read_sql(
    "SELECT * FROM snow_source",
    conn
)

conn.close()

snow_df = spark.createDataFrame(pandas_df)

snow_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("zeyodb.snow_tab")

print("Snowflake data written to zeyodb.snow_tab")

# Preview
spark.sql("SELECT * FROM zeyodb.snow_tab LIMIT 10").display()
