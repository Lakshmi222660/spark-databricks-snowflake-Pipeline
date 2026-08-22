# MASTER.py
# Joins the three source tables into one, then writes the result to Databricks
# and back to Snowflake.

import snowflake.connector
import pandas as pd
from pyspark.sql.functions import col, to_json

# ---------------------------------------------------------------------------
# Build the join
# ---------------------------------------------------------------------------

adl_tab = spark.table("zeyodb.adl_tab")

# Normalize column names to lowercase for consistent joins
snow_tab = spark.table("zeyodb.snow_tab")
for col_name in snow_tab.columns:
    snow_tab = snow_tab.withColumnRenamed(col_name, col_name.lower())

api_tab = spark.table("zeyodb.api_tab").dropDuplicates(["username"])

# LEFT join to preserve all adl_tab rows even if snow_tab has no matches
joindf = (
    adl_tab
    .join(snow_tab, ["username"], "left")
    .join(api_tab, ["username"], "left")
)

joindf.display()

# ---------------------------------------------------------------------------
# Write joined result to Databricks
# ---------------------------------------------------------------------------

spark.sql("DROP TABLE IF EXISTS zeyodb.master_tab")

joindf.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("zeyodb.master_tab")

print("Master table written to Databricks")

# ---------------------------------------------------------------------------
# Prepare data for Snowflake
# ---------------------------------------------------------------------------

# Password is entered at runtime
dbutils.widgets.text("snowflake_password", "")
snowflake_password = dbutils.widgets.get("snowflake_password").strip()

# Convert scores to JSON string if the column exists
if "scores" in joindf.columns:
    joindf_with_json = joindf.withColumn(
        "scores",
        to_json(col("scores"))
    )
else:
    joindf_with_json = joindf

# Collect Spark rows
rows = joindf_with_json.collect()

# Convert Spark rows to Pandas
pandas_out = pd.DataFrame(
    [row.asDict(recursive=True) for row in rows]
)

# Check the DataFrame before sending it to Snowflake
print("Number of rows:", len(pandas_out))
print("Number of columns:", len(pandas_out.columns))
print("Columns:", list(pandas_out.columns))

# Make sure duplicate column names are removed
pandas_out = pandas_out.loc[
    :,
    ~pandas_out.columns.duplicated()
]

# Make column names safe for Snowflake
clean_columns = []

for i, column in enumerate(pandas_out.columns):

    column = str(column).strip()

    if column == "":
        column = f"COLUMN_{i}"

    column = column.replace(" ", "_")
    column = column.replace("-", "_")
    column = column.replace(".", "_")

    clean_columns.append(column)

pandas_out.columns = clean_columns

# Convert scores to string
if "scores" in pandas_out.columns:
    pandas_out["scores"] = pandas_out["scores"].astype(str)

# Final check
print("Final columns:")
print(list(pandas_out.columns))

# Stop if there are no columns
if len(pandas_out.columns) == 0:
    raise ValueError(
        "The DataFrame has no columns. Cannot create MASTER_TAB."
    )

# ---------------------------------------------------------------------------
# Connect to Snowflake and write the table
# ---------------------------------------------------------------------------

conn = snowflake.connector.connect(
    account="SAORQXH-NO09703",
    user="LAKSHMI09",
    password=snowflake_password,
    database="zeyodb",
    schema="zeyoschema",
    warehouse="COMPUTE_WH",
    role="ACCOUNTADMIN"
)

print("Connected to Snowflake successfully")

from snowflake.connector.pandas_tools import write_pandas

success, nchunks, nrows, output = write_pandas(
    conn,
    pandas_out,
    "MASTER_TAB",
    auto_create_table=True,
    overwrite=True,
    quote_identifiers=True
)

print("Success:", success)
print("Chunks:", nchunks)
print("Rows written:", nrows)

conn.close()

print("Master table written to Snowflake successfully")

# Preview
spark.sql("SELECT * FROM zeyodb.master_tab LIMIT 10").display()
