# Databricks notebook source
# MAGIC %md
# MAGIC Fetch the latest tube status whenever the notebook is run, retaining the history of statuses from all runs.
# MAGIC
# MAGIC Retrieve the response from the public TFL API using the Python requests library.
# MAGIC
# MAGIC Create a custom schema to map the response JSON object.
# MAGIC
# MAGIC Create a DataFrame and select only the required columns (current_timestamp, line, status, disruption_reason).
# MAGIC
# MAGIC Append the data into a history table to maintain a record of all statuses.
# MAGIC
# MAGIC Load the data into the latest status table to capture the most recent statuses.
# MAGIC
# MAGIC This approach supports further analysis of the tube lines and ensures scalability through the use of a well-defined schema.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA TFL_DATA;

# COMMAND ----------

import requests
import json
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType, IntegerType
from pyspark.sql.functions import col,lit
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO)

# URL for the TFL Open API
url = "https://api.tfl.gov.uk/Line/Mode/tube/Status"

# Get the current timestamp
current_timestamp = datetime.now()

# Make a request to the API
try:
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
    else:
        response.raise_for_status()

except Exception as e:
    logging.error(f"Failed to fetch data: {e}")
    raise

# COMMAND ----------

# Define schema for the DataFrame
schema = StructType([
    StructField("type", StringType(), True),
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("modeName", StringType(), True),
    StructField("disruptions", ArrayType(StringType()), True),
    StructField("created", StringType(), True),
    StructField("modified", StringType(), True),
    StructField("lineStatuses", ArrayType(
        StructType([
            StructField("type", StringType(), True),
            StructField("id", IntegerType(), True),
            StructField("lineId", StringType(), True),
            StructField("statusSeverity", IntegerType(), True),
            StructField("statusSeverityDescription", StringType(), True),
            StructField("reason", StringType(), True),
            StructField("created", StringType(), True),
            StructField("validityPeriods", ArrayType(
                StructType([
                    StructField("type", StringType(), True),
                    StructField("fromDate", StringType(), True),
                    StructField("toDate", StringType(), True),
                    StructField("isNow", StringType(), True)
                ])
            ), True),
            StructField("disruption", StructType([
                StructField("type", StringType(), True),
                StructField("category", StringType(), True),
                StructField("categoryDescription", StringType(), True),
                StructField("description", StringType(), True),
                StructField("affectedRoutes", ArrayType(StringType()), True),
                StructField("affectedStops", ArrayType(StringType()), True),
                StructField("closureText", StringType(), True)
            ]), True)
        ])
    ), True)
])

df = spark.createDataFrame(data, schema=schema)
df = df.select("name","lineStatuses").withColumnRenamed("name","line") \
       .withColumn("status",col("lineStatuses")[0].statusSeverityDescription) \
       .withColumn("disruption_reason",col("lineStatuses")[0].reason) \
       .withColumn("status_timestamp",lit(current_timestamp)).drop("lineStatuses")

df = df.select("status_timestamp","line","status","disruption_reason")

#Write DataFrame to Databricks SQL table
#df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("tfl_tube_status")
df.write.format("hive").mode("append").saveAsTable("tfl_tube_status_h")

# COMMAND ----------

# SQL query to get the latest status for each line
spark.sql(""" CREATE OR REPLACE TABLE  latest_tfl_tube_status
              AS 
              SELECT
                status_timestamp,
                line,
                status,
                disruption_reason
              FROM tfl_tube_status_h
              QUALIFY ROW_NUMBER() OVER (PARTITION BY line ORDER BY status_timestamp DESC) = 1 
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify the entries to check the latest status
# MAGIC select * from latest_tfl_tube_status

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify the new entries in history table
# MAGIC select * from tfl_tube_status_h order by  status_timestamp desc, line
