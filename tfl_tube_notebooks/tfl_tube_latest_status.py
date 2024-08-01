# Databricks notebook source
# MAGIC %md
# MAGIC Fetch the latest tube status whenever the notebook is run.
# MAGIC
# MAGIC Retrieve the response from the public TFL API using the Python requests library.
# MAGIC
# MAGIC For each line in the response JSON array, create a list with only the required columns (current_timestamp, line, status, disruption_reason).
# MAGIC
# MAGIC Transform the data and use a custom schema to create a DataFrame.
# MAGIC
# MAGIC Write the DataFrame to a SQL table tfl_tube_status.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA TFL_DATA;

# COMMAND ----------

import requests
import json
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import logging

# URL for the TFL Open API
url = "https://api.tfl.gov.uk/Line/Mode/tube/Status"

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

# Initialize an empty list to store the transformed data
transformed_data = []

# Get the current timestamp
current_timestamp = datetime.now()

# Process each line's status
for line_status in data:
    line = line_status.get('name')
    status = line_status.get('lineStatuses')[0].get('statusSeverityDescription')
    disruption_reason = line_status.get('lineStatuses')[0].get('reason', '')

    # Append the processed data to the list
    transformed_data.append((current_timestamp, line, status, disruption_reason))

# Define schema for the DataFrame
schema = StructType([
    StructField("status_timestamp", TimestampType(), True),
    StructField("line", StringType(), True),
    StructField("status", StringType(), True),
    StructField("disruption_reason", StringType(), True)])

# Create DataFrame
df = spark.createDataFrame(transformed_data, schema)
# Write DataFrame to Databricks SQL table
df.write.format("hive").mode("overwrite").saveAsTable("tfl_tube_status")

# COMMAND ----------

# MAGIC %sql
# MAGIC --Verify the results in the table to check the latest status of all tube lines
# MAGIC select * from tfl_tube_status
