# Databricks notebook source
# DBTITLE 1,Import HTTP Request and sql types
import requests
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# COMMAND ----------

# DBTITLE 1,Convert JSON Response to DataFrame
TFL_API = "https://api.tfl.gov.uk/Line/Mode/tube/Status"
response = requests.get(TFL_API)

# Check if request was successful
if response.status_code == 200:
    # Convert JSON response to DataFrame
    json_data = response.json()
    
    # Define the schema for the DataFrame
    schema = StructType([
        StructField("$type", StringType(), True),
        StructField("id", StringType(), True),
        StructField("modeName", StringType(), True),
        StructField("name", StringType(), True),
        StructField("disruptions", StringType(), True),
        StructField("created", StringType(), True),
        StructField("modified", StringType(), True),
        StructField("lineStatuses", ArrayType(StructType([
            StructField("$type", StringType(), True),
            StructField("id", StringType(), True),
            StructField("statusSeverity", StringType(), True),
            StructField("statusSeverityDescription", StringType(), True),
            StructField("reason", StringType(), True),
            StructField("created", StringType(), True),
            StructField("validityPeriods", ArrayType(StructType([
                StructField("$type", StringType(), True),
                StructField("fromDate", StringType(), True),
                StructField("toDate", StringType(), True),
                StructField("isNow", StringType(), True)
            ])), True)
        ])), True)
    ])
    
    # Create DataFrame with the specified schema
    df = spark.createDataFrame(json_data, schema=schema)
    df.show()
else:
    print("Failed to fetch data from API:", response.status_code, response.reason)

# COMMAND ----------

# DBTITLE 1,tempView with Line Statuses
df.createOrReplaceTempView('tfl')
results = spark.sql(" SELECT created as current_timestamp, id as line , flt.statusSeverityDescription as status,flt.reason as disruption_reason \
FROM tfl \
LATERAL VIEW explode(lineStatuses) AS flt")

# COMMAND ----------

# DBTITLE 1,Spark SQL Create, Write, and Save Code
spark.sql('create database if not exists dbo')
results.write.format("delta").mode("overwrite").saveAsTable("dbo.tubelinestat")

# COMMAND ----------

# DBTITLE 1,SQL Select Tubeline Stats
# MAGIC %sql
# MAGIC select * from dbo.tubelinestat
