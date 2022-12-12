# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.dbfs.dfs.core.windows.net",
    dbutils.secrets.get(scope="", key=""),
)

DfYellow = (
    spark.read.format("csv")
    .options(header="true", inferSchema="true")
    .option("mode", "PERMISSIVE")
    .load(
        "abfss://taxi@dbfs.dfs.core.windows.net/RawTaxiData/YellowTaxiTripData_201812.csv"
    )
)

DfGreen = (
    spark.read.format("csv")
    .options(header="true", inferSchema="true")
    .option("mode", "PERMISSIVE")
    .option("delimiter", "\t")
    .load(
        "abfss://taxi@dbfs.dfs.core.windows.net/RawTaxiData/GreenTaxiTripData_201812.csv"
    )
)

DimPaymentTypes = (
    spark.read.format("json")
    .options(header="true", inferSchema="true")
    .option("mode", "PERMISSIVE")
    .load("abfss://taxi@dbfs.dfs.core.windows.net/RawTaxiData/PaymentTypes.json")
)

DimRateCodes = (
    spark.read.format("json")
    .options(header="true", inferSchema="true")
    .option("mode", "DropMalformed")
    .load("abfss://taxi@dbfs.dfs.core.windows.net/RawTaxiData/RateCodes.json")
)

DimTaxiZones = (
    spark.read.format("csv")
    .options(header="true", inferSchema="true")
    .option("mode", "PERMISSIVE")
    .load("abfss://taxi@dbfs.dfs.core.windows.net/RawTaxiData/TaxiZones.csv")
)

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Fix the corrupt data in the Rate Codes json file

columns = StructType(
    [
        StructField("RateCode", StringType(), True),
        StructField("RateCodeID", IntegerType(), True),
    ]
)
values = [("Westchester", 4)]

DfCorrupt = spark.createDataFrame(data=values, schema=columns)

DimRateCodes = DfCorrupt.union(DimRateCodes).sort("RateCodeID")

# Filter out rows without passengers or distance

DfGreen = DfGreen[(DfGreen["passenger_count"] != 0) & (DfGreen["trip_distance"] != 0)]

DfYellow = DfYellow[
    (DfYellow["passenger_count"] != 0) & (DfYellow["trip_distance"] != 0)
]

# Drop Duplicates

DfGreen.drop_duplicates()
DfYellow.drop_duplicates()

# Filter out rows which is not needed

DfYellow = DfYellow[
    (DfYellow["tpep_pickup_datetime"] >= "2018-12-01")
    & (DfYellow["tpep_dropoff_datetime"] < "2019-01-01")
]

DfGreen = DfGreen[
    (DfGreen["lpep_pickup_datetime"] >= "2018-12-01")
    & (DfGreen["lpep_dropoff_datetime"] < "2019-01-01")
]

# Rename and drop columns

DfYellow = (
    DfYellow.withColumnRenamed("tpep_pickup_datetime", "PickUpTime")
    .withColumnRenamed("tpep_dropoff_datetime", "DropOffTime")
    .withColumnRenamed("passenger_count", "PassengerCount")
    .withColumnRenamed("trip_distance", "TripDistance")
    .withColumnRenamed("PULocationID", "PickUpLocationID")
    .withColumnRenamed("DOLocationID", "DropOffLocationID")
    .withColumnRenamed("payment_type", "PaymentType")
    .withColumnRenamed("fare_amount", "FareAmount")
    .withColumnRenamed("extra", "Extra")
    .withColumnRenamed("mta_tax", "MTAtax")
    .withColumnRenamed("tip_amount", "TipAmount")
    .withColumnRenamed("tolls_amount", "TollsAmount")
    .withColumnRenamed("improvement_surcharge", "ImprovementSurcharge")
    .withColumnRenamed("total_amount", "TotalAmount")
    .drop("store_and_fwd_flag")
)


DfGreen = (
    DfGreen.withColumnRenamed("lpep_pickup_datetime", "PickUpTime")
    .withColumnRenamed("lpep_dropoff_datetime", "DropOffTime")
    .withColumnRenamed("passenger_count", "PassengerCount")
    .withColumnRenamed("trip_distance", "TripDistance")
    .withColumnRenamed("PULocationID", "PickUpLocationID")
    .withColumnRenamed("DOLocationID", "DropOffLocationID")
    .withColumnRenamed("payment_type", "PaymentType")
    .withColumnRenamed("fare_amount", "FareAmount")
    .withColumnRenamed("extra", "Extra")
    .withColumnRenamed("mta_tax", "MTAtax")
    .withColumnRenamed("tip_amount", "TipAmount")
    .withColumnRenamed("tolls_amount", "TollsAmount")
    .withColumnRenamed("improvement_surcharge", "ImprovementSurcharge")
    .withColumnRenamed("total_amount", "TotalAmount")
    .drop("store_and_fwd_flag")
    .drop("ehail_fee")
    .drop("trip_type")
)

# Add columns

DfYellow = (
    DfYellow.withColumn("TripYear", year(DfYellow.PickUpTime))
    .withColumn("TripMonth", month(DfYellow.PickUpTime))
    .withColumn("PickUpTime", to_timestamp(col("PickUpTime")))
    .withColumn("TripDay", date_format(col("PickUpTime"), "d"))
    .withColumn(
        "TripType",
        when(DfYellow.PassengerCount == 1, "Solo trip").otherwise("Shared Trip"))
    .withColumn(
    "TripTimeMinutes",
    round((unix_timestamp(col("DropOffTime")) - unix_timestamp(col("PickUpTime"))
                         )/60)
          )
)

DfGreen = (
    DfGreen.withColumn("TripYear", year(DfGreen.PickUpTime))
    .withColumn("TripMonth", month(DfGreen.PickUpTime))
    .withColumn("PickUpTime", to_timestamp(col("PickUpTime")))
    .withColumn("TripDay", date_format(col("PickUpTime"), "d"))
    .withColumn(
        "TripType",
        when(DfGreen.PassengerCount == 1, "Solo trip").otherwise("Shared Trip"))
        .withColumn(
    "TripTimeMinutes",
    round((unix_timestamp(col("DropOffTime")) - unix_timestamp(col("PickUpTime"))
                         )/60)
          )
)
    
# DID NOT NEED THE BELOW TRANSFORMATION!

    # Add Cartype dataframe for the union process
    
# Data = [(1, 'Green'), (2, 'Yellow')]


# schema = StructType(
#     [
#         StructField("CarTypeID", IntegerType(), True),
#         StructField("CarType", StringType(), True),
#     ]
# )

# DfType = spark.createDataFrame(data = Data, schema = schema)
  
 # Add CartypeIDs in the dataframes for the union process   
    
DfGreen = DfGreen.withColumn("CarType", (lit("Green")))

DfYellow = DfYellow.withColumn("CarType", (lit("Yellow")))
    

# Merge dataframes and reorder columns   
    
TaxiData = DfGreen.union(DfYellow)

TaxiData = TaxiData.select(
    "CarType",
    "VendorID",
    "TripYear",
    "TripMonth",
    "TripDay",
    "TripType",
    "PickUpTime",
    "DropOffTime",
    "TripTimeMinutes",
    "RatecodeID",
    "PickUpLocationID",
    "DropOffLocationID",
    "PassengerCount",
    "TripDistance",
    "FareAmount",
    "Extra",
    "MTAtax",
    "TipAmount",
    "TollsAmount",
    "ImprovementSurcharge",
    "TotalAmount",
    "PaymentType"
)

display(TaxiData)


TaxiData.write.option("header", True).parquet("abfss://taxi@dbfs.dfs.core.windows.net/CleanTaxiData/FactTaxi")

DimPaymentTypes.write.option("header", True).parquet("abfss://taxi@dbfs.dfs.core.windows.net/CleanTaxiData/DimPaymentTypes")

DimRateCodes.write.option("header", True).parquet("abfss://taxi@dbfs.dfs.core.windows.net/CleanTaxiData/DimRateCodes")

DimTaxiZones.write.option("header", True).parquet("abfss://taxi@dbfs.dfs.core.windows.net/CleanTaxiData/DimTaxiZones")

    
    


# COMMAND ----------

create database if not exists taxiservicewarehouse

# COMMAND ----------

TaxiData.write.option(
    "path", "abfss://taxi@dbfs.dfs.core.windows.net/CleanTaxiData/Database/FactTaxi"
).saveAsTable("taxiservicewarehouse.FactTaxiData")

DimPaymentTypes.write.option(
    "path",
    "abfss://taxi@dbfs.dfs.core.windows.net/CleanTaxiData/Database/DimPaymentTypes",
).saveAsTable("taxiservicewarehouse.DimPaymentTypes")

DimRateCodes.write.option(
    "path", "abfss://taxi@dbfs.dfs.core.windows.net/CleanTaxiData/Database/DimRateCodes"
).saveAsTable("taxiservicewarehouse.DimRateCodes")

DimTaxiZones.write.option(
    "path", "abfss://taxi@dbfs.dfs.core.windows.net/CleanTaxiData/Database/DimTaxiZones"
).saveAsTable("taxiservicewarehouse.DimTaxiZones")
