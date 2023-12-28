from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark
import pyspark.sql.functions as F
import os
import logging

""" Initialization """  
HOST = os.getenv('POSTGRES_CONTAINER_NAME')
DB = os.getenv('POSTGRES_DB')
USERNAME = os.getenv('POSTGRES_USER')
PASSWORD = os.getenv('POSTGRES_PASSWORD')

""" SETUP JDBC URL """
localHOST = f"jdbc:postgresql://{HOST}/{DB}"
properties = {
    "user": USERNAME,
    "password": PASSWORD,
    "driver": "org.postgresql.Driver"
}

""" SETUP SPARK SESSION """
spark_session = SparkSession.builder \
    .appName("Read-Data") \
    .getOrCreate()

# Set Spark log level
spark_context = spark_session.sparkContext
spark_context.setLogLevel("WARN")

# Add logging statement
logging.info("Reading data from PostgreSQL")

""" Read data from PostgreSQL """
df = spark_session.read.jdbc(
    localHOST,
    "public.retail",
    properties=properties
)
# Show the first 10 rows
df.show(5)

# Add logging statement
logging.info("Finished reading data from PostgreSQL")

# Amount data
print(f"Amount Data: {df.count()}")

""" Transform """
def transformData(data, columns:list):
    data = data.withColumn(columns[0], F.col(columns[0]).cast(IntegerType()))
    data = data.withColumn(columns[1], F.col(columns[1]).cast(IntegerType()))
    data = data.withColumn(columns[3], F.col(columns[3]).cast(IntegerType()))
    data = data.withColumn(columns[5], F.col(columns[5]).cast(FloatType()))
    data = data.withColumn(columns[6], F.col(columns[6]).cast(IntegerType()))
    return data
    
""" Cleaning """
def cleaningData(data, columns:list):
    for col in columns:
        data = data.withColumn(col, F.trim(F.col(col)))
    return data

# Get all Columns
colName = df.columns

# Initialize Column
invoiceNo = colName[0]
stockCode = colName[1]
description = colName[2]
quantity = colName[3]
invoiceDate = colName[4]
unitPrice = colName[5]
custumerId = colName[6]
country = colName[7]


""" Data Frame """
newDF = transformData(
    data=df,
    columns=colName
)

newDF = cleaningData(
    data=newDF,
    columns=colName
)


""" Analyze Data """
# Total Sales per day
newDF = newDF.withColumn(
    'totalSales',
    (F.col(quantity) * F.col(unitPrice)).cast(FloatType())
    )

newDF = newDF.dropDuplicates()

# Total Sales
totalPerMonth = newDF.groupBy(F.to_date(F.col(invoiceDate)).alias('date')) \
                .agg(F.round(F.sum('totalSales'), 2) \
                .alias('totalSales')) \
                .orderBy(F.col('date').desc())

totalPerMonth.na.drop(subset=['date'])
totalPerMonth.show(10)

""" CAST DATA TYPE """
totalPerMonth = totalPerMonth.withColumn('date', F.col('date').cast(DateType())) \
                            .withColumn('totalSales', F.col('totalSales').cast(FloatType()))
totalPerMonth.printSchema()

# Top Selling Products
topSelling = newDF.groupBy(F.col(stockCode),F.col(description)) \
                .agg(F.round(F.sum('totalSales'), 2) \
                .alias('totalSales')) \
                .orderBy(F.col('totalSales').desc())

topSelling.na.fill({stockCode:1, description:"Unknown", 'totalSales':0})
topSelling.show(10)

""" CAST DATA TYPE """
topSelling = topSelling.withColumn('totalSales', F.col('totalSales').cast(FloatType()))
topSelling.printSchema()

# Trend product based on Country
trendCountry = newDF.groupBy(F.col(description),F.col(country)) \
                .agg(F.countDistinct(F.col(custumerId)) \
                .alias('totalCustomer')) \
                .orderBy(F.col('totalCustomer').desc())
trendCountry.na.fill({description:"Unknown", country:"Unknown", 'totalCustomer':0})
trendCountry.show(10)

""" CAST DATA TYPE """
trendCountry = trendCountry.withColumn('totalCustomer', F.col('totalCustomer').cast(IntegerType()))
trendCountry.printSchema()

""" Save into Memory """
totalPerMonth.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
topSelling.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
trendCountry.persist(pyspark.StorageLevel.MEMORY_AND_DISK)

""" Write Data to CSV """
DESTINATION = '/spark-scripts'

totalPerMonth.write.option("header",True) \
                    .option('delimiter','|') \
                    .mode("overwrite") \
                    .partitionBy("date") \
                    .parquet(f"{DESTINATION}/totalPerMonth")

topSelling.write.option("header",True) \
                .option('delimiter','|') \
                .mode("overwrite") \
                .partitionBy("totalSales") \
                .parquet(f"{DESTINATION}/topSelling")

trendCountry.write.option("header",True) \
                .option('delimiter','|') \
                .mode("overwrite") \
                .partitionBy("country") \
                .parquet(f"{DESTINATION}/trendCountry")

""" Realease Data from Memory """
totalPerMonth.unpersist()
topSelling.unpersist()
trendCountry.unpersist()

""" Close Spark Session """
spark_session.stop()