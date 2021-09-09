# @Author: Sjors Grooff
# Date: 09-09-2021
# Command: spark-submit pdp_assignment_3.py

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, IntegerType
import pyspark.sql.functions as func

if __name__ == "__main__":
    # Create a SparkSession (the config bit is only for Windows!)
    spark = SparkSession.builder.appName("PDP_Assignment_3").getOrCreate()
    # Remove most of the logging
    spark.sparkContext.setLogLevel('WARN')

    # Headers: Survived,Pclass,Name,Sex,Age,Siblings/Spouses Aboard,Parents/Children Aboard,Fare
    # Load the raw data and use the headers from the csv file
    df = spark.read.option("header", "true").csv(
        "hdfs:///user/maria_dev/titanic.csv")
    
    df = df.withColumn('fare', df['fare'].cast(FloatType()))
    df = df.withColumn('Survived', df['Survived'].cast(IntegerType()))

    # Assignment 3a. Calculate survivability per sex and class    
    df.groupBy(['Sex', 'Pclass']) \
        .agg(func.sum("Survived").alias('Survived'), func.count("Survived").alias('Total')) \
        .withColumn('%', func.round((func.col('Survived')/func.col('Total'))*100,2)) \
        .sort(['Sex', 'Pclass']) \
        .show()

    # Assignment 3c.
    fareExpectation = df.groupBy('pClass').avg('fare').orderBy('pClass', ascending = True)
    fareExpectation.show()

    # Stop the session
    spark.stop()
