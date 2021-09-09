from datetime import time
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import FloatType, IntegerType
import pyspark.sql.functions as func

# Command: spark-submit pdp_assignment_3.py

if __name__ == "__main__":
    # Create a SparkSession (the config bit is only for Windows!)
    spark = SparkSession.builder.appName("PDP_Assignment_3").getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    # Headers: Survived,Pclass,Name,Sex,Age,Siblings/Spouses Aboard,Parents/Children Aboard,Fare
    # Load the raw data
    df = spark.read.option("header", "true").csv(
        "hdfs:///user/maria_dev/titanic.csv")
    
    df = df.withColumn('fare', df['fare'].cast(FloatType()))
    df = df.withColumn('Survived', df['Survived'].cast(IntegerType()))

    # Assignment 3a. Calculate per sex and class    
    df.groupBy(['Sex', 'Pclass']) \
        .agg(func.sum("Survived").alias('Survived'), func.count("Survived").alias('Total')) \
        .withColumn('%', func.round((func.col('Survived')/func.col('Total'))*100,2)) \
        .sort(['Sex', 'Pclass']) \
        .show()

    fareExpectation = df.groupBy('pClass').avg('fare').orderBy('pClass', ascending = True)
    fareExpectation.show()

    # Stop the session
    spark.stop()
