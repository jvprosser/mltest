from __future__ import print_function
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType

spark = SparkSession\
    .builder\
    .appName("PythonSQL")\
    .config("spark.executor.memory", "4g")\
    .config("spark.executor.instances", 2)\
    .config("spark.yarn.access.hadoopFileSystems","s3a://ml-field/demo/flight-analysis/data/")\
    .config("spark.driver.maxResultSize","4g")\
    .config("spark.hadoop.fs.s3a.s3guard.ddb.region", "us-west-2")\
    .getOrCreate()

spark.sql("SHOW databases").show()
spark.sql("USE hospdb")
spark.sql("SHOW tables").show()

#spark.sql("SELECT COUNT(*) FROM `default`.`flights`").show()
#spark.sql("SELECT * FROM `default`.`covid_mv` LIMIT 10").take(5)
#spark.sql("SELECT DepDelay FROM `default`.`flights` WHERE DepDelay > 0.0").take(5)

#spark.sql("SELECT COUNT(*) FROM `default`.`airports`").show()
spark.sql("SELECT * FROM `hospdb`.`cost_age_flt` LIMIT 10").show()
