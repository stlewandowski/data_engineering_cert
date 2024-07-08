"""SimpleApp.py"""
from pyspark.sql import SparkSession

logFile = r"C:\Users\slewan\Downloads\names_8085.txt"
spark = (SparkSession
         .builder
         .appName("SimpleApp")
         .master("local[4]")
         .getOrCreate()
         )
logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

spark.stop()