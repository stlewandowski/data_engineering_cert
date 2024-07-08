import os
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("MongoDBIntegration") \
    .getOrCreate()

# Define MongoDB parameters

# Read 100 documents from MongoDB into a DataFrame
df = spark.read \
    .format("mongo") \
    .option("uri", f"mongodb://{os.getenv('MONGOUSER')}:{os.getenv('MONGOPASS')}@mongod0:27017/certstream.certstream_raw_test?authSource=admin") \
    .option("pipeline", '[{"$limit": 1000}]') \
    .load()

# Show the DataFrame
#df.show()
df.printSchema()
print(df.columns)
df.show(5, False)

# only take the columns we need
df1 = df.select("all_domains", "subject_CN", "extensions_subjectAltName", "extensions_keyUsage", "update_type")
df1.printSchema()
print(df.columns)
df1.show(5, False)

# filter
df_filtered = df1.filter(df1.update_type == 'X509LogEntry')
df_filtered.show(20, False)
#
# # Define ClickHouse parameters
# clickhouse_url = "clickhouse://clickhouse:8123/default"
#
# # Write the DataFrame to ClickHouse
# df.write \
#     .format("jdbc") \
#     .option("url", clickhouse_url) \
#     .option("dbtable", "certstream_raw_test") \
#     .option("user", "default") \
#     .option("password", "") \
#     .save()