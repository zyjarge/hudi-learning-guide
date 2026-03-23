from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("HudiQueryAndUpsertTest") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.jars", "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1.jar") \
    .getOrCreate()

table_path = "hdfs://namenode:9000/user/hive/warehouse/hudi_test_cow.db/hudi_cow_table"

print("=== Querying Hudi Table ===")
result_df = spark.read.format("hudi").load(table_path)
result_df.show()
print(f"Total records: {result_df.count()}")

print("\n=== Time Travel Query (as of first commit) ===")
commits = result_df.select("_hoodie_commit_time").distinct().collect()
print(f"Available commits: {[c[0] for c in commits]}")

if len(commits) >= 1:
    first_commit = sorted([c[0] for c in commits])[0]
    print(f"Querying as of commit: {first_commit}")
    time_travel_df = spark.read.format("hudi").option("as.of.instant", first_commit).load(table_path)
    time_travel_df.show()

print("\n=== Upsert Test (update existing record) ===")
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import time

schema = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("ts", LongType(), True),
])

updated_data = [
    ("1", "Alice_Updated", 26, int(time.time() * 1000)),
    ("4", "David", 28, int(time.time() * 1000)),
]

df_update = spark.createDataFrame(updated_data, schema)

hudi_options = {
    "hoodie.table.name": "hudi_cow_table",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "upsert",
}

print("Performing upsert...")
df_update.write.format("hudi").options(**hudi_options).mode("append").save(table_path)

print("\n=== Querying After Upsert ===")
final_df = spark.read.format("hudi").load(table_path)
final_df.show()
print(f"Total records after upsert: {final_df.count()}")

print("\nHudi Query and Upsert Test Completed!")
spark.stop()
