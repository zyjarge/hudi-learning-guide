from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import time

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("HudiPartitionTest") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.jars", "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1.jar") \
    .getOrCreate()

partition_table_path = "hdfs://namenode:9000/user/hive/warehouse/hudi_partition_test.db/hudi_partition_table"

print("=== Step 1: Create Table with Partitioned Data ===")
schema = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("value", IntegerType(), True),
    StructField("ts", LongType(), True),
])

data = [
    ("1", "Product_A", "electronics", 100, int(time.time() * 1000)),
    ("2", "Product_B", "electronics", 200, int(time.time() * 1000)),
    ("3", "Product_C", "clothing", 150, int(time.time() * 1000)),
    ("4", "Product_D", "clothing", 250, int(time.time() * 1000)),
    ("5", "Product_E", "food", 80, int(time.time() * 1000)),
    ("6", "Product_F", "food", 120, int(time.time() * 1000)),
]

df = spark.createDataFrame(data, schema)

hudi_options = {
    "hoodie.table.name": "hudi_partition_table",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.partitionpath.field": "category",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "bulk_insert",
    "hoodie.datasource.write.hive_style_partitioning": "true",
}

print("Writing partitioned table...")
df.write.format("hudi").options(**hudi_options).mode("overwrite").save(partition_table_path)

print("\n=== Step 2: Query Full Table ===")
full_df = spark.read.format("hudi").load(partition_table_path)
print(f"Total records: {full_df.count()}")
full_df.select("id", "name", "category", "value", "_hoodie_partition_path").show(10, truncate=False)

print("\n=== Step 3: Query by Partition (Partition Pruning) ===")
electronics_df = spark.read.format("hudi").load(f"{partition_table_path}/category=electronics")
print(f"Electronics records: {electronics_df.count()}")
electronics_df.select("id", "name", "category", "value").show(10, truncate=False)

print("\n=== Step 4: Add New Data to Different Partition ===")
new_data = [
    ("7", "Product_G", "electronics", 300, int(time.time() * 1000)),
    ("8", "Product_H", "books", 50, int(time.time() * 1000)),
]
df_new = spark.createDataFrame(new_data, schema)
df_new.write.format("hudi").options(**hudi_options).mode("append").save(partition_table_path)

print("\n=== Step 5: Verify All Partitions ===")
final_df = spark.read.format("hudi").load(partition_table_path)
print(f"Total records: {final_df.count()}")
print("\nRecords by partition:")
final_df.groupBy("_hoodie_partition_path").count().show()

print("\n=== Step 6: Time Travel by Partition ===")
commits = [r._hoodie_commit_time for r in spark.read.format("hudi").load(partition_table_path).select("_hoodie_commit_time").distinct().orderBy("_hoodie_commit_time").collect()]
print(f"Commits: {commits}")

if len(commits) > 0:
    first = commits[0]
    print(f"\nQuery as of {first}:")
    tt_df = spark.read.format("hudi").option("as.of.instant", first).load(partition_table_path)
    print(f"Records: {tt_df.count()}")
    tt_df.groupBy("_hoodie_partition_path").count().show()

print("\n=== Partition Test Completed ===")
spark.stop()
