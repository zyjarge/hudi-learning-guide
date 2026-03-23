from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import time

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("HudiDeleteTest") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.jars", "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1.jar") \
    .getOrCreate()

delete_table_path = "hdfs://namenode:9000/user/hive/warehouse/hudi_delete_test.db/hudi_delete_table"

print("=== Step 1: Create Table with Test Data ===")
schema = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("value", IntegerType(), True),
    StructField("ts", LongType(), True),
])

initial_data = [
    ("1", "Alice", 100, int(time.time() * 1000)),
    ("2", "Bob", 200, int(time.time() * 1000)),
    ("3", "Charlie", 300, int(time.time() * 1000)),
    ("4", "David", 400, int(time.time() * 1000)),
    ("5", "Eve", 500, int(time.time() * 1000)),
]

df_initial = spark.createDataFrame(initial_data, schema)

hudi_options = {
    "hoodie.table.name": "hudi_delete_table",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "bulk_insert",
}

df_initial.write.format("hudi").options(**hudi_options).mode("overwrite").save(delete_table_path)

print("Initial records:")
initial_df = spark.read.format("hudi").load(delete_table_path)
print(f"Total: {initial_df.count()}")
initial_df.select("id", "name", "value").show(10, truncate=False)

print("\n=== Step 2: Soft Delete - Mark Record for Deletion ( hoodie.datasource.write.drop.partition.columns ) ===")

delete_data = [
    ("2", "Bob_Deleted", 200, int(time.time() * 1000)),
]

df_soft_delete = spark.createDataFrame(delete_data, schema)
df_soft_delete.write.format("hudi") \
    .option("hoodie.datasource.write.operation", "delete") \
    .options(**hudi_options) \
    .mode("append") \
    .save(delete_table_path)

print("Records after soft delete (record marked):")
after_soft_df = spark.read.format("hudi").load(delete_table_path)
print(f"Total: {after_soft_df.count()}")
after_soft_df.select("id", "name", "value", "_hoodie_commit_time").show(10, truncate=False)

print("\n=== Step 3: Delete via Spark SQL ===")
spark.sql(f"DELETE FROM hudi.`{delete_table_path}` WHERE id = '3'").show()

print("Records after SQL delete:")
after_sql_df = spark.read.format("hudi").load(delete_table_path)
print(f"Total: {after_sql_df.count()}")
after_sql_df.select("id", "name", "value", "_hoodie_commit_time").show(10, truncate=False)

print("\n=== Step 4: Time Travel - Query Before Deletes ===")
commits = [r._hoodie_commit_time for r in spark.read.format("hudi").load(delete_table_path).select("_hoodie_commit_time").distinct().orderBy("_hoodie_commit_time").collect()]
if len(commits) > 1:
    first_commit = commits[0]
    print(f"Query as of first commit: {first_commit}")
    before_delete = spark.read.format("hudi").option("as.of.instant", first_commit).load(delete_table_path)
    print(f"Records before deletes: {before_delete.count()}")
    before_delete.select("id", "name", "value").show(10, truncate=False)

print("\n=== Step 5: Verify Current State ===")
current = spark.read.format("hudi").load(delete_table_path)
print(f"Current records: {current.count()}")
print("\nTimeline:")
current.select("_hoodie_commit_time").distinct().orderBy("_hoodie_commit_time").show(truncate=False)

print("\n=== Delete Test Completed ===")
spark.stop()
