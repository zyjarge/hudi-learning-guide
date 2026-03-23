from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import time

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("HudiIncrementalPullTest") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.jars", "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1.jar") \
    .getOrCreate()

table_path = "hdfs://namenode:9000/user/hive/warehouse/hudi_test_cow.db/hudi_cow_table"

print("=== Step 1: Get All Commits Timeline ===")
commits_df = spark.read.format("hudi").load(table_path).select("_hoodie_commit_time").distinct().orderBy("_hoodie_commit_time")
all_commits = [row._hoodie_commit_time for row in commits_df.collect()]
print(f"Available commits ({len(all_commits)}): {all_commits}")

print("\n=== Step 2: Full Table Snapshot ===")
full_df = spark.read.format("hudi").load(table_path)
print(f"Full table records: {full_df.count()}")
full_df.show(truncate=False)

print("\n=== Step 3: Time Travel - Query as of First Commit ===")
begin_time = all_commits[0]
print(f"Querying table state at commit: {begin_time}")
time_travel_start = spark.read.format("hudi") \
    .option("as.of.instant", begin_time) \
    .load(table_path)
print(f"Records at {begin_time}: {time_travel_start.count()}")
time_travel_start.show(truncate=False)

print("\n=== Step 4: Time Travel - Query as of Second Commit ===")
if len(all_commits) >= 2:
    second_commit = all_commits[1]
    print(f"Querying table state at commit: {second_commit}")
    time_travel_second = spark.read.format("hudi") \
        .option("as.of.instant", second_commit) \
        .load(table_path)
    print(f"Records at {second_commit}: {time_travel_second.count()}")
    time_travel_second.show(truncate=False)

print("\n=== Step 5: Time Travel Query - As of Previous Commit ===")
if len(all_commits) >= 2:
    prev_commit = all_commits[-2]
    print(f"Querying as of commit: {prev_commit}")
    
    time_travel_df = spark.read.format("hudi") \
        .option("as.of.instant", prev_commit) \
        .load(table_path)
    
    print(f"Records as of {prev_commit}: {time_travel_df.count()}")
    time_travel_df.show(truncate=False)

print("\n=== Step 6: Insert New Records to Create More Commits ===")
schema = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("ts", LongType(), True),
])

new_records = [
    ("5", "NewUser1", 22, int(time.time() * 1000)),
    ("6", "NewUser2", 24, int(time.time() * 1000)),
]

df_new = spark.createDataFrame(new_records, schema)

hudi_options = {
    "hoodie.table.name": "hudi_cow_table",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "bulk_insert",
}

print("Inserting new records...")
df_new.write.format("hudi").options(**hudi_options).mode("append").save(table_path)

print("\n=== Step 7: Verify New Commits ===")
commits_df2 = spark.read.format("hudi").load(table_path).select("_hoodie_commit_time").distinct().orderBy("_hoodie_commit_time")
all_commits2 = [row._hoodie_commit_time for row in commits_df2.collect()]
print(f"Updated commits ({len(all_commits2)}): {all_commits2}")

print("\n=== Step 8: Time Travel - Query as of Previous State ===")
if len(all_commits) >= 1:
    prev_last = all_commits[-1]
    print(f"Querying as of previous last commit: {prev_last}")
    
    time_travel_prev = spark.read.format("hudi") \
        .option("as.of.instant", prev_last) \
        .load(table_path)
    
    print(f"Records as of {prev_last}: {time_travel_prev.count()}")
    time_travel_prev.show(truncate=False)

print("\n=== Step 9: Verify Final Table State ===")
final_df = spark.read.format("hudi").load(table_path)
print(f"Final table records: {final_df.count()}")
final_df.show(truncate=False)

print("\n=== Incremental Pull Test Completed Successfully! ===")
spark.stop()
