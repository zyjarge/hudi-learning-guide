from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import time

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("HudiMORIncrementalTest") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.jars", "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1.jar") \
    .getOrCreate()

mor_table_path = "hdfs://namenode:9000/user/hive/warehouse/hudi_test_mor.db/hudi_mor_table"

print("=== Step 1: Get MOR Table Commit Timeline ===")
mor_commits_df = spark.read.format("hudi").load(mor_table_path).select("_hoodie_commit_time").distinct().orderBy("_hoodie_commit_time")
mor_commits = [row._hoodie_commit_time for row in mor_commits_df.collect()]
print(f"MOR Table commits: {mor_commits}")
print(f"Current records: {spark.read.format('hudi').load(mor_table_path).count()}")

print("\n=== Step 2: Full Snapshot Query (Default MOR Read) ===")
mor_full = spark.read.format("hudi").load(mor_table_path)
print(f"MOR full snapshot records: {mor_full.count()}")
mor_full.show(truncate=False)

print("\n=== Step 3: Time Travel Query on MOR ===")
if len(mor_commits) >= 1:
    first_mor_commit = mor_commits[0]
    print(f"Querying MOR as of: {first_mor_commit}")
    mor_time_travel = spark.read.format("hudi") \
        .option("as.of.instant", first_mor_commit) \
        .load(mor_table_path)
    print(f"Records at {first_mor_commit}: {mor_time_travel.count()}")
    mor_time_travel.show(truncate=False)

print("\n=== Step 4: Incremental Pull Query on MOR ===")
if len(mor_commits) >= 1:
    begin_mor = mor_commits[0]
    print(f"Incremental query from: {begin_mor}")
    
    mor_incremental = spark.read.format("hudi") \
        .option("hoodie.datasource.query.type", "incremental") \
        .option("hoodie.datasource.read.begin.instanttime", begin_mor) \
        .load(mor_table_path)
    
    print(f"Incremental records since {begin_mor}: {mor_incremental.count()}")
    mor_incremental.show(truncate=False)

print("\n=== Step 5: Insert More Data into MOR Table ===")
schema = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("ts", LongType(), True),
])

new_mor_records = [
    ("4", "Student4", 88, int(time.time() * 1000)),
    ("5", "Student5", 92, int(time.time() * 1000)),
    ("6", "Student6", 75, int(time.time() * 1000)),
]

df_mor_new = spark.createDataFrame(new_mor_records, schema)

mor_options = {
    "hoodie.table.name": "hudi_mor_table",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.table.type": "MERGE_ON_READ",
    "hoodie.datasource.write.operation": "upsert",
}

print("Inserting new students into MOR table...")
df_mor_new.write.format("hudi").options(**mor_options).mode("append").save(mor_table_path)

print("\n=== Step 6: Verify New Commits ===")
mor_commits_df2 = spark.read.format("hudi").load(mor_table_path).select("_hoodie_commit_time").distinct().orderBy("_hoodie_commit_time")
mor_commits2 = [row._hoodie_commit_time for row in mor_commits_df2.collect()]
print(f"Updated MOR commits: {mor_commits2}")

print("\n=== Step 7: Incremental Pull - Get Only New Changes ===")
if len(mor_commits) >= 1 and len(mor_commits2) > len(mor_commits):
    last_old_commit = mor_commits[-1]
    print(f"Fetching changes since last commit: {last_old_commit}")
    
    mor_incremental2 = spark.read.format("hudi") \
        .option("hoodie.datasource.query.type", "incremental") \
        .option("hoodie.datasource.read.begin.instanttime", last_old_commit) \
        .load(mor_table_path)
    
    print(f"New records since {last_old_commit}: {mor_incremental2.count()}")
    mor_incremental2.show(truncate=False)

print("\n=== Step 8: Compare Snapshot vs Incremental ===")
print("Full snapshot (after insert):")
full_after = spark.read.format("hudi").load(mor_table_path)
print(f"Total records: {full_after.count()}")
full_after.show(truncate=False)

print("Incremental only (new records):")
if len(mor_commits2) >= 2:
    prev_commit = mor_commits2[-2]
    incremental_only = spark.read.format("hudi") \
        .option("hoodie.datasource.query.type", "incremental") \
        .option("hoodie.datasource.read.begin.instanttime", prev_commit) \
        .load(mor_table_path)
    print(f"New records since {prev_commit}: {incremental_only.count()}")
    incremental_only.show(truncate=False)

print("\n=== MOR Incremental Test Completed! ===")
spark.stop()
