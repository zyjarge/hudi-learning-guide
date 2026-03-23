from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import time

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("HudiCompactionTest") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.jars", "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1.jar") \
    .getOrCreate()

compaction_table_path = "hdfs://namenode:9000/user/hive/warehouse/hudi_compaction_test.db/hudi_compaction_table"

print("=== Step 1: Create MOR Table with Inline Compaction Disabled ===")

schema = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("value", IntegerType(), True),
    StructField("ts", LongType(), True),
])

initial_data = [
    ("1", "Record_A", 100, int(time.time() * 1000)),
    ("2", "Record_B", 200, int(time.time() * 1000)),
]

df_initial = spark.createDataFrame(initial_data, schema)

hudi_options = {
    "hoodie.table.name": "hudi_compaction_table",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.table.type": "MERGE_ON_READ",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.compact.inline": "false",
    "hoodie.compact.inline.max.delta.commits": 5,
    "hoodie.cleaner.policy": "KEEP_LATEST_COMMITS",
    "hoodie.keep.min.commits": 6,
    "hoodie.keep.max.commits": 8,
    "hoodie.metadata.enable": "true",
    "hoodie.metadata.index.column.stats.enable": "true",
}

print("Writing initial data...")
df_initial.write.format("hudi").options(**hudi_options).mode("overwrite").save(compaction_table_path)

print("\n=== Step 2: Insert Multiple Batches to Create Delta Commits ===")
for i in range(3):
    batch_data = [
        (f"{i+10}", f"Record_{i+10}", (i+1)*100, int(time.time() * 1000)),
    ]
    df_batch = spark.createDataFrame(batch_data, schema)
    df_batch.write.format("hudi").options(**hudi_options).mode("append").save(compaction_table_path)
    print(f"Inserted batch {i+1}")

print("\n=== Step 3: Check Timeline and File Layout ===")
commits = [r._hoodie_commit_time for r in spark.read.format("hudi").load(compaction_table_path).select("_hoodie_commit_time").distinct().orderBy("_hoodie_commit_time").collect()]
print(f"Total commits: {len(commits)}")
print(f"Commits: {commits}")

print("\nListing HDFS files:")
import subprocess
result = subprocess.run(
    ["docker", "exec", "hudi-hdfs-namenode", "hdfs", "dfs", "-ls", "-R", compaction_table_path],
    capture_output=True, text=True
)
for line in result.stdout.split('\n')[-20:]:
    if line:
        print(line)

print("\n=== Step 4: Manual Compaction via Hudi CLI ===")
print("Compaction status will be checked after running...")

print("\n=== Step 5: Schedule and Run Compaction ===")
from pyspark.sql.functions import lit

compact_options = {
    "hoodie.compact.schedule": "true",
    "hoodie.compact.ask.inline": "true",
}

df_empty = spark.createDataFrame([], schema)
df_empty.write.format("hudi").options(**compact_options).mode("append").save(compaction_table_path)

print("\n=== Step 6: Check Compaction Plan ===")
result = subprocess.run(
    ["docker", "exec", "hudi-hdfs-namenode", "hdfs", "dfs", "-ls", f"{compaction_table_path}/.hoodie/"],
    capture_output=True, text=True
)
print("Hoodie directory contents:")
for line in result.stdout.split('\n'):
    if 'compaction' in line.lower() or '.log' in line.lower():
        print(line)

print("\n=== Step 7: Query Table Before/After Compaction ===")
print("Current records:")
current_df = spark.read.format("hudi").load(compaction_table_path)
print(f"Total: {current_df.count()}")
current_df.select("id", "name", "value", "_hoodie_commit_time").orderBy("id").show(10, truncate=False)

print("\n=== Step 8: Verify MOR Table Properties ===")
props = spark.read.format("hudi").option("includeHoodieConfig", "true").load(compaction_table_path)
print("Table configuration loaded successfully")

print("\n=== Compaction Test Completed ===")
spark.stop()
