from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("HudiMORTableTest") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.jars", "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1.jar") \
    .getOrCreate()

print("=== Creating MOR Table ===")

schema = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("ts", LongType(), True),
])

data = [
    ("1", "Student1", 85, 1700000000000),
    ("2", "Student2", 90, 1700000000000),
    ("3", "Student3", 78, 1700000000000),
]

df = spark.createDataFrame(data, schema)

table_path = "hdfs://namenode:9000/user/hive/warehouse/hudi_test_mor.db/hudi_mor_table"

hudi_options = {
    "hoodie.table.name": "hudi_mor_table",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.table.type": "MERGE_ON_READ",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.compact.inline": "false",
    "hoodie.cleaner.policy": "KEEP_LATEST_COMMITS",
    "hoodie.keep.min.commits": 4,
    "hoodie.keep.max.commits": 5,
}

print("Writing MOR table...")
df.write.format("hudi").options(**hudi_options).mode("overwrite").save(table_path)

print("\n=== Reading MOR Table (Query Default) ===")
mor_df = spark.read.format("hudi").load(table_path)
mor_df.show()
print(f"Total records: {mor_df.count()}")

print("\n=== Incremental Query Test ===")
from pyspark.sql.functions import col
commits_df = spark.read.format("hudi").load(table_path).select("_hoodie_commit_time").distinct()
all_commits = sorted([row[0] for row in commits_df.collect()])
print(f"Available commits: {all_commits}")

if len(all_commits) >= 1:
    first_commit = all_commits[0]
    print(f"Incremental query from commit: {first_commit}")
    
print("\n=== MOR Table Hudi Properties ===")
props_df = spark.read.format("hudi").option("includeHoodieConfig", "true").load(table_path)
props_df.show(1, truncate=False)

print("\nMOR Table Test Completed!")
spark.stop()
