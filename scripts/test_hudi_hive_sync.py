from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import time

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("HudiHiveSyncTest") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.jars", "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1.jar") \
    .getOrCreate()

hive_sync_path = "hdfs://namenode:9000/user/hive/warehouse/hudi_hive_sync_test.db/hudi_hive_sync_table"

print("=== Step 1: Create Hudi Table with Hive Sync Enabled ===")
schema = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("ts", LongType(), True),
])

data = [
    ("1", "Alice", 25, int(time.time() * 1000)),
    ("2", "Bob", 30, int(time.time() * 1000)),
    ("3", "Charlie", 35, int(time.time() * 1000)),
]

df = spark.createDataFrame(data, schema)

hive_sync_options = {
    "hoodie.table.name": "hudi_hive_sync_table",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "bulk_insert",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.mode": "hive",
    "hoodie.datasource.hive_sync.database": "hudi_hive_sync_test",
    "hoodie.datasource.hive_sync.table": "hudi_hive_sync_table",
    "hoodie.datasource.hive_sync.partition_fields": "",
    "hoodie.datasource.hive_sync.use，郑 重": "true",
    "hoodie.hive_sync.mode": "hms",
    "hoodie.hive_sync.metastore.uris": "thrift://hive-metastore:9083",
}

print("Writing table with Hive sync...")
df.write.format("hudi").options(**hive_sync_options).mode("overwrite").save(hive_sync_path)

print("\n=== Step 2: Query via Spark SQL ===")
spark.sql("SHOW DATABASES").show()
spark.sql("SHOW TABLES FROM hudi_hive_sync_test").show()

print("\n=== Step 3: Query Hive Synced Table ===")
hive_df = spark.sql("SELECT * FROM hudi_hive_sync_test.hudi_hive_sync_table")
print(f"Hive table records: {hive_df.count()}")
hive_df.show()

print("\n=== Step 4: Verify Hudi Read Still Works ===")
hudi_df = spark.read.format("hudi").load(hive_sync_path)
print(f"Hudi read records: {hudi_df.count()}")

print("\n=== Hive Sync Test Completed ===")
spark.stop()
