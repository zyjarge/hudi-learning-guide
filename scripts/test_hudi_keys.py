from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import time

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("HudiKeyGeneratorTest") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.jars", "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1.jar") \
    .getOrCreate()

print("=== Test 1: Simple Key (Single Field) ===")
simple_path = "hdfs://namenode:9000/user/hive/warehouse/hudi_keys_test.db/simple_key"

schema = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("value", IntegerType(), True),
])

data = [("1", "Alice", 100), ("2", "Bob", 200)]
df = spark.createDataFrame(data, schema)

simple_options = {
    "hoodie.table.name": "simple_key",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "value",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "bulk_insert",
}

df.write.format("hudi").options(**simple_options).mode("overwrite").save(simple_path)
result = spark.read.format("hudi").load(simple_path)
print(f"Record key field: id")
print(f"Records: {result.count()}")
result.select("_hoodie_record_key", "id", "name").show(truncate=False)

print("\n=== Test 2: Composite Key (Multiple Fields) ===")
composite_path = "hdfs://namenode:9000/user/hive/warehouse/hudi_keys_test.db/composite_key"

composite_schema = StructType([
    StructField("tenant_id", StringType(), False),
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), True),
    StructField("timestamp", LongType(), True),
])

composite_data = [
    ("tenant1", "evt1", "click", int(time.time() * 1000)),
    ("tenant1", "evt2", "view", int(time.time() * 1000)),
    ("tenant2", "evt1", "click", int(time.time() * 1000)),
]
cdf = spark.createDataFrame(composite_data, composite_schema)

composite_options = {
    "hoodie.table.name": "composite_key",
    "hoodie.datasource.write.recordkey.field": "tenant_id:event_id",
    "hoodie.datasource.write.precombine.field": "timestamp",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "bulk_insert",
}

cdf.write.format("hudi").options(**composite_options).mode("overwrite").save(composite_path)
cresult = spark.read.format("hudi").load(composite_path)
print(f"Composite record key: tenant_id:event_id")
print(f"Records: {cresult.count()}")
cresult.select("_hoodie_record_key", "tenant_id", "event_id", "event_type").show(truncate=False)

print("\n=== Test 3: Timestamp-based Key Generator ===")
timestamp_path = "hdfs://namenode:9000/user/hive/warehouse/hudi_keys_test.db/timestamp_key"

ts_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("order_time", LongType(), False),
])

ts_data = [
    ("ord001", "cust1", 100, int(time.time() * 1000)),
    ("ord002", "cust2", 200, int(time.time() * 1000)),
]
tsdf = spark.createDataFrame(ts_data, ts_schema)

timestamp_options = {
    "hoodie.table.name": "timestamp_key",
    "hoodie.datasource.write.recordkey.field": "order_id",
    "hoodie.datasource.write.precombine.field": "order_time",
    "hoodie.datasource.write.partitionpath.field": "order_time",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "bulk_insert",
    "hoodie.datasource.write.keygenerator.class.name": "org.apache.hudi.keygen.TimestampBasedKeyGenerator",
    "hoodie.datasource.write.partitionpath.dateformat": "yyyy/MM/dd",
}

tsdf.write.format("hudi").options(**timestamp_options).mode("overwrite").save(timestamp_path)
tsresult = spark.read.format("hudi").load(timestamp_path)
print(f"Timestamp-based partition: yyyy/MM/dd")
print(f"Records: {tsresult.count()}")
tsresult.select("order_id", "_hoodie_partition_path", "amount").show(truncate=False)

print("\n=== Test 4: Complex Key Generator (Custom Partition) ===")
complex_path = "hdfs://namenode:9000/user/hive/warehouse/hudi_keys_test.db/complex_key"

complex_data = [
    ("txn1", "user1", 100, "2026-03-23"),
    ("txn2", "user2", 200, "2026-03-23"),
    ("txn3", "user3", 300, "2026-03-22"),
]
complex_df = spark.createDataFrame(complex_data, ["txn_id", "user_id", "amount", "txn_date"])

complex_options = {
    "hoodie.table.name": "complex_key",
    "hoodie.datasource.write.recordkey.field": "txn_id",
    "hoodie.datasource.write.precombine.field": "amount",
    "hoodie.datasource.write.partitionpath.field": "txn_date",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "bulk_insert",
    "hoodie.datasource.write.hive_style_partitioning": "true",
}

complex_df.write.format("hudi").options(**complex_options).mode("overwrite").save(complex_path)
compresult = spark.read.format("hudi").load(complex_path)
print(f"Date-based partition: txn_date")
print(f"Records: {compresult.count()}")
compresult.select("txn_id", "_hoodie_partition_path", "amount").show(truncate=False)

print("\n=== Key Generator Test Completed ===")
spark.stop()
