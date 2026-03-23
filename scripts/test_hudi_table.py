from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("HudiCOWTableTest") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.jars", "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1.jar") \
    .getOrCreate()

print("Spark Version:", spark.version)
print("Hudi Test Starting...")

schema = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("ts", LongType(), True),
])

data = [
    ("1", "Alice", 25, 1700000000000),
    ("2", "Bob", 30, 1700000000000),
    ("3", "Charlie", 35, 1700000000000),
]

df = spark.createDataFrame(data, schema)

table_path = "hdfs://namenode:9000/user/hive/warehouse/hudi_test_cow.db/hudi_cow_table"

hudi_options = {
    "hoodie.table.name": "hudi_cow_table",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "bulk_insert",
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.mode": "hive",
    "hoodie.datasource.hive_sync.database": "hudi_test_cow",
    "hoodie.datasource.hive_sync.table": "hudi_cow_table",
    "hoodie.datasource.hive_sync.partition_fields": "",
    "hoodie.upsert.shuffle.parallelism": "200",
    "hoodie.insert.shuffle.parallelism": "200",
}

print("Writing Hudi COW table...")
df.write.format("hudi").options(**hudi_options).mode("overwrite").save(table_path)

print("Reading Hudi table...")
result_df = spark.read.format("hudi").load(table_path)
result_df.show()

print("Hudi COW Table Test Completed Successfully!")
spark.stop()
