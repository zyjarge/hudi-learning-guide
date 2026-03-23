from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import time
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("Chapter2-COW") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.jars", "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1.jar") \
    .getOrCreate()
print("=" * 50)
print("创建 COW 表")
print("=" * 50)
# 准备数据
schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("product", StringType(), True),
    StructField("price", IntegerType(), True),
])
data = [
    ("ORD001", "iPhone", 6999),
    ("ORD002", "MacBook", 12999),
    ("ORD003", "AirPods", 1299),
]
df = spark.createDataFrame(data, schema)
# COW 表配置
cow_options = {
    "hoodie.table.name": "orders_cow",
    "hoodie.datasource.write.recordkey.field": "order_id",
    "hoodie.datasource.write.precombine.field": "price",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",  # 关键配置
    "hoodie.datasource.write.operation": "bulk_insert",
}
cow_path = "hdfs://namenode:9000/user/hive/warehouse/learning/orders_cow"
# 写入 COW 表
start_time = time.time()
df.write.format("hudi").options(**cow_options).mode("overwrite").save(cow_path)
cow_write_time = time.time() - start_time
print(f"✓ COW 表写入完成，耗时: {cow_write_time:.2f}秒")
spark.stop()
