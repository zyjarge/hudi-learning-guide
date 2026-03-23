from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import time
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("Chapter2-Upsert") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.jars", "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1.jar") \
    .getOrCreate()
print("=" * 50)
print("MOR 表 Upsert 操作")
print("=" * 50)
schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("product", StringType(), True),
    StructField("price", IntegerType(), True),
])
# 更新 ORD001 的价格，新增 ORD004
update_data = [
    ("ORD001", "iPhone", 5999),  # 更新：价格变化
    ("ORD004", "iPad", 3999),    # 新增
]
df_update = spark.createDataFrame(update_data, schema)
# MOR 表配置（使用 upsert 操作）
mor_options = {
    "hoodie.table.name": "orders_mor",
    "hoodie.datasource.write.recordkey.field": "order_id",
    "hoodie.datasource.write.precombine.field": "price",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.table.type": "MERGE_ON_READ",
    "hoodie.datasource.write.operation": "upsert",  # 关键：使用 upsert
}
mor_path = "hdfs://namenode:9000/user/hive/warehouse/learning/orders_mor"
# 执行 Upsert
df_update.write.format("hudi").options(**mor_options).mode("append").save(mor_path)
print("✓ Upsert 完成！")
print("\n现在去查看 MOR 表的文件结构，应该能看到 log 文件了")
spark.stop()
