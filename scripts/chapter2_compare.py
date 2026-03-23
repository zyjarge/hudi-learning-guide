from pyspark.sql import SparkSession
import time
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("Chapter2-Compare") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.jars", "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1.jar") \
    .getOrCreate()
print("=" * 50)
print("COW vs MOR 读取对比")
print("=" * 50)
cow_path = "hdfs://namenode:9000/user/hive/warehouse/learning/orders_cow"
mor_path = "hdfs://namenode:9000/user/hive/warehouse/learning/orders_mor"
# 读取 COW 表
print("\n【COW 表读取】")
start = time.time()
cow_df = spark.read.format("hudi").load(cow_path)
cow_count = cow_df.count()
cow_time = time.time() - start
print(f"记录数: {cow_count}")
print(f"耗时: {cow_time:.3f}秒")
cow_df.select("order_id", "product", "price").show()
# 读取 MOR 表
print("\n【MOR 表读取】")
start = time.time()
mor_df = spark.read.format("hudi").load(mor_path)
mor_count = mor_df.count()
mor_time = time.time() - start
print(f"记录数: {mor_count}")
print(f"耗时: {mor_time:.3f}秒")
mor_df.select("order_id", "product", "price").show()
# 对比结果
print("=" * 50)
print("读取性能对比")
print("=" * 50)
print(f"COW 读取: {cow_time:.3f}秒")
print(f"MOR 读取: {mor_time:.3f}秒")
if cow_time < mor_time:
    print("结论: COW 读取更快 ✓")
else:
    print("结论: MOR 读取更快 (小数据量差异不明显)")
spark.stop()
