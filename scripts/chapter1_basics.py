"""
第一章：Hudi 基础概念 - 动手练习
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("Chapter1-HudiBasics") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.jars", "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1.jar") \
    .getOrCreate()

print("=" * 60)
print("第一章：Hudi 基础概念")
print("=" * 60)

# 创建一个简单的表
schema = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("score", IntegerType(), True),
])

data = [
    ("1", "张三", 85),
    ("2", "李四", 92),
    ("3", "王五", 78),
]

df = spark.createDataFrame(data, schema)

table_path = "hdfs://namenode:9000/user/hive/warehouse/learning/chapter1_basics"

hudi_options = {
    "hoodie.table.name": "chapter1_basics",
    "hoodie.datasource.write.recordkey.field": "id",           # 主键
    "hoodie.datasource.write.precombine.field": "score",        # 排序字段
    "hoodie.datasource.write.partitionpath.field": "",          # 不分区
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",      # COW 表
    "hoodie.datasource.write.operation": "bulk_insert",         # 批量插入
}

print("\n【练习 1.1】创建第一个 Hudi 表")
print("-" * 40)
df.write.format("hudi").options(**hudi_options).mode("overwrite").save(table_path)
print("✓ 表创建完成")

print("\n【练习 1.2】读取 Hudi 表")
print("-" * 40)
result = spark.read.format("hudi").load(table_path)
result.show()

print("\n【练习 1.3】查看 Hudi 元数据字段")
print("-" * 40)
print("Hudi 自动添加的元数据字段:")
result.select(
    "_hoodie_commit_time",     # 提交时间
    "_hoodie_record_key",      # 记录主键
    "_hoodie_partition_path",  # 分区路径
    "_hoodie_file_name"        # 文件名
).show(truncate=False)

print("\n【练习 1.4】查看提交历史")
print("-" * 40)
commits = result.select("_hoodie_commit_time").distinct().orderBy("_hoodie_commit_time")
print(f"共有 {commits.count()} 次提交:")
commits.show()

print("\n" + "=" * 60)
print("第一章 练习完成！")
print("你应该已经了解:")
print("1. Hudi 表的基本结构")
print("2. Hudi 自动添加的元数据字段")
print("3. Timeline 的概念 (提交历史)")
print("=" * 60)

spark.stop()
