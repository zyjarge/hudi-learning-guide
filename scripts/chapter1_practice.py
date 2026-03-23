"""
第一章：Hudi 基础概念 - 实操练习
目标：理解 Hudi 表结构、元数据字段、Timeline
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("Chapter1-Practice") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.jars", "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1.jar") \
    .getOrCreate()

table_path = "hdfs://namenode:9000/user/hive/warehouse/learning/chapter1_practice"

# ========================================
# 练习 1：创建表
# ========================================
print("=" * 50)
print("练习 1：创建 Hudi 表")
print("=" * 50)

schema = StructType([
    StructField("student_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("subject", StringType(), True),
    StructField("score", IntegerType(), True),
])

data = [
    ("S001", "小明", "数学", 95),
    ("S002", "小红", "数学", 88),
    ("S003", "小刚", "数学", 72),
]

df = spark.createDataFrame(data, schema)

hudi_options = {
    "hoodie.table.name": "chapter1_practice",
    "hoodie.datasource.write.recordkey.field": "student_id",
    "hoodie.datasource.write.precombine.field": "score",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "bulk_insert",
}

df.write.format("hudi").options(**hudi_options).mode("overwrite").save(table_path)
print("✓ 表创建成功")

# ========================================
# 练习 2：读取并查看数据
# ========================================
print("\n" + "=" * 50)
print("练习 2：读取 Hudi 表数据")
print("=" * 50)

result_df = spark.read.format("hudi").load(table_path)
result_df.show(truncate=False)

# ========================================
# 练习 3：查看 Hudi 元数据字段
# ========================================
print("\n" + "=" * 50)
print("练习 3：查看 Hudi 元数据字段")
print("=" * 50)

print("【问题 3.1】查看 _hoodie_record_key 字段:")
result_df.select("_hoodie_record_key", "student_id", "name").show()

print("【问题 3.2】_hoodie_record_key 和 student_id 的值是否一致？")
print("答：应该一致，因为 recordkey.field 配置的是 student_id")

# ========================================
# 练习 4：查看提交历史 (Timeline)
# ========================================
print("\n" + "=" * 50)
print("练习 4：查看提交历史 Timeline")
print("=" * 50)

commits = result_df.select("_hoodie_commit_time").distinct()
print(f"提交次数: {commits.count()}")
commits.show()

# 获取提交时间
commit_time = commits.first()[0]
print(f"提交时间戳: {commit_time}")
print(f"格式化: {commit_time[:4]}-{commit_time[4:6]}-{commit_time[6:8]}")

# ========================================
# 练习 5：查看 HDFS 文件结构
# ========================================
print("\n" + "=" * 50)
print("练习 5：查看 HDFS 上的文件结构")
print("=" * 50)

import subprocess
result = subprocess.run(
    ["docker", "exec", "hudi-hdfs-namenode", "hdfs", "dfs", "-ls", "-R", table_path],
    capture_output=True, text=True
)
print("文件列表:")
for line in result.stdout.strip().split('\n'):
    if line and '.hoodie' not in line:
        print(line)

# ========================================
# 练习 6：统计字段数量
# ========================================
print("\n" + "=" * 50)
print("练习 6：统计字段数量")
print("=" * 50)

all_columns = result_df.columns
print(f"所有字段: {all_columns}")
print(f"总字段数: {len(all_columns)}")

hoodie_fields = [c for c in all_columns if c.startswith("_hoodie")]
business_fields = [c for c in all_columns if not c.startswith("_hoodie")]

print(f"\nHudi 元数据字段 ({len(hoodie_fields)} 个): {hoodie_fields}")
print(f"业务字段 ({len(business_fields)} 个): {business_fields}")

# ========================================
# 总结
# ========================================
print("\n" + "=" * 50)
print("实操完成！你应该理解了:")
print("=" * 50)
print("1. 如何创建 Hudi 表")
print("2. _hoodie_record_key 与主键的关系")
print("3. Timeline 提交历史的概念")
print("4. Hudi 在 HDFS 上的文件结构")
print("5. Hudi 元数据字段 vs 业务字段")

spark.stop()
