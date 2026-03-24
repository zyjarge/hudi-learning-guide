from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import time
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("Chapter3-Upsert") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.jars", "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1.jar") \
    .getOrCreate()
print("=" * 50)
print("第2步：Upsert 操作（更新 + 新增）")
print("=" * 50)
schema = StructType([
    StructField("student_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("ts", LongType(), True),
])
# 操作1: 更新张三的分数（student_id=S001 已存在）
# 操作2: 新增赵六（student_id=S004 不存在）
current_ts = int(time.time() * 1000)
upsert_data = [
    ("S001", "张三", 95, current_ts),   # 更新：分数从85改为95
    ("S004", "赵六", 88, current_ts),   # 新增：新学生
]
df_upsert = spark.createDataFrame(upsert_data, schema)
hudi_options = {
    "hoodie.table.name": "students_crud",
    "hoodie.datasource.write.recordkey.field": "student_id",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "upsert",  # 关键：使用 upsert
}
table_path = "hdfs://namenode:9000/user/hive/warehouse/learning/students_crud"
df_upsert.write.format("hudi").options(**hudi_options).mode("append").save(table_path)
print("✓ Upsert 完成！")
print("\n操作说明：")
print("  • S001 张三：分数 85 → 95（更新）")
print("  • S004 赵六：新增（插入）")
# 验证结果
result = spark.read.format("hudi").load(table_path)
print(f"\n当前数据 ({result.count()} 条):")
result.select("student_id", "name", "score").orderBy("student_id").show()
spark.stop()
