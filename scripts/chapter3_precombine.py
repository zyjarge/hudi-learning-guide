from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import time
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("Chapter3-Precombine") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.jars", "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1.jar") \
    .getOrCreate()
print("=" * 50)
print("第4步：验证 precombine.field 效果")
print("=" * 50)
schema = StructType([
    StructField("student_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("ts", LongType(), True),
])
table_path = "hdfs://namenode:9000/user/hive/warehouse/learning/students_crud"
# 查看当前张三的数据
result = spark.read.format("hudi").load(table_path)
print("【当前状态】张三的分数：")
result.filter("student_id = 'S001'").select("student_id", "name", "score", "ts").show()
# 模拟同一用户同一时刻发来两条数据（分数不同）
# 注意：第一条 ts 小，第二条 ts 大（更新）
old_ts = int(time.time() * 1000) - 1000  # 1秒前
new_ts = int(time.time() * 1000)         # 现在
# 关键：score=60 但 ts 小（旧数据）
#       score=98 且 ts 大（新数据）
conflict_data = [
    ("S001", "张三", 60, old_ts),  # 旧版本
    ("S001", "张三", 98, new_ts),  # 新版本
]
print(f"【写入冲突数据】")
print(f"  版本1: score=60, ts={old_ts} (旧)")
print(f"  版本2: score=98, ts={new_ts} (新)")
df_conflict = spark.createDataFrame(conflict_data, schema)
hudi_options = {
    "hoodie.table.name": "students_crud",
    "hoodie.datasource.write.recordkey.field": "student_id",
    "hoodie.datasource.write.precombine.field": "ts",  # 关键：用 ts 决定
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "upsert",
}
df_conflict.write.format("hudi").options(**hudi_options).mode("append").save(table_path)
print("\n✓ 写入完成！")
# 验证结果
result = spark.read.format("hudi").load(table_path)
print("\n【结果】张三现在的分数：")
result.filter("student_id = 'S001'").select("student_id", "name", "score").show()
print("【结论】")
print("  • 写入了 score=60 和 score=98")
print("  • precombine.field=ts，取 ts 大的")
print("  • 结果保留 score=98（新版本）")
spark.stop()
