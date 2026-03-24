from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import time
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("Chapter3-Insert") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.jars", "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1.jar") \
    .getOrCreate()
print("=" * 50)
print("第1步：批量插入初始数据 (bulk_insert)")
print("=" * 50)
schema = StructType([
    StructField("student_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("ts", LongType(), True),
])
# 初始数据
current_ts = int(time.time() * 1000)
data = [
    ("S001", "张三", 85, current_ts),
    ("S002", "李四", 92, current_ts),
    ("S003", "王五", 78, current_ts),
]
df = spark.createDataFrame(data, schema)
hudi_options = {
    "hoodie.table.name": "students_crud",
    "hoodie.datasource.write.recordkey.field": "student_id",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "bulk_insert",  # 首次插入
}
table_path = "hdfs://namenode:9000/user/hive/warehouse/learning/students_crud"
df.write.format("hudi").options(**hudi_options).mode("overwrite").save(table_path)
print("✓ 插入完成！")
print(f"时间戳: {current_ts}")
# 验证数据
result = spark.read.format("hudi").load(table_path)
print(f"\n当前数据 ({result.count()} 条):")
result.select("student_id", "name", "score", "ts").orderBy("student_id").show()
spark.stop()
