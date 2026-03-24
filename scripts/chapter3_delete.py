from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import time
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("Chapter3-Delete") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.jars", "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1.jar") \
    .getOrCreate()
print("=" * 50)
print("第3步：Delete 操作（删除王五 S003）")
print("=" * 50)
schema = StructType([
    StructField("student_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("ts", LongType(), True),
])
# 要删除的记录
current_ts = int(time.time() * 1000)
delete_data = [
    ("S003", "王五", 78, current_ts),
]
df_delete = spark.createDataFrame(delete_data, schema)
table_path = "hdfs://namenode:9000/user/hive/warehouse/learning/students_crud"
# 删除操作
df_delete.write.format("hudi") \
    .option("hoodie.table.name", "students_crud") \
    .option("hoodie.datasource.write.recordkey.field", "student_id") \
    .option("hoodie.datasource.write.precombine.field", "ts") \
    .option("hoodie.datasource.write.partitionpath.field", "") \
    .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE") \
    .option("hoodie.datasource.write.operation", "delete") \
    .mode("append") \
    .save(table_path)
print("✓ 删除完成！")
# 验证结果
result = spark.read.format("hudi").load(table_path)
print(f"\n当前数据 ({result.count()} 条):")
result.select("student_id", "name", "score").orderBy("student_id").show()
print("已删除的记录：王五 (S003)")
spark.stop()
