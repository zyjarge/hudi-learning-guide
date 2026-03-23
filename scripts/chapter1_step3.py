from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("Chapter1-Step3") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.jars", "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1.jar") \
    .getOrCreate()
print("Step 3: 写入 Hudi 表")
# 准备数据
schema = StructType([
    StructField("student_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("score", IntegerType(), True),
])
students = [
    ("S001", "张三", 85),
    ("S002", "李四", 92),
    ("S003", "王五", 78),
]
df = spark.createDataFrame(students, schema)
# ==================== Hudi 配置 ====================
# 请仔细阅读每个配置的说明！
hudi_options = {
    # 【知识点 1】表名
    "hoodie.table.name": "student_scores",
    
    # 【知识点 2】主键字段 - 唯一标识每条记录
    "hoodie.datasource.write.recordkey.field": "student_id",
    
    # 【知识点 3】排序字段 - 冲突时取分数最大的
    "hoodie.datasource.write.precombine.field": "score",
    
    # 【知识点 4】分区字段 - 空字符串表示不分区
    "hoodie.datasource.write.partitionpath.field": "",
    
    # 【知识点 5】表类型 - COW（写时复制）
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    
    # 【知识点 6】操作类型 - 批量插入
    "hoodie.datasource.write.operation": "bulk_insert",
}
# 数据保存路径
table_path = "hdfs://namenode:9000/user/hive/warehouse/learning/student_scores"
# 写入 Hudi 表
df.write.format("hudi").options(**hudi_options).mode("overwrite").save(table_path)
print("写入成功！")
print(f"表路径: {table_path}")
spark.stop()
