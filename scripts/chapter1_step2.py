from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("Chapter1-Step2") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.jars", "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1.jar") \
    .getOrCreate()
print("Step 2: 创建学生数据")
# 定义数据结构（Schema）
schema = StructType([
    StructField("student_id", StringType(), False),  # 学生ID，不能为空
    StructField("name", StringType(), True),         # 姓名
    StructField("score", IntegerType(), True),        # 分数
])
# 准备数据
students = [
    ("S001", "张三", 85),
    ("S002", "李四", 92),
    ("S003", "王五", 78),
]
# 创建 DataFrame
df = spark.createDataFrame(students, schema)
print("数据准备完成，共 {} 条记录".format(df.count()))
df.show()
spark.stop()
