from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("Chapter1-Step4") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.jars", "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1.jar") \
    .getOrCreate()
print("Step 4: 读取 Hudi 表")
# 数据保存路径
table_path = "hdfs://namenode:9000/user/hive/warehouse/learning/student_scores"
# 读取 Hudi 表
df = spark.read.format("hudi").load(table_path)
print("读取成功！")
print(f"记录数: {df.count()}")
print("\n所有字段:")
print(df.columns)
print("\n数据内容:")
df.show(truncate=False)
spark.stop()
