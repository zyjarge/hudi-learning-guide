from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("Chapter1-Summary") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.jars", "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1.jar") \
    .getOrCreate()
print("=" * 60)
print("第一章实操总结")
print("=" * 60)
table_path = "hdfs://namenode:9000/user/hive/warehouse/learning/student_scores"
df = spark.read.format("hudi").load(table_path)
print("\n【1】数据内容：")
df.select("student_id", "name", "score").show()
print("【2】Hudi 元数据字段：")
print(f"   _hoodie_record_key: {df.select('_hoodie_record_key').first()[0]}")
print(f"   _hoodie_partition_path: '{df.select('_hoodie_partition_path').first()[0]}'")
print(f"   _hoodie_commit_time: {df.select('_hoodie_commit_time').first()[0]}")
print("\n【3】提交时间戳解读：")
commit_time = df.select('_hoodie_commit_time').first()[0]
print(f"   原始值: {commit_time}")
print(f"   格式化: {commit_time[:4]}-{commit_time[4:6]}-{commit_time[6:8]} {commit_time[8:10]}:{commit_time[10:12]}:{commit_time[12:14]}")
print("\n" + "=" * 60)
print("实操完成！准备进入测验阶段。")
print("=" * 60)
spark.stop()
