from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("Chapter1-Step5") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.jars", "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1.jar") \
    .getOrCreate()
print("Step 5: 查看 Hudi 元数据字段")
table_path = "hdfs://namenode:9000/user/hive/warehouse/learning/student_scores"
df = spark.read.format("hudi").load(table_path)
print("\n【问题】_hoodie_record_key 的值是什么？")
print("答案：它应该等于 student_id 的值（因为我们配置了 recordkey.field: student_id）")
df.select("_hoodie_record_key", "student_id", "name").show()
print("\n【问题】_hoodie_partition_path 的值是什么？")
print("答案：为空（因为我们配置了 partitionpath.field: 空字符串）")
df.select("_hoodie_partition_path").distinct().show()
print("\n【问题】_hoodie_commit_time 是什么？")
print("答案：提交时间戳，记录这次写入操作的时间")
df.select("_hoodie_commit_time").distinct().show()
spark.stop()
