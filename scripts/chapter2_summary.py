from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("Chapter2-Summary") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.jars", "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1.jar") \
    .getOrCreate()
print("=" * 60)
print("第二章 COW vs MOR 实操总结")
print("=" * 60)
# COW 表数据
cow_path = "hdfs://namenode:9000/user/hive/warehouse/learning/orders_cow"
cow_df = spark.read.format("hudi").load(cow_path)
print("\n【COW 表 - 初始数据】")
print(f"记录数: {cow_df.count()}")
cow_df.select("order_id", "product", "price").orderBy("order_id").show()
# MOR 表数据（包含更新）
mor_path = "hdfs://namenode:9000/user/hive/warehouse/learning/orders_mor"
mor_df = spark.read.format("hudi").load(mor_path)
print("【MOR 表 - 更新后数据】")
print(f"记录数: {mor_df.count()}")
mor_df.select("order_id", "product", "price").orderBy("order_id").show()
print("=" * 60)
print("实验观察：")
print("=" * 60)
print("""
【COW 表】
  • 配置: table.type = COPY_ON_WRITE
  • 写入: 20.89秒
  • 文件: 只有 .parquet 文件
  • 读取: 13.595秒
【MOR 表】
  • 配置: table.type = MERGE_ON_READ  
  • 写入: 18.56秒
  • 文件: .parquet 文件（小数据量直接写Parquet）
  • 读取: 1.045秒
  • Upsert: ORD001价格更新，ORD004新增
【核心区别】
  • COW: 更新时复制整个文件重写
  • MOR: 更新时追加写入（大数据量时生成.log文件）
  
【结论】
  小数据量场景下差异不明显
  大数据量、频繁更新场景下MOR优势明显
""")
spark.stop()
