from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("Chapter3-Summary") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.jars", "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1.jar") \
    .getOrCreate()
print("=" * 60)
print("第三章 表操作（增删改查） 实操总结")
print("=" * 60)
table_path = "hdfs://namenode:9000/user/hive/warehouse/learning/students_crud"
df = spark.read.format("hudi").load(table_path)
print("\n【最终数据】")
print(f"记录数: {df.count()}")
df.select("student_id", "name", "score").orderBy("student_id").show()
print("=" * 60)
print("本章操作回顾：")
print("=" * 60)
print("""
【Step 1】bulk_insert - 批量插入
  • 插入 3 条初始数据：张三(85)、李四(92)、王五(78)
  • 配置: operation = "bulk_insert"
【Step 2】upsert - 更新+新增
  • 更新: 张三 85 → 95
  • 新增: 赵六(88)
  • 配置: operation = "upsert"
【Step 3】delete - 删除
  • 删除: 王五
  • 配置: operation = "delete"
【Step 4】precombine.field 验证
  • 同一条记录写入两个版本（score=60, score=98）
  • precombine.field=ts，保留 ts 大的（score=98）
  • 技巧：负数法可实现"取最小"
""")
print("=" * 60)
print("操作类型总结：")
print("=" * 60)
print("""
┌──────────────┬────────────────────┬──────────────────────┐
│ 操作类型      │ operation 配置值    │ 说明                 │
├──────────────┼────────────────────┼──────────────────────┤
│ 首次批量插入  │ bulk_insert        │ 不检查重复，最快      │
│ 更新/新增    │ upsert             │ 有则更新，无则插入    │
│ 删除         │ delete             │ 根据主键删除          │
└──────────────┴────────────────────┴──────────────────────┘
""")
print("实操完成！准备进入测验阶段。")
spark.stop()
