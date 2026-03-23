from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("VerifyIncremental") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.jars", "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1.jar") \
    .getOrCreate()

mor_path = "hdfs://namenode:9000/user/hive/warehouse/hudi_test_mor.db/hudi_mor_table"

commits = [r._hoodie_commit_time for r in spark.read.format("hudi").load(mor_path).select("_hoodie_commit_time").distinct().orderBy("_hoodie_commit_time").collect()]
print(f"Commits: {commits}")

print("\n=== Full Snapshot ===")
full = spark.read.format("hudi").load(mor_path)
print(f"Total records: {full.count()}")
full.select("id", "name", "score", "_hoodie_commit_time").orderBy("id").show(10, truncate=False)

print("\n=== Incremental from FIRST commit (all records) ===")
inc1 = spark.read.format("hudi").option("hoodie.datasource.query.type", "incremental").option("hoodie.datasource.read.begin.instanttime", commits[0]).load(mor_path)
print(f"Records since {commits[0]}: {inc1.count()}")
inc1.select("id", "name", "score", "_hoodie_commit_time").orderBy("id", "_hoodie_commit_time").show(10, truncate=False)

print("\n=== Incremental from LAST commit (only new records) ===")
if len(commits) > 1:
    inc2 = spark.read.format("hudi").option("hoodie.datasource.query.type", "incremental").option("hoodie.datasource.read.begin.instanttime", commits[1]).load(mor_path)
    print(f"Records since {commits[1]}: {inc2.count()}")
    inc2.select("id", "name", "score", "_hoodie_commit_time").orderBy("id").show(10, truncate=False)

spark.stop()
