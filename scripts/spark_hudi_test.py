"""
Spark + Hudi 测试脚本
测试 Hudi 表的创建、插入、查询和 upsert 操作
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

# HDFS NameNode 地址
HDFS_NAMENODE = "hdfs://namenode:9000"
TABLE_NAME = "hudi_test_table"
TABLE_PATH = f"{HDFS_NAMENODE}/hudi/tables/{TABLE_NAME}"

def create_spark_session():
    """创建 Spark Session with Hudi"""
    return SparkSession.builder \
        .appName("HudiLearningTest") \
        .master("spark://spark-master:7077") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .config("spark.hoodie.metadata.enabled", "true") \
        .config("spark.sql.legacy.pathOptionBehavior.enabled", "true") \
        .config("fs.defaultFS", HDFS_NAMENODE) \
        .getOrCreate()

def create_table(spark):
    """创建 Hudi 表"""
    print("\n" + "="*50)
    print("1. 创建 Hudi 表")
    print("="*50)
    
    table_schema = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("city", StringType(), True),
        StructField("update_time", TimestampType(), True)
    ])
    
    # 生成测试数据
    data = [
        ("001", "Alice", 25, "Beijing", "2024-01-01 10:00:00"),
        ("002", "Bob", 30, "Shanghai", "2024-01-01 10:00:00"),
        ("003", "Charlie", 35, "Guangzhou", "2024-01-01 10:00:00"),
        ("004", "David", 28, "Shenzhen", "2024-01-01 10:00:00"),
    ]
    
    columns = ["id", "name", "age", "city", "update_time"]
    df = spark.createDataFrame(data, columns)
    
    # 写入 Hudi 表 (Copy on Write)
    df.write.format("hudi") \
        .option("hoodie.table.name", TABLE_NAME) \
        .option("hoodie.datasource.write.recordkey.field", "id") \
        .option("hoodie.datasource.write.partitionpath.field", "") \
        .option("hoodie.datasource.write.precombine.field", "update_time") \
        .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE") \
        .option("hoodie.datasource.write.operation", "bulk_insert") \
        .option("hoodie.datasource.write.hive_sync.enable", "false") \
        .mode("overwrite") \
        .save(TABLE_PATH)
    
    print(f"✅ Hudi 表创建成功: {TABLE_PATH}")

def query_table(spark):
    """查询 Hudi 表"""
    print("\n" + "="*50)
    print("2. 查询 Hudi 表")
    print("="*50)
    
    df = spark.read.format("hudi").load(TABLE_PATH)
    df.show()
    print(f"总记录数: {df.count()}")
    return df

def insert_data(spark):
    """插入新数据"""
    print("\n" + "="*50)
    print("3. 插入新数据")
    print("="*50)
    
    new_data = [
        ("005", "Eve", 32, "Hangzhou", "2024-01-02 10:00:00"),
        ("006", "Frank", 40, "Chengdu", "2024-01-02 10:00:00"),
    ]
    
    columns = ["id", "name", "age", "city", "update_time"]
    new_df = spark.createDataFrame(new_data, columns)
    
    new_df.write.format("hudi") \
        .option("hoodie.table.name", TABLE_NAME) \
        .option("hoodie.datasource.write.recordkey.field", "id") \
        .option("hoodie.datasource.write.partitionpath.field", "") \
        .option("hoodie.datasource.write.precombine.field", "update_time") \
        .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE") \
        .option("hoodie.datasource.write.operation", "insert") \
        .option("hoodie.datasource.write.hive_sync.enable", "false") \
        .mode("append") \
        .save(TABLE_PATH)
    
    print("✅ 新数据插入成功")

def update_data(spark):
    """更新数据 (Upsert)"""
    print("\n" + "="*50)
    print("4. 更新数据 (Upsert)")
    print("="*50)
    
    # 更新已存在的数据，同时插入新数据
    update_data = [
        ("003", "Charlie Updated", 36, "Guangzhou", "2024-01-03 10:00:00"),  # 更新
        ("007", "Grace", 29, "Wuhan", "2024-01-03 10:00:00"),  # 新插入
    ]
    
    columns = ["id", "name", "age", "city", "update_time"]
    update_df = spark.createDataFrame(update_data, columns)
    
    update_df.write.format("hudi") \
        .option("hoodie.table.name", TABLE_NAME) \
        .option("hoodie.datasource.write.recordkey.field", "id") \
        .option("hoodie.datasource.write.partitionpath.field", "") \
        .option("hoodie.datasource.write.precombine.field", "update_time") \
        .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE") \
        .option("hoodie.datasource.write.operation", "upsert") \
        .option("hoodie.datasource.write.hive_sync.enable", "false") \
        .mode("append") \
        .save(TABLE_PATH)
    
    print("✅ Upsert 操作成功 (更新 id=003, 插入 id=007)")

def show_timeline(spark):
    """显示 Hudi 时间线"""
    print("\n" + "="*50)
    print("5. Hudi 时间线和元数据")
    print("="*50)
    
    df = spark.read.format("hudi").load(TABLE_PATH)
    
    # 显示 Hudi 元数据列
    print("Hudi 元数据列:")
    df.select(
        "_hoodie_commit_time",
        "_hoodie_commit_seqno", 
        "_hoodie_record_key",
        "_hoodie_partition_path",
        "_hoodie_file_id"
    ).show(10, truncate=False)
    
    # 显示不同版本
    print("按提交时间分组:")
    df.groupBy("_hoodie_commit_time").count().orderBy(desc("_hoodie_commit_time")).show()

def main():
    print("\n" + "#"*60)
    print("# Spark + Hudi 学习测试")
    print("#"*60)
    
    spark = create_spark_session()
    print(f"✅ Spark Session 创建成功")
    print(f"Spark 版本: {spark.version}")
    
    try:
        # 1. 创建表
        create_table(spark)
        
        # 2. 查询表
        query_table(spark)
        
        # 3. 插入数据
        insert_data(spark)
        query_table(spark)
        
        # 4. Upsert 操作
        update_data(spark)
        query_table(spark)
        
        # 5. 显示时间线
        show_timeline(spark)
        
        print("\n" + "#"*60)
        print("# 测试完成!")
        print("#"*60)
        
    except Exception as e:
        print(f"❌ 错误: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
