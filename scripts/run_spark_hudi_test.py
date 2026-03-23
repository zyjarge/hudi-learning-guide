#!/usr/bin/env python3
"""
使用 Python subprocess 运行 Spark + Hudi 测试
"""

import subprocess
import sys
import time

# Spark + Hudi 测试代码
HUDI_TEST_CODE = '''
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

println("=" * 60)
println("Spark + Hudi 学习测试")
println("=" * 60)

val spark = SparkSession.builder()
  .appName("HudiLearningTest")
  .master("spark://spark-master:7077")
  .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
  .getOrCreate()

import spark.implicits._

println(s"Spark Version: $${spark.version}")

val tablePath = "hdfs://namenode:9000/hudi/tables/hudi_test"

// [1] 创建 Hudi 表
println("\\n[1] 创建 Hudi 表...")
val data = Seq(
  ("001", "Alice", 25, "Beijing"),
  ("002", "Bob", 30, "Shanghai"),
  ("003", "Charlie", 35, "Guangzhou"),
  ("004", "David", 28, "Shenzhen")
)
val df = data.toDF("id", "name", "age", "city")

df.write.format("hudi")
  .option("hoodie.table.name", "hudi_test")
  .option("hoodie.datasource.write.recordkey.field", "id")
  .option("hoodie.datasource.write.partitionpath.field", "")
  .option("hoodie.datasource.write.precombine.field", "id")
  .option("hoodie.datasource.write.operation", "bulk_insert")
  .mode("overwrite")
  .save(tablePath)

println("表创建成功!")

// [2] 查询表
println("\\n[2] 查询 Hudi 表...")
val result = spark.read.format("hudi").load(tablePath)
result.show()
println(s"记录数: $${result.count()}")

// [3] 插入新数据
println("\\n[3] 插入新数据...")
val newData = Seq(
  ("005", "Eve", 32, "Hangzhou"),
  ("006", "Frank", 40, "Chengdu")
)
val newDf = newData.toDF("id", "name", "age", "city")
newDf.write.format("hudi")
  .option("hoodie.table.name", "hudi_test")
  .option("hoodie.datasource.write.recordkey.field", "id")
  .option("hoodie.datasource.write.partitionpath.field", "")
  .option("hoodie.datasource.write.precombine.field", "id")
  .option("hoodie.datasource.write.operation", "insert")
  .mode("append")
  .save(tablePath)

println("新数据插入成功!")

// [4] Upsert 操作
println("\\n[4] Upsert 操作...")
val upsertData = Seq(
  ("003", "Charlie Updated", 36, "Guangzhou"),
  ("007", "Grace", 29, "Wuhan")
)
val upsertDf = upsertData.toDF("id", "name", "age", "city")
upsertDf.write.format("hudi")
  .option("hoodie.table.name", "hudi_test")
  .option("hoodie.datasource.write.recordkey.field", "id")
  .option("hoodie.datasource.write.partitionpath.field", "")
  .option("hoodie.datasource.write.precombine.field", "id")
  .option("hoodie.datasource.write.operation", "upsert")
  .mode("append")
  .save(tablePath)

println("Upsert 成功!")

// [5] 最终查询
println("\\n[5] 最终查询...")
val finalResult = spark.read.format("hudi").load(tablePath)
finalResult.show()
println(s"最终记录数: $${finalResult.count()}")

// [6] Hudi 元数据
println("\\n[6] Hudi 元数据...")
finalResult.select("_hoodie_commit_time", "_hoodie_record_key", "id", "name").show(10)

spark.stop()
println("\\n" + "=" * 60)
println("测试完成!")
println("=" * 60)

:quit
'''

def run_spark_hudi_test():
    print("==============================================")
    print("  Spark + Hudi 学习测试")
    print("==============================================")
    print()

    # 将代码写入临时文件
    with open('/tmp/hudi_spark_test.scala', 'w') as f:
        f.write(HUDI_TEST_CODE)

    print("[1] 测试代码已准备")
    print("[2] 开始运行 Spark + Hudi 测试...")
    print()

    # 构建 spark-shell 命令
    cmd = [
        'docker', 'exec', 'hudi-spark-master',
        '/opt/spark/bin/spark-shell',
        '--master', 'spark://spark-master:7077',
        '--conf', 'spark.serializer=org.apache.spark.serializer.KryoSerializer',
        '--conf', 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension',
        '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog',
        '--jars', '/opt/spark/jars/hudi-spark3.3-bundle_2.12-1.1.1.jar',
        '-I', '/tmp/hudi_spark_test.scala'
    ]

    print(f"执行命令: {' '.join(cmd)}")
    print()

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300
        )

        print("=== STDOUT ===")
        print(result.stdout)
        print()
        print("=== STDERR ===")
        print(result.stderr)
        print()
        print(f"返回码: {result.returncode}")

    except subprocess.TimeoutExpired:
        print("超时！测试运行时间超过 5 分钟")
    except Exception as e:
        print(f"错误: {e}")

if __name__ == "__main__":
    run_spark_hudi_test()
