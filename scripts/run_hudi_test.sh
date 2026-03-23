#!/bin/bash
# Spark + Hudi Test Script

echo "=============================================="
echo "  Spark + Hudi 学习测试"
echo "=============================================="
echo ""

# Configuration
SPARK_MASTER="spark://spark-master:7077"
HDFS_NAMENODE="hdfs://namenode:9000"
TABLE_PATH="${HDFS_NAMENODE}/hudi/tables/hudi_test"

echo "[1] 创建 Hudi 测试表..."
echo "    路径: ${TABLE_PATH}"

# Create test data and write to Hudi
docker exec hudi-spark-master bash -c '
cat > /tmp/hudi_test.scala << "ENDSCALA"
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object HudiTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HudiLearningTest")
      .master("spark://spark-master:7077")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .getOrCreate()
    
    import spark.implicits._
    
    println("=" * 60)
    println("Spark + Hudi 学习测试")
    println("=" * 60)
    println(s"Spark Version: \${spark.version}")
    
    // 创建测试数据
    println("\n[1] 创建 Hudi 表...")
    val data = Seq(
      ("001", "Alice", 25, "Beijing"),
      ("002", "Bob", 30, "Shanghai"),
      ("003", "Charlie", 35, "Guangzhou"),
      ("004", "David", 28, "Shenzhen")
    )
    val df = data.toDF("id", "name", "age", "city")
    
    val tablePath = "hdfs://namenode:9000/hudi/tables/hudi_test"
    
    df.write.format("hudi")
      .option("hoodie.table.name", "hudi_test")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.partitionpath.field", "")
      .option("hoodie.datasource.write.precombine.field", "id")
      .option("hoodie.datasource.write.operation", "bulk_insert")
      .option("hoodie.datasource.write.hive_sync.enable", "false")
      .mode("overwrite")
      .save(tablePath)
    
    println("表创建成功!")
    
    // 查询表
    println("\n[2] 查询 Hudi 表...")
    val result = spark.read.format("hudi").load(tablePath)
    result.show()
    println(s"记录数: \${result.count()}")
    
    // 插入新数据
    println("\n[3] 插入新数据...")
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
      .option("hoodie.datasource.write.hive_sync.enable", "false")
      .mode("append")
      .save(tablePath)
    
    println("新数据插入成功!")
    
    // Upsert 操作
    println("\n[4] Upsert 操作 (更新 id=003, 插入 id=007)...")
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
      .option("hoodie.datasource.write.hive_sync.enable", "false")
      .mode("append")
      .save(tablePath)
    
    println("Upsert 成功!")
    
    // 最终查询
    println("\n[5] 最终查询...")
    val finalResult = spark.read.format("hudi").load(tablePath)
    finalResult.show()
    println(s"最终记录数: \${finalResult.count()}")
    
    // Hudi 元数据
    println("\n[6] Hudi 元数据 (提交时间线)...")
    finalResult.select("_hoodie_commit_time", "_hoodie_record_key", "id", "name", "age").show(10)
    
    spark.stop()
    println("\n" + "=" * 60)
    println("测试完成!")
    println("=" * 60)
  }
}
ENDSCALA

# 检查文件
echo "Scala 脚本已创建"
ls -la /tmp/hudi_test.scala

# 由于无法直接编译 Scala，我们使用 spark-shell 的 batch 模式
echo ""
echo "使用 spark-shell 运行测试..."
'

echo ""
echo "[完成] 测试脚本已准备就绪"
echo ""
echo "请在容器中手动运行以下命令:"
echo "-------------------------------------------"
echo "docker exec -it hudi-spark-master bash"
echo ""
echo "然后粘贴以下 Scala 代码:"
echo "-------------------------------------------"
