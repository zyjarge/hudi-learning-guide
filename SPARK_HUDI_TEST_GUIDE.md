# Spark + Hudi 测试指南

## 当前环境状态

### 已验证的服务
- ✅ Zookeeper: 运行正常
- ✅ HDFS: 运行正常，可以创建目录
- ✅ Kafka: 运行正常，消息发送/消费正常
- ✅ Spark Master/Worker: 运行正常
- ✅ Hive Metastore: 运行正常

### Hudi 测试文件
已创建测试脚本: `/root/workspace/datalake/hudi-docker/scripts/spark_hudi_test.py`

## 手动测试步骤

### 1. 进入 Spark Master 容器
```bash
docker exec -it hudi-spark-master bash
```

### 2. 启动 Spark Shell
```bash
/opt/spark/bin/spark-shell \
  --master spark://spark-master:7077 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
  --jars /opt/spark/jars/hudi-spark3.3-bundle_2.12-1.1.1.jar
```

### 3. 在 Spark Shell 中执行测试
```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import spark.implicits._

val HDFS_NAMENODE = "hdfs://namenode:9000"
val TABLE_NAME = "hudi_test_table"
val TABLE_PATH = s"$HDFS_NAMENODE/hudi/tables/$TABLE_NAME"

// 创建测试数据
val data = Seq(
  ("001", "Alice", 25, "Beijing"),
  ("002", "Bob", 30, "Shanghai"),
  ("003", "Charlie", 35, "Guangzhou")
)
val df = data.toDF("id", "name", "age", "city")

// 写入 Hudi 表
df.write.format("hudi")
  .option("hoodie.table.name", TABLE_NAME)
  .option("hoodie.datasource.write.recordkey.field", "id")
  .option("hoodie.datasource.write.partitionpath.field", "")
  .option("hoodie.datasource.write.operation", "bulk_insert")
  .mode("overwrite")
  .save(TABLE_PATH)

// 查询 Hudi 表
val result = spark.read.format("hudi").load(TABLE_PATH)
result.show()

// Upsert 测试
val updateData = Seq(
  ("003", "Charlie Updated", 36, "Guangzhou"),
  ("004", "David", 28, "Shenzhen")
)
val updateDf = updateData.toDF("id", "name", "age", "city")
updateDf.write.format("hudi")
  .option("hoodie.table.name", TABLE_NAME)
  .option("hoodie.datasource.write.recordkey.field", "id")
  .option("hoodie.datasource.write.operation", "upsert")
  .mode("append")
  .save(TABLE_PATH)

// 查询更新后的表
val finalResult = spark.read.format("hudi").load(TABLE_PATH)
finalResult.show()

// 显示 Hudi 元数据
finalResult.select("_hoodie_commit_time", "id", "name").show()
```

## 备选方案: 使用 spark-submit

### 创建测试脚本
在容器中创建 `/tmp/HudiTest.scala`:
```scala
import org.apache.spark.sql.SparkSession

object HudiTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HudiTest")
      .master("spark://spark-master:7077")
      .getOrCreate()
    
    import spark.implicits._
    val df = Seq(("1","Alice"),("2","Bob")).toDF("id","name")
    
    df.write.format("hudi")
      .option("hoodie.table.name", "test")
      .option("hoodie.datasource.write.recordkey.field", "id")
      .option("hoodie.datasource.write.operation", "bulk_insert")
      .mode("overwrite")
      .save("hdfs://namenode:9000/hudi/test")
    
    spark.read.format("hudi").load("hdfs://namenode:9000/hudi/test").show()
    spark.stop()
  }
}
```

### 编译并运行
由于容器中没有 scalac，需要使用 spark-shell 或其他方式编译。

## 常见问题

### 1. Hudi JAR 未找到
确保 Hudi JAR 在 Spark 的 classpath 中:
```bash
ls -la /opt/spark/jars/hudi-spark3.3-bundle_2.12-1.1.1.jar
```

### 2. Spark 连接失败
检查 Spark Master 是否正常运行:
```bash
curl http://localhost:8082
```

### 3. HDFS 连接失败
检查 HDFS NameNode 是否正常运行:
```bash
docker exec hudi-hdfs-namenode hdfs dfs -ls /
```
