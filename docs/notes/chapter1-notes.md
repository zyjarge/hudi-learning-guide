# 第一章：Hudi 基础概念 - 学习笔记

## 实验环境

| 组件 | 版本 |
|------|------|
| Spark | 3.5.8 |
| Hudi | 1.1.1 |
| Scala | 2.12 |
| HDFS | Hadoop |

---

## 知识点总结

### 1. Hudi 核心配置参数

| 参数 | 值 | 说明 |
|------|-----|------|
| `hoodie.table.name` | student_scores | 表的逻辑名称 |
| `hoodie.datasource.write.recordkey.field` | student_id | 主键字段，唯一标识一条记录 |
| `hoodie.datasource.write.precombine.field` | score | 排序字段，冲突时取最大值 |
| `hoodie.datasource.write.partitionpath.field` | 空 | 分区字段，空表示不分区 |
| `hoodie.datasource.write.table.type` | COPY_ON_WRITE | COW 表类型 |
| `hoodie.datasource.write.operation` | bulk_insert | 批量插入操作 |

### 2. Hudi 自动添加的元数据字段

| 字段名 | 值示例 | 说明 |
|--------|--------|------|
| `_hoodie_commit_time` | 20260323154715499 | 提交时间戳 |
| `_hoodie_commit_seqno` | 20260323154715499_0_0 | 提交序列号 |
| `_hoodie_record_key` | S001 | 记录主键（等于 recordkey.field 的值） |
| `_hoodie_partition_path` | 空 | 分区路径（等于 partitionpath.field 的值） |
| `_hoodie_file_name` | *.parquet | 数据所在的文件名 |

### 3. HDFS 文件结构

```
student_scores/
├── .hoodie/                      # Hudi 元数据目录
│   ├── hoodie.properties         # 表配置信息
│   └── timeline/                 # 时间线
│       ├── *.commit.requested    # 提交请求
│       ├── *.inflight            # 提交进行中
│       ├── *.commit              # 提交完成
│       └── history/              # 历史归档
│
├── .hoodie_partition_metadata    # 分区元数据
│
└── *.parquet                     # 数据文件（2个）
```

### 4. Timeline 提交状态

```
requested → inflight → commit
 (请求)    (进行中)   (完成)
```

---

## 实操步骤回顾

### Step 1：验证环境
- 连接 Spark 集群
- 验证 Spark 版本：3.5.8

### Step 2：准备数据
- 定义 Schema（student_id, name, score）
- 创建 DataFrame（3条学生数据）

### Step 3：写入 Hudi 表
- 配置 Hudi 参数
- 使用 `df.write.format("hudi").save()` 写入

### Step 4：读取 Hudi 表
- 使用 `spark.read.format("hudi").load()` 读取
- 查看所有字段（包括 `_hoodie_*`）

### Step 5：理解元数据字段
- `_hoodie_record_key` = `student_id`（因为配置了 recordkey.field）
- `_hoodie_partition_path` = 空（因为没配置分区）

### Step 6：查看 HDFS 文件结构
- `.hoodie/` 目录包含表元数据
- Parquet 文件存储实际数据

### Step 7：查看 Timeline
- 每次写入生成 commit 记录
- Timeline 是时间旅行的基础

---

## 关键理解

### 问题 1：_hoodie_record_key 和 student_id 的关系
**答：** 值相同，因为配置了 `recordkey.field: student_id`，表示用 student_id 作为 Hudi 的主键。

### 问题 2：为什么 _hoodie_partition_path 是空的？
**答：** 因为配置了 `partitionpath.field: 空字符串`，表示这个表没有使用分区。

### 问题 3：为什么有 2 个 Parquet 文件？
**答：** Spark 并行写入时会生成多个文件，3条数据分布在2个文件中。

---

## 代码模板

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# 1. 创建 SparkSession
spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("HudiExample") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.jars", "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1.jar") \
    .getOrCreate()

# 2. 准备数据
df = spark.createDataFrame(data, schema)

# 3. 配置 Hudi 参数
hudi_options = {
    "hoodie.table.name": "my_table",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "bulk_insert",
}

# 4. 写入 Hudi
df.write.format("hudi").options(**hudi_options).mode("overwrite").save(path)

# 5. 读取 Hudi
result = spark.read.format("hudi").load(path)
result.show()

spark.stop()
```

---

## 实验数据

| student_id | name | score |
|------------|------|-------|
| S001 | 张三 | 85 |
| S002 | 李四 | 92 |
| S003 | 王五 | 78 |

**表路径：** `hdfs://namenode:9000/user/hive/warehouse/learning/student_scores`

**提交时间：** 20260323154715499

---

> 笔记创建时间：2026-03-23
> 章节：第一章 - Hudi 基础概念
> 下一步：章节测验
