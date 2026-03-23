# Apache Hudi 学习指南

> 本指南基于 Spark 3.5.8 + Hudi 1.1.1 实战环境

---

## 目录

- [第一章：Hudi 基础概念](#第一章hudi-基础概念)
- [第二章：表类型详解（COW vs MOR）](#第二章表类型详解cow-vs-mor)
- [第三章：表操作（增删改查）](#第三章表操作增删改查)
- [第四章：时间旅行](#第四章时间旅行)
- [第五章：增量查询](#第五章增量查询)
- [第六章：分区策略](#第六章分区策略)
- [第七章：主键设计](#第七章主键设计)
- [综合实战项目](#综合实战项目)

---

## 第一章：Hudi 基础概念

### 1.1 什么是 Hudi？

Hudi（**H**adoop **U**psert **D**eletion and **I**ncremental）是一个**数据湖存储框架**，由 Uber 开发，用于解决大数据场景下的：

- 数据写入慢
- 查询不支持增量
- 数据质量难以保证

### 1.2 核心概念

```
┌─────────────────────────────────────────────────────────────┐
│                        Hudi 表结构                           │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐                                            │
│  │  Timeline   │  时间线：记录所有操作的历史                   │
│  └─────────────┘                                            │
│        │                                                    │
│        ▼                                                    │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                    Hoodie Table                      │    │
│  │                                                      │    │
│  │   ┌──────────┐   ┌──────────┐   ┌──────────┐        │    │
│  │   │ Commit 1 │   │ Commit 2 │   │ Commit 3 │        │    │
│  │   └──────────┘   └──────────┘   └──────────┘        │    │
│  │                                                      │    │
│  │   每个 Commit 包含：                                   │    │
│  │   - Instant Time (时间戳)                             │    │
│  │   - Action Type (commit/deltacommit)                 │    │
│  │   - Metadata (元数据)                                 │    │
│  └─────────────────────────────────────────────────────┘    │
│        │                                                    │
│        ▼                                                    │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                  File Groups                         │    │
│  │                                                      │    │
│  │   Group 1: file1.parquet ← log1, log2 (MOR)         │    │
│  │   Group 2: file2.parquet                             │    │
│  │   Group 3: file3.parquet                             │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

### 1.3 Hudi 表的元数据字段

| 字段名 | 说明 | 示例 |
|--------|------|------|
| `_hoodie_commit_time` | 提交时间戳 | 20260323092248726 |
| `_hoodie_commit_seqno` | 提交序列号 | 20260323092248726_0_0 |
| `_hoodie_record_key` | 记录主键 | 1 |
| `_hoodie_partition_path` | 分区路径 | category=electronics |
| `_hoodie_file_name` | 所属文件名 | abc123-...-0.parquet |

### 1.4 动手练习

**练习 1.1：查看 Hudi 表的元数据字段**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("spark://spark-master:7077") \
    .appName("HudiBasics") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.jars", "/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1.jar") \
    .getOrCreate()

# 读取已有的 Hudi 表
df = spark.read.format("hudi").load("hdfs://namenode:9000/user/hive/warehouse/hudi_test_cow.db/hudi_cow_table")

# 查看所有字段
df.printSchema()

# 查看 Hudi 元数据字段
df.select("_hoodie_commit_time", "_hoodie_record_key", "_hoodie_partition_path").show()
```

**练习 1.2：查看 Timeline**

```python
# 查看所有提交记录
commits = df.select("_hoodie_commit_time").distinct().orderBy("_hoodie_commit_time")
commits.show()

# 打印提交次数
print(f"总提交次数: {commits.count()}")
```

### 1.5 章节小测验

| 序号 | 问题 | 答案 |
|------|------|------|
| 1 | Hudi 的全称是什么？ | Hadoop Upsert Deletion and Incremental |
| 2 | `_hoodie_commit_time` 表示什么？ | 提交时间戳 |
| 3 | `_hoodie_record_key` 的作用是什么？ | 记录的唯一主键 |
| 4 | Timeline 记录的是什么？ | 所有操作的历史 |
| 5 | File Group 是什么？ | 一组关联的数据文件 |

---

## 第二章：表类型详解（COW vs MOR）

### 2.1 COW（Copy-On-Write）写时复制

**原理：** 更新时复制整个文件，写入新版本。

```
写入流程:
┌─────────────┐     复制     ┌─────────────┐    修改     ┌─────────────┐
│ 原文件 v1   │ ──────────► │ 临时副本    │ ─────────► │ 新文件 v2   │
│ id=1,age=25 │             │             │            │ id=1,age=26 │
└─────────────┘             └─────────────┘            └─────────────┘

读取流程:
直接读取最新 Parquet 文件
```

**特点：**
- ✅ 读取性能好
- ✅ 数据已合并，查询简单
- ❌ 写入性能差（每次都要重写）
- ❌ 存储占用大

**适用场景：** 批量分析、历史快照查询

### 2.2 MOR（Merge-On-Read）读时合并

**原理：** 更新只写 Log 文件，读取时合并 Base + Log。

```
写入流程:
┌─────────────┐                          ┌─────────────┐
│ Base文件    │     追加写入              │  Log文件    │
│ id=1,age=25 │ ────────────────────────► │ id=1,age=26 │
└─────────────┘                          └─────────────┘

读取流程:
┌─────────────┐     合并     ┌─────────────┐
│ Base文件    │ ──────────► │ 最终结果    │
│ id=1,age=25 │    +        │ id=1,age=26 │
└─────────────┘   Log文件    └─────────────┘
```

**特点：**
- ✅ 写入性能好
- ✅ 适合频繁更新
- ❌ 读取性能差（需要合并）
- ❌ 可能有延迟

**适用场景：** 流式写入、CDC 场景

### 2.3 对比表

| 特性 | COW | MOR |
|------|-----|-----|
| 写入性能 | 慢 | 快 |
| 读取性能 | 快 | 慢 |
| 存储占用 | 大 | 小 |
| 数据格式 | 只有 Parquet | Parquet + Log |
| 适用场景 | OLAP 分析 | OLTP/CDC |

### 2.4 动手练习

**练习 2.1：创建 COW 表**

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import time

schema = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("value", IntegerType(), True),
])

data = [("1", "Alice", 100), ("2", "Bob", 200)]
df = spark.createDataFrame(data, schema)

cow_options = {
    "hoodie.table.name": "my_cow_table",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "value",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "bulk_insert",
}

df.write.format("hudi").options(**cow_options).mode("overwrite") \
    .save("hdfs://namenode:9000/user/hive/warehouse/my_cow_table")

# 查询
spark.read.format("hudi").load("hdfs://namenode:9000/user/hive/warehouse/my_cow_table").show()
```

**练习 2.2：创建 MOR 表**

```python
mor_options = {
    "hoodie.table.name": "my_mor_table",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "value",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.table.type": "MERGE_ON_READ",
    "hoodie.datasource.write.operation": "bulk_insert",
    "hoodie.compact.inline": "false",
}

df.write.format("hudi").options(**mor_options).mode("overwrite") \
    .save("hdfs://namenode:9000/user/hive/warehouse/my_mor_table")
```

**练习 2.3：对比文件结构**

```bash
# 查看 COW 表文件（只有 parquet）
docker exec hudi-hdfs-namenode hdfs dfs -ls /user/hive/warehouse/my_cow_table/

# 查看 MOR 表文件（parquet + log）
docker exec hudi-hdfs-namenode hdfs dfs -ls /user/hive/warehouse/my_mor_table/
```

### 2.5 章节小测验

| 序号 | 问题 | 答案 |
|------|------|------|
| 1 | COW 的全称是什么？ | Copy-On-Write |
| 2 | MOR 的全称是什么？ | Merge-On-Read |
| 3 | 哪种表类型写入更快？ | MOR |
| 4 | 哪种表类型读取更快？ | COW |
| 5 | 流式数据写入应该用哪种？ | MOR |
| 6 | MOR 表的数据文件有哪些类型？ | Parquet + Log |

---

## 第三章：表操作（增删改查）

### 3.1 插入数据（Insert）

**批量插入：**
```python
# 使用 bulk_insert 优化性能
options = {
    "hoodie.table.name": "my_table",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "bulk_insert",
    "hoodie.upsert.shuffle.parallelism": "200",
}

df.write.format("hudi").options(**options).mode("append").save(table_path)
```

### 3.2 更新数据（Upsert）

**Upsert = Update + Insert**
- 如果主键存在 → 更新
- 如果主键不存在 → 插入

```python
# 原始数据
original_data = [("1", "Alice", 25), ("2", "Bob", 30)]

# 更新数据（id=1更新，id=3插入）
update_data = [("1", "Alice_New", 26), ("3", "Charlie", 35)]

options = {
    "hoodie.table.name": "my_table",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "upsert",
}

df_update.write.format("hudi").options(**options).mode("append").save(table_path)
```

### 3.3 删除数据（Delete）

**方式 1：使用 delete 操作**
```python
delete_data = [("1", "Alice", 25)]

df_delete.write.format("hudi") \
    .option("hoodie.datasource.write.operation", "delete") \
    .options(**options) \
    .mode("append") \
    .save(table_path)
```

**方式 2：使用 Spark SQL**
```python
spark.sql(f"DELETE FROM hudi.`{table_path}` WHERE id = '1'")
```

### 3.4 查询数据（Read）

**全量查询：**
```python
df = spark.read.format("hudi").load(table_path)
df.show()
```

**条件查询：**
```python
df = spark.read.format("hudi").load(table_path)
df.filter(df.age > 25).show()
```

**指定字段查询：**
```python
df.select("id", "name", "value").show()
```

### 3.5 动手练习

**练习 3.1：完整 CRUD 操作**

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import time

table_path = "hdfs://namenode:9000/user/hive/warehouse/my_crud_table"

schema = StructType([
    StructField("id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("score", IntegerType(), True),
    StructField("ts", LongType(), True),
])

# Step 1: 插入初始数据
data_insert = [
    ("1", "语文", 90, int(time.time() * 1000)),
    ("2", "数学", 85, int(time.time() * 1000)),
    ("3", "英语", 95, int(time.time() * 1000)),
]
df_insert = spark.createDataFrame(data_insert, schema)

options = {
    "hoodie.table.name": "my_crud_table",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "bulk_insert",
}

df_insert.write.format("hudi").options(**options).mode("overwrite").save(table_path)
print("=== 初始数据 ===")
spark.read.format("hudi").load(table_path).show()

# Step 2: Upsert（更新 id=1，插入 id=4）
time.sleep(2)
data_update = [
    ("1", "语文", 92, int(time.time() * 1000)),
    ("4", "物理", 88, int(time.time() * 1000)),
]
df_update = spark.createDataFrame(data_update, schema)
options["hoodie.datasource.write.operation"] = "upsert"
df_update.write.format("hudi").options(**options).mode("append").save(table_path)
print("=== Upsert 后 ===")
spark.read.format("hudi").load(table_path).show()

# Step 3: 删除 id=3
time.sleep(2)
data_delete = [("3", "英语", 95, int(time.time() * 1000))]
df_delete = spark.createDataFrame(data_delete, schema)
df_delete.write.format("hudi") \
    .option("hoodie.datasource.write.operation", "delete") \
    .options(**options).mode("append").save(table_path)
print("=== 删除后 ===")
spark.read.format("hudi").load(table_path).show()
```

### 3.6 章节小测验

| 序号 | 问题 | 答案 |
|------|------|------|
| 1 | Upsert 是什么操作？ | Update + Insert 的组合 |
| 2 | `bulk_insert` 和 `upsert` 有什么区别？ | bulk_insert 是批量插入优化，upsert 支持更新 |
| 3 | 如何用 SQL 删除数据？ | DELETE FROM hudi.`path` WHERE condition |
| 4 | `precombine.field` 的作用是什么？ | 当主键冲突时决定保留哪个版本 |

---

## 第四章：时间旅行

### 4.1 概念

时间旅行（Time Travel）可以查询**过去某个时间点**的数据状态。

```
时间线:
T1 ──────────► T2 ──────────► T3 ──────────► T4
   插入3条        更新1条        删除1条        插入2条
   (3条)          (4条)         (3条)          (5条)
```

```
查询 T1 时刻: 返回 3 条
查询 T2 时刻: 返回 4 条
查询 T4 时刻: 返回 5 条
```

### 4.2 使用方式

**查询指定版本：**
```python
# 获取所有提交时间
commits = spark.read.format("hudi").load(table_path) \
    .select("_hoodie_commit_time").distinct().orderBy("_hoodie_commit_time")
commit_list = [row._hoodie_commit_time for row in commits.collect()]

# 查询最早版本
first_commit = commit_list[0]
df_v1 = spark.read.format("hudi") \
    .option("as.of.instant", first_commit) \
    .load(table_path)
df_v1.show()

# 查询特定时间点
df_t = spark.read.format("hudi") \
    .option("as.of.instant", "20260323092248726") \
    .load(table_path)
```

### 4.3 实际应用

| 场景 | 说明 |
|------|------|
| 数据回滚 | 发现错误，查询错误前的数据状态 |
| 审计追溯 | 查看某个时间点的数据快照 |
| 数据对比 | 对比前后两个版本的差异 |

### 4.4 动手练习

**练习 4.1：对比不同版本的数据**

```python
# 获取所有提交
commits = spark.read.format("hudi").load(table_path) \
    .select("_hoodie_commit_time").distinct().orderBy("_hoodie_commit_time")
commit_list = [row._hoodie_commit_time for row in commits.collect()]

print(f"共有 {len(commit_list)} 个版本: {commit_list}")

# 对比第一个和最后一个版本
for i, commit in enumerate(commit_list):
    df_version = spark.read.format("hudi") \
        .option("as.of.instant", commit) \
        .load(table_path)
    print(f"\n版本 {i+1} ({commit}): {df_version.count()} 条记录")
    df_version.select("id", "name", "value", "_hoodie_commit_time").show(truncate=False)
```

**练习 4.2：发现数据异常并回溯**

```python
# 假设发现当前数据有问题
current = spark.read.format("hudi").load(table_path)
print("当前数据:")
current.show()

# 回溯到之前的版本查看
previous = spark.read.format("hudi") \
    .option("as.of.instant", commit_list[-2]) \
    .load(table_path)
print("上一版本数据:")
previous.show()
```

### 4.5 章节小测验

| 序号 | 问题 | 答案 |
|------|------|------|
| 1 | 时间旅行的参数是什么？ | `as.of.instant` |
| 2 | 时间旅行基于什么实现？ | Hudi Timeline |
| 3 | 时间旅行有什么实际用途？ | 数据回滚、审计追溯、版本对比 |
| 4 | COW 表支持时间旅行吗？ | 支持 |
| 5 | MOR 表支持时间旅行吗？ | 支持 |

---

## 第五章：增量查询

### 5.1 概念

增量查询（Incremental Query）只获取**指定时间点之后的新数据**，而不是全量数据。

```
全量查询:
┌─────────────────────────────────────┐
│ 返回所有数据（100万条）              │
└─────────────────────────────────────┘

增量查询:
┌─────────────────────────────────────┐
│ 只返回上次处理后的新增数据（1万条）   │
└─────────────────────────────────────┘
```

### 5.2 使用方式

**MOR 表增量查询：**
```python
# 从某个提交时间开始增量
df_incremental = spark.read.format("hudi") \
    .option("hoodie.datasource.query.type", "incremental") \
    .option("hoodie.datasource.read.begin.instanttime", "20260323092248726") \
    .load(table_path)

print(f"增量记录: {df_incremental.count()}")
df_incremental.show()
```

### 5.3 全量 vs 增量对比

| 特性 | 全量查询 | 增量查询 |
|------|---------|---------|
| 返回数据 | 所有数据 | 指定时间后的新数据 |
| 性能 | 慢 | 快 |
| 参数 | 默认 | `query.type=incremental` |
| 适用场景 | 首次同步 | 定时同步 |

### 5.4 增量同步架构

```
┌──────────┐        ┌──────────┐        ┌──────────┐
│  Hudi    │ 增量   │  Spark   │        │  目标库  │
│  源表    │ ─────► │  任务    │ ─────► │  (Hive)  │
└──────────┘        └──────────┘        └──────────┘
                        │
                   每次记录上次处理的
                   commit_time，下次
                   从这个时间点继续
```

### 5.5 动手练习

**练习 5.1：模拟增量同步场景**

```python
# Step 1: 获取当前时间作为起始点
import time
start_time = str(int(time.time() * 1000))

# Step 2: 写入第一批数据
data_batch1 = [("1", "A", 100), ("2", "B", 200)]
df1 = spark.createDataFrame(data_batch1, ["id", "name", "value"])
df1.write.format("hudi").options(**options).mode("append").save(table_path)
print("第一批数据写入完成")

# Step 3: 等待一会儿，写入第二批
time.sleep(2)
data_batch2 = [("3", "C", 300), ("4", "D", 400)]
df2 = spark.createDataFrame(data_batch2, ["id", "name", "value"])
df2.write.format("hudi").options(**options).mode("append").save(table_path)
print("第二批数据写入完成")

# Step 4: 增量查询只获取第二批数据
df_incremental = spark.read.format("hudi") \
    .option("hoodie.datasource.query.type", "incremental") \
    .option("hoodie.datasource.read.begin.instanttime", start_time) \
    .load(table_path)

print(f"\n增量数据（只返回第二批）:")
df_incremental.select("id", "name", "value").show()
```

**练习 5.2：对比全量和增量性能**

```python
import time

# 全量查询
start = time.time()
df_full = spark.read.format("hudi").load(table_path)
full_count = df_full.count()
full_time = time.time() - start
print(f"全量查询: {full_count} 条, 耗时: {full_time:.2f}s")

# 增量查询
start = time.time()
df_inc = spark.read.format("hudi") \
    .option("hoodie.datasource.query.type", "incremental") \
    .option("hoodie.datasource.read.begin.instanttime", commit_list[-2]) \
    .load(table_path)
inc_count = df_inc.count()
inc_time = time.time() - start
print(f"增量查询: {inc_count} 条, 耗时: {inc_time:.2f}s")
```

### 5.6 章节小测验

| 序号 | 问题 | 答案 |
|------|------|------|
| 1 | 增量查询的参数是什么？ | `hoodie.datasource.read.begin.instanttime` |
| 2 | 增量查询适合什么场景？ | 定时同步、增量ETL |
| 3 | COW 表支持增量查询吗？ | 不直接支持，需要特殊配置 |
| 4 | MOR 表支持增量查询吗？ | 支持 |
| 5 | 增量查询和全量查询哪个更快？ | 增量查询 |

---

## 第六章：分区策略

### 6.1 什么是分区？

分区是按某个字段将数据**分开存储**，提高查询效率。

```
不分区:
/user/hive/warehouse/table/
├── data1.parquet   (包含所有数据)
├── data2.parquet
└── data3.parquet

按日期分区:
/user/hive/warehouse/table/
├── dt=2026-03-01/
│   └── data.parquet
├── dt=2026-03-02/
│   └── data.parquet
└── dt=2026-03-03/
    └── data.parquet
```

### 6.2 分区的好处

| 好处 | 说明 |
|------|------|
| 查询加速 | 只扫描需要的分区 |
| 数据管理 | 按分区删除/归档 |
| 存储优化 | 不同分区可使用不同存储策略 |

### 6.3 分区配置

```python
options = {
    "hoodie.table.name": "my_partitioned_table",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.partitionpath.field": "category",  # 分区字段
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "bulk_insert",
    "hoodie.datasource.write.hive_style_partitioning": "true",  # category=value 格式
}
```

### 6.4 Hive 风格分区

```
Hive 风格 (category=electronics):
/user/hive/warehouse/table/category=electronics/data.parquet

普通风格 (electronics):
/user/hive/warehouse/table/electronics/data.parquet
```

### 6.5 动手练习

**练习 6.1：创建分区表**

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import time

schema = StructType([
    StructField("id", StringType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", IntegerType(), True),
    StructField("ts", LongType(), True),
])

data = [
    ("1", "iPhone 15", "electronics", 6999, int(time.time() * 1000)),
    ("2", "MacBook Pro", "electronics", 12999, int(time.time() * 1000)),
    ("3", "T-Shirt", "clothing", 99, int(time.time() * 1000)),
    ("4", "Jeans", "clothing", 299, int(time.time() * 1000)),
    ("5", "Apple", "food", 15, int(time.time() * 1000)),
    ("6", "Banana", "food", 8, int(time.time() * 1000)),
]

df = spark.createDataFrame(data, schema)

partitioned_options = {
    "hoodie.table.name": "partitioned_products",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.partitionpath.field": "category",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "bulk_insert",
    "hoodie.datasource.write.hive_style_partitioning": "true",
}

partitioned_path = "hdfs://namenode:9000/user/hive/warehouse/partitioned_products"

df.write.format("hudi").options(**partitioned_options).mode("overwrite").save(partitioned_path)
```

**练习 6.2：按分区查询**

```python
# 查询所有数据
all_df = spark.read.format("hudi").load(partitioned_path)
all_df.select("id", "product_name", "category", "_hoodie_partition_path").show()

# 只查询 electronics 分区
electronics_df = spark.read.format("hudi") \
    .load(f"{partitioned_path}/category=electronics")
print("Electronics 分区数据:")
electronics_df.show()

# 查看分区分布
all_df.groupBy("_hoodie_partition_path").count().show()
```

**练习 6.3：添加新分区数据**

```python
new_data = [
    ("7", "Samsung Galaxy", "electronics", 5999, int(time.time() * 1000)),
    ("8", "Nike Shoes", "sports", 899, int(time.time() * 1000)),
]
df_new = spark.createDataFrame(new_data, schema)
df_new.write.format("hudi").options(**partitioned_options).mode("append").save(partitioned_path)

# 验证新分区
spark.read.format("hudi").load(partitioned_path).groupBy("_hoodie_partition_path").count().show()
```

### 6.6 章节小测验

| 序号 | 问题 | 答案 |
|------|------|------|
| 1 | 分区的目的是什么？ | 提高查询效率，数据管理 |
| 2 | `hoodie.datasource.write.partitionpath.field` 是什么？ | 分区字段配置 |
| 3 | Hive 风格分区是什么格式？ | `key=value` |
| 4 | 分区表查询时会扫描所有数据吗？ | 不会，只扫描指定分区 |
| 5 | 分区字段应该选择什么类型？ | 通常选择低基数字段（如日期、类别） |

---

## 第七章：主键设计

### 7.1 主键的重要性

主键是 Hudi 中**唯一标识一条记录**的字段，用于：
- Upsert 时匹配记录
- 删除时定位记录
- 去重

### 7.2 主键类型

**简单主键：**
```python
"hoodie.datasource.write.recordkey.field": "id"
```

**复合主键：**
```python
# 使用冒号分隔多个字段
"hoodie.datasource.write.recordkey.field": "tenant_id:event_id"
```

### 7.3 主键设计原则

| 原则 | 说明 |
|------|------|
| 唯一性 | 主键必须能唯一标识记录 |
| 稳定性 | 主键一旦确定不应修改 |
| 足够长 | 避免碰撞，建议8位以上 |
| 业务相关 | 使用业务相关的自然键或合成键 |

### 7.4 动手练习

**练习 7.1：简单主键表**

```python
simple_data = [
    ("1001", "订单A", 100),
    ("1002", "订单B", 200),
]

options_simple = {
    "hoodie.table.name": "simple_key_table",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "amount",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "bulk_insert",
}

df = spark.createDataFrame(simple_data, ["id", "order_name", "amount"])
df.write.format("hudi").options(**options_simple).mode("overwrite") \
    .save("hdfs://namenode:9000/user/hive/warehouse/simple_key_table")

# 查看主键
spark.read.format("hudi").load("hdfs://namenode:9000/user/hive/warehouse/simple_key_table") \
    .select("_hoodie_record_key", "id", "order_name").show()
```

**练习 7.2：复合主键表**

```python
composite_data = [
    ("tenant1", "order1", "订单A", 100),
    ("tenant1", "order2", "订单B", 200),
    ("tenant2", "order1", "订单C", 300),  # 同 order1 但不同 tenant
]

options_composite = {
    "hoodie.table.name": "composite_key_table",
    "hoodie.datasource.write.recordkey.field": "tenant_id:order_id",  # 复合主键
    "hoodie.datasource.write.precombine.field": "amount",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "bulk_insert",
}

df = spark.createDataFrame(composite_data, ["tenant_id", "order_id", "order_name", "amount"])
df.write.format("hudi").options(**options_composite).mode("overwrite") \
    .save("hdfs://namenode:9000/user/hive/warehouse/composite_key_table")

# 查看复合主键
spark.read.format("hudi").load("hdfs://namenode:9000/user/hive/warehouse/composite_key_table") \
    .select("_hoodie_record_key", "tenant_id", "order_id", "order_name").show()
```

### 7.5 章节小测验

| 序号 | 问题 | 答案 |
|------|------|------|
| 1 | 主键的配置参数是什么？ | `hoodie.datasource.write.recordkey.field` |
| 2 | 复合主键用什么分隔？ | 冒号 `:` |
| 3 | 主键应该具备什么特性？ | 唯一性、稳定性 |
| 4 | Upsert 操作依赖什么匹配记录？ | 主键 |

---

## 综合实战项目

### 项目：电商订单数据湖

**目标：** 使用 Hudi 构建一个电商订单数据湖，实现：
1. 创建 COW 表存储订单主数据
2. 创建 MOR 表存储订单明细流式数据
3. 实现 UPSERT 操作
4. 使用时间旅行进行数据回溯
5. 使用增量查询实现数据同步

### 项目步骤

**Step 1：创建订单主表（COW）**

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType
import time

# 订单主表 Schema
order_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("order_amount", DoubleType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_time", LongType(), True),
])

# 初始订单数据
orders = [
    ("ORD001", "CUST001", 199.99, "pending", int(time.time() * 1000)),
    ("ORD002", "CUST002", 299.99, "pending", int(time.time() * 1000)),
    ("ORD003", "CUST001", 99.99, "pending", int(time.time() * 1000)),
]

df_orders = spark.createDataFrame(orders, order_schema)

# 写入 COW 表
cow_options = {
    "hoodie.table.name": "orders_cow",
    "hoodie.datasource.write.recordkey.field": "order_id",
    "hoodie.datasource.write.precombine.field": "order_time",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "bulk_insert",
}

orders_path = "hdfs://namenode:9000/user/hive/warehouse/ecommerce.db/orders"
df_orders.write.format("hudi").options(**cow_options).mode("overwrite").save(orders_path)
print("订单主表创建完成")
```

**Step 2：更新订单状态（Upsert）**

```python
time.sleep(2)

# 订单状态更新
updates = [
    ("ORD001", "CUST001", 199.99, "paid", int(time.time() * 1000)),
    ("ORD004", "CUST003", 599.99, "pending", int(time.time() * 1000)),  # 新订单
]

df_updates = spark.createDataFrame(updates, order_schema)
cow_options["hoodie.datasource.write.operation"] = "upsert"
df_updates.write.format("hudi").options(**cow_options).mode("append").save(orders_path)

# 查看更新后数据
spark.read.format("hudi").load(orders_path).select(
    "order_id", "order_status", "order_amount"
).show()
```

**Step 3：创建订单明细表（MOR）**

```python
# 订单明细 Schema
detail_schema = StructType([
    StructField("detail_id", StringType(), False),
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("ts", LongType(), True),
])

# 明细数据（模拟流式写入）
details = [
    ("D001", "ORD001", "P001", 2, int(time.time() * 1000)),
    ("D002", "ORD001", "P002", 1, int(time.time() * 1000)),
    ("D003", "ORD002", "P003", 3, int(time.time() * 1000)),
]

df_details = spark.createDataFrame(details, detail_schema)

mor_options = {
    "hoodie.table.name": "order_details_mor",
    "hoodie.datasource.write.recordkey.field": "detail_id",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.table.type": "MERGE_ON_READ",
    "hoodie.datasource.write.operation": "bulk_insert",
}

details_path = "hdfs://namenode:9000/user/hive/warehouse/ecommerce.db/order_details"
df_details.write.format("hudi").options(**mor_options).mode("overwrite").save(details_path)
print("订单明细表创建完成")
```

**Step 4：时间旅行分析**

```python
# 获取订单表的所有提交
commits_df = spark.read.format("hudi").load(orders_path) \
    .select("_hoodie_commit_time").distinct().orderBy("_hoodie_commit_time")
commits = [row._hoodie_commit_time for row in commits_df.collect()]

print(f"订单表有 {len(commits)} 次提交")

# 查看订单创建时的状态
df_v1 = spark.read.format("hudi") \
    .option("as.of.instant", commits[0]) \
    .load(orders_path)
print("订单创建时的状态:")
df_v1.select("order_id", "order_status").show()

# 查看订单更新后的状态
df_v2 = spark.read.format("hudi") \
    .option("as.of.instant", commits[-1]) \
    .load(orders_path)
print("订单更新后的状态:")
df_v2.select("order_id", "order_status").show()
```

**Step 5：增量同步**

```python
# 模拟增量同步（只获取最新更新的订单）
detail_commits = spark.read.format("hudi").load(details_path) \
    .select("_hoodie_commit_time").distinct().orderBy("_hoodie_commit_time")
detail_commit_list = [row._hoodie_commit_time for row in detail_commits.collect()]

# 增量获取订单明细
df_incremental_details = spark.read.format("hudi") \
    .option("hoodie.datasource.query.type", "incremental") \
    .option("hoodie.datasource.read.begin.instanttime", detail_commit_list[0]) \
    .load(details_path)

print("增量同步的订单明细:")
df_incremental_details.select("detail_id", "order_id", "product_id", "quantity").show()
```

### 项目小测验

| 序号 | 问题 | 答案 |
|------|------|------|
| 1 | 订单主表为什么选择 COW？ | 读多写少，需要快速查询 |
| 2 | 订单明细表为什么选择 MOR？ | 流式写入，频繁更新 |
| 3 | Upsert 操作的作用是什么？ | 同时处理更新和插入 |
| 4 | 时间旅行用于什么？ | 数据回溯、状态对比 |
| 5 | 增量查询用于什么？ | 定时同步、减少数据传输 |

---

## 附录

### A. 常用配置参数

| 参数 | 说明 | 示例值 |
|------|------|--------|
| `hoodie.table.name` | 表名 | my_table |
| `hoodie.datasource.write.recordkey.field` | 主键字段 | id |
| `hoodie.datasource.write.precombine.field` | 排序字段 | ts |
| `hoodie.datasource.write.partitionpath.field` | 分区字段 | category |
| `hoodie.datasource.write.table.type` | 表类型 | COPY_ON_WRITE / MERGE_ON_READ |
| `hoodie.datasource.write.operation` | 写入操作 | bulk_insert / upsert / delete |
| `hoodie.datasource.write.hive_style_partitioning` | Hive风格分区 | true / false |

### B. 故障排查

| 问题 | 可能原因 | 解决方案 |
|------|---------|---------|
| Hudi JAR 未找到 | 版本不匹配 | 检查 Spark 和 Hudi 版本 |
| Incremental 查询失败 | 表类型不支持 | 使用 MOR 表 |
| 写入超时 | Spark 资源不足 | 增加 Worker 资源 |
| 数据丢失 | 主键冲突 | 检查 precombine.field |

### C. 学习资源

- [Apache Hudi 官方文档](https://hudi.apache.org/docs/overview)
- [Hudi GitHub](https://github.com/apache/hudi)
- [Hudi 快速入门](https://hudi.apache.org/docs/quick-start-guide)

---

> 文档版本: v1.0
> 更新日期: 2026-03-23
> 适用环境: Spark 3.5.8 + Hudi 1.1.1
