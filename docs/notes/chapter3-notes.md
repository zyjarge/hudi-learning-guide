# 第三章：表操作（增删改查） - 学习笔记

## 实验环境

| 组件 | 版本 |
|------|------|
| Spark | 3.5.8 |
| Hudi | 1.1.1 |

---

## 知识点总结

### 1. 操作类型

| 操作 | 参数值 | 说明 |
|------|--------|------|
| 首次批量插入 | `bulk_insert` | 不检查重复，性能最快 |
| 普通插入 | `insert` | 会检查重复 |
| 更新/新增 | `upsert` | 主键存在则更新，不存在则插入 |
| 删除 | `delete` | 根据主键删除记录 |

### 2. 核心配置参数

| 参数 | 说明 | 常用值 |
|------|------|--------|
| `hoodie.datasource.write.operation` | 操作类型 | bulk_insert/upsert/delete |
| `hoodie.datasource.write.recordkey.field` | 主键字段 | 业务主键字段名 |
| `hoodie.datasource.write.precombine.field` | 排序字段 | ts（时间戳） |
| `hoodie.upsert.shuffle.parallelism` | Upsert并行度 | 200 |
| `hoodie.insert.shuffle.parallelism` | Insert并行度 | 200 |

### 3. precombine.field 原理

```
同一条记录有多个版本时，取 precombine.field 值最大的保留

输入:
  版本1: id=1, score=80,  ts=1000
  版本2: id=1, score=95,  ts=2000

precombine.field = ts:
  比较 1000 vs 2000，取最大值 2000
  
输出:
  id=1, score=95, ts=2000
```

### 4. 保留最小值的技巧（负数法）

```
需求：取 ts 最小的记录

原始 ts:  1000, 2000, 500
转负数:   -1000, -2000, -500
取最大:   -500 (对应原值 500，恰好是最小值)

代码:
df.withColumn("ts_neg", -col("ts").cast("long"))
```

---

## 实操步骤回顾

### Step 1：批量插入（bulk_insert）

```python
hudi_options = {
    "hoodie.datasource.write.operation": "bulk_insert",
}
```

**插入数据：**
| student_id | name | score |
|------------|------|-------|
| S001 | 张三 | 85 |
| S002 | 李四 | 92 |
| S003 | 王五 | 78 |

### Step 2：Upsert 操作

```python
hudi_options = {
    "hoodie.datasource.write.operation": "upsert",
}
```

**操作结果：**
| 学生 | 操作 | 变化 |
|------|------|------|
| 张三 | 更新 | score: 85 → 95 |
| 赵六 | 新增 | score: 88 |

### Step 3：删除操作

```python
df.write.format("hudi") \
    .option("hoodie.datasource.write.operation", "delete") \
    .mode("append").save(path)
```

**删除结果：** 王五(S003) 被删除

### Step 4：precombine.field 验证

**写入冲突数据：**
| student_id | score | ts | 说明 |
|------------|-------|-----|------|
| S001 | 60 | 小 | 旧版本 |
| S001 | 98 | 大 | 新版本 |

**结果：** 保留 score=98（ts 大的版本）

---

## 最终数据

| student_id | name | score |
|------------|------|-------|
| S001 | 张三 | 95 |
| S002 | 李四 | 92 |
| S004 | 赵六 | 88 |

---

## 代码模板

### 批量插入
```python
options = {
    "hoodie.table.name": "my_table",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "bulk_insert",
}

df.write.format("hudi").options(**options).mode("overwrite").save(path)
```

### Upsert 更新/新增
```python
options["hoodie.datasource.write.operation"] = "upsert"
df.write.format("hudi").options(**options).mode("append").save(path)
```

### 删除记录
```python
df.write.format("hudi") \
    .option("hoodie.table.name", "my_table") \
    .option("hoodie.datasource.write.recordkey.field", "id") \
    .option("hoodie.datasource.write.precombine.field", "ts") \
    .option("hoodie.datasource.write.table.type", "COPY_ON_WRITE") \
    .option("hoodie.datasource.write.operation", "delete") \
    .mode("append") \
    .save(path)
```

---

## 常见问题

### Q: bulk_insert 和 upsert 的区别？
**A:** bulk_insert 不检查重复直接写入，upsert 会根据主键判断是更新还是插入。

### Q: precombine.field 丢弃旧数据了吗？
**A:** 是的，旧版本数据会被丢弃，只保留 precombine 后的结果。

### Q: 如何保留最早的记录（最小ts）？
**A:** 使用负数法，把 ts 转成负数后配置为 precombine.field。

---

## 实操路径

```
数据路径: hdfs://namenode:9000/user/hive/warehouse/learning/students_crud
表名: students_crud
```

---

> 笔记创建时间：2026-03-24
> 章节：第三章 - 表操作（增删改查）
> 下一步：章节测验
