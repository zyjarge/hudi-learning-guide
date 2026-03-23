# 第二章：COW vs MOR（表类型详解） - 学习笔记

## 实验环境

| 组件 | 版本 |
|------|------|
| Spark | 3.5.8 |
| Hudi | 1.1.1 |
| Scala | 2.12 |

---

## 知识点总结

### 1. COW 和 MOR 全称

| 类型 | 全称 | 中文 | 一句话解释 |
|------|------|------|-----------|
| **COW** | Copy-On-Write | 写时复制 | 更新时复制整个文件再改 |
| **MOR** | Merge-On-Read | 读时合并 | 更新时追加日志，读时合并 |

### 2. 配置参数

| 参数 | COW 值 | MOR 值 |
|------|--------|--------|
| `hoodie.datasource.write.table.type` | `COPY_ON_WRITE` | `MERGE_ON_READ` |
| `hoodie.compact.inline` | 不需要 | `false`（推荐） |
| `hoodie.datasource.write.operation` | `bulk_insert`/`upsert` | `bulk_insert`/`upsert` |

### 3. 特性对比

| 特性 | COW | MOR |
|------|-----|-----|
| **写入性能** | 慢（重写文件） | 快（追加日志） |
| **读取性能** | 快（已合并） | 慢（需合并） |
| **存储空间** | 大（保留多版本） | 小（只存增量） |
| **数据格式** | 仅 Parquet | Parquet + Log |
| **适用场景** | 读多写少 | 写多读少 |

### 4. 工作原理

**COW（写时复制）：**
```
更新操作：
┌──────────┐     复制     ┌──────────┐    修改     ┌──────────┐
│ 原文件   │ ──────────► │ 副本     │ ─────────► │ 新文件   │
└──────────┘             └──────────┘            └──────────┘
```

**MOR（读时合并）：**
```
更新操作：
┌──────────┐                         ┌──────────┐
│ 基础文件 │    追加 log             │ Log 文件 │
│ (Parquet)│ ─────────────────────► │ (.log)   │
└──────────┘                        └──────────┘
```

---

## 实操步骤回顾

### Step 1：创建 COW 表
```python
cow_options = {
    "hoodie.table.name": "orders_cow",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    # ...
}
```
**结果：** 写入耗时 20.89秒

### Step 2：查看 COW 文件结构
```
orders_cow/
├── .hoodie/
├── *.parquet  # 只有 Parquet 文件
└── *.parquet
```

### Step 3：创建 MOR 表
```python
mor_options = {
    "hoodie.table.name": "orders_mor",
    "hoodie.datasource.write.table.type": "MERGE_ON_READ",
    "hoodie.compact.inline": "false",
    # ...
}
```
**结果：** 写入耗时 18.56秒

### Step 4：查看 MOR 文件结构
```
orders_mor/
├── .hoodie/
│   └── metadata/  # 元数据表包含 .log 文件
└── *.parquet      # 主表（小数据量直接写Parquet）
```

### Step 5：Upsert 操作
```python
mor_options["hoodie.datasource.write.operation"] = "upsert"
```
- 更新 ORD001 价格：6999 → 5999
- 新增 ORD004：iPad 3999

### Step 6：读取性能对比

| 表类型 | 读取耗时 | 记录数 |
|--------|---------|--------|
| COW | 13.595秒 | 3条 |
| MOR | 1.045秒 | 4条 |

**注意：** 小数据量下差异不明显，甚至 MOR 可能更快

---

## 实验数据

### COW 表初始数据
| order_id | product | price |
|----------|---------|-------|
| ORD001 | iPhone | 6999 |
| ORD002 | MacBook | 12999 |
| ORD003 | AirPods | 1299 |

### MOR 表更新后数据
| order_id | product | price | 说明 |
|----------|---------|-------|------|
| ORD001 | iPhone | 5999 | 更新 |
| ORD002 | MacBook | 12999 | 不变 |
| ORD003 | AirPods | 1299 | 不变 |
| ORD004 | iPad | 3999 | 新增 |

---

## 关键发现

### 发现 1：小数据量时 MOR 的 Log 文件
- 在 Hudi 1.1.1 中，小数据量时 MOR 表可能直接生成新的 Parquet 文件
- Log 文件主要在 `.hoodie/metadata/` 元数据表中出现
- 这是 Hudi 的优化策略

### 发现 2：读取性能
- 小数据量下 COW 和 MOR 读取差异不明显
- 大数据量、频繁更新场景下 COW 读取优势才会体现

---

## 选择建议

```
┌─────────────────────────────────────────────────────┐
│                   如何选择？                          │
├─────────────────────────────────────────────────────┤
│                                                      │
│  场景                    推荐表类型                   │
│  ─────────────────────────────────────────────────── │
│  批量分析、报表查询       COW                        │
│  流式写入、实时更新       MOR                        │
│  CDC 数据同步            MOR                        │
│  历史快照查询多           COW                        │
│                                                      │
└─────────────────────────────────────────────────────┘
```

---

## 代码模板

### 创建 COW 表
```python
cow_options = {
    "hoodie.table.name": "my_cow_table",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "bulk_insert",
}

df.write.format("hudi").options(**cow_options).mode("overwrite").save(path)
```

### 创建 MOR 表
```python
mor_options = {
    "hoodie.table.name": "my_mor_table",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.table.type": "MERGE_ON_READ",
    "hoodie.datasource.write.operation": "bulk_insert",
    "hoodie.compact.inline": "false",
}

df.write.format("hudi").options(**mor_options).mode("overwrite").save(path)
```

---

## 问题记录

### Q: 为什么 MOR 表没有 .log 文件？
**A:** 在 Hudi 1.1.1 中，小数据量时 Hudi 会优化为直接写入新的 Parquet 文件，而不是生成 log 文件。Log 文件在大数据量频繁更新时才会明显出现。

### Q: COW 和 MOR 写入时间差不多？
**A:** 在小数据量下，两种表类型的写入性能差异不明显。大数据量时，MOR 的写入优势才会体现。

---

> 笔记创建时间：2026-03-23
> 章节：第二章 - COW vs MOR
> 下一步：章节测验
