# Hudi 速查卡片

## 快速配置模板

### 创建 COW 表
```python
options = {
    "hoodie.table.name": "my_table",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "bulk_insert",
}
```

### 创建 MOR 表
```python
options = {
    "hoodie.table.name": "my_table",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "ts",
    "hoodie.datasource.write.partitionpath.field": "",
    "hoodie.datasource.write.table.type": "MERGE_ON_READ",
    "hoodie.datasource.write.operation": "upsert",
}
```

## 常用操作

| 操作 | 代码 |
|------|------|
| 插入 | `df.write.format("hudi").options(**opt).mode("append").save(path)` |
| 覆盖 | `df.write.format("hudi").options(**opt).mode("overwrite").save(path)` |
| 查询 | `spark.read.format("hudi").load(path)` |
| 时间旅行 | `.option("as.of.instant", "20260323092248726")` |
| 增量查询 | `.option("hoodie.datasource.query.type", "incremental").option("hoodie.datasource.read.begin.instanttime", time)` |

## COW vs MOR

| | COW | MOR |
|--|-----|-----|
| 写入 | 慢 | 快 |
| 读取 | 快 | 慢 |
| 文件 | Parquet | Parquet + Log |
| 场景 | OLAP | CDC/流式 |

## Hudi 元数据字段

- `_hoodie_commit_time` - 提交时间
- `_hoodie_record_key` - 主键
- `_hoodie_partition_path` - 分区路径
- `_hoodie_file_name` - 文件名
