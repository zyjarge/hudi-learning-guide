# Hudi Learning Guide

> Apache Hudi 实战学习指南 - 基于 Spark 3.5.8 + Hudi 1.1.1

## 📚 项目简介

本项目提供完整的 Apache Hudi 学习资源，包括：

- **Docker 环境** - 一键启动 Hudi 学习环境
- **学习文档** - 系统的 Hudi 知识讲解
- **示例代码** - 可直接运行的练习脚本
- **速查卡片** - 常用操作快速参考

## 🏗️ 技术栈

| 组件 | 版本 | 用途 |
|------|------|------|
| Spark | 3.5.8 | 计算引擎 |
| Hudi | 1.1.1 | 数据湖框架 |
| HDFS | 3.x | 分布式存储 |
| Kafka | 7.4.0 | 消息队列 |
| Hive Metastore | 3.1.3 | 元数据服务 |

## 📁 项目结构

```
hudi-docker/
├── docker-compose.yml      # Docker 编排配置
├── .gitignore
├── README.md
├── docs/                   # 学习文档
│   ├── Hudi-Learning-Guide.md    # 主学习指南
│   └── Hudi-Quick-Reference.md   # 速查卡片
├── scripts/                # 示例脚本
│   ├── test_hudi_table.py        # COW 表测试
│   ├── test_hudi_upsert.py       # Upsert 测试
│   ├── test_hudi_mor.py          # MOR 表测试
│   ├── test_hudi_incremental.py  # 增量查询测试
│   ├── test_hudi_compaction.py   # Compaction 测试
│   ├── test_hudi_delete.py       # 删除操作测试
│   ├── test_hudi_partition.py    # 分区策略测试
│   └── test_hudi_keys.py         # 主键设计测试
├── config/                 # 配置文件
│   ├── hdfs/                    # HDFS 配置
│   └── hive/                    # Hive 配置
└── jars/                   # JAR 文件 (需单独下载)
    └── README.md
```

## 🚀 快速开始

### 1. 启动环境

```bash
cd hudi-docker
docker compose up -d
```

### 2. 验证服务

```bash
# 查看运行状态
docker compose ps

# Spark Master UI: http://localhost:8082
# Kafka UI: http://localhost:32789
# HDFS UI: http://localhost:32770
```

### 3. 下载 Hudi JAR

```bash
# 下载到 jars/ 目录
wget -P jars/ https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.5-bundle_2.12/1.1.1/hudi-spark3.5-bundle_2.12-1.1.1.jar
```

### 4. 运行示例

```bash
docker exec hudi-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog \
  --conf spark.jars=/opt/spark/jars/hudi-spark3.5-bundle_2.12-1.1.1.jar \
  /scripts/test_hudi_table.py
```

## 📖 学习路径

1. **基础概念** - 了解 Hudi 核心概念
2. **表类型** - COW vs MOR 的区别
3. **CRUD 操作** - 增删改查实践
4. **时间旅行** - 版本查询与回溯
5. **增量查询** - 增量数据同步
6. **分区策略** - 数据分区优化
7. **主键设计** - 主键选择策略

## 📝 学习文档

- [学习指南](docs/Hudi-Learning-Guide.md) - 完整的章节讲解 + 练习 + 测验
- [速查卡片](docs/Hudi-Quick-Reference.md) - 常用配置快速参考

## 🐛 常见问题

<details>
<summary>Hudi JAR 版本不匹配</summary>

确保使用 `hudi-spark3.5-bundle_2.12-1.1.1.jar`，Spark 3.5 需要 Scala 2.12 版本。
</details>

<details>
<summary>增量查询失败</summary>

增量查询主要支持 MOR 表，COW 表需要特殊配置。
</details>

## 📄 License

MIT License

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！
