# JAR 文件目录

本目录用于存放 Hudi 和相关依赖的 JAR 文件。

## 需要的 JAR 文件

下载以下文件到本目录：

1. **Hudi Spark Bundle (必须)**
   ```
   hudi-spark3.5-bundle_2.12-1.1.1.jar
   ```

2. **MySQL Connector (Hive Metastore 需要)**
   ```
   mysql-connector-j-8.2.0.jar
   ```

## 下载命令

```bash
# Hudi Spark 3.5 Bundle
wget https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.5-bundle_2.12/1.1.1/hudi-spark3.5-bundle_2.12-1.1.1.jar

# MySQL Connector
wget https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.2.0/mysql-connector-j-8.2.0.jar
```

## 文件大小

- hudi-spark3.5-bundle_2.12-1.1.1.jar: ~80MB
- mysql-connector-j-8.2.0.jar: ~2MB

> 注意：由于 JAR 文件较大，已添加到 .gitignore，不会被提交到 Git。
