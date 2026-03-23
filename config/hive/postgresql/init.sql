-- 初始化Hive Metastore数据库
CREATE DATABASE metastore;
CREATE USER hive WITH PASSWORD 'hive';
GRANT ALL PRIVILEGES ON DATABASE metastore TO hive;

-- 切换到metastore数据库
\c metastore

-- 创建必要的扩展
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";