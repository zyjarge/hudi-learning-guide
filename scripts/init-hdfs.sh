#!/bin/bash
# 初始化HDFS目录结构

echo "等待HDFS NameNode启动..."
MAX_WAIT=120
WAITED=0
until hdfs dfs -ls / 2>/dev/null; do
  if [ $WAITED -ge $MAX_WAIT ]; then
    echo "超时：HDFS未能在 $MAX_WAIT 秒内启动"
    exit 1
  fi
  echo "HDFS未就绪，等待中... ($WAITED/$MAX_WAIT)"
  sleep 5
  WAITED=$((WAITED + 5))
done

echo "创建Hudi相关目录..."
hdfs dfs -mkdir -p /user/hive/warehouse
hdfs dfs -mkdir -p /user/hudi/warehouse
hdfs dfs -mkdir -p /var/log/hive
hdfs dfs -chmod -R 777 /user/hive/warehouse
hdfs dfs -chmod -R 777 /user/hudi/warehouse

echo "创建Kafka相关目录..."
hdfs dfs -mkdir -p /var/kafka-logs

echo "创建测试目录..."
hdfs dfs -mkdir -p /user/hudi/test
hdfs dfs -chmod -R 777 /user/hudi/test

echo "HDFS初始化完成！"
echo "HDFS目录结构："
hdfs dfs -ls -R /user