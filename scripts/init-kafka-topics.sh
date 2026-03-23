#!/bin/bash
# 创建Kafka主题

KAFKA_BROKER="kafka-broker:9092"

echo "等待Kafka启动..."
MAX_WAIT=120
WAITED=0
until kafka-broker-api-versions --bootstrap-server $KAFKA_BROKER 2>/dev/null; do
  if [ $WAITED -ge $MAX_WAIT ]; then
    echo "超时：Kafka未能在 $MAX_WAIT 秒内启动"
    exit 1
  fi
  echo "Kafka未就绪，等待中... ($WAITED/$MAX_WAIT)"
  sleep 5
  WAITED=$((WAITED + 5))
done

echo "创建Hudi测试主题..."
kafka-topics --create --topic hudi-source-topic --bootstrap-server $KAFKA_BROKER --partitions 2 --replication-factor 1 --if-not-exists
kafka-topics --create --topic hudi-sink-topic --bootstrap-server $KAFKA_BROKER --partitions 2 --replication-factor 1 --if-not-exists
kafka-topics --create --topic stock-ticks --bootstrap-server $KAFKA_BROKER --partitions 2 --replication-factor 1 --if-not-exists

echo "列出所有主题："
kafka-topics --list --bootstrap-server $KAFKA_BROKER

echo "Kafka主题创建完成！"