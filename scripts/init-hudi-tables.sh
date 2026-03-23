#!/bin/bash
# 创建Hudi测试表

echo "等待所有服务启动..."
sleep 30

# 检查HDFS是否可用
if ! hdfs dfs -ls / 2>/dev/null; then
  echo "错误：HDFS未就绪"
  exit 1
fi

# 创建Hudi表配置文件
cat > /tmp/hudi-config.properties << EOF
hoodie.table.name=stock_ticks_cow
hoodie.datasource.write.recordkey.field=symbol
hoodie.datasource.write.partitionpath.field=dt
hoodie.datasource.write.precombine.field=ts
hoodie.datasource.write.table.type=COPY_ON_WRITE
hoodie.datasource.write.operation=upsert
hoodie.datasource.hive_sync.enable=true
hoodie.datasource.hive_sync.database=default
hoodie.datasource.hive_sync.table=stock_ticks_cow
hoodie.datasource.hive_sync.mode=hms
hoodie.datasource.hive_sync.partition_extractor_class=org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor
hoodie.datasource.hive_sync.partition_fields=dt
hoodie.datasource.hive_sync.partition_value_extractor_class=org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor
EOF

echo "Hudi表配置已创建：/tmp/hudi-config.properties"
echo ""
echo "可以通过以下方式创建Hudi表："
echo "1. 使用Spark SQL："
echo "   CREATE TABLE stock_ticks_cow ("
echo "     symbol STRING,"
echo "     ts BIGINT,"
echo "     volume DOUBLE,"
echo "     open DOUBLE,"
echo "     close DOUBLE,"
echo "     dt STRING"
echo "   ) USING hudi"
echo "   LOCATION 'hdfs://namenode:9000/user/hive/warehouse/stock_ticks_cow'"
echo ""
echo "2. 使用Flink SQL："
echo "   CREATE TABLE stock_ticks_cow ("
echo "     symbol STRING,"
echo "     ts BIGINT,"
echo "     volume DOUBLE,"
echo "     open DOUBLE,"
echo "     close DOUBLE,"
echo "     dt STRING"
echo "   ) PARTITIONED BY (dt) WITH ("
echo "     'connector' = 'hudi',"
echo "     'path' = 'hdfs://namenode:9000/user/hive/warehouse/stock_ticks_cow',"
echo "     'table.type' = 'COPY_ON_WRITE'"
echo "   )"
echo ""
echo "表配置已准备就绪！"