-- 创建Hudi COW表
CREATE TABLE hudi_table (
    uuid VARCHAR(40) PRIMARY KEY NOT ENFORCED,
    rider VARCHAR(20),
    driver VARCHAR(20),
    fare DOUBLE,
    city VARCHAR(20),
    ts BIGINT
) PARTITIONED BY (city) WITH (
    'connector' = 'hudi',
    'path' = 'hdfs://namenode:9000/user/hudi/warehouse/hudi_table',
    'table.type' = 'COPY_ON_WRITE',
    'write.operation' = 'upsert',
    'hoodie.table.name' = 'hudi_table',
    'hoodie.datasource.write.recordkey.field' = 'uuid',
    'hoodie.datasource.write.partitionpath.field' = 'city',
    'hoodie.datasource.write.precombine.field' = 'ts'
);

-- 插入一些测试数据
INSERT INTO hudi_table VALUES
    ('uuid1', 'rider1', 'driver1', 10.5, 'beijing', 1695046462179),
    ('uuid2', 'rider2', 'driver2', 20.3, 'shanghai', 1695046462180),
    ('uuid3', 'rider3', 'driver3', 15.8, 'beijing', 1695046462181),
    ('uuid4', 'rider4', 'driver4', 25.2, 'shanghai', 1695046462182);

-- 查询数据
SELECT * FROM hudi_table;

-- 更新数据（模拟upsert）
INSERT INTO hudi_table VALUES
    ('uuid1', 'rider1_updated', 'driver1', 12.5, 'beijing', 1695046462190);

-- 再次查询，查看更新结果
SELECT * FROM hudi_table;