# DataProxy

<p align="center">
<a href="./README.zh-CN.md">简体中文</a>｜<a href="./README.md">English</a>
</p>

DataProxy is a data service framework based on [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) that
accesses rich data sources and provides unified, easy-to-use, efficient, and robust data reading and writing services.
With DataProxy:

* You can access various types of data sources, including MySQL, Hive, S3, Aliyun OSS, local disk, etc.
* You can use a consistent read/write interface to realize read/write operations on different data sources.

## Documentation

Currently, we only provide detailed documentations in Chinese.

- [Development](./docs/development/build_dataproxy_cn.md)

## Disclaimer

Non-release version of DataProxy is only for demonstration and should not be used in production environments.
Although this version of DataProxy covers the basic abilities, there may be some security issues and functional defects
due to insufficient functionality and unfinished items in the project.
We welcome your active suggestions and look forward to the official release.



# hive 分区


```shell
CREATE TABLE sales (
  item_id INT,
  amount DOUBLE
)
PARTITIONED BY (sale_date STRING);

# 对应的文件 /user/hive/warehouse/sales/sale_date=2024-07-01/part-00000.parquet

# 静态分区

INSERT INTO TABLE sales PARTITION (sale_date='2024-07-01')
VALUES (101, 88.0);


#  动态分区 需要配置hive自动分区
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT INTO TABLE sales
SELECT item_id, amount, sale_date FROM staging_sales;   #正常的insert语句

# 动态分区会自动分区
```

