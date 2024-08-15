# DataProxy

<p align="center">
<a href="./README.zh-CN.md">简体中文</a>｜<a href="./README.md">English</a>
</p>

DataProxy 是一个基于 [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) 的数据服务框架，接入丰富的数据源，提供统一、易用、高效、健壮的数据读写服务。通过 DataProxy：

* 你可以接入丰富的数据源，其中包括 MySQL、S3、Aliyun OSS、本地磁盘等
* 你可以使用统一的接口来实现对不同数据源的读写操作

## Documentation

- [Development](./docs/development/build_dataproxy_cn)

## 声明

非正式发布的 DataProxy 版本仅用于演示，请勿在生产环境中使用。尽管此版本已涵盖 DataProxy 的基础功能，但由于项目存在功能不足和待完善项，可能存在部分安全问题和功能缺陷。因此，我们欢迎你积极提出建议，并期待正式版本的发布。