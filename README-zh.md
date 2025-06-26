# FLINK-CDC-MULTI

## 介绍

该工具用于将数据近乎实时地导入数据湖。

它使用单个 Flink CDC 作业处理数据库中的所有表，并将其保存到 HDFS 或 S3 兼容系统中，格式为 **Parquet**。这不仅节省了资源，也更容易管理和操作。

它不关心源或目标的模式，因为您可以手动创建目标模式，或者使用类似 AWS Glue 的爬虫来为您完成。这省去了为每个表创建模式的大量工作，特别是当表有数百个时。

该工具是自包含的，不需要第三方中间件如 Apache Kafka。这是一个有意的设计选择，以简化操作。这也意味着它依赖于 binlog 保留来从故障中恢复，我相信这对大多数生产系统来说不是问题。

每当表结构发生变化时（例如，当表在 2024/04/01 更改时为 `mytable_v20240401`），它通过创建表的新版本来实现模式演变。这样数据可以在后续层中合并，而不是在原地进行模式演变，这既危险又难以追踪。目前，这是通过使用表映射选项手动完成的。我计划稍后实现自动化。

它旨在自动化大部分工作，包括 binlog 偏移记录、创建表版本、发布作业状态和统计信息等。这是为了鼓励快速适应该解决方案，特别是对于知识和运维资源有限的小团队。

它注释丰富，具有大量日志记录，并有调试模式选项。这是为了方便故障排除和学习。

## 规格

- 支持的数据库
    - MySQL
        - 开发和测试基于 `8.0.38`，但应该也适用于 `5.x`
        - 当收到  语句时，自动停止任务并发送消息
        - 每个作业都会创建一个特殊的 DDL 表（`{source_id}_{db_name}__{db_name}_ddl`）来记录 DDL 供您参考
        - **新增**：MySQL 多数据库支持 - 在单个作业中捕获多个数据库的变更
          - 使用 `source.database.list` 替代 `source.database.name` 来支持多个数据库
          - 使用数据库前缀指定表：`source.table.list: "db1.table1,db1.table2,db2.table3"`
          - 与现有单数据库配置向后兼容
    - MongoDB
        - 开发和测试基于 `3.6`
            - 目前只同步 1 个表，因为 `3.6` 不支持监视数据库或部署
            - 支持时间戳偏移，但在 `3.6` 上不工作，因为此版本不支持此功能
- 支持的平台
    - EMR 6.15.0 / Flink 1.17.1
        - 这是一个有意的选择，以利用更成熟版本的 Flink
        - 只要设置了正确的依赖关系（例如，将 `provided` 改为 `compile`），它将在本地或 K8s 集群中正常运行
- 目标接收端
    - 本地文件系统（用于测试）
    - HDFS
    - Amazon S3
- 接收格式
    - 数据：Parquet，snappy 压缩
    - Binlog 偏移存储：纯文本文件，逗号分隔
- 功能
    - 单个作业同步数据库中的所有或部分表
        - （可能）单个作业同步整个部署
    - 源或目标不需要模式定义
    - 自动记录和恢复每个表的 binlog 偏移，偏移记录在 HDFS 或 Amazon S3 的文件中
    - 用于手动模式演变的表名映射
        - （计划中）在遇到 DDL 时自动表名映射
    - 基于消息时间的自动数据分区
        - （计划中）基于事件时间的数据分区
    - 仅快照模式
    - 快照条件
        - （计划中）仅对数据补充进行部分数据的快照
    - 快照 + CDC 模式
    - 仅 CDC 模式，即从给定的 binlog 偏移开始
    - 调试模式，使用 `--debug` 在测试期间显示详细消息（在生产中关闭，因为这会为每个 binlog 创建多个日志）
    - 用于监控的作业统计表
    - （计划中）从 AWS Secrets Manager、AWS Parameters Store 或其他配置管理器中读取凭据，以提高安全性
    - （计划中）Parquet 压缩，单独的作业
    - （计划中）干运行模式，打印到控制台而不是写入文件
    - （计划中）自动生成目标 `CREATE TABLE` SQL

## 快速开始

- 下载和编译，或使用发布的 JAR
- 启动 Amazon EMR 集群（`emr-6.15.0` 和 `flink 1.17.1`）
- 编辑配置
    - `flink-conf`
        - `s3.endpoint.region` = `cn-northwest-1`（如果您使用的是非全球区域或自定义区域，此配置不会被 AWS 版本的 `flink-s3` 尊重）
        - `containerized.master.env.JAVA_HOME` = `/usr/lib/jvm/jre-11`（我们使用 Java 11）
        - `containerized.taskmanager.env.JAVA_HOME` = `/usr/lib/jvm/jre-11`
        - `env.java.home` = `/usr/lib/jvm/jre-11`
    - `core-site`
        - `fs.s3a.endpoint.region` = `cn-northwest-1`（如果您使用的是非全球区域或自定义区域）
    - 将 `flink-s3` 插件移动到库中并删除 S3 插件目录
        - 即使我们可以在 `plugins` 目录中加载它，但配置不会以这种方式加载，至少在这个版本的 EMR 中仍然需要这样做
        - `sudo mv /usr/lib/flink/plugins/s3/*.jar /usr/lib/flink/lib`
        - `sudo rmdir /usr/lib/flink/plugins/s3`
- 使用 Per-Job 模式运行
    - `flink run -t yarn-per-job -Dyarn.application.name=job-name ./flink-cdc-multi.jar`
- CLI 选项
    - `--config s3://mybucket/myconfig.json`（也可以用于测试的本地文件，或 HDFS）
        - 参见 [example-configs](/src/resources/example-configs) 获取示例
    - `--debug`，将显示所有 DEBUG 和 TRACE 日志（仅由此应用程序发送）
- 通过更改 `pom.xml` 和使用 `compile` 而不是 provided，您应该能够在任何环境中运行它
    - 或者，您也可以从正在运行的 EMR 集群中复制 Flink 和 Hadoop JAR

## 多数据库配置

### 多数据库
使用 `source.database.list` 和逗号分隔的数据库名称：

```json
{
  "source.database.list": "test,production,staging",
  "source.table.list": "test.users,test.orders,production.customers,staging.analytics"
}
```

### 配置示例

#### 来自多个数据库的所有表
```json
{
  "source.database.list": "test,production,staging"
}
```
这将捕获所有三个数据库中的所有表。

#### 来自多个数据库的特定表
```json
{
  "source.database.list": "test,production,staging",
  "source.table.list": "test.users,test.orders,production.customers,production.transactions,staging.analytics"
}
```

#### 数据库名称映射
```json
{
  "source.database.list": "test,production,staging",
  "database.name.map": {
    "test": "test_prod",
    "production": "prod_env",
    "staging": "staging_env"
  }
}
```

#### 表名映射
```json
{
  "source.database.list": "test,production,staging",
  "source.table.list": "test.users,test.orders,production.customers",
  "table.name.map": {
    "test.users": "users_v20240713",
    "test.orders": "orders_v20240713",
    "production.customers": "customers_v20240713"
  }
}
```

### 多数据库重要注意事项

1. **必需配置**：现在所有 MySQL 配置都需要 `source.database.list`
2. **表规范**：所有表必须使用数据库前缀指定：
   - **必需格式**：`"db1.table1,db2.table2"` - 来自特定数据库的特定表
   - **通配符支持**：`"db1.*"` - 来自特定数据库的所有表
3. **数据库名称映射**：每个数据库可以使用 `database.name.map` 映射到不同的目标名称
4. **表名映射**：表名可以使用 `table.name.map` 映射到不同的目标名称。所有表映射键必须包含数据库前缀（例如，`"db1.table1": "new_table_name"`）
5. **DDL 表**：每个数据库都有自己的 DDL 表来跟踪模式变更
6. **性能**：由于复杂性增加，多数据库作业可能需要更多资源
7. **偏移管理**：系统现在支持每数据库偏移跟踪，以便更好地恢复
8. **未知表处理**：默认情况下，未知表会被跳过并发出警告。设置 `fail.on.unknown.tables=true` 在遇到未配置的表时使作业失败

### 表列表示例

#### 来自特定数据库的特定表
```json
{
  "source.database.list": "test,production,staging",
  "source.table.list": "test.users,test.orders,production.customers,staging.analytics"
}
```

#### 来自所有数据库的所有表
```json
{
  "source.database.list": "test,production,staging"
}
```
这将捕获所有三个数据库中的所有表。

#### 混合特定表和通配符表
```json
{
  "source.database.list": "test,production,staging",
  "source.table.list": "test.users,test.orders,production.*,staging.analytics"
}
```
这将捕获：
- 来自 test 数据库的 `users` 和 `orders`
- 来自 production 数据库的所有表
- 来自 staging 数据库的 `analytics`

### 多数据库偏移格式

系统对所有数据库使用单一偏移，因为它们共享同一个二进制日志流。

#### 偏移配置
```json
{
  "source.database.list": "test,production,staging",
  "_offset.value": "mysql-bin.000003,43650"
}
```

此单一偏移适用于配置中的所有数据库。

## 已知问题

- 数据库和表名中的连字符（`-`）将被转换为下划线（`_`）
    - 这是由于 Avro 模式的限制
    - 这也是一个设计决策，因为连字符很容易与减号混淆，而编程语言中不允许使用这样的名称
    - 混合使用连字符和下划线也显得不美观
- 当从 binlog 偏移存储自动恢复操作时，最后的语句会重复
    - Debezium 只会在有事务时捕获第一条语句的结束 binlog 偏移（例如，使用 `BEGIN`）
    - 使用 `ROW` binlog 模式时，此类事务的第一条语句可能是"表映射"
    - 如果我们跳过表映射语句，Debezium 会抱怨缺少表映射
    - 因此对于非 DDL 语句，我们捕获的是开始 binlog 偏移而不是结束的
    - 这导致在最后一个事务中重复语句
    - 下游处理器必须能够在作业重启的情况下处理这些重复记录
- 仅快照模式将在有 CDC 事件时才会停止
    - 目前，连接器在有 CDC 事件之前无法知道快照扫描是否完成
    - （计划中）我们可以添加一个超时，因为快照扫描应该是连续的
- MongoDB
    - 如果您的表定义会变化，建议使用 `doc-string` 模式，将整个文档作为 JSON 字符串存储
        - 在其他模式中，如果检测到表定义变化，会抛出错误，并且会有大量工作（表名映射、偏移等）
    - `_id` 字段的特别处理
        - 如果是 `$oid` 或 `ObjectID` 类型，则其值（一个哈希字符串）将被提取为 `_id` 字段的值
        - 如果是字符串、整数，其字面值将被转换为字符串并用作 `_id` 字段的值
        - 因此，如果 `_id` 不遵循良好实践（例如，混合使用字符串、整数和 ObjectID），则可能会有重复的 `_id`
- 不支持 Application 模式
    - 由于 Application 模式的类加载问题，此应用程序只能通过 Per-Job 模式正确运行
    - 如果您决定使用应用程序模式，请将类加载更改为 `parent-first`，请注意这会破坏 Session 模式

# 最佳实践

- 因尽量提早区分日志表和对象表
  - 日志表记录事件（也叫事实表，比如订单），对象表记录对象（也叫维度表，比如用户、商品）
  - 通常日志表量级很大，并且会持续扩张，而对象表的量级相对较小并且比较固定
  - 在数据库设计有问题时
    - 对象表有可能会相对频繁地进行 DDL，字段增减时，很可能使用 `UPDATE` 对原始数据进行字段默认值填充
    - 这意味着 binlog 会存储该表的所有数据，并完整被下游处理一遍
    - 如无特别需要，我们还不如直接删除整个表从零进行同步
    - 所以我们应该尽量区分日志表和对象表，日志表因为其体量，相对较少进行此类操作
- 






