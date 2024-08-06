# FLINK-CDC-MULTI

[中文](./README-zh.md)

## Introduction

This tool is for ingesting data into a Data Lake in near-real-time. 

It uses a single Flink CDC job to process all tables in a database and save them to HDFS or S3-compatible systems as **Parquet**. This not only saves resources, but also easier to manage and operate.

It does not care about source or target schema because you can either create target schema manually or use a crawler like AWS Glue to do it for you. This saves a lot of work on creating schemas for each of the tables, especially when there are hundreds of them.

It is self-contained and does not require 3rd party middleware like Apache Kafka. This is a deliberate design choice to simplify operations. This also means it relies on binlog retention to restore from a failure, which I believe should not be an issue for most production systems.

It does schema evolution by creating new versions of a table every time when a table structure is changed (e.g. `mytable_v20240401` when the table is changed on 2024/04/01). This way data can be consolidated in later layers instead of doing schema evolution in place, which is dangerous and difficult to trace. Currently, this is done manually using a table mapping option. I aim to automate this later.

It aims to automate most work including binlog offset recording, creating versions of tables, posting job status and stats, etc. This is to encourage quick adaption of the solution, especially by small teams with limited knowledge and ops resources.

It is commented fiercely with abundant logging, with a debug mode option. This is for both troubleshooting and learning.

## Specs

- Supported databases
  - MySQL
    - Developed and tested against `8.0.38`, but should also work on `5.x`)
    - When a DDL statement is received, auto stop task with a message
    - A special DDL table (`{source_id}_{db_name}__{db_name}_ddl`) is created for each job to record DDLs for your reference
  - MongoDB
    - Developed and tested against `3.6`
      - Currently only syncs 1 table because `3.6` does not support watching db or deployment
      - Supports timestamp offset but will not work on `3.6` as this version does not support this feature
- Supported platforms
  - EMR 6.15.0 / Flink 1.17.1
    - This is a deliberate choice to make use of more mature versions of Flink
    - It will run fine locally or in a K8s cluster when correct dependencies are set, i.e. changing `provided` to `compile`
- Sink target
  - Local file system (for testing)
  - HDFS
  - Amazon S3
- Sink format
  - Data: Parquet, snappy-compressed
  - Binlog offset store: Plain text file, comma separated
- Features
  - A single job to sync all or some tables in a database
    - (maybe) A single job to sync a whole deployment
  - No schema definition is required for source or target
  - Auto binlog offset recording and restore for each table, with offsets recorded in files on HDFS or Amazon S3
  - Table name mapping for manual schema evolution
    - (planned) Auto table name mapping when a DDL is met
  - Auto data partitioning based on message time
    - (planned) Data partitioning based on event time
  - Snapshot only mode
  - Snapshot conditions
    - (planned) Snapshot only a subset of data for data refilling
  - Snapshot + CDC mode
  - CDC only mode, i.e. starting from given binlog offset
  - Debug mode, using `--debug` to show verbose message during testing (turn off in production as this creates multiple logs for every binlog)
  - Job stats table for monitoring
  - (planned) Reading credentials from AWS Secrets Manager, AWS Parameters Store or other configuration managers for better security
  - (planned) Parquet compaction, a separate job
  - (planned) Dry-run mode, printing to console instead of writing to files
  - (planned) Auto-generate target `CREATE TABLE` SQL

## Quick Start

- Download and compile, or use a release JAR
- Start Amazon EMR cluster (`emr-6.15.0` and `flink 1.17.1`)
- Edit configuration
  - `flink-conf`
    - `s3.endpoint.region` = `cn-northwest-1` (if you are using non-global regions or custom region, somehow this is not respected by AWS version of `flink-s3`)
    - `containerized.master.env.JAVA_HOME` = `/usr/lib/jvm/jre-11` (we use Java 11)
    - `containerized.taskmanager.env.JAVA_HOME` = `/usr/lib/jvm/jre-11`
    - `env.java.home` = `/usr/lib/jvm/jre-11`
  - `core-site`
    - `fs.s3a.endpoint.region` = `cn-northwest-1` (if you are using non-global regions or custom region)
  - Move `flink-s3` plugin to library and delete S3 plugin directory
    - Even if we could load it in the `plugins` directory, the configurations are not loaded this way, still need this at least for this version of EMR 
    - `sudo mv /usr/lib/flink/plugins/s3/*.jar /usr/lib/flink/lib`
    - `sudo rmdir /usr/lib/flink/plugins/s3`
- Run using Per-Job mode
  - `flink run -t yarn-per-job -Dyarn.application.name=job-name ./flink-cdc-multi.jar`
- CLI Options
  - `--config s3://mybucket/myconfig.json` (can also be local for testing, or HDFS)
    - See [example-configs](/src/resources/example-configs) for examples
  - `--debug`, will show all DEBUG and TRACE logs (sent by this app only)
- You should be able to get it running in any environment by changing `pom.xml` and use `compile` instead of provided
  - Or, you can also copy Flink and Hadoop jars from a running EMR cluster

## Known Issues

- Hyphen (`-`) in database and table names will be converted to underscore (`_`)
  - This is due to limitation of Avro schema
  - This is also a design decision because hyphen could easily be confused with a minus and such names are not tolerated in programming languages
  - It also looks ugly to mix hyphen and underscore
- Last statements are duplicated when auto-restoring operation from binlog offset store
  - Debezium will only capture the ending binlog offset of first statement when there is a transaction (e.g. with `BEGIN`)
  - When using `ROW` binlog mode, first statement of such transaction could be "table mapping"
  - If we skip the table mapping statement then Debezium will complain about missing table mapping
  - So for non-DDL statement we capture the starting binlog offset instead the ending one
  - This leads to repeated statements in last transaction
  - Downstream processors must be able to process such duplicate records in case of job restarts
- Snapshot only mode will only stop when there is a CDC event
  - Currently, the connector will not know whether a snapshot scan is completed until there is a CDC event
  - (planned) We could add a timeout because snapshot scan is supposed to be continuous
- MongoDB
  - It is recommended to use `doc-string` mode with which the whole document is stored as a JSON string if your schema is not consistent
    - In other modes, errors will be thrown if schema change is detected, and there will be a lot of work (table name mapping, offset, etc.)
  - The `_id` field is specially treated
    - If it is `$oid` or `ObjectID` type, then its value (a hash string) will be extracted as the value of `_id` field
    - If it is a string or integer its literal value is converted into a string and used as value of `_id` field
      - So, if you are not following good practices of using consistent `_id` (e.g. mixing string, int and ObjectID), you will risk having duplicate `_id`s
- Application Mode is not supported
  - Due to class loading issues with Application Mode, this app will only run properly using Per-Job mode
  - Should you decide to use Application Mode, change class loading to `parent-first`, note this will break Session Mode
