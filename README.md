# FLINK-CDC-MULTI

## Quick Start

- Start Amazon EMR cluster (`emr-6.15.0` and `flink 1.17.1`)
- Edit configuration
  - `flink-conf`
    - `s3.endpoint.region` = `cn-northwest-1` (if you are using non-global regions or custom region, somehow this is not respected by AWS version of `flink-s3`)
    - `containerized.master.env.JAVA_HOME` = `/usr/lib/jvm/jre-11` (we use Java 11)
    - `containerized.taskmanager.env.JAVA_HOME` = `/usr/lib/jvm/jre-11`
    - `env.java.home` = `/usr/lib/jvm/jre-11`
    - `classloader.resolve-order` = `parent-first` (there are multiple versions of some libs, without this Flink classloader will clash with Java classloader)
  - `core-site`
    - `fs.s3a.endpoint.region` = `cn-northwest-1` (if you are using non-global regions or custom region)