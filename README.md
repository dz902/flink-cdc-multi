# FLINK-CDC-MULTI

## Quick Start

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