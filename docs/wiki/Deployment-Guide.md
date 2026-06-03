# Deployment Guide

SparkLens attaches as a `SparkListener` — zero cluster-side setup required. The only requirement is that the SparkLens JAR is on the **driver classpath**.

---

## YARN (client mode)

The driver runs on the submitting machine. The report is written to whatever path you configure. YARN captures driver stdout automatically.

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --packages io.github.nidhal-saadaoui:spark-lens_2.12:1.3.0 \
  --conf spark.extraListeners=com.github.saadaouini.sparklens.SparkLensListener \
  --conf spark.sparklens.output=text,json \
  --conf spark.sparklens.report.path=/tmp/spark-lens/{app_id} \
  myJob.jar
```

For HDFS output (so the report survives after the client machine exits):
```bash
  --conf spark.sparklens.report.path=hdfs:///user/${USER}/reports/{app_name}/{date}/{app_id}
```

---

## YARN (cluster mode)

The driver runs inside a YARN ApplicationMaster container. Use HDFS or S3 for the report path — local filesystem paths on the container are discarded when the container exits.

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --jars s3://my-bucket/jars/spark-lens_2.12-1.3.0-assembly.jar \
  --conf spark.extraListeners=com.github.saadaouini.sparklens.SparkLensListener \
  --conf spark.sparklens.output=text,json \
  --conf spark.sparklens.report.path=hdfs:///reports/{app_id} \
  myJob.jar
```

> **Important:** In cluster mode the driver runs on a remote node. A local filesystem path (`/tmp/…`) will write to the container's ephemeral storage and be lost. Always use HDFS/S3 in cluster mode.

---

## Kubernetes

The driver runs in a pod. Use an object-store path (S3, GCS, ADLS) or a persistent volume mount for the report.

```bash
spark-submit \
  --master k8s://https://<cluster-endpoint> \
  --deploy-mode cluster \
  --conf spark.kubernetes.driverEnv.EXTRA_JARS=s3://bucket/jars/spark-lens_2.12-1.3.0-assembly.jar \
  --conf spark.extraListeners=com.github.saadaouini.sparklens.SparkLensListener \
  --conf spark.sparklens.output=json \
  --conf spark.sparklens.report.path=s3a://my-bucket/reports/{app_id}.json \
  myJob.jar
```

Or mount the JAR via an init container and reference via `--driver-class-path`.

---

## Databricks

### Attach via cluster init script or cluster libraries

1. Upload the fat assembly JAR to DBFS:
   ```
   dbfs cp spark-lens_2.12-1.3.0-assembly.jar dbfs:/FileStore/jars/
   ```

2. In the Databricks cluster configuration, under **Spark** → **Spark config**, add:
   ```
   spark.extraListeners com.github.saadaouini.sparklens.SparkLensListener
   spark.sparklens.output json
   spark.sparklens.report.path dbfs:/reports/{app_name}/{date}/{app_id}.json
   ```

3. Under **Libraries**, add the JAR from DBFS.

### Writing to Unity Catalog volumes

```
spark.sparklens.report.path /Volumes/my_catalog/my_schema/reports/{app_id}
```

### Notes for Databricks

- Databricks wraps the application in a managed shutdown sequence. The `log` format without a path falls back to `System.err` if log4j2 has already started shutting down — both stdout and stderr are captured in the driver log.
- The `{app_id}` token resolves to the Spark application ID (e.g., `application_1748959200_0001`), not the Databricks run ID.
- For `fail.on` to abort a Databricks job, set it and handle the resulting `RuntimeException` in your notebook or job runner.

---

## Amazon EMR

### EMR on EC2

```bash
aws emr add-steps --cluster-id j-XXXX --steps Type=Spark,Name="My Job",\
  ActionOnFailure=CONTINUE,Args=[\
    --conf,spark.extraListeners=com.github.saadaouini.sparklens.SparkLensListener,\
    --conf,spark.sparklens.output=text,\
    --conf,"spark.sparklens.report.path=s3://my-bucket/reports/{app_id}",\
    --jars,s3://my-bucket/jars/spark-lens_2.12-1.3.0-assembly.jar,\
    --class,com.example.MyJob,\
    s3://my-bucket/myJob.jar\
  ]
```

For persistent configuration, add to the EMR cluster Spark configuration:
```json
[{
  "Classification": "spark-defaults",
  "Properties": {
    "spark.extraListeners": "com.github.saadaouini.sparklens.SparkLensListener",
    "spark.sparklens.output": "json",
    "spark.sparklens.report.path": "s3://my-bucket/reports/{app_id}.json"
  }
}]
```

### EMR Serverless

Place the JAR in S3 and reference it via `--jars`:
```
--jars s3://my-bucket/jars/spark-lens_2.12-1.3.0-assembly.jar
```
Write the report to S3 — EMR Serverless containers have no persistent local filesystem.

---

## Local mode

For testing and development. The report writes to the local filesystem.

```bash
spark-submit \
  --master "local[*]" \
  --driver-class-path spark-lens_2.12-1.3.0-assembly.jar \
  --conf spark.extraListeners=com.github.saadaouini.sparklens.SparkLensListener \
  --conf spark.sparklens.output=text \
  --conf spark.sparklens.report.path=/tmp/report.txt \
  myJob.jar
```

---

## Multiple output formats

Use a comma-separated list for `spark.sparklens.output`. Each format gets its own path:

```
--conf spark.sparklens.output=text,json,html
--conf spark.sparklens.report.path=hdfs:///reports/{app_id}
# → writes: report.txt, report.json, report.html
```

Or set format-specific paths for full control:
```
--conf spark.sparklens.output=text,json
--conf spark.sparklens.report.path.text=hdfs:///reports/{app_id}.txt
--conf spark.sparklens.report.path.json=s3a://bucket/reports/{app_id}.json
```

---

## Using `log` format for inline driver log output

The `log` format without a path automatically routes through the driver's logging framework:

```
--conf spark.sparklens.output=log
# No report.path needed — lines appear in driver log via SLF4J
```

With all configured appenders (Splunk, Datadog, CloudWatch) also receiving the lines. Combine with a path for both a log-aggregation feed and a file archive:

```
--conf spark.sparklens.output=log,json
--conf spark.sparklens.report.path.log=hdfs:///reports/{app_id}.log
--conf spark.sparklens.report.path.json=hdfs:///reports/{app_id}.json
```

---

## CI/CD: fail the job on critical issues

```
--conf spark.sparklens.output=text
--conf spark.sparklens.report.path=/tmp/report-{app_id}.txt
--conf spark.sparklens.fail.on=critical
```

If any Critical issues are found, SparkLens throws a `RuntimeException` at application end. The exit code is non-zero, which causes most CI systems to mark the run as failed. The full report is still written to the configured path.
