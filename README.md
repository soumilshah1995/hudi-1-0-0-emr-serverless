# hudi-1-0-0-emr-serverless
hudi-1-0-0-emr-serverless

# Steps 
```

export APPLICATION_ID="*****"
export BUCKET_HUDI="*****"
export IAM_ROLE="arn:aws:iam::************:role/*****"

# COPY JOB
aws s3 rm s3://*****/jobs/hudi_job.py
aws s3 cp /Users/*****/IdeaProjects/SparkProject/hudi-emr-1-0-0/hudi_job.py  s3://*****/jobs/hudi_job.py

# COPY JAR
curl -O https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.5-bundle_2.12/1.0.0/hudi-spark3.5-bundle_2.12-1.0.0.jar
aws s3 cp hudi-spark3.5-bundle_2.12-1.0.0.jar s3://*****/jar/hudi-spark3.5-bundle_2.12-1.0.0.jar

# ===========================================
# EMR
# ===========================================
aws emr-serverless start-job-run \
    --application-id $APPLICATION_ID \
    --name "HudiJobRun" \
    --execution-role-arn $IAM_ROLE \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://*****/jobs/hudi_job.py",
            "sparkSubmitParameters": "--jars s3://*****/jar/hudi-spark3.5-bundle_2.12-1.0.0.jar --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
        }
    }' \
    --configuration-overrides '{
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {
                "logUri": "s3://'$BUCKET_HUDI'/logs/"
            }
        }
    }'

```
