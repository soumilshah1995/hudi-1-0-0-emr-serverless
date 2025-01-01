from pyspark.sql import SparkSession
from pyspark.sql.functions import col

print("CREATING SPARK SESSION")

# Initialize Spark session with Hudi
spark = SparkSession.builder \
    .appName("HudiIndexedTable") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar") \
    .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.0") \
    .getOrCreate()

print("Spark Session is Ready****** ")
print("\n")

# Database and table information
DB_NAME = "hudidb"
TABLE_NAME = "hudi_indexed_table"
TABLE_S3_LOCATION = "s3://<BUCKET>/warehouse/default/hudi_indexed_table"

# Check if the database already exists
existing_databases = [db.name for db in spark.catalog.listDatabases()]
print("existing_databases", existing_databases)

if DB_NAME not in existing_databases:
    # Create the database
    query = f"CREATE DATABASE IF NOT EXISTS {DB_NAME} LOCATION '{TABLE_S3_LOCATION}'"
    spark.sql(query)
    print("Database '{}' created.".format(DB_NAME))

# Create the table using Spark SQL
print("Creating Hudi table...")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DB_NAME}.{TABLE_NAME} (
    ts BIGINT,
    uuid STRING,
    rider STRING,
    driver STRING,
    fare DOUBLE,
    city STRING
) USING HUDI
OPTIONS (
    primaryKey = "uuid",
    hoodie.write.record.merge.mode = "COMMIT_TIME_ORDERING"
)
PARTITIONED BY (city)
""")
print("Hudi table created successfully.")

# Insert data using Spark SQL
print("Inserting data into the Hudi table...")
spark.sql(f"""
INSERT INTO {DB_NAME}.{TABLE_NAME}
VALUES
    (1695159649, '334e26e9-8355-45cc-97c6-c31daf0df330', 'rider-A', 'driver-K', 19.10, 'san_francisco'),
    (1695091554, 'e96c4396-3fad-413a-a942-4cb36106d721', 'rider-C', 'driver-M', 27.70, 'san_francisco'),
    (1695046462, '9909a8b1-2d15-4d3d-8ec9-efc48c536a00', 'rider-D', 'driver-L', 33.90, 'san_francisco'),
    (1695332066, '1dced545-862b-4ceb-8b43-d2a568f6616b', 'rider-E', 'driver-O', 93.50, 'san_francisco'),
    (1695516137, 'e3cf430c-889d-4015-bc98-59bdce1e530c', 'rider-F', 'driver-P', 34.15, 'sao_paulo'),
    (1695376420, '7a84095f-737f-40bc-b62f-6b69664712d2', 'rider-G', 'driver-Q', 43.40, 'sao_paulo'),
    (1695173887, '3eeb61f7-c2b0-4636-99bd-5d7a5a1d2c04', 'rider-I', 'driver-S', 41.06, 'chennai'),
    (1695115999, 'c8abbe79-8d89-47ea-b4ce-4d224bae5bfa', 'rider-J', 'driver-T', 17.85, 'chennai')
""")
print("Data inserted successfully.")

# Enable record index
print("Enabling record index...")
spark.sql("SET hoodie.metadata.record.index.enable = true")
print("Record index enabled.")

# Create record index
print("Creating record index...")
spark.sql(f"CREATE INDEX record_index ON {DB_NAME}.{TABLE_NAME} (uuid)")
print("Record index created.")

# Create secondary index on rider column
print("Creating secondary index on rider column...")
spark.sql(f"CREATE INDEX idx_rider ON {DB_NAME}.{TABLE_NAME} (rider)")
print("Secondary index on rider column created.")

# Show indexes
print("Showing indexes from the Hudi table...")
indexes = spark.sql(f"SHOW INDEXES FROM {DB_NAME}.{TABLE_NAME}")
indexes.show()

# Query using the secondary index with Spark SQL
print("Querying the Hudi table for rider = 'rider-E' using SQL...")
result_sql = spark.sql(f"SELECT * FROM {DB_NAME}.{TABLE_NAME} WHERE rider = 'rider-E'")
result_sql.show()


print("DONE")
