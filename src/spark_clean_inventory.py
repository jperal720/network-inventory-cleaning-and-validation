import os
import sys
import shutil
import subprocess
from datetime import datetime as dt

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

UTILS_PATH = os.path.join(os.path.dirname(__file__), '..', 'src', 'utils')
sys.path.append(UTILS_PATH)

DRIVER_HOST = "host.docker.internal"
DRIVER_PORT = "4042"
BLOCK_MANAGER_PORT = "4043"

AWS_BUNDLE = "com.amazonaws:aws-java-sdk-bundle:1.12.262"
HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.3.4"

from utils.InventoryTransformations import InventoryTransformations
from utils.spark_transformations import *
from utils.spark_schemas import *
from utils.spark_udfs import *

def main():

    spark = (
        SparkSession.builder
        .appName("data_cleansing")
        .master("spark://localhost:7077")          # driver the notebook
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.host", DRIVER_HOST)
        .config("spark.driver.port", DRIVER_PORT)
        .config("spark.blockManager.port", BLOCK_MANAGER_PORT)
        # Including dependencies
        .config("spark.submit.pyFiles", f"{UTILS_PATH}/InventoryTransformations.py")
        # ↓ Let Spark fetch & ship jars to executors
        .config("spark.jars.packages", f"{HADOOP_AWS},{AWS_BUNDLE}")
        # MINIO/S3A
        .config("spark.hadoop.fs.s3a.endpoint", "http://host.docker.internal:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Explicit creds provider (so the access/secret keys below are actually used)
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        # Failure settings
        .config("spark.hadoop.fs.s3a.connection.timeout", "10000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        .config("spark.hadoop.fs.s3a.socket.timeout", "30000")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
        .config("spark.hadoop.fs.s3a.retry.limit", "2")
        .getOrCreate()
    )

    print(spark.version, spark.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion())

    hadoop_config = spark._jsc.hadoopConfiguration()
    print(hadoop_config.get("fs.s3a.endpoint"))
    print(hadoop_config.get("fs.s3a.aws.credentials.provider"))
    print(hadoop_config.get("fs.s3a.path.style.access"))
    print(hadoop_config.get("fs.s3a.connection.ssl.enabled"))

    jvm = spark.sparkContext._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI("s3a://inventory/"), hadoop_config)
    print(fs)
    for s in fs.listStatus(jvm.org.apache.hadoop.fs.Path("s3a://inventory/")):
        print("->", s.getPath().toString())

    # Loading DataFrame
    df_raw = spark.read.csv("s3a://inventory/inventory_raw.csv", header=True, inferSchema=True)

    # Setting schemas
    traceability_schema = get_traceability_schema()
    ipv4_schema = get_ipv4_schema(traceability_schema)
    hostname_schema = get_hostname_schema(traceability_schema)
    site_schema = get_site_schema(traceability_schema)
    fqdn_schema = get_fqdn_schema(traceability_schema)
    mac_schema = get_mac_schema(traceability_schema)
    owner_schema = get_owner_schema(traceability_schema)
    device_type_schema = get_device_type_schema(traceability_schema)

    # Setting UDFs
    it = InventoryTransformations()
    ipv4_udf = get_ipv4_udf(it, ipv4_schema)
    ipv4_type_udf = get_ipv4_type_udf(it)
    default_subnet_udf = get_default_subnet_udf(it)
    hostname_udf = get_hostname_udf(it, hostname_schema)
    site_udf = get_site_udf(it, site_schema)
    reverse_ptr_udf = get_reverse_ptr_udf(it)
    fqdn_udf = get_fqdn_udf(it, fqdn_schema)
    mac_udf = get_mac_udf(it, mac_schema)
    owner_udf = get_owner_udf(it, owner_schema)
    device_type_udf = get_device_type_udf(it, device_type_schema)

    # Transforming DataFrame
    df = df_raw

    df = ipv4_transform(df, ipv4_udf)
    df = ip_type_transform(df, ipv4_type_udf)
    df = subnet_cidr_transform(df, default_subnet_udf)
    df = hostname_transform(df, hostname_udf)
    df = site_transform(df, site_udf)
    df = reverse_ptr_transform(df, reverse_ptr_udf)
    df = fqdn_transform(df, fqdn_udf)
    df = mac_transform(df, mac_udf)
    df = owner_transform(df, owner_udf)
    df = device_type_transform(df, device_type_udf)
    df = normalization_steps_transform(df)
    df_final = df_final_transform(df)

    # Upload to minIO
    MINIO_PATH = os.path.join("s3a://", "inventory", "tmp", "01-ingest-and-transform-inventory")
    FILE_NAME = out_path = os.path.join(MINIO_PATH, f"{dt.today().year}-{dt.today().day}-{dt.today().month}_inventory_tmp")

    (df_final
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(FILE_NAME))
    
    return FILE_NAME

if __name__ == '__main__':
    main()