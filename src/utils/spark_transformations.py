from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

def ipv4_transform(df: DataFrame, ipv4_udf: F.udf) -> DataFrame:
    return (df.withColumn("ipv4", ipv4_udf(F.col("ip")))
      .withColumn("ip_valid", F.col("ipv4.ip_valid"))
      .withColumn("ip_canonical", F.col("ipv4.ip_canonical"))
      .withColumn("ip_tr_metadata", F.col("ipv4.tr_metadata"))
      .drop("ipv4"))

def ip_type_transform(df: DataFrame, ipv4_type_udf: F.udf) -> DataFrame:
    return df.withColumn("ip_type", ipv4_type_udf(F.col("ip_canonical")))

def subnet_cidr_transform(df: DataFrame, default_subnet_udf: F.udf) -> DataFrame:
    return df.withColumn("subnet_cidr", F.when(F.col("ip_valid") == True, default_subnet_udf(F.col('ip_canonical'), F.col("ip_type"))).otherwise(F.lit(None)))

def hostname_transform(df: DataFrame, hostname_udf: F.udf) -> DataFrame:
    return (
        df.withColumn("hr", hostname_udf(F.col("hostname")))
            .withColumn("hostname_valid", F.col("hr.hostname_valid"))
            .withColumn("hostname_canonical", F.col("hr.hostname_canonical"))
            .withColumn("hostname_tr_metadata", F.col("hr.tr_metadata"))
            .drop("hr")
    )

def site_transform(df: DataFrame, site_udf: F.udf) -> DataFrame:
    return (
        df.withColumn("site_res", site_udf(F.col("site")))
            .withColumn("site_normalized", F.col("site_res.site_normalized"))
            .withColumn("site_tr_metadata", F.col("site_res.tr_metadata"))
            .drop("site_res")
    )

def reverse_ptr_transform(df: DataFrame, reverse_ptr_udf: F.udf) -> DataFrame:
    return df.withColumn("reverse_ptr", reverse_ptr_udf(F.col("ip_canonical")))

def fqdn_transform(df: DataFrame, fqdn_udf: F.udf) -> DataFrame:
    return (
        df.withColumn("fqdn_res", fqdn_udf(F.col('fqdn'), F.col("hostname_canonical"), F.col("site_normalized")))
            .withColumn("fqdn_valid", F.col("fqdn_res.fqdn_valid"))
            .withColumn("fqdn_canonical", F.col("fqdn_res.fqdn_canonical"))
            .withColumn("fqdn_tr_metadata", F.col("fqdn_res.tr_metadata"))
            .withColumn("fqdn_consistent", F.col("fqdn_res.fqdn_consistent"))
            .drop("fqdn_res")
    )

def mac_transform(df: DataFrame, mac_udf: F.udf) -> DataFrame:
    return (
        df.withColumn("mac_res", mac_udf(F.col("mac")))
            .withColumn("mac_valid", F.col("mac_res.mac_valid"))
            .withColumn("mac_canonical", F.col("mac_res.mac_canonical"))
            .withColumn("mac_tr_metadata", F.col("mac_res.tr_metadata"))
            .drop("mac_res")
    )

def owner_transform(df: DataFrame, owner_udf: F.udf) -> DataFrame:
    return (
        df.withColumn("owner_res", owner_udf(F.col("owner")))
            .withColumn("owner_normalized", F.col("owner_res.owner"))
            .withColumn("owner_email", F.col("owner_res.owner_email"))
            .withColumn("owner_team", F.col('owner_res.owner_team'))
            .withColumn("owner_tr_metadata", F.col("owner_res.tr_metadata"))
            .drop("owner_res")
    )

def device_type_transform(df: DataFrame, device_type_udf: F.udf) -> DataFrame:
    return (
        df.withColumn("dt_res", device_type_udf(F.col("device_type")))
            .withColumn("device_type_normalized", F.col("dt_res.device_type"))
            .withColumn("device_type_confidence", F.col("dt_res.device_type_confidence"))
            .withColumn("device_type_tr_metadata", F.col("dt_res.tr_metadata"))
            .drop("dt_res")
    )

def normalization_steps_transform(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "normalization_steps",
        F.array(
            "ip_tr_metadata",
            "hostname_tr_metadata",
            "fqdn_tr_metadata",
            "mac_tr_metadata",
            "owner_tr_metadata",
            "device_type_tr_metadata",
            "site_tr_metadata"
        )
    )

    return df.withColumn("normalization_steps", F.to_json(F.col("normalization_steps")))

def df_final_transform(df: DataFrame):
    return df.select(
        F.col("ip_canonical").alias("ip"),
        F.col("ip_valid"),
        F.col("ip_type"),
        F.col("subnet_cidr"),
        F.col("hostname_canonical").alias("hostname"),
        F.col("hostname_valid"),
        F.col("fqdn_canonical").alias("fqdn"),
        F.col("fqdn_consistent"),
        F.col("reverse_ptr"),
        F.col("mac_canonical").alias("mac"),
        F.col("mac_valid"),
        F.col("owner_normalized").alias("owner"),
        F.col("owner_email"),
        F.col("owner_team"),
        F.col("device_type_normalized").alias("device_type"),
        F.col("device_type_confidence"),
        F.col("site"),
        F.col("site_normalized"),
        F.col("source_row_id"),
        F.col("normalization_steps"),
    )

