from pyspark.sql import functions as F
from pyspark.sql import types as T

def get_traceability_schema() -> T.StructType:
    return T.StructType([
        T.StructField("field", T.StringType(), True),
        T.StructField("from", T.StringType(), True),
        T.StructField("to", T.StringType(), True),
        T.StructField("reason", T.StringType(), True)
    ])

def get_ipv4_schema(traceability_schema: T.StructType) -> T.StructType:
    return T.StructType([
        T.StructField("ip_valid", T.BooleanType(), True),
        T.StructField("ip_canonical", T.StringType(), True),
        T.StructField("tr_metadata", traceability_schema, True)
    ])

def get_hostname_schema(traceability_schema: T.StructType) -> T.StructType:
    return T.StructType([
        T.StructField("hostname_valid", T.BooleanType(), True), 
        T.StructField("hostname_canonical", T.StringType(), True),
        T.StructField("tr_metadata", traceability_schema, True)
    ])

def get_site_schema(traceability_schema: T.StructType) -> T.StructType:
    return T.StructType([
        T.StructField("site_normalized", T.StringType(), True),
        T.StructField("tr_metadata", traceability_schema, True)
    ])

def get_fqdn_schema(traceability_schema: T.StructType) -> T.StructType:
    return T.StructType([
        T.StructField("fqdn_valid", T.BooleanType(), True),
        T.StructField("fqdn_canonical", T.StringType(), True),
        T.StructField("tr_metadata", traceability_schema, True),
        T.StructField("fqdn_consistent", T.StringType(), True)
    ])

def get_mac_schema(traceability_schema: T.StructType) -> T.StructType:
    return T.StructType([
        T.StructField("mac_valid", T.BooleanType(), True),
        T.StructField("mac_canonical", T.StringType(), True),
        T.StructField("tr_metadata", traceability_schema, True)
    ])

def get_owner_schema(traceability_schema: T.StructType) -> T.StructType:
    return T.StructType([
        T.StructField("owner", T.StringType(), True),
        T.StructField("owner_email", T.StringType(), True),
        T.StructField("owner_team", T.StringType(), True),
        T.StructField("tr_metadata", traceability_schema, True)
    ])

def get_device_type_schema(traceability_schema: T.StructType) -> T.StructType:
    return T.StructType([
        T.StructField("device_type", T.StringType(), True),
        T.StructField("device_type_confidence", T.IntegerType(), True),
        T.StructField("tr_metadata", traceability_schema, True),
    ])