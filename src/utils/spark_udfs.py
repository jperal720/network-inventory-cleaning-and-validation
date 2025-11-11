from pyspark.sql import types as T
from pyspark.sql import functions as F
from InventoryTransformations import InventoryTransformations

def get_ipv4_udf(it: InventoryTransformations, ipv4_schema: T.StructType) -> F.udf:
    return F.udf(lambda x: it.ipv4_validate_and_normalize(x), ipv4_schema)

def get_ipv4_type_udf(it: InventoryTransformations) -> F.udf:
    return F.udf(lambda x: it.classify_ipv4_type(x), T.StringType())

def get_default_subnet_udf(it: InventoryTransformations) -> F.udf:
    return F.udf(lambda ip, ip_type: it.default_subnet(ip, ip_type), T.StringType())

def get_hostname_udf(it: InventoryTransformations, hostname_schema: T.StructType) -> F.udf:
    return F.udf(lambda x: it.validate_hostname(x), hostname_schema)

def get_site_udf(it: InventoryTransformations, site_schema: T.StructType) -> F.udf:
    return F.udf(lambda x: it.normalize_site(x), site_schema)

def get_reverse_ptr_udf(it: InventoryTransformations) -> F.udf:
    return F.udf(lambda x: it.generate_reverse_ptr(x), T.StringType())

def get_fqdn_udf(it: InventoryTransformations, fqdn_schema: T.StructType) -> F.udf:
    return F.udf(lambda fqdn, hostname, site: it.validate_fqdn(fqdn, hostname, site), fqdn_schema)

def get_mac_udf(it: InventoryTransformations, mac_schema: T.StructType) -> F.udf:
    return F.udf(lambda x: it.validate_mac(x), mac_schema)

def get_owner_udf(it: InventoryTransformations, owner_schema: T.StructType) -> F.udf:
    return F.udf(lambda x: it.parse_owner(x), owner_schema)

def get_device_type_udf(it: InventoryTransformations, device_type_schema: T.StructType) -> F.udf:
    return F.udf(lambda x: it.normalize_device_type(x), device_type_schema)