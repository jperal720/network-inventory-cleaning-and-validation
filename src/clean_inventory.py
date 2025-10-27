import os
import sys
import pandas as pd

from datetime import datetime as dt

UTILS_PATH = os.path.join(os.path.dirname(__file__), '..', 'utils')
sys.path.append(UTILS_PATH)

from utils.InventoryTransformations import InventoryTransformations

def get_df_final(df_ipv4, df_hostname, df_fqdn, df_mac, df_owner, df_dt, df_raw, df_site, df_normalization_steps):
    df_final = pd.DataFrame()
    # IP 
    df_final['ip'] = df_ipv4['ip_canonical']
    df_final['ip_valid'] = df_ipv4['ip_valid']
    df_final['ip_type'] = df_ipv4['ip_type']
    df_final['subnet_cidr'] = df_ipv4['subnet_cidr']

    # Hostname
    df_final['hostname'] = df_hostname['hostname_canonical']
    df_final['hostname_valid'] = df_hostname['hostname_valid']

    # FQDN
    df_final['fqdn'] = df_fqdn['fqdn_canonical']
    df_final['fqdn_consistent'] = df_fqdn['fqdn_consistent']
    df_final['reverse_ptr'] = df_fqdn['reverse_ptr']

    # MAC
    df_final['mac'] = df_mac['mac_canonical']
    df_final['mac_valid'] = df_mac['mac_valid']

    # OWNER
    df_final['owner'] = df_owner['owner']
    df_final['owner_email'] = df_owner['owner_email']
    df_final['owner_team'] = df_owner['owner_team']

    # DEVICE_TYPE
    df_final['device_type'] = df_dt['device_type']
    df_final['device_type_confidence'] = df_dt['device_type_confidence']

    # SITE
    df_final['site'] = df_raw['site']
    df_final['site_normalized'] = df_site['site_normalized']

    # NORMALIZATION_STEPS
    df_final['source_row_id'] = df_normalization_steps['source_row_id']
    df_final['normalization_steps'] = df_normalization_steps['normalization_steps']
    
    return df_final

def transform_inventory(csv_path: str):
    if csv_path is None or csv_path == "":
        raise ValueError("Missing inventory_raw.csv as arg")
    try:
        df_raw = pd.read_csv(csv_path)
    except ValueError as e:
        raise e
    
    it = InventoryTransformations()

    # IP TRANSFORMATIONS
    df_ipv4 = pd.DataFrame()
    df_ipv4[['ip_valid', 'ip_canonical', 'tr_metadata']] = df_raw['ip'].apply(
        lambda x: pd.Series(it.ipv4_validate_and_normalize(x))
    )
    df_ipv4['ip_type'] = df_ipv4['ip_canonical'].apply(
        lambda x: pd.Series(it.classify_ipv4_type(x))
    )
    df_ipv4['subnet_cidr'] = df_ipv4.apply(
        lambda x: it.default_subnet(x['ip_canonical'], x['ip_type'])
        if x['ip_valid'] else "",
        axis=1
    )

    # HOSTNAME TRANSFORMATIONS
    df_hostname = pd.DataFrame()
    df_hostname['hostname'] = df_raw['hostname']
    df_hostname[['hostname_valid', 'hostname_canonical', 'tr_metadata']] = df_hostname['hostname'].apply(
        lambda x: pd.Series(it.validate_hostname(x))
    )

    # SITE TRANSFORMATIONS
    df_site = pd.DataFrame()

    df_site['original_site'] = df_raw['site']
    df_site[['site_normalized', 'tr_metadata']] = df_raw['site'].apply(
        lambda x: pd.Series(it.normalize_site(x))
    ).apply(pd.Series)

    # FQDN TRANSFORMATIONS
    df_fqdn = pd.DataFrame()
    df_fqdn['fqdn'] = df_raw['fqdn']
    df_fqdn['hostname_canonical'] = df_hostname['hostname_canonical']
    df_fqdn['site'] = df_site['site_normalized']
    df_fqdn[['reverse_ptr']] = df_ipv4['ip_canonical'].apply(
        lambda x: pd.Series(it.generate_reverse_ptr(x))
    )
    df_fqdn[['fqdn_valid', 'fqdn_canonical', 'tr_metadata', 'fqdn_consistent']] = (
        df_fqdn.apply(
            lambda x: it.validate_fqdn(x['fqdn'], x['hostname_canonical'], x['site']),
            axis='columns'
        ).apply(pd.Series)
    )

    # MAC TRANSFORMATIONS
    df_mac = pd.DataFrame()
    df_mac['mac'] = df_raw['mac']
    df_mac[['mac_valid', 'mac_canonical', 'tr_metadata']] = df_mac['mac'].apply(
        lambda x: pd.Series(it.validate_mac(x))
    )

    # OWNER PARSING
    # TODO: MIGHT HAVE TO PASS THIS ONE THROUGH THE LLM
    df_owner = pd.DataFrame()
    df_owner['original_owner'] = df_raw['owner']
    df_owner[['owner', 'owner_email', 'owner_team', 'tr_metadata']] = df_raw['owner'].apply(
        lambda x: it.parse_owner(x)
    ).apply(pd.Series)

    # DEVICE_TYPE TRANSFORMATIONS
    df_dt = pd.DataFrame()
    df_dt['original_dt'] = df_raw['device_type']
    df_dt[['device_type', 'device_type_confidence', 'tr_metadata']] = df_raw['device_type'].apply(
        lambda x: it.normalize_device_type(x)
    ).apply(pd.Series)

    # NORMALIZATION_STEPS
    df_normalization_steps = pd.DataFrame({
        "source_row_id": df_raw['source_row_id'],
        "ip_traceability_metadata": df_ipv4['tr_metadata'],
        "hostname_traceability_metadata": df_hostname['tr_metadata'],
        "fqdn_traceability_metadata": df_fqdn['tr_metadata'],
        "mac_traceability_metadata": df_mac['tr_metadata'],
        "owner_traceability_metadata": df_owner['tr_metadata'],
        "device_type_traceability_metadata": df_dt['tr_metadata'],
        "site_traceability_metadata": df_site['tr_metadata']
    })

    df_normalization_steps['normalization_steps'] = df_normalization_steps.apply(
        lambda x: it.append_metadata(x),
        axis=1
    )

    # PUTTING IT ALL TOGETHER
    df_final = get_df_final(df_ipv4, df_hostname, df_fqdn, df_mac, df_owner, df_dt, df_raw, df_site, df_normalization_steps)
    print(df_final.columns)

    OUTPUTS_PATH = os.path.join(os.path.dirname(__file__), '..', 'tmp', '01-ingest-and-transform-inventory')
    csv_path = os.path.join(OUTPUTS_PATH, f'{dt.today().year}-{dt.today().day}-{dt.today().month}_inventory_tmp.csv')

    df_final.to_csv(csv_path)

    return csv_path