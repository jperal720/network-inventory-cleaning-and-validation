import re
import pandas as pd
import numpy as np
import ipaddress

from urllib.parse import urlparse

class InventoryTransformations:

    def __init__(self):
        return

    def traceability_metadata(self, field: str, from_value, to_value, reason: str):
        return {
            "field": field,
            "from": from_value,
            "to": to_value,
            "reason": reason
        }
    
    def ipv4_validate_and_normalize(self, ip_str):
        FIELD="ip"
        FROM=ip_str

        if pd.isna(ip_str):
            final_ip = None
            return (False, final_ip, self.traceability_metadata(FIELD, FROM, final_ip, "missing"))
        s = str(ip_str).strip()
        if ':' in s:
            final_ip = None
            return (False, final_ip, self.traceability_metadata(FIELD, FROM, final_ip, "ipv6_or_non_ipv4"))
        parts = s.split(".")
        if len(parts) != 4:
            final_ip = None
            return (False, final_ip, self.traceability_metadata(FIELD, FROM, final_ip, "wrong_octet_count"))
        
        canonical_parts = []
        for p in parts:
            if p == '':
                final_ip = None
                return (False, final_ip, self.traceability_metadata(FIELD, FROM, final_ip, "empty_octet"))
            if not (p.lstrip("+").isdigit() and not p.startswith("-")):
                final_ip = None
                return (False, final_ip, self.traceability_metadata(FIELD, FROM, final_ip, "non_numeric_or_negative"))
            try:
                v = int(p, 10)
            except ValueError:
                final_ip = None
                return (False, final_ip, self.traceability_metadata(FIELD, FROM, final_ip, "non_decimal_format"))
            if v < 0 or v > 255:
                final_ip = None
                return (False, final_ip, self.traceability_metadata(FIELD, FROM, final_ip, "octet_out_of_range"))
            canonical_parts.append(str(v))
        canonical = ".".join(canonical_parts)

        
        return (True, canonical, self.traceability_metadata(FIELD, FROM, canonical, "ok"))

    def classify_ipv4_type(self, ip):
        if pd.isna(ip) or ip is None:
            return "invalid"
        
        try:
            octets = list(map(int, ip.split('.')))

            # Checking private ranges
            if octets[0] == 10:
                return "private_rfc1918"
            elif octets[0] == 172 and 16 <= octets[1] <= 31:
                return "private_rfc1918"
            elif octets[0] == 192 and octets[1] == 168:
                return "private_rfc1918"
            
            # Other ranges
            elif octets[0] == 169 and octets[1] == 254:
                return "link_local_apipa"
            elif octets[0] == 127:
                return "loopback"
            
            return "public_or_other"
        except (ValueError, AttributeError, IndexError):
            return "invalid_format"
        
    def default_subnet(self, ip, ip_type=None):
        if pd.isna(ip) or ip is None:
            return ""
        
        try:
            octets = list(map(int, ip.split('.')))

            # Get Ip type, if not provided
            if ip_type is None:
                ip_type = classify_ipv4_type_pandas(ip)

            # Subnet strategies
            if ip_type == 'private_rfc1918':
                if octets[0] == 10:
                    return f"10.0.0.0/8"
                elif octets[0] == 172:
                    return f"172.{octets[1]}.0.0/16"
                elif octets[0] == 192 and octets[1] == 168:
                    return f"192.168.{octets[2]}.0/24"
                
            elif ip_type == "link_local_apipa":
                return "169.254.0.0/16"

            elif ip_type == "loopback":
                return "127.0.0.0/8"
            
            elif ip_type == "public_or_other":
                # Assumign /24 for public IPs
                return f"{octets[0]}.{octets[1]}.{octets[2]}.0/24"
            
            return ""
        
        except (ValueError, AttributeError, IndexError):
            return ""
        
    def validate_hostname(self, hostname_str):
        """Validating hostname according to RFC1123"""

        FIELD='hostname'
        FROM=hostname_str

        if pd.isna(hostname_str):
            final_hostname = None
            return (False, None, self.traceability_metadata(FIELD, FROM, final_hostname, "missing"))
        
        s = str(hostname_str).strip()

        final_hostname = s

        if s == "":
            final_hostname = None
            return (False, final_hostname, self.traceability_metadata(FIELD, FROM, final_hostname, "empty_string"))
        
        if len(s) > 63:
            return (False, final_hostname, self.traceability_metadata(FIELD, FROM, final_hostname, "too_long"))
        
        # Checking periods
        if '.' in s:
            return (False, final_hostname, self.traceability_metadata(FIELD, FROM, final_hostname, "contains_periods"))
        
        # Character validation - RFC 1123
        if not re.match(r'^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?$', s):
            return (False, final_hostname, self.traceability_metadata(FIELD, FROM, final_hostname, "invalid_characters"))
        
        # Cannot start or end with hyphen
        if s.startswith('-') or s.endswith('-'):
            return (False, final_hostname, self.traceability_metadata(FIELD, FROM, final_hostname, "hyphen_at_edge"))
        
        # Normalize to lowercase for consistency
        canonical = s.lower()

        return (True, canonical, self.traceability_metadata(FIELD, FROM, canonical, "ok"))
    
    def normalize_site(self, site: str) -> str:
        """
        Normalize a site name string and return a standardized form.
        
        Examples:
        'BLR Campus'    -> 'blr-campus'
        'HQ Bldg 1'     -> 'hq-bldg-1'
        'HQ-BUILDING-1' -> 'hq-bldg-1'
        'Lab-1'         -> 'lab-1'
        NaN or None     -> 'unknown'
        """
        FIELD='site'
        FROM=site

        final_site = 'unknown'
        if pd.isna(site) or site is None:
            return (final_site, self.traceability_metadata(FIELD, FROM, final_site, "missing"))
        
        s = str(site).strip().lower()
        if s == "":
            return (final_site, self.traceability_metadata(FIELD, FROM, final_site, "empty_string"))
        
        # Replace underscores, multiple spaces, and commas with hyphens
        s = re.sub(r"[\s,_]+", "-", s)
        
        # Common standardization: building → bldg, campus → campus (unchanged)
        s = s.replace("building", "bldg")
        
        # Remove duplicate hyphens
        s = re.sub(r"-{2,}", "-", s)
        
        # Strip trailing or leading hyphens
        s = s.strip("-")
        
        # Handle very generic cases
        if s in {"n/a", "na", "none", "null", "unknown"}:
            return (final_site, self.traceability_metadata(FIELD, FROM, final_site, "missing"))
        
        return (s, self.traceability_metadata(FIELD, FROM, s, "normalized_site"))

    def validate_fqdn(self, fqdn_str, hostname_part=None, site_part=None):
        """
        Validates FQDN according to RFC 1035 and checks consistency with hostname/site.
        Returns a tuple: (is_valid: bool, normalized_value: str, error_code: str, consistency: str)
        """
        FIELD='fqdn'
        FROM=fqdn_str

        final_fqdn = None
        if pd.isna(fqdn_str) or fqdn_str is None:
            return (False, final_fqdn, self.traceability_metadata(FIELD, FROM, final_fqdn, "missing"), "inconsistent")
        
        s = str(fqdn_str).strip()

        if s == "":
            return (False, final_fqdn, self.traceability_metadata(FIELD, FROM, final_fqdn, "empty_string"), "inconsistent")
        
        final_fqdn = s
        # Remove trailing dot if present (optional in some systems)
        if s.endswith('.'):
            s = s[:-1]
        
        # Check overall length (RFC 1035: max 255 chars including dots)
        if len(s) > 255:
            return (False, final_fqdn, self.traceability_metadata(FIELD, FROM, final_fqdn, "too_long"), "inconsistent")
        
        # Split into labels
        labels = s.split('.')
        
        # Must have at least 2 labels (hostname + domain)
        if len(labels) < 2:
            return (False, final_fqdn, self.traceability_metadata(FIELD, FROM, final_fqdn, "too_few_labels"), "inconsistent")
        
        # Validate each label
        for i, label in enumerate(labels):
            # Check label length (max 63 chars per RFC 1035)
            if len(label) > 63:
                return (False, final_fqdn, self.traceability_metadata(FIELD, FROM, final_fqdn, f"label_{i}_too_long"), "inconsistent")
            
            # Check for empty labels (consecutive periods)
            if len(label) == 0:
                return (False, final_fqdn, self.traceability_metadata(FIELD, FROM, final_fqdn, "empty_label"), "inconsistent")
            
            # First and last label have special rules
            if i == 0:
                # First label is essentially the hostname - apply hostname rules
                if not re.match(r'^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?$', label):
                    return (False, final_fqdn, self.traceability_metadata(FIELD, FROM, final_fqdn, "invalid_hostname_label"), "inconsistent")
            else:
                # Other labels (domain parts) can start/end with digits
                # But still no leading/trailing hyphens
                if not re.match(r'^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?$', label):
                    # Allow digits at start/end for domain labels (like "2ndfloor" or "lab1")
                    if not re.match(r'^[a-zA-Z0-9-]+$', label):
                        return (False, final_fqdn, self.traceability_metadata(FIELD, FROM, final_fqdn, f"invalid_domain_label{i}"), "inconsistent")
            
            # Check for leading/trailing hyphens in all labels
            if label.startswith('-') or label.endswith('-'):
                return (False, final_fqdn, self.traceability_metadata(FIELD, FROM, final_fqdn, f"label_{i}_hyphen_edge"), "inconsistent")
        
        # Normalize to lowercase
        canonical = s.lower()
        
        # Check consistency with hostname and site if provided
        consistency_status = "unknown"
        if hostname_part is not None and site_part is not None:
            expected_fqdn = f"{hostname_part}.{site_part}".lower()
            if canonical == expected_fqdn:
                consistency_status = "consistent"
            else:
                consistency_status = "inconsistent"

        
        return (True, canonical, self.traceability_metadata(FIELD, FROM, canonical, "valid"), consistency_status)


    def generate_reverse_ptr(self, ip):
        if pd.isna(ip) or ip is None:
            return None

        s_ip = str(ip).strip()
        if s_ip == "":
            return None

        try:
            ip_obj = ipaddress.ip_address(s_ip)
        except ValueError:
            return None

        reverse_key = ip_obj.reverse_pointer.lower().rstrip('.') 

        return reverse_key
    
    def validate_mac(self, mac_str):
        """
        Validates a MAC address string.
        Returns a tuple: (is_valid: bool, normalized_value: str | None, error_code: str)
        
        Rules:
        - Must not be missing or empty
        - Must match one of the common formats (colon, hyphen, or dot separated)
        - Normalized output: colon-separated lowercase (e.g., 'aa:bb:cc:dd:ee:ff')
        """

        FIELD='mac'
        FROM=mac_str
        
        final_mac = None
        if pd.isna(mac_str) or mac_str is None:
            return (False, final_mac, self.traceability_metadata(FIELD, FROM, final_mac, "missing"))
        
        s = str(mac_str).strip()
        if s == "":
            return (False, final_mac, self.traceability_metadata(FIELD, FROM, final_mac, "empty_string"))

        # Define regex patterns for common formats
        patterns = [
            r"^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$",   # AA:BB:CC:DD:EE:FF or AA-BB-CC-DD-EE-FF
            r"^([0-9A-Fa-f]{4}\.){2}([0-9A-Fa-f]{4})$",     # AAAA.BBBB.CCCC
        ]
        
        # Check match
        if not any(re.match(p, s) for p in patterns):
            return (False, final_mac, self.traceability_metadata(FIELD, FROM, final_mac, "invalid_format"))
        
        # Normalize to colon-separated lowercase format
        s = s.lower().replace('-', ':')
        if '.' in s:
            # Cisco-style -> flatten and regroup
            s = s.replace('.', '')
            s = ':'.join([s[i:i+2] for i in range(0, 12, 2)])

        
        return (True, s, self.traceability_metadata(FIELD, FROM, s, "valid"))
    
    def parse_owner(self, owner_str):
        """
        Parses a single owner string into (owner, owner_email, owner_team).
        
        Returns:
            tuple: (owner, owner_email, owner_team)
        """
        FIELD='owner'
        FROM=owner_str

        final_owner=None
        if pd.isna(owner_str) or not str(owner_str).strip():
            return (None, None, None, self.traceability_metadata(FIELD, FROM, final_owner, "missing"))
        
        s = str(owner_str).strip()
        
        # Regex for email detection
        email_pattern = re.compile(r"[\w\.-]+@[\w\.-]+\.\w+")
        
        # Extract email if present
        email_match = email_pattern.search(s)
        email = email_match.group(0) if email_match else None
        s_no_email = email_pattern.sub("", s).strip()
        
        # Extract team (inside parentheses)
        team_match = re.search(r"\((.*?)\)", s_no_email)
        team = team_match.group(1).strip() if team_match else None
        s_no_paren = re.sub(r"\(.*?\)", "", s_no_email).strip()
        
        # Remaining part is likely the owner name or alias
        owner = s_no_paren.lower() if s_no_paren else None
        
        return (owner, email, team, self.traceability_metadata(FIELD, FROM, owner, "owner_valid_and_normalized"))

    def normalize_device_type(self, device_type: str):
        """
        Normalize a device_type string and return a tuple:
        (normalized_device_type: str, device_type_confidence: int)
        
        Rules:
        - 100 → exact match to known canonical type
        - 90  → fuzzy or alias match (e.g. abbreviation, keyword)
        - 0   → unrecognized or missing
        """

        # Canonical device types
        canonical_types = {
            "switch", "router", "firewall", "server", "printer",
            "wireless_ap", "wireless_controller", "load_balancer",
            "storage", "ups", "ip_phone", "camera", "unknown"
        }

        FIELD='device_type'
        FROM=device_type

        final_device_type = 'unknown'

        # Strong / exact aliases → canonical
        strong_map = {
            "switch": "switch",
            "router": "router",
            "firewall": "firewall",
            "server": "server",
            "printer": "printer",
            "access_point": "wireless_ap",
            "wireless_ap": "wireless_ap",
            "controller": "wireless_controller",
            "load_balancer": "load_balancer",
            "storage": "storage",
            "ups": "ups",
            "phone": "ip_phone",
            "camera": "camera"
        }

        # Fuzzy patterns for common abbreviations or variations
        fuzzy_patterns = [
            (r"\bsw\b|switch", "switch"),
            (r"\brtr\b|router", "router"),
            (r"\bfw\b|firewall|asa|pa\d+", "firewall"),
            (r"\bap\b|access[_-]?point", "wireless_ap"),
            (r"wlc|controller", "wireless_controller"),
            (r"f5|ltm|lb|load[_-]?balancer", "load_balancer"),
            (r"server|vm|esxi", "server"),
            (r"printer|print", "printer"),
            (r"ups", "ups"),
            (r"voip|phone", "ip_phone"),
            (r"camera|cam", "camera"),
            (r"nas|san|storage", "storage"),
        ]

        # --- Clean input ---
        if pd.isna(device_type) or device_type is None:
            return ("unknown", 0 , self.traceability_metadata(FIELD, FROM, final_device_type, "missing"))

        s = str(device_type).strip().lower()
        s = re.sub(r"[,_;/]", " ", s)
        s = re.sub(r"\s+", " ", s)
        if s == "":
            return ("unknown", 0, self.traceability_metadata(FIELD, FROM, final_device_type, "empty_string"))

        # --- Exact match ---
        if s in strong_map:
            final_device_type = strong_map[s]
            return (strong_map[s], 100, self.traceability_metadata(FIELD, FROM, final_device_type, "exact_match"))
        if s in canonical_types:
            final_device_type = s
            return (s, 100, self.traceability_metadata(FIELD, FROM, final_device_type, "exact_match"))

        # --- Fuzzy match ---
        for pattern, normalized in fuzzy_patterns:
            final_device_type = normalized
            if re.search(pattern, s):
                return (normalized, 90, self.traceability_metadata(FIELD, FROM, final_device_type, "fuzzy_match"))
            

        # --- No match ---
        return ("unknown", 0, self.traceability_metadata(FIELD, FROM, final_device_type, "no_match"))
    
    def append_metadata(self, row):
        """Inputs: (row) row of df_normalization_steps
        Returns: (metdata_steps) list including all steps in each row"""
        metadata_steps = []

        for i in range(1, len(row)):
            metadata_steps.append(row[i])

        return metadata_steps