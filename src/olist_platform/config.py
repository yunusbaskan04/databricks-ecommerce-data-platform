"""Single source of truth for where data lives.

Raw files stay in a Unity Catalog Volume; every Delta table is a UC managed
table in ``CATALOG.SCHEMA`` named ``<layer>_<entity>`` (see ADR-0001).
"""

CATALOG = "workspace"
SCHEMA = "olist"

RAW_VOLUME_PATH = "/Volumes/workspace/default/olist_data"


def table_name(layer: str, entity: str) -> str:
    """Return the fully qualified name of a medallion table, e.g. bronze_orders."""
    return f"{CATALOG}.{SCHEMA}.{layer}_{entity}"


def raw_file_path(file_name: str) -> str:
    """Return the path of a raw source file inside the Volume."""
    return f"{RAW_VOLUME_PATH}/{file_name}"
