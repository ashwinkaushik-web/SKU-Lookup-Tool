"""
Product Catalogue Lookup — Pattern
Streamlit Cloud | Key-pair auth | No login needed
"""

import streamlit as st
import pandas as pd
import snowflake.connector
import datetime
import time
import io
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

st.set_page_config(page_title="Product Catalogue Lookup — Pattern", page_icon="📦", layout="wide")

# ══════════════════════════════════════════════
# CSS
# ══════════════════════════════════════════════
st.markdown("""
<style>
    .main-header {display:flex;align-items:center;gap:16px;margin-bottom:4px;}
    .header-icon {width:48px;height:48px;background:linear-gradient(135deg,#3b82f6,#6366f1);border-radius:12px;display:grid;place-items:center;font-size:22px;color:#fff;box-shadow:0 0 24px rgba(59,130,246,0.3);flex-shrink:0;}
    .header-title {font-size:28px;font-weight:700;margin:0;}
    .header-sub {font-size:14px;color:#64748b;margin:0;}

    /* Colored metric cards */
    .metric-card {border-radius:12px;padding:18px 22px;border:1px solid rgba(255,255,255,0.06);}
    .metric-card .label {font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px;opacity:0.8;}
    .metric-card .value {font-size:32px;font-weight:700;margin-top:2px;}
    .mc-total {background:rgba(59,130,246,0.12);color:#60a5fa;}
    .mc-dno {background:rgba(239,68,68,0.12);color:#f87171;}
    .mc-ship {background:rgba(34,197,94,0.12);color:#4ade80;}
    .mc-noship {background:rgba(245,158,11,0.12);color:#fbbf24;}
    .mc-fba {background:rgba(168,85,247,0.12);color:#c084fc;}
    .mc-active {background:rgba(6,182,212,0.12);color:#22d3ee;}

    /* Missing items */
    .missing-item {display:inline-block;background:rgba(239,68,68,0.15);color:#f87171;padding:3px 10px;border-radius:999px;font-size:12px;font-weight:500;margin:2px 4px;}

    #MainMenu {visibility:hidden;}
    footer {visibility:hidden;}

    /* Sidebar */
    div[data-testid="stSidebar"] {background:rgba(13,17,23,0.97);}
    .sidebar-section {background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.06);border-radius:10px;padding:14px;margin-bottom:12px;}
    .sidebar-section h4 {margin:0 0 8px 0;font-size:14px;}
    .sidebar-section p {margin:0;font-size:12px;color:#94a3b8;line-height:1.5;}

    /* Usage counter */
    .usage-badge {background:linear-gradient(135deg,rgba(59,130,246,0.15),rgba(99,102,241,0.15));border:1px solid rgba(59,130,246,0.2);border-radius:10px;padding:12px 14px;text-align:center;margin-bottom:12px;}
    .usage-badge .num {font-size:28px;font-weight:700;color:#60a5fa;}
    .usage-badge .lbl {font-size:10px;text-transform:uppercase;letter-spacing:0.5px;color:#94a3b8;margin-top:2px;}

    /* Copy buttons */
    .copy-section {background:rgba(255,255,255,0.03);border:1px solid rgba(255,255,255,0.06);border-radius:8px;padding:10px;margin-top:8px;}

    /* ── FR Check Redesign ── */
    .fr-summary-tile {background:#161b22;border:1px solid #30363d;border-radius:10px;padding:14px 18px;}
    .fr-summary-tile .lbl {font-size:10px;color:#7d8590;text-transform:uppercase;letter-spacing:0.5px;font-weight:600;}
    .fr-summary-tile .val {font-size:26px;font-weight:700;margin-top:2px;}
    .fr-summary-tile.passed {background:rgba(46,160,67,0.12);border-color:rgba(46,160,67,0.4);}
    .fr-summary-tile.passed .val {color:#3fb950;}
    .fr-summary-tile.flagged {background:rgba(248,81,73,0.12);border-color:rgba(248,81,73,0.4);}
    .fr-summary-tile.flagged .val {color:#f85149;}
    .fr-summary-tile.missing {background:rgba(210,153,34,0.10);border-color:rgba(210,153,34,0.3);}
    .fr-summary-tile.missing .val {color:#d29922;}
    .fr-summary-tile.total .val {color:#60a5fa;}

    /* Listing cards */
    .fr-card {background:#161b22;border:1px solid #30363d;border-radius:10px;margin-bottom:10px;overflow:hidden;}
    .fr-card.flagged {border-left:3px solid #f85149;}
    .fr-card.passed {border-left:3px solid #3fb950;}
    .fr-card-head {padding:14px 18px;display:grid;grid-template-columns:1fr auto;gap:14px;align-items:center;}
    .fr-card-title {font-size:14px;font-weight:600;color:#e6edf3;}
    .fr-card-meta {font-size:11px;color:#7d8590;margin-top:3px;font-family:monospace;}
    .fr-card-meta b {color:#c9d1d9;font-weight:500;}
    .fr-card-status {display:flex;align-items:center;gap:14px;}
    .fr-status-pill {padding:5px 12px;border-radius:14px;font-size:11px;font-weight:700;letter-spacing:0.3px;white-space:nowrap;}
    .fr-status-pill.ok {background:rgba(46,160,67,0.15);color:#3fb950;}
    .fr-status-pill.bad {background:rgba(248,81,73,0.15);color:#f85149;}
    .fr-attr-strip {display:flex;gap:4px;flex-wrap:wrap;}
    .fr-attr-dot {width:22px;height:22px;border-radius:50%;display:inline-flex;align-items:center;justify-content:center;font-size:10px;font-weight:700;}
    .fr-attr-dot.g {background:rgba(46,160,67,0.18);color:#3fb950;}
    .fr-attr-dot.r {background:rgba(248,81,73,0.18);color:#f85149;}
    .fr-attr-dot.i {background:rgba(99,110,123,0.18);color:#8b949e;}

    /* Attribute details grid */
    .fr-attr-grid {display:grid;grid-template-columns:repeat(2, 1fr);gap:8px;}
    .fr-attr-row {display:flex;align-items:center;gap:10px;padding:10px 12px;background:#161b22;border-radius:6px;border-left:3px solid transparent;}
    .fr-attr-row.g {border-left-color:#3fb950;}
    .fr-attr-row.r {border-left-color:#f85149;}
    .fr-attr-row.i {border-left-color:#636e7b;}
    .fr-attr-row .nm {flex:1;font-size:12px;color:#c9d1d9;font-weight:500;}
    .fr-attr-row .vl {font-family:monospace;font-size:11px;color:#7d8590;}
    .fr-attr-row .mk {font-size:13px;}
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════
# Column config
# ══════════════════════════════════════════════
COLUMN_MAP = {
    "SKU": {"label": "SKU", "default": True},
    "LISTING_ID": {"label": "Listing ID", "default": True},
    "MARKETPLACE": {"label": "Marketplace", "default": True},
    "VENDOR": {"label": "Vendor", "default": True},
    "PRODUCT_NAME": {"label": "Product Name", "default": True},
    "IS_DNO": {"label": "DNO", "default": True},
    "SHIPPABLE_TAG": {"label": "Shippable", "default": True},
    "LISTING_FULFILLMENT_TYPE": {"label": "Fulfillment Type", "default": True},
    "LISTING_TYPE": {"label": "Listing Type", "default": False},
    "ASIN": {"label": "ASIN", "default": True},
    "FNSKU": {"label": "FNSKU", "default": False},
    "MASTER_ID": {"label": "Master ID", "default": False},
    "MPN": {"label": "MPN", "default": False},
    "COMMINGLED_STATUS": {"label": "Commingled", "default": False},
    "IS_ACTIVE": {"label": "Active", "default": True},
    "IS_DISCONTINUED": {"label": "Discontinued", "default": False},
    "UPC": {"label": "UPC", "default": False},
    "EAN": {"label": "EAN", "default": False},
    "CAN_EXPIRE": {"label": "Can Expire", "default": False},
    "WHOLESALE_PRICE": {"label": "Wholesale Price", "default": False},
    "MAP_PRICE": {"label": "MAP Price", "default": False},
    "RETAIL_PRICE": {"label": "Retail Price", "default": False},
    "MSRP_PRICE": {"label": "MSRP Price", "default": False},
    "DNO_NOTE": {"label": "DNO Note", "default": True},
    "DNO_REASON_CODE": {"label": "DNO Reason Code", "default": True},
}

BOOL_COLS = {
    "IS_DNO": ("⛔ YES — DNO", "✅ NO"),
    "SHIPPABLE_TAG": ("✅ YES", "⛔ NO"),
    "IS_ACTIVE": ("✅ Active", "❌ Inactive"),
    "IS_DISCONTINUED": ("⛔ Discontinued", "✅ No"),
    "CAN_EXPIRE": ("⚠️ Yes", "✅ No"),
}

# ══════════════════════════════════════════════
# Usage tracker (session-based counter)
# ══════════════════════════════════════════════
if "lookup_count" not in st.session_state:
    st.session_state["lookup_count"] = 0
if "total_items_looked_up" not in st.session_state:
    st.session_state["total_items_looked_up"] = 0


# ══════════════════════════════════════════════
# Snowflake connection
# ══════════════════════════════════════════════
@st.cache_resource
def get_connection():
    sf = st.secrets["snowflake"]
    pk_pem = sf["private_key"].encode("utf-8")
    pk = serialization.load_pem_private_key(pk_pem, password=None, backend=default_backend())
    pk_bytes = pk.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    conn = snowflake.connector.connect(
        account=sf["account"], user=sf["user"], private_key=pk_bytes,
        warehouse=sf["warehouse"], role=sf["role"],
        database="ANALYTICS_DB", schema="STG_CATALOG",
    )
    conn.cursor().execute(f"USE WAREHOUSE {sf['warehouse']}")
    return conn


def build_query(skus):
    def safe(s): return s.strip().replace("'", "''")
    upper_list = ", ".join(f"UPPER('{safe(s)}')" for s in skus if s.strip())
    return f"""
WITH q1 AS (
    SELECT c.name AS marketplace, par.name AS vendor, a.Listing_MP_Primary_ID AS sku,
        a.LISTING_FULFILLMENT_TYPE AS listing_fulfillment_type, a.LISTING_ID AS listing_id,
        b.MASTER_ID AS master_id, b.MPN AS mpn, a.LISTING_MP_PAGE_ID AS asin,
        a.LISTING_MP_SECONDARY_ID AS fnsku,
        CASE
            WHEN a.LISTING_FULFILLMENT_TYPE <> 'FBA' THEN NULL
            WHEN a.LISTING_MP_SECONDARY_ID = a.LISTING_MP_PAGE_ID AND a.Listing_is_commingled = TRUE THEN 'Commingled'
            WHEN a.LISTING_MP_SECONDARY_ID <> a.LISTING_MP_PAGE_ID AND a.Listing_is_commingled = FALSE THEN 'NOT Commingled'
            WHEN a.LISTING_MP_SECONDARY_ID <> a.LISTING_MP_PAGE_ID AND a.Listing_is_commingled = TRUE THEN 'Amazon not set to commingled, but Pattern flagged'
            WHEN a.LISTING_MP_SECONDARY_ID = a.LISTING_MP_PAGE_ID AND a.Listing_is_commingled = FALSE THEN 'Amazon set as commingled, but Pattern flag not on'
            WHEN a.LISTING_MP_SECONDARY_ID IS NULL THEN 'Missing FNSKU for analysis'
            ELSE 'Check'
        END AS commingled_status,
        dno.DNO_NOTE AS dno_note,
        dno_rc.DNO_REASON_CODE AS dno_reason_code
    FROM ANALYTICS_DB.STG_CATALOG.STG_CATALOG__LISTINGS a
    LEFT JOIN ANALYTICS_DB.STG_CATALOG.STG_CATALOG__PRODUCTS b ON b.ID = a.PRODUCT_ID
    LEFT JOIN ANALYTICS_DB.STG_CATALOG.STG_CATALOG__MARKETPLACES c ON a.MARKETPLACE_ID = c.ID
    LEFT JOIN ANALYTICS_DB.STG_CATALOG.STG_CATALOG__PARTNERS par ON par.ID = b.PARTNER_ID
    LEFT JOIN ANALYTICS_DB.STG_CATALOG.STG_CATALOG__DNO_SETTINGS dno ON dno.ID = a.DNO_SETTING_ID
    LEFT JOIN ANALYTICS_DB.STG_CATALOG.STG_CATALOG__DNO_REASON_CODES dno_rc ON dno_rc.ID = dno.DNO_REASON_CODE_ID
),
q2 AS (
    SELECT pc.MARKETPLACE_NAME AS marketplace, pc.VENDOR_NAME AS vendor, pc.MARKETPLACE_PRIMARY_ID AS sku,
        pc.FULFILLMENT_TYPE AS listing_fulfillment_type, pc.LISTING_ID AS listing_id,
        pc.LISTING_IS_SHIPABLE AS shippable_tag, pc.LISTING_TYPE AS listing_type,
        pc.IS_ACTIVE AS is_active, pc.IS_DISCONTINUED AS is_discontinued,
        pc.PRODUCT_NAME AS product_name, pc.UPC AS upc, pc.EAN AS ean,
        pc.CAN_EXPIRE AS can_expire,
        pc.FINANCE_APPROVED_WHOLESALE_PRICE_W_CURRENCY AS wholesale_price,
        pc.MAP_W_CURRENCY AS map_price, pc.RETAIL_W_CURRENCY AS retail_price,
        pc.MSRP_W_CURRENCY AS msrp_price
    FROM PATTERN_DB.PUBLIC.PRODUCT_CATALOG_PRODUCTS_AND_LISTINGS_VIEW pc
),
q3 AS (
    SELECT h.LISTING_ID AS listing_id, h.IS_DNO AS is_dno
    FROM PATTERN_DB.PUBLIC.CATALOG_LISTING_STATUS_HISTORY h
    WHERE h."DATE" = (SELECT MAX("DATE") FROM PATTERN_DB.PUBLIC.CATALOG_LISTING_STATUS_HISTORY)
),
base AS (
    SELECT COALESCE(q2.marketplace, q1.marketplace) AS MARKETPLACE,
        COALESCE(q2.vendor, q1.vendor) AS VENDOR,
        COALESCE(q2.sku, q1.sku) AS SKU,
        COALESCE(q2.listing_fulfillment_type, q1.listing_fulfillment_type) AS LISTING_FULFILLMENT_TYPE,
        COALESCE(q2.listing_id, q1.listing_id) AS LISTING_ID,
        q1.master_id AS MASTER_ID, q1.mpn AS MPN,
        q1.asin AS ASIN, q1.fnsku AS FNSKU,
        q1.commingled_status AS COMMINGLED_STATUS,
        q2.shippable_tag AS SHIPPABLE_TAG,
        q2.listing_type AS LISTING_TYPE,
        COALESCE(q3.is_dno, FALSE) AS IS_DNO,
        q2.is_active AS IS_ACTIVE, q2.is_discontinued AS IS_DISCONTINUED,
        q2.product_name AS PRODUCT_NAME, q2.upc AS UPC, q2.ean AS EAN,
        q2.can_expire AS CAN_EXPIRE, q2.wholesale_price AS WHOLESALE_PRICE,
        q2.map_price AS MAP_PRICE, q2.retail_price AS RETAIL_PRICE,
        q2.msrp_price AS MSRP_PRICE, q1.dno_note AS DNO_NOTE,
        q1.dno_reason_code AS DNO_REASON_CODE
    FROM q1 FULL OUTER JOIN q2 ON q1.listing_id = q2.listing_id
    LEFT JOIN q3 ON q3.listing_id = COALESCE(q1.listing_id, q2.listing_id)
)
SELECT * FROM base
WHERE UPPER(SKU) IN ({upper_list}) OR UPPER(LISTING_ID) IN ({upper_list})
   OR UPPER(ASIN) IN ({upper_list}) OR UPPER(MPN) IN ({upper_list})
   OR UPPER(MASTER_ID) IN ({upper_list}) OR UPPER(FNSKU) IN ({upper_list})
ORDER BY MARKETPLACE, VENDOR, SKU
"""


def run_lookup(skus):
    conn = get_connection()
    return pd.read_sql(build_query(skus), conn)


def build_brand_query(vendor, region_marketplaces=None, limit=None):
    """Build query to fetch all listings for a brand/vendor, optionally filtered by region."""
    mp_filter = ""
    if region_marketplaces:
        mp_filter = "AND UPPER(MARKETPLACE) IN (" + ", ".join(f"UPPER('{mp}')" for mp in region_marketplaces) + ")"

    limit_clause = f"LIMIT {limit}" if limit else ""

    return f"""
WITH q1 AS (
    SELECT c.name AS marketplace, par.name AS vendor, a.Listing_MP_Primary_ID AS sku,
        a.LISTING_FULFILLMENT_TYPE AS listing_fulfillment_type, a.LISTING_ID AS listing_id,
        b.MASTER_ID AS master_id, b.MPN AS mpn, a.LISTING_MP_PAGE_ID AS asin,
        a.LISTING_MP_SECONDARY_ID AS fnsku,
        CASE
            WHEN a.LISTING_FULFILLMENT_TYPE <> 'FBA' THEN NULL
            WHEN a.LISTING_MP_SECONDARY_ID = a.LISTING_MP_PAGE_ID AND a.Listing_is_commingled = TRUE THEN 'Commingled'
            WHEN a.LISTING_MP_SECONDARY_ID <> a.LISTING_MP_PAGE_ID AND a.Listing_is_commingled = FALSE THEN 'NOT Commingled'
            WHEN a.LISTING_MP_SECONDARY_ID <> a.LISTING_MP_PAGE_ID AND a.Listing_is_commingled = TRUE THEN 'Amazon not set to commingled, but Pattern flagged'
            WHEN a.LISTING_MP_SECONDARY_ID = a.LISTING_MP_PAGE_ID AND a.Listing_is_commingled = FALSE THEN 'Amazon set as commingled, but Pattern flag not on'
            WHEN a.LISTING_MP_SECONDARY_ID IS NULL THEN 'Missing FNSKU for analysis'
            ELSE 'Check'
        END AS commingled_status,
        dno.DNO_NOTE AS dno_note,
        dno_rc.DNO_REASON_CODE AS dno_reason_code
    FROM ANALYTICS_DB.STG_CATALOG.STG_CATALOG__LISTINGS a
    LEFT JOIN ANALYTICS_DB.STG_CATALOG.STG_CATALOG__PRODUCTS b ON b.ID = a.PRODUCT_ID
    LEFT JOIN ANALYTICS_DB.STG_CATALOG.STG_CATALOG__MARKETPLACES c ON a.MARKETPLACE_ID = c.ID
    LEFT JOIN ANALYTICS_DB.STG_CATALOG.STG_CATALOG__PARTNERS par ON par.ID = b.PARTNER_ID
    LEFT JOIN ANALYTICS_DB.STG_CATALOG.STG_CATALOG__DNO_SETTINGS dno ON dno.ID = a.DNO_SETTING_ID
    LEFT JOIN ANALYTICS_DB.STG_CATALOG.STG_CATALOG__DNO_REASON_CODES dno_rc ON dno_rc.ID = dno.DNO_REASON_CODE_ID
),
q2 AS (
    SELECT pc.MARKETPLACE_NAME AS marketplace, pc.VENDOR_NAME AS vendor, pc.MARKETPLACE_PRIMARY_ID AS sku,
        pc.FULFILLMENT_TYPE AS listing_fulfillment_type, pc.LISTING_ID AS listing_id,
        pc.LISTING_IS_SHIPABLE AS shippable_tag, pc.LISTING_TYPE AS listing_type,
        pc.IS_ACTIVE AS is_active, pc.IS_DISCONTINUED AS is_discontinued,
        pc.PRODUCT_NAME AS product_name, pc.UPC AS upc, pc.EAN AS ean,
        pc.CAN_EXPIRE AS can_expire,
        pc.FINANCE_APPROVED_WHOLESALE_PRICE_W_CURRENCY AS wholesale_price,
        pc.MAP_W_CURRENCY AS map_price, pc.RETAIL_W_CURRENCY AS retail_price,
        pc.MSRP_W_CURRENCY AS msrp_price
    FROM PATTERN_DB.PUBLIC.PRODUCT_CATALOG_PRODUCTS_AND_LISTINGS_VIEW pc
),
q3 AS (
    SELECT h.LISTING_ID AS listing_id, h.IS_DNO AS is_dno
    FROM PATTERN_DB.PUBLIC.CATALOG_LISTING_STATUS_HISTORY h
    WHERE h."DATE" = (SELECT MAX("DATE") FROM PATTERN_DB.PUBLIC.CATALOG_LISTING_STATUS_HISTORY)
),
base AS (
    SELECT COALESCE(q2.marketplace, q1.marketplace) AS MARKETPLACE,
        COALESCE(q2.vendor, q1.vendor) AS VENDOR,
        COALESCE(q2.sku, q1.sku) AS SKU,
        COALESCE(q2.listing_fulfillment_type, q1.listing_fulfillment_type) AS LISTING_FULFILLMENT_TYPE,
        COALESCE(q2.listing_id, q1.listing_id) AS LISTING_ID,
        q1.master_id AS MASTER_ID, q1.mpn AS MPN,
        q1.asin AS ASIN, q1.fnsku AS FNSKU,
        q1.commingled_status AS COMMINGLED_STATUS,
        q2.shippable_tag AS SHIPPABLE_TAG,
        q2.listing_type AS LISTING_TYPE,
        COALESCE(q3.is_dno, FALSE) AS IS_DNO,
        q2.is_active AS IS_ACTIVE, q2.is_discontinued AS IS_DISCONTINUED,
        q2.product_name AS PRODUCT_NAME, q2.upc AS UPC, q2.ean AS EAN,
        q2.can_expire AS CAN_EXPIRE, q2.wholesale_price AS WHOLESALE_PRICE,
        q2.map_price AS MAP_PRICE, q2.retail_price AS RETAIL_PRICE,
        q2.msrp_price AS MSRP_PRICE, q1.dno_note AS DNO_NOTE,
        q1.dno_reason_code AS DNO_REASON_CODE
    FROM q1 FULL OUTER JOIN q2 ON q1.listing_id = q2.listing_id
    LEFT JOIN q3 ON q3.listing_id = COALESCE(q1.listing_id, q2.listing_id)
)
SELECT * FROM base
WHERE UPPER(VENDOR) = UPPER('{vendor.replace(chr(39), chr(39)+chr(39))}')
{mp_filter}
ORDER BY MARKETPLACE, SKU
{limit_clause}
"""


def run_brand_lookup(vendor, region_marketplaces=None, limit=None):
    conn = get_connection()
    return pd.read_sql(build_brand_query(vendor, region_marketplaces, limit), conn)


def get_vendor_list():
    """Fetch distinct vendor names for the brand dropdown."""
    conn = get_connection()
    df = pd.read_sql("SELECT DISTINCT par.name AS VENDOR FROM ANALYTICS_DB.STG_CATALOG.STG_CATALOG__PARTNERS par WHERE par.name IS NOT NULL ORDER BY 1", conn)
    return df["VENDOR"].tolist()


# ══════════════════════════════════════════════
# FR Check — Functional Readiness query & rules
# ══════════════════════════════════════════════
FR_ATTRIBUTES = [
    {"id": "LISTING_IS_COMMINGLED", "label": "Commingled", "type": "check"},
    {"id": "IS_ACTIVE", "label": "Is Active", "type": "check"},
    {"id": "MARKETPLACE_COUNTRY_CODE", "label": "Country Code", "type": "info"},
    {"id": "PRODUCT_DIMENSIONS", "label": "Product Dimensions", "type": "info"},
    {"id": "CASEPACK_DIMENSIONS", "label": "Casepack Dimensions", "type": "info"},
    {"id": "IS_HAZMAT", "label": "Is Hazmat", "type": "info"},
    {"id": "LISTING_IS_SHIPABLE", "label": "Is Shipable", "type": "check"},
    {"id": "LISTING_PREP_PLAN_ID", "label": "Listing Prep Plan ID", "type": "check"},
    {"id": "ITEM_PREP_PLAN_ID", "label": "Item Prep Plan ID", "type": "check"},
    {"id": "CAN_EXPIRE", "label": "Can Expire", "type": "check"},
    {"id": "IS_GLASS", "label": "Is Glass", "type": "check"},
    {"id": "CATALOG_DNO_STATUS", "label": "DNO Status", "type": "check"},
    {"id": "IS_TEMPORARY", "label": "Is Temporary", "type": "info"},
    {"id": "MARKETPLACE_SECONDARY_ID_TYPE", "label": "Secondary ID Type", "type": "info"},
]

# Quick Check preset = the most important checks (no info-only)
QUICK_CHECK_ATTRS = [
    "LISTING_IS_COMMINGLED", "IS_ACTIVE", "LISTING_IS_SHIPABLE",
    "LISTING_PREP_PLAN_ID", "ITEM_PREP_PLAN_ID", "CAN_EXPIRE",
    "IS_GLASS", "CATALOG_DNO_STATUS",
]


def build_fr_query(ids, id_type="LISTING_ID"):
    """Build query for FR Check — pulls catalogue data live from Snowflake.
    id_type: 'LISTING_ID' or 'SKU' (MARKETPLACE_PRIMARY_ID)
    """
    def safe(s): return s.strip().replace("'", "''")
    upper_list = ", ".join(f"UPPER('{safe(s)}')" for s in ids if s.strip())

    if id_type == "SKU":
        where_clause = f"WHERE UPPER(pc.MARKETPLACE_PRIMARY_ID) IN ({upper_list})"
    else:  # LISTING_ID
        where_clause = f"WHERE UPPER(pc.LISTING_ID) IN ({upper_list})"

    return f"""
SELECT
    pc.LISTING_ID,
    pc.MARKETPLACE_PRIMARY_ID AS SKU,
    pc.MARKETPLACE_SECONDARY_ID,
    pc.MARKETPLACE_SECONDARY_ID_TYPE,
    pc.MARKETPLACE_NAME AS MARKETPLACE,
    pc.MARKETPLACE_COUNTRY_CODE,
    pc.VENDOR_NAME AS VENDOR,
    pc.PRODUCT_NAME,
    pc.IS_ACTIVE,
    pc.IS_DISCONTINUED,
    pc.LISTING_IS_SHIPABLE,
    pc.CAN_EXPIRE,
    pc.IS_HAZMAT,
    pc.IS_GLASS,
    pc.LISTING_PREP_PLAN_ID,
    pc.PRODUCT_DIMENSIONS,
    pc.CASEPACK_DIMENSIONS,
    pcl.LISTING_IS_COMMINGLED,
    dno.CATALOG_DNO_STATUS,
    dno.IS_TEMPORARY,
    li.ITEM_PREP_PLAN_ID
FROM PATTERN_DB.PUBLIC.PRODUCT_CATALOG_PRODUCTS_AND_LISTINGS_VIEW pc
LEFT JOIN PATTERN_DB.PUBLIC.PRODUCT_CATALOG_PRODUCTS_AND_LISTINGS_VIEW_LITE pcl
    ON pc.LISTING_ID = pcl.LISTING_ID
LEFT JOIN PATTERN_DB.PUBLIC.PRODUCT_CATALOG_CURRENT_DNO_STATUS dno
    ON pc.LISTING_ID = dno.LISTING_ID
LEFT JOIN ANALYTICS_DB.STG_CATALOG.STG_CATALOG__LISTINGS li
    ON pc.LISTING_ID = li.LISTING_ID
{where_clause}
"""


def run_fr_lookup(ids, id_type="LISTING_ID"):
    conn = get_connection()
    return pd.read_sql(build_fr_query(ids, id_type), conn)


# ── Validation engine (ported from HTML tool) ──
def _lo(v):
    return str(v or "").strip().lower()


def _is_y(v):
    s = _lo(v)
    return s in ("true", "yes", "y")


def _is_blank(v):
    return _lo(v) == ""


def _linked_check(row):
    """Commingled + Glass + Prep Plan linked rule.
    Returns dict with ok (bool) and why (str).
    """
    cm = _is_y(row.get("LISTING_IS_COMMINGLED", ""))
    sid = str(row.get("MARKETPLACE_SECONDARY_ID", "") or "").strip()
    b0 = sid.startswith("B0")
    gl = _is_y(row.get("IS_GLASS", ""))
    lp = str(row.get("LISTING_PREP_PLAN_ID", "") or "").strip()
    ip = str(row.get("ITEM_PREP_PLAN_ID", "") or "").strip()

    def has_plan(v):
        return v and v != "0" and _lo(v) not in ("false", "no", "none")

    has_any_plan = has_plan(lp) or has_plan(ip)

    # Bypass: Commingled + B0 + Glass=N -> auto-pass
    if cm and b0 and not gl:
        return {"ok": True, "why": "Comm+B0+NoGlass — bypass"}
    if has_any_plan:
        return {"ok": True, "why": "Prep plan filled"}
    return {"ok": False, "why": "Prep plan required"}


def evaluate_attribute(attr_id, row):
    """Evaluate one attribute for one row.
    Returns dict: {status: 'g'/'r'/'i'/'n', text: str, value: str}
    g = green/pass, r = red/fail, i = info, n = not applicable
    """
    v = str(row.get(attr_id, "") or "").strip()
    vl = _lo(v)
    sid = str(row.get("MARKETPLACE_SECONDARY_ID", "") or "").strip()

    if attr_id == "LISTING_IS_COMMINGLED":
        lc = _linked_check(row)
        pfx = sid[:2] if sid else "—"
        if lc["ok"]:
            return {"status": "g", "text": f"Commingled={v or '—'} / {pfx} ✓ ({lc['why']})", "value": v}
        return {"status": "r", "text": f"Commingled={v or '—'} / {pfx} — {lc['why']}", "value": v}

    if attr_id == "IS_ACTIVE":
        if _is_y(v):
            return {"status": "g", "text": "Active ✓", "value": v}
        return {"status": "r", "text": "Inactive / blank", "value": v}

    if attr_id == "MARKETPLACE_COUNTRY_CODE":
        return {"status": "i", "text": v or "—", "value": v}

    if attr_id == "PRODUCT_DIMENSIONS":
        return {"status": "i", "text": v or "—", "value": v}

    if attr_id == "CASEPACK_DIMENSIONS":
        return {"status": "i", "text": v or "—", "value": v}

    if attr_id == "IS_HAZMAT":
        return {"status": "i", "text": v or "—", "value": v}

    if attr_id == "LISTING_IS_SHIPABLE":
        if _is_y(v):
            return {"status": "g", "text": "Shipable ✓", "value": v}
        return {"status": "r", "text": "Not shipable", "value": v}

    if attr_id == "LISTING_PREP_PLAN_ID":
        lc = _linked_check(row)
        if lc["ok"]:
            return {"status": "g", "text": f"Listing Plan: {v or '—'} ✓ ({lc['why']})", "value": v}
        return {"status": "r", "text": f"Listing Plan: {v or 'blank'} — {lc['why']}", "value": v}

    if attr_id == "ITEM_PREP_PLAN_ID":
        lc = _linked_check(row)
        if lc["ok"]:
            return {"status": "g", "text": f"Item Plan: {v or '—'} ✓ ({lc['why']})", "value": v}
        return {"status": "r", "text": f"Item Plan: {v or 'blank'} — {lc['why']}", "value": v}

    if attr_id == "CAN_EXPIRE":
        # Simple pass for any non-blank, blank also passes (no LE partner field available)
        return {"status": "g", "text": f"Can Expire: {v or 'blank'} ✓", "value": v}

    if attr_id == "IS_GLASS":
        lc = _linked_check(row)
        if lc["ok"]:
            return {"status": "g", "text": f"Glass={v or '—'} ✓ ({lc['why']})", "value": v}
        return {"status": "r", "text": f"Glass={v or '—'} — {lc['why']}", "value": v}

    if attr_id == "CATALOG_DNO_STATUS":
        if vl == "dno" or _is_y(v):
            return {"status": "r", "text": "DNO active ⚠", "value": v}
        return {"status": "g", "text": "Not DNO ✓", "value": v}

    if attr_id == "IS_TEMPORARY":
        return {"status": "i", "text": v or "—", "value": v}

    if attr_id == "MARKETPLACE_SECONDARY_ID_TYPE":
        return {"status": "i", "text": v or "—", "value": v}

    return {"status": "n", "text": v or "—", "value": v}


def run_fr_check(df, selected_attrs):
    """Run FR check on each row, return list of result dicts."""
    results = []
    for _, row in df.iterrows():
        row_results = {}
        green_count = 0
        red_count = 0
        info_count = 0
        for attr in selected_attrs:
            res = evaluate_attribute(attr, row)
            row_results[attr] = res
            if res["status"] == "g":
                green_count += 1
            elif res["status"] == "r":
                red_count += 1
            elif res["status"] == "i":
                info_count += 1
        results.append({
            "listing_id": row.get("LISTING_ID", ""),
            "sku": row.get("SKU", ""),
            "product_name": row.get("PRODUCT_NAME", ""),
            "marketplace": row.get("MARKETPLACE", ""),
            "country_code": row.get("MARKETPLACE_COUNTRY_CODE", ""),
            "vendor": row.get("VENDOR", ""),
            "passed": red_count == 0,
            "green_count": green_count,
            "red_count": red_count,
            "info_count": info_count,
            "details": row_results,
            "raw_row": row.to_dict(),
        })
    return results


def multiselect_filter(df, column, label, key):
    unique_vals = sorted(df[column].dropna().unique().tolist())
    if not unique_vals:
        return df
    selected = st.multiselect(label, options=unique_vals, default=[], key=key, placeholder="All")
    if selected:
        return df[df[column].isin(selected)]
    return df


def bool_multiselect_filter(df, column, label, key):
    selected = st.multiselect(label, options=["YES", "NO"], default=[], key=key, placeholder="All")
    if not selected:
        return df
    conds = []
    if "YES" in selected:
        conds.append(df[column] == True)
    if "NO" in selected:
        conds.append(df[column] != True)
    if len(conds) == 1:
        return df[conds[0]]
    return df[conds[0] | conds[1]]


def find_missing_items(skus, df):
    found = set()
    for col in ["SKU", "LISTING_ID", "ASIN", "MPN", "MASTER_ID", "FNSKU"]:
        if col in df.columns:
            found.update(df[col].dropna().str.upper().tolist())
    return [s for s in skus if s.upper() not in found]


# ══════════════════════════════════════════════
# Sidebar
# ══════════════════════════════════════════════
with st.sidebar:
    st.markdown("## 📦 Product Catalogue Lookup")
    st.caption("Pattern — Merchandise Planning")

    # Usage tracker
    st.markdown(
        f'<div class="usage-badge">'
        f'<div class="num">{st.session_state["lookup_count"]}</div>'
        f'<div class="lbl">Lookups this session</div>'
        f'</div>',
        unsafe_allow_html=True,
    )
    if st.session_state["total_items_looked_up"] > 0:
        st.caption(f"📊 {st.session_state['total_items_looked_up']} total items looked up")

    st.markdown('<div class="sidebar-section"><h4>📖 About This Tool</h4><p>'
                'A one-stop lookup tool for the Merchandise Planning team to quickly check '
                'listing-level attributes across all marketplaces. Paste any identifier — SKU, '
                'Listing ID, ASIN, MPN, Master ID, or FNSKU — and instantly retrieve DNO status, '
                'shippable tags, fulfillment type, commingling details, pricing, and more. '
                'Powered by live Snowflake queries, the data is always fresh and up to date.</p></div>', unsafe_allow_html=True)

    st.markdown('<div class="sidebar-section"><h4>📖 How to Use</h4><p>'
                '1. Paste identifiers one per line, or upload a CSV/Excel file<br>'
                '2. Click Lookup to query Snowflake<br>'
                '3. Switch to the Results tab to view data<br>'
                '4. Use filters to narrow down results<br>'
                '5. Toggle columns to customise your view<br>'
                '6. Export to CSV when done</p></div>', unsafe_allow_html=True)

    st.markdown('<div class="sidebar-section"><h4>🔍 Supported Lookups</h4><p>'
                '• SKU (Marketplace Primary ID)<br>'
                '• Listing ID<br>'
                '• ASIN<br>'
                '• MPN<br>'
                '• Master ID<br>'
                '• FNSKU</p></div>', unsafe_allow_html=True)

    st.markdown('<div class="sidebar-section"><h4>ℹ️ Data Info</h4><p>'
                '• DNO date: Latest available<br>'
                '• Data source: Snowflake (live queries)<br>'
                '• Max 500 items per lookup</p></div>', unsafe_allow_html=True)

    st.markdown("---")

    # Feedback section
    st.markdown("#### 💬 Feedback")
    feedback_type = st.selectbox("Type", ["Report an issue", "Suggest a feature", "General feedback"], key="fb_type", label_visibility="collapsed")
    feedback_text = st.text_area("Your feedback", placeholder="Tell us what's on your mind...", height=80, key="fb_text", label_visibility="collapsed")
    if st.button("Send Feedback", use_container_width=True, key="fb_send"):
        if feedback_text.strip():
            st.success("✅ Thanks for your feedback!")
            # In future, this could send to Slack or email
        else:
            st.warning("Please enter some feedback first.")

    st.markdown("---")
    st.caption(f"v2.1 • {datetime.date.today().strftime('%B %Y')}")
    st.caption("Built by Merchandise Planning Team")


# ══════════════════════════════════════════════
# Header
# ══════════════════════════════════════════════
st.markdown('<div class="main-header"><div class="header-icon">📦</div><div>'
            '<p class="header-title">Product Catalogue Lookup</p>'
            '<p class="header-sub">Instantly check DNO, Shippable, Commingled, pricing & more across all marketplaces</p>'
            '</div></div>', unsafe_allow_html=True)

# ══════════════════════════════════════════════
# Region mapping
# ══════════════════════════════════════════════
REGION_MAP = {
    "UK": [
        "Amazon.co.uk", "TikTok UK", "Ebay UK", "Tesco",
    ],
    "EU": [
        "Amazon.de", "Amazon.fr", "Amazon.es", "Amazon.it", "Amazon.nl",
        "Amazon.pl", "Amazon.se", "Amazon.com.be", "Amazon.ie", "Amazon.com.tr",
        "Amazon.ae", "Amazon.sa", "Noon",
        "Bol.com", "Bol.com (BE)",
        "Cdiscount", "Ebay DE", "Ebay EU",
        "MediaMarkt", "Otto", "EU DTC", "EU Amazon", "Allegro",
        "Zalando DE", "Zalando ES", "Zalando FR", "Zalando AT", "Zalando IT",
        "Zalando CH", "Zalando BE", "Zalando NL", "Zalando PL", "Zalando SE",
        "Zalando DK", "Zalando FI", "Zalando LU",
    ],
}

# ══════════════════════════════════════════════
# Input + Results in tabs
# ══════════════════════════════════════════════
input_tab, brand_tab, fr_tab, results_tab = st.tabs(["🔍 Search by ID", "🏷️ Browse by Brand", "🔬 FR Check", "📊 Results"])

with input_tab:
    st.markdown("")
    paste_col, upload_col = st.columns(2)
    skus_to_lookup = []

    with paste_col:
        st.markdown("#### ✏️ Paste Items")
        sku_text = st.text_area(
            "Enter identifiers", placeholder="One item per line, e.g.\nUK-BOSCH-786700-COM\nL0NC2POW\nB0BXT6YCHK",
            height=220, label_visibility="collapsed",
        )
        if sku_text.strip():
            skus_to_lookup = [s.strip() for s in sku_text.strip().split("\n") if s.strip()]
        st.caption(f"{len(skus_to_lookup)} item(s) entered • Max 500")

    with upload_col:
        st.markdown("#### 📁 Upload File")
        uploaded_file = st.file_uploader("CSV or Excel", type=["csv", "xlsx", "xls"], label_visibility="collapsed")
        if uploaded_file:
            try:
                if uploaded_file.name.lower().endswith(".csv"):
                    upload_df = pd.read_csv(uploaded_file, dtype=str)
                else:
                    upload_df = pd.read_excel(uploaded_file, dtype=str, engine="openpyxl")
                skus_to_lookup = upload_df.iloc[:, 0].dropna().astype(str).str.strip().tolist()
                skus_to_lookup = [s for s in skus_to_lookup if s]
                st.success(f"📎 Loaded **{len(skus_to_lookup)}** items from `{uploaded_file.name}`")
            except Exception as e:
                st.error(f"Failed to read file: {e}")

    if skus_to_lookup:
        if len(skus_to_lookup) > 500:
            st.warning("⚠️ Max 500 items. Only first 500 processed.")
            skus_to_lookup = skus_to_lookup[:500]

        if st.button("🔍 Lookup", type="primary", use_container_width=True):
            # Progress bar animation
            progress_bar = st.progress(0, text="Connecting to Snowflake...")
            time.sleep(0.3)
            progress_bar.progress(15, text="Connected. Building query...")
            time.sleep(0.2)
            progress_bar.progress(30, text=f"Querying {len(skus_to_lookup)} item(s)...")

            try:
                df = run_lookup(skus_to_lookup)
                progress_bar.progress(80, text="Processing results...")
                time.sleep(0.2)

                if df.empty:
                    progress_bar.progress(100, text="Done — no results found.")
                    st.warning("No results found for the provided items.")
                    st.session_state["results_df"] = pd.DataFrame()
                else:
                    progress_bar.progress(100, text=f"Done — {len(df)} results found!")
                    st.session_state["results_df"] = df
                    st.session_state["skus_count"] = len(skus_to_lookup)
                    st.session_state["skus_list"] = skus_to_lookup
                    st.session_state["lookup_count"] += 1
                    st.session_state["total_items_looked_up"] += len(skus_to_lookup)
                    st.success(f"✅ Found **{len(df)}** results! Switch to the **📊 Results** tab.")

                time.sleep(0.5)
                progress_bar.empty()
            except Exception as e:
                progress_bar.empty()
                st.error(f"Query failed: {e}")


with brand_tab:
    st.markdown("")
    st.markdown("#### 🏷️ Browse All Listings for a Brand")
    st.caption("Select a brand and region to fetch all their listings. Use filters in the Results tab to narrow down further.")

    bb1, bb2, bb3 = st.columns(3)

    with bb1:
        # Fetch vendor list (cached)
        try:
            vendor_list = get_vendor_list()
        except Exception:
            vendor_list = []
        sel_brand = st.selectbox("Select Brand / Vendor", options=[""] + vendor_list,
                                  key="brand_select", placeholder="Start typing to search...")

    with bb2:
        brand_region = st.selectbox("Region", ["All", "UK", "EU"], key="brand_region")

    with bb3:
        result_limit = st.selectbox("Max Results", ["500", "1000", "2000", "5000", "No limit"],
                                     key="brand_limit")

    if sel_brand:
        # Determine region marketplaces
        region_mps = None
        if brand_region != "All":
            region_mps = REGION_MAP.get(brand_region, [])

        # Determine limit
        limit_val = None if result_limit == "No limit" else int(result_limit)

        if st.button("🏷️ Fetch All Listings", type="primary", use_container_width=True, key="brand_fetch"):
            progress_bar = st.progress(0, text="Connecting to Snowflake...")
            time.sleep(0.3)
            progress_bar.progress(20, text=f"Fetching listings for {sel_brand}...")
            time.sleep(0.2)
            progress_bar.progress(40, text="Querying across all marketplaces...")

            try:
                df = run_brand_lookup(sel_brand, region_mps, limit_val)
                progress_bar.progress(80, text="Processing results...")
                time.sleep(0.2)

                if df.empty:
                    progress_bar.progress(100, text="Done — no listings found.")
                    st.warning(f"No listings found for **{sel_brand}**" +
                              (f" in **{brand_region}** region." if brand_region != "All" else "."))
                    st.session_state["results_df"] = pd.DataFrame()
                else:
                    progress_bar.progress(100, text=f"Done — {len(df)} listings found!")
                    st.session_state["results_df"] = df
                    st.session_state["skus_count"] = len(df)
                    st.session_state["skus_list"] = []
                    st.session_state["lookup_count"] += 1
                    st.session_state["total_items_looked_up"] += len(df)
                    region_label = f" in **{brand_region}**" if brand_region != "All" else ""
                    st.success(f"✅ Found **{len(df)}** listings for **{sel_brand}**{region_label}! Switch to the **📊 Results** tab.")

                time.sleep(0.5)
                progress_bar.empty()
            except Exception as e:
                progress_bar.empty()
                st.error(f"Query failed: {e}")
    else:
        st.info("👆 Select a brand to get started.")


with fr_tab:
    st.markdown("")
    st.markdown("### 🔬 Functional Readiness Check")
    st.caption("Validate listing attributes against Pattern's readiness rules. Paste identifiers, pick what to check, and get pass/fail per listing.")
    st.markdown("")

    # ── INPUT CARD ──
    in_col1, in_col2 = st.columns([1, 4])
    with in_col1:
        fr_id_type = st.radio("ID Type", ["Listing ID", "SKU"], key="fr_id_type", label_visibility="collapsed")
    with in_col2:
        fr_text = st.text_area(
            "Identifiers",
            placeholder="Paste identifiers — one per line, or comma/space separated\ne.g.  L0NC2POW, L09SMWN7, L05RO0W8",
            height=100, key="fr_text", label_visibility="collapsed",
        )

    # Parse identifiers
    fr_ids = []
    if fr_text.strip():
        import re
        fr_ids = [s.strip() for s in re.split(r"[\n,\s\t]+", fr_text.strip()) if s.strip()]
        seen = set()
        fr_ids = [x for x in fr_ids if not (x in seen or seen.add(x))]
    st.caption(f"**{len(fr_ids)}** identifier(s) entered • Max 500")

    st.markdown("")

    # ── ATTRIBUTE PRESETS ──
    if "fr_preset" not in st.session_state:
        st.session_state["fr_preset"] = "quick"
    if "fr_custom_attrs" not in st.session_state:
        st.session_state["fr_custom_attrs"] = QUICK_CHECK_ATTRS.copy()

    st.markdown("**Attributes to Check**")
    p_col1, p_col2, p_col3, _ = st.columns([1, 1, 1, 3])
    with p_col1:
        if st.button(f"⚡ Quick Check ({len(QUICK_CHECK_ATTRS)})",
                     type="primary" if st.session_state["fr_preset"] == "quick" else "secondary",
                     use_container_width=True, key="fr_btn_quick"):
            st.session_state["fr_preset"] = "quick"
            st.rerun()
    with p_col2:
        if st.button(f"📋 All Attributes ({len(FR_ATTRIBUTES)})",
                     type="primary" if st.session_state["fr_preset"] == "all" else "secondary",
                     use_container_width=True, key="fr_btn_all"):
            st.session_state["fr_preset"] = "all"
            st.rerun()
    with p_col3:
        if st.button("🛠 Custom...",
                     type="primary" if st.session_state["fr_preset"] == "custom" else "secondary",
                     use_container_width=True, key="fr_btn_custom"):
            st.session_state["fr_preset"] = "custom"
            st.rerun()

    # Determine which attributes are selected based on preset
    if st.session_state["fr_preset"] == "quick":
        selected_attr_ids = QUICK_CHECK_ATTRS.copy()
        st.caption("✓ Commingled · Active · Shipable · Listing Prep Plan · Item Prep Plan · Can Expire · Glass · DNO Status")
    elif st.session_state["fr_preset"] == "all":
        selected_attr_ids = [a["id"] for a in FR_ATTRIBUTES]
        st.caption(f"✓ All {len(FR_ATTRIBUTES)} attributes (10 checks + 4 info)")
    else:  # custom
        attr_label_map = {a["label"]: a["id"] for a in FR_ATTRIBUTES}
        attr_id_to_label_local = {a["id"]: a["label"] for a in FR_ATTRIBUTES}
        default_labels = [attr_id_to_label_local[aid] for aid in st.session_state["fr_custom_attrs"] if aid in attr_id_to_label_local]
        selected_labels = st.multiselect(
            "Pick attributes",
            options=[a["label"] for a in FR_ATTRIBUTES],
            default=default_labels,
            key="fr_attr_select",
            label_visibility="collapsed",
        )
        selected_attr_ids = [attr_label_map[lbl] for lbl in selected_labels] if selected_labels else []
        st.session_state["fr_custom_attrs"] = selected_attr_ids
        st.caption(f"✓ {len(selected_attr_ids)} attribute(s) selected")

    st.markdown("")

    # ── RUN BUTTON ──
    if fr_ids and selected_attr_ids:
        if len(fr_ids) > 500:
            st.warning("⚠️ Max 500 identifiers. Only the first 500 will be processed.")
            fr_ids = fr_ids[:500]

        if st.button(f"▶ Run FR Check on {len(fr_ids)} listing(s)", type="primary", use_container_width=True, key="fr_run"):
            progress_bar = st.progress(0, text="Connecting to Snowflake...")
            time.sleep(0.2)
            progress_bar.progress(20, text=f"Fetching catalogue data for {len(fr_ids)} listing(s)...")

            try:
                id_type = "SKU" if fr_id_type == "SKU" else "LISTING_ID"
                fr_df = run_fr_lookup(fr_ids, id_type)
                progress_bar.progress(60, text="Running validation rules...")
                time.sleep(0.2)

                if fr_df.empty:
                    progress_bar.progress(100, text="No results found.")
                    st.warning(f"No catalogue data found for the given {fr_id_type}s.")
                    st.session_state.pop("fr_results", None)
                else:
                    fr_results = run_fr_check(fr_df, selected_attr_ids)
                    progress_bar.progress(100, text=f"Done — checked {len(fr_results)} listing(s)!")
                    st.session_state["fr_results"] = fr_results
                    st.session_state["fr_selected_attrs_at_run"] = selected_attr_ids
                    st.session_state["fr_input_ids"] = fr_ids
                    passed = sum(1 for r in fr_results if r["passed"])
                    st.success(f"✅ Done — {passed} passed, {len(fr_results) - passed} flagged.")

                time.sleep(0.3)
                progress_bar.empty()
            except Exception as e:
                progress_bar.empty()
                st.error(f"FR Check failed: {e}")
    elif fr_ids and not selected_attr_ids:
        st.info("Select at least one attribute to check.")
    elif not fr_ids:
        st.info("👆 Enter at least one identifier above to begin.")

    # ══════════════════════════════════════════════
    # RESULTS — CARD VIEW
    # ══════════════════════════════════════════════
    if "fr_results" in st.session_state and st.session_state["fr_results"]:
        results = st.session_state["fr_results"]
        attrs_at_run = st.session_state.get("fr_selected_attrs_at_run", [])
        input_ids = st.session_state.get("fr_input_ids", [])

        attr_id_to_label = {a["id"]: a["label"] for a in FR_ATTRIBUTES}
        attr_id_to_type = {a["id"]: a["type"] for a in FR_ATTRIBUTES}

        st.markdown("---")

        # Summary tiles
        total = len(results)
        passed = sum(1 for r in results if r["passed"])
        flagged = total - passed
        found_ids = set()
        for r in results:
            found_ids.add(str(r.get("listing_id", "")).upper())
            found_ids.add(str(r.get("sku", "")).upper())
        missing_ids = [i for i in input_ids if i.upper() not in found_ids]

        s1, s2, s3, s4 = st.columns(4)
        with s1:
            st.markdown(f'<div class="fr-summary-tile total"><div class="lbl">Total Checked</div><div class="val">{total}</div></div>', unsafe_allow_html=True)
        with s2:
            st.markdown(f'<div class="fr-summary-tile passed"><div class="lbl">Passed</div><div class="val">{passed}</div></div>', unsafe_allow_html=True)
        with s3:
            st.markdown(f'<div class="fr-summary-tile flagged"><div class="lbl">Flagged</div><div class="val">{flagged}</div></div>', unsafe_allow_html=True)
        with s4:
            st.markdown(f'<div class="fr-summary-tile missing"><div class="lbl">Not Found</div><div class="val">{len(missing_ids)}</div></div>', unsafe_allow_html=True)

        st.markdown("")

        # Missing items
        if missing_ids:
            with st.expander(f"⚠️ {len(missing_ids)} identifier(s) not found in catalogue", expanded=False):
                missing_html = "".join(f'<span class="missing-item">{m}</span>' for m in missing_ids)
                st.markdown(missing_html, unsafe_allow_html=True)

        # Filter
        fr_filter = st.radio("Show", [f"All ({total})", f"✅ Passed ({passed})", f"⛔ Flagged ({flagged})"],
                             key="fr_filter", horizontal=True, label_visibility="collapsed")

        if "Passed" in fr_filter:
            shown = [r for r in results if r["passed"]]
        elif "Flagged" in fr_filter:
            shown = [r for r in results if not r["passed"]]
        else:
            shown = results

        st.caption(f"Showing **{len(shown)}** of **{total}** results")

        # ── Listing cards ──
        for idx, r in enumerate(shown):
            card_class = "passed" if r["passed"] else "flagged"
            status_class = "ok" if r["passed"] else "bad"
            status_text = "✅ PASSED" if r["passed"] else f"⛔ FLAGGED · {r['red_count']} issue{'s' if r['red_count'] != 1 else ''}"

            # Build attribute dots strip
            dots_html = ""
            for attr_id in attrs_at_run:
                det = r["details"].get(attr_id, {})
                status = det.get("status", "n")
                # Skip info-only on the dot strip (less visual noise)
                if attr_id_to_type.get(attr_id) == "info":
                    continue
                icon = {"g": "✓", "r": "✕", "i": "·"}.get(status, "·")
                lbl = attr_id_to_label.get(attr_id, attr_id)
                tooltip = f"{lbl}: {det.get('text', '')}"
                # Use html escape for tooltip
                tooltip_escaped = tooltip.replace('"', "&quot;").replace("<", "&lt;").replace(">", "&gt;")
                dots_html += f'<span class="fr-attr-dot {status}" title="{tooltip_escaped}">{icon}</span>'

            # Card header
            product_name = str(r["product_name"])[:80] if r["product_name"] else "(no product name)"
            card_html = f'''
            <div class="fr-card {card_class}">
                <div class="fr-card-head">
                    <div>
                        <div class="fr-card-title">{product_name}</div>
                        <div class="fr-card-meta"><b>{r["listing_id"]}</b> • {r["sku"] or "—"} • {r["marketplace"] or "—"} • {r["country_code"] or "—"} • {r["vendor"] or "—"}</div>
                    </div>
                    <div class="fr-card-status">
                        <div class="fr-attr-strip">{dots_html}</div>
                        <div class="fr-status-pill {status_class}">{status_text}</div>
                    </div>
                </div>
            </div>
            '''
            st.markdown(card_html, unsafe_allow_html=True)

            # Streamlit expander for details (below the card)
            with st.expander(f"🔍 View details for {r['listing_id']}", expanded=False):
                # Build attribute grid
                attr_rows_html = '<div class="fr-attr-grid">'
                for attr_id in attrs_at_run:
                    det = r["details"].get(attr_id, {})
                    status = det.get("status", "n")
                    mark = {"g": "✅", "r": "⛔", "i": "ℹ️", "n": "—"}.get(status, "—")
                    lbl = attr_id_to_label.get(attr_id, attr_id)
                    val = det.get("value", "") or "—"
                    note = det.get("text", "")
                    # Show note if it adds info beyond the value
                    display_val = val if note == val or note in ("Active ✓", "Shipable ✓", "Not DNO ✓") else f"{val} — {note}"
                    if status == "i":
                        display_val = val
                    display_val_escaped = str(display_val).replace("<", "&lt;").replace(">", "&gt;")
                    lbl_escaped = lbl.replace("<", "&lt;").replace(">", "&gt;")
                    attr_rows_html += f'''
                    <div class="fr-attr-row {status}">
                        <span class="mk">{mark}</span>
                        <span class="nm">{lbl_escaped}</span>
                        <span class="vl">{display_val_escaped}</span>
                    </div>
                    '''
                attr_rows_html += '</div>'
                st.markdown(attr_rows_html, unsafe_allow_html=True)

        # ── Exports ──
        st.markdown("")
        st.markdown("##### 📤 Export Results")
        ex_c1, ex_c2, ex_c3 = st.columns(3)

        def build_export_df(rows_filter):
            rows_out = []
            for r in results:
                if rows_filter == "pass" and not r["passed"]:
                    continue
                if rows_filter == "fail" and r["passed"]:
                    continue
                base = {
                    "Listing ID": r["listing_id"],
                    "SKU": r["sku"],
                    "Product Name": r["product_name"],
                    "Marketplace": r["marketplace"],
                    "Country": r["country_code"],
                    "Vendor": r["vendor"],
                }
                for attr_id in attrs_at_run:
                    label = attr_id_to_label.get(attr_id, attr_id)
                    det = r["details"].get(attr_id, {})
                    if rows_filter == "pass":
                        if det.get("status") in ("g", "i"):
                            base[f"{label} — Value"] = det.get("value", "")
                            base[f"{label} — Status"] = "Pass" if det["status"] == "g" else "Info"
                            base[f"{label} — Note"] = det.get("text", "")
                        else:
                            base[f"{label} — Value"] = ""
                            base[f"{label} — Status"] = ""
                            base[f"{label} — Note"] = ""
                    else:
                        if det.get("status") == "r":
                            base[f"{label} — Value"] = det.get("value", "")
                            base[f"{label} — Status"] = "Fail"
                            base[f"{label} — Note"] = det.get("text", "")
                        else:
                            base[f"{label} — Value"] = ""
                            base[f"{label} — Status"] = ""
                            base[f"{label} — Note"] = ""
                rows_out.append(base)
            return pd.DataFrame(rows_out)

        passed_df = build_export_df("pass")
        flagged_df = build_export_df("fail")

        with ex_c1:
            st.download_button(
                f"⬇️ Passed ({len(passed_df)}) — CSV",
                passed_df.to_csv(index=False),
                f"fr_passed_{datetime.date.today().isoformat()}.csv",
                "text/csv", use_container_width=True,
            )
        with ex_c2:
            st.download_button(
                f"⬇️ Flagged ({len(flagged_df)}) — CSV",
                flagged_df.to_csv(index=False),
                f"fr_flagged_{datetime.date.today().isoformat()}.csv",
                "text/csv", use_container_width=True,
            )
        with ex_c3:
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                passed_df.to_excel(writer, sheet_name="Passed", index=False)
                flagged_df.to_excel(writer, sheet_name="Flagged", index=False)
            st.download_button(
                "⬇️ Both — Excel (2 sheets)",
                buf.getvalue(),
                f"fr_check_{datetime.date.today().isoformat()}.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )


with results_tab:
    if "results_df" not in st.session_state or st.session_state.get("results_df", pd.DataFrame()).empty:
        st.info("👈 Use **Search by ID** or **Browse by Brand** tab, then check results here.")
    else:
        df = st.session_state["results_df"].copy()
        skus_count = st.session_state.get("skus_count", 0)
        skus_list = st.session_state.get("skus_list", [])

        # ── Missing items ──
        missing = find_missing_items(skus_list, df)
        if missing:
            with st.expander(f"⚠️ {len(missing)} item(s) returned no results — click to see", expanded=False):
                missing_html = "".join(f'<span class="missing-item">{m}</span>' for m in missing)
                st.markdown(missing_html, unsafe_allow_html=True)

        # ── Colored summary cards ──
        dno_count = int((df["IS_DNO"] == True).sum()) if "IS_DNO" in df.columns else 0
        shippable_count = int((df["SHIPPABLE_TAG"] == True).sum()) if "SHIPPABLE_TAG" in df.columns else 0
        fba_count = int((df["LISTING_FULFILLMENT_TYPE"].str.upper() == "FBA").sum()) if "LISTING_FULFILLMENT_TYPE" in df.columns else 0
        active_count = int((df["IS_ACTIVE"] == True).sum()) if "IS_ACTIVE" in df.columns else 0

        mc1, mc2, mc3, mc4, mc5, mc6 = st.columns(6)
        with mc1: st.markdown(f'<div class="metric-card mc-total"><div class="label">Total</div><div class="value">{len(df)}</div></div>', unsafe_allow_html=True)
        with mc2: st.markdown(f'<div class="metric-card mc-dno"><div class="label">DNO = True</div><div class="value">{dno_count}</div></div>', unsafe_allow_html=True)
        with mc3: st.markdown(f'<div class="metric-card mc-ship"><div class="label">Shippable</div><div class="value">{shippable_count}</div></div>', unsafe_allow_html=True)
        with mc4: st.markdown(f'<div class="metric-card mc-noship"><div class="label">Not Shippable</div><div class="value">{len(df) - shippable_count}</div></div>', unsafe_allow_html=True)
        with mc5: st.markdown(f'<div class="metric-card mc-fba"><div class="label">FBA</div><div class="value">{fba_count}</div></div>', unsafe_allow_html=True)
        with mc6: st.markdown(f'<div class="metric-card mc-active"><div class="label">Active</div><div class="value">{active_count}</div></div>', unsafe_allow_html=True)

        st.markdown("")

        # ── Filters ──
        st.markdown("### 🔽 Filters")
        search_text = st.text_input("🔍 Search across all fields", placeholder="Type to search...", key="search_all")

        # Region filter row
        reg_col, mp_col, vn_col, dno_col = st.columns(4)
        filtered = df.copy()

        with reg_col:
            sel_region = st.selectbox("🌍 Region", ["All", "UK", "EU"], key="f_region")
            if sel_region != "All":
                region_mps = REGION_MAP.get(sel_region, [])
                filtered = filtered[filtered["MARKETPLACE"].isin(region_mps)]

        with mp_col:
            # Marketplace filter shows only marketplaces available after region filter
            mp_vals = sorted(filtered["MARKETPLACE"].dropna().unique().tolist())
            sel_mps = st.multiselect("Marketplace", options=mp_vals, default=[], key="f_mp", placeholder="All")
            if sel_mps:
                filtered = filtered[filtered["MARKETPLACE"].isin(sel_mps)]

        with vn_col: filtered = multiselect_filter(filtered, "VENDOR", "Vendor", "f_vn")
        with dno_col: filtered = bool_multiselect_filter(filtered, "IS_DNO", "DNO", "f_dno")

        r1c1, r1c2, r1c3, r1c4 = st.columns(4)
        with r1c1: filtered = bool_multiselect_filter(filtered, "SHIPPABLE_TAG", "Shippable", "f_ship")
        with r1c2: filtered = multiselect_filter(filtered, "LISTING_FULFILLMENT_TYPE", "Fulfillment Type", "f_ff")
        with r1c3: filtered = multiselect_filter(filtered, "LISTING_TYPE", "Listing Type", "f_lt")
        with r1c4: filtered = multiselect_filter(filtered, "COMMINGLED_STATUS", "Commingled", "f_cm")

        r2c1, r2c2, r2c3, r2c4 = st.columns(4)
        with r2c1:
            if "IS_ACTIVE" in filtered.columns:
                filtered = bool_multiselect_filter(filtered, "IS_ACTIVE", "Active", "f_active")
        with r2c2:
            if "IS_DISCONTINUED" in filtered.columns:
                filtered = bool_multiselect_filter(filtered, "IS_DISCONTINUED", "Discontinued", "f_disc")
        with r2c3:
            if "CAN_EXPIRE" in filtered.columns:
                filtered = bool_multiselect_filter(filtered, "CAN_EXPIRE", "Can Expire", "f_expire")
        with r2c4:
            if "DNO_REASON_CODE" in filtered.columns:
                filtered = multiselect_filter(filtered, "DNO_REASON_CODE", "DNO Reason Code", "f_dno_rc")

        if search_text.strip():
            mask = filtered.astype(str).apply(
                lambda row: row.str.contains(search_text.strip(), case=False).any(), axis=1
            )
            filtered = filtered[mask]

        st.caption(f"Showing **{len(filtered)}** of **{len(df)}** results")

        # ── Quick copy column buttons ──
        with st.expander("📋 Quick Copy — grab a full column of values"):
            copy_cols = {"SKU": "SKU", "LISTING_ID": "Listing ID", "ASIN": "ASIN", "MPN": "MPN", "MASTER_ID": "Master ID", "FNSKU": "FNSKU"}
            cc_cols = st.columns(len(copy_cols))
            for i, (col_key, col_label) in enumerate(copy_cols.items()):
                with cc_cols[i]:
                    if col_key in filtered.columns:
                        vals = filtered[col_key].dropna().unique().tolist()
                        copy_text = "\n".join(str(v) for v in vals)
                        st.download_button(
                            f"📋 {col_label} ({len(vals)})",
                            copy_text,
                            f"{col_key.lower()}_values.txt",
                            "text/plain",
                            use_container_width=True,
                            key=f"copy_{col_key}",
                        )

        # ── Column visibility ──
        available_cols = [k for k in COLUMN_MAP if k in filtered.columns]
        default_cols = [k for k in available_cols if COLUMN_MAP[k]["default"]]
        friendly_options = {COLUMN_MAP[k]["label"]: k for k in available_cols}

        with st.expander("👁 Toggle Columns"):
            selected_friendly = st.multiselect(
                "Choose columns to display",
                options=[COLUMN_MAP[k]["label"] for k in available_cols],
                default=[COLUMN_MAP[k]["label"] for k in default_cols],
                key="col_select",
            )
        selected_cols = [friendly_options[f] for f in selected_friendly] if selected_friendly else default_cols

        # ── Format display ──
        display_df = filtered[selected_cols].copy()
        rename_map = {k: COLUMN_MAP[k]["label"] for k in selected_cols if k in COLUMN_MAP}
        display_df = display_df.rename(columns=rename_map)

        for col_key, (true_label, false_label) in BOOL_COLS.items():
            friendly_name = COLUMN_MAP.get(col_key, {}).get("label", col_key)
            if friendly_name in display_df.columns:
                display_df[friendly_name] = display_df[friendly_name].apply(
                    lambda x, tl=true_label, fl=false_label: tl if x == True else fl
                )

        # ── Advanced conditional formatting ──
        dno_friendly = COLUMN_MAP["IS_DNO"]["label"]
        ship_friendly = COLUMN_MAP["SHIPPABLE_TAG"]["label"]
        active_friendly = COLUMN_MAP["IS_ACTIVE"]["label"]
        disc_friendly = COLUMN_MAP["IS_DISCONTINUED"]["label"]

        def color_rows(row):
            n = len(row)
            # Priority: DNO > Discontinued > Inactive > Shippable
            if dno_friendly in row.index and "⛔ YES — DNO" in str(row.get(dno_friendly, "")):
                return ["background-color: rgba(239,68,68,0.10)"] * n  # Red
            if disc_friendly in row.index and "⛔ Discontinued" in str(row.get(disc_friendly, "")):
                return ["background-color: rgba(245,158,11,0.08)"] * n  # Orange
            if active_friendly in row.index and "❌ Inactive" in str(row.get(active_friendly, "")):
                return ["background-color: rgba(100,116,139,0.10)"] * n  # Grey
            if ship_friendly in row.index and "✅ YES" in str(row.get(ship_friendly, "")):
                return ["background-color: rgba(34,197,94,0.05)"] * n  # Green
            return [""] * n

        # Skip row coloring for large datasets (Styler has a row limit ~262k cells)
        STYLE_LIMIT = 5000
        if len(display_df) > STYLE_LIMIT:
            st.info(f"📊 Showing {len(display_df):,} rows — row coloring is disabled for large datasets to keep the app responsive. Use filters to narrow results if you want colored rows.")
            st.dataframe(
                display_df,
                use_container_width=True, hide_index=True,
                height=600,
            )
        else:
            st.dataframe(
                display_df.style.apply(color_rows, axis=1),
                use_container_width=True, hide_index=True,
                height=min(len(display_df) * 38 + 40, 600),
            )

        # ── Export & Copy ──
        st.markdown("### 📤 Export & Copy")

        ex1, ex2, ex3, ex4 = st.columns(4)

        # CSV exports
        with ex1:
            st.download_button(
                "⬇️ All — CSV",
                df[selected_cols].rename(columns=rename_map).to_csv(index=False),
                f"catalogue_lookup_all_{datetime.date.today().isoformat()}.csv",
                "text/csv", use_container_width=True,
            )
        with ex2:
            st.download_button(
                "⬇️ Filtered — CSV",
                filtered[selected_cols].rename(columns=rename_map).to_csv(index=False),
                f"catalogue_lookup_filtered_{datetime.date.today().isoformat()}.csv",
                "text/csv", use_container_width=True,
            )

        # Excel exports
        with ex3:
            buffer_all = io.BytesIO()
            df[selected_cols].rename(columns=rename_map).to_excel(buffer_all, index=False, engine="openpyxl")
            st.download_button(
                "⬇️ All — Excel",
                buffer_all.getvalue(),
                f"catalogue_lookup_all_{datetime.date.today().isoformat()}.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        with ex4:
            buffer_filt = io.BytesIO()
            filtered[selected_cols].rename(columns=rename_map).to_excel(buffer_filt, index=False, engine="openpyxl")
            st.download_button(
                "⬇️ Filtered — Excel",
                buffer_filt.getvalue(),
                f"catalogue_lookup_filtered_{datetime.date.today().isoformat()}.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )

        # Copy to clipboard — one-click button using HTML/JS component
        st.markdown("")
        with st.expander("📋 Copy to Clipboard — one click, includes headers"):
            copy_df = filtered[selected_cols].rename(columns=rename_map)
            tsv_text = copy_df.to_csv(index=False, sep="\t")

            # Escape for embedding in JS
            tsv_escaped = (
                tsv_text.replace("\\", "\\\\")
                .replace("`", "\\`")
                .replace("$", "\\$")
            )

            row_count = len(copy_df)
            col_count = len(copy_df.columns)

            copy_html = f"""
            <div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;">
                <button id="copy-btn" onclick="copyToClipboard()" style="
                    background:linear-gradient(135deg,#3b82f6,#6366f1);
                    color:white;border:none;border-radius:8px;
                    padding:10px 20px;font-size:14px;font-weight:600;
                    cursor:pointer;box-shadow:0 2px 8px rgba(59,130,246,0.3);
                    transition:all 0.2s;
                ">📋 Copy {row_count} rows × {col_count} columns</button>
                <span id="copy-status" style="font-size:13px;color:#64748b;"></span>
            </div>
            <textarea id="copy-data" style="position:absolute;left:-9999px;">{tsv_text}</textarea>
            <script>
                function copyToClipboard() {{
                    const textarea = document.getElementById('copy-data');
                    const status = document.getElementById('copy-status');
                    const btn = document.getElementById('copy-btn');
                    textarea.select();
                    textarea.setSelectionRange(0, 99999);
                    try {{
                        navigator.clipboard.writeText(textarea.value).then(() => {{
                            status.innerHTML = '✅ Copied! Paste into Excel or Google Sheets.';
                            status.style.color = '#22c55e';
                            btn.style.background = 'linear-gradient(135deg,#22c55e,#16a34a)';
                            setTimeout(() => {{
                                status.innerHTML = '';
                                btn.style.background = 'linear-gradient(135deg,#3b82f6,#6366f1)';
                            }}, 3000);
                        }});
                    }} catch (err) {{
                        document.execCommand('copy');
                        status.innerHTML = '✅ Copied!';
                        status.style.color = '#22c55e';
                    }}
                }}
            </script>
            """
            st.components.v1.html(copy_html, height=60)
            st.caption(f"Click the button above — it copies {row_count} rows with column headers, ready to paste into Excel or Google Sheets.")

            # Preview/fallback: show the text directly (no nested expander)
            st.markdown("**Manual copy fallback** (if the button doesn't work):")
            st.code(tsv_text, language=None)
