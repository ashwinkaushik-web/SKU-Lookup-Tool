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
import re
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
    /* ── Inventory Summary Cards (2-row layout, PFS/FBA split) ── */
    .inv-card {
        background:#161b22;
        border:1px solid #30363d;
        border-radius:10px;
        padding:14px 16px;
        height:100%;
        min-height:110px;
        display:flex;
        flex-direction:column;
        justify-content:center;
    }
    .inv-card .inv-card-label {font-size:15px;color:#7d8590;text-transform:uppercase;letter-spacing:0.4px;font-weight:600;margin-bottom:8px;}
    .inv-card .inv-card-row {display:flex;justify-content:space-between;align-items:baseline;font-size:12px;padding:3px 0;}
    .inv-card .inv-card-row .ch {color:#94a3b8;font-weight:600;font-size:15px;}
    .inv-card .inv-card-row .v {font-weight:700;font-size:22px;color:#e6edf3;}
    .inv-card .inv-card-row .v.pfs {color:#ec4899;}
    .inv-card .inv-card-row .v.fba {color:#f59e0b;}
    .inv-card .inv-card-row .v.master {color:#a855f7;font-size:36px;}
    .inv-card .inv-card-row .v.dno {color:#f85149;}
    .inv-card .inv-card-row .v.muted {color:#525965;font-weight:500;font-size:16px;}
    .inv-card.clickable {cursor:pointer;transition:border-color 0.15s, background 0.15s;}
    .inv-card.clickable:hover {border-color:#58a6ff;background:rgba(59,130,246,0.05);}
    .inv-card.active {border-color:#58a6ff;background:rgba(59,130,246,0.10);}
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════
# Column config
# ══════════════════════════════════════════════
COLUMN_MAP = {
    # ── PRIORITY COLUMNS (appear first in the table) ──
    "SKU": {"label": "SKU", "default": True},
    "LISTING_ID": {"label": "Listing ID", "default": True},
    "ASIN": {"label": "ASIN", "default": True},
    "MASTER_ID": {"label": "Master ID", "default": True},
    "MPN": {"label": "MPN", "default": True},
    "MARKETPLACE": {"label": "Marketplace", "default": True},
    "VENDOR": {"label": "Vendor", "default": True},
    "MARKETPLACE_SELLER": {"label": "Marketplace Seller", "default": True},
    "LISTING_FULFILLMENT_TYPE": {"label": "Fulfillment Type", "default": True},
    "IS_DNO": {"label": "DNO", "default": True},
    "DNO_REASON_CODE": {"label": "DNO Reason Code", "default": True},
    "DNO_NOTE": {"label": "DNO Note", "default": True},
    "SHIPPABLE_TAG": {"label": "Shippable", "default": True},
    # ── SECONDARY COLUMNS (everything else) ──
    "PRODUCT_NAME": {"label": "Product Name", "default": True},
    "LISTING_TYPE": {"label": "Listing Type", "default": True},
    "FNSKU": {"label": "FNSKU", "default": True},
    "COMMINGLED_STATUS": {"label": "Commingled", "default": True},
    "IS_ACTIVE": {"label": "Active", "default": True},
    "IS_DISCONTINUED": {"label": "Discontinued", "default": True},
    "UPC": {"label": "UPC", "default": True},
    "EAN": {"label": "EAN", "default": True},
    "CAN_EXPIRE": {"label": "Can Expire", "default": True},
    "WHOLESALE_PRICE": {"label": "Wholesale Price", "default": True},
    "MAP_PRICE": {"label": "MAP Price", "default": True},
    "RETAIL_PRICE": {"label": "Retail Price", "default": True},
    "MSRP_PRICE": {"label": "MSRP Price", "default": True},
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
    # ── PERFORMANCE NOTE ──
    # The original query did:
    #   q1 = full scan of LISTINGS + 5 joins  (millions of rows)
    #   q2 = full scan of catalog view        (millions of rows)
    #   q3 = full scan of status history
    #   base = FULL OUTER JOIN of all three   (millions of rows)
    #   filter at the end → had to materialize everything first → SLOW (60+ sec)
    #
    # Now we push the filter into q1 and q2 directly, so each CTE only fetches
    # the small set of rows matching the user's input — typically 1-50 rows.
    # The joins now operate on these tiny sets — turns 60sec → ~2-5sec.
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
    -- PUSHED-DOWN FILTER: only fetch listings that match the user's input on any identifier
    WHERE UPPER(a.Listing_MP_Primary_ID) IN ({upper_list})
       OR UPPER(a.LISTING_ID) IN ({upper_list})
       OR UPPER(a.LISTING_MP_PAGE_ID) IN ({upper_list})
       OR UPPER(a.LISTING_MP_SECONDARY_ID) IN ({upper_list})
       OR UPPER(b.MASTER_ID) IN ({upper_list})
       OR UPPER(b.MPN) IN ({upper_list})
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
        pc.MSRP_W_CURRENCY AS msrp_price,
        pc.SELLER_NAME AS seller_name
    FROM PATTERN_DB.PUBLIC.PRODUCT_CATALOG_PRODUCTS_AND_LISTINGS_VIEW pc
    -- PUSHED-DOWN FILTER: fetch catalog rows matching the user's input directly, OR any
    -- listing q1 already resolved. Without the last clause, searching by Master ID / MPN /
    -- ASIN / FNSKU returned blank Shippable / Active / Product Name / prices, because q2
    -- only matches SKU / Listing ID / UPC / EAN. q1 resolves ALL identifier types to
    -- listing_ids, so this makes q2 pull its columns for exactly those listings.
    WHERE UPPER(pc.MARKETPLACE_PRIMARY_ID) IN ({upper_list})
       OR UPPER(pc.LISTING_ID) IN ({upper_list})
       OR UPPER(pc.UPC) IN ({upper_list})
       OR UPPER(pc.EAN) IN ({upper_list})
       OR pc.LISTING_ID IN (SELECT listing_id FROM q1 WHERE listing_id IS NOT NULL)
),
q3 AS (
    -- Restrict to only listings that appeared in q1 or q2 (small set), and only latest date
    SELECT h.LISTING_ID AS listing_id, h.IS_DNO AS is_dno
    FROM PATTERN_DB.PUBLIC.CATALOG_LISTING_STATUS_HISTORY h
    WHERE h."DATE" = (SELECT MAX("DATE") FROM PATTERN_DB.PUBLIC.CATALOG_LISTING_STATUS_HISTORY)
      AND h.LISTING_ID IN (
          SELECT listing_id FROM q1 WHERE listing_id IS NOT NULL
          UNION
          SELECT listing_id FROM q2 WHERE listing_id IS NOT NULL
      )
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
        q1.dno_reason_code AS DNO_REASON_CODE,
        q2.seller_name AS MARKETPLACE_SELLER
    FROM q1 FULL OUTER JOIN q2 ON q1.listing_id = q2.listing_id
    LEFT JOIN q3 ON q3.listing_id = COALESCE(q1.listing_id, q2.listing_id)
)
SELECT * FROM base
WHERE UPPER(SKU) IN ({upper_list}) OR UPPER(LISTING_ID) IN ({upper_list})
   OR UPPER(ASIN) IN ({upper_list}) OR UPPER(MPN) IN ({upper_list})
   OR UPPER(MASTER_ID) IN ({upper_list}) OR UPPER(FNSKU) IN ({upper_list})
ORDER BY MARKETPLACE, VENDOR, SKU
"""


@st.cache_data(ttl=300, show_spinner=False)
def _cached_run_lookup(skus_tuple):
    """Internal cached version of PC Lookup. Returns DataFrame."""
    skus = list(skus_tuple)
    conn = get_connection()
    return pd.read_sql(build_query(skus), conn)


def run_lookup(skus):
    """Run PC Lookup query for given SKUs / Listing IDs / ASINs / MPNs / Master IDs / FNSKUs (cached 5 min)."""
    return _cached_run_lookup(tuple(sorted(set(s.strip() for s in skus if s.strip()))))


def build_brand_query(vendor, region_marketplaces=None, limit=None):
    """Build query to fetch all listings for a brand/vendor, optionally filtered by region."""
    safe_vendor = vendor.replace("'", "''")
    mp_filter_q1 = ""
    mp_filter_q2 = ""
    if region_marketplaces:
        mp_list = ", ".join(f"UPPER('{mp}')" for mp in region_marketplaces)
        mp_filter_q1 = f"AND UPPER(c.name) IN ({mp_list})"
        mp_filter_q2 = f"AND UPPER(pc.MARKETPLACE_NAME) IN ({mp_list})"
    limit_clause = f"LIMIT {limit}" if limit else ""
    # ── PERFORMANCE NOTE ──
    # The vendor filter is pushed into BOTH q1 (via partners.name) and q2 (via VENDOR_NAME)
    # so each CTE only fetches rows for this specific brand instead of full table scans.
    # Same trick as build_query() — turns slow brand fetches into fast ones.
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
    -- PUSHED-DOWN FILTER: only fetch listings for this brand
    WHERE UPPER(par.name) = UPPER('{safe_vendor}')
    {mp_filter_q1}
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
        pc.MSRP_W_CURRENCY AS msrp_price,
        pc.SELLER_NAME AS seller_name
    FROM PATTERN_DB.PUBLIC.PRODUCT_CATALOG_PRODUCTS_AND_LISTINGS_VIEW pc
    -- PUSHED-DOWN FILTER: only fetch catalog rows for this brand
    WHERE UPPER(pc.VENDOR_NAME) = UPPER('{safe_vendor}')
    {mp_filter_q2}
),
q3 AS (
    -- Only fetch history for listings we actually need (q1 + q2)
    SELECT h.LISTING_ID AS listing_id, h.IS_DNO AS is_dno
    FROM PATTERN_DB.PUBLIC.CATALOG_LISTING_STATUS_HISTORY h
    WHERE h."DATE" = (SELECT MAX("DATE") FROM PATTERN_DB.PUBLIC.CATALOG_LISTING_STATUS_HISTORY)
      AND h.LISTING_ID IN (
          SELECT listing_id FROM q1 WHERE listing_id IS NOT NULL
          UNION
          SELECT listing_id FROM q2 WHERE listing_id IS NOT NULL
      )
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
        q1.dno_reason_code AS DNO_REASON_CODE,
        q2.seller_name AS MARKETPLACE_SELLER
    FROM q1 FULL OUTER JOIN q2 ON q1.listing_id = q2.listing_id
    LEFT JOIN q3 ON q3.listing_id = COALESCE(q1.listing_id, q2.listing_id)
)
SELECT * FROM base
WHERE UPPER(VENDOR) = UPPER('{safe_vendor}')
ORDER BY MARKETPLACE, SKU
{limit_clause}
"""


@st.cache_data(ttl=300, show_spinner=False)
def _cached_run_brand_lookup(vendor, region_marketplaces_tuple, limit):
    """Internal cached version of brand lookup."""
    region_marketplaces = list(region_marketplaces_tuple) if region_marketplaces_tuple else None
    conn = get_connection()
    return pd.read_sql(build_brand_query(vendor, region_marketplaces, limit), conn)


def run_brand_lookup(vendor, region_marketplaces=None, limit=None):
    return _cached_run_brand_lookup(
        vendor.strip(),
        tuple(sorted(region_marketplaces)) if region_marketplaces else None,
        limit if limit else 0,
    )


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
    {"id": "LISTING_IS_EXPIRABLE", "label": "Listing Is Expirable", "type": "check"},
    {"id": "IS_GLASS", "label": "Is Glass", "type": "info"},
    {"id": "ENROLLED_IN_TRANSPARENCY_PROGRAM", "label": "Enrolled In Transparency", "type": "check"},
    {"id": "ITEM_REQUIRES_TRANSPARENCY_LABEL", "label": "Requires Transparency Label", "type": "info"},
    {"id": "IS_BUNDLE", "label": "Is Bundle", "type": "info"},
    {"id": "IS_SOLD_INDIVIDUALLY", "label": "Is Sold Individually", "type": "info"},
    {"id": "LISTING_OPT_OUT_OF_AUTOMATED_WOI_CREATION", "label": "Opt Out of Auto WOI Creation", "type": "info"},
    {"id": "CATALOG_DNO_STATUS", "label": "DNO Status", "type": "info"},
    {"id": "IS_TEMPORARY", "label": "Is Temporary", "type": "info"},
    {"id": "MARKETPLACE_SECONDARY_ID_TYPE", "label": "Secondary ID Type", "type": "info"},
]

# Columns not (yet) available live in Snowflake — pulled as blank so the attribute
# still displays; dependent rules degrade gracefully (never false-fail on blanks).
FR_UNAVAILABLE_COLS = [
    "ENROLLED_IN_TRANSPARENCY_PROGRAM",
    "ITEM_REQUIRES_TRANSPARENCY_LABEL",
    "IS_SOLD_INDIVIDUALLY",
]

# Quick Check preset = the check-type attributes only (no info-only)
QUICK_CHECK_ATTRS = [
    "LISTING_IS_COMMINGLED", "IS_ACTIVE", "LISTING_IS_SHIPABLE",
    "LISTING_PREP_PLAN_ID", "ITEM_PREP_PLAN_ID", "CAN_EXPIRE",
    "LISTING_IS_EXPIRABLE", "ENROLLED_IN_TRANSPARENCY_PROGRAM",
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
    pc.MPN,
    pc.IS_ACTIVE,
    pc.IS_DISCONTINUED,
    pc.LISTING_IS_SHIPABLE,
    pc.CAN_EXPIRE,
    pc.IS_HAZMAT,
    pc.IS_GLASS,
    pc.IS_BUNDLE,
    pc.LISTING_PREP_PLAN_ID,
    pc.PRODUCT_DIMENSIONS,
    pc.CASEPACK_DIMENSIONS,
    pcl.LISTING_IS_COMMINGLED,
    dno.CATALOG_DNO_STATUS,
    dno.IS_TEMPORARY,
    li.ITEM_PREP_PLAN_ID,
    inm.LISTING_IS_OPT_OUT_OF_AUTO_WOI_CREATION AS LISTING_OPT_OUT_OF_AUTOMATED_WOI_CREATION,
    pc.CAN_EXPIRE AS LISTING_IS_EXPIRABLE,
    CAST(NULL AS VARCHAR) AS ENROLLED_IN_TRANSPARENCY_PROGRAM,
    CAST(NULL AS VARCHAR) AS ITEM_REQUIRES_TRANSPARENCY_LABEL,
    CAST(NULL AS VARCHAR) AS IS_SOLD_INDIVIDUALLY
FROM PATTERN_DB.PUBLIC.PRODUCT_CATALOG_PRODUCTS_AND_LISTINGS_VIEW pc
LEFT JOIN PATTERN_DB.PUBLIC.PRODUCT_CATALOG_PRODUCTS_AND_LISTINGS_VIEW_LITE pcl
    ON pc.LISTING_ID = pcl.LISTING_ID
LEFT JOIN PATTERN_DB.PUBLIC.PRODUCT_CATALOG_CURRENT_DNO_STATUS dno
    ON pc.LISTING_ID = dno.LISTING_ID
LEFT JOIN ANALYTICS_DB.STG_CATALOG.STG_CATALOG__LISTINGS li
    ON pc.LISTING_ID = li.LISTING_ID
LEFT JOIN PATTERN_DB.OPERATIONS.ITEMS_NORMALIZED inm
    ON pc.LISTING_ID = inm.LISTING_ID
{where_clause}
"""


@st.cache_data(ttl=300, show_spinner=False)
def _cached_run_fr_lookup(ids_tuple, id_type):
    """Internal cached version of FR query."""
    ids = list(ids_tuple)
    conn = get_connection()
    return pd.read_sql(build_fr_query(ids, id_type), conn)


def run_fr_lookup(ids, id_type="LISTING_ID"):
    return _cached_run_fr_lookup(
        tuple(sorted(set(s.strip() for s in ids if s.strip()))),
        id_type,
    )


# ── Validation engine (ported from HTML tool) ──
def _lo(v):
    return str(v or "").strip().lower()


def _is_y(v):
    s = _lo(v)
    return s in ("true", "yes", "y")


def _is_blank(v):
    return _lo(v) == ""


def _g(row, key):
    """Read a row value as a trimmed string, treating None/NaN as blank."""
    v = row.get(key, "")
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
    s = str(v).strip()
    return "" if s.lower() == "nan" else s


def _sku(row):
    """SKU is aliased as SKU in the query; fall back to MARKETPLACE_PRIMARY_ID."""
    return _g(row, "SKU") or _g(row, "MARKETPLACE_PRIMARY_ID")


def _plan_filled(v):
    return bool(v) and v != "0" and v.lower() not in ("false", "no")


def _R(status, text, value):
    return {"status": status, "text": text, "value": value}


# ── Info-only attributes (display the value, never pass/fail) ──
_FR_INFO_ATTRS = {
    "MARKETPLACE_COUNTRY_CODE", "PRODUCT_DIMENSIONS", "CASEPACK_DIMENSIONS",
    "IS_HAZMAT", "IS_GLASS", "ITEM_REQUIRES_TRANSPARENCY_LABEL", "IS_BUNDLE",
    "IS_SOLD_INDIVIDUALLY", "LISTING_OPT_OUT_OF_AUTOMATED_WOI_CREATION",
    "CATALOG_DNO_STATUS", "IS_TEMPORARY", "MARKETPLACE_SECONDARY_ID_TYPE",
}


def evaluate_attribute(attr_id, row):
    """Evaluate one attribute for one row — ported 1:1 from the FR-Checker tool's ev().
    Returns {status: 'g'/'r'/'i'/'n', text: str, value: str}
    g = green/pass, r = red/fail, i = info, n = not applicable
    """
    v = _g(row, attr_id)
    sid = _g(row, "MARKETPLACE_SECONDARY_ID")

    if attr_id in _FR_INFO_ATTRS:
        return _R("i", v or "—", v)

    if attr_id == "LISTING_IS_COMMINGLED":
        b0 = sid.startswith("B0")
        x0 = sid.startswith("X0")
        cm = _is_y(v)
        glC = _is_y(_g(row, "IS_GLASS"))
        enrC = _is_y(_g(row, "ENROLLED_IN_TRANSPARENCY_PROGRAM"))
        buC = _is_y(_g(row, "IS_BUNDLE"))
        planC = _plan_filled(_g(row, "LISTING_PREP_PLAN_ID"))
        if b0 and cm:
            return _R("g", "Commingled + B0 ✓", v)
        if b0 and not cm and enrC and planC:
            return _R("g", "Not Commingled + B0 + Enrolled + Listing Plan ✓ (transparency bypass)", v)
        if b0 and not cm and glC:
            return _R("g", "Not Commingled + B0 + Glass=Y ✓ (glass override)", v)
        if b0 and not cm and buC:
            return _R("g", "Not Commingled + B0 + Bundle=Y ✓ (bundle override)", v)
        if b0 and not cm:
            return _R("r", "Not Commingled but B0", v)
        if x0 and cm:
            return _R("r", "Commingled but X0", v)
        if x0 and not cm:
            return _R("g", "Not Commingled + X0 ✓", v)
        return _R("n", "Prefix: " + (sid[:2] if sid else "—"), v)

    if attr_id == "IS_ACTIVE":
        return _R("g", "Active ✓", v) if _is_y(v) else _R("r", "Inactive / blank", v)

    if attr_id == "LISTING_IS_SHIPABLE":
        return _R("g", "Shipable ✓", v) if _is_y(v) else _R("r", "Not shipable", v)

    if attr_id == "LISTING_PREP_PLAN_ID":
        is_fbm = _sku(row).upper().endswith("-FBM")
        lp = _g(row, "LISTING_PREP_PLAN_ID")
        lp_ok = _plan_filled(lp)
        if is_fbm:
            if lp == "316":
                return _R("g", "FBM SKU + Prep 316 ✓", v)
            if lp_ok:
                return _R("r", f"FBM SKU must use Prep 316 (found: {lp})", v)
            return _R("r", "FBM SKU must use Prep 316 (blank)", v)
        cm = _is_y(_g(row, "LISTING_IS_COMMINGLED"))
        b0 = _g(row, "MARKETPLACE_SECONDARY_ID").startswith("B0")
        gl = _is_y(_g(row, "IS_GLASS"))
        if cm and b0 and not gl:
            return _R("g", "Commingled + B0 + No Glass — prep plan not required ✓", v)
        if lp_ok:
            return _R("g", f"Listing Plan: {lp} ✓", v)
        return _R("r", "Listing Prep Plan missing" + (" (Glass=Y requires)" if gl else ""), v)

    if attr_id == "ITEM_PREP_PLAN_ID":
        is_fbm = _sku(row).upper().endswith("-FBM")
        lp = _g(row, "LISTING_PREP_PLAN_ID")
        ip = _g(row, "ITEM_PREP_PLAN_ID")
        lp_ok = _plan_filled(lp)
        if is_fbm:
            if lp == "316":
                return _R("g", "FBM SKU + Listing Prep 316 ✓", v)
            return _R("r", "FBM SKU must use Prep 316", v)
        cm = _is_y(_g(row, "LISTING_IS_COMMINGLED"))
        b0 = _g(row, "MARKETPLACE_SECONDARY_ID").startswith("B0")
        gl = _is_y(_g(row, "IS_GLASS"))
        if cm and b0 and not gl:
            return _R("g", "Commingled + B0 + No Glass — prep plan not required ✓", v)
        if lp_ok:
            return _R("g", f"Listing Plan filled — Item Plan: {ip or '—'} ✓", v)
        return _R("r", "Listing Prep Plan missing" + (" (Glass=Y requires)" if gl else ""), v)

    if attr_id == "CAN_EXPIRE":
        ce = _g(row, "CAN_EXPIRE").upper()
        lie = _g(row, "LISTING_IS_EXPIRABLE").upper()
        if ce == "" and lie == "":
            return _R("g", "Both blank ✓", v)
        if (ce == "Y" and lie == "N") or (ce == "N" and lie == "Y"):
            return _R("r", f"Mismatch — CE: {ce} / LE: {lie}", v)
        return _R("g", f"Can Expire: {ce or 'blank'} / LE: {lie or 'blank'} ✓", v)

    if attr_id == "LISTING_IS_EXPIRABLE":
        ce = _g(row, "CAN_EXPIRE").upper()
        lie = _g(row, "LISTING_IS_EXPIRABLE").upper()
        if ce == "" and lie == "":
            return _R("g", "Both blank ✓", v)
        if (ce == "Y" and lie == "N") or (ce == "N" and lie == "Y"):
            return _R("r", f"Mismatch — CE: {ce} / LE: {lie}", v)
        return _R("g", f"Expirable: {lie or 'blank'} / CE: {ce or 'blank'} ✓", v)

    if attr_id == "ENROLLED_IN_TRANSPARENCY_PROGRAM":
        if _is_y(v):
            return _R("g", "Enrolled in Transparency ✓ (bypass active)", v)
        return _R("g", v or "Not enrolled (no effect)", v)

    return _R("n", v or "—", v)


# ── Suggested prep plan (advisory only — not a pass/fail attribute) ──
_APPAREL_KEYWORDS = [
    "shirt", "t-shirt", "tshirt", "tee", "polo", "pants", "jeans", "leggings",
    "shorts", "trousers", "dress", "skirt", "gown", "jacket", "coat", "hoodie",
    "sweater", "cardigan", "sweatshirt", "socks", "underwear", "briefs", "boxers",
    "bra", "apparel", "clothing", "garment", "attire",
]
_LIQUID_KEYWORDS = [
    "oil", "liquid", "drops", "syrup", "juice", "serum", "lotion", "tonic",
    "elixir", "solution", "spray", "mist",
]
_MPN_PREP_MAP = {
    "SA512": 109, "SA518": 110, "SA519": 111, "SA520": 112, "SA525": 113, "SA540": 114,
    "SB332": 115, "SB335": 116, "SD415": 117, "SF714": 118, "SF815": 119,
    "ADK060": 317, "DIG060": 318, "CPB060": 319, "HGD060": 320, "ANT30L": 321, "PBM050": 322,
    "MGC060": 323, "BER060": 324, "OV1000": 325, "VB1260": 326, "LYS120": 327, "NEUMAG": 328,
    "MGC240": 329, "HYS120": 330, "BVC120": 331, "INF060": 332, "EMD10Z": 333, "BSP060": 334,
    "BSP120": 335, "DIG180": 336, "QUN090": 337, "IMP120": 338, "STC090": 339, "MGC120": 340,
    "FEC120": 341, "DIG090": 342, "CBF144": 343, "FLM060": 344, "HIS120": 345, "OVU120": 346,
    "ORG120": 347, "SEN120": 348, "OPO120": 349, "WBC390": 350, "SPM060": 351, "PBM100": 353,
    "NTS120": 418, "PPPVAN": 419, "PPPCHC": 420, "MHN120": 421, "PBS060": 422, "NAC120": 423,
}
_EU_PREFIXES = ("UK-", "EU-", "DE-", "FR-", "IT-", "ES-", "NL-", "PL-", "BE-", "SE-")


def _detect_region(sku):
    s = (sku or "").strip().upper()
    if not s:
        return "UNKNOWN"
    if s.startswith("SD-") or s.startswith("MX-") or s.startswith("CA-"):
        return "US"
    for p in _EU_PREFIXES:
        if s.startswith(p):
            return "EU"
    return "UNKNOWN"


def _kw_match(name, keywords):
    if not name:
        return False
    n = name.lower()
    for k in keywords:
        if re.search(r"\b" + re.escape(k) + r"\b", n):
            return True
    return False


def _is_pandora(sku):
    return "DORA" in (sku or "").upper()


def _pandora_size(name):
    if not name:
        return "unclear"
    n = name.lower()
    if re.search(r"\b(ring|earring|earrings|stud|studs)\b", n):
        return "small"
    if re.search(r"\b(necklace|bracelet|bangle|chain)\b", n):
        return "large"
    return "unclear"


def suggest_prep(row):
    """Return {'id','desc','note'} advisory prep plan, or None if region unknown."""
    sku = _sku(row)
    region = _detect_region(sku)
    if region == "UNKNOWN":
        return None
    glass = _is_y(_g(row, "IS_GLASS"))
    bundle = _is_y(_g(row, "IS_BUNDLE"))
    enrolled = _is_y(_g(row, "ENROLLED_IN_TRANSPARENCY_PROGRAM"))
    name = _g(row, "PRODUCT_NAME")
    apparel = _kw_match(name, _APPAREL_KEYWORDS)

    if region == "US":
        if enrolled:
            if glass and bundle:
                return {"id": "311", "desc": "Bundle in a bubble bag, FNSKU/transparency label", "note": ""}
            if glass:
                return {"id": "310", "desc": "Bubble Bag, FNSKU/Transparency Label", "note": ""}
            if bundle:
                return {"id": "307", "desc": "Transparency/FNSKU Label & Polybag", "note": ""}
            if apparel:
                return {"id": "308", "desc": "Poly Bag & Transparency/FNSKU Label", "note": ""}
            return {"id": "309", "desc": "Transparency/FNSKU Label", "note": ""}
        if glass and bundle:
            return {"id": "315", "desc": "Bundle in a bubble bag and FNSKU", "note": ""}
        if glass:
            return {"id": "304", "desc": "Bubble Bag and FNSKU Label", "note": ""}
        if bundle:
            return {"id": "—", "desc": "Bundle in a poly and FNSKU", "note": "No standard ID assigned"}
        if apparel:
            return {"id": "2", "desc": "Poly Bag and FNSKU", "note": ""}
        return {"id": "1", "desc": "FNSKU over barcode (default)", "note": ""}

    if region == "EU":
        mpn = _g(row, "MPN").upper()
        if mpn and mpn in _MPN_PREP_MAP:
            pid = _MPN_PREP_MAP[mpn]
            if 109 <= pid <= 119:
                return {"id": str(pid), "desc": f"Apply label {mpn} and FNSKU label", "note": ""}
            return {"id": str(pid), "desc": f"Apply label {mpn} and FNSKU & expiry date label", "note": ""}
        if _is_pandora(sku):
            sz = _pandora_size(name)
            if sz == "small":
                return {"id": "84", "desc": "Small Pandora box, Pandora poly bag, FNSKU transparency label", "note": ""}
            if sz == "large":
                return {"id": "85", "desc": "Large Pandora box, Pandora poly bag, FNSKU transparency label", "note": ""}
            return {"id": "—", "desc": "Pandora item — size unclear from product name", "note": "Review manually"}
        if glass:
            return {"id": "83", "desc": "Bubble bag and FNSKU label", "note": ""}
        if _kw_match(name, _LIQUID_KEYWORDS) or apparel:
            return {"id": "82", "desc": "Poly bag and FNSKU", "note": ""}
        return {"id": "81", "desc": "FNSKU over barcode (default)", "note": ""}

    return None


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
            "suggested_prep": suggest_prep(row),
            "raw_row": row.to_dict(),
        })
    return results


# ══════════════════════════════════════════════
# Inventory Lookup — query & helpers
# ══════════════════════════════════════════════
INVENTORY_QUERY_TEMPLATE = """
WITH wh_base AS (
  -- Current-warehouse rows for the requested masters, tagged with a CW (central-warehouse)
  -- region inferred from WAREHOUSE_NAME — the table has no region/country column.
  -- Mapping confirmed from the live warehouse list (16 warehouses, 2026-06-11). If a
  -- warehouse's stock isn't mapping, check its exact spelling in the CASE below.
  SELECT
    MASTER_ID, WAREHOUSE_NAME, STORAGE_ID, STATUS, AREA, QUANTITY, DATE,
    PART_NUMBER_FINAL, BRAND,
    CASE
      WHEN WAREHOUSE_NAME IN ('Hebron-Progress','Las Vegas','Lindon','Bethlehem','Kraus','CFS','Steelcase ND','Select Brands') THEN 'US'
      WHEN WAREHOUSE_NAME IN ('CA-Milton','Toronto') THEN 'CA'
      WHEN WAREHOUSE_NAME = 'Northampton' THEN 'GB'
      WHEN WAREHOUSE_NAME IN ('Wroclaw','EU GMBH Refurb','Europe GMBH Refurb') THEN 'EU'
      WHEN WAREHOUSE_NAME = 'Dubai' THEN 'AE'
      WHEN WAREHOUSE_NAME = 'Melbourne' THEN 'AU'
      WHEN WAREHOUSE_NAME IN ('Hong Kong Hub','HK Hub') THEN 'CN'
    END AS CW_REGION
  FROM "ANALYTICS_DB"."REPORTING"."REPORT__WAREHOUSE_INVENTORY_BY_PRODUCT_CURRENT"
  WHERE MASTER_ID IN ({master_ids})
),
warehouse_region AS (
  -- CW Inventory = sellable, pickable units per (region, master).
  -- "Pickable" areas = any storage module (…-MOD) plus named storage areas; transit /
  -- processing areas (Crossdock, Shipping, Receiving, Sorting) are excluded.
  SELECT
    CW_REGION, MASTER_ID,
    MAX(DATE)     AS SNAPSHOT_DATE,
    SUM(QUANTITY) AS STOW_PICKABLE_QTY
  FROM wh_base
  WHERE CW_REGION IS NOT NULL
    AND STATUS = 'Sellable'
    AND (UPPER(AREA) LIKE '%-MOD' OR UPPER(AREA) IN ('CAGE','BIO','OVERAGE','SHELVING','COLD STORAGE','COLDSTORAGE'))
  GROUP BY CW_REGION, MASTER_ID
),
warehouse_master AS (
  -- Master-level attributes (region-independent) so BRAND / part number always show
  SELECT
    MASTER_ID,
    MAX(PART_NUMBER_FINAL) AS PART_NUMBER_FINAL,
    MAX(BRAND)             AS BRAND,
    MAX(DATE)              AS SNAPSHOT_DATE
  FROM wh_base
  WHERE CW_REGION IS NOT NULL
  GROUP BY MASTER_ID
),
warehouse_attrs AS (
  -- Storage IDs + statuses + warehouse name(s) per (region, master).
  -- No status/area filter, so STATUS reflects the full picture (Sellable, Reserved, …).
  SELECT
    CW_REGION, MASTER_ID,
    LISTAGG(DISTINCT WAREHOUSE_NAME, ', ') WITHIN GROUP (ORDER BY WAREHOUSE_NAME) AS WAREHOUSE_NAMES,
    LISTAGG(DISTINCT STORAGE_ID, ', ')     WITHIN GROUP (ORDER BY STORAGE_ID)     AS STORAGE_IDS,
    LISTAGG(DISTINCT STATUS, ', ')         WITHIN GROUP (ORDER BY STATUS)         AS STATUSES
  FROM wh_base
  WHERE CW_REGION IS NOT NULL
  GROUP BY CW_REGION, MASTER_ID
),
inventory_hub AS (
  SELECT
    PART_NUMBER, MASTER_ID, ASIN, ASIN_LIST,
    TITLE AS HUB_TITLE, VENDOR, REGION, INVENTORY_POOL, FULFILLMENT_NETWORK,
    INVENTORY_TYPE, BUNDLE_TYPE, DNO_STATUS, WHOLESALE_PRICE_USD,
    FULFILLABLE, WAREHOUSE_TRANSFER, WAREHOUSE_RESERVED, UNFULFILLABLE,
    OUTBOUND_RESERVED, INBOUND,
    PATTERN_WAREHOUSE_RESERVED_FOR_MARKETPLACE,
    PATTERN_OUTBOUND_RESERVED_FOR_MARKETPLACE,
    PATTERN_WAREHOUSE_TRANSFER_FOR_MARKETPLACE,
    ON_ORDER, PATTERN_ON_ORDER_RESERVED_FOR_MARKETPLACE,
    FULFILLMENT_CHANNEL_UNITS,
    FULFILLMENT_CHANNEL_UNITS_3PL,
    FULFILLMENT_CHANNEL_UNITS_PATTERN_OWNED,
    PIPELINE_UNITS,
    PIPELINE_UNITS_3PL,
    PIPELINE_UNITS_PATTERN_OWNED,
    FULFILLABLE_VALUE_USD, UNFULFILLABLE_VALUE_USD,
    OUTBOUND_RESERVED_VALUE_USD, INBOUND_VALUE_USD,
    WAREHOUSE_RESERVED_VALUE_USD, WAREHOUSE_TRANSFER_VALUE_USD,
    PATTERN_WAREHOUSE_RESERVED_FOR_MARKETPLACE_VALUE_USD,
    PATTERN_OUTBOUND_RESERVED_FOR_MARKETPLACE_VALUE_USD,
    PATTERN_WAREHOUSE_TRANSFER_FOR_MARKETPLACE_VALUE_USD,
    ON_ORDER_VALUE_USD, PATTERN_ON_ORDER_RESERVED_FOR_MARKETPLACE_VALUE_USD,
    FULFILLMENT_CHANNEL_VALUE_USD_PATTERN_OWNED,
    PIPELINE_VALUE_USD_PATTERN_OWNED,
    FULFILLABLE_WOS,
    WAREHOUSE_TRANSFER_WOS,
    FULFILLMENT_CHANNEL_UNITS_PATTERN_OWNED_WOS,
    FULFILLMENT_CHANNEL_UNITS_PATTERN_OWNED_WOS_DEFAULT_FCST,
    PIPELINE_UNITS_PATTERN_OWNED_WOS,
    PIPELINE_UNITS_PATTERN_OWNED_WOS_DEFAULT_FCST
  FROM PATTERN_DB.OPERATIONS.INVENTORY_HUB_NORMALIZED_INVENTORY_ITEMS
  WHERE MASTER_ID IN ({master_ids})
    {region_filter}
    {network_filter}
    {inv_type_filter}
),
hub_reserved_pfs AS (
  -- Pattern PFS warehouse-reserved per (region, master)
  SELECT
    MASTER_ID, REGION,
    SUM(CASE WHEN FULFILLMENT_NETWORK = 'Pattern PFS'
             THEN PATTERN_WAREHOUSE_RESERVED_FOR_MARKETPLACE END) AS PFS_RESERVED
  FROM inventory_hub
  GROUP BY MASTER_ID, REGION
)
SELECT
  COALESCE(ih.MASTER_ID, wm.MASTER_ID) AS MASTER_ID,
  ih.PART_NUMBER, wm.PART_NUMBER_FINAL,
  -- ASIN backfill: the hub's per-row ASIN is blank for some pools (esp. multi-country
  -- EU / Pan-EU). Fill blanks with any ASIN known for the same Master ID, then with the
  -- first entry of ASIN_LIST. ASIN_RAW keeps the exact untouched per-pool value.
  COALESCE(
    ih.ASIN,
    MAX(ih.ASIN) OVER (PARTITION BY ih.MASTER_ID),
    SPLIT_PART(ih.ASIN_LIST, ',', 1)
  ) AS ASIN,
  ih.ASIN AS ASIN_RAW,
  ih.ASIN_LIST, ih.HUB_TITLE AS TITLE,
  wm.BRAND, ih.VENDOR, wm.SNAPSHOT_DATE,
  ih.REGION, ih.INVENTORY_POOL, ih.FULFILLMENT_NETWORK,
  ih.INVENTORY_TYPE, ih.BUNDLE_TYPE, ih.DNO_STATUS,
  ih.WHOLESALE_PRICE_USD,
  -- Current-warehouse attributes (region-matched: hub REGION = warehouse CW region)
  wa.WAREHOUSE_NAMES AS WAREHOUSE_NAME,
  wa.STORAGE_IDS,
  wa.STATUSES AS WAREHOUSE_STATUS,
  -- Core block (region-aware via the region join)
  ih.FULFILLABLE,
  wr.STOW_PICKABLE_QTY AS STOW_PICKABLE_QTY,
  hr.PFS_RESERVED AS PFS_RESERVED,
  CASE
    WHEN wr.STOW_PICKABLE_QTY IS NULL AND hr.PFS_RESERVED IS NULL THEN NULL
    ELSE COALESCE(wr.STOW_PICKABLE_QTY, 0) - COALESCE(hr.PFS_RESERVED, 0)
  END AS ACTUAL_AVAILABLE_QTY,
  ih.UNFULFILLABLE,
  -- Secondary metrics
  ih.INBOUND, ih.ON_ORDER, ih.WAREHOUSE_RESERVED, ih.OUTBOUND_RESERVED,
  ih.WAREHOUSE_TRANSFER,
  ih.PATTERN_WAREHOUSE_RESERVED_FOR_MARKETPLACE,
  ih.PATTERN_OUTBOUND_RESERVED_FOR_MARKETPLACE,
  ih.PATTERN_WAREHOUSE_TRANSFER_FOR_MARKETPLACE,
  ih.PATTERN_ON_ORDER_RESERVED_FOR_MARKETPLACE,
  ih.FULFILLMENT_CHANNEL_UNITS,
  ih.FULFILLMENT_CHANNEL_UNITS_3PL,
  ih.FULFILLMENT_CHANNEL_UNITS_PATTERN_OWNED,
  ih.PIPELINE_UNITS,
  ih.PIPELINE_UNITS_3PL,
  ih.PIPELINE_UNITS_PATTERN_OWNED,
  -- WOS columns (hidden by default)
  ih.FULFILLABLE_WOS,
  ih.WAREHOUSE_TRANSFER_WOS,
  ih.FULFILLMENT_CHANNEL_UNITS_PATTERN_OWNED_WOS,
  ih.FULFILLMENT_CHANNEL_UNITS_PATTERN_OWNED_WOS_DEFAULT_FCST,
  ih.PIPELINE_UNITS_PATTERN_OWNED_WOS,
  ih.PIPELINE_UNITS_PATTERN_OWNED_WOS_DEFAULT_FCST,
  -- USD values (hidden by default)
  ih.FULFILLABLE_VALUE_USD, ih.UNFULFILLABLE_VALUE_USD,
  ih.OUTBOUND_RESERVED_VALUE_USD, ih.INBOUND_VALUE_USD,
  ih.WAREHOUSE_RESERVED_VALUE_USD, ih.WAREHOUSE_TRANSFER_VALUE_USD,
  ih.PATTERN_WAREHOUSE_RESERVED_FOR_MARKETPLACE_VALUE_USD,
  ih.PATTERN_OUTBOUND_RESERVED_FOR_MARKETPLACE_VALUE_USD,
  ih.PATTERN_WAREHOUSE_TRANSFER_FOR_MARKETPLACE_VALUE_USD,
  ih.ON_ORDER_VALUE_USD, ih.PATTERN_ON_ORDER_RESERVED_FOR_MARKETPLACE_VALUE_USD,
  ih.FULFILLMENT_CHANNEL_VALUE_USD_PATTERN_OWNED,
  ih.PIPELINE_VALUE_USD_PATTERN_OWNED
FROM inventory_hub ih
FULL OUTER JOIN warehouse_master wm ON ih.MASTER_ID = wm.MASTER_ID
LEFT JOIN warehouse_region wr
  ON wr.MASTER_ID = COALESCE(ih.MASTER_ID, wm.MASTER_ID) AND wr.CW_REGION = ih.REGION
LEFT JOIN hub_reserved_pfs hr
  ON hr.MASTER_ID = COALESCE(ih.MASTER_ID, wm.MASTER_ID) AND hr.REGION = ih.REGION
LEFT JOIN warehouse_attrs wa
  ON wa.MASTER_ID = COALESCE(ih.MASTER_ID, wm.MASTER_ID) AND wa.CW_REGION = ih.REGION
ORDER BY MASTER_ID, ih.REGION, ih.FULFILLMENT_NETWORK
"""


def build_inventory_query(master_ids, regions=None, networks=None, inv_types=None):
    """
    Build inventory query for given master IDs with optional upstream filters.
    Args:
        master_ids: list of master IDs
        regions: list of regions to include (e.g. ['GB'], ['EU'], ['GB','EU']). None = both.
        networks: list of networks (e.g. ['Pattern PFS'], ['Amazon FBA']). None = both.
        inv_types: list of inventory types (e.g. ['Pattern Owned']). None = no filter.
    """
    def safe(s): return s.strip().replace("'", "''")
    quoted = ", ".join(f"'{safe(m)}'" for m in master_ids if m.strip())
    # Build region filter for inventory_hub — empty = ALL regions (GB, EU, US, …)
    if regions and len(regions) > 0:
        region_list = ", ".join(f"'{safe(r)}'" for r in regions)
        region_filter = f"AND REGION IN ({region_list})"
    else:
        region_filter = ""
    # Build network filter — empty = ALL networks
    if networks and len(networks) > 0:
        net_list = ", ".join(f"'{safe(n)}'" for n in networks)
        network_filter = f"AND FULFILLMENT_NETWORK IN ({net_list})"
    else:
        network_filter = ""
    # Build inventory_type filter
    if inv_types and len(inv_types) > 0:
        type_list = ", ".join(f"'{safe(t)}'" for t in inv_types)
        inv_type_filter = f"AND INVENTORY_TYPE IN ({type_list})"
    else:
        inv_type_filter = ""
    return INVENTORY_QUERY_TEMPLATE.format(
        master_ids=quoted,
        region_filter=region_filter,
        network_filter=network_filter,
        inv_type_filter=inv_type_filter,
    ).strip()


@st.cache_data(ttl=60 * 60 * 26, show_spinner=False)  # cached once per IST business day
def _cached_run_inventory_lookup(master_ids_tuple, regions_tuple, networks_tuple, inv_types_tuple, day=None):
    """Internal cached inventory lookup. `day` (IST business day) is part of the
    cache key so an identical lookup stays cached until the next morning."""
    master_ids = list(master_ids_tuple)
    regions = list(regions_tuple) if regions_tuple else None
    networks = list(networks_tuple) if networks_tuple else None
    inv_types = list(inv_types_tuple) if inv_types_tuple else None
    conn = get_connection()
    return pd.read_sql(build_inventory_query(master_ids, regions, networks, inv_types), conn)


def run_inventory_lookup(master_ids, regions=None, networks=None, inv_types=None):
    """Run the inventory query for a list of master IDs with optional upstream filters.
    Results are cached per IST business day (06:00 rollover): the first run of a
    given lookup is slow, then that exact lookup is instant for the rest of the day."""
    return _cached_run_inventory_lookup(
        tuple(sorted(set(master_ids))),
        tuple(sorted(regions)) if regions else None,
        tuple(sorted(networks)) if networks else None,
        tuple(sorted(inv_types)) if inv_types else None,
        ist_data_day(),
    )


@st.cache_data(ttl=300, show_spinner=False)
def _cached_resolve_listing_to_master(listing_ids_tuple):
    """Internal cached version of listing→master resolution."""
    listing_ids = list(listing_ids_tuple)
    if not listing_ids:
        return {}
    def safe(s): return s.strip().replace("'", "''")
    upper_list = ", ".join(f"UPPER('{safe(lid)}')" for lid in listing_ids if lid.strip())
    query = f"""
SELECT DISTINCT
    UPPER(a.LISTING_ID) AS LISTING_ID,
    b.MASTER_ID AS MASTER_ID
FROM ANALYTICS_DB.STG_CATALOG.STG_CATALOG__LISTINGS a
LEFT JOIN ANALYTICS_DB.STG_CATALOG.STG_CATALOG__PRODUCTS b ON b.ID = a.PRODUCT_ID
WHERE (UPPER(a.LISTING_ID) IN ({upper_list})
   OR UPPER(a.Listing_MP_Primary_ID) IN ({upper_list})
   OR UPPER(a.LISTING_MP_PAGE_ID) IN ({upper_list})
   OR UPPER(a.LISTING_MP_SECONDARY_ID) IN ({upper_list}))
  AND b.MASTER_ID IS NOT NULL
"""
    conn = get_connection()
    df = pd.read_sql(query, conn)
    return dict(zip(df["LISTING_ID"], df["MASTER_ID"]))


def resolve_listing_to_master(listing_ids):
    """Resolve Listing IDs to Master IDs (cached)."""
    return _cached_resolve_listing_to_master(tuple(sorted(set(listing_ids))))


@st.cache_data(ttl=300, show_spinner=False)
def _cached_resolve_to_master(values_tuple, id_type):
    """Resolve ASINs or Part Numbers to Master IDs.
    id_type: 'ASIN' (via catalog) or 'PART_NUMBER' (via Inventory Hub).
    Returns {'masters': [...], 'matched_keys': [...]}.
    """
    values = list(values_tuple)
    if not values:
        return {"masters": [], "matched_keys": []}
    def safe(s): return s.strip().replace("'", "''")
    upper_list = ", ".join(f"UPPER('{safe(v)}')" for v in values if v.strip())
    if id_type == "ASIN":
        query = f"""
SELECT DISTINCT UPPER(a.LISTING_MP_PAGE_ID) AS KEY, b.MASTER_ID AS MASTER_ID
FROM ANALYTICS_DB.STG_CATALOG.STG_CATALOG__LISTINGS a
LEFT JOIN ANALYTICS_DB.STG_CATALOG.STG_CATALOG__PRODUCTS b ON b.ID = a.PRODUCT_ID
WHERE UPPER(a.LISTING_MP_PAGE_ID) IN ({upper_list})
  AND b.MASTER_ID IS NOT NULL
"""
    else:  # PART_NUMBER — resolved against the Inventory Hub's PART_NUMBER column
        query = f"""
SELECT DISTINCT UPPER(PART_NUMBER) AS KEY, MASTER_ID
FROM PATTERN_DB.OPERATIONS.INVENTORY_HUB_NORMALIZED_INVENTORY_ITEMS
WHERE UPPER(PART_NUMBER) IN ({upper_list})
  AND MASTER_ID IS NOT NULL
"""
    conn = get_connection()
    df = pd.read_sql(query, conn)
    return {
        "masters": sorted(df["MASTER_ID"].dropna().astype(str).unique().tolist()),
        "matched_keys": sorted(df["KEY"].dropna().astype(str).unique().tolist()),
    }


def resolve_to_master(values, id_type):
    """Resolve a list of ASINs or Part Numbers to Master IDs (cached)."""
    return _cached_resolve_to_master(
        tuple(sorted(set(v.strip() for v in values if v.strip()))), id_type
    )


@st.cache_data(ttl=60 * 60 * 26, show_spinner=False)  # cached once per IST business day
def _cached_get_brand_master_ids(brand_name, limit, regions_tuple, networks_tuple, inv_types_tuple, day=None):
    """Internal cached brand→master IDs lookup (day-keyed to the IST business day)."""
    def safe(s): return s.strip().replace("'", "''")
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    regions = list(regions_tuple) if regions_tuple else None
    networks = list(networks_tuple) if networks_tuple else None
    inv_types = list(inv_types_tuple) if inv_types_tuple else None
    if regions and len(regions) > 0:
        rlist = ", ".join(f"'{safe(r)}'" for r in regions)
        region_clause = f"AND REGION IN ({rlist})"
    else:
        region_clause = ""
    network_clause = ""
    if networks and len(networks) > 0:
        nlist = ", ".join(f"'{safe(n)}'" for n in networks)
        network_clause = f"AND FULFILLMENT_NETWORK IN ({nlist})"
    inv_type_clause = ""
    if inv_types and len(inv_types) > 0:
        tlist = ", ".join(f"'{safe(t)}'" for t in inv_types)
        inv_type_clause = f"AND INVENTORY_TYPE IN ({tlist})"
    query = f"""
SELECT DISTINCT MASTER_ID
FROM PATTERN_DB.OPERATIONS.INVENTORY_HUB_NORMALIZED_INVENTORY_ITEMS
WHERE UPPER(VENDOR) = UPPER('{safe(brand_name)}')
  AND MASTER_ID IS NOT NULL
  {region_clause}
  {network_clause}
  {inv_type_clause}
{limit_clause}
"""
    conn = get_connection()
    df = pd.read_sql(query, conn)
    return df["MASTER_ID"].tolist()


def get_brand_master_ids(brand_name, limit=None, regions=None, networks=None, inv_types=None):
    """Get master IDs for a brand (cached, case-insensitive against VENDOR in inventory hub)."""
    return _cached_get_brand_master_ids(
        brand_name.strip(),
        limit if limit else 0,
        tuple(sorted(regions)) if regions else None,
        tuple(sorted(networks)) if networks else None,
        tuple(sorted(inv_types)) if inv_types else None,
        ist_data_day(),
    )


# Inventory column metadata
INVENTORY_COLUMNS = [
    # ── Identifiers ──
    {"key": "MASTER_ID",            "label": "Master ID",              "default": True,  "type": "str", "group": "Identifiers", "desc": "Pattern's internal product identifier"},
    {"key": "PART_NUMBER",          "label": "Part Number",            "default": True,  "type": "str", "group": "Identifiers", "desc": "Vendor-provided SKU"},
    {"key": "PART_NUMBER_FINAL",    "label": "Part Number (WH)",       "default": False, "type": "str", "group": "Identifiers", "desc": "Final part number used in warehouse systems"},
    {"key": "ASIN",                 "label": "ASIN",                   "default": True,  "type": "str", "group": "Identifiers", "desc": "Amazon Standard Identification Number. Blanks are backfilled with another ASIN known for the same Master ID (or the first in ASIN List), so EU / Pan-EU rows aren't empty. See 'ASIN (raw)' for the exact per-pool value."},
    {"key": "ASIN_RAW",             "label": "ASIN (raw)",             "default": False, "type": "str", "group": "Identifiers", "desc": "The exact ASIN the Inventory Hub holds for this specific pool row — blank where the hub has no single ASIN for that pool (common for multi-country EU pools)."},
    {"key": "ASIN_LIST",            "label": "ASIN List",              "default": False, "type": "str", "group": "Identifiers", "desc": "All ASINs linked to this Master ID — useful for multi-country EU / Pan-EU pools where one row spans several country ASINs."},
    {"key": "TITLE",                "label": "Title",                  "default": True,  "type": "str", "group": "Identifiers", "desc": "Product title"},
    {"key": "BRAND",                "label": "Brand",                  "default": True,  "type": "str", "group": "Identifiers", "desc": "Brand name (from warehouse)"},
    {"key": "VENDOR",               "label": "Vendor",                 "default": True,  "type": "str", "group": "Identifiers", "desc": "Vendor name (from Inventory Hub)"},
    {"key": "SNAPSHOT_DATE",        "label": "As of",                  "default": False, "type": "str", "group": "Identifiers", "desc": "Date this inventory snapshot was taken"},
    # ── Categorization ──
    {"key": "REGION",               "label": "Region",                 "default": True,  "type": "str", "group": "Categorization", "desc": "GB (UK / Northampton) or EU (Wroclaw)"},
    {"key": "WAREHOUSE_NAME",       "label": "Warehouse",              "default": True,  "type": "str", "group": "Categorization", "desc": "Central warehouse holding this region's stock — Northampton (GB) or Wroclaw (EU). From the current-warehouse table."},
    {"key": "INVENTORY_POOL",       "label": "Inventory Pool",         "default": True,  "type": "str", "group": "Categorization", "desc": "Specific marketplace/pool (e.g., Amazon DE, Pattern GB)"},
    {"key": "FULFILLMENT_NETWORK",  "label": "Network",                "default": True,  "type": "str", "group": "Categorization", "desc": "Pattern PFS or Amazon FBA"},
    {"key": "INVENTORY_TYPE",       "label": "Inventory Type",         "default": False, "type": "str", "group": "Categorization", "desc": "e.g., Pattern Owned"},
    {"key": "BUNDLE_TYPE",          "label": "Bundle Type",            "default": False, "type": "str", "group": "Categorization", "desc": "e.g., Single, Bundle"},
    {"key": "DNO_STATUS",           "label": "DNO",                    "default": True,  "type": "str", "group": "Categorization", "desc": "Do-Not-Order flag"},
    {"key": "WHOLESALE_PRICE_USD",  "label": "Wholesale (USD)",        "default": False, "type": "num", "group": "Categorization", "desc": "Wholesale cost in USD"},
    # ── Core Inventory (region-aware) ──
    {"key": "FULFILLABLE",          "label": "Fulfillable",            "default": True,  "type": "num", "group": "Core", "desc": "Units ready to ship at Amazon / Pattern / Marketplace"},
    {"key": "STOW_PICKABLE_QTY",    "label": "CW Inventory",           "default": True,  "type": "num", "group": "Core", "desc": "Units in our central warehouse, sellable, in pickable areas (region-aware: NH for GB, WR for EU)"},
    {"key": "PFS_RESERVED",         "label": "PFS Reserved",           "default": True,  "type": "num", "group": "Core", "desc": "Units in our warehouse already reserved for PFS orders"},
    {"key": "ACTUAL_AVAILABLE_QTY", "label": "Pickable",               "default": True,  "type": "num", "group": "Core", "desc": "CW Inventory − PFS Reserved (true free stock in our central warehouse)"},
    {"key": "UNFULFILLABLE",        "label": "Unfulfillable",          "default": True,  "type": "num", "group": "Core", "desc": "Units that can't be sold (de-listed, returns, damaged…)"},
    # ── Movement & Pipeline ──
    {"key": "INBOUND",              "label": "Inbound",                "default": True,  "type": "num", "group": "Movement", "desc": "Units inbounding to a marketplace"},
    {"key": "ON_ORDER",             "label": "On Order",               "default": True,  "type": "num", "group": "Movement", "desc": "On Order with no work order — will go to STOW"},
    {"key": "OUTBOUND_RESERVED",    "label": "Outbound Reserved",      "default": False, "type": "num", "group": "Movement", "desc": "Units reserved for sale"},
    {"key": "WAREHOUSE_RESERVED",   "label": "WH Reserved",            "default": False, "type": "num", "group": "Movement", "desc": "At marketplace but reserved for sale"},
    {"key": "WAREHOUSE_TRANSFER",   "label": "WH Transfer",            "default": False, "type": "num", "group": "Movement", "desc": "Units transferring between FBA warehouses"},
    {"key": "PATTERN_WAREHOUSE_RESERVED_FOR_MARKETPLACE", "label": "Pattern WH Reserved (Mkt)", "default": False, "type": "num", "group": "Movement", "desc": "In Pattern WH but reserved to move to marketplace (Pick from STOW work order)"},
    {"key": "PATTERN_OUTBOUND_RESERVED_FOR_MARKETPLACE", "label": "Pattern OB Reserved (Mkt)", "default": False, "type": "num", "group": "Movement", "desc": "In Pattern WH but moving out to a marketplace"},
    {"key": "PATTERN_WAREHOUSE_TRANSFER_FOR_MARKETPLACE", "label": "Pattern WH Transfer (Mkt)", "default": False, "type": "num", "group": "Movement", "desc": "Pattern warehouse transfer earmarked for a specific marketplace"},
    {"key": "PATTERN_ON_ORDER_RESERVED_FOR_MARKETPLACE", "label": "Pattern On Order Reserved (Mkt)", "default": False, "type": "num", "group": "Movement", "desc": "On order with a work order to go to a marketplace"},
    # ── Totals (Channel + Pipeline) ──
    {"key": "FULFILLMENT_CHANNEL_UNITS",                "label": "FC Units",               "default": False, "type": "num", "group": "Totals", "desc": "Total units excluding On Order & Unfulfillable"},
    {"key": "FULFILLMENT_CHANNEL_UNITS_3PL",            "label": "FC Units (3PL)",         "default": False, "type": "num", "group": "Totals", "desc": "Channel units in 3PL stock (rare — only if Pattern holds 3PL stock)"},
    {"key": "FULFILLMENT_CHANNEL_UNITS_PATTERN_OWNED",  "label": "FC Units (Pattern Owned)", "default": False, "type": "num", "group": "Totals", "desc": "Channel units that are Pattern-owned (excl. On Order & Unfulfillable)"},
    {"key": "PIPELINE_UNITS",                           "label": "Pipeline Units",         "default": False, "type": "num", "group": "Totals", "desc": "Total units including On Order, excl. Unfulfillable"},
    {"key": "PIPELINE_UNITS_3PL",                       "label": "Pipeline Units (3PL)",   "default": False, "type": "num", "group": "Totals", "desc": "Pipeline in 3PL stock (rare)"},
    {"key": "PIPELINE_UNITS_PATTERN_OWNED",             "label": "Pipeline Units (Pattern Owned)", "default": False, "type": "num", "group": "Totals", "desc": "Pipeline units that are Pattern-owned"},
    # ── Planning Metrics (Weeks of Supply — hidden by default) ──
    {"key": "FULFILLABLE_WOS",                                       "label": "Fulfillable WOS",                 "default": False, "type": "num", "group": "Planning", "desc": "Weeks of Supply based on Fulfillable units"},
    {"key": "WAREHOUSE_TRANSFER_WOS",                                "label": "WH Transfer WOS",                 "default": False, "type": "num", "group": "Planning", "desc": "Weeks of Supply based on Warehouse Transfer"},
    {"key": "FULFILLMENT_CHANNEL_UNITS_PATTERN_OWNED_WOS",           "label": "FC WOS (Pattern Owned)",          "default": False, "type": "num", "group": "Planning", "desc": "Weeks of Supply on fulfillment channel (Pattern owned, actual sales)"},
    {"key": "FULFILLMENT_CHANNEL_UNITS_PATTERN_OWNED_WOS_DEFAULT_FCST", "label": "FC WOS (Pattern Owned, Default Fcst)", "default": False, "type": "num", "group": "Planning", "desc": "Weeks of Supply on fulfillment channel (Pattern owned, default forecast)"},
    {"key": "PIPELINE_UNITS_PATTERN_OWNED_WOS",                      "label": "Pipeline WOS (Pattern Owned)",    "default": False, "type": "num", "group": "Planning", "desc": "Weeks of Supply on pipeline (Pattern owned, actual sales)"},
    {"key": "PIPELINE_UNITS_PATTERN_OWNED_WOS_DEFAULT_FCST",         "label": "Pipeline WOS (Pattern Owned, Default Fcst)", "default": False, "type": "num", "group": "Planning", "desc": "Weeks of Supply on pipeline (Pattern owned, default forecast)"},
    # ── Per-warehouse Breakouts (hidden by default) ──
    {"key": "STORAGE_IDS",          "label": "Storage ID(s)",          "default": True,  "type": "str", "group": "Warehouse Detail", "desc": "Storage / bin locations holding this master in the warehouse (comma-separated, from the current-warehouse table). Covers all statuses, not just sellable."},
    {"key": "WAREHOUSE_STATUS",     "label": "Warehouse Status",       "default": True,  "type": "str", "group": "Warehouse Detail", "desc": "Stock statuses present for this master in the warehouse (e.g. Sellable, Reserved, Unfulfillable). From the current-warehouse table."},
    # ── USD Values (all hidden by default) ──
    {"key": "FULFILLABLE_VALUE_USD",         "label": "Fulfillable $",          "default": False, "type": "num", "group": "USD", "desc": "Fulfillable units value in USD"},
    {"key": "UNFULFILLABLE_VALUE_USD",       "label": "Unfulfillable $",        "default": False, "type": "num", "group": "USD", "desc": "Unfulfillable units value in USD"},
    {"key": "INBOUND_VALUE_USD",             "label": "Inbound $",              "default": False, "type": "num", "group": "USD", "desc": "Inbound units value in USD"},
    {"key": "ON_ORDER_VALUE_USD",            "label": "On Order $",             "default": False, "type": "num", "group": "USD", "desc": "On Order units value in USD"},
    {"key": "OUTBOUND_RESERVED_VALUE_USD",   "label": "Outbound Reserved $",    "default": False, "type": "num", "group": "USD", "desc": "Outbound Reserved units value in USD"},
    {"key": "WAREHOUSE_RESERVED_VALUE_USD",  "label": "WH Reserved $",          "default": False, "type": "num", "group": "USD", "desc": "Warehouse Reserved value in USD"},
    {"key": "WAREHOUSE_TRANSFER_VALUE_USD",  "label": "WH Transfer $",          "default": False, "type": "num", "group": "USD", "desc": "Warehouse Transfer value in USD"},
    {"key": "PATTERN_WAREHOUSE_RESERVED_FOR_MARKETPLACE_VALUE_USD", "label": "Pattern WH Reserved (Mkt) $", "default": False, "type": "num", "group": "USD", "desc": "Pattern WH Reserved for Marketplace value in USD"},
    {"key": "PATTERN_OUTBOUND_RESERVED_FOR_MARKETPLACE_VALUE_USD",  "label": "Pattern OB Reserved (Mkt) $", "default": False, "type": "num", "group": "USD", "desc": "Pattern OB Reserved for Marketplace value in USD"},
    {"key": "PATTERN_WAREHOUSE_TRANSFER_FOR_MARKETPLACE_VALUE_USD", "label": "Pattern WH Transfer (Mkt) $", "default": False, "type": "num", "group": "USD", "desc": "Pattern WH Transfer for Marketplace value in USD"},
    {"key": "PATTERN_ON_ORDER_RESERVED_FOR_MARKETPLACE_VALUE_USD",  "label": "Pattern On Order Reserved (Mkt) $", "default": False, "type": "num", "group": "USD", "desc": "Pattern On Order Reserved for Marketplace value in USD"},
    {"key": "FULFILLMENT_CHANNEL_VALUE_USD_PATTERN_OWNED",          "label": "FC Value (Owned) $",     "default": False, "type": "num", "group": "USD", "desc": "Fulfillment Channel value (Pattern owned) in USD"},
    {"key": "PIPELINE_VALUE_USD_PATTERN_OWNED",                     "label": "Pipeline Value (Owned) $", "default": False, "type": "num", "group": "USD", "desc": "Pipeline value (Pattern owned) in USD"},
]


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
                '3. Results appear right below your search<br>'
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
    st.caption(f"v2.2 • {datetime.date.today().strftime('%B %Y')}")
    st.caption("Built by Merchandise Planning Team")

# ══════════════════════════════════════════════
# Header
# ══════════════════════════════════════════════
st.markdown('<div class="main-header"><div class="header-icon">📦</div><div>'
            '<p class="header-title">Product Catalogue Lookup</p>'
            '<p class="header-sub">Catalogue, FR readiness &amp; listing issues — DNO, shippable, commingled, pricing &amp; more across all marketplaces</p>'
            '</div></div>', unsafe_allow_html=True)

# ══════════════════════════════════════════════
# Region mapping
# ══════════════════════════════════════════════
REGION_MAP = {
    "UK": [
        "Amazon.co.uk", "TikTok UK", "Ebay UK", "Tesco", "Tesco (DEPRECATED)",
    ],
    "EU": [
        "Amazon.de", "Amazon.fr", "Amazon.es", "Amazon.it", "Amazon.nl",
        "Amazon.pl", "Amazon.se", "Amazon.com.be", "Amazon.ie", "Amazon.com.tr",
        "Bol.com", "Bol.com (BE)",
        "Cdiscount", "Ebay DE", "Ebay EU",
        "MediaMarkt", "Otto", "EU DTC", "EU Amazon", "Allegro",
        "Zalando DE", "Zalando ES", "Zalando FR", "Zalando AT", "Zalando IT",
        "Zalando CH", "Zalando BE", "Zalando NL", "Zalando PL", "Zalando SE",
        "Zalando DK", "Zalando FI", "Zalando LU",
    ],
    "Middle East": [
        "Amazon.ae", "Amazon.sa", "Noon",
    ],
    "US": [
        "Amazon.com", "Walmart US", "Best Buy US", "Target+", "Kohl's",
        "Macys.com", "BedBathandBeyond.com", "Belk.com", "Kroger.com", "Lowe's",
        "Costco", "Nordstrom", "TikTok US", "Ebay US", "SHEIN US", "Shopify US",
        "API Orders US",
    ],
    "Australia": [
        "Amazon.com.au", "Catch AU", "Ebay AU", "B2B AU", "Shopify AU",
    ],
    "Canada": [
        "Amazon.ca", "Walmart CA", "Best Buy CA", "B2B CA", "API Orders CA",
    ],
    "APAC": [
        "Amazon.co.jp", "Amazon.sg", "B2B CN", "B2B SEA", "Aikucun", "HKTVmall",
        "Jingdong", "Jingdong B2B", "Jingdong CG", "Jingdong VC", "Kaola",
        "Kidswant", "Kuaishou", "Meituan", "Onion Global", "Pinduoduo", "Poizon",
        "Rakuten JP", "Red", "Weidian", "Yin He", "Youzan", "Coupang KR",
        "Naver KR", "Lazada ID", "Lazada MY", "Lazada PH", "Lazada SG",
        "Shopee MY", "Shopee PH", "Shopee SG", "Shopify HK", "Shopify MY",
        "Shopify SG", "TikTok CN", "Tmall CN", "Tmall VC", "APAC General",
    ],
}

# Region dropdown options (shared by Results + Brand tabs).
# "Other" = any marketplace not mapped to a named region above (derived by exclusion).
REGION_OPTIONS = ["All", "UK", "EU", "Middle East", "US", "Australia", "Canada", "APAC", "Other"]

# ══════════════════════════════════════════════
# Input + Results in tabs
# ══════════════════════════════════════════════
# ══════════════════════════════════════════════
# Listing IRS — Outstanding listing-issues lookup
# ══════════════════════════════════════════════
# Column order as requested (display). Extra columns after "Distinct Listings"
# power the in-app filters.
IRS_DISPLAY_COLS = [
    "Rank", "Listing Identifier", "SKU", "Master ID", "Storage Units (Global)",
    "Brand", "Seller", "Country Code", "Inbound Issue Type", "Inbound Issue Details",
    "IB Issue Details", "Listing Failure Period Details", "Listing DNO Details",
    "Listing Last Shipped Date", "Listing ID", "WO Source Type", "WO Source",
    "WO Warehouse", "Storage Units (Warehouse)", "Work Order (WO) ID",
    "Work Order Item (WOI) ID", "Inbound Order (IO) Number", "Component IO Number",
    "ISL", "WOI Ship By Date", "ISL Appt/IO Placed Date", "DNO End Date", "DNO Note",
    "DNO Reason Code", "DNO Start Date", "DNO Status", "WOI Remaining Qty",
    "WOI Updated Qty", "IOI Ordered Qty", "IOI Updated Qty",
    "IOI Ordered Component Qty", "IOI Updated Component Qty", "Distinct Listings",
    # ── filter-support extras ──
    "Days Outstanding", "Marketplace", "WOI Status", "ISL Status", "Fulfillment Channel",
    "Is Active IB Issue", "Has Units in Storage", "Item Is 3P Owned",
]


def build_irs_query(ids=None):
    """Outstanding listing issues from LISTING_ISSUES_OUTSTANDING_DETAILS.

    The source is a compute-on-read VIEW: it recomputes the whole outstanding
    population (ranks + aggregates) before any WHERE applies, so filtering by ID
    does NOT make it faster. Strategy: pull the WHOLE view ONCE (ids=None),
    cache it in memory, then search / filter instantly in pandas. Passing ids
    keeps the old server-side single-listing path available if ever needed.
    """
    def safe(s):
        return s.strip().replace("'", "''")
    where = ""
    if ids:
        upper_list = ", ".join(f"UPPER('{safe(s)}')" for s in ids if s.strip()) or "''"
        where = (
            f"WHERE UPPER(LISTING_SKU) IN ({upper_list})\n"
            f"   OR UPPER(LISTING_IDENTIFIER) IN ({upper_list})\n"
            f"   OR UPPER(LISTING_MASTER_ID) IN ({upper_list})\n"
            f"   OR UPPER(TO_VARCHAR(LISTING_ID)) IN ({upper_list})"
        )
    return f"""
SELECT
    OVERALL_LISTING_RANK                               AS "Rank",
    LISTING_IDENTIFIER                                 AS "Listing Identifier",
    LISTING_SKU                                        AS "SKU",
    LISTING_MASTER_ID                                  AS "Master ID",
    CURRENT_GLOBAL_UNITS                               AS "Storage Units (Global)",
    BRAND_NAME                                         AS "Brand",
    LISTING_SELLER                                     AS "Seller",
    LISTING_MARKETPLACE_COUNTRY_CODE                   AS "Country Code",
    INBOUND_ISSUE_TYPE                                 AS "Inbound Issue Type",
    INBOUND_ISSUE_DETAILS                              AS "Inbound Issue Details",
    REPLACE(INBOUND_ISSUE_DETAILS, '_', ' ')           AS "IB Issue Details",
    LISTING_FAILURE_PERIOD_DETAILS                     AS "Listing Failure Period Details",
    CASE WHEN LISTING_DNO_START_DATE IS NOT NULL
         THEN 'DNO Start: ' || LISTING_DNO_START_DATE
              || ' | DNO End: ' || LISTING_DNO_END_DATE
              || ' | DNO Note: ' || LISTING_DNO_NOTE
    END                                                AS "Listing DNO Details",
    LISTING_LAST_SHIPPED_AT                            AS "Listing Last Shipped Date",
    LISTING_ID                                         AS "Listing ID",
    WO_SOURCE_TYPE                                     AS "WO Source Type",
    WO_SOURCE                                          AS "WO Source",
    WO_WAREHOUSE_NAME                                  AS "WO Warehouse",
    CURRENT_WAREHOUSE_UNITS                            AS "Storage Units (Warehouse)",
    WO_ID                                              AS "Work Order (WO) ID",
    WOI_ID                                             AS "Work Order Item (WOI) ID",
    IO_NUMBER                                          AS "Inbound Order (IO) Number",
    COALESCE(COMP_IO_NUMBER, CALC_COMPONENT_IO_NUMBER) AS "Component IO Number",
    ISL                                                AS "ISL",
    WOI_ORIGINAL_SHIP_BY_DATE                          AS "WOI Ship By Date",
    COALESCE(ISL_APT_START_TIME, IO_PLACED_AT)         AS "ISL Appt/IO Placed Date",
    LISTING_DNO_END_DATE                               AS "DNO End Date",
    LISTING_DNO_NOTE                                   AS "DNO Note",
    LISTING_DNO_REASON_CODE                            AS "DNO Reason Code",
    LISTING_DNO_START_DATE                             AS "DNO Start Date",
    LISTING_DNO_STATUS                                 AS "DNO Status",
    WOI_REMAINING_QTY                                  AS "WOI Remaining Qty",
    WOI_UPDATED_QTY                                    AS "WOI Updated Qty",
    IOI_ORDERED_QTY                                    AS "IOI Ordered Qty",
    IOI_UPDATED_QTY                                    AS "IOI Updated Qty",
    IOI_ORDERED_COMPONENT_QTY                          AS "IOI Ordered Component Qty",
    IOI_UPDATED_COMPONENT_QTY                          AS "IOI Updated Component Qty",
    LISTING_FAILURE_PERIOD_DAYS_OUTSTANDING            AS "Days Outstanding",
    LISTING_MARKETPLACE                                AS "Marketplace",
    WOI_STATUS                                         AS "WOI Status",
    ISL_STATUS                                         AS "ISL Status",
    LISTING_FULFILLMENT_CHANNEL                        AS "Fulfillment Channel",
    IS_ACTIVE_IB_ISSUE                                 AS "Is Active IB Issue",
    LISTING_HAS_STORAGE_UNITS                          AS "Has Units in Storage",
    LISTING_ITEM_IS_THIRD_PARTY_OWNED                  AS "Item Is 3P Owned"
FROM PATTERN_DB.OPERATIONS.LISTING_ISSUES_OUTSTANDING_DETAILS
{where}
ORDER BY "Rank"
"""


def ist_data_day():
    """IST business day that rolls over at 06:00 IST. Used as the cache key so
    the dataset refreshes once each morning (IST) and is held for the whole day.
    The first person to open the tab after 06:00 IST triggers the fresh pull;
    everyone else that IST day gets the cached copy instantly."""
    ist = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=5, minutes=30)
    if ist.hour < 6:
        ist = ist - datetime.timedelta(days=1)
    return ist.strftime("%Y-%m-%d")


@st.cache_data(ttl=60 * 60 * 26, show_spinner=False)
def load_irs_all(day):
    """Pull the ENTIRE outstanding-issues view once per IST business day and
    cache it (keyed by `day` from ist_data_day()). Slow the first time each
    morning, then every search/filter runs instantly against this in-memory
    copy for the rest of the IST day."""
    conn = get_connection()
    return pd.read_sql(build_irs_query(None), conn)


def filter_irs(df, ids):
    """In-pandas ID filter across the four searchable ID columns."""
    if df.empty or not ids:
        return df.iloc[0:0] if ids else df
    wanted = {s.strip().upper() for s in ids if s.strip()}

    def col_up(col):
        return df[col].astype(str).str.strip().str.upper() if col in df.columns else pd.Series([""] * len(df), index=df.index)

    mask = (
        col_up("SKU").isin(wanted)
        | col_up("Listing Identifier").isin(wanted)
        | col_up("Master ID").isin(wanted)
        | col_up("Listing ID").isin(wanted)
    )
    return df[mask]


cat_fr_tab, combined_tab, inventory_tab, irs_tab = st.tabs(
    ["🔍 Catalogue Lookup", "🔗 Combined Lookup", "📦 Inventory", "🚨 Listing IRS"]
)

with cat_fr_tab:
    st.markdown("")
    # ── Mode toggle: Search by ID  ·  Browse by Brand ──
    if "search_mode" not in st.session_state:
        st.session_state["search_mode"] = "ids"
    sm1, sm2, _ = st.columns([1, 1, 4])
    with sm1:
        if st.button("🔍 Search by ID",
                     type="primary" if st.session_state["search_mode"] == "ids" else "secondary",
                     use_container_width=True, key="search_mode_ids"):
            st.session_state["search_mode"] = "ids"
            st.rerun()
    with sm2:
        if st.button("🏷️ Browse by Brand",
                     type="primary" if st.session_state["search_mode"] == "brand" else "secondary",
                     use_container_width=True, key="search_mode_brand"):
            st.session_state["search_mode"] = "brand"
            st.rerun()
    st.markdown("")

    if st.session_state["search_mode"] == "ids":
        # ══════════════ SEARCH BY ID ══════════════
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
                        st.success(f"✅ Found **{len(df)}** results — see them below. 👇")
                    time.sleep(0.5)
                    progress_bar.empty()
                except Exception as e:
                    progress_bar.empty()
                    st.error(f"Query failed: {e}")
    else:
        # ══════════════ BROWSE BY BRAND ══════════════
        st.markdown("#### 🏷️ Browse All Listings for a Brand")
        st.caption("Select a brand and region to fetch all their listings. Use the filters below to narrow down further.")
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
            brand_region = st.selectbox("Region", REGION_OPTIONS, key="brand_region")
        with bb3:
            result_limit = st.selectbox("Max Results", ["500", "1000", "2000", "5000", "No limit"],
                                         key="brand_limit")

        if sel_brand:
            # Determine region marketplaces
            region_mps = None
            exclude_mapped = False
            if brand_region == "Other":
                exclude_mapped = True            # fetch all, then keep only un-mapped marketplaces
            elif brand_region != "All":
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
                    if exclude_mapped and not df.empty:
                        mapped = set().union(*REGION_MAP.values())
                        df = df[~df["MARKETPLACE"].isin(mapped)]
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
                        st.success(f"✅ Found **{len(df)}** listings for **{sel_brand}**{region_label} — see them below. 👇")
                    time.sleep(0.5)
                    progress_bar.empty()
                except Exception as e:
                    progress_bar.empty()
                    st.error(f"Query failed: {e}")
        else:
            st.info("👆 Select a brand to get started.")

    # ══════════════════════════════════════════════
    # RESULTS (inline — render directly below the search controls)
    # ══════════════════════════════════════════════
    st.markdown("---")
    if "results_df" not in st.session_state or st.session_state.get("results_df", pd.DataFrame()).empty:
        st.info("👆 Run a search above (by ID or by brand) and your results will appear here.")
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
            sel_region = st.selectbox("🌍 Region", REGION_OPTIONS, key="f_region")
            if sel_region == "Other":
                # "Other" = any marketplace not assigned to a named region
                mapped = set().union(*REGION_MAP.values())
                filtered = filtered[~filtered["MARKETPLACE"].isin(mapped)]
            elif sel_region != "All":
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

        with st.expander("👁 Show / Hide Columns"):
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


with combined_tab:
    st.markdown("")
    st.markdown("### 🔗 Combined — Catalogue + FR in one view")
    st.caption(
        "Paste ANY identifier — SKU, Listing ID, ASIN, Master ID, MPN, FNSKU, UPC or EAN. "
        "Every matching listing is resolved, then shown with the full Catalogue view AND the "
        "full FR check below. (e.g. one Part Number that maps to 20 listings checks all 20.)"
    )
    st.markdown("")

    cmb_text = st.text_area(
        "Enter IDs",
        placeholder="One per line, or comma / space separated — any ID type\ne.g.\n12345\nUK-HD-699391\nB0BXT6YCHK\nP0M3SJXI",
        height=140, key="cmb_text", label_visibility="collapsed",
    )
    cmb_ids = []
    if cmb_text.strip():
        cmb_ids = [s.strip() for s in re.split(r"[\n,\s\t]+", cmb_text.strip()) if s.strip()]
        _seen = set()
        cmb_ids = [x for x in cmb_ids if not (x in _seen or _seen.add(x))]
    st.caption(f"**{len(cmb_ids)}** ID(s) entered • Max 200")
    if len(cmb_ids) > 200:
        st.warning("⚠️ Max 200 IDs. Only the first 200 will be processed.")
        cmb_ids = cmb_ids[:200]

    if cmb_ids and st.button(f"🔗 Look up {len(cmb_ids)} ID(s)", type="primary",
                             use_container_width=True, key="cmb_run"):
        with st.spinner("Resolving IDs → listings, then running catalogue + FR check…"):
            try:
                _dfc = run_lookup(cmb_ids)
            except Exception as e:
                _dfc = pd.DataFrame()
                st.error(f"Catalogue lookup failed: {e}")
            _frdf = pd.DataFrame()
            if isinstance(_dfc, pd.DataFrame) and not _dfc.empty and "LISTING_ID" in _dfc.columns:
                _lids = sorted({str(x).strip() for x in _dfc["LISTING_ID"].dropna().tolist() if str(x).strip()})
                _lids = _lids[:500]
                if _lids:
                    try:
                        _frdf = run_fr_lookup(_lids, "LISTING_ID")
                    except Exception as e:
                        st.error(f"FR check failed: {e}")
            st.session_state["cmb_cat"] = _dfc
            st.session_state["cmb_frdf"] = _frdf

    dfc = st.session_state.get("cmb_cat")
    frdf = st.session_state.get("cmb_frdf")

    if isinstance(dfc, pd.DataFrame) and not dfc.empty:
        # De-dupe to one row per listing for both sections
        dfc = dfc.drop_duplicates(subset=["LISTING_ID"]).copy() if "LISTING_ID" in dfc.columns else dfc.copy()

        cmb_view = st.radio(
            "View", ["📊 All-in-one table", "📋 Catalogue view", "🔬 FR check"],
            horizontal=True, key="cmb_view", label_visibility="collapsed",
        )
        st.markdown("")
        if cmb_view == "📋 Catalogue view":
            # ══════════════════════════════════════════════
            # 📋 CATALOGUE SECTION
            # ══════════════════════════════════════════════
            st.markdown("## 📋 Catalogue")
            _dno_c = int((dfc["IS_DNO"] == True).sum()) if "IS_DNO" in dfc.columns else 0
            _ship_c = int((dfc["SHIPPABLE_TAG"] == True).sum()) if "SHIPPABLE_TAG" in dfc.columns else 0
            _fba_c = int((dfc["LISTING_FULFILLMENT_TYPE"].str.upper() == "FBA").sum()) if "LISTING_FULFILLMENT_TYPE" in dfc.columns else 0
            _act_c = int((dfc["IS_ACTIVE"] == True).sum()) if "IS_ACTIVE" in dfc.columns else 0
            _mc = st.columns(6)
            _mc[0].markdown(f'<div class="metric-card mc-total"><div class="label">Total</div><div class="value">{len(dfc)}</div></div>', unsafe_allow_html=True)
            _mc[1].markdown(f'<div class="metric-card mc-dno"><div class="label">DNO = True</div><div class="value">{_dno_c}</div></div>', unsafe_allow_html=True)
            _mc[2].markdown(f'<div class="metric-card mc-ship"><div class="label">Shippable</div><div class="value">{_ship_c}</div></div>', unsafe_allow_html=True)
            _mc[3].markdown(f'<div class="metric-card mc-noship"><div class="label">Not Shippable</div><div class="value">{len(dfc) - _ship_c}</div></div>', unsafe_allow_html=True)
            _mc[4].markdown(f'<div class="metric-card mc-fba"><div class="label">FBA</div><div class="value">{_fba_c}</div></div>', unsafe_allow_html=True)
            _mc[5].markdown(f'<div class="metric-card mc-active"><div class="label">Active</div><div class="value">{_act_c}</div></div>', unsafe_allow_html=True)
            st.markdown("")

            st.markdown("#### 🔽 Filters")
            cmb_search = st.text_input("🔍 Search across all fields", placeholder="Type to search...", key="cmb_search_all")
            pcf = dfc.copy()
            _fr1 = st.columns(4)
            with _fr1[0]:
                _reg = st.selectbox("🌍 Region", REGION_OPTIONS, key="cmb_f_region")
                if _reg == "Other":
                    _mapped = set().union(*REGION_MAP.values())
                    pcf = pcf[~pcf["MARKETPLACE"].isin(_mapped)]
                elif _reg != "All":
                    pcf = pcf[pcf["MARKETPLACE"].isin(REGION_MAP.get(_reg, []))]
            with _fr1[1]:
                _mpv = sorted(pcf["MARKETPLACE"].dropna().unique().tolist()) if "MARKETPLACE" in pcf.columns else []
                _selmp = st.multiselect("Marketplace", options=_mpv, default=[], key="cmb_f_mp", placeholder="All")
                if _selmp:
                    pcf = pcf[pcf["MARKETPLACE"].isin(_selmp)]
            with _fr1[2]:
                pcf = multiselect_filter(pcf, "VENDOR", "Vendor", "cmb_f_vn") if "VENDOR" in pcf.columns else pcf
            with _fr1[3]:
                pcf = bool_multiselect_filter(pcf, "IS_DNO", "DNO", "cmb_f_dno") if "IS_DNO" in pcf.columns else pcf
            _fr2 = st.columns(4)
            with _fr2[0]:
                pcf = bool_multiselect_filter(pcf, "SHIPPABLE_TAG", "Shippable", "cmb_f_ship") if "SHIPPABLE_TAG" in pcf.columns else pcf
            with _fr2[1]:
                pcf = multiselect_filter(pcf, "LISTING_FULFILLMENT_TYPE", "Fulfillment Type", "cmb_f_ff") if "LISTING_FULFILLMENT_TYPE" in pcf.columns else pcf
            with _fr2[2]:
                pcf = multiselect_filter(pcf, "COMMINGLED_STATUS", "Commingled", "cmb_f_cm") if "COMMINGLED_STATUS" in pcf.columns else pcf
            with _fr2[3]:
                pcf = bool_multiselect_filter(pcf, "IS_ACTIVE", "Active", "cmb_f_active") if "IS_ACTIVE" in pcf.columns else pcf
            if cmb_search.strip():
                _m = pcf.astype(str).apply(lambda row: row.str.contains(cmb_search.strip(), case=False).any(), axis=1)
                pcf = pcf[_m]
            st.caption(f"Showing **{len(pcf)}** of **{len(dfc)}** listings")

            _avail = [k for k in COLUMN_MAP if k in pcf.columns]
            _defcols = [k for k in _avail if COLUMN_MAP[k]["default"]]
            _friendly = {COLUMN_MAP[k]["label"]: k for k in _avail}
            with st.expander("👁 Show / Hide Columns"):
                _self = st.multiselect("Choose columns", options=[COLUMN_MAP[k]["label"] for k in _avail],
                                       default=[COLUMN_MAP[k]["label"] for k in _defcols], key="cmb_col_select")
            _selcols = [_friendly[f] for f in _self] if _self else _defcols
            _disp = pcf[_selcols].copy()
            _rmap = {k: COLUMN_MAP[k]["label"] for k in _selcols if k in COLUMN_MAP}
            _disp = _disp.rename(columns=_rmap)
            for _ck, (_tl, _fl) in BOOL_COLS.items():
                _fn = COLUMN_MAP.get(_ck, {}).get("label", _ck)
                if _fn in _disp.columns:
                    _disp[_fn] = _disp[_fn].apply(lambda x, tl=_tl, fl=_fl: tl if x == True else fl)

            def _cmb_color_rows(row):
                n = len(row)
                _d = COLUMN_MAP["IS_DNO"]["label"]; _s = COLUMN_MAP["SHIPPABLE_TAG"]["label"]
                _a = COLUMN_MAP["IS_ACTIVE"]["label"]; _di = COLUMN_MAP["IS_DISCONTINUED"]["label"]
                if _d in row.index and "⛔ YES — DNO" in str(row.get(_d, "")):
                    return ["background-color: rgba(239,68,68,0.10)"] * n
                if _di in row.index and "⛔ Discontinued" in str(row.get(_di, "")):
                    return ["background-color: rgba(245,158,11,0.08)"] * n
                if _a in row.index and "❌ Inactive" in str(row.get(_a, "")):
                    return ["background-color: rgba(100,116,139,0.10)"] * n
                if _s in row.index and "✅ YES" in str(row.get(_s, "")):
                    return ["background-color: rgba(34,197,94,0.05)"] * n
                return [""] * n

            if len(_disp) > 5000:
                st.dataframe(_disp, use_container_width=True, hide_index=True, height=600)
            else:
                st.dataframe(_disp.style.apply(_cmb_color_rows, axis=1), use_container_width=True,
                             hide_index=True, height=min(len(_disp) * 38 + 40, 600))
            _pe1, _pe2 = st.columns(2)
            _pe1.download_button("⬇️ Catalogue — all (CSV)", dfc[_selcols].rename(columns=_rmap).to_csv(index=False),
                                 f"combined_catalogue_all_{datetime.date.today().isoformat()}.csv", "text/csv",
                                 use_container_width=True, key="cmb_pc_dl_all")
            _pe2.download_button("⬇️ Catalogue — filtered (CSV)", pcf[_selcols].rename(columns=_rmap).to_csv(index=False),
                                 f"combined_catalogue_filtered_{datetime.date.today().isoformat()}.csv", "text/csv",
                                 use_container_width=True, key="cmb_pc_dl_filt")

        elif cmb_view == "🔬 FR check":
            _albl = {a["id"]: a["label"] for a in FR_ATTRIBUTES}
            if not isinstance(frdf, pd.DataFrame) or frdf.empty:
                st.info("No FR data resolved for these listings.")
            else:
                _preset = st.radio("Attributes to check", ["Quick Check", "All Attributes", "Custom"], horizontal=True, key="cmb_fr_preset")
                if _preset == "Quick Check":
                    _sel_attr = [a for a in QUICK_CHECK_ATTRS if a in _albl]
                elif _preset == "All Attributes":
                    _sel_attr = [a["id"] for a in FR_ATTRIBUTES]
                else:
                    _lbl2id = {a["label"]: a["id"] for a in FR_ATTRIBUTES}
                    _picklbls = st.multiselect("Pick attributes", options=[a["label"] for a in FR_ATTRIBUTES], default=[_albl[a] for a in QUICK_CHECK_ATTRS if a in _albl], key="cmb_fr_custom")
                    _sel_attr = [_lbl2id[l] for l in _picklbls] if _picklbls else [a["id"] for a in FR_ATTRIBUTES]
                fr_results = run_fr_check(frdf, _sel_attr)
                with st.expander("🔎 FR filters", expanded=False):
                    ff = st.columns(4)
                    _mkts = sorted({(r.get("marketplace") or "") for r in fr_results if r.get("marketplace")})
                    _vnds = sorted({(r.get("vendor") or "") for r in fr_results if r.get("vendor")})
                    _failopts = [_albl[a] for a in _sel_attr if next((x for x in FR_ATTRIBUTES if x["id"] == a), {}).get("type") == "check"]
                    with ff[0]:
                        _res_sel = st.multiselect("Result", ["Pass", "Flagged"], default=[], key="cmb_fr_f_res", placeholder="All")
                    with ff[1]:
                        _mkt_sel = st.multiselect("Marketplace", _mkts, default=[], key="cmb_fr_f_mkt", placeholder="All")
                    with ff[2]:
                        _vnd_sel = st.multiselect("Vendor", _vnds, default=[], key="cmb_fr_f_vnd", placeholder="All")
                    with ff[3]:
                        _fail_sel = st.multiselect("Failing attribute", _failopts, default=[], key="cmb_fr_f_fail", placeholder="Any")
                _fid = {a["label"]: a["id"] for a in FR_ATTRIBUTES}
                def _keep(r):
                    if _res_sel and not (("Pass" in _res_sel and r["passed"]) or ("Flagged" in _res_sel and not r["passed"])):
                        return False
                    if _mkt_sel and (r.get("marketplace") or "") not in _mkt_sel:
                        return False
                    if _vnd_sel and (r.get("vendor") or "") not in _vnd_sel:
                        return False
                    if _fail_sel:
                        for lbl in _fail_sel:
                            if r["details"].get(_fid[lbl], {}).get("status") != "r":
                                return False
                    return True
                fr_results = [r for r in fr_results if _keep(r)]
                _passed = sum(1 for r in fr_results if r["passed"])
                _fm = st.columns(3)
                _fm[0].metric("Listings", len(fr_results))
                _fm[1].metric("✅ Passed", _passed)
                _fm[2].metric("⛔ Flagged", len(fr_results) - _passed)
                _sel_checks = [a for a in _sel_attr if next((x for x in FR_ATTRIBUTES if x["id"] == a), {}).get("type") == "check"]
                _icons = {"g": "✅", "r": "⛔", "i": "ℹ️", "n": "—"}
                _sumrows = []
                for r in fr_results:
                    row = {"Listing ID": r["listing_id"], "SKU": r["sku"], "Product": (str(r["product_name"])[:40] if r["product_name"] else ""), "Result": "✅ Pass" if r["passed"] else f"⛔ {r['red_count']} issue(s)"}
                    for aid in _sel_checks:
                        row[_albl[aid]] = _icons.get(r["details"].get(aid, {}).get("status"), "—")
                    _sumrows.append(row)
                _frsum = pd.DataFrame(_sumrows)
                if _frsum.empty:
                    st.info("No listings match the FR filters.")
                else:
                    def _cmb_fr_color(row):
                        return (["background-color: rgba(239,68,68,0.08)"] * len(row) if "⛔" in str(row.get("Result", "")) else ["background-color: rgba(34,197,94,0.05)"] * len(row))
                    st.dataframe(_frsum.style.apply(_cmb_fr_color, axis=1), use_container_width=True, hide_index=True, height=min(len(_frsum) * 36 + 44, 560))
                    _fe1, _fe2 = st.columns(2)
                    _fe1.download_button("⬇️ FR — shown (CSV)", _frsum.to_csv(index=False), f"combined_fr_{datetime.date.today().isoformat()}.csv", "text/csv", use_container_width=True, key="cmb_fr_dl_shown")
                    _fe2.download_button("⬇️ FR — flagged only (CSV)", _frsum[_frsum["Result"].str.startswith("⛔")].to_csv(index=False), f"combined_fr_flagged_{datetime.date.today().isoformat()}.csv", "text/csv", use_container_width=True, key="cmb_fr_dl_flag")
                    st.markdown("##### 🔍 FR drill-down")
                    _dopts = ["— Select a listing —"] + [f"{r['listing_id']} • {r['sku'] or '—'}" for r in fr_results]
                    _dpick = st.selectbox("Listing", _dopts, key="cmb_fr_drill", label_visibility="collapsed")
                    if _dpick != "— Select a listing —":
                        _dr = fr_results[_dopts.index(_dpick) - 1]
                        _sp = _dr.get("suggested_prep")
                        if _sp:
                            st.markdown(f"💡 **Suggested Prep:** {_sp['id']} — {_sp['desc']}" + (f" · {_sp['note']}" if _sp.get('note') else ""))
                        _chk, _inf = [], []
                        for aid in _sel_attr:
                            _a = next((x for x in FR_ATTRIBUTES if x["id"] == aid), None)
                            if not _a:
                                continue
                            det = _dr["details"].get(aid, {})
                            rowd = {"Attribute": _a["label"], "Value": det.get("value", "") or "—", "Status": {"g": "✅ Pass", "r": "⛔ Fail", "i": "ℹ️ Info", "n": "—"}.get(det.get("status"), "—"), "Note": det.get("text", "") or "—"}
                            (_inf if _a["type"] == "info" else _chk).append(rowd)
                        if _chk:
                            st.markdown("**Checks**")
                            def _dcolor(row):
                                if "Fail" in str(row.get("Status", "")):
                                    return ["background-color: rgba(239,68,68,0.10)"] * len(row)
                                if "Pass" in str(row.get("Status", "")):
                                    return ["background-color: rgba(34,197,94,0.05)"] * len(row)
                                return [""] * len(row)
                            st.dataframe(pd.DataFrame(_chk).style.apply(_dcolor, axis=1), use_container_width=True, hide_index=True)
                        if _inf:
                            st.markdown("**Info**")
                            st.dataframe(pd.DataFrame(_inf), use_container_width=True, hide_index=True)

        else:
            st.markdown("#### 📊 Everything in one table — Catalogue + FR per listing")
            fr_all = run_fr_check(frdf, [a["id"] for a in FR_ATTRIBUTES]) if (isinstance(frdf, pd.DataFrame) and not frdf.empty) else []
            fr_by = {str(r["listing_id"]).strip().upper(): r for r in fr_all}
            _pc_cols = [c for c in COLUMN_MAP if c in dfc.columns]
            _pc_key = [("ASIN", "ASIN"), ("MPN", "MPN"), ("WHOLESALE_PRICE", "Wholesale Price"), ("SHIPPABLE_TAG", "Shippable"), ("MARKETPLACE_SELLER", "Seller")]
            def _sval(v):
                if v is None:
                    return ""
                s = str(v).strip()
                return "" if s.lower() == "nan" else s
            recs = []
            for _, prow in dfc.iterrows():
                lid = _sval(prow.get("LISTING_ID"))
                if not lid:
                    continue
                frr = fr_by.get(lid.upper())
                rec = {}
                for c in _pc_cols:
                    rec[COLUMN_MAP[c]["label"]] = _sval(prow.get(c))
                fr_fails = []
                for a in FR_ATTRIBUTES:
                    det = frr["details"].get(a["id"], {}) if frr else {}
                    stt = det.get("status")
                    if a["type"] == "check":
                        rec["FR · " + a["label"]] = {"g": "✅", "r": "⛔", "i": "ℹ️", "n": "—"}.get(stt, "—" if frr else "n/a")
                        if stt == "r":
                            fr_fails.append(a["label"])
                    else:
                        rec["FR · " + a["label"]] = (det.get("value", "") or "—") if frr else "n/a"
                pc_missing = [lbl for col, lbl in _pc_key if col in dfc.columns and not _sval(prow.get(col))]
                flags = []
                if fr_fails:
                    flags.append("FR: " + ", ".join(fr_fails))
                if pc_missing:
                    flags.append("Missing: " + ", ".join(pc_missing))
                if frr:
                    rec["FR Result"] = "✅ Pass" if frr["passed"] else "⛔ Flagged"
                    rec["FR Issues"] = frr["red_count"]
                else:
                    rec["FR Result"] = "n/a"
                    rec["FR Issues"] = ""
                rec["Flags"] = " · ".join(flags) if flags else "✅ none"
                recs.append(rec)
            full = pd.DataFrame(recs)
            only_flagged = False
            with st.expander("🔎 Filters", expanded=False):
                fcol = st.columns(4)
                with fcol[0]:
                    if "Marketplace" in full.columns:
                        full = multiselect_filter(full, "Marketplace", "Marketplace", "cmb_all_mp")
                with fcol[1]:
                    if "Vendor" in full.columns:
                        full = multiselect_filter(full, "Vendor", "Vendor", "cmb_all_vn")
                with fcol[2]:
                    if "FR Result" in full.columns:
                        full = multiselect_filter(full, "FR Result", "FR Result", "cmb_all_fr")
                with fcol[3]:
                    only_flagged = st.checkbox("Only flagged listings", key="cmb_all_flag")
            if only_flagged and "Flags" in full.columns:
                full = full[full["Flags"] != "✅ none"]
            _nf = int((full["Flags"] != "✅ none").sum()) if "Flags" in full.columns else 0
            mm = st.columns(3)
            mm[0].metric("Listings", len(full))
            mm[1].metric("Flagged", _nf)
            mm[2].metric("Clean", len(full) - _nf)
            all_cols = list(full.columns)
            _prio = ["Listing ID", "SKU", "ASIN", "Master ID", "MPN", "FNSKU", "Product Name", "Marketplace", "Marketplace Seller", "Vendor", "Fulfillment Type", "DNO", "Shippable", "FR Result", "FR Issues", "Flags"]
            _defc = [c for c in _prio if c in all_cols]
            show_all = st.checkbox("Show all columns (every catalogue field + all FR attributes)", key="cmb_all_showall")
            if show_all:
                _selc = all_cols
            else:
                _selc = st.multiselect("Columns to show", options=all_cols, default=_defc, key="cmb_all_cols")
                if not _selc:
                    _selc = _defc
            _selc = [c for c in all_cols if c in set(_selc)]
            _dispf = full[_selc]
            def _acolor(r):
                return (["background-color: rgba(239,68,68,0.08)"] * len(r) if r.get("Flags") != "✅ none" else [""] * len(r))
            _hh = min(680, 90 + 34 * max(len(_dispf), 1))
            if "Flags" in _selc:
                st.dataframe(_dispf.style.apply(_acolor, axis=1), use_container_width=True, hide_index=True, height=_hh)
            else:
                st.dataframe(_dispf, use_container_width=True, hide_index=True, height=_hh)
            _ae1, _ae2 = st.columns(2)
            _ae1.download_button("⬇️ Shown columns (CSV)", _dispf.to_csv(index=False), f"combined_all_shown_{datetime.date.today().isoformat()}.csv", "text/csv", use_container_width=True, key="cmb_all_dl_shown")
            _ae2.download_button("⬇️ Everything (CSV)", full.to_csv(index=False), f"combined_all_full_{datetime.date.today().isoformat()}.csv", "text/csv", use_container_width=True, key="cmb_all_dl_full")

    elif isinstance(dfc, pd.DataFrame):
        st.info("No listings found for those IDs.")


with inventory_tab:
    st.markdown("")
    st.markdown("### 📦 Inventory Lookup")
    st.caption("Stock levels across **all regions** (GB, EU, US, CA, AU, AE, CN, …) from the Inventory Hub, joined with live central-warehouse stock. Search by Master ID, ASIN, or Part Number — or Browse by Brand.")
    st.caption("ℹ️ Hub metrics (Fulfillable, Inbound, On Order, etc.) cover every region. The warehouse columns — **CW Inventory, Pickable, PFS Reserved, Storage ID(s), Warehouse Status, Warehouse** — populate for any region with a Pattern central warehouse (GB→Northampton, EU→Wroclaw, US→Hebron/Las Vegas/Lindon/…, CA→Milton, AE→Dubai, AU→Melbourne, CN→Hong Kong). Regions without a mapped warehouse show blanks there.")
    st.markdown("")

    # ── SEARCH MODE TOGGLE ──
    if "inv_mode" not in st.session_state:
        st.session_state["inv_mode"] = "ids"

    mode_col1, mode_col2, _ = st.columns([1, 1, 4])
    with mode_col1:
        if st.button("🔍 Search by ID", type="primary" if st.session_state["inv_mode"] == "ids" else "secondary",
                     use_container_width=True, key="inv_mode_ids"):
            st.session_state["inv_mode"] = "ids"
            st.rerun()
    with mode_col2:
        if st.button("🏷️ Browse by Brand", type="primary" if st.session_state["inv_mode"] == "brand" else "secondary",
                     use_container_width=True, key="inv_mode_brand"):
            st.session_state["inv_mode"] = "brand"
            st.rerun()
    st.markdown("")

    # ── UPSTREAM FILTERS (apply to Snowflake query — reduce data volume) ──
    with st.expander("⚡ Pre-fetch Filters (faster queries)", expanded=False):
        st.caption("These filters are applied **at the Snowflake level** — only matching rows will be fetched. Leave any of them empty to pull **everything** (all regions, all networks, all types).")
        uf_c1, uf_c2, uf_c3 = st.columns(3)
        with uf_c1:
            inv_uf_region = st.multiselect(
                "Region",
                options=["GB", "EU", "US", "CA", "AU", "AE", "MX", "BR", "CN", "JP", "SG"],
                default=[],
                key="inv_uf_region",
                placeholder="All regions (no filter)",
                help="Leave empty for all regions. Pick one or more to restrict. GB / EU / US / CA / AE / AU / CN are the regions backed by Pattern central warehouses, so the warehouse-only columns (CW Inventory, Pickable, etc.) populate for those.",
            )
        with uf_c2:
            inv_uf_network = st.multiselect(
                "Fulfillment Network",
                options=["Amazon FBA", "Pattern PFS", "Bol FBB", "Coupang CFS",
                         "Noon FBN", "Walmart WFS", "Zalando ZFS"],
                default=[],
                key="inv_uf_network",
                placeholder="All networks (no filter)",
                help="Leave empty for all networks. Pick one or more to restrict.",
            )
        with uf_c3:
            inv_uf_inv_type = st.multiselect(
                "Inventory Type",
                options=["Pattern Owned", "Customer Owned", "3PL"],
                default=[],
                key="inv_uf_inv_type",
                placeholder="All types (no filter)",
                help="Leave empty for all types. Pick one or more to restrict.",
            )

    inventory_master_ids = []
    inv_run_triggered = False

    # Capture upstream filter values for use in queries below
    # Empty list = no filter (build_inventory_query then pulls ALL regions / networks)
    uf_regions = inv_uf_region if inv_uf_region else None
    uf_networks = inv_uf_network if inv_uf_network else None
    uf_inv_types = inv_uf_inv_type if inv_uf_inv_type else None

    if st.session_state["inv_mode"] == "ids":
        # ── ID-BASED INPUT (Master ID · ASIN · Part Number) ──
        inv_id_type = st.radio(
            "Search by", ["Master ID", "ASIN", "Part Number"],
            horizontal=True, key="inv_id_type",
        )
        _ph = {
            "Master ID": "One per line, or comma/space separated\ne.g.\nP0M3SJXI\nP0NEIWB5",
            "ASIN": "One per line, or comma/space separated\ne.g.\nB07PGL4G2R\nB0BXT6YCHK",
            "Part Number": "One per line, or comma/space separated\ne.g.\nUK-BOSCH-786700\n0601072P00",
        }[inv_id_type]
        if inv_id_type == "Master ID":
            st.markdown("**Paste Master IDs** — one per line, or comma/space separated")
        else:
            st.markdown(f"**Paste {inv_id_type}s** — one per line, or comma/space separated "
                        f"(each {inv_id_type} is resolved to its Master ID)")
        inv_text = st.text_area(
            "IDs", placeholder=_ph,
            height=140, key="inv_text", label_visibility="collapsed",
        )

        # Parse input
        raw_ids = []
        if inv_text.strip():
            import re
            raw_ids = [s.strip() for s in re.split(r"[\n,\s\t]+", inv_text.strip()) if s.strip()]
            seen = set()
            raw_ids = [x for x in raw_ids if not (x in seen or seen.add(x))]
        st.caption(f"**{len(raw_ids)}** {inv_id_type}(s) entered • Max 500")
        if raw_ids and len(raw_ids) > 500:
            st.warning("⚠️ Max 500 identifiers. Only first 500 will be processed.")
            raw_ids = raw_ids[:500]
        st.markdown("")

        if raw_ids:
            if st.button(f"▶ Look Up Inventory for {len(raw_ids)} {inv_id_type}(s)",
                         type="primary", use_container_width=True, key="inv_run_ids"):
                inv_run_triggered = True
                progress_bar = st.progress(0, text="🔌 Connecting to Snowflake...")
                try:
                    resolution_map = {}
                    if inv_id_type == "Master ID":
                        # Used as-is — no resolution needed
                        resolved_masters = list({r for r in raw_ids if r})
                    else:
                        id_key = "ASIN" if inv_id_type == "ASIN" else "PART_NUMBER"
                        progress_bar.progress(15, text=f"🔗 Resolving {len(raw_ids)} {inv_id_type}(s) → Master IDs...")
                        try:
                            res = resolve_to_master(raw_ids, id_key)
                            resolved_masters = res["masters"]
                            resolution_map = {"matched": res["matched_keys"]}
                        except Exception as e:
                            st.warning(f"{inv_id_type} resolution had an issue: {e}.")
                            resolved_masters = []

                    if not resolved_masters:
                        progress_bar.empty()
                        st.error(f"No Master IDs could be resolved from your {inv_id_type}(s). Please check the values.")
                    else:
                        progress_bar.progress(35, text="📦 Pulling central-warehouse stock...")
                        time.sleep(0.1)  # let UI breathe
                        progress_bar.progress(55, text=f"🔄 Joining with Inventory Hub for {len(resolved_masters)} Master ID(s)...")
                        inv_df = run_inventory_lookup(resolved_masters,
                                                      regions=uf_regions, networks=uf_networks, inv_types=uf_inv_types)
                        progress_bar.progress(85, text="🧮 Calculating Pickable (CW Inventory − PFS Reserved)...")
                        time.sleep(0.1)
                        progress_bar.progress(100, text=f"✅ Done — {len(inv_df)} row(s) found!")
                        time.sleep(0.3)
                        progress_bar.empty()
                        if inv_df.empty:
                            st.warning("No inventory data found for the given IDs.")
                            st.session_state.pop("inv_df", None)
                        else:
                            st.session_state["inv_df"] = inv_df
                            st.session_state["inv_input_master_ids"] = resolved_masters
                            st.session_state["inv_resolution_map"] = resolution_map
                            st.success(f"✅ Found **{len(inv_df)}** inventory row(s) across **{inv_df['MASTER_ID'].nunique()}** Master ID(s).")
                            if inv_id_type != "Master ID" and resolution_map.get("matched"):
                                st.info(f"🔗 Resolved {len(resolution_map['matched'])} {inv_id_type}(s) → {len(resolved_masters)} Master ID(s).")
                except Exception as e:
                    progress_bar.empty()
                    st.error(f"Inventory lookup failed: {e}")
        else:
            st.info(f"👆 Paste at least one {inv_id_type} to begin.")
    else:
        # ── BRAND-BASED INPUT ──
        st.markdown("**Enter brand name** — case-insensitive, exact match")
        b_col1, b_col2 = st.columns([3, 1])
        with b_col1:
            inv_brand = st.text_input("Brand", placeholder="e.g. The North Face, Patagonia, HEYDUDE",
                                      key="inv_brand", label_visibility="collapsed")
        with b_col2:
            inv_brand_limit = st.selectbox("Max Results", [500, 1000, 2000, 5000, "No limit"],
                                            index=0, key="inv_brand_limit", label_visibility="collapsed")
        st.markdown("")
        if inv_brand.strip():
            if st.button(f"🏷️ Fetch Inventory for '{inv_brand.strip()}'",
                         type="primary", use_container_width=True, key="inv_run_brand"):
                inv_run_triggered = True
                progress_bar = st.progress(0, text="🔌 Connecting to Snowflake...")
                try:
                    progress_bar.progress(15, text=f"🔍 Finding Master IDs for brand '{inv_brand.strip()}'...")
                    limit = None if inv_brand_limit == "No limit" else int(inv_brand_limit)
                    brand_masters = get_brand_master_ids(inv_brand.strip(), limit=limit,
                                                          regions=uf_regions, networks=uf_networks, inv_types=uf_inv_types)
                    if not brand_masters:
                        progress_bar.empty()
                        st.warning(f"No Master IDs found for brand '{inv_brand.strip()}' with the current pre-fetch filters. Try widening the filters.")
                        st.session_state.pop("inv_df", None)
                    else:
                        progress_bar.progress(35, text=f"📦 Pulling central-warehouse stock for {len(brand_masters)} Master IDs...")
                        time.sleep(0.1)
                        progress_bar.progress(55, text=f"🔄 Joining with Inventory Hub data...")
                        inv_df = run_inventory_lookup(brand_masters,
                                                       regions=uf_regions, networks=uf_networks, inv_types=uf_inv_types)
                        progress_bar.progress(85, text="🧮 Calculating Pickable (CW Inventory − PFS Reserved)...")
                        time.sleep(0.1)
                        progress_bar.progress(100, text=f"✅ Done — {len(inv_df)} row(s) found!")
                        time.sleep(0.3)
                        progress_bar.empty()
                        if inv_df.empty:
                            st.warning(f"No inventory rows found for '{inv_brand.strip()}' in the configured pools.")
                            st.session_state.pop("inv_df", None)
                        else:
                            st.session_state["inv_df"] = inv_df
                            st.session_state["inv_input_master_ids"] = brand_masters
                            st.session_state["inv_resolution_map"] = {}
                            st.success(f"✅ Found **{len(inv_df)}** inventory row(s) for **'{inv_brand.strip()}'** across **{inv_df['MASTER_ID'].nunique()}** Master ID(s).")
                except Exception as e:
                    progress_bar.empty()
                    st.error(f"Brand lookup failed: {e}")
        else:
            st.info("👆 Enter a brand name to begin.")

    # ══════════════════════════════════════════════
    # RESULTS DISPLAY
    # ══════════════════════════════════════════════
    if "inv_df" in st.session_state and not st.session_state["inv_df"].empty:
        inv_df_full = st.session_state["inv_df"]

        st.markdown("---")
        st.markdown("##### 📊 Results")

        # ── SUMMARY CARDS (8 cards, 2 rows of 4, with PFS/FBA split) ──
        # Calculate splits
        def split_sum(df, col, network_filter=None, only_pfs=False):
            """Sum a column, optionally filtered by network. Returns formatted string."""
            if col not in df.columns:
                return "—"
            sub = df
            if network_filter:
                sub = sub[sub["FULFILLMENT_NETWORK"] == network_filter]
            return int(sub[col].fillna(0).sum())

        total_rows = len(inv_df_full)
        unique_masters = inv_df_full["MASTER_ID"].nunique()
        dno_count = int((inv_df_full["DNO_STATUS"].astype(str).str.upper() == "TRUE").sum())

        # Per-metric PFS and FBA splits
        # Note: For ACTUAL_AVAILABLE, STOW_PICKABLE, and Pattern WH Reserved (Mkt) — only PFS applies; FBA shows "—"
        sum_fulfillable_pfs = split_sum(inv_df_full, "FULFILLABLE", network_filter="Pattern PFS")
        sum_fulfillable_fba = split_sum(inv_df_full, "FULFILLABLE", network_filter="Amazon FBA")
        sum_unfulfillable_pfs = split_sum(inv_df_full, "UNFULFILLABLE", network_filter="Pattern PFS")
        sum_unfulfillable_fba = split_sum(inv_df_full, "UNFULFILLABLE", network_filter="Amazon FBA")
        # Actual Available — only PFS (dedupe by master to avoid double-counting per pool)
        actual_avail_pfs_df = inv_df_full[inv_df_full["FULFILLMENT_NETWORK"] == "Pattern PFS"].drop_duplicates(["MASTER_ID", "REGION"])
        sum_actual_pfs = int(actual_avail_pfs_df["ACTUAL_AVAILABLE_QTY"].fillna(0).sum()) if "ACTUAL_AVAILABLE_QTY" in actual_avail_pfs_df.columns else 0
        # Pattern WH Reserved (Mkt) — only PFS
        sum_pwhr_pfs = split_sum(inv_df_full, "PATTERN_WAREHOUSE_RESERVED_FOR_MARKETPLACE", network_filter="Pattern PFS")
        # Fulfillment Channel Units
        sum_fc_pfs = split_sum(inv_df_full, "FULFILLMENT_CHANNEL_UNITS", network_filter="Pattern PFS")
        sum_fc_fba = split_sum(inv_df_full, "FULFILLMENT_CHANNEL_UNITS", network_filter="Amazon FBA")
        # Inbound
        sum_inb_pfs = split_sum(inv_df_full, "INBOUND", network_filter="Pattern PFS")
        sum_inb_fba = split_sum(inv_df_full, "INBOUND", network_filter="Amazon FBA")
        # On Order
        sum_oo_pfs = split_sum(inv_df_full, "ON_ORDER", network_filter="Pattern PFS")
        sum_oo_fba = split_sum(inv_df_full, "ON_ORDER", network_filter="Amazon FBA")

        def render_card_dual(label, pfs_val, fba_val=None, color_class=""):
            """Render a card with PFS / FBA split (or just one value if fba_val is None)."""
            html = f'<div class="inv-card"><div class="inv-card-label">{label}</div>'
            if fba_val is None:
                # Single value (e.g., Master IDs)
                v_class = color_class or "v"
                html += f'<div class="inv-card-row"><span class="ch"></span><span class="v {v_class}">{pfs_val:,}</span></div>'
            else:
                # Two values: PFS / FBA
                html += f'<div class="inv-card-row"><span class="ch">PFS</span><span class="v pfs">{pfs_val:,}</span></div>'
                if fba_val == "—":
                    html += f'<div class="inv-card-row"><span class="ch">FBA</span><span class="v muted">—</span></div>'
                else:
                    html += f'<div class="inv-card-row"><span class="ch">FBA</span><span class="v fba">{fba_val:,}</span></div>'
            html += '</div>'
            return html

        # Row 1: Master IDs · Fulfillable · Unfulfillable · Actual Available
        r1c1, r1c2, r1c3, r1c4 = st.columns(4)
        with r1c1:
            st.markdown(render_card_dual("Master IDs", unique_masters, fba_val=None, color_class="master"), unsafe_allow_html=True)
        with r1c2:
            st.markdown(render_card_dual("Fulfillable", sum_fulfillable_pfs, sum_fulfillable_fba), unsafe_allow_html=True)
        with r1c3:
            st.markdown(render_card_dual("Unfulfillable", sum_unfulfillable_pfs, sum_unfulfillable_fba), unsafe_allow_html=True)
        with r1c4:
            st.markdown(render_card_dual("Pickable", sum_actual_pfs, "—"), unsafe_allow_html=True)

        # Row 2: Pattern WH Reserved · Fulfillment Channel Units · Inbound · On Order
        r2c1, r2c2, r2c3, r2c4 = st.columns(4)
        with r2c1:
            st.markdown(render_card_dual("Pattern WH Reserved (Mkt)", sum_pwhr_pfs, "—"), unsafe_allow_html=True)
        with r2c2:
            st.markdown(render_card_dual("Fulfillment Channel Units", sum_fc_pfs, sum_fc_fba), unsafe_allow_html=True)
        with r2c3:
            st.markdown(render_card_dual("Inbound", sum_inb_pfs, sum_inb_fba), unsafe_allow_html=True)
        with r2c4:
            st.markdown(render_card_dual("On Order", sum_oo_pfs, sum_oo_fba), unsafe_allow_html=True)
        st.markdown("")

        # ── FILTERS ──
        with st.expander("🔎 Filters", expanded=True):
            f1, f2, f3, f4 = st.columns(4)
            with f1:
                inv_f_region = st.multiselect("Region", sorted(inv_df_full["REGION"].dropna().unique().tolist()),
                                              key="inv_f_region", placeholder="All regions")
            with f2:
                inv_f_network = st.multiselect("Fulfillment Network",
                                               sorted(inv_df_full["FULFILLMENT_NETWORK"].dropna().unique().tolist()),
                                               key="inv_f_network", placeholder="All networks")
            with f3:
                inv_f_pool = st.multiselect("Inventory Pool",
                                            sorted(inv_df_full["INVENTORY_POOL"].dropna().unique().tolist()),
                                            key="inv_f_pool", placeholder="All pools")
            with f4:
                inv_f_brand = st.multiselect("Brand",
                                             sorted(inv_df_full["BRAND"].dropna().astype(str).unique().tolist()),
                                             key="inv_f_brand", placeholder="All brands")
            f5, f6, f7, f8 = st.columns(4)
            with f5:
                inv_f_vendor = st.multiselect("Vendor",
                                              sorted(inv_df_full["VENDOR"].dropna().astype(str).unique().tolist()),
                                              key="inv_f_vendor", placeholder="All vendors")
            with f6:
                inv_f_dno = st.multiselect("DNO",
                                           sorted(inv_df_full["DNO_STATUS"].dropna().astype(str).unique().tolist()),
                                           key="inv_f_dno", placeholder="All")
            with f7:
                if "INVENTORY_TYPE" in inv_df_full.columns:
                    inv_f_invtype = st.multiselect("Inventory Type",
                                                    sorted(inv_df_full["INVENTORY_TYPE"].dropna().astype(str).unique().tolist()),
                                                    key="inv_f_invtype", placeholder="All")
                else:
                    inv_f_invtype = []
            with f8:
                if "BUNDLE_TYPE" in inv_df_full.columns:
                    inv_f_bundle = st.multiselect("Bundle Type",
                                                   sorted(inv_df_full["BUNDLE_TYPE"].dropna().astype(str).unique().tolist()),
                                                   key="inv_f_bundle", placeholder="All")
                else:
                    inv_f_bundle = []
            # Text search across all string cols
            inv_search = st.text_input("🔍 Quick search (matches any column)", key="inv_search", placeholder="Type to filter...")

        # Apply filters
        inv_filtered = inv_df_full.copy()
        if inv_f_region: inv_filtered = inv_filtered[inv_filtered["REGION"].isin(inv_f_region)]
        if inv_f_network: inv_filtered = inv_filtered[inv_filtered["FULFILLMENT_NETWORK"].isin(inv_f_network)]
        if inv_f_pool: inv_filtered = inv_filtered[inv_filtered["INVENTORY_POOL"].isin(inv_f_pool)]
        if inv_f_brand: inv_filtered = inv_filtered[inv_filtered["BRAND"].astype(str).isin(inv_f_brand)]
        if inv_f_vendor: inv_filtered = inv_filtered[inv_filtered["VENDOR"].astype(str).isin(inv_f_vendor)]
        if inv_f_dno: inv_filtered = inv_filtered[inv_filtered["DNO_STATUS"].astype(str).isin(inv_f_dno)]
        if inv_f_invtype and "INVENTORY_TYPE" in inv_filtered.columns:
            inv_filtered = inv_filtered[inv_filtered["INVENTORY_TYPE"].astype(str).isin(inv_f_invtype)]
        if inv_f_bundle and "BUNDLE_TYPE" in inv_filtered.columns:
            inv_filtered = inv_filtered[inv_filtered["BUNDLE_TYPE"].astype(str).isin(inv_f_bundle)]
        if inv_search.strip():
            term = inv_search.strip().lower()
            mask = inv_filtered.astype(str).apply(lambda r: term in " ".join(r.values).lower(), axis=1)
            inv_filtered = inv_filtered[mask]

        # ── COLUMN VISIBILITY (clean: one preset picker + optional fine-tune) ──
        with st.expander("⚙️ Columns", expanded=False):
            COL_PRESETS = {
                "Compact":   ["MASTER_ID", "PART_NUMBER", "ASIN", "REGION", "INVENTORY_POOL",
                              "FULFILLMENT_NETWORK", "DNO_STATUS", "FULFILLABLE",
                              "STOW_PICKABLE_QTY", "ACTUAL_AVAILABLE_QTY"],
                "Standard":  [c["key"] for c in INVENTORY_COLUMNS if c["default"]],
                "Planning":  [c["key"] for c in INVENTORY_COLUMNS if c["group"] in ("Identifiers", "Categorization", "Core", "Planning")],
                "Financial": [c["key"] for c in INVENTORY_COLUMNS if c["group"] in ("Identifiers", "Categorization", "Core", "USD")],
                "All":       [c["key"] for c in INVENTORY_COLUMNS],
            }
            chosen_preset = st.radio(
                "Quick view",
                list(COL_PRESETS.keys()) + ["Custom"],
                index=1,  # default to Standard
                horizontal=True,
                key="inv_preset",
            )
            if chosen_preset == "Custom":
                # One searchable multiselect, grouped labels — only shown when fine-tuning
                label_to_key = {f"{c['group']} · {c['label']}": c["key"] for c in INVENTORY_COLUMNS}
                key_to_label = {c["key"]: f"{c['group']} · {c['label']}" for c in INVENTORY_COLUMNS}
                default_labels = [key_to_label[k] for k in COL_PRESETS["Standard"]]
                chosen_labels = st.multiselect(
                    "Pick exactly the columns you want",
                    options=list(label_to_key.keys()),
                    default=default_labels,
                    key="inv_custom_cols",
                    placeholder="Search columns…",
                )
                visible = [label_to_key[lbl] for lbl in chosen_labels]
            else:
                visible = COL_PRESETS[chosen_preset]
            # Store in INVENTORY_COLUMNS order so table column order stays consistent
            visible_set = set(visible)
            st.session_state["inv_visible_cols"] = [c["key"] for c in INVENTORY_COLUMNS if c["key"] in visible_set]
            st.caption(
                f"Showing **{len(st.session_state['inv_visible_cols'])}** columns. "
                "Pick **Custom** to fine-tune, and drag column headers in the table to reorder."
            )

        # ── DISPLAY: TABLE VIEW ──
        visible_keys = [k for k in st.session_state["inv_visible_cols"] if k in inv_filtered.columns]
        if not visible_keys:
            st.warning("No columns selected. Pick at least one in '⚙️ Columns'.")
        else:
            # Rename columns to friendly labels for display
            label_map = {c["key"]: c["label"] for c in INVENTORY_COLUMNS}
            display_df = inv_filtered[visible_keys].copy()
            display_df.columns = [label_map.get(k, k) for k in visible_keys]

            st.caption(f"Showing **{len(display_df):,}** of **{len(inv_df_full):,}** rows")

            # Style: highlight calculated columns + DNO rows + visually separate Master ID groups
            # st.dataframe ignores border styling, so we use alternating background tints per Master ID group
            master_id_group_num = {}  # master_id -> group index (0, 1, 2, ...)
            if "Master ID" in display_df.columns:
                seen_masters = []
                for val in display_df["Master ID"].tolist():
                    if val not in seen_masters:
                        seen_masters.append(val)
                master_id_group_num = {m: i for i, m in enumerate(seen_masters)}

            def inv_color_rows(row):
                styles = [""] * len(row)
                # Base: alternating Master ID group tints (very subtle, just enough to see the boundary)
                if "Master ID" in row.index:
                    master = row.get("Master ID")
                    if master in master_id_group_num:
                        # Even groups: slightly lighter; Odd groups: slightly darker (creates visible alternation)
                        if master_id_group_num[master] % 2 == 1:
                            styles = ["background-color: rgba(148,163,184,0.12)"] * len(row)
                # DNO red tint (overrides alternation)
                if "DNO" in row.index:
                    if str(row.get("DNO", "")).strip().upper() == "TRUE":
                        styles = ["background-color: rgba(239,68,68,0.12)"] * len(row)
                # Highlight the two key calculated columns in green: CW Inventory & Pickable
                GREEN_COLS = {"CW Inventory", "Pickable"}
                for i, col in enumerate(row.index):
                    if col in GREEN_COLS:
                        try:
                            v = float(row[col]) if pd.notna(row[col]) else None
                        except (ValueError, TypeError):
                            v = None
                        if v is None:
                            continue
                        if col == "Pickable" and v < 0:
                            # negative free stock is a red flag — keep an amber warning
                            styles[i] = "background-color: rgba(245,158,11,0.25); color: #f59e0b; font-weight: 600;"
                        else:
                            styles[i] = "background-color: rgba(34,197,94,0.18); color: #22c55e; font-weight: 600;"
                return styles

            if len(display_df) > 5000:
                st.info(f"📊 Showing {len(display_df):,} rows — row coloring disabled for performance.")
                st.dataframe(display_df, use_container_width=True, hide_index=True, height=600,
                             column_config={display_df.columns[0]: st.column_config.Column(pinned="left")} if len(display_df.columns) > 0 else None)
            else:
                col_config = {}
                if len(display_df.columns) > 0:
                    col_config[display_df.columns[0]] = st.column_config.Column(pinned="left")
                st.dataframe(
                    display_df.style.apply(inv_color_rows, axis=1),
                    use_container_width=True, hide_index=True,
                    height=min(len(display_df) * 38 + 40, 700),
                    column_config=col_config,
                )

        # ── QUICK COPY ──
        st.markdown("")
        with st.expander("📋 Quick Copy — copy a full column of values", expanded=False):
            st.caption("Pick a column from the filtered results, then click Copy to download all its unique values as a list (one per line).")
            # Build list of available columns from the actual data + use friendly labels
            label_map_all = {c["key"]: c["label"] for c in INVENTORY_COLUMNS}
            available_for_copy = [k for k in inv_filtered.columns if k in label_map_all]
            label_to_key_copy = {label_map_all[k]: k for k in available_for_copy}
            copy_labels = [label_map_all[k] for k in available_for_copy]
            if not copy_labels:
                st.info("No data available to copy.")
            else:
                qc_c1, qc_c2 = st.columns([3, 1])
                with qc_c1:
                    # Default to Master ID if available
                    default_idx = 0
                    if "Master ID" in copy_labels:
                        default_idx = copy_labels.index("Master ID")
                    chosen_label = st.selectbox(
                        "Column to copy",
                        options=copy_labels,
                        index=default_idx,
                        key="inv_qc_col",
                        label_visibility="collapsed",
                    )
                    chosen_key = label_to_key_copy[chosen_label]
                    unique_vals = inv_filtered[chosen_key].dropna().unique().tolist()
                    copy_text = "\n".join(str(v) for v in unique_vals)
                with qc_c2:
                    st.download_button(
                        f"📋 Copy {chosen_label} ({len(unique_vals)})",
                        copy_text,
                        f"{chosen_key.lower()}_values.txt",
                        "text/plain",
                        use_container_width=True,
                        key="inv_qc_download",
                    )
                # Show a preview of the values
                if unique_vals:
                    preview = "\n".join(str(v) for v in unique_vals[:10])
                    if len(unique_vals) > 10:
                        preview += f"\n… and {len(unique_vals) - 10} more"
                    st.code(preview, language=None)

        # ── EXPORTS ──
        st.markdown("")
        st.markdown("##### 📤 Export")
        e1, e2, e3, e4 = st.columns(4)
        with e1:
            st.download_button(
                f"⬇️ All ({len(inv_df_full):,}) — CSV",
                inv_df_full.to_csv(index=False),
                f"inventory_all_{datetime.date.today().isoformat()}.csv",
                "text/csv", use_container_width=True, key="inv_dl_all_csv",
            )
        with e2:
            st.download_button(
                f"⬇️ Filtered ({len(inv_filtered):,}) — CSV",
                inv_filtered.to_csv(index=False),
                f"inventory_filtered_{datetime.date.today().isoformat()}.csv",
                "text/csv", use_container_width=True, key="inv_dl_filt_csv",
            )
        with e3:
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                inv_df_full.to_excel(writer, sheet_name="Inventory", index=False)
            st.download_button(
                f"⬇️ All — Excel",
                buf.getvalue(),
                f"inventory_all_{datetime.date.today().isoformat()}.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True, key="inv_dl_all_xlsx",
            )
        with e4:
            buf2 = io.BytesIO()
            with pd.ExcelWriter(buf2, engine="openpyxl") as writer:
                inv_filtered.to_excel(writer, sheet_name="Inventory_Filtered", index=False)
            st.download_button(
                f"⬇️ Filtered — Excel",
                buf2.getvalue(),
                f"inventory_filtered_{datetime.date.today().isoformat()}.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True, key="inv_dl_filt_xlsx",
            )


with irs_tab:
    st.markdown("")
    st.markdown("### 🚨 Listing IRS — Outstanding Issues")
    st.caption(
        "Outstanding inbound / work-order issues from `LISTING_ISSUES_OUTSTANDING_DETAILS`. "
        "Search by **SKU, Listing Identifier, Master ID, or Listing ID**."
    )
    st.info(
        "⚡ **How this stays fast:** the source is a heavy compute-on-read view, so instead "
        "of querying it on every search, the tab loads the whole issues dataset **once each "
        "morning (06:00 IST)** and keeps it in memory for the rest of the IST day. The first "
        "load of the day takes ~1–3 min; after that, every search and filter is **instant**. "
        "Use Refresh to force a fresh pull."
    )
    st.markdown("")

    _data_day = ist_data_day()

    # ── Load / refresh the full dataset (cached per IST business day) ──
    lc1, lc2 = st.columns([1, 3])
    with lc1:
        if st.button("⤓ Load / refresh data", type="primary", use_container_width=True, key="irs_load"):
            load_irs_all.clear()
            st.session_state.pop("irs_all_day", None)
            with st.spinner("Loading the outstanding-issues dataset (one-time, ~1–3 min)…"):
                try:
                    st.session_state["irs_all"] = load_irs_all(_data_day)
                    st.session_state["irs_all_day"] = _data_day
                except Exception as e:
                    st.error(f"Load failed: {e}")

    # Auto-load on first visit, and auto-refresh when the IST business day rolls over
    if st.session_state.get("irs_all_day") != _data_day or "irs_all" not in st.session_state:
        with st.spinner("Loading the outstanding-issues dataset (one-time, ~1–3 min)…"):
            try:
                st.session_state["irs_all"] = load_irs_all(_data_day)
                st.session_state["irs_all_day"] = _data_day
            except Exception as e:
                st.error(f"Load failed: {e}")

    irs_all = st.session_state.get("irs_all")
    with lc2:
        if isinstance(irs_all, pd.DataFrame) and not irs_all.empty:
            _nlist = irs_all["Listing ID"].nunique() if "Listing ID" in irs_all.columns else "?"
            st.caption(f"✅ Loaded **{len(irs_all):,}** issue rows across **{_nlist:,}** listings "
                       f"· data as of **{_data_day}** (06:00 IST daily refresh) · searches are instant")
        else:
            st.caption("No data loaded yet — click **Load / refresh data**.")

    st.markdown("")
    irs_text = st.text_area(
        "Enter IDs",
        placeholder="One per line, or comma / space separated — SKU, Listing Identifier, "
                    "Master ID, or Listing ID\ne.g.\nUK-HD-699391\nL0NC2POW\nP0M3SJXI\n1234567",
        height=140, key="irs_text", label_visibility="collapsed",
    )
    irs_ids = []
    if irs_text.strip():
        irs_ids = [s.strip() for s in re.split(r"[\n,\s\t]+", irs_text.strip()) if s.strip()]
        _seen = set()
        irs_ids = [x for x in irs_ids if not (x in _seen or _seen.add(x))]
    st.caption(f"**{len(irs_ids)}** ID(s) entered — results update instantly as you type")

    dfi = None
    if isinstance(irs_all, pd.DataFrame) and not irs_all.empty and irs_ids:
        dfi = filter_irs(irs_all, irs_ids).copy()
        if dfi.empty:
            st.info("No outstanding issues found for those IDs. ✅  (No news = good.)")

    if dfi is not None and not dfi.empty:
        st.markdown("#### 🔎 Filters")
        fc1, fc2, fc3 = st.columns(3)
        with fc1:
            dfi = multiselect_filter(dfi, "Brand", "Brand", "irs_f_brand")
            dfi = multiselect_filter(dfi, "Seller", "Seller", "irs_f_seller")
            dfi = multiselect_filter(dfi, "Country Code", "Country Code", "irs_f_country")
            dfi = multiselect_filter(dfi, "Marketplace", "Marketplace", "irs_f_mp")
        with fc2:
            dfi = multiselect_filter(dfi, "Inbound Issue Type", "Inbound Issue Type", "irs_f_ibtype")
            dfi = multiselect_filter(dfi, "Inbound Issue Details", "Inbound Issue Details", "irs_f_ibdet")
            dfi = multiselect_filter(dfi, "WO Source Type", "WO Source Type", "irs_f_wosrc")
            dfi = multiselect_filter(dfi, "WO Warehouse", "WO Warehouse", "irs_f_whse")
        with fc3:
            dfi = multiselect_filter(dfi, "DNO Status", "DNO Status", "irs_f_dno")
            dfi = bool_multiselect_filter(dfi, "Is Active IB Issue", "Is Active IB Issue", "irs_f_activeib")
            dfi = bool_multiselect_filter(dfi, "Has Units in Storage", "Listing Has Units in Storage", "irs_f_stor")
            dfi = bool_multiselect_filter(dfi, "Item Is 3P Owned", "Item Is 3P Owned", "irs_f_3p")

        if "Days Outstanding" in dfi.columns:
            _days = pd.to_numeric(dfi["Days Outstanding"], errors="coerce")
            _max_days = int(_days.max()) if _days.notna().any() else 0
            if _max_days > 0:
                min_days = st.slider("Min Days Outstanding", 0, _max_days, 0, key="irs_f_days")
                if min_days > 0:
                    dfi = dfi[_days.fillna(0) >= min_days]

        _distinct = int(dfi["Listing ID"].nunique()) if "Listing ID" in dfi.columns else 0
        dfi["Distinct Listings"] = _distinct

        m1, m2 = st.columns(2)
        m1.metric("Issue Rows", len(dfi))
        m2.metric("Distinct Listings", _distinct)

        # Show columns in the requested order (any missing are simply skipped)
        show_cols = [c for c in IRS_DISPLAY_COLS if c in dfi.columns]
        st.dataframe(dfi[show_cols], use_container_width=True, hide_index=True, height=600)

        st.download_button(
            "⬇️ Download results (CSV)",
            dfi[show_cols].to_csv(index=False),
            f"listing_irs_{datetime.date.today().isoformat()}.csv",
            "text/csv", use_container_width=True, key="irs_dl_csv",
        )
