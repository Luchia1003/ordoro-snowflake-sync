

import os
import time
from typing import Any, Dict, List, Iterable, Optional
import pandas as pd
import requests

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from datetime import datetime

def _fmt_dur(seconds: float) -> str:
    seconds = int(seconds)
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

# ===== 必填环境变量读取 =====
def _req(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v

# ===== 配置（全部来自环境变量）=====
BASE = "https://api.ordoro.com"
CLIENT_ID = _req("ORDORO_CLIENT_ID")
CLIENT_SECRET = _req("ORDORO_CLIENT_SECRET")

PAGE_LIMIT   = int(os.getenv("PAGE_LIMIT", "100"))
MAX_RETRIES  = int(os.getenv("MAX_RETRIES", "5"))
BACKOFF_BASE = float(os.getenv("BACKOFF_BASE", "1.4"))

SF_USER      = _req("SF_USER")
SF_ACCOUNT   = _req("SF_ACCOUNT")
SF_WAREHOUSE = _req("SF_WAREHOUSE")
SF_DATABASE  = _req("SF_DATABASE")
SF_SCHEMA    = _req("SF_SCHEMA")
SF_ROLE      = _req("SF_ROLE")

# 私钥 PEM 内容（整段，以 -----BEGIN PRIVATE KEY----- 开头）
SF_PRIVATE_KEY_PEM = _req("SF_PRIVATE_KEY_PEM")

TABLE_PRODUCTS_SNAP    = "INVENTORY_PRODUCT_LEVEL_SNAP"
TABLE_WAREHOUSES_SNAP  = "INVENTORY_WAREHOUSE_LEVEL_SNAP"
TABLE_PRODUCTS_HIST    = "INVENTORY_PRODUCT_LEVEL_HIST"
TABLE_WAREHOUSES_HIST  = "INVENTORY_WAREHOUSE_LEVEL_HIST"

# 这次不写历史表
ENABLE_HISTORY = False
FILTER_ARCHIVED = False

# ===== Snowflake 连接（使用环境变量里的 PEM 字符串）=====
def connect_snowflake():
    private_key_pem_bytes = SF_PRIVATE_KEY_PEM.encode("utf-8")
    p_key = serialization.load_pem_private_key(
        private_key_pem_bytes, password=None, backend=default_backend()
    )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return snowflake.connector.connect(
        user=SF_USER,
        account=SF_ACCOUNT,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
        role=SF_ROLE,
        private_key=pkb,
    )

# ===== 请求 + 重试 =====
def _request_with_retry(url: str, params: dict) -> requests.Response:
    attempt = 0
    while True:
        try:
            r = requests.get(url, params=params, auth=(CLIENT_ID, CLIENT_SECRET),
                             headers={"Accept":"application/json"}, timeout=60, allow_redirects=False)
            if r.is_redirect:
                loc = r.headers.get("Location") or r.headers.get("location")
                if loc and loc.startswith("http://"):
                    loc = "https://" + loc[len("http://"):]
                    r = requests.get(loc, auth=(CLIENT_ID, CLIENT_SECRET),
                                     headers={"Accept":"application/json"}, timeout=60, allow_redirects=False)
            if r.status_code in (429,500,502,503,504):
                raise requests.HTTPError(f"Retryable status {r.status_code}", response=r)
            r.raise_for_status()
            return r
        except requests.RequestException:
            attempt += 1
            if attempt > MAX_RETRIES: raise
            time.sleep(BACKOFF_BASE ** attempt)

# ===== 双模翻页（offset→page）=====
def iter_products_batches(limit_each: int = PAGE_LIMIT) -> Iterable[List[dict]]:
    """
    Ordoro API 分页迭代器：
    自动在最后一页停止（返回数量 < limit_each 时）。
    支持 offset 模式与 page 模式。
    """
    offset = 0
    last_first_id: Optional[Any] = None
    use_page_mode = False
    page = 1

    while True:
        params = {"limit": limit_each, ("page" if use_page_mode else "offset"): (page if use_page_mode else offset)}
        r = _request_with_retry(f"{BASE}/product/", params=params)
        data = r.json()
        batch: List[dict] = []

        if isinstance(data, dict):
            for key in ("product", "products", "results", "data", "items"):
                if key in data and isinstance(data[key], list):
                    batch = data[key]
                    break
            if not batch and data:
                batch = [data]
        elif isinstance(data, list):
            batch = data

        if not batch:
            print("📘 No more data, pagination ended.")
            break

        first_id = (batch[0] or {}).get("id")
        if not use_page_mode and last_first_id is not None and first_id == last_first_id:
            use_page_mode = True
            page = (offset // limit_each) + 2
            continue

        yield batch

        if len(batch) < limit_each:
            print(f"📘 Final batch reached ({len(batch)} records). Stop iteration.")
            break

        if use_page_mode:
            page += 1
        else:
            offset += len(batch)
            last_first_id = first_id

# ===== 文本清洗 =====
def _safe_str(x):
    s = str(x) if x is not None else ""
    return s.replace("|","/").replace(":", "：")

def _join_tags(tags_obj)->str:
    out=[]
    if isinstance(tags_obj,list):
        for t in tags_obj:
            if isinstance(t,str): out.append(_safe_str(t))
            elif isinstance(t,dict):
                v=t.get("name") or t.get("label") or t.get("tag") or t.get("value") or t.get("title")
                out.append(_safe_str(v if isinstance(v,str) else t))
            else: out.append(_safe_str(t))
    elif isinstance(tags_obj,dict):
        v=tags_obj.get("name") or tags_obj.get("label") or tags_obj.get("tag") or tags_obj.get("value") or tags_obj.get("title")
        if v: out.append(_safe_str(v))
    elif isinstance(tags_obj,str): out.append(_safe_str(tags_obj))
    return "|".join(out)

def _join_carts(carts_obj)->str:
    out=[]
    if isinstance(carts_obj,list):
        for c in carts_obj:
            if isinstance(c,dict):
                vendor=c.get("vendor") or c.get("channel") or c.get("platform") or c.get("site") or ""
                name=c.get("name") or c.get("store") or c.get("account") or ""
                out.append(f"{_safe_str(vendor)}:{_safe_str(name)}".strip(":"))
            else: out.append(_safe_str(c))
    elif isinstance(carts_obj,dict):
        vendor=carts_obj.get("vendor") or c.get("channel") or c.get("platform") or c.get("site") or ""
        name=carts_obj.get("name") or c.get("store") or c.get("account") or ""
        out.append(f"{_safe_str(vendor)}:{_safe_str(name)}".strip(":"))
    elif isinstance(carts_obj,str): out.append(_safe_str(carts_obj))
    return "|".join(out)

# ===== 字段映射 =====
def _is_archived_like(p: Dict[str,Any]) -> bool:
    return bool(p.get("archived") or p.get("is_archived") or p.get("deleted") or p.get("is_deleted"))

def base_product_cols(p: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if FILTER_ARCHIVED and _is_archived_like(p): return None
    return {
        "PRODUCT_ID": p.get("id"),
        "SKU": p.get("sku"),
        "NAME": p.get("name"),
        "PRICE": p.get("price"),
        "COST": p.get("cost"),
        "UPC": p.get("upc"),
        "ASIN": p.get("asin"),
        "COUNTRY": p.get("country_of_origin"),
        "UPDATED": p.get("updated"),
        "TOTAL_ON_HAND": p.get("total_on_hand"),
        "TOTAL_AVAILABLE": p.get("total_available"),
        "TOTAL_COMMITTED": p.get("total_committed"),
        "TOTAL_ALLOCATED": p.get("total_allocated"),
        "TOTAL_UNALLOCATED": p.get("total_unallocated"),
        "TOTAL_MFG_ORDERED": p.get("total_mfg_ordered"),
        "TO_BE_SHIPPED": p.get("to_be_shipped"),
        "HEIGHT": p.get("height"),
        "WEIGHT": p.get("weight"),
        "WIDTH": p.get("width"),
        "TAGS": _join_tags(p.get("tags")),
        "CARTS": _join_carts(p.get("carts") or []),
    }

def rows_for_warehouses(p: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows=[]; base=base_product_cols(p)
    if base is None: return rows
    for w in (p.get("warehouses") or []):
        if not isinstance(w,dict): continue
        rows.append({ **base,
            "WH_ID": w.get("id"), "WH_NAME": w.get("warehouse_name"),
            "WH_UPDATED": w.get("updated"), "WH_CREATED": w.get("warehouse_created_date"),
            "WH_LAST_CHANGE": w.get("warehouse_updated_date"),
            "LOW_STOCK_THTD": w.get("low_stock_threshold"), "OOS_THTD": w.get("out_of_stock_threshold"),
            "POH": w.get("physical_on_hand"), "AOH": w.get("available"),
            "CMT": w.get("committed"), "OMO": w.get("mfg_ordered"), "OPO": w.get("po_committed"),
            "ON_HAND": w.get("on_hand"), "ALLOCATED": w.get("allocated"), "UNALLOCATED": w.get("unallocated"),
            "WH_SHIP_CFG": w.get("is_configured_for_shipping"), "WH_IS_DEFAULT": w.get("is_default_location"),
            "LOCATION": w.get("location_in_warehouse"),
        })
    return rows

# ===== 清洗 =====
PRODUCT_TARGET_COLS = [
    "PRODUCT_ID","SKU","NAME","PRICE","COST","UPC","ASIN","COUNTRY","UPDATED",
    "TOTAL_ON_HAND","TOTAL_AVAILABLE","TOTAL_COMMITTED","TOTAL_ALLOCATED",
    "TOTAL_UNALLOCATED","TOTAL_MFG_ORDERED","TO_BE_SHIPPED","HEIGHT","WEIGHT","WIDTH","TAGS","CARTS",
]
WAREHOUSE_TARGET_COLS = [
    "PRODUCT_ID","SKU","NAME","PRICE","COST","UPC","ASIN","COUNTRY","UPDATED",
    "TOTAL_ON_HAND","TOTAL_AVAILABLE","TOTAL_COMMITTED","TOTAL_ALLOCATED",
    "TOTAL_UNALLOCATED","TOTAL_MFG_ORDERED","TO_BE_SHIPPED","HEIGHT","WEIGHT","WIDTH","TAGS","CARTS",
    "WH_ID","WH_NAME","WH_UPDATED","WH_CREATED","WH_LAST_CHANGE",
    "LOW_STOCK_THTD","OOS_THTD","POH","AOH","CMT","OMO","OPO","ON_HAND","ALLOCATED","UNALLOCATED",
    "WH_SHIP_CFG","WH_IS_DEFAULT","LOCATION",
]
PRODUCT_QTY_COLS = ["TOTAL_ON_HAND","TOTAL_AVAILABLE","TOTAL_COMMITTED","TOTAL_ALLOCATED",
                    "TOTAL_UNALLOCATED","TOTAL_MFG_ORDERED","TO_BE_SHIPPED"]
WAREHOUSE_QTY_COLS = ["LOW_STOCK_THTD","OOS_THTD","POH","AOH","CMT","OMO","OPO","ON_HAND","ALLOCATED","UNALLOCATED"]

def _strip_tz(series: pd.Series) -> pd.Series:
    try: return series.dt.tz_convert("UTC").dt.tz_localize(None)
    except Exception:
        try: return series.dt.tz_localize(None)
        except Exception: return series

def clean_products_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df.reindex(columns=PRODUCT_TARGET_COLS)
    if "UPDATED" in df.columns:
        df["UPDATED"]=pd.to_datetime(df["UPDATED"], errors="coerce"); df["UPDATED"]=_strip_tz(df["UPDATED"])
    for c in ["PRICE","COST"]:
        if c in df.columns: df[c]=pd.to_numeric(df[c], errors="coerce").round(2)
    for c in PRODUCT_QTY_COLS:
        if c in df.columns: df[c]=pd.to_numeric(df[c], errors="coerce").fillna(0).round(0).astype("Int64")
    for c in ["HEIGHT","WEIGHT","WIDTH","PRODUCT_ID"]:
        if c in df.columns: df[c]=pd.to_numeric(df[c], errors="coerce")
    return df.reindex(columns=PRODUCT_TARGET_COLS)

def clean_wh_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df.reindex(columns=WAREHOUSE_TARGET_COLS)
    for t in ["UPDATED","WH_UPDATED","WH_CREATED","WH_LAST_CHANGE"]:
        if t in df.columns:
            df[t]=pd.to_datetime(df[t], errors="coerce"); df[t]=_strip_tz(df[t])
    for c in ["PRICE","COST"]:
        if c in df.columns: df[c]=pd.to_numeric(df[c], errors="coerce").round(2)
    for c in WAREHOUSE_QTY_COLS+["PRODUCT_ID","WH_ID"]:
        if c in df.columns: df[c]=pd.to_numeric(df[c], errors="coerce").fillna(0).round(0).astype("Int64")
    for c in ["HEIGHT","WEIGHT","WIDTH"]:
        if c in df.columns: df[c]=pd.to_numeric(df[c], errors="coerce")
    for b in ["WH_SHIP_CFG","WH_IS_DEFAULT"]:
        if b in df.columns: df[b]=df[b].astype("boolean")
    return df.reindex(columns=WAREHOUSE_TARGET_COLS)

# =====（可关的）COPY 错误打印 =====
def _print_copy_errors(conn, table_fqn: str, limit: int = 50, verbose: bool = False):
    if not verbose:
        return
    with conn.cursor() as cur:
        job_id = cur.execute("SELECT LAST_QUERY_ID()").fetchone()[0]
        try:
            rows = cur.execute(
                f"SELECT * FROM TABLE(VALIDATE({table_fqn}, JOB_ID => '{job_id}')) "
                f"WHERE ERROR IS NOT NULL LIMIT {limit}"
            ).fetchall()
            if rows:
                print(f"⚠️ COPY errors for {table_fqn} (JOB_ID={job_id}):")
                for r in rows:
                    print(r)
        except snowflake.connector.errors.ProgrammingError:
            pass

def _write_df(conn, df: pd.DataFrame, table: str, verbose: bool = False) -> int:
    ok, nchunks, nrows, _ = write_pandas(
        conn, df, table_name=table, database=SF_DATABASE, schema=SF_SCHEMA,
        quote_identifiers=False, use_logical_type=True,
        on_error="CONTINUE", auto_create_table=False, parallel=4,
    )
    _print_copy_errors(conn, f"{SF_SCHEMA}.{table}", verbose=verbose)
    return int(nrows)

# ===== 快照覆盖 =====
def truncate_snapshots(conn):
    with conn.cursor() as cur:
        cur.execute(f"USE DATABASE {SF_DATABASE}")
        cur.execute(f"USE SCHEMA {SF_SCHEMA}")
        cur.execute(f"TRUNCATE TABLE {TABLE_PRODUCTS_SNAP}")
        cur.execute(f"TRUNCATE TABLE {TABLE_WAREHOUSES_SNAP}")

# ===== 主流程（只打印总耗时）=====
def main():
    start_wall = datetime.now()
    t0 = time.perf_counter()

    conn = connect_snowflake()
    try:
        truncate_snapshots(conn)   # 覆盖今日 SNAP

        seen_pid=set(); seen_wh=set()
        prod_snap=wh_snap=0
        batch_idx = 0

        for batch in iter_products_batches(limit_each=PAGE_LIMIT):
            batch_idx += 1
            prod_rows=[]; wh_rows=[]
            for p in batch:
                pid=p.get("id")
                if pid is None:
                    continue
                if pid not in seen_pid:
                    row=base_product_cols(p)
                    if row is not None:
                        seen_pid.add(pid); prod_rows.append(row)
                for r in rows_for_warehouses(p):
                    key=(r.get("PRODUCT_ID"), r.get("WH_ID"), str(r.get("WH_UPDATED")))
                    if key not in seen_wh:
                        seen_wh.add(key); wh_rows.append(r)

            if prod_rows:
                df = clean_products_df(pd.DataFrame(prod_rows))
                prod_snap += _write_df(conn, df, TABLE_PRODUCTS_SNAP, verbose=False)
            if wh_rows:
                df = clean_wh_df(pd.DataFrame(wh_rows))
                wh_snap += _write_df(conn, df, TABLE_WAREHOUSES_SNAP, verbose=False)

            if ENABLE_HISTORY:
                pass

        total_elapsed = time.perf_counter() - t0
        print(f"\n✅ 任务完成，总耗时：{_fmt_dur(total_elapsed)}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
