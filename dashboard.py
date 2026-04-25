from __future__ import annotations

import datetime as dt
import difflib
import io
import os
import re
import sqlite3

import pandas as pd
import streamlit as st
import streamlit.components.v1 as st_components
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side

from ingest_xlsx import classify_ship_raw, infer_settlement_ship_series
from pricing import format_won, load_unit_prices
from recipient_identity import assign_recipient_ids
from sales_period_agg import summarize_sales_period


def _note_icons(note_text: str) -> str:
    """Derive quick icons from attention-note text."""
    t = (note_text or "").strip()
    if not t:
        return ""

    icons: list[str] = []
    # Cancellation / cancel request
    if any(k in t for k in ["취소", "취소요청", "취소 요청", "주문취소", "주문 취소"]):
        icons.append("⛔")
    # Product change (not necessarily color-only)
    if "제품변경" in t or "제품 변경" in t:
        icons.append("🔄")
    # Color change only (색상/컬러 변경)
    if any(k in t for k in ["색상변경", "색상 변경", "컬러변경", "컬러 변경", "색깔변경", "색깔 변경"]):
        icons.append("🔄")
    # Addition
    if any(k in t for k in ["추가", "추가됨", "추가했습니다"]):
        icons.append("➕")
    # Color designation (pin)
    # Common vendor phrasing: "컬러지정", "색상지정"
    if ("컬러지정" in t) or ("색상지정" in t) or ("색깔지정" in t):
        icons.append("📌")
    elif any(k in t for k in ["컬러", "색상", "색깔"]) and ("지정" in t) and ("변경" not in t):
        icons.append("📌")

    # Remove duplicates while preserving order
    out: list[str] = []
    for ic in icons:
        if ic not in out:
            out.append(ic)
    return " ".join(out) if out else ""


def _status_lead_icon(status: str) -> str:
    """Exactly one status emoji before the name (empty for 접수/기타)."""
    s = (status or "").strip()
    if s == "출고":
        return "🚚 "
    if s == "클레임":
        return "⚠️ "
    if s == "마감":
        return "🧾 "
    if s == "납품취소":
        return "⛔ "
    return ""


def _truncate_display_name(name: str, max_chars: int = 8) -> str:
    n = (name or "").strip()
    if len(n) <= max_chars:
        return n
    if max_chars < 2:
        return n[:max_chars]
    return n[: max_chars - 1] + "…"


def _attention_note_str(att_v: object) -> tuple[str, bool]:
    if att_v is None:
        return "", False
    try:
        if pd.isna(att_v):
            return "", False
    except Exception:
        pass
    s = str(att_v).strip()
    return s, bool(s)


def _fold_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _strip_order_list_overlap(text_v: object, order_list_v: object) -> str:
    """특이사항/수기 메모에서 DB `order_list`(품목 요약)와 동일한 줄·블록을 제거한다.

    엑셀 A열에 주문목록을 붙여 넣은 경우 피킹·출고_주문에서 특이사항과 품목이 이중으로 보이는 것을 막는다.
    """
    body, has = _attention_note_str(text_v)
    if not has:
        return ""
    ol = str(order_list_v or "").strip()
    if not ol:
        return body

    if ol in body:
        body = body.replace(ol, "").strip()
        if not body:
            return ""

    ol_lines = {_fold_ws(x) for x in ol.splitlines() if _fold_ws(x)}
    if not ol_lines:
        return body

    kept: list[str] = []
    for line in body.splitlines():
        if _fold_ws(line) in ol_lines:
            continue
        kept.append(line)
    body = "\n".join(kept).strip()

    # 한 줄에 요약 전체가 이어 붙은 경우
    ol2 = ol.strip()
    if len(ol2) > 40 and ol2 in body:
        body = body.replace(ol2, "").strip()

    return body


def _attention_note_export_for_order(att_v: object, order_list_v: object) -> str:
    cleaned = _strip_order_list_overlap(att_v, order_list_v)
    return _attention_note_with_icon_export(cleaned or None)


def _trailing_icon_segment(has_attention: bool, note_text: str, max_icons: int = 3) -> str:
    """Up to max_icons after the name: 🟥 first if present, then note-derived icons."""
    seq: list[str] = []
    if has_attention:
        seq.append("🟥")
    for tok in _note_icons(note_text).split():
        if tok:
            seq.append(tok)
    seen: set[str] = set()
    out: list[str] = []
    for t in seq:
        if t not in seen:
            seen.add(t)
            out.append(t)
        if len(out) >= max_icons:
            break
    return (" " + " ".join(out)) if out else ""


def _compact_name_display(status: str, receiver_name: str, attention_note_val: object) -> str:
    att, has_att = _attention_note_str(attention_note_val)
    lead = _status_lead_icon(status)
    nick = _truncate_display_name(receiver_name, 8)
    tail = _trailing_icon_segment(has_att, att, 3)
    return f"{lead}{nick}{tail}".strip()


def _attention_note_with_icon_export(att_val: object) -> str:
    """특이사항(자동) 본문 앞에 목록·엑셀과 동일한 아이콘 줄(🟥·🔄 등)을 붙인다."""
    att, has_att = _attention_note_str(att_val)
    icons = _trailing_icon_segment(has_att, att, 5).strip()
    body = att.strip()
    if icons and body:
        return f"{icons}\n\n{body}"
    if body:
        return body
    return icons


def _backup_sqlite(db_path: str, backup_dir: str = "backups", keep_days: int = 30) -> str | None:
    """Create a safe SQLite backup file and return its path.

    Uses sqlite3.Connection.backup() so it works even while the DB is in use.
    """
    try:
        os.makedirs(backup_dir, exist_ok=True)
    except Exception:
        return None

    if not db_path or not os.path.exists(db_path):
        return None

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    base = os.path.splitext(os.path.basename(db_path))[0] or "mutomo"
    out_path = os.path.join(backup_dir, f"{base}_{ts}.sqlite")

    try:
        src = sqlite3.connect(db_path)
        try:
            dst = sqlite3.connect(out_path)
            try:
                src.backup(dst)
            finally:
                dst.close()
        finally:
            src.close()
    except Exception:
        try:
            if os.path.exists(out_path):
                os.remove(out_path)
        except Exception:
            pass
        return None

    # Simple retention: keep last N days by filename timestamp (best-effort)
    try:
        cutoff = dt.datetime.now() - dt.timedelta(days=keep_days)
        for name in os.listdir(backup_dir):
            if not name.startswith(base + "_") or not name.endswith(".sqlite"):
                continue
            stamp = name[len(base) + 1 :].replace(".sqlite", "")
            try:
                d = dt.datetime.strptime(stamp, "%Y%m%d_%H%M%S")
            except Exception:
                continue
            if d < cutoff:
                try:
                    os.remove(os.path.join(backup_dir, name))
                except Exception:
                    pass
    except Exception:
        pass

    return out_path


@st.cache_data
def _price_map_cached(price_path: str, price_mtime: float) -> tuple[dict, tuple[str, ...]]:
    return load_unit_prices(price_path if os.path.isfile(price_path) else None)


@st.cache_data
def load_tables(db_path: str, _db_mtime: float, _db_size: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """`_db_mtime` / `_db_size`는 `mutomo.sqlite`가 바뀌면 같이 바뀌어 캐시를 무효화합니다."""
    con = sqlite3.connect(db_path)
    try:
        orders = pd.read_sql_query("select * from orders", con)
        items = pd.read_sql_query("select * from items", con)
    finally:
        con.close()
    return orders, items


def _db_stat_for_cache(db_path: str) -> tuple[float, int]:
    try:
        stt = os.stat(db_path)
        return (float(stt.st_mtime), int(stt.st_size))
    except OSError:
        return (0.0, 0)


def _db_ready(db_path: str) -> tuple[bool, str]:
    """Return (ok, reason) — dashboard needs orders + items tables from ingest."""
    path = (db_path or "").strip()
    if not path:
        return False, "DB 경로가 비어 있습니다. 사이드바 **설정**에서 경로를 지정하세요."
    if not os.path.exists(path):
        return False, f"DB 파일이 없습니다: `{path}`"
    con = sqlite3.connect(path)
    try:
        cur = con.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('orders', 'items')"
        )
        found = {row[0] for row in cur.fetchall()}
    finally:
        con.close()
    missing = [t for t in ("orders", "items") if t not in found]
    if missing:
        return (
            False,
            f"SQLite에 필요한 테이블이 없습니다: {', '.join(missing)}. "
            "엑셀 수집(`ingest_xlsx.py`)으로 DB를 만든 뒤 다시 실행하세요.",
        )
    return True, ""


def _migrate_orders_schema(db_path: str) -> None:
    """orders에 shipped_at·phone_norm·party_key 컬럼 보장 및 party 컬럼 백필."""
    con = sqlite3.connect(db_path)
    try:
        cur = con.cursor()
        cur.execute("PRAGMA table_info(orders)")
        cols = {row[1] for row in cur.fetchall()}
        if "shipped_at" not in cols:
            cur.execute("ALTER TABLE orders ADD COLUMN shipped_at TEXT")
        if "phone_norm" not in cols:
            cur.execute("ALTER TABLE orders ADD COLUMN phone_norm TEXT")
        if "party_key" not in cols:
            cur.execute("ALTER TABLE orders ADD COLUMN party_key TEXT")
        con.commit()

        cur.execute("SELECT COUNT(*) FROM orders")
        n_orders = int(cur.fetchone()[0] or 0)
        if n_orders:
            cur.execute(
                "SELECT COUNT(*) FROM orders WHERE party_key IS NULL OR TRIM(COALESCE(party_key,'')) = ''"
            )
            need_fill = int(cur.fetchone()[0] or 0) > 0
            if need_fill:
                df = pd.read_sql_query("SELECT * FROM orders", con)
                df = assign_recipient_ids(df)
                cur.executemany(
                    "UPDATE orders SET phone_norm = ?, party_key = ? WHERE order_id = ?",
                    list(
                        zip(
                            df["phone_norm"].astype(str).tolist(),
                            df["party_key"].astype(str).tolist(),
                            df["order_id"].astype(str).tolist(),
                            strict=False,
                        )
                    ),
                )
                con.commit()
    finally:
        con.close()


def _to_date_series(s: pd.Series) -> pd.Series:
    # created_at is ISO string; coerce anything else safely
    return pd.to_datetime(s, errors="coerce").dt.date


@st.cache_data
def _today_shipped_excel_cached(export_version: str, shipped_key: str, orders_all: pd.DataFrame, items_all: pd.DataFrame) -> bytes:
    # shipped_key forces cache bust when today's shipped set changes
    # shipped_key includes date selection
    shipped_date = dt.date.fromisoformat(shipped_key.split("|", 1)[0])
    return _build_shipped_excel_bytes(orders_all, items_all, shipped_date=shipped_date)


def _excel_line_pick_bucket(raw: object) -> str:
    """엑셀 품목 배송란 기준으로 피킹 시트(택배/직접) 분류. 미기재·판별불가는 택배 쪽에 둡니다."""
    if classify_ship_raw(raw) == "직접배송":
        return "직접배송"
    return "택배"


def _ship_display_label(canonical: str) -> str:
    """엑셀과 동일하게 표기: 직접배송 → 직접."""
    c = (canonical or "").strip()
    if c == "직접배송":
        return "직접"
    if c == "택배":
        return "택배"
    return c or "택배"


def _excel_pick_bucket_series(base: pd.DataFrame) -> pd.Series:
    """주문 단위 품목 행마다 택배/직접배송 버킷. 배송란 비어 있으면 같은 주문 위쪽 행 값(ffill)."""
    if "ship_raw" not in base.columns:
        return pd.Series(_excel_line_pick_bucket(None), index=base.index, dtype=object)
    work = base.copy()
    if "row_idx" in work.columns:
        work = work.sort_values("row_idx")

    def _is_blank(v: object) -> bool:
        if v is None:
            return True
        try:
            if pd.isna(v):
                return True
        except Exception:
            pass
        return isinstance(v, str) and not str(v).strip()

    sr = work["ship_raw"].astype(object)
    filled = sr.mask(sr.map(_is_blank), pd.NA).ffill()
    work["_pick_bucket"] = filled.map(_excel_line_pick_bucket)
    return work.loc[base.index, "_pick_bucket"]


def _excel_ship_for_picking_row(order_id: object, items_df: pd.DataFrame, pick_kind: str) -> str:
    """피킹 시트에 실린 품목 줄만으로 다수결(주문 전체 요약이 아님)."""
    oid = str(order_id or "").strip()
    if not oid or "order_id" not in items_df.columns or "ship_raw" not in items_df.columns:
        return _ship_display_label(pick_kind)
    base = items_df[items_df["order_id"].astype(str) == oid].copy()
    if len(base) == 0:
        return "택배"
    buckets = _excel_pick_bucket_series(base)
    sub = base.loc[buckets == pick_kind]
    if len(sub) == 0:
        return _ship_display_label(pick_kind)
    ser = infer_settlement_ship_series(sub)
    lab = str(ser.iloc[0]) if len(ser) else pick_kind
    return _ship_display_label(lab)


# ingest_xlsx.COLOR_WORDS / _shelf_color_fallback_from_leg_cell 와 동일 규칙 (표시용만; import 순환·버전 차이 방지)
_SHELF_FROM_NOTE_COLOR_KEYWORDS: tuple[str, ...] = (
    "크림화이트",
    "크림",
    "오프화이트",
    "화이트",
    "블랙",
    "그레이",
    "월넛",
    "오크",
    "베이지",
    "브라운",
    "내추럴",
    "자작",
    "올드블루진스",
    "런던브릭",
)


def _dashboard_shelf_color_from_note_raw(note_raw: object) -> str | None:
    """note_raw(엑셀 다리색 열 원본)에만 원단명이 있을 때 책장색 칸 표시 보정."""
    if note_raw is None:
        return None
    try:
        if pd.isna(note_raw):
            return None
    except Exception:
        pass
    t = str(note_raw).replace("\n", " ")
    if not t.strip():
        return None
    if re.search(r"다리", t):
        return None
    for c in _SHELF_FROM_NOTE_COLOR_KEYWORDS:
        if c in t:
            return c
    return None


def _items_view_from_item_rows(df: pd.DataFrame) -> pd.DataFrame:
    """이미 주문 1건 분의 품목 행만 담은 데이터프레임 → 표시용 뷰."""
    if df is None or len(df) == 0:
        return pd.DataFrame()

    df = df.copy()

    def _name(r: pd.Series) -> str:
        v = r.get("product_canonical")
        if pd.notna(v) and str(v).strip():
            return str(v).strip()
        v = r.get("product_raw")
        if pd.notna(v) and str(v).strip():
            return str(v).strip()
        return ""

    df["품목"] = df.apply(_name, axis=1)
    # DB에 shelf_color가 비었을 때 note_raw(엑셀 다리색 열 원본)에만 원단명이 있는 경우
    if "shelf_color" in df.columns and "note_raw" in df.columns:
        def _fill_shelf_from_note(r: pd.Series) -> object:
            s = r.get("shelf_color")
            if s is not None and not (isinstance(s, float) and pd.isna(s)) and str(s).strip():
                return s
            fb = _dashboard_shelf_color_from_note_raw(r.get("note_raw"))
            return fb if fb else s

        df["shelf_color"] = df.apply(_fill_shelf_from_note, axis=1)
    # Prefer spec_raw; fallback/append note_raw for options like
    # "(윈터선샤인 1EA, 런던브릭 1EA, ... / 57cm)" that we still want visible.
    if "spec_raw" in df.columns and "note_raw" in df.columns:
        spec = df["spec_raw"].astype(object)
        note = df["note_raw"].astype(object)

        spec_clean = spec.astype(str).fillna("").str.strip()
        note_clean = note.astype(str).fillna("").str.strip()

        # If spec is empty, use note.
        merged = spec.where(spec_clean != "", note)

        # If both exist, append note when it looks like a multi-option description.
        def _should_append(n: str) -> bool:
            n2 = (n or "").strip()
            if not n2:
                return False
            n2l = n2.lower()
            return ("ea" in n2l) or ("," in n2) or ("/" in n2) or ("cm" in n2l)

        out = []
        for s_val, n_val in zip(merged.tolist(), note_clean.tolist(), strict=False):
            s_txt = ("" if s_val is None else str(s_val)).strip()
            n_txt = (n_val or "").strip()
            if s_txt and n_txt and _should_append(n_txt) and n_txt not in s_txt:
                out.append(f"{s_txt} / {n_txt}")
            else:
                out.append(s_txt or n_txt or None)
        df["spec_raw"] = pd.Series(out, index=df.index, dtype=object)
    cols = []
    for c in ["품목", "spec_raw", "size", "shelf_color", "leg_color", "qty", "ship_raw"]:
        if c in df.columns:
            cols.append(c)
    view = df[cols].rename(
        columns={
            "spec_raw": "규격/옵션",
            "size": "사이즈",
            "shelf_color": "책장색상",
            "leg_color": "다리색상",
            "qty": "개수",
            "ship_raw": "배송",
        }
    )
    # Stable ordering
    if "row_idx" in df.columns:
        view = pd.concat([df[["row_idx"]], view], axis=1).sort_values("row_idx").drop(columns=["row_idx"])
    return view


def _items_view(items: pd.DataFrame, order_id: str) -> pd.DataFrame:
    base = items[items["order_id"].astype(str) == str(order_id)].copy() if "order_id" in items.columns else pd.DataFrame()
    return _items_view_from_item_rows(base)


def _items_view_excel_pick_kind(items: pd.DataFrame, order_id: str, pick_kind: str) -> pd.DataFrame:
    """엑셀 ship_raw 기준으로 해당 피킹(택배/직접)에 올릴 품목 줄만 보이게 필터."""
    base = items[items["order_id"].astype(str) == str(order_id)].copy() if "order_id" in items.columns else pd.DataFrame()
    if len(base) == 0:
        return base
    if "ship_raw" not in base.columns:
        return _items_view_from_item_rows(base) if pick_kind == "택배" else pd.DataFrame()
    b = _excel_pick_bucket_series(base)
    sub = base.loc[b == pick_kind].copy()
    return _items_view_from_item_rows(sub)


def _product_catalog_summary(items_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """정규명·미매핑 엑셀명별 (라인 수, 수량 합). `main`에서 쓰는 `items`와 동일 필터 범위."""
    empty_mapped = pd.DataFrame(columns=["정규상품명", "라인수", "수량합"])
    empty_raw = pd.DataFrame(columns=["엑셀상품명", "라인수", "수량합"])
    if items_df is None or len(items_df) == 0:
        return empty_mapped, empty_raw
    df = items_df.copy()
    if "qty" in df.columns:
        df["_qty_num"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0)
    else:
        df["_qty_num"] = 0.0

    if "product_canonical" in df.columns:
        c = df["product_canonical"]
        mask_mapped = c.notna() & c.astype(str).str.strip().ne("")
    else:
        mask_mapped = pd.Series(False, index=df.index)

    mapped = df.loc[mask_mapped]
    if len(mapped):
        g1 = (
            mapped.groupby("product_canonical", dropna=False)
            .agg(라인수=("order_id", "count"), 수량합=("_qty_num", "sum"))
            .reset_index()
            .rename(columns={"product_canonical": "정규상품명"})
            .sort_values(["라인수", "정규상품명"], ascending=[False, True])
        )
    else:
        g1 = empty_mapped.copy()

    mask_un = ~mask_mapped
    if "product_raw" in df.columns:
        pr = df["product_raw"]
        mask_raw = pr.notna() & pr.astype(str).str.strip().ne("")
    else:
        mask_raw = pd.Series(False, index=df.index)
    unmapped = df.loc[mask_un & mask_raw]
    if len(unmapped):
        g2 = (
            unmapped.groupby("product_raw", dropna=False)
            .agg(라인수=("order_id", "count"), 수량합=("_qty_num", "sum"))
            .reset_index()
            .rename(columns={"product_raw": "엑셀상품명"})
            .sort_values(["라인수", "엑셀상품명"], ascending=[False, True])
        )
    else:
        g2 = empty_raw.copy()

    return g1, g2


def _order_picking_sheet_hits(items: pd.DataFrame, order_id: object) -> tuple[bool, bool]:
    """(피킹리스트_택배에 실릴 품목 줄 있음, 피킹리스트_직접에 실릴 품목 줄 있음)."""
    oid = str(order_id or "").strip()
    if not oid:
        return False, False
    ta = _items_view_excel_pick_kind(items, oid, "택배")
    dr = _items_view_excel_pick_kind(items, oid, "직접배송")
    return len(ta) > 0, len(dr) > 0


def _picking_stats(shipped: pd.DataFrame, items_all: pd.DataFrame) -> tuple[int, int, int, int]:
    """(출고 주문 수, 피킹_택배 행 수, 피킹_직접 행 수, 혼합 배송 주문 수). 혼합은 두 피킹 시트에 각 1행."""
    n_u = len(shipped)
    if not n_u:
        return 0, 0, 0, 0
    n_t = n_d = n_m = 0
    for _, o in shipped.iterrows():
        ht, hd = _order_picking_sheet_hits(items_all, o.get("order_id"))
        if ht:
            n_t += 1
        if hd:
            n_d += 1
        if ht and hd:
            n_m += 1
    return n_u, n_t, n_d, n_m


def _items_text(items: pd.DataFrame, order_id: str) -> str:
    view = _items_view(items, order_id)
    if view is None or len(view) == 0:
        return ""

    def _s(v: object) -> str:
        if v is None:
            return ""
        try:
            if pd.isna(v):
                return ""
        except Exception:
            pass
        return str(v).strip().replace("\n", " ")

    lines: list[str] = []
    for _, r in view.iterrows():
        parts = []
        for col in ["품목", "규격/옵션", "사이즈", "책장색상", "다리색상", "개수", "배송"]:
            if col in view.columns:
                val = _s(r.get(col))
                if val:
                    parts.append(f"{col}:{val}")
        if parts:
            lines.append(" | ".join(parts))
    return "\n---\n".join(lines)


def _render_order_detail(container: st.delta_generator.DeltaGenerator, order_row: pd.Series, items: pd.DataFrame) -> None:
    purchase = str(order_row.get("purchase_date") or "").strip()
    name = str(order_row.get("receiver_name") or "").strip()
    phone = str(order_row.get("phone") or "").strip()
    address = str(order_row.get("address") or "").strip()
    req = str(order_row.get("delivery_request") or "").strip()
    att_for_detail = _attention_note_export_for_order(order_row.get("attention_note"), order_row.get("order_list"))
    status = str(order_row.get("status") or "").strip()
    oid = str(order_row.get("order_id") or "").strip()

    # Color-coded header box by status (high contrast)
    def _status_style(s: str) -> tuple[str, str, str, str]:
        # returns (label, bg, border, text)
        if s == "출고":
            return "🚚✅ 출고 완료", "#DFF5E1", "#1B5E20", "#1B5E20"
        if s == "클레임":
            return "⚠️ 클레임", "#FFE4CC", "#E65100", "#8A2E00"
        if s == "마감":
            return "🧾 마감", "#E0E0E0", "#37474F", "#263238"
        if s == "접수":
            return "📝⏳ 접수(납품예정)", "#D6ECFF", "#0D47A1", "#0D47A1"
        if s == "납품취소":
            return "⛔ 납품취소", "#FFEBEE", "#B71C1C", "#B71C1C"
        return (s or "상태 없음"), "#ECEFF1", "#607D8B", "#37474F"

    status_label, bg, border, text = _status_style(status)
    container.markdown(
        f"""
<div style="padding:10px 12px; border:2px solid {border}; background:{bg}; border-radius:10px; margin:6px 0 10px 0; box-shadow: 0 1px 4px rgba(0,0,0,0.12);">
  <div style="font-size:12px; font-weight:600; color:#263238; margin-bottom:4px;">상태</div>
  <div style="font-weight:800; color:{text}; font-size:16px;">{status_label}</div>
</div>
""",
        unsafe_allow_html=True,
    )

    header_lines = []
    if purchase:
        header_lines.append(f"- **구매일자**: {purchase}")
    if name:
        header_lines.append(f"- **받는분**: {name}")
    if phone:
        header_lines.append(f"- **전화**: {phone}")
    if address:
        header_lines.append(f"- **주소**: {address}")
    if req:
        header_lines.append(f"- **배송요청**: {req}")
    if att_for_detail:
        header_lines.append(f"- **특이사항(자동)**: {att_for_detail.replace(chr(10), '  \n')}")
    if oid:
        header_lines.append(f"- **ID**: `{oid}`")

    if header_lines:
        container.markdown("\n".join(header_lines))

    view = _items_view(items, oid)
    if view is None or len(view) == 0:
        txt = str(order_row.get("order_list") or "").strip()
        if txt:
            container.markdown("**품목**")
            container.markdown(txt.replace("\n", "  \n"))
        return

    container.markdown("**품목**")
    for i, (_, r) in enumerate(view.iterrows(), start=1):
        def _sv(col: str) -> str:
            if col not in view.columns:
                return ""
            v = r.get(col)
            if v is None:
                return ""
            try:
                if pd.isna(v):
                    return ""
            except Exception:
                pass
            return str(v).strip().replace("\n", " ")

        parts = {
            "품목": _sv("품목"),
            "규격/옵션": _sv("규격/옵션"),
            "사이즈": _sv("사이즈"),
            "책장색상": _sv("책장색상"),
            "다리색상": _sv("다리색상"),
            "개수": _sv("개수"),
            "배송": _sv("배송"),
        }
        title = parts["품목"] or "(품목명 없음)"
        container.markdown(f"**{i}. {title}**")

        # Compact line: 규격+사이즈+책장색상+다리색상+개수+배송 (skip empty)
        compact_parts = []
        if parts.get("규격/옵션"):
            compact_parts.append(parts["규격/옵션"])
        if parts.get("사이즈"):
            compact_parts.append(parts["사이즈"])
        if parts.get("책장색상"):
            compact_parts.append(f"책장:{parts['책장색상']}")
        if parts.get("다리색상"):
            compact_parts.append(f"다리:{parts['다리색상']}")
        if parts.get("개수"):
            compact_parts.append(f"개수:{parts['개수']}")
        if parts.get("배송"):
            compact_parts.append(f"배송:{parts['배송']}")
        if compact_parts:
            container.markdown(" / ".join(compact_parts))
        if i != len(view):
            container.markdown("---")


def _format_picking_worksheet(ws: object) -> None:
    """피킹리스트 시트 공통 인쇄 서식 (택배/직접 동일)."""
    ws.page_setup.orientation = "landscape"
    ws.page_setup.paperSize = ws.PAPERSIZE_A4
    ws.page_setup.fitToWidth = 1
    ws.page_setup.fitToHeight = 0
    ws.print_title_rows = "1:1"
    ws.page_margins.left = 0.25
    ws.page_margins.right = 0.25
    ws.page_margins.top = 0.35
    ws.page_margins.bottom = 0.35
    ws.page_margins.header = 0.2
    ws.page_margins.footer = 0.2
    ws.sheet_properties.pageSetUpPr.fitToPage = True

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
    header_fill = PatternFill("solid", fgColor="E3F2FD")
    header_font = Font(bold=True)
    thin_side = Side(style="thin", color="B0B0B0")
    header_side = Side(style="medium", color="607D8B")
    thin_border = Border(left=thin_side, right=thin_side, top=thin_side, bottom=thin_side)
    header_border = Border(left=header_side, right=header_side, top=header_side, bottom=header_side)
    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = header_border

    wrap_cols = {"주소", "배송요청", "특이사항", "품목"}
    header_map = {ws.cell(1, c).value: c for c in range(1, ws.max_column + 1)}
    for name, col_idx in header_map.items():
        if name == "No":
            ws.column_dimensions[ws.cell(1, col_idx).column_letter].width = 5
        elif name in ("받는분",):
            ws.column_dimensions[ws.cell(1, col_idx).column_letter].width = 12
        elif name == "배송(엑셀)":
            ws.column_dimensions[ws.cell(1, col_idx).column_letter].width = 11
        elif name in ("전화",):
            ws.column_dimensions[ws.cell(1, col_idx).column_letter].width = 14
        elif name == "주소":
            ws.column_dimensions[ws.cell(1, col_idx).column_letter].width = 40
        elif name == "배송요청":
            ws.column_dimensions[ws.cell(1, col_idx).column_letter].width = 24
        elif name == "특이사항":
            ws.column_dimensions[ws.cell(1, col_idx).column_letter].width = 44
        elif name == "품목":
            ws.column_dimensions[ws.cell(1, col_idx).column_letter].width = 52

    item_col_idx = header_map.get("품목")
    notes_col_idx = header_map.get("특이사항")
    item_col_width = 52
    notes_col_width = 44
    chars_per_line = max(10, int(item_col_width * 1.1))
    chars_notes = max(10, int(notes_col_width * 1.1))
    base_line_height = 15

    def _wrapped_line_count(text: str, cpl: int) -> int:
        raw_lines = text.splitlines() if text else [""]
        n = 0
        for ln in raw_lines:
            ln_len = len(ln)
            n += max(1, (ln_len + cpl - 1) // cpl)
        return n

    for r in range(2, ws.max_row + 1):
        ws.row_dimensions[r].height = 24
        for c in range(1, ws.max_column + 1):
            cell = ws.cell(r, c)
            header = ws.cell(1, c).value
            if header in wrap_cols:
                cell.alignment = Alignment(vertical="top", wrap_text=True)
            else:
                cell.alignment = Alignment(vertical="top")
            if r % 2 == 0:
                cell.fill = PatternFill("solid", fgColor="FAFAFA")
            cell.border = thin_border

        line_count = 0
        if item_col_idx:
            v = ws.cell(r, item_col_idx).value
            text = "" if v is None else str(v)
            line_count = max(line_count, _wrapped_line_count(text, chars_per_line))
        if notes_col_idx:
            v2 = ws.cell(r, notes_col_idx).value
            t2 = "" if v2 is None else str(v2)
            line_count = max(line_count, _wrapped_line_count(t2, chars_notes))
        if line_count:
            ws.row_dimensions[r].height = max(24, min(300, base_line_height * line_count + 12))


def _format_pick_summary_sheet(ws: object) -> None:
    """출고_피킹요약 시트 가독성."""
    header_fill = PatternFill("solid", fgColor="E3F2FD")
    header_font = Font(bold=True)
    thin_side = Side(style="thin", color="B0B0B0")
    thin_border = Border(left=thin_side, right=thin_side, top=thin_side, bottom=thin_side)
    header_side = Side(style="medium", color="607D8B")
    header_border = Border(left=header_side, right=header_side, top=header_side, bottom=header_side)
    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = header_border
    ws.column_dimensions["A"].width = 34
    ws.column_dimensions["B"].width = 72
    for r in range(2, ws.max_row + 1):
        for c in range(1, ws.max_column + 1):
            cell = ws.cell(r, c)
            cell.border = thin_border
            cell.alignment = Alignment(vertical="top", wrap_text=True)
        ws.row_dimensions[r].height = max(18, min(120, 14 * (1 + str(ws.cell(r, 2).value or "").count("\n"))))


def _picking_notes_excel_cell(o: pd.Series) -> str:
    """피킹 시트 '특이사항' 열: 아이콘 줄 + 엑셀 자동(attention_note) + 수기(special_issue)만.

    `order_list`는 같은 시트 **품목** 열에만 두고, `attention_note`/`special_issue` 안에 붙어 있는 동일 문구는 제거한다.
    """
    parts: list[str] = []
    ol = o.get("order_list")
    att_blk = _attention_note_export_for_order(o.get("attention_note"), ol)
    if att_blk:
        parts.append(att_blk)
    si = _strip_order_list_overlap(o.get("special_issue"), ol).strip()
    if si:
        parts.append(si)
    return "\n\n".join(parts) if parts else ""


def _build_shipped_excel_bytes(orders_all: pd.DataFrame, items_all: pd.DataFrame, shipped_date: dt.date) -> bytes:
    shipped = orders_all.copy()
    if "status" in shipped.columns:
        shipped = shipped[shipped["status"] == "출고"]
    # Filter to "today shipped" when shipped_at exists
    if "shipped_at" in shipped.columns:
        shipped["_shipped_date"] = pd.to_datetime(shipped["shipped_at"], errors="coerce").dt.date
        shipped = shipped[shipped["_shipped_date"] == shipped_date].drop(columns=["_shipped_date"], errors="ignore")
    shipped = shipped.reset_index(drop=True)

    # Picking list (print-friendly)
    def _item_line_from_view(view: pd.DataFrame) -> str:
        lines: list[str] = []

        def _sv(v: object) -> str:
            if v is None:
                return ""
            try:
                if pd.isna(v):
                    return ""
            except Exception:
                pass
            return str(v).strip().replace("\n", " ")

        for i, (_, r) in enumerate(view.iterrows(), start=1):
            name = _sv(r.get("품목")) or "(품목명 없음)"
            parts = []
            spec = _sv(r.get("규격/옵션"))
            size = _sv(r.get("사이즈"))
            shelf = _sv(r.get("책장색상"))
            leg = _sv(r.get("다리색상"))
            qty = _sv(r.get("개수"))
            if spec:
                parts.append(spec)
            if size:
                parts.append(size)
            if shelf:
                parts.append(f"책장:{shelf}")
            if leg:
                parts.append(f"다리:{leg}")
            if qty:
                parts.append(f"개수:{qty}")
            ship = _sv(r.get("배송"))
            if ship:
                parts.append(f"배송:{ship}")
            line = f"{i}. {name}"
            if parts:
                line += " — " + " / ".join(parts)
            lines.append(line)
        return "\n".join(lines)

    def _picking_rows_for_kind(pick_kind: str) -> pd.DataFrame:
        rows: list[dict[str, object]] = []
        n = 0
        for _, o in shipped.iterrows():
            oid = o.get("order_id")
            view = _items_view_excel_pick_kind(items_all, str(oid), pick_kind) if oid is not None else pd.DataFrame()
            if len(view) == 0:
                continue
            n += 1
            rows.append(
                {
                    "No": n,
                    "받는분": o.get("receiver_name"),
                    "배송(엑셀)": _excel_ship_for_picking_row(oid, items_all, pick_kind),
                    "전화": o.get("phone"),
                    "주소": o.get("address"),
                    "배송요청": o.get("delivery_request"),
                    "특이사항": _picking_notes_excel_cell(o),
                    "품목": _item_line_from_view(view),
                }
            )
        cols = ["No", "받는분", "배송(엑셀)", "전화", "주소", "배송요청", "특이사항", "품목"]
        return pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)

    picking_taek = _picking_rows_for_kind("택배")
    picking_direct = _picking_rows_for_kind("직접배송")

    n_u, n_t_rows, n_d_rows, n_m = _picking_stats(shipped, items_all)
    mixed_labels: list[str] = []
    for _, o in shipped.iterrows():
        ht, hd = _order_picking_sheet_hits(items_all, o.get("order_id"))
        if ht and hd:
            rn = str(o.get("receiver_name") or "").strip()
            mixed_labels.append(rn or str(o.get("order_id") or ""))
    mixed_text = ", ".join(mixed_labels) if mixed_labels else "—"

    summary_df = pd.DataFrame(
        [
            {"항목": "출고 기준일", "값": shipped_date.isoformat()},
            {"항목": "출고 주문 수(주문그룹)", "값": n_u},
            {"항목": "피킹리스트_택배 행 수", "값": n_t_rows},
            {"항목": "피킹리스트_직접 행 수", "값": n_d_rows},
            {"항목": "혼합 배송 주문 수", "값": n_m},
            {
                "항목": "검산",
                "값": f"택배 {n_t_rows}행 + 직접 {n_d_rows}행 = 출고 {n_u}건 + 혼합 {n_m}건",
            },
            {"항목": "혼합 주문(받는분)", "값": mixed_text},
            {
                "항목": "안내",
                "값": "품목 배송란에 직접·택배가 함께 있는 주문은 두 피킹 시트에 각 한 줄씩 들어갑니다. 행 수 합은 출고 건수와 다를 수 있습니다.",
            },
        ]
    )

    # 로젠택배 서식: 택배로 나가는 주문만 (직접배송 전용 주문 제외)
    def _lozen_item_name_taek(o: pd.Series) -> str:
        oid = o.get("order_id")
        view = _items_view_excel_pick_kind(items_all, str(oid), "택배") if oid is not None else pd.DataFrame()
        if len(view) and "품목" in view.columns:
            first = str(view.iloc[0]["품목"]).strip()
            if first:
                return first
        ol = str(o.get("order_list") or "").strip().replace("\n", " / ")
        return (ol[:50] + "…") if len(ol) > 51 else ol

    lozen_rows: list[dict[str, object]] = []
    for _, o in shipped.iterrows():
        oid = o.get("order_id")
        ht, _hd = _order_picking_sheet_hits(items_all, oid)
        if not ht:
            continue
        lozen_rows.append(
            {
                "수하인명": o.get("receiver_name"),
                "수하인주소": o.get("address"),
                "수하인전화번호": o.get("phone"),
                "수하인휴대폰번호": o.get("phone"),
                "박스수량": 1,
                "택배운임": 3000,
                "운임구분": "선불",
                "품목명": _lozen_item_name_taek(o),
                "배송메세지": o.get("delivery_request"),
            }
        )
    lozen_cols = [
        "수하인명",
        "수하인주소",
        "수하인전화번호",
        "수하인휴대폰번호",
        "박스수량",
        "택배운임",
        "운임구분",
        "품목명",
        "배송메세지",
    ]
    lozen_sheet = pd.DataFrame(lozen_rows, columns=lozen_cols) if lozen_rows else pd.DataFrame(columns=lozen_cols)

    if "order_id" in shipped.columns and len(items_all) and "order_id" in items_all.columns:
        _sm = infer_settlement_ship_series(items_all)
        shipped = shipped.copy()
        shipped["배송(엑셀)"] = shipped["order_id"].astype(str).map(_sm).fillna("택배")
        shipped["배송(엑셀)"] = shipped["배송(엑셀)"].map(_ship_display_label)

    # Order sheet (one row per order)
    order_cols = [
        "purchase_date",
        "receiver_name",
        "phone",
        "address",
        "delivery_request",
        "attention_note",
        "order_list",
        "special_issue",
        "배송(엑셀)",
        "status",
        "shipped_at",
        "order_id",
        "source_file",
        "group_no",
        "group_start_row",
        "deadline_raw",
        "order_date_raw",
        "created_at",
    ]
    order_sheet = shipped[[c for c in order_cols if c in shipped.columns]].copy()
    if "attention_note" in order_sheet.columns:
        if "order_list" in order_sheet.columns:

            def _order_sheet_att_row(r: pd.Series) -> str:
                return _attention_note_export_for_order(r.get("attention_note"), r.get("order_list"))

            order_sheet["attention_note"] = order_sheet.apply(_order_sheet_att_row, axis=1)
        else:
            order_sheet["attention_note"] = order_sheet["attention_note"].map(_attention_note_with_icon_export)

    if "special_issue" in order_sheet.columns and "order_list" in order_sheet.columns:
        order_sheet["special_issue"] = order_sheet.apply(
            lambda r: _strip_order_list_overlap(r.get("special_issue"), r.get("order_list")).strip(),
            axis=1,
        )

    # Items sheet: 준비할 품목/수량만 (품목별 합산)
    if len(shipped) and "order_id" in shipped.columns and "order_id" in items_all.columns:
        item_sheet = items_all[items_all["order_id"].isin(shipped["order_id"])].copy()
    else:
        item_sheet = items_all.head(0).copy()

    def _sv(v: object) -> str:
        if v is None:
            return ""
        try:
            if pd.isna(v):
                return ""
        except Exception:
            pass
        return str(v).strip().replace("\n", " ")

    if len(item_sheet):
        def _item_key(r: pd.Series) -> str:
            name = _sv(r.get("product_canonical")) or _sv(r.get("product_raw")) or "(품목명 없음)"
            parts = []
            spec = _sv(r.get("spec_raw"))
            note = _sv(r.get("note_raw"))
            size = _sv(r.get("size"))
            shelf = _sv(r.get("shelf_color"))
            leg = _sv(r.get("leg_color"))
            if spec:
                parts.append(spec)
            if note and note not in spec:
                parts.append(note)
            if size:
                parts.append(size)
            if shelf:
                parts.append(f"책장:{shelf}")
            if leg:
                parts.append(f"다리:{leg}")
            return f"{name} — " + " / ".join([p for p in parts if p]) if parts else name

        item_sheet["_품목"] = item_sheet.apply(_item_key, axis=1)
        item_sheet["_수량"] = pd.to_numeric(item_sheet.get("qty"), errors="coerce").fillna(0).astype(int)

        def _ship_join(s: pd.Series) -> str:
            vals = sorted({str(x).strip() for x in s.dropna().tolist() if str(x).strip()})
            return " / ".join(vals) if vals else ""

        item_sheet = (
            item_sheet.groupby("_품목", as_index=False)
            .agg(_수량=("_수량", "sum"), 엑셀배송=("ship_raw", _ship_join))
            .rename(columns={"_품목": "품목", "_수량": "수량"})
            .sort_values(["품목"])
            .reset_index(drop=True)
        )
    else:
        item_sheet = pd.DataFrame(columns=["품목", "수량", "엑셀배송"])

    # Friendly column names
    order_sheet = order_sheet.rename(
        columns={
            "purchase_date": "구매일자",
            "receiver_name": "받는분",
            "phone": "전화",
            "address": "주소",
            "delivery_request": "배송요청",
            "attention_note": "특이사항(엑셀자동)",
            "order_list": "주문목록",
            "special_issue": "특이사항",
            "status": "상태",
            "shipped_at": "출고처리시각",
            "order_id": "주문ID",
            "source_file": "원본파일",
            "group_no": "번호",
            "group_start_row": "그룹시작행",
            "deadline_raw": "납기",
            "order_date_raw": "엑셀주문일자",
            "created_at": "수집시각",
        }
    )
    # item_sheet columns are already Korean: 품목, 수량

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        summary_df.to_excel(writer, index=False, sheet_name="출고_피킹요약")
        picking_taek.to_excel(writer, index=False, sheet_name="피킹리스트_택배")
        picking_direct.to_excel(writer, index=False, sheet_name="피킹리스트_직접")
        lozen_sheet.to_excel(writer, index=False, sheet_name="로젠택배")
        order_sheet.to_excel(writer, index=False, sheet_name="출고_주문")
        item_sheet.to_excel(writer, index=False, sheet_name="출고_품목")

        header_fill = PatternFill("solid", fgColor="E3F2FD")
        header_font = Font(bold=True)
        thin_side = Side(style="thin", color="B0B0B0")
        thin_border = Border(left=thin_side, right=thin_side, top=thin_side, bottom=thin_side)
        header_side = Side(style="medium", color="607D8B")
        header_border = Border(left=header_side, right=header_side, top=header_side, bottom=header_side)

        _format_pick_summary_sheet(writer.book["출고_피킹요약"])
        _format_picking_worksheet(writer.book["피킹리스트_택배"])
        _format_picking_worksheet(writer.book["피킹리스트_직접"])

        # Formatting for 로젠택배 sheet (A4 landscape + grid)
        ws_l = writer.book["로젠택배"]
        ws_l.page_setup.orientation = "landscape"
        ws_l.page_setup.paperSize = ws_l.PAPERSIZE_A4
        ws_l.page_setup.fitToWidth = 1
        ws_l.page_setup.fitToHeight = 0
        ws_l.print_title_rows = "1:1"
        ws_l.page_margins.left = 0.25
        ws_l.page_margins.right = 0.25
        ws_l.page_margins.top = 0.35
        ws_l.page_margins.bottom = 0.35
        ws_l.sheet_properties.pageSetUpPr.fitToPage = True
        ws_l.freeze_panes = "A2"
        ws_l.auto_filter.ref = ws_l.dimensions
        for cell in ws_l[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            cell.border = header_border

        # Column widths close to Lozen template
        widths = {
            "수하인명": 12,
            "수하인주소": 48,
            "수하인전화번호": 14,
            "수하인휴대폰번호": 14,
            "박스수량": 8,
            "택배운임": 10,
            "운임구분": 8,
            "품목명": 20,
            "배송메세지": 40,
        }
        header_map_l = {ws_l.cell(1, c).value: c for c in range(1, ws_l.max_column + 1)}
        for h, col_idx in header_map_l.items():
            letter = ws_l.cell(1, col_idx).column_letter
            if h in widths:
                ws_l.column_dimensions[letter].width = widths[h]

        wrap_l = {"수하인주소", "배송메세지"}
        for r in range(2, ws_l.max_row + 1):
            ws_l.row_dimensions[r].height = 36
            for c in range(1, ws_l.max_column + 1):
                cell = ws_l.cell(r, c)
                header = ws_l.cell(1, c).value
                if header in wrap_l:
                    cell.alignment = Alignment(vertical="top", wrap_text=True)
                else:
                    cell.alignment = Alignment(vertical="top")
                cell.border = thin_border
                if r % 2 == 0:
                    cell.fill = PatternFill("solid", fgColor="FAFAFA")

        # Formatting for item sheet (prepare list)
        ws2 = writer.book["출고_품목"]
        # Print setup: A4 landscape, fit to page width
        ws2.page_setup.orientation = "landscape"
        ws2.page_setup.paperSize = ws2.PAPERSIZE_A4
        ws2.page_setup.fitToWidth = 1
        ws2.page_setup.fitToHeight = 0
        ws2.print_title_rows = "1:1"
        ws2.page_margins.left = 0.25
        ws2.page_margins.right = 0.25
        ws2.page_margins.top = 0.35
        ws2.page_margins.bottom = 0.35
        ws2.page_margins.header = 0.2
        ws2.page_margins.footer = 0.2
        ws2.sheet_properties.pageSetUpPr.fitToPage = True

        ws2.freeze_panes = "A2"
        ws2.auto_filter.ref = ws2.dimensions
        for cell in ws2[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            cell.border = header_border

        # Column widths + wrap for long text
        wrap_cols2 = {"품목", "엑셀배송"}
        header_map2 = {ws2.cell(1, c).value: c for c in range(1, ws2.max_column + 1)}
        for name, col_idx in header_map2.items():
            letter = ws2.cell(1, col_idx).column_letter
            if name == "품목":
                ws2.column_dimensions[letter].width = 70
            elif name == "수량":
                ws2.column_dimensions[letter].width = 8
            elif name == "엑셀배송":
                ws2.column_dimensions[letter].width = 18

        for r in range(2, ws2.max_row + 1):
            ws2.row_dimensions[r].height = 28
            for c in range(1, ws2.max_column + 1):
                cell = ws2.cell(r, c)
                header = ws2.cell(1, c).value
                if header in wrap_cols2:
                    cell.alignment = Alignment(vertical="top", wrap_text=True)
                else:
                    cell.alignment = Alignment(vertical="top")
                if r % 2 == 0:
                    cell.fill = PatternFill("solid", fgColor="FAFAFA")
                cell.border = thin_border
    return buf.getvalue()


def _build_lozen_xlsx_bytes_for_orders(
    orders_all: pd.DataFrame,
    items_all: pd.DataFrame,
    order_ids: list[str],
) -> bytes:
    """선택된 주문들로 '로젠택배' 업로드용 단일 시트 엑셀을 만든다."""
    cols = [
        "수하인명",
        "수하인주소",
        "수하인전화번호",
        "수하인휴대폰번호",
        "박스수량",
        "택배운임",
        "운임구분",
        "품목명",
        "배송메세지",
    ]

    if orders_all is None or len(orders_all) == 0 or not order_ids:
        buf0 = io.BytesIO()
        with pd.ExcelWriter(buf0, engine="openpyxl") as w:
            pd.DataFrame(columns=cols).to_excel(w, index=False, sheet_name="로젠택배")
        return buf0.getvalue()

    sub = orders_all[orders_all["order_id"].astype(str).isin([str(x) for x in order_ids])].copy().reset_index(drop=True)

    # 택배만 포함 (엑셀 배송란 추정)
    if items_all is not None and len(items_all) and "order_id" in items_all.columns:
        ship_series = infer_settlement_ship_series(items_all)
        ship_kind = sub["order_id"].astype(str).map(ship_series).fillna("택배")
        sub = sub[ship_kind.astype(str).isin(["택배"])].copy()

    def _item_name(o: pd.Series) -> str:
        ol = str(o.get("order_list") or "").strip().replace("\n", " / ")
        if not ol:
            return ""
        return (ol[:50] + "…") if len(ol) > 51 else ol

    lozen = pd.DataFrame(
        [
            {
                "수하인명": o.get("receiver_name"),
                "수하인주소": o.get("address"),
                "수하인전화번호": o.get("phone"),
                "수하인휴대폰번호": o.get("phone"),
                "박스수량": 1,
                "택배운임": 3000,
                "운임구분": "선불",
                "품목명": _item_name(o),
                "배송메세지": o.get("delivery_request"),
            }
            for _, o in sub.iterrows()
        ],
        columns=cols,
    )

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        lozen.to_excel(w, index=False, sheet_name="로젠택배")
    return buf.getvalue()


def update_special_issue_for_orders(db_path: str, order_ids: list[str], text: str) -> None:
    if not order_ids:
        return
    con = sqlite3.connect(db_path)
    try:
        cur = con.cursor()
        cur.executemany(
            "UPDATE orders SET special_issue=? WHERE order_id=?",
            [(text, oid) for oid in order_ids],
        )
        con.commit()
    finally:
        con.close()


def init_shared_session_state() -> None:
    if "active_selector" not in st.session_state:
        st.session_state["active_selector"] = None  # "search" | "table"
    if "search_pick_ids" not in st.session_state:
        st.session_state["search_pick_ids"] = []
    if "selected_ids_from_table" not in st.session_state:
        st.session_state["selected_ids_from_table"] = []
    if "prev_search_pick_ids" not in st.session_state:
        st.session_state["prev_search_pick_ids"] = []
    if "prev_table_pick_ids" not in st.session_state:
        st.session_state["prev_table_pick_ids"] = []
    if "request_clear_search_pick_labels" not in st.session_state:
        st.session_state["request_clear_search_pick_labels"] = False
    if "table_sync_from_search" not in st.session_state:
        st.session_state["table_sync_from_search"] = False


# 이름 검색 → 전체 접수 표 선택 연동 시 한 번에 넘길 최대 행 수 (브라우저·Streamlit 부담 완화)
_MAX_SEARCH_TO_TABLE_ROWS = 2000


def _unique_receiver_names(orders: pd.DataFrame) -> list[str]:
    """비어 있지 않은 받는분 이름을 대략 등장 순으로 유일하게 모은다."""
    if orders.empty or "receiver_name" not in orders.columns:
        return []
    s = orders["receiver_name"].astype(str).str.strip()
    s = s[s != ""]
    seen: set[str] = set()
    out: list[str] = []
    for x in s:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _focus_streamlit_input_by_placeholder(placeholder: str) -> None:
    """Streamlit에 네이티브 포커스 API가 없어, '다음 이름' 등 placeholder로 입력칸을 찾아 포커스."""
    import html

    ph = html.escape(placeholder, quote=True)
    st_components.html(
        f"""<script>
(function () {{
  const root = window.parent.document;
  const el = root.querySelector('input[placeholder="{ph}"]');
  if (el) {{
    el.focus();
    el.scrollIntoView({{ block: "nearest", behavior: "instant" }});
  }}
}})();
</script>""",
        height=1,
        width=1,
    )


def _fuzzy_receiver_name_suggestions(part: str, candidates: list[str], *, limit: int = 10) -> list[str]:
    """부분 문자열 검색이 0건일 때 오타 완화용 제안(한글·영문 모두 difflib 기준)."""
    q = part.strip()
    if len(q) < 2 or not candidates:
        return []
    # 1차·2차: difflib 근접 매칭 (한글 오타는 cutoff를 한 단계 낮춰도 시도)
    hits = difflib.get_close_matches(q, candidates, n=limit, cutoff=0.48)
    if not hits:
        hits = difflib.get_close_matches(q, candidates, n=limit, cutoff=0.40)
    if hits:
        return hits
    # 2차: 유일 이름 수가 적을 때만 비율 상위 (전수는 수만 건에서 부담)
    cap = 2500
    if len(candidates) > cap:
        return []
    scored: list[tuple[float, str]] = []
    for c in candidates:
        r = difflib.SequenceMatcher(a=q, b=c, autojunk=False).ratio()
        if r >= 0.38:
            scored.append((r, c))
    scored.sort(key=lambda t: (-t[0], t[1]))
    return [c for _r, c in scored[:limit]]


def _receiver_name_match_count(orders: pd.DataFrame, term: str) -> int:
    """받는분 이름에 term(부분일치)이 포함된 행 수."""
    t = (term or "").strip()
    if not t or orders.empty or "receiver_name" not in orders.columns:
        return 0
    ns = orders["receiver_name"].astype(str)
    return int(ns.str.contains(t, case=False, na=False, regex=False).sum())


def _receiver_name_or_match_count(orders: pd.DataFrame, parts_list: list[str]) -> int:
    """parts_list 전체를 OR(부분일치)로 합친 접수 건수."""
    if not parts_list or orders.empty or "receiver_name" not in orders.columns:
        return 0
    nm = orders["receiver_name"].astype(str)
    mask = nm.str.contains(parts_list[0], case=False, na=False, regex=False)
    for t in parts_list[1:]:
        mask = mask | nm.str.contains(t, case=False, na=False, regex=False)
    return int(mask.sum())


def _identity_hint_lines(hits: pd.DataFrame) -> list[str]:
    """검색 결과에 동명이인·재주문 패턴이 있으면 안내 문장 목록."""
    lines: list[str] = []
    if hits.empty or "receiver_name" not in hits.columns:
        return lines
    hn = hits.copy()
    hn["_rn"] = hn["receiver_name"].astype(str).str.strip()
    hn = hn[hn["_rn"] != ""]
    if hn.empty:
        return lines

    if "party_key" in hn.columns:
        pk = hn["party_key"].fillna("").astype(str)
        for name, grp in hn.groupby("_rn", sort=False):
            if len(grp) < 2:
                continue
            u = {x for x in pk.loc[grp.index].tolist() if x and x != "nan"}
            if len(u) >= 2:
                lines.append(
                    f"「{name}」은(는) **동명이인**일 가능성이 큽니다. (연락처·주소 기준으로 **{len(u)}무리**로 갈립니다.)"
                )
        seen_pk: set[str] = set()
        for pkey, grp in hn.groupby(pk, sort=False):
            ps = str(pkey).strip()
            if len(grp) < 2 or not ps or ps.startswith("order:"):
                continue
            if ps in seen_pk:
                continue
            seen_pk.add(ps)
            nm = str(grp.iloc[0].get("receiver_name") or "").strip() or "동일 연락처"
            lines.append(
                f"「{nm}」은(는) **재주문**으로 보입니다. (**동일 연락처** 접수가 **{len(grp)}건** 묶여 있습니다.)"
            )
    else:
        vc = hn["_rn"].value_counts()
        for name, c in vc.items():
            if int(c) >= 2:
                lines.append(
                    f"「{name}」이(가) **{int(c)}건** 있습니다. **동명이인**·**재주문**·여러 접수일 수 있으니 전화·날짜로 구분해 주세요."
                )

    out: list[str] = []
    for x in lines:
        if x not in out:
            out.append(x)
    return out[:12]


def _identity_hint_tags(hits: pd.DataFrame) -> list[str]:
    """검색 결과(한 검색어 범위)에 동명이인·재주문 패턴이 있으면 짧은 태그 목록."""
    tags: list[str] = []
    if hits.empty or "receiver_name" not in hits.columns:
        return tags
    hn = hits.copy()
    hn["_rn"] = hn["receiver_name"].astype(str).str.strip()
    hn = hn[hn["_rn"] != ""]
    if hn.empty:
        return tags

    if "party_key" in hn.columns:
        pk = hn["party_key"].fillna("").astype(str)
        for _name, grp in hn.groupby("_rn", sort=False):
            if len(grp) < 2:
                continue
            u = {x for x in pk.loc[grp.index].tolist() if x and x != "nan"}
            if len(u) >= 2 and "동명이인" not in tags:
                tags.append("동명이인")
        seen_pk: set[str] = set()
        for pkey, grp in hn.groupby(pk, sort=False):
            ps = str(pkey).strip()
            if len(grp) < 2 or not ps or ps.startswith("order:"):
                continue
            if ps in seen_pk:
                continue
            seen_pk.add(ps)
            if "재주문" not in tags:
                tags.append("재주문")
    else:
        vc = hn["_rn"].value_counts()
        if (vc >= 2).any() and "동명이인" not in tags:
            tags.append("동명이인")

    return tags


def _clear_row_pick_label_keys() -> None:
    for k in list(st.session_state.keys()):
        if isinstance(k, str) and k.startswith("row_pick_labels_"):
            st.session_state.pop(k, None)


def _clear_name_search_row_input_keys() -> None:
    for k in list(st.session_state.keys()):
        if isinstance(k, str) and k.startswith("name_search_row_input_"):
            st.session_state.pop(k, None)


def _prune_name_search_row_input_keys(n_terms: int) -> None:
    for k in list(st.session_state.keys()):
        if not isinstance(k, str) or not k.startswith("name_search_row_input_"):
            continue
        suf = k.removeprefix("name_search_row_input_")
        try:
            j = int(suf)
        except ValueError:
            st.session_state.pop(k, None)
            continue
        if j >= n_terms:
            st.session_state.pop(k, None)


def _build_order_picker_lists(rows: pd.DataFrame) -> tuple[dict[str, str], list[str], list[str]]:
    """행별 multiselect용 (라벨→order_id, 옵션, 기본 선택). rows는 정렬된 검색 결과."""
    if rows.empty or "order_id" not in rows.columns:
        return {}, [], []
    r = rows.copy()
    rkey = r["receiver_name"].astype(str).str.strip() if "receiver_name" in r.columns else pd.Series([""] * len(r))
    _vc = rkey.value_counts()
    r["_homonym_hit"] = rkey.map(lambda x: int(_vc.get(x, 0) > 1))

    def _digits_tail(phone_val: object, n: int = 4) -> str:
        s = "".join(ch for ch in str(phone_val or "") if ch.isdigit())
        return s[-n:] if len(s) >= n else ""

    def _label(row: pd.Series) -> str:
        purchase = str(row.get("purchase_date") or "").strip()
        name = str(row.get("receiver_name") or "").strip()
        status = str(row.get("status") or "").strip()
        att_v = _strip_order_list_overlap(row.get("attention_note"), row.get("order_list"))
        name_seg = _compact_name_display(status, name, att_v or None)
        order_list = str(row.get("order_list") or "").strip().replace("\n", " / ")
        order_list = (order_list[:120] + "…") if len(order_list) > 121 else order_list
        base = f"{purchase} | {name_seg} | {order_list}".strip(" |")
        if int(row.get("_homonym_hit") or 0):
            tail = _digits_tail(row.get("phone"), 4)
            if tail:
                base = f"{base} ·전화끝{tail}".strip()
            else:
                oid = str(row.get("order_id") or "").strip()
                if oid:
                    base = f"{base} ·ID {oid[:10]}".strip()
        return base

    r["_label"] = r.apply(_label, axis=1)
    dup = r["_label"].duplicated(keep=False)
    if dup.any():
        r.loc[dup, "_label"] = r.loc[dup].apply(lambda row: f"{row['_label']} ({row['order_id']})", axis=1)

    label_to_id = dict(zip(r["_label"].tolist(), r["order_id"].tolist(), strict=False))
    options = r["_label"].tolist()
    default_lbl: list[str] = []
    if "status" in r.columns:
        for lbl in options:
            oid = str(label_to_id.get(lbl, ""))
            hit = r[r["order_id"].astype(str) == oid]
            if len(hit) and str(hit.iloc[0].get("status") or "").strip() != "출고":
                default_lbl = [lbl]
                break
    if not default_lbl and options:
        default_lbl = [options[0]]
    return label_to_id, options, default_lbl


def render_receiver_name_search(
    orders: pd.DataFrame,
    items: pd.DataFrame,
    db_path: str,
    *,
    orders_name_hints: pd.DataFrame | None = None,
) -> None:
    """이름(좁은 칸) + 옆 multiselect(라벨 숨김, 줄 맞춤). 다음 줄은 폼에서 Enter로 추가. OR 합쳐 출고."""
    _ = items  # 상세는 메인에서 제거; 사이드바/표에서 확인
    nh = orders_name_hints if orders_name_hints is not None else orders
    st.subheader("검색")
    if "name_search_terms" not in st.session_state:
        st.session_state["name_search_terms"] = []
    # text_input(key=...) 생성 이후에는 같은 런에서 해당 키를 쓸 수 없음 → 이전 런에서 요청한 초기화만 여기서 처리
    if st.session_state.pop("_reset_name_search_draft", False):
        st.session_state.pop("name_search_draft", None)
        st.session_state.pop("name_search_draft_next", None)
        st.session_state.pop("name_search_inline_next_draft", None)
        for _k in list(st.session_state.keys()):
            if isinstance(_k, str) and _k.startswith("name_inline_draft_"):
                st.session_state.pop(_k, None)
        _clear_name_search_row_input_keys()

    terms: list[str] = st.session_state["name_search_terms"]
    parts = [str(p).strip() for p in terms if str(p).strip()]

    nm_series = orders.get("receiver_name", "").astype(str)
    if parts:
        mask = nm_series.str.contains(parts[0], case=False, na=False, regex=False)
        for q in parts[1:]:
            mask = mask | nm_series.str.contains(q, case=False, na=False, regex=False)
        hits = orders[mask].copy()
    else:
        hits = orders.iloc[:0].copy()

    if not parts:
        with st.form("name_search_first_row", clear_on_submit=True):
            c1, c2 = st.columns([1, 7], gap="small")
            with c1:
                st.text_input(
                    "이름",
                    key="name_search_draft",
                    placeholder="이름",
                    label_visibility="collapsed",
                )
            with c2:
                submitted_first = st.form_submit_button("⏎", type="secondary", use_container_width=False)
        if submitted_first:
            q = (st.session_state.get("name_search_draft") or "").strip()
            if q and q not in st.session_state["name_search_terms"]:
                st.session_state["name_search_terms"].append(q)
                if _receiver_name_match_count(orders, q) == 0:
                    st.session_state["_focus_next_name_inline"] = True
                st.rerun()

    search_block_on = bool(parts)
    pick_ids: list[str] = []
    if not search_block_on:
        st.divider()
        st.session_state["search_pick_ids"] = []
        st.session_state["prev_search_pick_ids"] = []
        st.session_state.pop("search_pick_labels", None)
        st.session_state.pop("_search_sync_order_ids", None)
        st.session_state.pop("_search_sync_sig", None)
        st.session_state["table_sync_from_search"] = False
        if "mutomo_full_orders_df" in st.session_state:
            st.session_state["mutomo_full_orders_df"] = {
                "selection": {"rows": [], "columns": [], "cells": []},
            }
        return

    st.divider()
    hdr_l, hdr_m = st.columns([1, 7], gap="small")
    with hdr_l:
        st.markdown("")
    with hdr_m:
        b_pop, b_clr = st.columns(2, gap="small")
        with b_pop:
            pop_clicked = st.button(
                "맨 끝만 삭제",
                key="name_search_pop_btn",
                use_container_width=True,
                help="검색 조건에서 가장 마지막 줄만 삭제합니다.",
            )
        with b_clr:
            clear_clicked = st.button(
                "전부 비우기",
                key="name_search_clear_btn",
                use_container_width=True,
                help="검색 조건·입력·선택·아래 표 연동을 한꺼번에 초기화합니다.",
            )

    if pop_clicked:
        if st.session_state["name_search_terms"]:
            i_last = len(st.session_state["name_search_terms"]) - 1
            st.session_state["name_search_terms"].pop()
            st.session_state.pop(f"row_pick_labels_{i_last}", None)
            st.session_state.pop(f"name_search_row_input_{i_last}", None)
        if not st.session_state["name_search_terms"]:
            st.session_state["_search_ui_full_reset"] = True
        st.rerun()
    if clear_clicked:
        st.session_state["name_search_terms"] = []
        _clear_row_pick_label_keys()
        _clear_name_search_row_input_keys()
        for _k in list(st.session_state.keys()):
            if isinstance(_k, str) and _k.startswith("name_inline_draft_"):
                st.session_state.pop(_k, None)
        st.session_state["_reset_name_search_draft"] = True
        st.session_state["_search_ui_full_reset"] = True
        st.rerun()

    def _sort_hits_block(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        if out.empty:
            return out
        if "status" in out.columns:
            out["_shipped_last"] = out["status"].astype(str).str.strip().eq("출고").astype(int)
            _sc = [c for c in ["_shipped_last", "purchase_date"] if c in out.columns]
            if _sc:
                out = out.sort_values(_sc, ascending=[True, False], na_position="last")
            out = out.drop(columns=["_shipped_last"], errors="ignore")
        elif "purchase_date" in out.columns:
            out = out.sort_values("purchase_date", ascending=False, na_position="last")
        return out

    if st.session_state.get("request_clear_search_pick_labels"):
        _clear_row_pick_label_keys()
        st.session_state.pop("search_pick_labels", None)
        st.session_state["request_clear_search_pick_labels"] = False

    terms_rows: list[str] = st.session_state["name_search_terms"]
    _prune_name_search_row_input_keys(len(terms_rows))

    # fuzzy 이름 버튼은 위젯 생성 이후라 같은 런에서 row_input 키를 못 바꿈 → 다음 런 초반에 반영
    _sug_apply = st.session_state.pop("_name_suggest_apply", None)
    if isinstance(_sug_apply, (list, tuple)) and len(_sug_apply) == 2:
        _sug_i, _sug_nm = _sug_apply
        if isinstance(_sug_i, int) and isinstance(_sug_nm, str):
            _sn2 = _sug_nm.strip()
            _ts = st.session_state["name_search_terms"]
            if _sn2 and 0 <= _sug_i < len(_ts):
                _ts[_sug_i] = _sn2
                st.session_state.pop(f"name_search_row_input_{_sug_i}", None)
                st.session_state.pop(f"row_pick_labels_{_sug_i}", None)

    def _row_term_live(idx: int) -> str:
        ts2 = st.session_state["name_search_terms"]
        if idx >= len(ts2):
            return ""
        rk = f"name_search_row_input_{idx}"
        base = str(ts2[idx]).strip()
        if rk not in st.session_state:
            return base
        return str(st.session_state[rk]).strip()

    def _row_term_for_match(idx: int) -> str:
        """위젯이 잠깐 비면 term_live가 ''가 되는데, pandas str.contains('')는 전 행 True라 라벨·선택이 깨짐 → 커밋된 줄 이름으로 보정."""
        ts2 = st.session_state["name_search_terms"]
        if idx >= len(ts2):
            return ""
        live = (_row_term_live(idx) or "").strip()
        if live:
            return live
        return str(ts2[idx]).strip()

    for i in range(len(terms_rows)):
        term0 = str(terms_rows[i]).strip()
        if not term0:
            continue
        rk = f"name_search_row_input_{i}"
        if rk not in st.session_state:
            st.session_state[rk] = term0
        term_live = _row_term_live(i)
        term_match = _row_term_for_match(i)
        row_hits = (
            orders.iloc[:0].copy()
            if not term_match
            else orders[nm_series.str.contains(term_match, case=False, na=False, regex=False)].copy()
        )
        row_hits = _sort_hits_block(row_hits)
        tags = _identity_hint_tags(row_hits)
        mk = f"row_pick_labels_{i}"
        _, options_r, default_lbl_r = _build_order_picker_lists(row_hits)
        if not options_r:
            # 0건일 때 pop 하면 다음 렌더에서 복귀해도 선택이 영구 사라짐 → 일시 0건은 키 유지
            if len(row_hits) > 0:
                st.session_state.pop(mk, None)
        else:
            if mk not in st.session_state:
                st.session_state[mk] = [] if tags else (list(default_lbl_r) if default_lbl_r else [])
            raw = st.session_state.get(mk, [])
            if not isinstance(raw, list):
                raw = []
            valid = [x for x in raw if x in options_r]
            if valid != raw:
                # 옵션 라벨이 한 프레임만 어긋나면 valid가 []가 되기 쉬움 → 기존 선택 통째로 비우지 않음
                if valid or not raw:
                    st.session_state[mk] = valid
            if not tags and not (st.session_state.get(mk) or []):
                st.session_state[mk] = list(default_lbl_r) if default_lbl_r else ([options_r[0]] if options_r else [])

        c_nm, c_ord = st.columns([1, 7], gap="small")
        with c_nm:
            st.text_input(
                "이름",
                key=rk,
                placeholder="이름",
                label_visibility="collapsed",
            )
        with c_ord:
            if options_r:
                st.multiselect(
                    " ",
                    options=options_r,
                    key=mk,
                    label_visibility="collapsed",
                )
            elif len(row_hits) == 0:
                if term_match and _receiver_name_match_count(orders, term_match) == 0 and _receiver_name_match_count(nh, term_match) > 0:
                    st.caption(
                        "전체 접수에는 이 이름이 있으나 **현재 사이드바 상태 필터**에는 없습니다. "
                        "필터에 **접수·출고** 등을 포함하면 오른쪽에서 고를 수 있습니다."
                    )
                _cand_row = _unique_receiver_names(nh)
                _sugs_row = _fuzzy_receiver_name_suggestions(term_match, _cand_row, limit=6)
                if term_match and _sugs_row:
                    st.caption("이 이름으로는 접수가 없습니다. **등록명**과 한 글자만 달라도 안 잡힙니다. 비슷한 이름:")
                    _nc = min(4, len(_sugs_row))
                    _rcols = st.columns(_nc)
                    for _si, _sn in enumerate(_sugs_row):
                        with _rcols[_si % _nc]:
                            if st.button(_sn, key=f"_row_name_suggest_{i}_{_si}"):
                                st.session_state["_name_suggest_apply"] = (i, _sn)
                                st.rerun()
                elif term_match:
                    st.caption("일치하는 접수가 없습니다.")
            else:
                st.caption("주문 목록을 만들 수 없습니다(데이터에 `order_id`가 있는지 확인).")

    def _row_pick_satisfied(idx: int) -> bool:
        t = _row_term_for_match(idx)
        if not t:
            return True
        rh = orders[nm_series.str.contains(t, case=False, na=False, regex=False)].copy()
        rh = _sort_hits_block(rh)
        mk2 = f"row_pick_labels_{idx}"
        picked = st.session_state.get(mk2, [])
        if len(rh) == 0:
            return True
        _, opts, _ = _build_order_picker_lists(rh)
        if not opts:
            return True
        return len(picked) >= 1

    can_add_next = all(_row_pick_satisfied(i) for i in range(len(terms_rows)))
    _rows_missing_pick: list[str] = []
    for _j in range(len(terms_rows)):
        if not _row_pick_satisfied(_j):
            _lab = (_row_term_for_match(_j) or str(terms_rows[_j]).strip() or f"{_j + 1}번째").strip()
            if _lab:
                _rows_missing_pick.append(_lab)

    if len(terms_rows) >= 1:
        _n_terms = len(terms_rows)
        _inline_draft_key = f"name_inline_draft_{_n_terms}"
        _inline_form_key = f"name_inline_form_{_n_terms}"
        st.session_state.pop("name_search_inline_next_draft", None)
        with st.form(_inline_form_key, clear_on_submit=True):
            a1, a2 = st.columns([1, 7], gap="small")
            with a1:
                st.text_input(
                    "이름",
                    key=_inline_draft_key,
                    placeholder="다음 이름",
                    label_visibility="collapsed",
                )
            with a2:
                submitted_inline = st.form_submit_button("⏎", type="secondary", use_container_width=False)
        if submitted_inline:
            qn = (st.session_state.get(_inline_draft_key) or "").strip()
            if not can_add_next:
                if qn:
                    if _rows_missing_pick:
                        _who = "**, **".join(_rows_missing_pick)
                        st.warning(
                            f"아래 이름 **검색 줄마다** 오른쪽에서 **처리할 주문**을 골라야 합니다. "
                            f"아직 선택이 없는 줄: **{_who}**."
                        )
                    else:
                        st.warning("검색 줄에서 **처리할 주문**을 먼저 고른 뒤에 다음 이름을 추가할 수 있습니다.")
            elif qn and qn not in st.session_state["name_search_terms"]:
                st.session_state["name_search_terms"].append(qn)
                if _receiver_name_match_count(orders, qn) == 0:
                    st.session_state["_focus_next_name_inline"] = True
                st.rerun()

    parts_live: list[str] = []
    for _i in range(len(terms_rows)):
        _t = _row_term_for_match(_i)
        if _t:
            parts_live.append(_t)
    if parts_live:
        mask_live = nm_series.str.contains(parts_live[0], case=False, na=False, regex=False)
        for _q in parts_live[1:]:
            mask_live = mask_live | nm_series.str.contains(_q, case=False, na=False, regex=False)
        hits_live = orders[mask_live].copy()
    else:
        hits_live = orders.iloc[:0].copy()

    st.markdown("**선택 요약**")
    pick_ids = []
    seen_oid: set[str] = set()
    for i in range(len(terms_rows)):
        tl = _row_term_for_match(i)
        if not tl:
            continue
        row_hits = orders[nm_series.str.contains(tl, case=False, na=False, regex=False)].copy()
        row_hits = _sort_hits_block(row_hits)
        ltd, _opts, _ = _build_order_picker_lists(row_hits)
        mk3 = f"row_pick_labels_{i}"
        for lbl in st.session_state.get(mk3, []):
            oid = str(ltd.get(lbl, "")).strip()
            if oid and oid not in seen_oid:
                seen_oid.add(oid)
                pick_ids.append(oid)

    n_sum = len(pick_ids)
    st.caption(f"**{n_sum}건**")

    _already_done_status = frozenset({"출고", "마감", "납품취소"})
    if pick_ids and "status" in orders.columns and "order_id" in orders.columns:
        _sp = orders[orders["order_id"].astype(str).isin([str(x) for x in pick_ids])]
        if len(_sp):
            _st = _sp["status"].astype(str).str.strip()
            _n_done = int(_st.isin(_already_done_status).sum())
            if _n_done:
                st.warning(
                    f"선택 **{n_sum}건** 중 **{_n_done}건**은 이미 처리된 주문입니다 "
                    f"(출고·마감·납품취소). **출고** 버튼은 출고 완료 건을 건너뜁니다."
                )

    if len(hits_live) == 0:
        pass

    _hint_lines = _identity_hint_lines(hits_live)
    if _hint_lines and len(hits_live):
        st.info("\n\n".join(_hint_lines))

    st.session_state["search_pick_ids"] = pick_ids

    if pick_ids != st.session_state.get("prev_search_pick_ids"):
        st.session_state["prev_search_pick_ids"] = pick_ids

    ship_action = st.button("선택 건 출고 처리", type="primary", key="ship_one_button")
    rc1, rc2, rc3, rc4, rc5 = st.columns([1, 1, 1, 1, 1.4])
    with rc1:
        claim_action = st.button("클레임", key="status_claim")
    with rc2:
        back_to_received_action = st.button("접수", key="status_received")
    with rc3:
        close_action = st.button("마감", key="status_close")
    with rc4:
        cancel_action = st.button("납품취소", key="status_cancel")
    with rc5:
        st.download_button(
            "로젠 업로드(.xlsx)",
            data=_build_lozen_xlsx_bytes_for_orders(orders, items_all, pick_ids) if pick_ids else b"",
            file_name=f"lozen_upload_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            disabled=not bool(pick_ids),
            help="선택된 주문 중 택배(엑셀 기준)만 포함해 로젠 업로드용 엑셀을 만듭니다.",
        )

    if ship_action and pick_ids:
        sub = orders[orders["order_id"].astype(str).isin([str(x) for x in pick_ids])]
        if "status" not in sub.columns:
            sub_st = pd.Series(["접수"] * len(sub))
        else:
            sub_st = sub["status"].astype(str).str.strip()
        already_shipped = sub[sub_st == "출고"]["order_id"].astype(str).tolist()
        pending_ship = [str(x) for x in pick_ids if str(x) not in set(already_shipped)]
        if not pending_ship:
            st.warning(
                "선택한 주문이 모두 **이미 출고 처리**되었습니다. "
                "미출고(접수·클레임 등) 주문만 골라 주세요."
            )
        else:
            if already_shipped:
                st.info(f"이미 출고 {len(already_shipped)}건은 건너뛰고, **{len(pending_ship)}건**만 출고 처리합니다.")
            con = sqlite3.connect(db_path)
            try:
                cur = con.cursor()
                now = dt.datetime.now().isoformat(timespec="seconds")
                cur.executemany(
                    "UPDATE orders SET status=?, shipped_at=COALESCE(shipped_at, ?) WHERE order_id=?",
                    [("출고", now, oid) for oid in pending_ship],
                )
                con.commit()
            finally:
                con.close()
            st.success("출고 처리했습니다.")
            st.cache_data.clear()
            st.rerun()

    if claim_action and pick_ids:
        con = sqlite3.connect(db_path)
        try:
            cur = con.cursor()
            cur.executemany(
                "UPDATE orders SET status=? WHERE order_id=?",
                [("클레임", oid) for oid in pick_ids],
            )
            con.commit()
        finally:
            con.close()
        st.success("클레임으로 변경했습니다.")
        st.cache_data.clear()
        st.rerun()

    if back_to_received_action and pick_ids:
        con = sqlite3.connect(db_path)
        try:
            cur = con.cursor()
            cur.executemany(
                "UPDATE orders SET status=?, shipped_at=NULL WHERE order_id=?",
                [("접수", oid) for oid in pick_ids],
            )
            con.commit()
        finally:
            con.close()
        st.success("출고 취소(접수로 변경 + 출고시간 초기화) 했습니다.")
        st.cache_data.clear()
        st.rerun()

    if close_action and pick_ids:
        con = sqlite3.connect(db_path)
        try:
            cur = con.cursor()
            cur.executemany(
                "UPDATE orders SET status=? WHERE order_id=?",
                [("마감", oid) for oid in pick_ids],
            )
            con.commit()
        finally:
            con.close()
        st.success("마감으로 변경했습니다.")
        st.cache_data.clear()
        st.rerun()

    if cancel_action and pick_ids:
        con = sqlite3.connect(db_path)
        try:
            cur = con.cursor()
            cur.executemany(
                "UPDATE orders SET status=? WHERE order_id=?",
                [("납품취소", oid) for oid in pick_ids],
            )
            con.commit()
        finally:
            con.close()
        st.success("납품취소로 변경했습니다.")
        st.cache_data.clear()
        st.rerun()

    sig_key = "|".join(parts_live)

    if parts_live and len(hits_live):
        if "order_id" in hits_live.columns:
            hit_id_set = set(hits_live["order_id"].astype(str).tolist())
            if pick_ids:
                oids = [str(x) for x in pick_ids if str(x) in hit_id_set]
            else:
                oids = []
            if len(oids) > _MAX_SEARCH_TO_TABLE_ROWS:
                st.caption(
                    f"선택 {len(oids)}건 중 표·사이드바에는 앞에서 {_MAX_SEARCH_TO_TABLE_ROWS}건만 연동합니다. "
                    "더 좁히려면 오른쪽에서 선택을 줄이거나 검색어를 나누세요."
                )
                oids = oids[:_MAX_SEARCH_TO_TABLE_ROWS]
            st.session_state["_search_sync_order_ids"] = oids
            st.session_state["table_sync_from_search"] = True
            st.session_state["selected_ids_from_table"] = list(oids)
            st.session_state["prev_table_pick_ids"] = list(oids)
            st.session_state["active_selector"] = "table"
            sig = (sig_key, tuple(oids))
            if st.session_state.get("_search_sync_sig") != sig:
                st.session_state["_search_sync_sig"] = sig
                st.session_state["_search_sync_need_df_apply"] = True
    elif parts_live and len(hits_live) == 0:
        _ts0 = st.session_state.get("table_sync_from_search")
        st.session_state.pop("search_pick_labels", None)
        st.session_state["search_pick_ids"] = []
        st.session_state["prev_search_pick_ids"] = []
        if _ts0:
            st.session_state["selected_ids_from_table"] = []
            st.session_state["prev_table_pick_ids"] = []
            st.session_state["active_selector"] = None
        cand = _unique_receiver_names(nh)
        merged: list[str] = []
        for p in parts_live:
            merged.extend(_fuzzy_receiver_name_suggestions(p, cand))
        seen: set[str] = set()
        sugs: list[str] = []
        for s in merged:
            if s not in seen:
                seen.add(s)
                sugs.append(s)
        sugs = sugs[:12]
        if sugs:
            st.caption("등록된 이름 중 비슷한 후보. 누르면 **OR 표에 추가**됩니다.")
            ncols = min(4, len(sugs))
            for row_start in range(0, len(sugs), ncols):
                row = sugs[row_start : row_start + ncols]
                cols = st.columns(len(row))
                for j, name in enumerate(row):
                    idx = row_start + j
                    if cols[j].button(name, key=f"_name_suggest_btn_{idx}"):
                        if name not in st.session_state["name_search_terms"]:
                            st.session_state["name_search_terms"].append(name)
                        st.rerun()
        st.session_state.pop("_search_sync_order_ids", None)
        st.session_state.pop("_search_sync_sig", None)
        st.session_state["table_sync_from_search"] = False
        if _ts0 and "mutomo_full_orders_df" in st.session_state:
            st.session_state["mutomo_full_orders_df"] = {
                "selection": {"rows": [], "columns": [], "cells": []},
            }
    elif not parts_live:
        _full_reset = st.session_state.pop("_search_ui_full_reset", False)
        _ts_empty = st.session_state.get("table_sync_from_search")
        st.session_state["search_pick_ids"] = []
        st.session_state["prev_search_pick_ids"] = []
        st.session_state.pop("search_pick_labels", None)
        if _full_reset or _ts_empty:
            st.session_state["selected_ids_from_table"] = []
            st.session_state["prev_table_pick_ids"] = []
            st.session_state["active_selector"] = None
        st.session_state.pop("_search_sync_order_ids", None)
        st.session_state.pop("_search_sync_sig", None)
        st.session_state["table_sync_from_search"] = False
        if (_full_reset or _ts_empty) and "mutomo_full_orders_df" in st.session_state:
            st.session_state["mutomo_full_orders_df"] = {
                "selection": {"rows": [], "columns": [], "cells": []},
            }

    if st.session_state.pop("_focus_next_name_inline", False):
        _focus_streamlit_input_by_placeholder("다음 이름")


def render_sales_period_tab(orders_all: pd.DataFrame, items_all: pd.DataFrame) -> None:
    """이름 검색과 같이 메인 탭에서 기간·상태별 집계(단가표는 합계만 반영)."""
    st.subheader("기간별 판매집계")
    st.caption(
        "기간·포함 상태는 **이 탭에서만** 적용됩니다(사이드바 **상태 필터**와 별개). "
        "금액은 프로젝트 루트 **단가표.csv**로 계산하며, 단가 행은 표시하지 않습니다."
    )
    today = dt.date.today()
    c1, c2 = st.columns(2)
    with c1:
        d_start = st.date_input("시작일", value=today - dt.timedelta(days=30), key="dash_sales_d0")
    with c2:
        d_end = st.date_input("종료일", value=today, key="dash_sales_d1")
    if d_start > d_end:
        st.warning("시작일이 종료일보다 늦습니다.")
        return

    basis_label = st.radio(
        "기준일",
        options=[
            ("purchase", "구매일자(파일명에서 추출)"),
            ("created", "수집시각(created_at)"),
            ("shipped", "출고일(shipped_at) — 미출고 주문은 기간에 안 잡힘"),
        ],
        format_func=lambda x: x[1],
        horizontal=True,
        key="dash_sales_basis",
    )
    date_basis = basis_label[0]

    status_sel = st.multiselect(
        "포함할 주문 상태 (비우면 전체)",
        options=["접수", "클레임", "출고", "마감", "납품취소"],
        default=["접수", "출고", "클레임", "마감"],
        key="dash_sales_status_filter",
    )
    status_filter = None if not status_sel else set(status_sel)

    _root = os.path.dirname(os.path.abspath(__file__))
    price_path = os.path.join(_root, "단가표.csv")
    price_mtime = os.path.getmtime(price_path) if os.path.isfile(price_path) else -1.0
    price_map, price_warns = _price_map_cached(price_path, float(price_mtime))

    if price_warns:
        st.info("단가표: " + " ".join(w for w in price_warns if w))

    out = summarize_sales_period(
        orders_all,
        items_all,
        price_map,
        d_start,
        d_end,
        date_basis=date_basis,
        status_filter=status_filter,
    )

    m1, m2, m3, m4, m5, m6 = st.columns(6)
    with m1:
        st.metric("주문 건수", f"{out['n_orders']:,}")
    with m2:
        st.metric("품목 라인 수", f"{out['n_item_lines']:,}")
    with m3:
        st.metric("판매 수량 합", f"{out['qty_sum']:,}")
    with m4:
        st.metric("판매금액(단가표)", format_won(out["sale_amount"]))
    with m5:
        st.metric("광진금액(단가표)", format_won(out["gwangjin_amount"]))
    with m6:
        st.metric("단가 미매칭 라인", f"{out['n_unpriced_lines']:,}")

    if out["unpriced_qty"]:
        st.caption(f"단가표에 없는 품목의 수량 합(참고): **{out['unpriced_qty']:,}**")

    st.subheader("상품별 요약")
    bp = out["by_product"]
    if len(bp) == 0:
        st.info("해당 기간·조건에 맞는 품목이 없습니다.")
    else:
        show = bp.copy()
        show["판매금액"] = show["판매금액"].map(lambda x: format_won(x))
        show["광진금액"] = show["광진금액"].map(lambda x: format_won(x))
        st.dataframe(show, use_container_width=True, hide_index=True)


def main() -> None:
    st.set_page_config(page_title="Mutomo 판매관리", layout="wide")

    # Wider sidebar + 메인 본문을 위로(기본 상단 패딩 축소)
    st.markdown(
        """
<style>
  section[data-testid="stSidebar"] { width: 560px !important; }
  section[data-testid="stSidebar"] > div { width: 560px !important; }
  .block-container { padding-top: 0.75rem !important; padding-bottom: 0.5rem !important; }
</style>
        """,
        unsafe_allow_html=True,
    )

    init_shared_session_state()

    # DB missing / unreadable messages (written from sidebar 설정 expander)
    main_db_slot = st.empty()

    # Settings in sidebar (collapsed)
    with st.sidebar.expander("설정", expanded=False):
        db_path = st.text_input("DB 경로", value="mutomo.sqlite", key="mutomo_db_path")
        status_filter = st.multiselect(
            "상태 필터",
            options=["접수", "클레임", "출고", "마감", "납품취소"],
            default=["접수", "출고"],
            key="mutomo_status_filter",
        )
        date_basis = st.selectbox(
            "‘오늘 접수’ 기준",
            options=["purchase_date(구매일자: 파일명)", "created_at(수집시각)"],
            index=0,
        )
        show_admin_cols = st.toggle("관리 컬럼 보기", value=False)
        try:
            mtime = dt.datetime.fromtimestamp(os.path.getmtime(__file__)).isoformat(timespec="seconds")
        except Exception:
            mtime = "unknown"
        st.caption(f"dashboard.py: {__file__} (mtime: {mtime})")

        # Automatic DB backup (once per day per user session)
        if "last_db_backup_day" not in st.session_state:
            st.session_state["last_db_backup_day"] = None
        today_key = dt.date.today().isoformat()
        if st.session_state.get("last_db_backup_day") != today_key:
            out = _backup_sqlite(db_path, backup_dir="backups", keep_days=30)
            st.session_state["last_db_backup_day"] = today_key
            if out:
                st.caption(f"DB 자동백업: `{out}`")

        st.caption(
            "`order_list` 안 엑셀만 바꿨다면 DB는 안 바뀝니다. 터미널에서 **ingest**를 다시 실행한 뒤 아래를 누르거나 브라우저를 새로고침하세요."
        )
        if st.button("DB 다시 읽기", key="reload_db_cache", help="ingest 후 화면이 그대로일 때"):
            st.cache_data.clear()
            st.rerun()

        ok_db, db_msg = _db_ready(db_path)
        if not ok_db:
            main_db_slot.error(db_msg)
            main_db_slot.markdown(
                "아래 명령으로 `orders` / `items` 테이블을 만듭니다. 기본으로 **`order_list` 폴더** 안의 `.xlsx`만 읽습니다. "
                "그 폴더에 파일이 없으면 **빈 테이블만** 만들어져 대시보드가 열리고, 엑셀을 넣고 같은 명령을 다시 실행하면 데이터가 채워집니다.\n\n"
                "```bash\n"
                "python ingest_xlsx.py --db mutomo.sqlite --aliases product_aliases.yml\n"
                "```\n\n"
                "다른 위치에만 두고 싶으면 `--input-dir \"D:\\경로\"` 처럼 지정하면 됩니다. 다른 이름의 DB를 쓰는 경우 **설정**의 DB 경로를 그 파일로 맞추세요."
            )
            st.stop()

        _migrate_orders_schema(db_path)
        _mt, _sz = _db_stat_for_cache(db_path)
        orders_all, items_all = load_tables(db_path, _mt, _sz)

        try:
            con0 = sqlite3.connect(db_path)
            try:
                recent_files = pd.read_sql_query(
                    """
                    select
                      source_file as 파일,
                      count(*) as 주문건수,
                      max(created_at) as 마지막수집
                    from orders
                    group by source_file
                    order by 마지막수집 desc
                    limit 12
                    """,
                    con0,
                )
            finally:
                con0.close()
            if len(recent_files):
                st.markdown("**최근 수집 파일(최대 12개)**")
                st.caption("표에는 최대 12개까지 표시되며, 약 5행만 보이고 나머지는 스크롤합니다.")
                st.dataframe(recent_files, use_container_width=True, hide_index=True, height=220)
        except Exception:
            pass

    with st.sidebar.expander("출고 목록 엑셀", expanded=False):
        export_version = "v12_strip_order_list_from_notes"
        shipped_date = st.date_input("출고 기준 날짜", value=dt.date.today(), key="export_shipped_date")
        shipped_today_cnt = 0
        if "status" in orders_all.columns:
            shipped = orders_all[orders_all["status"] == "출고"].copy()
            if "shipped_at" in shipped.columns:
                shipped["_d"] = pd.to_datetime(shipped["shipped_at"], errors="coerce").dt.date
                shipped_today_cnt = int((shipped["_d"] == shipped_date).sum())
            else:
                shipped_today_cnt = len(shipped)
        st.caption(f"{shipped_date.isoformat()} 출고: {shipped_today_cnt}건")
        if shipped_today_cnt > 0 and "status" in orders_all.columns:
            shipped_pick = orders_all[orders_all["status"] == "출고"].copy()
            if "shipped_at" in shipped_pick.columns:
                shipped_pick["_d"] = pd.to_datetime(shipped_pick["shipped_at"], errors="coerce").dt.date
                shipped_pick = shipped_pick[shipped_pick["_d"] == shipped_date]
            _nu, _nt, _nd, _nm = _picking_stats(shipped_pick, items_all)
            st.caption(f"피킹 시트 행: 택배 {_nt}, 직접 {_nd} — 혼합 {_nm}건은 두 시트에 각 1행 (검산 {_nt}+{_nd} = {_nu}+{_nm})")
        filename = f"mutomo_shipped_{shipped_date.isoformat()}_shipped.xlsx"
        shipped_today_ids = []
        if shipped_today_cnt > 0 and "status" in orders_all.columns:
            shipped = orders_all[orders_all["status"] == "출고"].copy()
            if "shipped_at" in shipped.columns:
                shipped["_d"] = pd.to_datetime(shipped["shipped_at"], errors="coerce").dt.date
                shipped = shipped[shipped["_d"] == shipped_date]
            shipped_today_ids = shipped.get("order_id", pd.Series([], dtype=str)).astype(str).tolist()
        shipped_key = f"{shipped_date.isoformat()}|{shipped_today_cnt}|{'/'.join(shipped_today_ids[:200])}"
        xlsx_bytes = _today_shipped_excel_cached(export_version, shipped_key, orders_all, items_all) if shipped_today_cnt > 0 else b""

        st.download_button(
            "출고 목록 엑셀 다운로드",
            data=xlsx_bytes,
            file_name=filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary",
            disabled=(shipped_today_cnt == 0),
            key=f"download_shipped_{export_version}_{shipped_date.isoformat()}_{shipped_today_cnt}",
        )

    if status_filter:
        orders = orders_all[orders_all["status"].isin(status_filter)]
        items = items_all[items_all["order_id"].isin(orders["order_id"])]
    else:
        orders = orders_all.copy()
        items = items_all.copy()

    today = dt.date.today()
    if date_basis.startswith("purchase_date") and "purchase_date" in orders.columns:
        orders["_date"] = _to_date_series(orders["purchase_date"])
    else:
        orders["_date"] = _to_date_series(orders["created_at"])

    ship_tbl = claim_tbl = back_tbl = close_tbl = cancel_tbl = False

    tab_sales, tab_name_search, tab_sales_period = st.tabs(["판매 요약", "이름 검색", "기간별 판매집계"])
    with tab_name_search:
        render_receiver_name_search(orders, items, db_path, orders_name_hints=orders_all)
        st.divider()
        st.subheader("전체 접수 목록")
        if len(orders) > 5000:
            st.caption(
                f"현재 표에 올리는 행이 **{len(orders):,}건**입니다. "
                "느려지면 상태 필터로 줄이거나, ingest 전에 SQLite에서 기간·상태로 나눠 저장하는 방식을 검토하세요."
            )
        st.caption(
            "🟥 표시는 엑셀 A열의 ‘특이사항(자동)’이 있는 주문입니다. "
            "**배송(엑셀)** 은 목록에서만 보는 품목 배송란 다수결 요약입니다. "
            "사이드바 상세의 배송은 **품목** 아래 **배송:** 에 적힌 엑셀 원문만 쓰며, 변경은 현장에서 처리합니다. "
            "**동일연락처** 는 전화(또는 주소)로 묶은 그룹 안의 접수 건수(재주문·동일 수하인 힌트)이며, "
            "확정 식별은 항상 **order_id**(한 줄 한 접수)입니다."
        )
        user_cols = [
            "purchase_date",
            "receiver_name",
            "동일연락처",
            "배송(엑셀)",
            "order_list",
            "address",
            "phone",
            "delivery_request",
            "attention_note",
            "special_issue",
            "status",
        ]
        admin_cols = [
            "order_id",
            "party_key",
            "source_file",
            "group_no",
            "deadline_raw",
            "order_date_raw",
            "created_at",
        ]
        all_cols = user_cols + (admin_cols if show_admin_cols else [])
        sort_cols_all = [c for c in ["source_file", "group_no"] if c in orders.columns]
        sorted_orders = orders.drop(columns=["_date"], errors="ignore")
        if sort_cols_all:
            sorted_orders = sorted_orders.sort_values(sort_cols_all, na_position="last")
        if "order_id" in sorted_orders.columns and len(items_all) and "order_id" in items_all.columns:
            _ship_map = infer_settlement_ship_series(items_all)
            sorted_orders = sorted_orders.copy()
            sorted_orders["배송(엑셀)"] = (
                sorted_orders["order_id"].astype(str).map(_ship_map).fillna("택배").map(_ship_display_label)
            )
        if "party_key" in sorted_orders.columns:
            _pk = sorted_orders["party_key"].astype(str)
            _grp_n = _pk.groupby(_pk).transform("count")
            sorted_orders["동일연락처"] = _grp_n.map(lambda x: f"{int(x)}건" if int(x) > 1 else "—")
        else:
            sorted_orders = sorted_orders.copy()
            sorted_orders["동일연락처"] = "—"
        view_orders = sorted_orders[[c for c in all_cols if c in sorted_orders.columns]].copy()

        # Make status visually distinct (emoji badges). Streamlit's dataframe has limited per-row styling.
        if "status" in view_orders.columns:
            def _badge(s: object) -> str:
                ss = "" if s is None else str(s).strip()
                if ss == "출고":
                    return "🚚✅ 출고완료"
                if ss == "접수":
                    return "📝⏳ 접수"
                if ss == "클레임":
                    return "⚠️ 클레임"
                if ss == "마감":
                    return "🧾 마감"
                if ss == "납품취소":
                    return "⛔ 납품취소"
                return ss or ""

            view_orders["status"] = view_orders["status"].map(_badge)

        # 한 칸: 상태 이모지 1 + 이름(최대 8자) + 뒤 아이콘 최대 3 (status 열은 긴 뱃지 유지).
        if "receiver_name" in view_orders.columns and "status" in sorted_orders.columns:
            def _name_emoji(row) -> str:
                name = "" if row.get("receiver_name") is None else str(row.get("receiver_name")).strip()
                stt = "" if row.get("status") is None else str(row.get("status")).strip()
                att_c = _strip_order_list_overlap(row.get("attention_note"), row.get("order_list"))
                return _compact_name_display(stt, name, att_c or None)

            # Use sorted_orders for original status values
            cols_tmp = [
                c for c in ["receiver_name", "status", "attention_note", "order_list"] if c in sorted_orders.columns
            ]
            tmp = sorted_orders.loc[view_orders.index, cols_tmp].copy()
            view_orders["receiver_name"] = tmp.apply(_name_emoji, axis=1)

        st.caption("행을 선택하면 왼쪽 사이드바에 주문상세가 표시됩니다. 선택을 모두 해제하면 상세가 사라집니다.")
        st.caption("목록에서 행을 고른 뒤 아래 버튼으로 출고·클레임 등 상태를 바꿀 수 있습니다.")
        tb1, tb2, tb3, tb4, tb5 = st.columns(5)
        with tb1:
            ship_tbl = st.button("선택 출고", type="primary", key="table_ship_btn")
        with tb2:
            claim_tbl = st.button("클레임", key="table_claim_btn")
        with tb3:
            back_tbl = st.button("접수", key="table_received_btn")
        with tb4:
            close_tbl = st.button("마감", key="table_close_btn")
        with tb5:
            cancel_tbl = st.button("납품취소", key="table_cancel_btn")
        selected_ids: list[str] = []
        table_selection_supported = False
        name_col_cfg: dict | None = None
        _cols_cfg: dict = {}
        if "receiver_name" in view_orders.columns:
            _cols_cfg["receiver_name"] = st.column_config.TextColumn("이름", width="medium")
        if "동일연락처" in view_orders.columns:
            _cols_cfg["동일연락처"] = st.column_config.TextColumn("동일연락처", width="small")
        if "배송(엑셀)" in view_orders.columns:
            _cols_cfg["배송(엑셀)"] = st.column_config.TextColumn("배송(엑셀)", width="small")
        if "party_key" in view_orders.columns:
            _cols_cfg["party_key"] = st.column_config.TextColumn("party_key", width="small")
        if _cols_cfg:
            name_col_cfg = _cols_cfg
        if "order_id" in sorted_orders.columns:
            _df_sel_key = "mutomo_full_orders_df"
            if st.session_state.pop("_search_sync_need_df_apply", False):
                sync_ids = st.session_state.get("_search_sync_order_ids") or []
                idset = set(sync_ids)
                row_idxs = [
                    i
                    for i in range(len(sorted_orders))
                    if str(sorted_orders.iloc[i]["order_id"]) in idset
                ]
                st.session_state[_df_sel_key] = {
                    "selection": {"rows": row_idxs, "columns": [], "cells": []},
                }
            try:
                # Streamlit row selection (supported in recent versions)
                state = st.dataframe(
                    view_orders,
                    use_container_width=True,
                    hide_index=True,
                    column_config=name_col_cfg,
                    on_select="rerun",
                    selection_mode="multi-row",
                    key=_df_sel_key,
                )
                table_selection_supported = True
                if state is not None and hasattr(state, "selection"):
                    rows = getattr(state.selection, "rows", []) or []
                    # Map visible row indices -> order_id from sorted_orders
                    selected_ids = sorted_orders.iloc[rows]["order_id"].astype(str).tolist()
            except TypeError:
                # Older Streamlit: no selection support, just show the table.
                st.dataframe(view_orders, use_container_width=True, hide_index=True, column_config=name_col_cfg)
        else:
            st.dataframe(view_orders, use_container_width=True, hide_index=True, column_config=name_col_cfg)


    with tab_sales:
        with st.container():

            def _sales_section_bar() -> None:
                st.markdown(
                    '<div style="height:3px;background:linear-gradient(90deg,transparent,#90A4AE,#37474F,#90A4AE,transparent);'
                    'border-radius:2px;margin:1.35rem 0 0.9rem 0;opacity:0.95;"></div>',
                    unsafe_allow_html=True,
                )

            # Right-side summary metrics
            s = orders_all["status"] if "status" in orders_all.columns else pd.Series([], dtype=str)
            total_cnt = len(orders_all)
            planned_cnt = int((s == "접수").sum()) if len(s) else 0
            done_cnt = int((s == "출고").sum()) if len(s) else 0
            claim_cnt = int((s == "클레임").sum()) if len(s) else 0
            cancel_cnt = int((s == "납품취소").sum()) if len(s) else 0

            left, m1, m2, m3, m4, m5 = st.columns([2, 1, 1, 1, 1, 1])
            with left:
                st.metric("오늘 접수(주문그룹)", int((orders["_date"] == today).sum()))
            with m1:
                st.metric("전체접수", total_cnt)
            with m2:
                st.metric("납품예정", planned_cnt)
            with m3:
                st.metric("납품완료", done_cnt)
            with m4:
                st.metric("클레임", claim_cnt)
            with m5:
                st.metric("납품취소", cancel_cnt)

            _sales_section_bar()
            st.subheader("오늘접수")

            # Use 접수 상태만 집계/표시 (출고/클레임 등은 전체접수 목록에서 확인)
            recent_base = orders.copy()
            if "status" in recent_base.columns:
                recent_base = recent_base[recent_base["status"] == "접수"]

            def _day_panel(col, day: dt.date) -> None:
                df = recent_base[recent_base["_date"] == day].copy()
                n_actual = len(df)
                # Show request/attention icons in the "today" panels too
                keep = [
                    c for c in ["purchase_date", "receiver_name", "attention_note", "order_list", "status"] if c in df.columns
                ]
                df = df[keep]
                if "purchase_date" not in df.columns:
                    df["purchase_date"] = day.isoformat()
                if "receiver_name" not in df.columns:
                    df["receiver_name"] = ""
                if "attention_note" not in df.columns:
                    df["attention_note"] = ""
                if "order_list" not in df.columns:
                    df["order_list"] = ""
                if "status" not in df.columns:
                    df["status"] = ""

                def _disp_name(r: pd.Series) -> str:
                    nm = "" if r.get("receiver_name") is None else str(r.get("receiver_name")).strip()
                    stt = "" if r.get("status") is None else str(r.get("status")).strip()
                    att_c = _strip_order_list_overlap(r.get("attention_note"), r.get("order_list"))
                    return _compact_name_display(stt, nm, att_c or None)

                df["_이름표시"] = df.apply(_disp_name, axis=1)
                df = df.rename(columns={"purchase_date": "날짜"})
                df = df[["날짜", "_이름표시"]].rename(columns={"_이름표시": "이름"}).sort_values(["이름"], na_position="last")
                h_date, h_qty = col.columns([5, 2])
                with h_date:
                    st.markdown(f"**{day.isoformat()}**")
                    if n_actual > 0:
                        st.caption(f"표 **{n_actual}**건")
                with h_qty:
                    _qk = f"day_panel_qty_접수_{day.isoformat()}"
                    if _qk not in st.session_state:
                        st.session_state[_qk] = int(n_actual)
                    st.number_input(
                        "수량",
                        min_value=0,
                        step=1,
                        key=_qk,
                        help="아래 표 건수와 **다르게** 둘 수 있습니다(현장 메모·목표 등). 브라우저 세션에만 저장됩니다.",
                    )
                col.dataframe(
                    df,
                    use_container_width=True,
                    hide_index=True,
                    column_config={"이름": st.column_config.TextColumn("이름", width="medium")},
                )

            c1, c2, c3, c4 = st.columns(4)
            _day_panel(c1, today)
            _day_panel(c2, today - dt.timedelta(days=1))
            _day_panel(c3, today - dt.timedelta(days=2))
            _day_panel(c4, today - dt.timedelta(days=3))

            _sales_section_bar()
            st.subheader("최근 출고")
            ship_base = orders.copy()
            if "status" in ship_base.columns and len(ship_base):
                ship_base = ship_base[ship_base["status"].astype(str).str.strip() == "출고"].copy()
            else:
                ship_base = orders.iloc[:0].copy()
            if len(ship_base) and "shipped_at" in ship_base.columns:
                ship_base["_ship_day"] = pd.to_datetime(ship_base["shipped_at"], errors="coerce").dt.date
            else:
                ship_base["_ship_day"] = pd.Series(dtype=object)

            def _ship_day_panel(col, day: dt.date) -> None:
                if len(ship_base) and "_ship_day" in ship_base.columns:
                    df = ship_base[ship_base["_ship_day"] == day].copy()
                else:
                    df = ship_base.iloc[:0].copy()
                n_ship = len(df)
                keep = [
                    c for c in ["shipped_at", "receiver_name", "attention_note", "order_list", "status"] if c in df.columns
                ]
                df = df[keep] if len(keep) else pd.DataFrame()
                if "shipped_at" not in df.columns:
                    df["shipped_at"] = pd.NaT
                if "receiver_name" not in df.columns:
                    df["receiver_name"] = ""
                if "attention_note" not in df.columns:
                    df["attention_note"] = ""
                if "order_list" not in df.columns:
                    df["order_list"] = ""
                if "status" not in df.columns:
                    df["status"] = ""

                def _disp_ship_name(r: pd.Series) -> str:
                    nm = "" if r.get("receiver_name") is None else str(r.get("receiver_name")).strip()
                    stt = "" if r.get("status") is None else str(r.get("status")).strip()
                    att_c = _strip_order_list_overlap(r.get("attention_note"), r.get("order_list"))
                    return _compact_name_display(stt, nm, att_c or None)

                df["_이름표시"] = df.apply(_disp_ship_name, axis=1)
                ts = pd.to_datetime(df["shipped_at"], errors="coerce")
                df["_출고일시"] = ts.dt.strftime("%Y-%m-%d %H:%M").fillna("")
                df = (
                    df[["_출고일시", "_이름표시"]]
                    .rename(columns={"_출고일시": "출고", "_이름표시": "이름"})
                    .sort_values(["이름"], na_position="last")
                )
                h_sd, h_sq = col.columns([5, 2])
                with h_sd:
                    st.markdown(f"**{day.isoformat()}**")
                    if n_ship > 0:
                        st.caption(f"표 **{n_ship}**건")
                with h_sq:
                    _sk = f"day_panel_qty_출고_{day.isoformat()}"
                    if _sk not in st.session_state:
                        st.session_state[_sk] = int(n_ship)
                    st.number_input(
                        "수량",
                        min_value=0,
                        step=1,
                        key=_sk,
                        help="아래 표 건수와 **다르게** 둘 수 있습니다(현장 메모·목표 등). 브라우저 세션에만 저장됩니다.",
                    )
                col.dataframe(
                    df,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "출고": st.column_config.TextColumn("출고", width="small"),
                        "이름": st.column_config.TextColumn("이름", width="medium"),
                    },
                )

            s1, s2, s3, s4 = st.columns(4)
            _ship_day_panel(s1, today)
            _ship_day_panel(s2, today - dt.timedelta(days=1))
            _ship_day_panel(s3, today - dt.timedelta(days=2))
            _ship_day_panel(s4, today - dt.timedelta(days=3))

    with tab_sales_period:
        render_sales_period_tab(orders_all, items_all)

    # Mirror "name search" behavior: show selected rows in sidebar
    # Persist selection so button clicks don't lose it on rerun
    if selected_ids:
        if selected_ids != st.session_state.get("prev_table_pick_ids"):
            st.session_state["prev_table_pick_ids"] = selected_ids
            st.session_state["active_selector"] = "table"
            st.session_state["selected_ids_from_table"] = selected_ids
            st.session_state["table_sync_from_search"] = False
            # Clear search selection (including widget state) if table was used
            st.session_state["search_pick_ids"] = []
            st.session_state["prev_search_pick_ids"] = []
            st.session_state["request_clear_search_pick_labels"] = True
    elif (
        table_selection_supported
        and not st.session_state.get("table_sync_from_search")
        and st.session_state.get("active_selector") == "table"
        and st.session_state.get("selected_ids_from_table")
    ):
        st.session_state["selected_ids_from_table"] = []
        st.session_state["prev_table_pick_ids"] = []
        st.session_state["active_selector"] = None

    selected_ids_persisted = st.session_state.get("selected_ids_from_table", [])

    if selected_ids_persisted and st.session_state.get("active_selector") == "table":
        picked = sorted_orders[sorted_orders["order_id"].isin(selected_ids_persisted)].copy()
        sort_cols_pick = [c for c in ["purchase_date", "receiver_name"] if c in picked.columns]
        if sort_cols_pick:
            picked = picked.sort_values(sort_cols_pick, na_position="last")
        st.sidebar.subheader("접수목록 선택 상세")
        st.sidebar.caption(f"선택: {len(selected_ids_persisted)}건")
        for _, r in picked.iterrows():
            _render_order_detail(st.sidebar, r, items)
            st.sidebar.divider()

    # Claim details / special notes editor (applies to current selection)
    active_ids: list[str] = []
    active_label = ""
    if st.session_state.get("active_selector") == "search":
        active_ids = st.session_state.get("search_pick_ids", []) or []
        active_label = "검색 선택"
    elif st.session_state.get("active_selector") == "table":
        active_ids = selected_ids_persisted or []
        active_label = "접수목록 선택"

    if active_ids:
        st.sidebar.subheader("클레임/특이사항")
        if len(active_ids) > 1:
            st.sidebar.caption(f"{active_label}: {len(active_ids)}건 (한 번에 동일 내용 저장)")
        else:
            st.sidebar.caption(f"{active_label}: 1건")

        # Pre-fill from the first selected order
        first_row = orders_all[orders_all["order_id"].astype(str) == str(active_ids[0])]
        current_text = ""
        if len(first_row) and "special_issue" in first_row.columns:
            v = first_row.iloc[0].get("special_issue")
            current_text = "" if v is None else str(v)

        issue_text = st.sidebar.text_area(
            "내용",
            value=current_text,
            height=110,
            key=f"special_issue_editor_{st.session_state.get('active_selector')}_{str(active_ids[0])}",
            placeholder="예) 오염/파손, 색상 변경 요청, 부분 환불, 재발송 필요 등",
        )
        if st.sidebar.button("특이사항 저장", type="secondary", key="save_special_issue"):
            update_special_issue_for_orders(db_path, active_ids, issue_text.strip())
            st.sidebar.success("저장했습니다.")
            st.cache_data.clear()
            st.rerun()

    else:
        st.sidebar.caption(
            "**이름 검색** 탭 오른쪽 **출고할 주문 선택**에서 고르거나, 아래 **전체 접수 목록**에서 행을 고르면 "
            "**클레임/특이사항** 메모를 저장할 수 있습니다. "
            "목록의 **배송(엑셀)** 은 스캔용 요약이며, 상세 배송은 품목 줄의 엑셀 원문만 봅니다."
        )

    # 목록 선택 전용: 이름 검색 탭의 출고 버튼과 분리된 키를 씁니다.
    if ship_tbl and st.session_state.get("active_selector") == "table":
        if selected_ids_persisted:
            con = sqlite3.connect(db_path)
            try:
                cur = con.cursor()
                now = dt.datetime.now().isoformat(timespec="seconds")
                cur.executemany(
                    "UPDATE orders SET status=?, shipped_at=COALESCE(shipped_at, ?) WHERE order_id=?",
                    [("출고", now, oid) for oid in selected_ids_persisted],
                )
                con.commit()
            finally:
                con.close()
            st.sidebar.success(f"출고 처리: {len(selected_ids_persisted)}건")
            st.cache_data.clear()
            st.rerun()

    if claim_tbl and st.session_state.get("active_selector") == "table":
        if selected_ids_persisted:
            con = sqlite3.connect(db_path)
            try:
                cur = con.cursor()
                cur.executemany(
                    "UPDATE orders SET status=? WHERE order_id=?",
                    [("클레임", oid) for oid in selected_ids_persisted],
                )
                con.commit()
            finally:
                con.close()
            st.sidebar.success(f"클레임 처리: {len(selected_ids_persisted)}건")
            st.cache_data.clear()
            st.rerun()

    if back_tbl and st.session_state.get("active_selector") == "table":
        if selected_ids_persisted:
            con = sqlite3.connect(db_path)
            try:
                cur = con.cursor()
                cur.executemany(
                    "UPDATE orders SET status=?, shipped_at=NULL WHERE order_id=?",
                    [("접수", oid) for oid in selected_ids_persisted],
                )
                con.commit()
            finally:
                con.close()
            st.sidebar.success(f"출고 취소(접수로 변경 + 출고시간 초기화): {len(selected_ids_persisted)}건")
            st.cache_data.clear()
            st.rerun()

    if close_tbl and st.session_state.get("active_selector") == "table":
        if selected_ids_persisted:
            con = sqlite3.connect(db_path)
            try:
                cur = con.cursor()
                cur.executemany(
                    "UPDATE orders SET status=? WHERE order_id=?",
                    [("마감", oid) for oid in selected_ids_persisted],
                )
                con.commit()
            finally:
                con.close()
            st.sidebar.success(f"마감 처리: {len(selected_ids_persisted)}건")
            st.cache_data.clear()
            st.rerun()

    if cancel_tbl and st.session_state.get("active_selector") == "table":
        if selected_ids_persisted:
            con = sqlite3.connect(db_path)
            try:
                cur = con.cursor()
                cur.executemany(
                    "UPDATE orders SET status=? WHERE order_id=?",
                    [("납품취소", oid) for oid in selected_ids_persisted],
                )
                con.commit()
            finally:
                con.close()
            st.sidebar.success(f"납품취소 처리: {len(selected_ids_persisted)}건")
            st.cache_data.clear()
            st.rerun()

    with st.expander("제품 목록(집계)", expanded=False):
        st.caption(
            "위 **상태 필터**에 맞는 품목 라인만 집계합니다. "
            "정규명은 `product_aliases.yml` 매핑 뒤의 표준 상품명입니다. "
            "줄 단위 원본은 아래 **전체 품목(라인아이템) 보기**를 열어 확인하세요."
        )
        g_map, g_raw = _product_catalog_summary(items)
        if len(g_map) == 0 and len(g_raw) == 0:
            st.info("표시할 품목 라인이 없습니다.")
        else:
            if len(g_map):
                st.markdown("**정규 상품명** (매핑됨)")
                st.dataframe(g_map, use_container_width=True, hide_index=True)
            if len(g_raw):
                st.markdown("**엑셀 원문** (정규명 미매핑)")
                st.dataframe(g_raw, use_container_width=True, hide_index=True)

    with st.expander("전체 품목(라인아이템) 보기", expanded=False):
        show = items.copy()
        show["is_unmapped"] = show["product_canonical"].isna()
        st.dataframe(
            show.sort_values(["source_file", "order_id", "row_idx"], na_position="last"),
            use_container_width=True,
            hide_index=True,
        )

    with st.expander("상품명 매핑 작업(미매핑만)", expanded=False):
        unmapped = show[show["product_canonical"].isna() & show["product_key"].notna()].copy()
        if len(unmapped) == 0:
            st.info("현재 미매핑 상품이 없습니다.")
        else:
            st.warning("`product_aliases.yml`에 별칭을 추가하면 다음 수집부터 자동 매핑됩니다.")
            st.dataframe(
                unmapped[
                    [
                        "product_raw",
                        "product_key",
                        "suggested_canonical",
                        "suggestion_score",
                        "source_file",
                        "row_idx",
                    ]
                ]
                .drop_duplicates()
                .sort_values(["suggestion_score"], ascending=False, na_position="last"),
                use_container_width=True,
                hide_index=True,
            )


if __name__ == "__main__":
    main()

