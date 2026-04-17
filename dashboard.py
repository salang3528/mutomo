from __future__ import annotations

import datetime as dt
import io
import os
import sqlite3

import pandas as pd
import streamlit as st
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side


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


def _to_date_series(s: pd.Series) -> pd.Series:
    # created_at is ISO string; coerce anything else safely
    return pd.to_datetime(s, errors="coerce").dt.date


@st.cache_data
def _today_shipped_excel_cached(export_version: str, shipped_key: str, orders_all: pd.DataFrame, items_all: pd.DataFrame) -> bytes:
    # shipped_key forces cache bust when today's shipped set changes
    # shipped_key includes date selection
    shipped_date = dt.date.fromisoformat(shipped_key.split("|", 1)[0])
    return _build_shipped_excel_bytes(orders_all, items_all, shipped_date=shipped_date)


def _items_view(items: pd.DataFrame, order_id: str) -> pd.DataFrame:
    df = items[items["order_id"].astype(str) == str(order_id)].copy() if "order_id" in items.columns else pd.DataFrame()
    if len(df) == 0:
        return df

    def _name(r: pd.Series) -> str:
        v = r.get("product_canonical")
        if pd.notna(v) and str(v).strip():
            return str(v).strip()
        v = r.get("product_raw")
        if pd.notna(v) and str(v).strip():
            return str(v).strip()
        return ""

    df["품목"] = df.apply(_name, axis=1)
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
  <div style="font-size:12px; opacity:0.85; margin-bottom:4px;">상태</div>
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
        }
        title = parts["품목"] or "(품목명 없음)"
        container.markdown(f"**{i}. {title}**")

        # Compact line: 규격+사이즈+책장색상+다리색상+개수 (skip empty)
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
        if compact_parts:
            container.markdown(" / ".join(compact_parts))
        if i != len(view):
            container.markdown("---")


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
            line = f"{i}. {name}"
            if parts:
                line += " — " + " / ".join(parts)
            lines.append(line)
        return "\n".join(lines)

    pick_rows: list[dict[str, object]] = []
    for idx, o in shipped.iterrows():
        oid = o.get("order_id")
        view = _items_view(items_all, oid) if oid is not None else pd.DataFrame()
        pick_rows.append(
            {
                "No": idx + 1,
                "받는분": o.get("receiver_name"),
                "전화": o.get("phone"),
                "주소": o.get("address"),
                "배송요청": o.get("delivery_request"),
                "특이사항": o.get("special_issue"),
                "품목": _item_line_from_view(view) if len(view) else (o.get("order_list") or ""),
            }
        )
    picking_sheet = pd.DataFrame(pick_rows)

    # 로젠택배 서식 (오늘 출고 주문 1행=1송장)
    def _lozen_item_name(o: pd.Series) -> str:
        # Prefer first item name; fallback to order_list
        oid = o.get("order_id")
        view = _items_view(items_all, oid) if oid is not None else pd.DataFrame()
        if len(view) and "품목" in view.columns:
            first = str(view.iloc[0]["품목"]).strip()
            return first or str(o.get("order_list") or "").splitlines()[0:1][0] if str(o.get("order_list") or "").strip() else ""
        ol = str(o.get("order_list") or "").strip().replace("\n", " / ")
        return (ol[:50] + "…") if len(ol) > 51 else ol

    lozen_rows: list[dict[str, object]] = []
    for _, o in shipped.iterrows():
        lozen_rows.append(
            {
                "수하인명": o.get("receiver_name"),
                "수하인주소": o.get("address"),
                "수하인전화번호": o.get("phone"),
                "수하인휴대폰번호": o.get("phone"),
                "박스수량": 1,
                "택배운임": 3000,
                "운임구분": "선불",
                "품목명": _lozen_item_name(o),
                "배송메세지": o.get("delivery_request"),
            }
        )
    lozen_sheet = pd.DataFrame(lozen_rows)

    # Order sheet (one row per order)
    order_cols = [
        "purchase_date",
        "receiver_name",
        "phone",
        "address",
        "delivery_request",
        "order_list",
        "special_issue",
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
        item_sheet = (
            item_sheet.groupby("_품목", as_index=False)["_수량"]
            .sum()
            .rename(columns={"_품목": "품목", "_수량": "수량"})
            .sort_values(["품목"])
            .reset_index(drop=True)
        )
    else:
        item_sheet = pd.DataFrame(columns=["품목", "수량"])

    # Friendly column names
    order_sheet = order_sheet.rename(
        columns={
            "purchase_date": "구매일자",
            "receiver_name": "받는분",
            "phone": "전화",
            "address": "주소",
            "delivery_request": "배송요청",
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
        picking_sheet.to_excel(writer, index=False, sheet_name="피킹리스트")
        lozen_sheet.to_excel(writer, index=False, sheet_name="로젠택배")
        order_sheet.to_excel(writer, index=False, sheet_name="출고_주문")
        item_sheet.to_excel(writer, index=False, sheet_name="출고_품목")

        # Basic formatting for picking list
        ws = writer.book["피킹리스트"]
        # Print setup: A4 landscape, fit to page width
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
            # column widths
            if name == "No":
                ws.column_dimensions[ws.cell(1, col_idx).column_letter].width = 5
            elif name in ("받는분",):
                ws.column_dimensions[ws.cell(1, col_idx).column_letter].width = 12
            elif name in ("전화",):
                ws.column_dimensions[ws.cell(1, col_idx).column_letter].width = 14
            elif name == "주소":
                ws.column_dimensions[ws.cell(1, col_idx).column_letter].width = 40
            elif name == "배송요청":
                ws.column_dimensions[ws.cell(1, col_idx).column_letter].width = 24
            elif name == "특이사항":
                ws.column_dimensions[ws.cell(1, col_idx).column_letter].width = 18
            elif name == "품목":
                ws.column_dimensions[ws.cell(1, col_idx).column_letter].width = 52

        # Auto-fit-like row height for "품목" only (so full text is visible when opened/printed)
        item_col_idx = header_map.get("품목")
        item_col_width = 52  # must match column width above
        chars_per_line = max(10, int(item_col_width * 1.1))
        base_line_height = 15  # points

        for r in range(2, ws.max_row + 1):
            # default compact height; we'll expand only if 품목 needs it
            ws.row_dimensions[r].height = 24
            for c in range(1, ws.max_column + 1):
                cell = ws.cell(r, c)
                header = ws.cell(1, c).value
                if header in wrap_cols:
                    cell.alignment = Alignment(vertical="top", wrap_text=True)
                else:
                    cell.alignment = Alignment(vertical="top")
                # subtle zebra striping for print readability
                if r % 2 == 0:
                    cell.fill = PatternFill("solid", fgColor="FAFAFA")
                cell.border = thin_border

            if item_col_idx:
                v = ws.cell(r, item_col_idx).value
                text = "" if v is None else str(v)
                # estimate wrapped line count
                raw_lines = text.splitlines() if text else [""]
                line_count = 0
                for ln in raw_lines:
                    ln_len = len(ln)
                    line_count += max(1, (ln_len + chars_per_line - 1) // chars_per_line)
                # add a bit of padding
                ws.row_dimensions[r].height = max(24, min(300, base_line_height * line_count + 12))

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
        wrap_cols2 = {"품목"}
        header_map2 = {ws2.cell(1, c).value: c for c in range(1, ws2.max_column + 1)}
        for name, col_idx in header_map2.items():
            letter = ws2.cell(1, col_idx).column_letter
            if name == "품목":
                ws2.column_dimensions[letter].width = 78
            elif name == "수량":
                ws2.column_dimensions[letter].width = 8

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


def main() -> None:
    st.set_page_config(page_title="Mutomo 판매관리", layout="wide")
    st.markdown("<h1 style='text-align:center; margin-bottom: 0.5rem;'>MUTOMO 판매</h1>", unsafe_allow_html=True)

    # Wider sidebar for order details
    st.markdown(
        """
<style>
  section[data-testid="stSidebar"] { width: 560px !important; }
  section[data-testid="stSidebar"] > div { width: 560px !important; }
</style>
        """,
        unsafe_allow_html=True,
    )

    # Left sidebar layout (original style)
    st.sidebar.header("검색")
    query = st.sidebar.text_input("이름(받는분) 검색", value="")
    ship_action = st.sidebar.button("선택 건 출고 처리", type="primary", key="ship_one_button")
    c1, c2, c3, c4 = st.sidebar.columns(4)
    claim_action = c1.button("클레임", key="status_claim")
    back_to_received_action = c2.button("접수", key="status_received")
    close_action = c3.button("마감", key="status_close")
    cancel_action = c4.button("납품취소", key="status_cancel")

    def _update_special_issue(order_ids: list[str], text: str) -> None:
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

    # Track which selector is currently "active" so we don't show both.
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

    # Settings in sidebar (collapsed)
    with st.sidebar.expander("설정", expanded=False):
        db_path = st.text_input("DB 경로", value="mutomo.sqlite")
        status_filter = st.multiselect(
            "상태 필터",
            options=["접수", "클레임", "출고", "마감", "납품취소"],
            default=["접수", "출고"],
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
        st.error(db_msg)
        st.markdown(
            "아래 명령으로 `orders` / `items` 테이블을 만듭니다. 기본으로 **`order_list` 폴더** 안의 `.xlsx`만 읽습니다. "
            "그 폴더에 파일이 없으면 **빈 테이블만** 만들어져 대시보드가 열리고, 엑셀을 넣고 같은 명령을 다시 실행하면 데이터가 채워집니다.\n\n"
            "```bash\n"
            "python ingest_xlsx.py --db mutomo.sqlite --aliases product_aliases.yml\n"
            "```\n\n"
            "다른 위치에만 두고 싶으면 `--input-dir \"D:\\경로\"` 처럼 지정하면 됩니다. 다른 이름의 DB를 쓰는 경우 **설정**의 DB 경로를 그 파일로 맞추세요."
        )
        st.stop()

    _mt, _sz = _db_stat_for_cache(db_path)
    orders_all, items_all = load_tables(db_path, _mt, _sz)

    # Ensure shipped_at column exists (older DBs)
    con = sqlite3.connect(db_path)
    try:
        cur = con.cursor()
        cur.execute("PRAGMA table_info(orders)")
        cols = {row[1] for row in cur.fetchall()}
        if "shipped_at" not in cols:
            cur.execute("ALTER TABLE orders ADD COLUMN shipped_at TEXT")
            con.commit()
    finally:
        con.close()

    with st.sidebar.expander("출고 목록 엑셀", expanded=False):
        export_version = "v4_lozen_sheet"
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

    # Right-side summary metrics
    s = orders_all["status"] if "status" in orders_all.columns else pd.Series([], dtype=str)
    total_cnt = len(orders_all)
    planned_cnt = int((s == "접수").sum()) if len(s) else 0
    done_cnt = int((s == "출고").sum()) if len(s) else 0
    claim_cnt = int((s == "클레임").sum()) if len(s) else 0
    cancel_cnt = int((s == "납품취소").sum()) if len(s) else 0

    left, m1, m2, m3, m4, m5 = st.columns([2, 1, 1, 1, 1, 1])
    with left:
        # keep single metric; details are shown below in 4-column view
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

    # Search results & actions (in left column)
    if query.strip():
        q = query.strip()
        hits = orders[orders.get("receiver_name", "").astype(str).str.contains(q, case=False, na=False)].copy()
        st.sidebar.caption(f"검색 결과: {len(hits)}건")
        if len(hits):
            # If something (like table selection) requested clearing the widget,
            # do it before instantiating the multiselect.
            if st.session_state.get("request_clear_search_pick_labels") and "search_pick_labels" in st.session_state:
                st.session_state["search_pick_labels"] = []
                st.session_state["request_clear_search_pick_labels"] = False

            # Show order_list in the picker, but keep stable order_id for updates.
            def _label(row: pd.Series) -> str:
                purchase = str(row.get("purchase_date") or "").strip()
                name = str(row.get("receiver_name") or "").strip()
                status = str(row.get("status") or "").strip()
                status_tag = ""
                if status == "출고":
                    status_tag = "🚚✅[출고완료] "
                elif status == "클레임":
                    status_tag = "⚠️[클레임] "
                elif status == "접수":
                    status_tag = "📝⏳[접수] "
                order_list = str(row.get("order_list") or "").strip().replace("\n", " / ")
                order_list = (order_list[:120] + "…") if len(order_list) > 121 else order_list
                return f"{status_tag}{purchase} | {name} | {order_list}".strip(" |")

            rows = hits.copy()
            rows["_label"] = rows.apply(_label, axis=1)
            # Ensure uniqueness of labels
            dup = rows["_label"].duplicated(keep=False)
            if dup.any():
                rows.loc[dup, "_label"] = rows.loc[dup].apply(lambda r: f"{r['_label']} ({r['order_id']})", axis=1)

            label_to_id = dict(zip(rows["_label"].tolist(), rows["order_id"].tolist(), strict=False))
            options = rows["_label"].tolist()
            default = options[:1]

            picked_labels = st.sidebar.multiselect(
                "출고 처리할 주문 선택",
                options=options,
                default=default,
                key="search_pick_labels",
            )
            pick_ids = [label_to_id[lbl] for lbl in picked_labels if lbl in label_to_id]
            st.session_state["search_pick_ids"] = pick_ids

            # Detect change: if search selection changed, make it the active selector.
            if pick_ids != st.session_state.get("prev_search_pick_ids"):
                st.session_state["prev_search_pick_ids"] = pick_ids
                if pick_ids:
                    st.session_state["active_selector"] = "search"
                    st.session_state["selected_ids_from_table"] = []
                    st.session_state["prev_table_pick_ids"] = []

            if pick_ids and st.session_state.get("active_selector") == "search":
                st.sidebar.subheader("선택한 주문 상세")
                picked_rows = hits[hits["order_id"].isin(pick_ids)].copy()
                sort_cols_pick = [c for c in ["purchase_date", "receiver_name"] if c in picked_rows.columns]
                if sort_cols_pick:
                    picked_rows = picked_rows.sort_values(sort_cols_pick, na_position="last")

                for _, r in picked_rows.iterrows():
                    _render_order_detail(st.sidebar, r, items)
                    st.sidebar.divider()

            if ship_action and pick_ids:
                con = sqlite3.connect(db_path)
                try:
                    cur = con.cursor()
                    now = dt.datetime.now().isoformat(timespec="seconds")
                    cur.executemany(
                        "UPDATE orders SET status=?, shipped_at=COALESCE(shipped_at, ?) WHERE order_id=?",
                        [("출고", now, oid) for oid in pick_ids],
                    )
                    con.commit()
                finally:
                    con.close()
                st.sidebar.success("출고 처리했습니다.")
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
                st.sidebar.success("클레임으로 변경했습니다.")
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
                st.sidebar.success("출고 취소(접수로 변경 + 출고시간 초기화) 했습니다.")
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
                st.sidebar.success("마감으로 변경했습니다.")
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
                st.sidebar.success("납품취소로 변경했습니다.")
                st.cache_data.clear()
                st.rerun()
        else:
            st.sidebar.info("일치하는 이름이 없습니다.")
    else:
        # Clear search selection when query is empty
        st.session_state["search_pick_ids"] = []
        st.session_state["prev_search_pick_ids"] = []
        if "search_pick_labels" in st.session_state:
            st.session_state["search_pick_labels"] = []

    st.subheader("오늘접수")
    st.caption("오늘/1일전/2일전/3일전 (날짜 + 이름만)")

    # Use 접수 상태만 집계/표시 (출고/클레임 등은 전체접수 목록에서 확인)
    recent_base = orders.copy()
    if "status" in recent_base.columns:
        recent_base = recent_base[recent_base["status"] == "접수"]

    def _day_panel(col, day: dt.date) -> None:
        df = recent_base[recent_base["_date"] == day].copy()
        df = df[[c for c in ["purchase_date", "receiver_name"] if c in df.columns]]
        if "purchase_date" not in df.columns:
            df["purchase_date"] = day.isoformat()
        if "receiver_name" not in df.columns:
            df["receiver_name"] = ""
        df = df.rename(columns={"purchase_date": "날짜", "receiver_name": "이름"})
        df = df.sort_values(["이름"], na_position="last")
        col.markdown(f"**{day.isoformat()}**")
        col.dataframe(df, use_container_width=True, hide_index=True)

    c1, c2, c3, c4 = st.columns(4)
    _day_panel(c1, today)
    _day_panel(c2, today - dt.timedelta(days=1))
    _day_panel(c3, today - dt.timedelta(days=2))
    _day_panel(c4, today - dt.timedelta(days=3))

    st.subheader("전체 접수 목록")
    user_cols = [
        "purchase_date",
        "receiver_name",
        "order_list",
        "address",
        "phone",
        "delivery_request",
        "special_issue",
        "status",
    ]
    admin_cols = [
        "order_id",
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

    # Add status emoji after receiver name for quick scanning
    if "receiver_name" in view_orders.columns and "status" in sorted_orders.columns:
        def _name_emoji(row) -> str:
            name = "" if row.get("receiver_name") is None else str(row.get("receiver_name")).strip()
            stt = "" if row.get("status") is None else str(row.get("status")).strip()
            emoji = ""
            if stt == "출고":
                emoji = " 🚚✅"
            elif stt == "클레임":
                emoji = " ⚠️"
            elif stt == "마감":
                emoji = " 🧾"
            elif stt == "납품취소":
                emoji = " ⛔"
            return (name + emoji).strip()

        # Use sorted_orders for original status values
        tmp = sorted_orders.loc[view_orders.index, ["receiver_name", "status"]].copy()
        view_orders["receiver_name"] = tmp.apply(_name_emoji, axis=1)

    st.caption("행을 선택하면 왼쪽 사이드바에 주문상세가 표시됩니다.")
    selected_ids: list[str] = []
    if "order_id" in sorted_orders.columns:
        try:
            # Streamlit row selection (supported in recent versions)
            state = st.dataframe(
                view_orders,
                use_container_width=True,
                hide_index=True,
                on_select="rerun",
                selection_mode="multi-row",
            )
            if state is not None and hasattr(state, "selection"):
                rows = getattr(state.selection, "rows", []) or []
                # Map visible row indices -> order_id from sorted_orders
                selected_ids = sorted_orders.iloc[rows]["order_id"].astype(str).tolist()
        except TypeError:
            # Older Streamlit: no selection support, just show the table.
            st.dataframe(view_orders, use_container_width=True, hide_index=True)
    else:
        st.dataframe(view_orders, use_container_width=True, hide_index=True)

    # Mirror "name search" behavior: show selected rows in sidebar
    # Persist selection so button clicks don't lose it on rerun
    if selected_ids:
        if selected_ids != st.session_state.get("prev_table_pick_ids"):
            st.session_state["prev_table_pick_ids"] = selected_ids
            st.session_state["active_selector"] = "table"
            st.session_state["selected_ids_from_table"] = selected_ids
            # Clear search selection (including widget state) if table was used
            st.session_state["search_pick_ids"] = []
            st.session_state["prev_search_pick_ids"] = []
            st.session_state["request_clear_search_pick_labels"] = True

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
            _update_special_issue(active_ids, issue_text.strip())
            st.sidebar.success("저장했습니다.")
            st.cache_data.clear()
            st.rerun()

    # Single ship button behavior: if the active selector is table, ship those ids.
    if ship_action and st.session_state.get("active_selector") == "table":
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

    if claim_action and st.session_state.get("active_selector") == "table":
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

    if back_to_received_action and st.session_state.get("active_selector") == "table":
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

    if close_action and st.session_state.get("active_selector") == "table":
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

    if cancel_action and st.session_state.get("active_selector") == "table":
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

