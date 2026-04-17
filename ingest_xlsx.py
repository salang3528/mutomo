from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
from collections import Counter
import os
import re
import sqlite3
from typing import Any, Iterable

import openpyxl
import pandas as pd
import yaml
from rapidfuzz import fuzz, process


EXPECTED_COLS = 13  # current vendor sheet has 13 columns (incl. address/phone/request)

FILENAME_DATE_RE = re.compile(r"^(?P<yymmdd>\d{6})")
SIZE_RE = re.compile(r"\bW\s*(\d{2,5})\s*[*xX]\s*D\s*(\d{2,5})(?:\s*[*xX]\s*H\s*(\d{2,5}))?\b")
CM_RE = re.compile(r"\b(\d{2,3})\s*cm\b", re.IGNORECASE)

COLOR_WORDS = [
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
]


@dataclasses.dataclass(frozen=True)
class ItemRow:
    source_file: str
    row_idx: int
    deadline_raw: str | None
    group_no: str | None
    group_start_row: int | None
    receiver_name_raw: str | None
    order_date_raw: str | None
    product_raw: str | None
    spec_raw: str | None
    shelf_color_raw: str | None
    leg_color_raw: str | None
    qty_raw: str | None
    ship_raw: str | None
    address_raw: str | None
    phone_raw: str | None
    delivery_request_raw: str | None
    attention_note_raw: str | None


def purchase_date_from_filename(source_file: str) -> str | None:
    """
    Extract purchase date from file prefix like: 260401-02_....xlsx -> 2026-04-01
    """
    m = FILENAME_DATE_RE.match(source_file)
    if not m:
        return None
    yymmdd = m.group("yymmdd")
    try:
        yy = int(yymmdd[0:2])
        mm = int(yymmdd[2:4])
        dd = int(yymmdd[4:6])
        yyyy = 2000 + yy
        return dt.date(yyyy, mm, dd).isoformat()
    except Exception:
        return None


def extract_size(text: str | None) -> str | None:
    if not text:
        return None
    t = text.replace("\n", " ")
    m = SIZE_RE.search(t)
    if m:
        w, d, h = m.group(1), m.group(2), m.group(3)
        return f"W{w}*D{d}" + (f"*H{h}" if h else "")
    m2 = CM_RE.search(t)
    if m2:
        return f"{m2.group(1)}cm"
    return None


def _first_color(text: str | None) -> str | None:
    if not text:
        return None
    t = text.replace("\n", " ")
    for c in COLOR_WORDS:
        if c in t:
            return c
    return None


def extract_leg_color(text: str | None) -> str | None:
    if not text:
        return None
    t = text.replace("\n", " ")
    # try "다리: 블랙" / "다리 블랙" patterns first
    m = re.search(r"다리\s*[:\-]?\s*([가-힣]+)", t)
    if m:
        cand = m.group(1)
        for c in COLOR_WORDS:
            if c in cand:
                return c
    return None


def _to_str(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return s if s else None
    return str(v).strip() or None


def _looks_like_int(s: str | None) -> bool:
    if not s:
        return False
    return bool(re.fullmatch(r"\d+", s.strip()))


def _parse_excel(path: str) -> list[ItemRow]:
    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb[wb.sheetnames[0]]

    # Heuristic: header is first row with many non-empty cells.
    header_row = None
    for r in range(1, 21):
        vals = [_to_str(ws.cell(r, c).value) for c in range(1, EXPECTED_COLS + 1)]
        nonempty = sum(1 for v in vals if v)
        if nonempty >= 7:
            header_row = r
            break
    if header_row is None:
        header_row = 1

    rows: list[ItemRow] = []
    current_deadline = None
    current_group_no = None
    current_group_start_row: int | None = None
    current_receiver_name = None
    current_order_date = None
    current_address = None
    current_phone = None
    current_delivery_request = None
    current_attention_note = None

    # Some order sheets contain an "instruction/note" in column A (e.g. A2/A9)
    # like: "(3/28 주문 서혜선님 제품변경, 컬러지정하였습니다. ...)"
    # We capture these and attach them to the matching receiver as an attention note.
    pending_note_by_name: dict[str, str] = {}
    pending_unassigned_notes: list[str] = []

    def _extract_name_note(s: str | None) -> tuple[str | None, str | None]:
        if not s:
            return None, None
        s2 = str(s).strip()
        if not s2:
            return None, None
        # Typical keywords that appear in these attention notes.
        # Helps avoid false positives when scanning a lot of rows.
        if not re.search(r"(변경|컬러|색상|추가|수정|주소|옵션|취소|반품|교환)", s2):
            return None, None

        # Many sheets start these lines with a date like "3/28 ..."
        # Use it as an extra safety signal when order keywords are absent.
        has_order_kw = bool(re.search(r"(주문|주문자)", s2))
        has_date_prefix = bool(re.match(r"^\s*\d{1,2}\s*/\s*\d{1,2}", s2))
        if not has_order_kw and not has_date_prefix:
            # Allow lines like: "장재국 색상지정 ..." (no date/order keyword)
            m0 = re.search(r"([가-힣]{2,10}).*(?:색상|컬러|색깔).*(?:지정)", s2)
            if m0:
                nm0 = (m0.group(1) or "").strip()
                return (nm0 or None), s2
            return None, None

        # Primary: "주문 홍길동님" or "주문 홍길동 ..."
        m = re.search(r"(?:주문|주문자)\s*([가-힣]{2,10})(?:\s*님)?", s2)
        if m:
            name = (m.group(1) or "").strip()
            if name:
                return name, s2

        # Fallback: if it contains "님", grab the nearest Hangul name before it.
        m2 = re.search(r"([가-힣]{2,10})\s*님", s2)
        if m2:
            name = (m2.group(1) or "").strip()
            if name:
                return name, s2

        # Keep as unassigned note (we may still match by inclusion later)
        return None, s2

    def _merge_req(existing: str | None, extra: str | None) -> str | None:
        ex = (existing or "").strip()
        ex2 = (extra or "").strip()
        if not ex and not ex2:
            return None
        if not ex:
            return ex2
        if not ex2:
            return ex
        if ex2 in ex:
            return ex
        # Keep as a single line for UI scanning
        return f"{ex} / {ex2}"

    # Scan A-column notes across the sheet (some files put them far below, e.g. A106).
    # Keep it bounded to avoid pathological sheets.
    scan_end = min(ws.max_row, header_row + 500)
    for r in range(header_row + 1, scan_end + 1):
        note = _to_str(ws.cell(r, 1).value)
        nm, txt = _extract_name_note(note)
        if nm and txt:
            pending_note_by_name[nm] = _merge_req(pending_note_by_name.get(nm), txt) or txt
        elif txt:
            # Keep as a fallback: later we can match it by inclusion against receiver_name.
            pending_unassigned_notes.append(txt)

    def _pick_note_for_receiver(receiver: str | None) -> str | None:
        if not receiver:
            return None
        rn = str(receiver).strip()
        if not rn:
            return None
        # Exact match first
        if rn in pending_note_by_name:
            return pending_note_by_name[rn]
        # Fallback: containment match (handles cases like "서혜선(네이버)" vs "서혜선")
        for k, v in pending_note_by_name.items():
            kk = (k or "").strip()
            if not kk:
                continue
            if kk in rn or rn in kk:
                return v
        # Final fallback: match unassigned note text that contains the receiver name.
        rn2 = re.sub(r"\s+", "", rn)
        for txt in pending_unassigned_notes:
            t2 = re.sub(r"\s+", "", txt or "")
            if rn2 and t2 and (rn2 in t2):
                return txt
        return None

    blank_run = 0
    for r in range(header_row + 1, ws.max_row + 1):
        vals = [_to_str(ws.cell(r, c).value) for c in range(1, EXPECTED_COLS + 1)]
        if all(v is None for v in vals):
            blank_run += 1
            if blank_run >= 10:
                break
            continue
        blank_run = 0

        (
            deadline,
            group_no,
            receiver_name,
            order_date,
            product,
            spec,
            shelf_color,
            leg_color,
            qty,
            ship,
            address,
            phone,
            delivery_request,
        ) = vals

        # Rows where "번호" is a digit: may be a new order block, or the same order with the
        # number repeated on every line. Only advance group_start_row when the block actually
        # changes (new 번호, or same 번호 but a new 받는분), so one customer does not split into
        # multiple order_id values.
        if _looks_like_int(group_no):

            def _norm_group_digits(s: str | None) -> str | None:
                if not s or not _looks_like_int(s):
                    return None
                return str(int(str(s).strip()))

            new_gn = _norm_group_digits(group_no)
            old_gn = _norm_group_digits(str(current_group_no)) if current_group_no is not None else None
            recv_cell = _to_str(receiver_name)
            prev_recv = _to_str(current_receiver_name)

            advance_start = old_gn is None or new_gn != old_gn
            if not advance_start and recv_cell and prev_recv and recv_cell.strip() != prev_recv.strip():
                advance_start = True
            if not advance_start and recv_cell and not prev_recv:
                advance_start = True

            if advance_start:
                current_group_start_row = r

            current_deadline = deadline or current_deadline
            current_group_no = group_no
            current_receiver_name = receiver_name or current_receiver_name
            current_order_date = order_date or current_order_date
            current_address = address or current_address
            current_phone = phone or current_phone
            # delivery_request is order-specific; DO NOT carry over from previous groups.
            if advance_start:
                current_delivery_request = delivery_request
            else:
                dr = _to_str(delivery_request)
                if dr:
                    current_delivery_request = delivery_request

            # Attach any pending A-column note that matches this receiver name.
            current_attention_note = None
            extra = _pick_note_for_receiver(current_receiver_name)
            if extra:
                current_attention_note = extra

        # Item rows: product exists OR spec/option exists AND we have an active group.
        is_itemish = bool(product or spec or shelf_color or leg_color) and bool(current_group_no)
        if not is_itemish:
            continue

        rows.append(
            ItemRow(
                source_file=os.path.basename(path),
                row_idx=r,
                deadline_raw=deadline or current_deadline,
                group_no=current_group_no,
                group_start_row=current_group_start_row,
                receiver_name_raw=receiver_name or current_receiver_name,
                order_date_raw=order_date or current_order_date,
                product_raw=product,
                spec_raw=spec,
                shelf_color_raw=shelf_color,
                leg_color_raw=leg_color,
                qty_raw=qty,
                ship_raw=ship,
                address_raw=address or current_address,
                phone_raw=phone or current_phone,
                delivery_request_raw=delivery_request or current_delivery_request,
                attention_note_raw=current_attention_note,
            )
        )

    return rows


def _normalize_text(s: str) -> str:
    s = s.strip().lower()
    s = s.replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s)
    return s


def _product_key(s: str) -> str:
    s = _normalize_text(s)
    # remove common option markers but keep meaningful words/numbers
    s = s.replace("\\n", " ")
    s = re.sub(r"\([^)]*\)", " ", s)  # remove (...) blocks like (L)
    s = re.sub(r"\[[^]]*\]", " ", s)  # remove [...] blocks
    s = re.sub(r"\s+", " ", s).strip()
    return s


def load_alias_map(path: str) -> tuple[dict[str, str], list[str]]:
    """
    Returns:
      - alias_key -> canonical
      - list of canonicals
    """
    if not os.path.exists(path):
        return {}, []
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    alias_to_canon: dict[str, str] = {}
    canonicals: list[str] = []
    for canon, aliases in raw.items():
        canon = (canon or "").strip()
        if not canon:
            continue
        canonicals.append(canon)
        alias_to_canon[_product_key(canon)] = canon
        for a in (aliases or []):
            a = (a or "").strip()
            if not a:
                continue
            alias_to_canon[_product_key(a)] = canon
    return alias_to_canon, sorted(set(canonicals))


def resolve_product(
    product_raw: str | None,
    alias_to_canon: dict[str, str],
    canonicals: list[str],
) -> tuple[str | None, str | None, float | None]:
    if not product_raw:
        return None, None, None
    k = _product_key(product_raw)
    if not k:
        return None, None, None
    if k in alias_to_canon:
        return alias_to_canon[k], None, 100.0

    # Fuzzy suggestion against canonical list (by normalized key)
    if not canonicals:
        return None, None, None
    canon_keys = {_product_key(c): c for c in canonicals}
    hit = process.extractOne(
        k,
        list(canon_keys.keys()),
        scorer=fuzz.WRatio,
    )
    if not hit:
        return None, None, None
    best_key, score, _idx = hit
    return None, canon_keys.get(best_key), float(score)


def _to_int(s: str | None) -> int | None:
    if not s:
        return None
    s2 = re.sub(r"[^\d]", "", s)
    return int(s2) if s2 else None


# 엑셀 품목 `ship_raw` → 직접/택배 판별(주문 다수결에 사용).
SETTLEMENT_SHIP_VALUES: tuple[str, ...] = ("직접배송", "택배")


def classify_ship_raw(raw: object) -> str | None:
    """한 줄의 ship_raw를 직접배송/택배로 분류. 알 수 없으면 None."""
    if raw is None:
        return None
    try:
        if pd.isna(raw):
            return None
    except Exception:
        pass
    s = _normalize_text(str(raw))
    if not s:
        return None
    # 엑셀에 '직접' / '택배'만 적는 경우(가장 흔함)
    compact = s.replace(" ", "")
    if compact in ("직접", "직접."):
        return "직접배송"
    if compact in ("택배", "택배."):
        return "택배"
    direct_markers = (
        "직접배송",
        "직접 배송",
        "직접배",
        "직배",
        "직송",
        "직배송",
        "직배차",
        "방문수령",
        "방문 수령",
        "매장수령",
        "매장 수령",
        "직거래",
        "자체배송",
        "자체 배송",
        "자체픽업",
        "고객방문",
        "방문픽업",
        "방문 배송",
        "방문배송",
        "당사배송",
        "당사 배송",
        "지게차",
        "사다리차",
    )
    if any(k in s for k in direct_markers):
        return "직접배송"
    if any(k in s for k in ("택배", "로젠", "parcel", "courier")):
        return "택배"
    return None


def infer_settlement_ship_series(items_df: pd.DataFrame) -> pd.Series:
    """order_id -> '직접배송' | '택배' from 엑셀 품목 ship_raw (다수결; 동수·미기재는 택배)."""
    if items_df is None or len(items_df) == 0 or "order_id" not in items_df.columns:
        return pd.Series(dtype=str)
    tmp = items_df[["order_id", "ship_raw"]].copy()
    tmp["_kind"] = tmp["ship_raw"].map(classify_ship_raw)

    def _vote(series: pd.Series) -> str:
        vals = [v for v in series.dropna().tolist() if v in SETTLEMENT_SHIP_VALUES]
        if not vals:
            return "택배"
        c = Counter(vals)
        best_n = c.most_common(1)[0][1]
        tops = [k for k, v in c.items() if v == best_n]
        if len(tops) == 1:
            return tops[0]
        return "택배"

    out = tmp.groupby("order_id", sort=False)["_kind"].agg(_vote)
    # SQLite 등에서 order_id dtype이 섞이면 대시보드 map 시 전부 택배로 떨어질 수 있어 인덱스를 문자열로 고정
    out.index = out.index.astype(str)
    return out


def build_frames(rows: Iterable[ItemRow], alias_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    alias_to_canon, canonicals = load_alias_map(alias_path)
    order_rows: dict[str, dict[str, Any]] = {}
    item_out: list[dict[str, Any]] = []
    unknown_products: dict[str, int] = {}

    for it in rows:
        # group_no can repeat inside the same sheet; include group_start_row to disambiguate.
        start = it.group_start_row if it.group_start_row is not None else it.row_idx
        order_id = f"{it.source_file}#{it.group_no}@{start}"
        if order_id not in order_rows:
            order_rows[order_id] = {
                "order_id": order_id,
                "source_file": it.source_file,
                "group_no": it.group_no,
                "group_start_row": it.group_start_row,
                "deadline_raw": it.deadline_raw,
                "order_date_raw": it.order_date_raw,
                # 구매일자: 파일명에서 추출 (예: 260401-.. -> 2026-04-01)
                "purchase_date": purchase_date_from_filename(it.source_file),
                # 주문자(받는사람) / 배송 정보: 이 엑셀(3열, 11~13열)에서 추출
                "receiver_name": it.receiver_name_raw,
                "address": it.address_raw,
                "phone": it.phone_raw,
                "delivery_request": it.delivery_request_raw,
                # 엑셀 A열 특이사항(주문 ○○님 ...) 자동 반영
                "attention_note": it.attention_note_raw,
                # 주문목록(요약): items에서 생성
                "order_list": None,
                # 특이사항(리콜/클레임 등) - 수기 입력용
                "special_issue": None,
                "status": "접수",
                # 출고 처리 시각(대시보드에서 설정). 재수집 시 보존.
                "shipped_at": None,
                "created_at": dt.datetime.now().isoformat(timespec="seconds"),
            }
        else:
            # Fill missing order-level fields if later rows contain them
            o = order_rows[order_id]
            for k, v in [
                ("receiver_name", it.receiver_name_raw),
                ("address", it.address_raw),
                ("phone", it.phone_raw),
                ("delivery_request", it.delivery_request_raw),
                ("attention_note", it.attention_note_raw),
            ]:
                if (o.get(k) is None or str(o.get(k)).strip() == "") and v:
                    o[k] = v

        canon, suggestion, score = resolve_product(it.product_raw, alias_to_canon, canonicals)
        if canon is None and it.product_raw:
            unknown_products[_product_key(it.product_raw)] = unknown_products.get(_product_key(it.product_raw), 0) + 1

        item_out.append(
            {
                "order_id": order_id,
                "source_file": it.source_file,
                "row_idx": it.row_idx,
                "product_raw": it.product_raw,
                "product_key": _product_key(it.product_raw or "") if it.product_raw else None,
                "product_canonical": canon,
                "suggested_canonical": suggestion,
                "suggestion_score": score,
                "spec_raw": it.spec_raw,
                "note_raw": it.leg_color_raw,
                "size": extract_size(it.spec_raw) or extract_size(it.leg_color_raw) or extract_size(it.product_raw),
                # Prefer explicit sheet columns when present; otherwise fallback to keyword extraction.
                "shelf_color": it.shelf_color_raw or _first_color(it.spec_raw) or _first_color(it.product_raw),
                "leg_color": extract_leg_color(it.leg_color_raw) or extract_leg_color(it.spec_raw) or extract_leg_color(it.product_raw),
                "qty": _to_int(it.qty_raw),
                "ship_raw": it.ship_raw,
            }
        )

    orders_df = pd.DataFrame(list(order_rows.values()))
    items_df = pd.DataFrame(item_out)

    # Build order_list summary from items
    if len(orders_df) and len(items_df):
        def _s(v: Any) -> str:
            if v is None:
                return ""
            try:
                # pandas NaN
                if pd.isna(v):
                    return ""
            except Exception:
                pass
            return str(v).strip()

        def _item_line(r: pd.Series) -> str:
            name = _s(r.get("product_canonical")) or _s(r.get("product_raw")) or _s(r.get("spec_raw")) or _s(r.get("note_raw"))
            spec = _s(r.get("spec_raw"))
            size = _s(r.get("size"))
            shelf = _s(r.get("shelf_color"))
            leg = _s(r.get("leg_color"))
            qty = r.get("qty")
            qty_s = "" if qty is None or (hasattr(pd, "isna") and pd.isna(qty)) else str(int(qty))
            parts = [
                name,
                f"규격:{spec}" if spec else "규격:",
                f"사이즈:{size}" if size else "사이즈:",
                f"책장색상:{shelf}" if shelf else "책장색상:",
                f"다리색상:{leg}" if leg else "다리색상:",
                f"개수:{qty_s}" if qty_s else "개수:",
            ]
            return " | ".join(parts).strip(" |")

        tmp = items_df.copy()
        tmp["_line"] = tmp.apply(_item_line, axis=1)
        summary = tmp.groupby("order_id")["_line"].apply(lambda xs: "\n".join([x for x in xs if x])).reset_index()
        # Avoid duplicate order_list column: replace it with computed summary.
        if "order_list" in orders_df.columns:
            orders_df = orders_df.drop(columns=["order_list"])
        orders_df = orders_df.merge(summary, on="order_id", how="left").rename(columns={"_line": "order_list"})

    # Write unknown product report as a side effect (easy for user to update aliases)
    if unknown_products:
        unk = (
            pd.DataFrame([{"product_key": k, "count": v} for k, v in unknown_products.items()])
            .sort_values(["count", "product_key"], ascending=[False, True])
            .reset_index(drop=True)
        )
        unk.to_csv("unknown_products.csv", index=False, encoding="utf-8-sig")

    # Empty inputs: keep stable SQLite schema (column names) for the dashboard.
    if len(orders_df) == 0:
        orders_df = pd.DataFrame(
            columns=[
                "order_id",
                "source_file",
                "group_no",
                "group_start_row",
                "deadline_raw",
                "order_date_raw",
                "purchase_date",
                "receiver_name",
                "address",
                "phone",
                "delivery_request",
                "order_list",
                "special_issue",
                "status",
                "shipped_at",
                "created_at",
            ]
        )
    else:
        orders_df = orders_df.sort_values(["source_file", "group_no"], na_position="last")

    if len(items_df) == 0:
        items_df = pd.DataFrame(
            columns=[
                "order_id",
                "source_file",
                "row_idx",
                "product_raw",
                "product_key",
                "product_canonical",
                "suggested_canonical",
                "suggestion_score",
                "spec_raw",
                "note_raw",
                "size",
                "shelf_color",
                "leg_color",
                "qty",
                "ship_raw",
            ]
        )
    else:
        items_df = items_df.sort_values(["source_file", "order_id", "row_idx"], na_position="last")

    return orders_df, items_df


ORDER_EDITABLE_COLS = {
    "purchase_date",
    "receiver_name",
    "address",
    "phone",
    "delivery_request",
    "order_list",
    "special_issue",
    "status",
    "shipped_at",
}


def _merge_orders_preserving_edits(existing: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
    """
    Keep user-entered fields from existing orders when present.
    Overwrite the "parsed" fields from incoming.
    """
    if existing is None or len(existing) == 0:
        return incoming
    if "order_id" not in existing.columns:
        return incoming

    ex = existing.copy()
    inc = incoming.copy()
    ex = ex.set_index("order_id", drop=False)
    inc = inc.set_index("order_id", drop=False)

    # Ensure editable columns exist in incoming so they can be preserved
    for col in ORDER_EDITABLE_COLS:
        if col not in inc.columns:
            inc[col] = None

    # Start from incoming, then fill editable cols from existing when existing has a value
    merged = inc.copy()
    for col in ORDER_EDITABLE_COLS:
        if col in ex.columns:
            merged[col] = merged[col].where(merged[col].notna(), ex[col])

    # Also keep created_at if it already existed (so "오늘 접수" 기준이 안정적)
    if "created_at" in ex.columns:
        merged["created_at"] = merged["created_at"].where(merged["created_at"].notna(), ex["created_at"])
        merged["created_at"] = merged["created_at"].where(~merged.index.isin(ex.index), ex["created_at"])

    # NOTE: Do not keep orders that disappeared from incoming.
    # Keeping them can create "ghost" orders when parsing rules/order_id change.

    merged = merged.reset_index(drop=True)
    return merged


def write_sqlite(db_path: str, orders_df: pd.DataFrame, items_df: pd.DataFrame) -> None:
    con = sqlite3.connect(db_path)
    try:
        # Preserve user-entered columns in orders table on re-ingest.
        try:
            existing_orders = pd.read_sql_query("select * from orders", con)
        except Exception:
            existing_orders = pd.DataFrame()

        merged_orders = _merge_orders_preserving_edits(existing_orders, orders_df)
        merged_orders.to_sql("orders", con, if_exists="replace", index=False)

        # Items are derived from xlsx; safe to replace.
        items_df.to_sql("items", con, if_exists="replace", index=False)
    finally:
        con.close()


def iter_xlsx_files(input_dir: str) -> list[str]:
    out: list[str] = []
    if not os.path.isdir(input_dir):
        return out
    for root, _dirs, files in os.walk(input_dir):
        for name in files:
            if name.startswith("~$"):
                continue
            if name.lower().endswith(".xlsx"):
                out.append(os.path.join(root, name))
    return sorted(out)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--input-dir",
        default="order_list",
        help=".xlsx 수집 폴더 (기본: 프로젝트 기준 order_list)",
    )
    ap.add_argument("--aliases", default="product_aliases.yml", help="yaml mapping of canonical -> aliases")
    ap.add_argument("--db", default="mutomo.sqlite", help="output sqlite database path")
    args = ap.parse_args()

    input_dir = os.path.normpath(args.input_dir)
    if not os.path.isdir(input_dir):
        if input_dir == os.path.normpath("order_list"):
            os.makedirs(input_dir, exist_ok=True)
        else:
            raise SystemExit(
                "입력 폴더가 없습니다: "
                + input_dir
                + "\n경로를 확인하거나, order_list 폴더를 만든 뒤 .xlsx를 넣으세요."
            )

    paths = iter_xlsx_files(input_dir)
    if not paths:
        print(f"No .xlsx files found in: {input_dir} - writing empty orders/items tables.")
        orders_df, items_df = build_frames([], args.aliases)
        write_sqlite(args.db, orders_df, items_df)
        print(f"Wrote {len(orders_df)} orders and {len(items_df)} items to {args.db}")
        return

    all_rows: list[ItemRow] = []
    for p in paths:
        all_rows.extend(_parse_excel(p))

    orders_df, items_df = build_frames(all_rows, args.aliases)
    write_sqlite(args.db, orders_df, items_df)
    print(f"Wrote {len(orders_df)} orders and {len(items_df)} items to {args.db}")
    if os.path.exists("unknown_products.csv"):
        print("Also wrote unknown product keys to unknown_products.csv")


if __name__ == "__main__":
    main()

