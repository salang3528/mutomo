from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
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

        # Group header rows typically contain group_no (a number).
        if _looks_like_int(group_no):
            current_deadline = deadline or current_deadline
            current_group_no = group_no
            current_group_start_row = r
            current_receiver_name = receiver_name or current_receiver_name
            current_order_date = order_date or current_order_date
            current_address = address or current_address
            current_phone = phone or current_phone
            current_delivery_request = delivery_request or current_delivery_request

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
    for name in os.listdir(input_dir):
        if name.startswith("~$"):
            continue
        if name.lower().endswith(".xlsx"):
            out.append(os.path.join(input_dir, name))
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

