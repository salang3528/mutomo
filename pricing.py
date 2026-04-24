"""단가표.csv — 엑셀 상품명 기준 단가. `ingest_xlsx._product_key`와 동일 키로 품목과 매칭."""

from __future__ import annotations

import os
import re
from typing import Any

import pandas as pd

from ingest_xlsx import _product_key

DEFAULT_FILENAME = "단가표.csv"


def _repo_path(filename: str = DEFAULT_FILENAME) -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)


def load_unit_prices(path: str | None = None) -> tuple[dict[str, dict[str, Any]], tuple[str, ...]]:
    """Return (map: product_key -> row dict, warnings). Row dict keys: 엑셀상품명, 판매가격, 광진가격(60%)."""
    p = path or _repo_path()
    warns: list[str] = []
    if not os.path.isfile(p):
        return {}, (f"단가표 없음: {p}",)

    df = pd.read_csv(p, encoding="utf-8-sig")
    if df.empty:
        return {}, ("단가표가 비어 있습니다.",)

    # tolerate minor header variants
    col_map = {str(c).strip(): c for c in df.columns}
    name_col = col_map.get("엑셀상품명")
    sale_col = col_map.get("판매가격")
    gj_col = col_map.get("광진가격(60%)")
    if not name_col or not sale_col:
        return {}, (f"단가표 필수 열 누락: 엑셀상품명·판매가격 — 실제 열: {list(df.columns)}",)

    out: dict[str, dict[str, Any]] = {}
    for _, row in df.iterrows():
        name = str(row.get(name_col) or "").strip()
        if not name or name.lower() == "nan":
            continue
        k = _product_key(name)
        if not k:
            continue
        if k in out:
            warns.append(f"동일 키 '{k}': '{out[k]['엑셀상품명']}' ← '{name}'(뒤쪽 행 사용)")
        rec: dict[str, Any] = {
            "엑셀상품명": name,
            "판매가격": row.get(sale_col),
        }
        if gj_col:
            rec["광진가격(60%)"] = row.get(gj_col)
        out[k] = rec

    warns_cut = warns[:20]
    tup: tuple[str, ...] = tuple(warns_cut)
    if len(warns) > 20:
        tup = tup + ("… 외 중복 다수",)
    return out, tup


def lookup_line_price(item_row: pd.Series, price_map: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    """Match `items` 한 줄 against 단가표 (product_raw → product_canonical 순)."""
    if not price_map:
        return None
    for col in ("product_raw", "product_canonical"):
        v = item_row.get(col)
        if v is None:
            continue
        try:
            if pd.isna(v):
                continue
        except Exception:
            pass
        s = str(v).strip()
        if not s:
            continue
        hit = price_map.get(_product_key(s))
        if hit:
            return hit
    return None


_WON_SURCHARGE_RE = re.compile(r"\(\s*\+?\s*(?P<num>[\d,]+)\s*원\s*\)")


def option_surcharge_won(item_row: pd.Series) -> int:
    """품목 옵션 텍스트에 포함된 '(+16000원)' 같은 추가금(원)을 추출.

    예: '43cm (+16000원)' → 16000
    여러 개면 합산.
    """
    parts: list[str] = []
    for c in ("spec_raw", "note_raw", "product_raw", "product_canonical"):
        v = item_row.get(c)
        if v is None:
            continue
        try:
            if pd.isna(v):
                continue
        except Exception:
            pass
        s = str(v).strip()
        if s:
            parts.append(s)
    if not parts:
        return 0
    text = "\n".join(parts)
    total = 0
    for m in _WON_SURCHARGE_RE.finditer(text):
        raw = (m.group("num") or "").replace(",", "").strip()
        if not raw:
            continue
        try:
            total += int(raw)
        except ValueError:
            continue
    return int(total)


def line_unit_prices(item_row: pd.Series, price_map: dict[str, dict[str, Any]]) -> tuple[float, float] | None:
    """단가표 기준 1개 단가(판매/광진). 옵션 추가금(+원)이 있으면 판매는 그대로 더하고,
    광진은 60%로 환산해서 더한다(광진열이 없으면 0).
    """
    pr = lookup_line_price(item_row, price_map) if price_map else None
    if pr is None:
        return None
    try:
        sale = float(pr.get("판매가격") or 0.0)
    except (TypeError, ValueError):
        sale = 0.0
    gj_v = pr.get("광진가격(60%)")
    try:
        gj = float(gj_v) if gj_v is not None and not (isinstance(gj_v, float) and pd.isna(gj_v)) else 0.0
    except (TypeError, ValueError):
        gj = 0.0

    add = option_surcharge_won(item_row)
    if add:
        sale += float(add)
        # 광진금액은 기본 60% 정책을 따르되, 반올림해서 원 단위로 맞춘다.
        gj += float(int(round(float(add) * 0.6)))
    return float(sale), float(gj)


def format_won(v: object) -> str:
    if v is None:
        return "—"
    try:
        if isinstance(v, float) and pd.isna(v):
            return "—"
    except Exception:
        pass
    try:
        n = float(v)
    except (TypeError, ValueError):
        return str(v)
    if abs(n - round(n)) < 0.5:
        return f"{int(round(n)):,}원"
    return f"{n:,.0f}원"
