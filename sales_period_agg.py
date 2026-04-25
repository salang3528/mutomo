"""기간·상태 기준 주문 건수, 품목 수량, 단가표 기준 금액 집계 (UI 비의존)."""

from __future__ import annotations

import datetime as dt
from typing import Any

import pandas as pd

from ingest_xlsx import _product_key as _row_product_key

from pricing import lookup_line_price


def _to_date_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").dt.date


def order_basis_dates(orders: pd.DataFrame, basis: str) -> pd.Series:
    """basis: purchase | created | shipped — 주문 행마다 기준일(없으면 NaT)."""
    if orders is None or len(orders) == 0:
        return pd.Series(dtype="object")
    if basis == "purchase":
        if "purchase_date" not in orders.columns:
            return pd.Series([pd.NaT] * len(orders), index=orders.index)
        return _to_date_series(orders["purchase_date"])
    if basis == "created":
        if "created_at" not in orders.columns:
            return pd.Series([pd.NaT] * len(orders), index=orders.index)
        return _to_date_series(orders["created_at"])
    if basis == "shipped":
        if "shipped_at" not in orders.columns:
            return pd.Series([pd.NaT] * len(orders), index=orders.index)
        return _to_date_series(orders["shipped_at"])
    return _to_date_series(orders.get("purchase_date", pd.Series(dtype=object)))


def summarize_sales_period(
    orders: pd.DataFrame,
    items: pd.DataFrame,
    price_map: dict[str, dict[str, Any]],
    d_start: dt.date,
    d_end: dt.date,
    *,
    date_basis: str,
    status_filter: set[str] | None,
) -> dict[str, Any]:
    """
    date_basis: 'purchase' | 'created' | 'shipped'
    status_filter: None 이면 전체 상태. 집합이면 해당 상태만.
    """
    o = orders.copy()
    if len(o) == 0:
        return {
            "n_orders": 0,
            "n_item_lines": 0,
            "qty_sum": 0,
            "sale_amount": 0.0,
            "gwangjin_amount": 0.0,
            "n_unpriced_lines": 0,
            "unpriced_qty": 0,
            "by_product": pd.DataFrame(
                columns=["표시상품명", "수량합", "판매금액", "광진금액", "라인수", "단가미매칭수량"]
            ),
        }

    bd = order_basis_dates(o, date_basis)
    o["_basis_date"] = bd
    in_range = o["_basis_date"].notna() & (o["_basis_date"] >= d_start) & (o["_basis_date"] <= d_end)
    o = o.loc[in_range].copy()
    if status_filter is not None and "status" in o.columns:
        o = o[o["status"].astype(str).isin(status_filter)].copy()

    oids = set(o["order_id"].astype(str)) if "order_id" in o.columns else set()
    n_orders = len(oids)

    if not oids or items is None or len(items) == 0 or "order_id" not in items.columns:
        return {
            "n_orders": n_orders,
            "n_item_lines": 0,
            "qty_sum": 0,
            "sale_amount": 0.0,
            "gwangjin_amount": 0.0,
            "n_unpriced_lines": 0,
            "unpriced_qty": 0,
            "by_product": pd.DataFrame(
                columns=["표시상품명", "수량합", "판매금액", "광진금액", "라인수", "단가미매칭수량"]
            ),
        }

    it = items[items["order_id"].astype(str).isin(oids)].copy()
    n_item_lines = len(it)
    it["_q"] = pd.to_numeric(it.get("qty"), errors="coerce").fillna(0).astype(int).clip(lower=0)
    qty_sum = int(it["_q"].sum())

    sale_amount = 0.0
    gwangjin_amount = 0.0
    n_unpriced_lines = 0
    unpriced_qty = 0

    def _display_name(r: pd.Series) -> str:
        for c in ("product_canonical", "product_raw"):
            v = r.get(c)
            if v is None or (isinstance(v, float) and pd.isna(v)):
                continue
            s = str(v).strip()
            if s:
                return s
        return "(이름없음)"

    rows_by: dict[str, dict[str, Any]] = {}

    for _, r in it.iterrows():
        q = int(r["_q"])
        pr = lookup_line_price(r, price_map) if price_map else None
        name = _display_name(r)
        pk_cell = str(r.get("product_key") or "").strip()
        key = pk_cell or _row_product_key(name)

        if pr is None:
            n_unpriced_lines += 1
            unpriced_qty += q
            sale = gj = 0.0
        else:
            try:
                sale = float(pr.get("판매가격") or 0) * q
            except (TypeError, ValueError):
                sale = 0.0
            gj_v = pr.get("광진가격(60%)")
            try:
                gj = float(gj_v) * q if gj_v is not None and not (isinstance(gj_v, float) and pd.isna(gj_v)) else 0.0
            except (TypeError, ValueError):
                gj = 0.0

        sale_amount += sale
        gwangjin_amount += gj

        acc = rows_by.setdefault(key, {"표시상품명": name, "수량합": 0, "판매금액": 0.0, "광진금액": 0.0, "라인수": 0, "단가미매칭수량": 0})
        acc["수량합"] += q
        acc["판매금액"] += sale
        acc["광진금액"] += gj
        acc["라인수"] += 1
        if pr is None:
            acc["단가미매칭수량"] += q

    by_product = (
        pd.DataFrame(list(rows_by.values()))
        if rows_by
        else pd.DataFrame(columns=["표시상품명", "수량합", "판매금액", "광진금액", "라인수", "단가미매칭수량"])
    )
    if len(by_product):
        by_product = by_product.sort_values("판매금액", ascending=False).reset_index(drop=True)

    return {
        "n_orders": n_orders,
        "n_item_lines": n_item_lines,
        "qty_sum": qty_sum,
        "sale_amount": sale_amount,
        "gwangjin_amount": gwangjin_amount,
        "n_unpriced_lines": n_unpriced_lines,
        "unpriced_qty": unpriced_qty,
        "by_product": by_product,
    }
