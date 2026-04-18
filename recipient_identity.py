"""
받는분·연락처 기준 느슨한 그룹(= party) — CRM이 아니라 **배송 단위 추정**용.

설계 원칙
---------
1. **order_id**  
   한 건의 접수·출고·DB 업데이트의 **유일한 진실(source of truth)**. 이름이 같아도
   주문번호(여기서는 파일#그룹@행)가 다르면 다른 접수.

2. **phone_norm**  
   비교·검색용. 숫자만 두고, +82 등을 국내 0 시작 형태로 맞춤.

3. **party_key** (같은 “연락처·주소 덩어리” 힌트)  
   - 전화가 **8자리 이상**이면 `tel:<phone_norm>` → 재주문·동일 연락처는 같은 키.  
   - 전화가 비었으면 주소 문자열을 정규화한 뒤 해시 → `addr:<해시>` (짧은 주소는 신뢰 안 함).  
   - 둘 다 없으면 `order:<order_id>` → **접수마다 단독**(잘못 합치지 않음).

4. **동명이인**  
   이름은 표시용. 전화(또는 주소 해시)가 다르면 **party_key도 달라짐**.

5. **한계**  
   가족 공용 전화·회사 대표번호는 같은 party로 묶일 수 있음 → UI에서 “연락처
   그룹 N건”은 **힌트**로만 쓰고, 확정은 항상 order_id 단위.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any

import pandas as pd


def normalize_phone_digits(raw: Any) -> str:
    """숫자만 남기고, 흔한 +82 표기를 0으로 시작하는 국내형으로 맞춤."""
    s = "".join(ch for ch in str(raw or "") if ch.isdigit())
    if not s:
        return ""
    if s.startswith("82") and len(s) >= 10:
        s = "0" + s[2:]
    return s


def normalize_address_fingerprint(raw: Any) -> str:
    """주소 비교용 최소 정규화(공백·대소문). 해시 입력으로만 사용."""
    s = str(raw or "").strip().lower()
    if not s:
        return ""
    return re.sub(r"\s+", " ", s)


def party_key_for_row(order_id: str, phone: Any, address: Any) -> tuple[str, str]:
    """(phone_norm, party_key) 반환."""
    pn = normalize_phone_digits(phone)
    if len(pn) >= 8:
        return pn, f"tel:{pn}"
    addr = normalize_address_fingerprint(address)
    if len(addr) >= 16:
        digest = hashlib.sha256(addr.encode("utf-8")).hexdigest()[:24]
        return pn, f"addr:{digest}"
    oid = (order_id or "").strip() or "unknown"
    return pn, f"order:{oid}"


def assign_recipient_ids(df: pd.DataFrame) -> pd.DataFrame:
    """orders 행에 phone_norm, party_key 채움(없으면 컬럼 생성)."""
    out = df.copy()
    if out.empty:
        for c in ("phone_norm", "party_key"):
            if c not in out.columns:
                out[c] = pd.Series(dtype=str)
        return out
    norms: list[str] = []
    keys: list[str] = []
    for _, r in out.iterrows():
        oid = str(r.get("order_id") or "")
        pn, pk = party_key_for_row(oid, r.get("phone"), r.get("address"))
        norms.append(pn)
        keys.append(pk)
    out["phone_norm"] = norms
    out["party_key"] = keys
    return out
