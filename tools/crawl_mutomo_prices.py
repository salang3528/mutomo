from __future__ import annotations

import argparse
import csv
import re
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass


BASE = "https://www.mu-tomo.com"
LIST_PATH = "/ALLPRODUCTS/"


UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"


@dataclass(frozen=True)
class ProductPrice:
    idx: str
    brand: str
    name: str
    price_won: int
    image_url: str
    url: str


_ITEM_BLOCK_RE = re.compile(
    r'<a\s+href="/ALLPRODUCTS/\?idx=(\d+)"[\s\S]*?</a>',
    re.IGNORECASE,
)
_H2_RE = re.compile(r"<h2>\s*([^<]+?)\s*</h2>", re.IGNORECASE)
_PRICE_RE = re.compile(r"([0-9][0-9,]*)원")
_IMG_RE = re.compile(r'<img[^>]+src="([^"]+)"', re.IGNORECASE)
_BRACKET_BRAND_RE = re.compile(r"^\s*\[([^\]]+)\]\s*")


def _fetch(url: str, *, timeout: int = 20) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    return raw.decode("utf-8", errors="ignore")


def _parse_items(html: str) -> list[ProductPrice]:
    out: list[ProductPrice] = []
    for m in _ITEM_BLOCK_RE.finditer(html):
        idx = str(m.group(1))
        block = m.group(0)
        m_h2 = _H2_RE.search(block)
        m_p = _PRICE_RE.search(block)
        if not m_h2 or not m_p:
            continue
        raw_name = re.sub(r"\s+", " ", (m_h2.group(1) or "").strip())
        m_b = _BRACKET_BRAND_RE.match(raw_name)
        brand = (m_b.group(1).strip() if m_b else "").strip()
        name = _BRACKET_BRAND_RE.sub("", raw_name).strip()
        try:
            price_won = int((m_p.group(1) or "").replace(",", "").strip())
        except ValueError:
            continue
        m_img = _IMG_RE.search(block)
        image_url = ""
        if m_img:
            image_url = (m_img.group(1) or "").strip()
            if image_url.startswith("/"):
                image_url = urllib.parse.urljoin(BASE, image_url)
        url = urllib.parse.urljoin(BASE, f"{LIST_PATH}?idx={idx}")
        out.append(
            ProductPrice(
                idx=str(idx),
                brand=brand,
                name=name,
                price_won=price_won,
                image_url=image_url,
                url=url,
            )
        )
    return out


def crawl_all_products_prices(*, max_pages: int = 60, sleep_s: float = 0.35) -> list[ProductPrice]:
    """Crawl ALLPRODUCTS listing pages and extract (name, price)."""
    seen: dict[str, ProductPrice] = {}
    no_new_pages = 0
    for page in range(1, max_pages + 1):
        url = urllib.parse.urljoin(BASE, LIST_PATH)
        if page > 1:
            url = f"{url}?page={page}"
        html = _fetch(url)
        items = _parse_items(html)
        new = 0
        for it in items:
            if it.idx not in seen:
                seen[it.idx] = it
                new += 1
        if new == 0:
            no_new_pages += 1
        else:
            no_new_pages = 0
        # two consecutive pages with no new items → likely done
        if no_new_pages >= 2 and page >= 3:
            break
        time.sleep(max(0.0, float(sleep_s)))
    # stable output by idx numeric
    return sorted(seen.values(), key=lambda x: int(x.idx))


def write_price_csv(rows: list[ProductPrice], out_path: str) -> None:
    # 정리용 CSV(브랜드/이미지 포함). 필요 없는 부분은 사용자가 제거.
    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "brand",
                "엑셀상품명",
                "판매가격",
                "광진가격(60%)",
                "image_url",
                "url",
                "idx",
            ],
        )
        w.writeheader()
        for r in rows:
            w.writerow(
                {
                    "brand": r.brand,
                    "엑셀상품명": r.name,
                    "판매가격": r.price_won,
                    # 광진가격은 사이트에 없어서 비워 둠(필요하면 나중에 환산 규칙 추가)
                    "광진가격(60%)": "",
                    "image_url": r.image_url,
                    "url": r.url,
                    "idx": r.idx,
                }
            )


def main() -> None:
    ap = argparse.ArgumentParser(description="Crawl mu-tomo.com ALLPRODUCTS prices to a CSV.")
    ap.add_argument("--out", default="단가표_from_web.csv", help="Output CSV path")
    ap.add_argument("--max-pages", type=int, default=60, help="Max pages to crawl (safety)")
    ap.add_argument("--sleep", type=float, default=0.35, help="Sleep seconds between page fetches")
    args = ap.parse_args()

    rows = crawl_all_products_prices(max_pages=int(args.max_pages), sleep_s=float(args.sleep))
    if not rows:
        print("No products found. Site may require login or HTML structure changed.", file=sys.stderr)
        raise SystemExit(2)
    write_price_csv(rows, str(args.out))
    print(f"Wrote {len(rows)} products to {args.out}")


if __name__ == "__main__":
    main()

