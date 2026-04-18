"""
mutomo/order_list/ 아래 등에 흩어진 주문 .xlsx 를 저장소 루트의 order_list/ 로 모읍니다.

잘못된 예: mutomo/order_list/mutomo/order_list/*.xlsx
권장: order_list/*.xlsx (ingest_xlsx.py 기본 입력과 동일)
"""
from __future__ import annotations

import argparse
import shutil
from pathlib import Path


def _unique_dest(canonical: Path, src: Path) -> Path:
    dest = canonical / src.name
    if not dest.exists():
        return dest
    stem, suf = src.stem, src.suffix
    tag = src.parent.name.replace(" ", "_")[:40]
    candidate = canonical / f"{stem}__{tag}{suf}"
    n = 2
    while candidate.exists():
        candidate = canonical / f"{stem}__{tag}_{n}{suf}"
        n += 1
    return candidate


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="이동하지 않고 대상 파일만 출력",
    )
    args = ap.parse_args()

    root = Path(__file__).resolve().parent.parent
    canonical = root / "order_list"
    stray_root = root / "mutomo"

    canonical.mkdir(parents=True, exist_ok=True)

    if not stray_root.is_dir():
        print("mutomo/ 폴더가 없습니다. 루트 order_list/ 만 사용하면 됩니다.")
        return

    # mutomo 아래의 모든 xlsx (중첩 order_list 포함)
    sources = sorted(
        p
        for p in stray_root.rglob("*.xlsx")
        if p.is_file() and not p.name.startswith("~$")
    )
    if not sources:
        print("mutomo/ 아래에 옮길 .xlsx 가 없습니다.")
        return

    print(f"루트 order_list: {canonical}")
    for src in sources:
        dest = _unique_dest(canonical, src)
        rel = src.relative_to(root)
        if args.dry_run:
            print(f"  [dry-run] {rel} -> {dest.relative_to(root)}")
        else:
            shutil.move(str(src), str(dest))
            print(f"  이동: {rel} -> {dest.relative_to(root)}")

    if args.dry_run:
        print("\n--dry-run 이므로 디렉터리 정리는 하지 않았습니다.")
        return

    # mutomo 아래 빈 폴더만 아래에서 위로 제거
    dirs = sorted({p.parent for p in stray_root.rglob("*")}, key=lambda d: len(d.parts), reverse=True)
    for d in dirs:
        if d == stray_root or not str(d).startswith(str(stray_root)):
            continue
        try:
            if d.is_dir() and not any(d.iterdir()):
                d.rmdir()
                print(f"  빈 폴더 삭제: {d.relative_to(root)}")
        except OSError:
            pass

    # .venv 등이 남아 있으면 mutomo 자체는 못 지울 수 있음
    try:
        if stray_root.is_dir() and not any(stray_root.iterdir()):
            stray_root.rmdir()
            print(f"  삭제: mutomo/ (비었음)")
    except OSError:
        print(
            "\n참고: mutomo/ 안에 아직 파일·폴더(.venv 등)가 남아 있으면 수동으로 정리하세요."
        )


if __name__ == "__main__":
    main()
