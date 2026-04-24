from __future__ import annotations

import argparse
import datetime as dt
import os
import sqlite3


def backup_sqlite(db_path: str, backup_dir: str, keep_days: int) -> str:
    os.makedirs(backup_dir, exist_ok=True)
    if not os.path.exists(db_path):
        raise FileNotFoundError(db_path)

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    base = os.path.splitext(os.path.basename(db_path))[0] or "mutomo"
    out_path = os.path.join(backup_dir, f"{base}_{ts}.sqlite")

    src = sqlite3.connect(db_path)
    try:
        dst = sqlite3.connect(out_path)
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()

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

    return out_path


def main() -> None:
    p = argparse.ArgumentParser(description="Backup mutomo sqlite DB safely.")
    p.add_argument("--db", default="mutomo.sqlite", help="Path to sqlite DB file")
    p.add_argument("--out", default="backups", help="Backup directory")
    p.add_argument("--keep-days", type=int, default=30, help="보관 일수(기본 약 한 달, 초과 백업 파일 삭제)")
    args = p.parse_args()

    out = backup_sqlite(args.db, args.out, args.keep_days)
    print(out)


if __name__ == "__main__":
    main()

