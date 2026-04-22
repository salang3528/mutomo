"""
다음메일(Daum / hanmail) IMAP에서 .xlsx 첨부만 받아 order_list 폴더에 저장합니다.

  - 서버: imap.daum.net (동일 계정은 imap.hanmail.net 도 가능한 경우가 많음)
  - 포트: 993, SSL
  - 로그인: **전체 메일 주소** + **카카오계정「앱 비밀번호」** (2025-01-02부터 IMAP/POP3는 2단계 인증·앱 비밀번호가 필수인 경우가 많습니다. **웹 로그인 비밀번호로는 실패**합니다.)

사용 예 (PowerShell):

  $env:DAUM_EMAIL = "you@daum.net"
  $env:DAUM_APP_PASSWORD = "앱에서_발급한_비밀번호"
  .\\.venv\\Scripts\\python.exe fetch_daum_mail.py

이후 기존과 같이:

  .\\.venv\\Scripts\\python.exe ingest_xlsx.py --db mutomo.sqlite --aliases product_aliases.yml

도움말: https://cs.daum.net/faq/266/12145.html (IMAP/SMTP) · 앱 비밀번호는 카카오계정(accounts.kakao.com) → 계정 보안 쪽에서 발급합니다.
"""

from __future__ import annotations

import argparse
import email.header
import imaplib
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from email.message import Message
from email.parser import BytesParser
from email.policy import default as email_policy


def _repo_root() -> str:
    return os.path.dirname(os.path.abspath(__file__))


def _decode_filename(raw: str | None) -> str:
    if not raw:
        return ""
    out: list[str] = []
    for part, enc in email.header.decode_header(raw):
        if isinstance(part, bytes):
            out.append(part.decode(enc or "utf-8", errors="replace"))
        else:
            out.append(str(part))
    return "".join(out).strip()


def _safe_basename(name: str) -> str:
    base = os.path.basename(name.replace("\\", "/")).strip()
    base = re.sub(r'[<>:"/\\|?*]', "_", base)
    return base or "attachment"


def _unique_path(dir_path: str, basename: str) -> str:
    path = os.path.join(dir_path, basename)
    if not os.path.exists(path):
        return path
    stem, ext = os.path.splitext(basename)
    for i in range(1, 1000):
        cand = os.path.join(dir_path, f"{stem}_{i}{ext}")
        if not os.path.exists(cand):
            return cand
    raise OSError("could not allocate unique filename")


def _collect_xlsx_parts(msg: Message) -> list[tuple[str, bytes]]:
    found: list[tuple[str, bytes]] = []

    def walk(m: Message) -> None:
        if m.is_multipart():
            for p in m.iter_parts():
                walk(p)
            return
        disp = (m.get_content_disposition() or "").lower()
        fn = _decode_filename(m.get_filename())
        if disp == "attachment" or fn:
            ct = (m.get_content_type() or "").lower()
            if ct == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" or fn.lower().endswith(
                ".xlsx"
            ):
                payload = m.get_payload(decode=True)
                if isinstance(payload, bytes) and payload:
                    name = _safe_basename(fn or "attachment.xlsx")
                    if not name.lower().endswith(".xlsx"):
                        name = f"{name}.xlsx"
                    if name.startswith("~$"):
                        return
                    found.append((name, payload))

    walk(msg)
    return found


def _imap_since_arg(days: int) -> str:
    d = datetime.now(timezone.utc).astimezone().replace(tzinfo=None) - timedelta(days=max(1, days))
    # IMAP date: 01-Apr-2026
    return d.strftime("%d-%b-%Y")


def _imap_hosts_to_try(primary: str) -> list[str]:
    """같은 계정이라도 daum / hanmail 호스트 중 하나만 되는 경우가 있어 순서대로 시도."""
    p = (primary or "").strip().lower()
    daum = "imap.daum.net"
    han = "imap.hanmail.net"
    if p == daum:
        return [daum, han]
    if p == han:
        return [han, daum]
    return [primary.strip()]


def _print_imap_login_help(*, tried_hosts: list[str], login_id: str, err: imaplib.IMAP4.error) -> None:
    redacted = login_id if "@" in login_id else "(아이디 숨김)"
    print("IMAP 로그인에 실패했습니다: ", err, file=sys.stderr)
    print("", file=sys.stderr)
    print("시도한 서버:", ", ".join(tried_hosts), file=sys.stderr)
    print("로그인 ID(앞부분만):", redacted.split("@")[0][:3] + "***@" + redacted.split("@", 1)[-1] if "@" in redacted else redacted, file=sys.stderr)
    print("", file=sys.stderr)
    print(
        "다음을 순서대로 확인하세요.\n"
        "  1) 다음 웹메일 → 설정에서 **IMAP 사용**이 켜져 있는지\n"
        "  2) **카카오계정(accounts.kakao.com) → 계정 보안 → 2단계 인증** 켠 뒤\n"
        "     **앱 비밀번호**를 새로 만들고, 그 **16자리(또는 표시되는 비밀번호 전체)**를 DAUM_APP_PASSWORD에 넣었는지\n"
        "     (웹메일 로그인에 쓰는 비밀번호와 다릅니다.)\n"
        "  3) DAUM_EMAIL은 **you@daum.net** 또는 **you@hanmail.net** 처럼 **@까지 전체**인지\n"
        "  4) PowerShell에서 값 앞뒤 **공백·따옴표**가 붙지 않았는지 (스크립트가 앞뒤 공백은 제거합니다)\n"
        "  5) 그래도 동일하면 `--host imap.hanmail.net` 또는 `--host imap.daum.net` 을 바꿔서 재시도\n",
        file=sys.stderr,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="다음메일 IMAP → order_list 로 .xlsx 첨부 저장")
    ap.add_argument("--host", default=os.environ.get("DAUM_IMAP_HOST", "imap.daum.net"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("DAUM_IMAP_PORT", "993")))
    ap.add_argument("--mailbox", default=os.environ.get("DAUM_IMAP_MAILBOX", "INBOX"))
    ap.add_argument("--user", default=os.environ.get("DAUM_EMAIL", ""), help="전체 주소 (기본: 환경변수 DAUM_EMAIL)")
    ap.add_argument(
        "--password",
        default=os.environ.get("DAUM_APP_PASSWORD", ""),
        help="앱 비밀번호 등 (기본: 환경변수 DAUM_APP_PASSWORD)",
    )
    ap.add_argument(
        "--out-dir",
        default=os.path.join(_repo_root(), "order_list"),
        help="저장 폴더 (기본: 프로젝트의 order_list)",
    )
    ap.add_argument(
        "--since-days",
        type=int,
        default=30,
        metavar="N",
        help="최근 N일 안의 메일만 검색 (기본 30). UNSEEN과 함께 쓰면 둘 다 만족하는 메일만",
    )
    ap.add_argument("--unseen", action="store_true", help="읽지 않은 메일만")
    ap.add_argument("--mark-seen", action="store_true", help="첨부를 저장한 메일을 읽음으로 표시")
    ap.add_argument("--dry-run", action="store_true", help="연결·검색만 하고 저장하지 않음")
    args = ap.parse_args()

    user = (args.user or "").strip()
    password = (args.password or "").strip()
    if not user or not password:
        print(
            "DAUM_EMAIL / DAUM_APP_PASSWORD 가 필요합니다.\n"
            "다음메일 웹에서 IMAP 사용 허용 및(필요 시) 앱 비밀번호를 발급한 뒤 환경 변수로 넣어 주세요.",
            file=sys.stderr,
        )
        return 2

    out_dir = os.path.normpath(args.out_dir)
    os.makedirs(out_dir, exist_ok=True)

    since = _imap_since_arg(args.since_days)
    if args.unseen:
        criterion = f'(UNSEEN SINCE "{since}")'
    else:
        criterion = f'(SINCE "{since}")'

    hosts = _imap_hosts_to_try(args.host)
    last_err: imaplib.IMAP4.error | None = None
    imap: imaplib.IMAP4_SSL | None = None
    used_host = ""
    for h in hosts:
        conn: imaplib.IMAP4_SSL | None = None
        try:
            conn = imaplib.IMAP4_SSL(h, args.port)
            conn.login(user, password)
            imap = conn
            if h.lower() != (args.host or "").strip().lower():
                print(f"(참고) --host {args.host!r} 대신 {h!r} 로 로그인했습니다.", file=sys.stderr)
            break
        except imaplib.IMAP4.error as e:
            last_err = e
            try:
                if conn is not None:
                    conn.logout()
            except Exception:
                pass
    if imap is None:
        assert last_err is not None
        _print_imap_login_help(tried_hosts=hosts, login_id=user, err=last_err)
        return 4

    saved = 0
    with imap:
        typ, _ = imap.select(args.mailbox, readonly=False)
        if typ != "OK":
            print(f"select failed: {args.mailbox}", file=sys.stderr)
            return 3

        typ, data = imap.search(None, criterion)
        if typ != "OK" or not data or not data[0]:
            print("조건에 맞는 메일이 없습니다.")
            return 0

        uids = data[0].split()
        print(f"검색 조건: {criterion!r} → 메일 {len(uids)}통")

        parser = BytesParser(policy=email_policy)
        for uid in uids:
            typ, msgdata = imap.fetch(uid, "(RFC822)")
            if typ != "OK" or not msgdata or not isinstance(msgdata[0], tuple):
                continue
            raw = msgdata[0][1]
            if not isinstance(raw, (bytes, bytearray)):
                continue
            msg = parser.parsebytes(bytes(raw))
            parts = _collect_xlsx_parts(msg)
            for name, body in parts:
                dest = _unique_path(out_dir, name)
                if args.dry_run:
                    print(f"[dry-run] would save: {dest} ({len(body)} bytes)")
                else:
                    with open(dest, "wb") as f:
                        f.write(body)
                    print(f"saved: {dest}")
                saved += 1
            if parts and args.mark_seen and not args.dry_run:
                imap.store(uid, "+FLAGS", "\\Seen")

    print(f"완료: .xlsx 첨부 {saved}개 처리")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
