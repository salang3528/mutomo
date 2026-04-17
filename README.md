# Mutomo 판매관리

뮤토모 주문 엑셀(`.xlsx`)을 수집해 **주문·품목 단위**로 SQLite에 저장하고, Streamlit 대시보드에서 **접수·출고·클레임** 등 상태를 관리합니다. 상품명 표기 차이는 **별칭 YAML + 유사도 추천**으로 정리합니다.

---

## 요구 사항

- **Python** 3.11 이상 (3.12·3.13 권장)
- Windows / macOS / Linux (현장 PC는 Windows 기준 문서화)

---

## 설치

```powershell
cd C:\python\mutomo
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

---

## 주문 엑셀 두는 곳

`ingest_xlsx.py` 기본 입력 폴더는 저장소 루트의 **`order_list`** 입니다 (없으면 자동 생성).

다른 경로를 쓰려면 예:

```powershell
python ingest_xlsx.py --input-dir mutomo\order_list --db mutomo.sqlite --aliases product_aliases.yml
```

> 고객 정보가 들어 있는 `.xlsx`와 운영 DB(`mutomo.sqlite`)는 **Git에 올리지 마세요** (`.gitignore` 처리).

---

## 엑셀 → DB 수집

```powershell
python ingest_xlsx.py --db mutomo.sqlite --aliases product_aliases.yml
```

- 기본으로 **`order_list`** 아래의 모든 `.xlsx`를 읽습니다.
- **재수집** 시: 주문서에서 다시 읽는 필드는 덮어쓰고, 대시보드에서 바꾼 **상태·출고시각·특이사항** 등은 가능한 범위에서 유지됩니다.
- 매핑되지 않은 상품명은 **`unknown_products.csv`**에 쌓입니다. `product_aliases.yml`에 별칭을 추가한 뒤 다시 수집하면 됩니다.

---

## 대시보드 실행

```powershell
python -m streamlit run dashboard.py
```

- **설정**에서 DB 경로를 지정합니다 (기본 `mutomo.sqlite`).
- **오늘 접수 / 전체 목록**에서 주문 확인, **받는분 검색** 후 여러 건 선택 → **접수 / 출고 / 클레임 / 마감 / 납품취소**.
- 엑셀만 바꾼 뒤 DB가 그대로면 **ingest**를 다시 실행하고, 사이드바 **DB 다시 읽기** 또는 브라우저 새로고침을 사용합니다.

---

## 출고 목록 엑셀 (사이드바)

**출고 기준 날짜** 선택 후 `출고 목록 엑셀 다운로드`.  
포함: `status = 출고` 이고 `shipped_at` 날짜가 선택일인 주문.

| 시트 | 내용 |
|------|------|
| **출고_피킹요약** | 출고 건수, 피킹(택배/직접) 행 수, 혼합 건수·검산, 혼합 받는분 |
| **피킹리스트_택배** / **피킹리스트_직접** | 품목 배송 기준 피킹 (A4 가로) |
| **로젠택배** | **택배 출고만** (직접 전용 주문 제외) |
| **출고_주문** | 주문별 요약 |
| **출고_품목** | 품목·수량 합산 |

### 배송·피킹

- 품목 **배송**란으로 택배/직접 구분.
- 같은 주문에서 아래 줄 배송이 **비어 있으면** 위쪽 값을 이어 씁니다 (빈 칸을 택배로만 보는 오탐 완화).
- 한 주문에 택배·직접이 **함께 있으면 혼합**: 두 피킹 시트에 각 한 줄.  
  `택배 행 수 + 직접 행 수 = 출고 주문 수 + 혼합 주문 수`.

---

## 주문 식별

**`order_id`**: `원본파일명#번호@엑셀시작행` 형태. 동명이인도 서로 다른 주문으로 처리됩니다.

---

## 상품 별칭 (`product_aliases.yml`)

```yaml
"뮤커비 1600":
  - "뮤커비1600"
  - "뮤 커비 1600"
```

---

## 백업

대시보드 실행 시 **하루 1회** `backups\`에 DB 복사가 될 수 있습니다.

```powershell
python backup_db.py --db mutomo.sqlite --out backups --keep-days 30
```

다른 PC로 옮길 때: `mutomo.sqlite`, `product_aliases.yml`, 필요 시 `order_list`(또는 사용 중인 입력 폴더) 원본 엑셀.

---

## Docker (선택)

```bash
docker compose up --build
```

`http://localhost:8501` — 호스트의 `mutomo.sqlite`, `product_aliases.yml` 마운트 (`docker-compose.yml`).

---

## 데이터 초기화

Streamlit 종료 후 `mutomo.sqlite` 삭제 또는 이름 변경 → `ingest_xlsx.py`로 다시 수집. 백업은 `backups\` 등에 보관해 두세요.

---

## 의존성

`requirements.txt`: openpyxl, pandas, rapidfuzz, pyyaml, streamlit
