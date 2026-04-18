# Mutomo 판매관리

뮤토모 주문 엑셀(`.xlsx`)을 수집해 **주문·품목 단위**로 SQLite에 저장하고, Streamlit 대시보드에서 **접수·출고·클레임** 등 상태를 관리합니다. 상품명 표기 차이는 **별칭 YAML + 유사도 추천**으로 정리합니다.

---

## 요구 사항

- **Python** 3.11 이상 (3.12·3.13·3.14 등 최신 버전도 사용 가능)
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

**항상 저장소 루트의 `order_list` 폴더만** 쓰면 됩니다. `ingest_xlsx.py` 기본값과 같습니다 (없으면 자동 생성).

과거에 `mutomo/order_list/…`처럼 중첩된 경로에 두었거나, 실수로 **`mutomo/` 아래에 저장소를 또 복제**한 경우가 있으면, 루트에서 한 번 정리합니다.

```powershell
python tools\flatten_order_list.py --dry-run   # 미리 보기
python tools\flatten_order_list.py             # mutomo/ 아래 .xlsx → order_list/ 로 이동
```

이후 **`mutomo/` 폴더는 삭제**해 두세요(중복 클론·`.venv`만 있으면 용량만 큼). Git에는 `mutomo/`를 올리지 않도록 `.gitignore`에 넣어 두었습니다.

다른 드라이브만 쓰고 싶을 때만 예:

```powershell
python ingest_xlsx.py --input-dir "D:\주문백업\order_list" --db mutomo.sqlite --aliases product_aliases.yml
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
- 수집 시 **동일 연락처 힌트**용 `phone_norm`·`party_key` 등은 `recipient_identity` 로직으로 보강됩니다.

---

## 대시보드 실행

```powershell
python -m streamlit run dashboard.py
```

- **설정**(사이드바): DB 경로(기본 `mutomo.sqlite`), **상태 필터**, ‘오늘 접수’ 기준(구매일자 vs 수집시각), 관리 컬럼 표시.
- 메인 **탭**
  - **판매 요약**: 오늘·최근 접수/출고 패널 등
  - **이름 검색**: 받는분 검색·다중 선택 → **전체 접수 목록** 표에서 출고·클레임 등 일괄 처리
  - **기간별 판매집계**: 기간·기준일·포함 상태를 **이 탭 안에서만** 정해 주문 건수·수량·금액(아래 단가표 연동)을 봅니다. 사이드바 상태 필터와는 별개입니다.
- 사이드바 **접수목록 선택 상세** / **클레임·특이사항**: 표에서 고른 주문에 대해 품목·메모 확인·저장.
- 화면 하단 **제품 목록(집계)**: 정규 상품명·엑셀 원문 기준 줄 수·수량 합(상태 필터와 동일 범위).
- 엑셀만 바꾼 뒤 DB가 그대로면 **ingest**를 다시 실행하고, 사이드바 **DB 다시 읽기** 또는 브라우저 새로고침을 사용합니다.

---

## 단가표 (`단가표.csv`)

저장소 **루트**에 두는 CSV입니다. 열 예: **`엑셀상품명`**, **`판매가격`**, **`광진가격(60%)`**.  
수집된 품목명(`product_raw` / `product_canonical`)과 **ingest와 동일한 정규화 키**로 매칭해, **기간별 판매집계** 탭에서만 합계 금액에 반영합니다(단가 행을 대시보드에 뿌리지는 않습니다).

> 민감하거나 자주 바뀌는 경우 Git에 포함하지 않고 PC에만 두고 싶다면 `.gitignore`에 `단가표.csv`를 추가하면 됩니다.

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
- 피킹 시트 **특이사항** 열에는 엑셀 자동 특이사항·수기 특이사항만 넣고, **주문목록(물품 요약)** 은 옆 **품목** 열에만 둡니다. `출고_주문`의 **특이사항(엑셀자동)** 은 DB `order_list`와 겹치는 문구를 정리해 중복 표시를 줄입니다.

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

다른 PC로 옮길 때: `mutomo.sqlite`, `product_aliases.yml`, 필요 시 `단가표.csv`, `order_list`(또는 사용 중인 입력 폴더) 원본 엑셀.

---

## Docker (선택)

```bash
docker compose up --build
```

`http://localhost:8501` — 호스트의 `mutomo.sqlite`, `product_aliases.yml` 마운트 (`docker-compose.yml`).  
단가표를 컨테이너에서 쓰려면 같은 방식으로 `단가표.csv`를 마운트하거나 이미지에 포함하세요.

---

## 데이터 초기화

Streamlit 종료 후 `mutomo.sqlite` 삭제 또는 이름 변경 → `ingest_xlsx.py`로 다시 수집. 백업은 `backups\` 등에 보관해 두세요.

---

## 의존성

`requirements.txt`: openpyxl, pandas, rapidfuzz, pyyaml, streamlit
