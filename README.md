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

- 사이드바 상단 **페이지 버튼**: **출고 / 접수목록 / 마감**
- **설정**(사이드바): DB 경로(기본 `mutomo.sqlite`), **상태 필터**, ‘오늘 접수’ 기준(구매일자 vs 수집시각), 관리 컬럼 표시.
- **엑셀→DB 수집**(사이드바 설정): 저장소 루트 `order_list`의 `.xlsx`를 읽어 **설정의 DB 파일**에 반영합니다(터미널 `ingest_xlsx.py`와 동일).
- **DB 다시 읽기**(사이드바 설정): 디스크에 이미 반영된 DB를 **화면 캐시에서 다시 불러오기**만 합니다.
- 사이드바 **접수목록 선택 상세** / **클레임·특이사항**: 표에서 고른 주문에 대해 품목·메모 확인·저장.
- 화면 하단 **제품 목록(집계)**: 정규 상품명·엑셀 원문 기준 줄 수·수량 합(상태 필터와 동일 범위).
- **중요**: `order_list`에 엑셀 파일만 추가하면 SQLite는 자동으로 바뀌지 않습니다. **엑셀→DB 수집**을 누르거나 터미널에서 ingest를 실행해야 합니다.

### 도면 참조 표시(📐)

- 주문서 본문 어디에든 **`도면참조` / `도면참고`** 문구가 있으면(공백·줄바꿈 무시) 이름 뒤에 **📐** 아이콘이 붙습니다.

### 옵션 추가금(예: 다리높이) 단가 반영

- 품목 텍스트에 **`(+16000원)`** 같은 표기가 있으면 단가표의 **판매가격/광진가격**에 자동으로 더해서 집계합니다.

---

## 회사·다른 PC에서 이어서 작업

**코드**는 Git으로 맞추고, **주문·DB·단가**는 Git 밖에서 프로젝트 루트로 옮깁니다.

1. **저장소** — 새 PC면 `git clone` 후 해당 폴더에서 작업. 이미 받아 둔 폴더면 `git pull`으로 최신 반영.
2. **파이썬** — [설치](#설치)와 같이 `venv` → `pip install -r requirements.txt` (회사 PC Python 버전이 3.11 이상인지 확인).
3. **데이터 복사** (USB·외장디스크·회사 허용 NAS 등 → 저장소 **루트**에 둠)

   | 상황 | 가져올 것 |
   |------|------------|
   | 대시보드·출고 이력 그대로 | `mutomo.sqlite` 통째 복사 |
   | DB는 새로 만들 계획 | `order_list\` 안 **주문 `.xlsx`** 만 복사 후 아래 [엑셀 → DB 수집](#엑셀--db-수집) 실행 |
   | 기간별 금액 집계 | **`단가표.csv`** (레포에 없을 수 있음 → 같이 복사) |
   | 별칭을 회사에서 쓰는 경우 | `product_aliases.yml` |

4. **DB 만들기** — 집 DB를 복사하지 않을 때만, 엑셀을 둔 뒤 `python ingest_xlsx.py --db mutomo.sqlite --aliases product_aliases.yml` 로 채움.
5. **실행** — `python -m streamlit run dashboard.py` 후 사이드바 **설정**의 **DB 경로**가 회사 PC의 `mutomo.sqlite`를 가리키는지 확인(폴더가 다르면 여기만 수정).
6. **끝날 때** — 코드·README 변경은 `git commit` / `git push`. DB·엑셀·단가표는 **회사 보안·백업 규칙**에 맞게 별도 보관(Git에 올리지 않음).

---

## 단가표 (`단가표.csv`)

저장소 **루트**에 두는 CSV입니다. 열 예: **`엑셀상품명`**, **`판매가격`**, **`광진가격(60%)`**.  
수집된 품목명(`product_raw` / `product_canonical`)과 **ingest와 동일한 정규화 키**로 매칭해, **기간별 판매집계** 탭에서만 합계 금액에 반영합니다(단가 행을 대시보드에 뿌리지는 않습니다).

> 민감하거나 자주 바뀌는 경우 Git에 포함하지 않고 PC에만 두고 싶다면 `.gitignore`에 `단가표.csv`를 추가하면 됩니다.

### 웹 단가표(브랜드/이미지 포함) 만들기

`mu-tomo.com`의 `ALL PRODUCTS` 목록에서 **상품명/가격/썸네일**을 뽑아 CSV로 정리할 수 있습니다.

```powershell
python tools\crawl_mutomo_prices.py --out 단가표_from_web_detailed.csv
```

- 결과에는 `brand`(예: `TAKT`, `mono`, `MUTOMO` 등)와 `image_url`, `url`이 포함되어 있어서 **우리 제품만 남기고 삭제**하기 쉽습니다.
- 정리 후에는 `엑셀상품명/판매가격/광진가격(60%)` 열만 남겨 `단가표.csv`로 저장하면 대시보드 집계에 바로 반영됩니다.

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

대시보드 실행 시 `backups\`에 DB 복사가 자동으로 생길 수 있습니다.

- **자동 백업 빈도**: 로컬 시간 기준 **하루 최대 4번** (6시간 구간당 1회)
- **자동 삭제(보관 기간)**: **30일** 지난 백업은 삭제 (파일명 타임스탬프 기준)

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
