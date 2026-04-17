## Mutomo 오픈마켓 판매관리 (초안)

`.xlsx` 주문 접수 파일을 계속 받아서 **주문/품목으로 파싱**하고, 상품명 불일치(표기 흔들림)를 **별칭 매핑 + 유사도 추천**으로 정리하는 최소 골격입니다.

### 빠른 시작

의존성 설치:

```bash
python -m pip install -r requirements.txt
```

엑셀 수집(`order_list` 폴더의 `.xlsx`를 읽어서 SQLite 생성):

```bash
python ingest_xlsx.py --db mutomo.sqlite --aliases product_aliases.yml
```

대시보드 실행:

```bash
python -m streamlit run dashboard.py
```

### 이 프로그램이 저장하는 데이터

- **DB 파일**: `mutomo.sqlite`
  - 접수/출고/클레임/마감/납품취소 같은 상태
  - 출고 처리 시각(`shipped_at`)
  - 클레임/메모(`special_issue`)
- **상품명 별칭**: `product_aliases.yml`

USB/다른 PC로 옮길 때는 기본적으로 이 두 파일이 가장 중요합니다.

### 화면 구성(업무 흐름)

- **오늘접수**: 선택한 기준(구매일자 또는 수집시각)으로 오늘/전날 목록을 보여줍니다.
- **전체 접수 목록**: 전체 주문 목록을 보여주고, 행을 선택하면 **왼쪽 사이드바에 상세**가 뜹니다.
- **왼쪽 검색**: 받는분 이름으로 검색 → 여러 건 선택(멀티선택) → 상태 변경/메모 저장이 가능합니다.

### 상태(접수/출고/클레임/마감/납품취소) 운영 방법

주문을 **검색 선택**하거나 **전체 접수 목록에서 선택**한 뒤, 왼쪽 버튼으로 상태를 바꿉니다.

- **출고**: `선택 건 출고 처리`
  - 출고 시 `shipped_at`이 기록됩니다.
- **출고 취소(실수 처리)**: `접수`
  - 상태를 `접수`로 되돌리고, **`shipped_at`을 NULL로 초기화**합니다.
  - 그래서 “오늘 출고 엑셀”에도 다시 포함되지 않습니다.
- **클레임**: `클레임`
- **마감**: `마감`
- **납품취소**: `납품취소`

### 클레임 내용(메모) 작성 위치

왼쪽 사이드바에 있는 **`클레임/특이사항`** 입력칸에 적고 **`특이사항 저장`**을 누르면,
선택된 주문의 `special_issue`에 저장됩니다.

### 출고 목록 엑셀 다운로드(피킹리스트/로젠택배 포함)

사이드바의 **`출고 목록 엑셀`**에서 다운로드합니다.

- **출고 기준 날짜**를 선택해서, 해당 날짜에 출고 처리된 건만 뽑을 수 있습니다.
- 파일 안에 시트가 함께 생성됩니다.
  - `피킹리스트`: A4 가로 인쇄용
  - `출고_품목`: 준비할 품목/수량(품목별 합산)
  - `로젠택배`: 택배 업로드/출력용 서식
- **출고 포함 기준**: `status=출고` 이면서 `shipped_at` 날짜가 선택한 날짜인 건만 포함합니다.

### 동명이인(같은 이름) 구분

이름으로 구분하지 않고 **`order_id`(원본파일/그룹번호/시작행 기반)**로 주문을 식별합니다.
그래서 동명이인이 있어도 각각 별도로 선택/출고/클레임/메모가 저장됩니다.

### 상품명(제품명) 불일치 처리 방식

- `ingest_xlsx.py`는 `product_raw`(원본)에서 정규화된 `product_key`를 만들고,
- `product_aliases.yml`에 정의된 별칭으로 `product_canonical`(표준 제품명)을 붙입니다.
- 별칭이 없으면 `suggested_canonical` / `suggestion_score`로 “가장 가까운 표준명 후보”를 보여주고,
  동시에 `unknown_products.csv`에 미매핑 키 목록을 저장합니다.

### 별칭 추가 예시

`product_aliases.yml`:

```yaml
"키큰수납장":
  - "키큰 수납장"
  - "키 큰 수납장"
```

### 다른 PC에서 사용(요약)

1) 폴더 전체를 복사(또는 zip으로 압축해서 이동)  
2) 새 PC에서 파이썬 설치 후:

```bash
cd C:\python\mutomo
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m streamlit run dashboard.py
```

### USB로 옮기기(압축 파일)

현재 PC에서 `C:\python\mutomo_portable.zip` 형태로 압축해서 USB에 복사하면 됩니다.

### Docker로 실행(선택)

Docker Desktop 설치 후, 프로젝트 폴더에서:

```bash
docker compose up --build
```

브라우저에서 `http://localhost:8501` 접속.

- 데이터 유지: `docker-compose.yml`이 `mutomo.sqlite`를 호스트 파일로 마운트해서 **컨테이너를 지워도 DB는 남습니다.**

### DB 자동 백업(추천)

원본 엑셀을 지워도 운영은 가능하지만, 대신 **DB(`mutomo.sqlite`)를 자동으로 백업**하는 게 안전합니다.

- **앱 자동백업**: 대시보드를 켤 때(세션 기준) **하루 1번** `backups/` 폴더에 자동 백업을 남깁니다.
- **작업 스케줄러(완전 자동)**: 윈도우 작업 스케줄러에 아래 명령을 등록하면 됩니다.

```bash
cd C:\python\mutomo
python backup_db.py --db mutomo.sqlite --out backups --keep-days 30
```

### 데이터 초기화(처음부터 다시 시작)

완전히 새로 시작하려면 **DB 파일만 초기화**하면 됩니다.

1) Streamlit을 종료  
2) `mutomo.sqlite`를 삭제하거나 이름 변경(백업 권장)

```bash
cd C:\python\mutomo
ren mutomo.sqlite mutomo_old.sqlite
```

3) 다시 수집 후 실행

```bash
python ingest_xlsx.py --db mutomo.sqlite --aliases product_aliases.yml
python -m streamlit run dashboard.py
```

