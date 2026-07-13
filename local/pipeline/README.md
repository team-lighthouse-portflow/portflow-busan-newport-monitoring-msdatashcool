# Local Pipeline

PySpark + Delta Lake 기반 Bronze→Silver→Gold 파이프라인. Azure Databricks 노트북 로직을 로컬 실행용으로 포팅했습니다.

## 파일 구성

| 파일 | 역할 | 실행 주기 |
|---|---|---|
| `spark_session.py` | 공통 SparkSession 빌더 (JDK 17 자동탐색, Delta 확장 등록) | - (import 전용) |
| `00_historical_to_silver.py` | 과거 선석 이력(`N부두_YYYY.xls[x]`) → `silver_berth_schedule_historical` | 1회 |
| `01_raw_to_bronze.py` | 7개 부두 원본(xlsx/xls/xml) → `bronze_berth_schedule` | 매시간 |
| `02_bronze_to_silver.py` | Bronze → 표준 스키마 Silver 변환 + 검증(`silver_error_log`) | 매시간 |
| `03_container_to_silver.py` | 연도별 월간 컨테이너 실적 xlsx → `silver_monthly_container_{year}` | 신규 파일 유입 시 |
| `04_silver_to_gold.py` | Silver → Gold 6종 테이블 생성 (통합 스케줄, 변경이력, 시간대별 작업량, 오늘 스케줄, 월별 컨테이너, ML feature table) | 매시간 |
| `05_gold_to_postgres.py` | Gold Delta 테이블 → PostgreSQL(`gold` 스키마) 적재 (Spark JDBC) | 매시간 |
| `predict_cargo_from_gold.py` | PostgreSQL `gold.gold_integrated_schedule` 기반 XGBoost+Ridge 앙상블 물동량 예측, SCFI 시나리오/SHAP 분석, 결과를 `gold.cargo_forecast(_monthly)`로 적재 | 수동 (필요 시) |
| `run_pipeline.py` | 01→02→04→05 순차 실행 오케스트레이터, 단계별 로그를 `logs/`에 기록 | Task Scheduler가 매시간 호출 |
| `run_pipeline.bat` | Task Scheduler 등록용 배치 (venv python으로 `run_pipeline.py` 실행) | - |
| `run_test.bat` | 수동 실행/디버깅용 배치 (콘솔 출력 확인) | - |
| `check_env.py` | Spark+Delta 환경 정상 동작 확인용 스모크 테스트 | 최초 1회 |
| `inspect_results.py` | Bronze/Silver 테이블 건수, 유효성, 터미널별 분포 점검 | 필요 시 |

> `00`, `03`은 매시간 파이프라인(`run_pipeline.py`)에 포함되어 있지 않습니다. `00`은 이력 데이터 최초 적재용,
> `03`은 신규 월간 실적 파일이 들어왔을 때만 수동 실행합니다.

## 실행 순서 (최초 셋업)

```bash
# 1. 가상환경 + 패키지
python -m venv .venv
.venv\Scripts\activate
pip install -r ../requirements.txt

# 2. 환경변수 설정 (.env.example 참고)
copy .env.example .env
# .env 파일 열어서 PG_PASSWORD 등 실제 값 입력

# 3. Spark/Delta 동작 확인
python check_env.py

# 4. 이력 데이터 적재 (최초 1회)
python 00_historical_to_silver.py

# 5. 월간 컨테이너 실적 적재 (raw 파일 있을 때)
python 03_container_to_silver.py

# 6. 스케줄 파이프라인 순차 실행
python run_pipeline.py

# 7. 물동량 예측 (Gold까지 적재된 후 수동 실행)
python predict_cargo_from_gold.py
```

## Windows Task Scheduler 등록 (매시간 자동 실행)

1. 작업 스케줄러 → 새 작업 만들기
2. 트리거: 매시간 반복
3. 동작: 프로그램/스크립트에 `run_pipeline.bat` 절대경로 지정
4. **"시작 위치(Start In)"를 반드시 프로젝트 루트로 지정** — 상대경로 깨짐 방지용으로
   `run_pipeline.py` 내부에서도 `os.chdir(스크립트 위치)` 처리는 되어 있지만, `.bat`도 동일하게 맞춰둠

## 환경변수 (.env)

PostgreSQL 접속 정보와 비밀번호는 코드에 하드코딩하지 않고 `.env`로 분리했습니다.
`.env`는 `.gitignore`에 포함되어 커밋되지 않으니, 새로 셋업하는 팀원은 `.env.example`을 복사해서 채워야 합니다.

```python
# 05_gold_to_postgres.py, predict_cargo_from_gold.py 상단
from dotenv import load_dotenv
load_dotenv()

PG_HOST = os.environ["PG_HOST"]
PG_PORT = os.environ["PG_PORT"]
PG_DB   = os.environ["PG_DB"]
PG_USER = os.environ["PG_USER"]
PG_PWD  = os.environ["PG_PASSWORD"]
```

## 알려진 이슈 / 트러블슈팅

- **JAVA_HOME 미설정 시 JVM not found**: `spark_session.py`가 pyspark import 전에 JDK 17 경로를 자동 탐색해 설정함.
  자동 탐색 실패 시 `winget install EclipseAdoptium.Temurin.17.JDK`로 설치.
- **Windows에서 Spark 종료 시 셧다운 훅 노이즈**: 각 스크립트 끝에 `sys.stdout.flush()` + `os._exit(0)` 처리.
- **pandas Timestamp → Spark TimestampType 거부**: `.to_pydatetime()`으로 변환 후 `dtype=object` Series로 감싸서 전달.
- **3부두 이력 파일 헤더 밀림**: `00_historical_to_silver.py`의 `fix_header()`가 제목행을 자동 감지해 보정.
- **BCT(6부두) 인증**: 실시간 수집(Power Automate Desktop 단계, 이 폴더 밖)에서 Nexacro SPA를 기존 쿠키 기반
  동기 XHR로 처리. 관련 상세는 `docs/troubleshooting.md` 참고.
