# Local Track

Azure 트랙(Databricks + ADLS Gen2 + Azure SQL + Power BI)과 **동일한 Medallion 아키텍처(Bronze→Silver→Gold)**를
로컬/오픈소스 스택으로 1:1 재구현한 버전입니다. Azure vs Local 비교 평가를 위해
컬럼 매핑, 검증 로직, Gold 집계 SQL 등 도메인 로직은 Azure 노트북과 최대한 동일하게 유지했습니다.

## 구성

| 폴더 | 내용 |
|---|---|
| [`pipeline/`](./pipeline) | PySpark + Delta Lake 기반 Bronze→Silver→Gold 파이프라인, PostgreSQL 적재, 물동량 예측 |
| [`postgresql/`](./postgresql) | 로컬 서빙 레이어(PostgreSQL) 스키마 설명 |
| [`streamlit/`](./streamlit) | Streamlit 대시보드 (Power BI의 로컬 대응) |

## 기술 스택

| 레이어 | Azure | Local |
|---|---|---|
| 컴퓨트 | Databricks Premium (Standard_D4ds_v5, Photon, All-Purpose) | PySpark 3.5.3 + delta-spark 3.3.2 |
| 스토리지 | ADLS Gen2, Azure SQL DB serverless | 로컬 폴더(Blob 경로 구조 미러링) + PostgreSQL |
| 오케스트레이션 | Auto Loader, Power Automate Cloud | Windows Task Scheduler, Power Automate Desktop |
| 서빙/BI | Power BI | Streamlit + Plotly |
| ML 트래킹 | MLflow (Databricks-managed) | MLflow (local) |

## 왜 Delta Lake인가

- Gold 테이블의 변경 이력 감지(`gold_schedule_change_history`)는 스냅샷 간 MERGE/upsert 성격의 연산이 필요한데,
  이건 Delta의 트랜잭션 로그가 있어야 안정적으로 처리됩니다.
- 무엇보다 **Databricks 노트북과 코드 구조를 최대한 동일하게 유지**해야 Azure vs Local 성능/비용 비교가
  "같은 로직, 다른 인프라"라는 전제 위에서 성립합니다. DuckDB/Parquet으로 가면 이 전제가 깨집니다.

## Azure 대비 알려진 차이점

- **분산처리 이점 미실현**: 데이터 규모가 MiB 단위라 Spark의 분산 처리 장점이 로컬/Azure 어느 쪽에서도
  실제로 발휘되지 않습니다. 성능 차이는 대부분 클러스터 warm-up 여부에서 옵니다.
- **파티셔닝 키 차이**: Azure는 `gold_key`(terminal_id 미포함), Local은 `business_key`(terminal_id 포함)를
  변경 감지 파티션 키로 사용 — 결과 완전 동일성이 보장되지 않는 잔여 이슈로 남아있음.
- 자세한 비교는 [`docs/azure_vs_local_comparison.md`](../docs/azure_vs_local_comparison.md) 참고.

## 로컬 환경 요구사항

- Python 3.10+ (venv 권장)
- JDK 17 (Eclipse Temurin) — `spark_session.py`가 `C:\Program Files\Eclipse Adoptium` 등에서 자동 탐색
- PostgreSQL (DB `busan_port`, 스키마 `gold` 사전 생성 필요)
- `pip install -r requirements.txt`

실행 방법은 [`pipeline/README.md`](./pipeline/README.md) 참고.
