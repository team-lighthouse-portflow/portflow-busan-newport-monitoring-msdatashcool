# PostgreSQL (Local Serving Layer)

Azure SQL DB(serverless)의 로컬 대응. `05_gold_to_postgres.py`가 Gold Delta 테이블들을 Spark JDBC로 적재합니다.

## 사전 준비

```sql
-- PostgreSQL에 접속해서 1회만 실행
CREATE DATABASE busan_port;
```

스키마(`gold`)는 `05_gold_to_postgres.py`가 최초 실행 시 `psycopg2`로 자동 생성합니다
(`CREATE SCHEMA IF NOT EXISTS gold;`). 별도 DDL 파일을 두지 않은 이유입니다.

## 테이블 목록 (`gold` 스키마)

| 테이블 | 적재 스크립트 | 내용 |
|---|---|---|
| `gold_integrated_schedule` | `05_gold_to_postgres.py` | 7개 부두 선석 스케줄 통합 |
| `gold_schedule_change_history` | 〃 | 스케줄 변경 이력 (NEW/CHANGED) |
| `gold_hourly_terminal_workload` | 〃 | 시간대별 예상 작업량 |
| `gold_today_terminal_schedule` | 〃 | 오늘(work_date) 기준 최신 스케줄 |
| `gold_monthly_container` | 〃 | 2022~2026 월별 컨테이너 물동량 |
| `gold_ml_feature_table` | 〃 | ML 학습용 피처 테이블 |
| `cargo_forecast` | `predict_cargo_from_gold.py` | 부두×시나리오별 반기/연간 예측 요약 |
| `cargo_forecast_monthly` | 〃 | 부두×시나리오×월별 예측 상세 |

## 적재 방식 주의점

- 매 실행마다 **overwrite(테이블 drop 후 재생성)** 방식입니다. 증분 적재가 아니므로,
  Gold Delta 테이블의 최신 스냅샷 전체가 매번 PostgreSQL에 그대로 반영됩니다.
- `array` 타입 컬럼(`berth_list`, `vessel_list`)은 JDBC 적재 전에 `", "`로 join해서 문자열로 변환
  (PostgreSQL JDBC가 array 타입을 직접 지원하지 않아서).

## Azure SQL DB 대비 알려진 갭

- Azure SQL 쪽은 public network access가 열려 있고 AAD 전용 인증이 강제되지 않은 상태 —
  "관리형 서비스라 보안이 낫다"는 주장과 실제 as-built 설정 사이에 갭이 있음. 발표 시 한계로 명시.
