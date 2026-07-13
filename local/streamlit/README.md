# Streamlit Dashboard (Local)

Power BI(Azure 트랙)의 로컬 대응. `gold` 스키마(PostgreSQL)를 SQLAlchemy로 조회해 4개 화면을 제공합니다.

## 화면 구성

| 화면 | 소스 테이블 | 설명 |
|---|---|---|
| 통합 선석 스케줄 | `gold.gold_integrated_schedule` | 7개 부두 스케줄을 하나의 보드로 통합 조회 |
| 스케줄 변경 알림 | `gold.gold_schedule_change_history` | ETA/ETD/선석/작업량 변경 이력, 신규/변경 건 하이라이트 |
| 부두별 작업량 | `gold.gold_hourly_terminal_workload` | 시간대별 예상 작업량(양하/적하/이적) |
| 물동량 예측 | `gold.cargo_forecast`, `gold.cargo_forecast_monthly` | SCFI 시나리오(Flat/Rise/Fall)별 물동량 예측 |

## 실행

```bash
cd ../pipeline   # .env 위치 기준 (또는 streamlit 폴더에도 .env 복사)
streamlit run app.py
```

> `app.py`는 PostgreSQL 접속 시 `pipeline/.env.example`과 동일한 환경변수(`PG_HOST`, `PG_PORT`, `PG_DB`,
> `PG_USER`, `PG_PASSWORD`)를 사용합니다. `05_gold_to_postgres.py` + `predict_cargo_from_gold.py`가
> 먼저 실행되어 `gold` 스키마에 데이터가 있어야 정상 조회됩니다.

## Power BI 대비 특징

- 무료(라이선스 비용 없음), 온프레미스로 완전히 로컬에서 구동
- ML 예측 결과(`cargo_forecast`)와 같은 DB에서 직접 연결 — 별도 게이트웨이/새로고침 설정 불필요
- 단, Power BI 대비 거버넌스 기능(행 수준 보안, 조직 전체 배포/공유)은 제공하지 않음 —
  Power BI 무료 티어는 보고서 공유가 안 되므로 실제 배포하려면 유료 전환이 필요하다는 점과 대비됨.
  자세한 비교는 [`docs/azure_vs_local_comparison.md`](../../docs/azure_vs_local_comparison.md) 참고.
