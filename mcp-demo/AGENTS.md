## 프로젝트

이 프로젝트는 부산신항 접안 스케줄 모니터링 시스템을 위한 포트폴리오용 데모입니다.

목표는 Databricks MCP 기반 Text-to-SQL 흐름을 보여주는 것입니다.

자연어 질문 -> Codex -> Databricks DBSQL MCP Server -> Unity Catalog 데모 View -> 간결한 한국어 답변

## Databricks MCP 사용 규칙

Databricks SQL MCP Server를 사용할 때는 다음 규칙을 따릅니다.

1. 반드시 `execute_sql_read_only` 도구만 사용합니다.
2. `execute_sql` 도구는 절대 사용하지 않습니다.
3. `SELECT`, `SHOW`, `DESCRIBE` 문만 실행합니다.
4. MCP를 통해 `INSERT`, `UPDATE`, `DELETE`, `DROP`, `CREATE`, `ALTER`, `TRUNCATE`, `MERGE` 또는 권한 변경 SQL을 절대 실행하지 않습니다.
5. 조회 결과는 10행 이하로 제한합니다.
6. 생성한 SQL을 실행하기 전에, 실행할 SQL을 먼저 보여줍니다.
7. 사용자가 SQL을 직접 제공한 경우, SQL 생성 설명은 생략하고 해당 SQL이 이 규칙을 따를 때만 실행합니다.
8. 설명은 최소화하고, 핵심 결과만 한국어로 요약합니다.
9. 사용자가 `SHOW` 또는 `DESCRIBE`로 사용 가능한 객체를 확인하라고 명시적으로 요청하지 않는 한, View 테이블만 조회합니다.

## 허용된 Databricks 객체

포트폴리오 데모 질문에는 아래 데모 View만 사용합니다.

- `dt4_project2_team3_databricks.gold.v_demo_terminal_status`
- `dt4_project2_team3_databricks.gold.v_demo_arrival_within_2h`
- `dt4_project2_team3_databricks.gold.v_demo_schedule_change_summary`

raw, bronze, silver 또는 원본 gold 테이블은 조회하지 않습니다.  
스키마 확인 또는 문제 해결이 필요한 경우에도 `execute_sql_read_only`를 통해 `SHOW` 또는 `DESCRIBE` 문만 사용합니다.

## 기본 데모 질문

아래와 같은 질문에는 데모 View를 사용해 답변합니다.

- 부두별 선박 수와 총 작업량을 요약해줘.
- 2시간 이내 입항 예정 선박을 부두별로 정리해줘.
- 스케줄 변경 유형별 건수를 요약해줘.

## 응답 스타일

- 핵심 결과만 보여줍니다.
- 쿼리가 느릴 수 있다면 더 좁은 범위의 쿼리나 데모 View 사용을 제안합니다.
- 필요한 데모 View가 존재하지 않는 경우, 먼저 실행해야 할 SQL 파일을 안내합니다.