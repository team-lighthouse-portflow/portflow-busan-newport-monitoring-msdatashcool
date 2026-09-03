## Project

This project is a portfolio demo for a Busan New Port berth schedule monitoring system.

The goal is to demonstrate a Databricks MCP based Text-to-SQL workflow:
Natural language question -> Codex -> Databricks DBSQL MCP Server -> Unity Catalog demo views -> concise Korean answer.

## Databricks MCP rules

When using the Databricks SQL MCP server:

1. Use only the `execute_sql_read_only` tool.
2. Never use the `execute_sql` tool.
3. Only run `SELECT`, `SHOW`, and `DESCRIBE` statements.
4. Never run `INSERT`, `UPDATE`, `DELETE`, `DROP`, `CREATE`, `ALTER`, `TRUNCATE`, `MERGE`, or permission-changing SQL through MCP.
5. Limit result sets to 10 rows or fewer.
6. Before executing generated SQL, show the SQL that will be executed.
7. If the user directly provides SQL, skip the SQL generation explanation and execute it only if it follows these rules.
8. Keep explanations minimal and summarize only the key result in Korean.
9. Query only view tables unless the user is explicitly asking to inspect available objects with `SHOW` or `DESCRIBE`.

## Allowed Databricks objects

For portfolio demo questions, use only these demo views:

- `dt4_project2_team3_databricks.gold.v_demo_terminal_status`
- `dt4_project2_team3_databricks.gold.v_demo_arrival_within_2h`
- `dt4_project2_team3_databricks.gold.v_demo_schedule_change_summary`

Do not query raw, bronze, silver, or original gold tables. For schema inspection or troubleshooting, use only `SHOW` or `DESCRIBE` statements through `execute_sql_read_only`.

## Default demo questions

Use the demo views to answer questions like:

- 부두별 선박 수와 총 작업량을 요약해줘.
- 2시간 이내 입항 예정 선박을 부두별로 정리해줘.
- 스케줄 변경 유형별 건수를 요약해줘.

## Response style

- Show only the key result.
- If a query might be slow, suggest a narrower query or a demo view.
- If a required demo view does not exist, tell the user which SQL file should be run first.
