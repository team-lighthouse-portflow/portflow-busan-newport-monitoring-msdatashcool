# Databricks MCP Text-to-SQL Demo

> 부산신항 접안 스케줄 모니터링 프로젝트에 Databricks DBSQL MCP Server와 Codex를 연결하여, 자연어 기반 DB 질의 흐름을 실험한 데모입니다.

---

## 🇰🇷 프로젝트 개요

기존 프로젝트는 부산신항 7개 부두의 접안 스케줄 데이터를 수집·정제하고, Power BI 대시보드로 시각화하는 구조였습니다.

이 데모에서는 기존 Databricks Gold 테이블을 기반으로, Codex를 MCP Client로 연결하여 자연어 질문을 SQL 질의로 변환하고 Databricks SQL Warehouse를 통해 결과를 조회하는 구조를 실험했습니다.

---

## 🧩 Architecture

```text
User Question
    ↓
Codex
    ↓
Databricks DBSQL MCP Server
    ↓
Databricks SQL Warehouse
    ↓
Unity Catalog Gold Tables / Views
    ↓
Read-only SQL Result
````

---

## 🔌 MCP Connection Flow

### 1. Databricks MCP Server 확인

Databricks Workspace에서 `DBSQL MCP Server`를 확인하고, Codex에서 사용할 MCP Server URL을 준비했습니다.

```text
Databricks Workspace
→ MCP Server
→ DBSQL MCP Server URL 확인
```

---

### 2. SQL Warehouse 활성화

MCP Server가 SQL을 실행하려면 Databricks SQL Warehouse가 실행 중이어야 합니다.

이번 데모에서는 `Starter Warehouse`를 사용했으며, Codex가 MCP를 통해 SQL을 요청하면 SQL Warehouse가 실제 컴퓨트 역할을 수행합니다.

```text
Codex
→ DBSQL MCP Server
→ SQL Warehouse
→ Unity Catalog Tables
```

---

### 3. PAT 토큰 생성

Codex에서 Databricks MCP Server에 접근하기 위해 Databricks Personal Access Token을 생성했습니다.

```text
Token name: codex-databricks-mcp-demo
Scope: BI Tools
Lifetime: 7 days
```

토큰 값은 보안상 GitHub, Notion, README에 남기지 않았습니다.

---

### 4. Windows 환경 변수 등록

생성한 PAT 토큰은 로컬 PC의 Windows 환경 변수에 저장했습니다.

```text
Variable name: DATABRICKS_TOKEN
Variable value: Databricks PAT
```

Codex에는 실제 토큰 값을 직접 입력하지 않고, 환경 변수 이름만 연결했습니다.

---

### 5. Codex MCP Client 연결

Codex에서 Databricks MCP Server를 추가했습니다.

```text
Name: databricks-sql
Type: Streamable HTTP
URL: Databricks DBSQL MCP Server URL
Bearer Token Env Var: DATABRICKS_TOKEN
```

이후 Codex가 Databricks MCP Server를 통해 SQL Warehouse와 Unity Catalog 테이블에 접근할 수 있는 구조를 구성했습니다.

---

## 🛡️ Read-only Query Rule

보안과 안정성을 위해 Codex에서는 읽기 전용 SQL 도구만 사용하도록 제한했습니다.

```text
Allowed tool:
- execute_sql_read_only

Allowed SQL:
- SELECT
- SHOW
- DESCRIBE

Blocked SQL:
- INSERT
- UPDATE
- DELETE
- DROP
- CREATE
- ALTER
- TRUNCATE
- MERGE
```

---

## 🧠 MCP vs RAG

이 데모는 일반적인 문서 기반 RAG가 아닙니다.

일반적인 RAG는 PDF, 문서, 텍스트를 chunk로 나눈 뒤 Vector DB에서 검색하여 답변을 생성합니다.

반면 이 데모는 Databricks의 정형 테이블을 대상으로 자연어 질문을 SQL로 변환하고, SQL Warehouse를 통해 결과를 조회하는 구조입니다.

```text
Document RAG:
Documents → Chunking → Vector DB → Retrieval → Answer

This Demo:
Natural Language → Text-to-SQL → SQL Warehouse → Table Query → Answer
```

따라서 이 작업은 **RAG보다는 MCP 기반 Text-to-SQL 데모**에 가깝습니다.

---

## 📊 Example Query

예시 질문:

```text
부두별 선박 수와 총 작업량을 요약해줘.
```

예상 SQL 흐름:

```sql
SELECT
  terminal_id,
  COUNT(*) AS vessel_count,
  SUM(total_workload) AS total_workload
FROM dt4_project2_team3_databricks.gold.gold_today_terminal_schedule
GROUP BY terminal_id
ORDER BY terminal_id;
```

---

## 📁 Suggested Folder Structure

```text
mcp-demo/
├─ README.md
├─ AGENTS.md
├─ 01_create_demo_views.sql

```

---

## 📝 Notes

* Azure Databricks 리소스는 프로젝트 종료 후 삭제되어 현재 동일 MCP 쿼리를 재실행할 수 없습니다.
* 이 폴더는 실제 운영 서비스가 아니라, 프로젝트 종료 후 포트폴리오 확장을 위해 정리한 MCP 기반 Text-to-SQL 연결 기록입니다.
* 인증 토큰과 민감 정보는 저장하지 않았습니다.

---
