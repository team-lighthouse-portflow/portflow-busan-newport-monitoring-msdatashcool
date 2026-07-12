
/* =============================================================================
  Databricks Unity Catalog External Location 권한 부여 스크립트
  =============================================================================
  
  목적
  -----------------------------------------------------------------------------
  Azure Blob Storage 컨테이너와 연결된 Databricks External Location 및
  Catalog / Schema에 대해 팀원별 작업 권한을 부여한다.

  적용 범위
  -----------------------------------------------------------------------------
  1. External Location 권한
     - BROWSE
     - READ FILES
     - WRITE FILES
     - CREATE EXTERNAL TABLE
     - CREATE EXTERNAL VOLUME

  2. Catalog / Schema 권한
     - USE CATALOG
     - USE SCHEMA
     - SELECT
     - MODIFY
     - CREATE TABLE

  설계 의도
  -----------------------------------------------------------------------------
  본 프로젝트에서는 Raw, Demo Raw, Raw Schedule, Container Operation 등
  컨테이너별 데이터 저장 목적이 달랐기 때문에, 각 External Location에
  필요한 권한을 명시적으로 부여했다.

  또한 Bronze / Silver / Gold 계층별 테이블 생성 및 조회가 가능하도록
  Catalog와 Schema 단위 권한도 함께 부여했다.

  운영 시 개선 방향
  -----------------------------------------------------------------------------
  MVP 단계에서는 팀원 개인 계정 단위로 권한을 부여했지만,
  실제 운영 환경에서는 Microsoft Entra ID 그룹 기반 권한 관리를 권장한다.

  예시:
    - Data Engineer Group  : Bronze / Silver / Gold 생성 및 수정
    - BI Developer Group   : Gold / Mart 조회
    - Viewer Group         : Mart View 조회 전용

  보안 주의사항
  -----------------------------------------------------------------------------
  본 스크립트에는 다음 정보를 포함하지 않는다.

    - Storage Account Key
    - SAS Token
    - Databricks Token
    - SQL DB Password
    - Connection String
    - 개인 계정 비밀번호
============================================================================= */

-- =============================================================================
-- 1. External Location 권한 부여
-- =============================================================================

-- 1. External Location에서 외부 테이블을 생성할 수 있는 권한을 부여한다.
GRANT CREATE EXTERNAL TABLE
ON EXTERNAL LOCATION `<USER_OR_GROUP External Location Name>`
TO `<USER_OR_GROUP@example.com>`;

-- 2. External Location 목록과 메타데이터를 탐색할 수 있는 권한을 부여한다.
GRANT BROWSE
ON EXTERNAL LOCATION `<USER_OR_GROUP External Location Name>`
TO `<USER_OR_GROUP@example.com>`;

-- 3. External Location에 저장된 파일을 읽을 수 있는 권한을 부여한다.
GRANT READ FILES
ON EXTERNAL LOCATION `<USER_OR_GROUP External Location Name>`
TO `<USER_OR_GROUP@example.com>`;

-- 4. External Location에 파일을 쓰거나 수정할 수 있는 권한을 부여한다.
GRANT WRITE FILES
ON EXTERNAL LOCATION `<USER_OR_GROUP External Location Name>`
TO `<USER_OR_GROUP@example.com>`;



-- =============================================================================
-- 2. Catalog / Schema 권한 부여
-- =============================================================================

-- 1. 지정한 Catalog에 접근할 수 있도록 USE CATALOG 권한을 부여한다.
GRANT USE CATALOG
ON CATALOG `<USER_OR_GROUP Catalog Name>`
TO `<USER_OR_GROUP@example.com>`;


-- 2. Bronze/Silver/Gold Schema에 접근할 수 있도록 USE SCHEMA 권한을 부여한다.
GRANT USE SCHEMA
ON SCHEMA `<USER_OR_GROUP Catalog Name>.`bronze`
TO `<USER_OR_GROUP@example.com>`;

GRANT USE SCHEMA
ON SCHEMA `<USER_OR_GROUP Catalog Name>.`silver`
TO `<USER_OR_GROUP@example.com>`;

GRANT USE SCHEMA
ON SCHEMA `<USER_OR_GROUP Catalog Name>.`gold`
TO `<USER_OR_GROUP@example.com>`;


-- 3. Bronze/Silver/Gold Schema에서 테이블 조회·수정·생성이 가능하도록 작업 권한을 부여한다.
GRANT SELECT, MODIFY, CREATE TABLE
ON SCHEMA `<USER_OR_GROUP Catalog Name>.`bronze`
TO `<USER_OR_GROUP@example.com>`;

GRANT SELECT, MODIFY, CREATE TABLE
ON SCHEMA `<USER_OR_GROUP Catalog Name>.`silver`
TO `<USER_OR_GROUP@example.com>`;

GRANT SELECT, MODIFY, CREATE TABLE
ON SCHEMA `<USER_OR_GROUP Catalog Name>.`gold`
TO `<USER_OR_GROUP@example.com>`;