## 01. Raw Schedule Files to Bronze Staging Parquet

이 단계는 Azure Blob Storage에 수집된 부산신항 7개 부두 선석 스케줄 파일을 Bronze Layer에 적재하기 전,
파일 형식별로 읽고 헤더 규칙을 정리하여 Parquet 기반 Staging 산출물로 저장하는 전처리 단계입니다.

### Input

- Azure Blob Storage의 7개 부두 선석 스케줄 파일
- 테스트 기준: 7개 부두 × 2개 스냅샷 = 14개 파일
- 파일 형식:
  - `.xlsx`
  - `.xls` 또는 HTML table 기반 파일
  - `.xml` Nexacro Dataset 형식

### Process

1. Azure Blob Storage에서 부두별 스케줄 파일을 읽습니다.
2. 파일 확장자와 실제 내부 구조를 기준으로 포맷을 판별합니다.
3. Excel, HTML table, XML 파일을 각각 파싱합니다.
4. 부두별 헤더 행 규칙을 맞춰 DataFrame 형태로 변환합니다.
5. 원본 컬럼명은 그대로 보존합니다.
6. Bronze 적재 전 단계의 Staging Parquet 파일로 저장합니다.

### Output

- 파일 단위 DataFrame 리스트
- Bronze 적재용 Staging Parquet 파일

### Scope

이 단계에서는 원본 파일을 안정적으로 읽고 Parquet 형태로 넘기는 것에 집중합니다.

- 수행함:
  - 파일 포맷 판별
  - xlsx / html-xls / xml 파싱
  - 헤더 규칙 정리
  - 원본 컬럼명 보존
  - Parquet 임시 저장

- 수행하지 않음:
  - 컬럼명 표준화
  - 데이터 타입 정제
  - 비즈니스 키 생성
  - row_hash, snapshot_id 등 추적 메타컬럼 생성
  - 검증 실패 행 분리

수행하지 않는 항목은 Bronze/Silver 단계에서 처리합니다.

### Special Case: Terminal 6

6부두 파일은 일반 Excel 다운로드가 어려운 구조였기 때문에,
웹 스크래핑을 통해 Nexacro Platform 기반 XML 파일로 수집했습니다.

XML 파일은 `ColumnInfo`에 정의된 컬럼 순서를 기준으로 Row 값을 매핑하여 DataFrame으로 변환했습니다.