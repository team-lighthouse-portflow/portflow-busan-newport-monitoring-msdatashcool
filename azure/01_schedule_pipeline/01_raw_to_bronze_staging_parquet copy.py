"""
01_raw_to_bronze_staging_parquet

목적:
- Azure Blob Storage에 저장된 7개 부두 선석 스케줄 파일을 읽어온다.
- xlsx, HTML 기반 xls, Nexacro XML 등 서로 다른 원본 파일 형식을 파싱한다.
- 원본 컬럼명은 보존한 상태로 부두별 헤더 규칙을 정리한다.
- Bronze 적재 전 단계에서 사용할 Staging Parquet 파일로 저장한다.

비고:
- 컬럼명 표준화는 Silver Layer에서 수행한다.
- snapshot_id, row_hash, business_key 등 추적용 메타컬럼은 Bronze/Silver 파이프라인에서 추가한다.
"""



# =================================
#  0. 패키지 및 라이브러리 불러오기
# ================================= 
%pip install openpyxl html5lib lxml beautifulsoup4 xlrd fsspec

dbutils.library.restartPython()

import os
import re
import pandas as pd
import numpy as np
import hashlib
import json
import xml.etree.ElementTree as ET
from io import StringIO
from openpyxl import load_workbook



# =================================
#  1. 파일 읽기
# ================================= 
# blob storage 컨테이너 이름
container = "raw-schedule"

# 스토리지 계정
storage_account = "[스토리지계정이름]"

blob_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/"

files = sorted(dbutils.fs.ls(blob_path), key=lambda x: x.name)
for f in files:
    print(f.name, f.size)



# =================================
#  2. 부두별 헤더규칙
# ================================= 
'''
no_header   : 원본에 헤더 없음 -> col_1, col_2... 부여, 0행부터 데이터
header_row  : header_row번째 행(0-base)이 헤더, 그 다음행부터 데이터
xml_custom  : xml(Nexacro 데이터셋) 전용, read_nexacro_xml에서 ColumnInfo 기준 처리
'''
TERMINAL_HEADER_RULES = {
    1: {"mode": "no_header"},                      # xls(html)
    2: {"mode": "no_header"},                      # xls(html)
    3: {"mode": "header_row", "header_row": 1},    # xlsx: 1행 제목 / 2행 헤더
    4: {"mode": "no_header"},                      # xls(html)
    5: {"mode": "header_row", "header_row": 0},    # xlsx: 1행 헤더
    6: {"mode": "xml_custom"},                     # xml - Nexacro ColumnInfo 기반, read_nexacro_xml에서 이미 헤더 확정
    7: {"mode": "header_row", "header_row": 0},    # xlsx: 1행 헤더
}


# ================================
#  3. 파일 파싱 과정은 함수로 모듈화하기 
# ================================= 
def copy_blob_file_to_local(blob_file_path):
    '''
    Blob 파일을 Workspace 공유 폴더로 복사하는 함수
    '''
    workspace_tmp_dir = "/Workspace/Shared/busan_port_project/01_schedule_pipeline/tmp_excel"
    dbutils.fs.mkdirs(workspace_tmp_dir)
 
    local_path = f"{workspace_tmp_dir}/{os.path.basename(blob_file_path)}"
    dbutils.fs.cp(blob_file_path, f"file:{local_path}")
 
    return local_path


def extract_terminal_no(file_name: str) -> int:
    """
    부두 번호를 추출하는 함수
    """
    m = re.search(r"terminal_(\d+)_", file_name)
    return int(m.group(1)) if m else None


def read_raw_table(local_path: str) -> pd.DataFrame:
    """
    로컬 파일 경로를 받아서 해당 파일의 원시 데이터를 DataFrame으로 반환하는 함수
    """
    file_name = os.path.basename(local_path)
 
    if file_name.endswith(".xlsx"):
        return pd.read_excel(local_path, header=None, engine="openpyxl")
 
    elif file_name.endswith(".xls"):
        with open(local_path, "rb") as f:
            content = f.read()
        content = content.replace(b"udf-8", b"utf-8").replace(b"UDF-8", b"UTF-8")
        try:
            html_text = content.decode("utf-8")
        except UnicodeDecodeError:
            html_text = content.decode("cp949", errors="replace")
        tables = pd.read_html(StringIO(html_text), header=None)
        return tables[0]
 
    elif file_name.endswith(".xml"):
        return read_nexacro_xml(local_path)
 
    else:
        raise ValueError(f"지원하지 않는 확장자: {file_name}")
    


def read_nexacro_xml(local_path: str) -> pd.DataFrame:
    """
    Nexacro Platform 데이터셋 XML 전용 파서.
    ColumnInfo에서 컬럼 순서를 먼저 확정한 뒤, 그 순서대로 Row를 채운다.
    -> 특정 행에 일부 Col 태그가 비어 있어도(예: 옵션값 없음) 컬럼이 밀리지 않는다.
    """
    import xml.etree.ElementTree as ET
 
    tree = ET.parse(local_path)
    root = tree.getroot()
 
    def clean(tag):
        return tag.split("}")[-1]
 
    # 1) ColumnInfo에서 컬럼 순서 확정
    column_order = []
    for elem in root.iter():
        if clean(elem.tag) == "ColumnInfo":
            for col in elem:
                if clean(col.tag) == "Column":
                    col_id = col.attrib.get("id")
                    if col_id:
                        column_order.append(col_id)
            break  # 첫 ColumnInfo만 사용 (Dataset이 여러 개일 경우 대비)
 
    if not column_order:
        raise ValueError("ColumnInfo에서 컬럼 정의를 찾지 못했습니다.")
 
    # 2) Row 단위로 값 채우기 (컬럼 순서 고정)
    rows = []
    for elem in root.iter():
        if clean(elem.tag) == "Row":
            row = {col_id: None for col_id in column_order}
            for child in elem:
                col_id = child.attrib.get("id")
                if col_id in row:
                    row[col_id] = child.text
            rows.append(row)
 
    # 3) 컬럼 순서를 명시적으로 지정해 DataFrame 생성
    df = pd.DataFrame(rows, columns=column_order)
    return df


def apply_header_rule(raw_df: pd.DataFrame, terminal_no: int) -> pd.DataFrame:
    """
    부두 규칙에 따라 "1행=컬럼명, 2행~=데이터"로 정제하는 함수
    """
    rule = TERMINAL_HEADER_RULES.get(terminal_no)
    if rule is None:
        raise ValueError(f"terminal_{terminal_no}에 대한 헤더 규칙이 없습니다.")
 
    mode = rule["mode"]
 
    if mode == "no_header":
        df = raw_df.copy()
        df.columns = [f"col_{i+1}" for i in range(df.shape[1])]
        return df.reset_index(drop=True)
 
    elif mode == "header_row":
        header_idx = rule["header_row"]
        header = raw_df.iloc[header_idx]
        df = raw_df.iloc[header_idx + 1:].copy()
        df.columns = header.values
        return df.reset_index(drop=True)
 
    elif mode == "xml_custom":
        # xml(Nexacro)은 read_raw_table -> read_nexacro_xml에서
        # ColumnInfo 기준으로 컬럼 순서를 이미 확정해 DataFrame을 만든 상태.
        # -> 추가 헤더 처리 불필요, 그대로 반환.
        return raw_df.reset_index(drop=True)
 
    else:
        raise ValueError(f"알 수 없는 모드: {mode}")
 
 
def process_one_file(local_path: str) -> dict:
    """
    로컬 파일 경로를 받아서, 부두번호 추출, 원시 데이터 읽기, 헤더 규칙 적용 후 정제된 DataFrame을 반환하는 함수.
    """
    file_name = os.path.basename(local_path)
    terminal_no = extract_terminal_no(file_name)
    if terminal_no is None:
        raise ValueError(f"파일명에서 부두번호를 추출할 수 없습니다: {file_name}")
 
    raw_df = read_raw_table(local_path)
    clean_df = apply_header_rule(raw_df, terminal_no)
 
    return {
        "file": file_name,
        "terminal_no": terminal_no,
        "df": clean_df,
    }


def build_bronze_inputs(files) -> list:
    """
    파일 리스트를 받아서, 각 파일에 대해 정제 작업을 수행하고 결과를 반환하는 함수.
    (실패한 파일은 별도 errors 리스트로 출력)
    """
    file_paths = sorted([
    f.path for f in files
    if f.name.lower().endswith((".xlsx", ".xls", ".xml"))
])
 
    results = []
    errors = []
 
    for path in file_paths:
        try:
            local_path = copy_blob_file_to_local(path)  
            result = process_one_file(local_path)
            results.append(result)
        except Exception as e:
            errors.append({"file": os.path.basename(path), "error": str(e)})
 
    if errors:
        print(f"[경고] {len(errors)}개 파일 처리 실패:")
        for e in errors:
            print(" -", e)
 
    return results


def process_one_file(local_path: str) -> dict:
    """
    로컬 파일 경로를 받아서, 부두번호 추출, 원시 데이터 읽기, 헤더 규칙 적용 후 정제된 DataFrame을 반환하는 함수.
    """
    file_name = os.path.basename(local_path)
    terminal_no = extract_terminal_no(file_name)
    if terminal_no is None:
        raise ValueError(f"파일명에서 부두번호를 추출할 수 없습니다: {file_name}")
 
    raw_df = read_raw_table(local_path)
    clean_df = apply_header_rule(raw_df, terminal_no)
 
    return {
        "file": file_name,
        "terminal_no": terminal_no,
        "df": clean_df,
    }


BRONZE_STAGING_DIR = "/Workspace/Shared/busan_port_project/01_schedule_pipeline/bronze_staging"
 
 
def save_bronze_inputs_to_staging(bronze_inputs: list, staging_dir: str = BRONZE_STAGING_DIR) -> dict:
    """
    bronze_inputs(= build_bronze_inputs() 결과)를 부두/스냅샷 파일 단위로 Parquet 저장하는 함수.
    각 파일의 컬럼명/순서/dtype은 원본 그대로 보존된다. (모든 컬럼을 string으로 캐스팅해서
    저장 - 타입 캐스팅은 silver 단계에서 처리하므로 bronze에서는 원본 표현을 그대로 둔다)
 
    주의: dbutils.fs.*(DBFS)와 순수 Python(os/pandas)이 보는 /Workspace/... 경로는
    서로 다른 파일시스템 레이어로 동작할 수 있다 (DBFS에는 쓰였지만 실제 Workspace
    파일에는 반영되지 않는 현상 확인됨). 따라서 이 함수는 dbutils.fs를 전혀 쓰지 않고
    os/shutil만으로 직접 써서, Workspace UI와 pandas.read_parquet() 양쪽에서
    동일하게 보이도록 한다.
 
    manifest.json에는 팀원이 읽어야 할 파일 목록과 메타정보를 기록한다.
    반환값: manifest dict (저장 직후 바로 확인할 수 있도록)
    """
    import json
 
    os.makedirs(staging_dir, exist_ok=True)
 
    manifest_entries = []
 
    for item in bronze_inputs:
        file_name = item["file"]
        terminal_no = item["terminal_no"]
        df = item["df"]
 
        # 원본 컬럼 표현을 그대로 보존하기 위해 전부 string으로 캐스팅 후 저장
        # (parquet은 컬럼별 단일 dtype을 요구하므로, object/mixed 타입 충돌을 막기 위함)
        # df_to_save = df.astype(str)
        df_to_save = df.where(pd.notna(df), None).astype("string")
 
        parquet_file_name = os.path.splitext(file_name)[0] + ".parquet"
        final_path = f"{staging_dir}/{parquet_file_name}"
        df_to_save.to_parquet(final_path, index=False)  # staging_dir에 직접 저장 (중간 복사 단계 제거)
 
        manifest_entries.append({
            "file": file_name,
            "parquet_file": parquet_file_name,
            "terminal_no": terminal_no,
            "rows": df.shape[0],
            "cols": df.shape[1],
            "columns": df.columns.tolist(),
        })
 
    manifest = {
        "staging_dir": staging_dir,
        "file_count": len(manifest_entries),
        "entries": manifest_entries,
    }
 
    manifest_path = f"{staging_dir}/manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
 
    print(f"저장 완료: {len(manifest_entries)}개 파일 -> {staging_dir}")
    print(f"manifest.json: {manifest_path}")
 
    return manifest

'''
기존 staging parquet 삭제 초기화
→ raw-schedule 현재 파일만 다시 parquet 생성
→ manifest.json도 현재 파일 기준으로 생성
'''

import shutil

staging_dir = "/Workspace/Shared/busan_port_project/01_schedule_pipeline/bronze_staging"

if os.path.exists(staging_dir):
    shutil.rmtree(staging_dir)

os.makedirs(staging_dir, exist_ok=True)


# =================================
#  4. 실행
# ================================= 
bronze_inputs = build_bronze_inputs(files)

manifest = save_bronze_inputs_to_staging(
    bronze_inputs,
    staging_dir=BRONZE_STAGING_DIR
)

print("처리 완료")
print("file_count:", manifest["file_count"])

for entry in manifest["entries"]:
    print(
        entry["file"],
        "terminal_no=", entry["terminal_no"],
        "rows=", entry["rows"],
        "cols=", entry["cols"]
    )