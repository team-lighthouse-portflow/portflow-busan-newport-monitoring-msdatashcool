"""
01_raw_to_bronze.py
-------------------
Azure 노트북(01_azureblob_to_bronze + 02 앞부분)을 로컬 PySpark로 포팅.

하는 일:
  data/raw/berth/ 의 원본 파일(xlsx / xls(HTML) / xml(Nexacro))을
  - 터미널별 헤더 규칙으로 "1행=컬럼명, 2행~=데이터"로 정제
  - Bronze 메타(snapshot_id, row_hash, bronze_row_id 등) 부착
  - 하나의 통합 Delta 테이블 data/lakehouse/bronze/bronze_berth_schedule 로 저장(overwrite)

원본과 달라진 점(로컬 전환):
  - dbutils / Blob / Workspace 복사 제거 (파일이 이미 로컬에 있음)
  - parquet staging + manifest 핸드오프 제거 (혼자 처리 → 메모리로 바로 연결)
  - saveAsTable(Unity Catalog) → 경로 기반 Delta .save()
도메인 로직(헤더 규칙 / xls=HTML / Nexacro XML / 메타 부착)은 원본 그대로 유지.

실행: 프로젝트 루트에서  python 01_raw_to_bronze.py
"""

import os
import sys
import re
import glob
import json
import hashlib
import xml.etree.ElementTree as ET
from io import StringIO
from datetime import datetime, timezone

import pandas as pd

# ----------------------------------------------------------------------
# 설정
# ----------------------------------------------------------------------
RAW_DIR     = "data/raw/berth"
BRONZE_PATH = "data/lakehouse/bronze/bronze_berth_schedule"

# 메타 컬럼 (raw_row_json 계산 시 제외할 대상)
META_COLS = [
    "terminal_id", "snapshot_id", "source_file_name", "source_file_path",
    "source_sheet_name", "source_row_number", "data_row_number",
]

# ----------------------------------------------------------------------
# 1) 부두번호 추출  (원본 extract_terminal_no)
# ----------------------------------------------------------------------
# 파일명이 부두명으로 시작하는 경우 매핑 (정규식 실패 시 fallback)
# ▶ 02 파일의 TERMINAL_NAME 번호와 반드시 일치시킬 것 (bct=6 확실, 나머지는 확인)
NAME_TO_NO = {"pnit": 1, "pnc": 2, "hjnc": 3, "hpnt": 4, "bnct": 5, "bct": 6, "dgt": 7}

def extract_terminal_no(file_name: str):
    """'terminal_3_...' -> 3,  'bct_20260629_10.xml' -> 6"""
    m = re.search(r"terminal_(\d+)_", file_name)
    if m:
        return int(m.group(1))
    return NAME_TO_NO.get(file_name.split("_")[0].lower())

# ----------------------------------------------------------------------
# 2) 부두별 헤더 규칙  (원본 TERMINAL_HEADER_RULES)
# ----------------------------------------------------------------------
TERMINAL_HEADER_RULES = {
    1: {"mode": "no_header"},                    # xls(html)
    2: {"mode": "no_header"},                    # xls(html)
    3: {"mode": "header_row", "header_row": 1},  # xlsx: 1행 제목 / 2행 헤더
    4: {"mode": "no_header"},                    # xls(html)
    5: {"mode": "header_row", "header_row": 0},  # xlsx: 1행 헤더
    6: {"mode": "xml_custom"},                   # xml - Nexacro ColumnInfo 기반
    7: {"mode": "header_row", "header_row": 0},  # xlsx: 1행 헤더
}

# ----------------------------------------------------------------------
# 3) Nexacro 데이터셋 XML 파서  (원본 read_nexacro_xml)
# ----------------------------------------------------------------------
def read_nexacro_xml(local_path: str) -> pd.DataFrame:
    tree = ET.parse(local_path)
    root = tree.getroot()

    def clean(tag):
        return tag.split("}")[-1]

    # ColumnInfo에서 컬럼 순서 확정
    column_order = []
    for elem in root.iter():
        if clean(elem.tag) == "ColumnInfo":
            for col in elem:
                if clean(col.tag) == "Column":
                    col_id = col.attrib.get("id")
                    if col_id:
                        column_order.append(col_id)
            break
    if not column_order:
        raise ValueError("ColumnInfo에서 컬럼 정의를 찾지 못했습니다.")

    # Row 단위 값 채우기 (컬럼 순서 고정)
    rows = []
    for elem in root.iter():
        if clean(elem.tag) == "Row":
            row = {col_id: None for col_id in column_order}
            for child in elem:
                col_id = child.attrib.get("id")
                if col_id in row:
                    row[col_id] = child.text
            rows.append(row)

    return pd.DataFrame(rows, columns=column_order)

# ----------------------------------------------------------------------
# 4) 확장자별 원시 읽기  (원본 read_raw_table)
# ----------------------------------------------------------------------
def read_raw_table(local_path: str) -> pd.DataFrame:
    file_name = os.path.basename(local_path)

    if file_name.endswith(".xlsx"):
        return pd.read_excel(local_path, header=None, engine="openpyxl")

    elif file_name.endswith(".xls"):
        # 이 .xls는 실제로는 HTML 테이블
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

# ----------------------------------------------------------------------
# 5) 헤더 규칙 적용  (원본 apply_header_rule)
# ----------------------------------------------------------------------
def apply_header_rule(raw_df: pd.DataFrame, terminal_no: int) -> pd.DataFrame:
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
        return raw_df.reset_index(drop=True)

    else:
        raise ValueError(f"알 수 없는 모드: {mode}")

# ----------------------------------------------------------------------
# 6) 스냅샷 시각 파싱 + 파일 해시  (원본 parse_snapshot_time / make_file_hash)
# ----------------------------------------------------------------------
def parse_snapshot_time(file_name: str) -> datetime:
    """terminal_1_schedule_20260626_10.xls -> 2026-06-26 10:00"""
    base = file_name.rsplit(".", 1)[0]
    parts = base.split("_")
    date_str = parts[-2]
    hour_str = parts[-1]
    return datetime.strptime(f"{date_str} {hour_str}:00", "%Y%m%d %H:%M")

def make_file_hash(df: pd.DataFrame) -> str:
    combined = df.to_json(orient="records", force_ascii=False)
    return hashlib.md5(combined.encode()).hexdigest()

# ----------------------------------------------------------------------
# 7) Bronze 메타 부착  (원본 02 노트북 cell 7 로직)
# ----------------------------------------------------------------------
def enrich_bronze(df: pd.DataFrame, file_name: str, terminal_no: int) -> pd.DataFrame:
    df = df.copy()
    snapshot_time = parse_snapshot_time(file_name)
    snapshot_id   = f"{terminal_no}_{snapshot_time.strftime('%Y%m%d_%H')}"
    file_hash     = make_file_hash(df)
    downloaded_at = snapshot_time.strftime("%Y-%m-%d %H:%M:%S")
    ingested_at   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df["terminal_id"]       = f"terminal_{terminal_no}"
    df["snapshot_id"]       = snapshot_id
    df["source_file_name"]  = file_name
    df["source_file_path"]  = f"/raw/terminal_{terminal_no}/{file_name}"
    df["source_sheet_name"] = "Sheet1"
    df["source_row_number"] = range(1, len(df) + 1)
    df["data_row_number"]   = range(1, len(df) + 1)

    df["raw_row_json"] = df.drop(columns=META_COLS).apply(
        lambda row: json.dumps(row.to_dict(), ensure_ascii=False, default=str), axis=1
    )
    df["row_hash"]  = df["raw_row_json"].apply(lambda s: hashlib.md5(s.encode()).hexdigest())
    df["file_hash"] = file_hash
    df["ingested_at"]   = ingested_at
    df["downloaded_at"] = downloaded_at
    df["bronze_row_id"] = [
        f"brz_{terminal_no:02d}_{snapshot_time.strftime('%Y%m%d_%H')}_{i:04d}"
        for i in range(1, len(df) + 1)
    ]
    return df

# ----------------------------------------------------------------------
# 8) 컬럼명 정리 (Delta가 허용 안 하는 문자만 치환)  (원본 cell 10)
# ----------------------------------------------------------------------
def sanitize_columns(cols):
    bad = " ;{}()\n\t="
    out = []
    for c in cols:
        c = str(c)
        for ch in bad:
            c = c.replace(ch, "_")
        out.append(c)
    return out

# ----------------------------------------------------------------------
# 9) 파일 1개 처리
# ----------------------------------------------------------------------
def process_one_file(path: str) -> pd.DataFrame:
    file_name = os.path.basename(path)
    terminal_no = extract_terminal_no(file_name)
    if terminal_no is None:
        raise ValueError(f"파일명에서 부두번호 추출 불가: {file_name}")
    raw_df   = read_raw_table(path)
    clean_df = apply_header_rule(raw_df, terminal_no)
    return enrich_bronze(clean_df, file_name, terminal_no)

# ----------------------------------------------------------------------
# main
# ----------------------------------------------------------------------
def main():
    from spark_session import get_spark   # Spark는 여기서만 (순수 파이썬 테스트 가능하도록)

    paths = sorted(glob.glob(os.path.join(RAW_DIR, "**", "*.*"), recursive=True))
    paths = [p for p in paths if p.lower().endswith((".xlsx", ".xls", ".xml"))]
    print(f"대상 파일 {len(paths)}개")

    processed, errors = [], []
    for p in paths:
        try:
            processed.append(process_one_file(p))
            print(f"  OK  {os.path.basename(p)}")
        except Exception as e:
            errors.append((os.path.basename(p), str(e)))
            print(f"  ERR {os.path.basename(p)} -> {e}")

    if not processed:
        print("처리된 파일이 없습니다. data/raw/berth/ 를 확인하세요.")
        return
    if errors:
        print(f"[경고] 실패 {len(errors)}건:")
        for f, e in errors:
            print("   -", f, e)

    # 모든 파일 결합 (컬럼은 union, 결측은 NaN). 원본은 전부 string으로 저장.
    combined = pd.concat(processed, ignore_index=True).astype(str)
    print(f"결합 결과: {combined.shape[0]}행 x {combined.shape[1]}열")

    spark = get_spark("raw-to-bronze")
    spark.sparkContext.setLogLevel("ERROR")

    sdf = spark.createDataFrame(combined).toDF(*sanitize_columns(combined.columns))
    (sdf.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(BRONZE_PATH))

    print(f"bronze_berth_schedule 적재 완료 -> {BRONZE_PATH}")
    print(f"행 수: {spark.read.format('delta').load(BRONZE_PATH).count()}")
    spark.stop()

    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0)

if __name__ == "__main__":
    main()
