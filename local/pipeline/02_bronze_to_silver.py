"""
02_bronze_to_silver.py
-----------------------
Azure 노트북(02_bronze_to_silver 의 silver 변환부)을 로컬 PySpark로 포팅.

하는 일:
  data/lakehouse/bronze/bronze_berth_schedule 를 읽어
  - COLUMN_MAP 으로 7개 터미널 컬럼을 표준 스키마로 통일
  - 작업량 파싱 / eta_work_date / business_key / is_valid 생성
  - data/lakehouse/silver/silver_berth_schedule (Delta, overwrite) 저장
  - 검증 실패 행은 data/lakehouse/silver/silver_error_log 로 저장

원본 대비 정리한 점(버그/중복 정리, 도메인 로직은 동일):
  - 원본은 silver를 append로 '두 번' 저장(cell 26, 32) → 데이터 중복. 여기선 한 번만 저장.
  - cell 28 오타 컬럼 eta_word_date 제거.
  - terminal_name 채움(TERMINAL_NAME), 최종 enrich(timestamp/feature)는 1회만.
  - saveAsTable(Unity Catalog) → 경로 기반 Delta .save()

실행: 프로젝트 루트에서  python 02_bronze_to_silver.py  (01 먼저 실행되어 있어야 함)
"""

import re
import json
import uuid
from datetime import datetime, timedelta, timezone
import os
import sys

import pandas as pd

# ----------------------------------------------------------------------
# 설정
# ----------------------------------------------------------------------
BRONZE_PATH = "data/lakehouse/bronze/bronze_berth_schedule"
SILVER_PATH = "data/lakehouse/silver/silver_berth_schedule"
ERROR_PATH  = "data/lakehouse/silver/silver_error_log"

# 터미널 표시명 (▶ 본인 부두 번호↔이름 매핑 확인 후 수정)
TERMINAL_NAME = {
    "terminal_1": "PNIT",
    "terminal_2": "PNC",
    "terminal_3": "HJNC",
    "terminal_4": "HPNT",
    "terminal_5": "BNCT",
    "terminal_6": "BCT",
    "terminal_7": "DGT",
}

# ----------------------------------------------------------------------
# 부두별 컬럼 통일 매핑  (원본 COLUMN_MAP 그대로)
# ----------------------------------------------------------------------
COLUMN_MAP = {
    "terminal_1": {
        "col_1": "berth", "col_2": "carrier", "col_3": "mother_vessel",
        "col_4": "sun_vessel", "col_5": "head_bridge_stern", "col_6": "vessel_name",
        "col_7": "route", "col_8": "closing", "col_9": "eta", "col_10": "etd",
        "col_11": "discharge", "col_12": "loading", "col_13": "shift",
        "col_14": "amp", "col_15": "status",
    },
    "terminal_2": {
        "col_1": "_drop", "col_2": "vessel_name", "col_3": "mother_vessel",
        "col_4": "sun_vessel", "col_5": "carrier", "col_6": "route",
        "col_7": "head_bridge_stern", "col_8": "eta", "col_9": "etd",
        "col_10": "berth", "col_11": "closing", "col_12": "discharge",
        "col_13": "loading", "col_14": "shift", "col_15": "_drop",
        "col_16": "_drop", "col_17": "_drop",
    },
    "terminal_3": {
        "No": "_drop", "선석": "berth", "항로": "route", "모선항차": "mother_vessel",
        "선박명": "vessel_name", "선사항차": "sun_vessel", "접안": "_drop",
        "선사": "carrier", "반입 시작일시": "_drop", "반입 마감일시": "closing",
        "입항일시": "eta", "출항일시": "etd", "작업 시작일시": "_drop",
        "작업 완료일시": "_drop", "양하": "discharge", "선적": "loading",
        "S/H": "shift", "전배": "_drop",
    },
    "terminal_4": {
        "col_1": "berth", "col_2": "carrier", "col_3": "mother_vessel",
        "col_4": "sun_vessel", "col_5": "vessel_name", "col_6": "route",
        "col_7": "_drop", "col_8": "eta", "col_9": "etd", "col_10": "_drop",
        "col_11": "closing", "col_12": "discharge", "col_13": "loading",
        "col_14": "shift", "col_15": "amp", "col_16": "status",
    },
    "terminal_5": {
        "선석": "berth", "선사": "carrier",
        "모선항차(선사항차)\nHead (Bridge) Stern": "_drop",
        "선명\n(ROUTE)": "vessel_name", "반입마감시한": "closing",
        "접안(예정)일시": "eta", "출항(예정)일시": "etd",
        "작업량\n양하 / 적하 / Shift": "_workload", "상태": "status",
    },
    "terminal_6": {
        "plvBerth": "berth", "cdvOperator": "carrier", "plvVslvoy": "mother_vessel",
        "cdvName": "vessel_name", "plvEvoyout": "sun_vessel", "plvRoute": "route",
        "plvAtb": "eta", "plvAtd": "etd", "plvDisvan": "discharge",
        "plvLodvan": "loading", "plvShiftvan": "shift", "plvStatus": "status",
    },
    "terminal_7": {
        "선석": "berth", "선사코드": "carrier", "모선항차(선사항차)": "mother_vessel",
        "모선명(Route)": "vessel_name", "반입마감시한": "closing",
        "접안예정일시": "eta", "출항예정일시": "etd", "작업시작시간": "_drop",
        "작업완료시간": "_drop", "Head (Bridge) Stern": "head_bridge_stern",
        "작업량\n양하/적하/Shift": "_workload", "상태": "status",
    },
}

# ----------------------------------------------------------------------
# 파싱 헬퍼  (원본 parse_workload / to_int)
# ----------------------------------------------------------------------
def parse_workload(val: str):
    """'1,015 / 960 / 12' -> (1015, 960, 12)"""
    if not val:
        return None, None, None
    parts = re.split(r"\s*/\s*|／|｜|\|", str(val).strip())
    parts = [p.strip().replace(",", "") for p in parts if p.strip() != ""]
    try:
        d = int(parts[0]) if len(parts) > 0 and parts[0].isdigit() else None
        l = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
        s = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None
        return d, l, s
    except Exception:
        return None, None, None

def to_int(val):
    try:
        return int(str(val).strip().replace(",", ""))
    except Exception:
        return None

# ----------------------------------------------------------------------
# bronze row -> silver row  (원본 bronze_row_to_silver 그대로)
# ----------------------------------------------------------------------
def bronze_row_to_silver(row: dict, terminal_id: str, parsed_at: str) -> dict:
    mapping = COLUMN_MAP.get(terminal_id, {})
    silver = {
        "terminal_id": terminal_id,
        "terminal_name": TERMINAL_NAME.get(terminal_id),
        "snapshot_id": row.get("snapshot_id"),
        "bronze_row_id": row.get("bronze_row_id"),
        "source_file_name": row.get("source_file_name"),
        "source_row_number": row.get("source_row_number"),
        "row_hash": row.get("row_hash"),
        "parsed_at": parsed_at,
        "berth": None, "carrier": None, "vessel_name": None, "mother_vessel": None,
        "sun_vessel": None, "head_bridge_stern": None, "route": None,
        "eta": None, "etd": None, "closing": None,
        "discharge": None, "loading": None, "shift": None,
        "amp": None, "status": None,
        "eta_work_date": None, "business_key": None,
        "is_valid": True, "error_msg": None,
    }
    try:
        for orig_col, std_col in mapping.items():
            val = row.get(orig_col, "")
            if val is None or str(val).strip() in ("", "nan", "None"):
                continue
            if std_col == "_drop":
                continue
            elif std_col == "_workload":
                d, l, s = parse_workload(val)
                silver["discharge"], silver["loading"], silver["shift"] = d, l, s
            else:
                silver[std_col] = str(val).strip()

        for col in ["discharge", "loading", "shift"]:
            if silver[col] is not None and isinstance(silver[col], str):
                silver[col] = to_int(silver[col])

        if not silver["berth"] and not silver["vessel_name"]:
            silver["is_valid"] = False
            silver["error_msg"] = "선석/선명 모두 없음"
    except Exception as e:
        silver["is_valid"] = False
        silver["error_msg"] = str(e)

    try:
        if silver["eta"]:
            eta_work_date = (pd.to_datetime(silver["eta"]) - timedelta(hours=6)).strftime("%Y-%m-%d")
            silver["eta_work_date"] = eta_work_date
            vessel_key = silver["mother_vessel"] or silver["vessel_name"]
            silver["business_key"] = f"{silver['terminal_id']}_{vessel_key}_{eta_work_date}"
    except Exception:
        pass

    return silver

# ----------------------------------------------------------------------
# main
# ----------------------------------------------------------------------
def main():
    from pyspark.sql import functions as F
    from pyspark.sql.types import (StructType, StructField, StringType,
                                    IntegerType, BooleanType)
    from spark_session import get_spark

    spark = get_spark("bronze-to-silver")
    spark.sparkContext.setLogLevel("ERROR")
    parsed_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # 1) bronze 읽기 -> 파이썬으로 수집 (원본과 동일하게 row 단위 변환)
    bronze_rows = spark.read.format("delta").load(BRONZE_PATH).collect()
    print(f"bronze 행 수: {len(bronze_rows)}")

    silver_rows, error_rows = [], []
    for r in bronze_rows:
        d = r.asDict()
        sv = bronze_row_to_silver(d, d.get("terminal_id"), parsed_at)
        silver_rows.append(sv)
        if not sv["is_valid"]:
            error_rows.append({
                "error_id": f"err_{uuid.uuid4().hex[:8]}",
                "bronze_row_id": sv["bronze_row_id"],
                "terminal_id": sv["terminal_id"],
                "source_file_name": sv["source_file_name"],
                "source_row_number": str(d.get("source_row_number")),
                "error_type": "PARSE_ERROR",
                "error_message": sv["error_msg"],
                "detected_at": parsed_at,
            })

    valid_cnt = sum(1 for r in silver_rows if r["is_valid"])
    print(f"silver 변환: 전체 {len(silver_rows)} / 정상 {valid_cnt} / 오류 {len(silver_rows)-valid_cnt}")

    # 2) 명시 스키마로 silver DataFrame 생성  (원본 silver_schema + terminal_name)
    silver_schema = StructType([
        StructField("terminal_id",       StringType(),  True),
        StructField("terminal_name",     StringType(),  True),
        StructField("snapshot_id",       StringType(),  True),
        StructField("bronze_row_id",     StringType(),  True),
        StructField("source_file_name",  StringType(),  True),
        StructField("source_row_number", StringType(),  True),
        StructField("row_hash",          StringType(),  True),
        StructField("parsed_at",         StringType(),  True),
        StructField("eta_work_date",     StringType(),  True),
        StructField("business_key",      StringType(),  True),
        StructField("berth",             StringType(),  True),
        StructField("carrier",           StringType(),  True),
        StructField("vessel_name",       StringType(),  True),
        StructField("mother_vessel",     StringType(),  True),
        StructField("sun_vessel",        StringType(),  True),
        StructField("head_bridge_stern", StringType(),  True),
        StructField("route",             StringType(),  True),
        StructField("eta",               StringType(),  True),
        StructField("etd",               StringType(),  True),
        StructField("closing",           StringType(),  True),
        StructField("discharge",         IntegerType(), True),
        StructField("loading",           IntegerType(), True),
        StructField("shift",             IntegerType(), True),
        StructField("amp",               StringType(),  True),
        StructField("status",            StringType(),  True),
        StructField("is_valid",          BooleanType(), True),
        StructField("error_msg",         StringType(),  True),
    ])
    # createDataFrame는 dict 순서가 아니라 schema 필드명 기준이므로 Row 변환
    field_names = [f.name for f in silver_schema.fields]
    silver_tuples = [tuple(r.get(n) for n in field_names) for r in silver_rows]
    silver_df = spark.createDataFrame(silver_tuples, schema=silver_schema)

    # 3) 타입 변환 + 파생 컬럼  (원본 cell 28/30, 오타/중복 정리)
    silver_df = (silver_df
        .withColumn("eta",     F.to_timestamp("eta"))
        .withColumn("etd",     F.to_timestamp("etd"))
        .withColumn("closing", F.to_timestamp("closing"))
        .withColumn("eta_date", F.to_date("eta"))
        .withColumn("business_key",
                    F.concat_ws("_", "terminal_id", "mother_vessel", "sun_vessel", "eta_work_date"))
        .withColumn("total_workload",
                    F.coalesce(F.col("discharge"), F.lit(0))
                    + F.coalesce(F.col("loading"), F.lit(0))
                    + F.coalesce(F.col("shift"), F.lit(0)))
        .withColumn("eta_hour", F.hour("eta"))
        .withColumn("eta_dayofweek", F.dayofweek("eta"))
        .withColumn("is_amp", F.when(F.upper(F.col("amp")) == "Y", 1).otherwise(0)))

    # 4) 저장 (한 번만, overwrite)
    (silver_df.write.format("delta")
        .mode("overwrite").option("overwriteSchema", "true").save(SILVER_PATH))
    print(f"silver_berth_schedule 저장 완료 -> {SILVER_PATH} ({silver_df.count()}행)")

    # 5) 에러 로그 저장
    if error_rows:
        err_df = spark.createDataFrame(pd.DataFrame(error_rows).astype(str))
        (err_df.write.format("delta")
            .mode("overwrite").option("overwriteSchema", "true").save(ERROR_PATH))
        print(f"error_log 저장: {len(error_rows)}건 -> {ERROR_PATH}")

    silver_df.select("terminal_id", "terminal_name", "vessel_name",
                     "eta", "eta_work_date", "business_key").show(10, truncate=False)
    spark.stop()

    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0)


if __name__ == "__main__":
    main()
