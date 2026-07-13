"""
00_historical_to_silver.py
---------------------------
과거 선석 이력(data/raw/historical/N부두_YYYY.xls[x])을 silver로 적재.
한 번만 돌리면 되는 정적 이력 (매시간 X).

특징:
  - 파일은 .xls 위장 HTML (read_html) 또는 실제 xlsx
  - 한 파일 = 한 부두의 1년치, 첫 행이 헤더(이름 있음)
  - 팀원 ML 코드의 col_rename(이 파일들 전용)을 재사용해 표준 컬럼으로 매핑
  - 5/7부두형 '작업량(양하/적하/Shift)' 통합 컬럼 자동 파싱
  - 02와 동일한 enrichment 적용 → silver_berth_schedule 와 스키마 동일
  - 별도 테이블 data/lakehouse/silver/silver_berth_schedule_historical 로 저장
    (실시간 02는 silver_berth_schedule를 overwrite하므로 분리)

실행: 프로젝트 루트에서  python 00_historical_to_silver.py  (한 번)
"""
import os
import re
import sys
import glob
import hashlib
from io import StringIO
from datetime import datetime, timedelta, timezone

import pandas as pd

RAW_HIST   = "data/raw/historical"
OUT_PATH   = "data/lakehouse/silver/silver_berth_schedule_historical"

TERMINAL_NAME = {1: "PNIT", 2: "PNC", 3: "HJNC", 4: "HPNT", 5: "BNCT", 6: "BCT", 7: "DGT"}

# 팀원 ML 코드의 col_rename 재사용 (정규화 키 -> 표준 silver 컬럼)
COL_RENAME = {
    '접안(예정)일시': 'eta', '접안예정시간': 'eta', '접안일시': 'eta', '입항일시': 'eta',
    'atb': 'eta', 'arrival': 'eta', '접안예정시간(etb)': 'eta', '접안예정일시': 'eta',
    '출항(예정)일시': 'etd', '출항예정시간': 'etd', '출항일시': 'etd', 'atd': 'etd',
    'departure': 'etd', '출항예정시간(etd)': 'etd', '출항예정일시': 'etd',
    '선석': 'berth', 'berth': 'berth',
    '선명': 'vessel_name', '모선명': 'vessel_name', '선박명': 'vessel_name', 'vessel': 'vessel_name',
    '모선항차': 'mother_vessel', '선사항차': 'sun_vessel',
    'head(bridge)stern': 'head_bridge_stern', '반입마감시한': 'closing', '반입마감시간': 'closing',
    '양하': 'discharge', 'discharge': 'discharge', 'import': 'discharge', '양하수량': 'discharge',
    '적하': 'loading', '선적': 'loading', 'load': 'loading', 'export': 'loading',
    '선적수량': 'loading', '적하수량': 'loading',
    '상태': 'status', 'status': 'status',
    '선사': 'carrier', '운항선사': 'carrier', '선사코드': 'carrier',
    'route': 'route', '항로': 'route', 'route명': 'route', 'amp': 'amp',
    'shift': 'shift', 's/h': 'shift', '이적': 'shift',
}
STD_TEXT = ['berth', 'carrier', 'vessel_name', 'mother_vessel',
            'sun_vessel', 'head_bridge_stern', 'route', 'amp', 'status']


def norm(c):
    return str(c).lower().replace(" ", "").replace("\n", "")


def read_table(path):
    if path.lower().endswith(".xls"):
        with open(path, "rb") as f:
            content = f.read()
        content = content.replace(b"udf-8", b"utf-8").replace(b"UDF-8", b"UTF-8")
        try:
            html = content.decode("utf-8")
        except UnicodeDecodeError:
            html = content.decode("cp949", errors="replace")
        return pd.read_html(StringIO(html), header=0)[0]
    return pd.read_excel(path, header=0)


def fix_header(df):
    """헤더가 0행이 아닌 경우(제목행 등) 자동 보정: 키워드가 있는 행을 헤더로 승격."""
    kws = ['선석', '입항', '출항', '양하', '적하', '선적', '접안', '모선', '선명', '선박']
    def has_kw(cols):
        return any(any(k in str(c) for k in kws) for c in cols)
    if has_kw(df.columns):
        return df
    for i in range(min(3, len(df))):
        if has_kw(df.iloc[i].tolist()):
            df = df.copy()
            df.columns = df.iloc[i]
            return df.iloc[i + 1:].reset_index(drop=True)
    return df


def parse_workload(v):
    if pd.isna(v):
        return 0.0, 0.0, 0.0
    p = str(v).split('/')
    if len(p) < 2:
        p = str(v).split()
    if len(p) < 2:
        return 0.0, 0.0, 0.0
    to = lambda s: float(''.join(ch for ch in s if ch.isdigit()) or 0)
    return to(p[0]), to(p[1]), (to(p[2]) if len(p) >= 3 else 0.0)


def terminal_of(path):
    m = re.search(r'(\d+)부두', os.path.basename(path))
    return int(m.group(1)) if m else None


def build_one(path, terminal_no):
    df = read_table(path)
    df = fix_header(df)   # 제목행 등으로 헤더가 밀린 경우 보정(3부두 등)
    # 5/7부두형 통합 작업량 컬럼 파싱
    wl = next((c for c in df.columns if '작업량' in norm(c)), None)
    if wl is not None:
        vals = df[wl].apply(parse_workload)
        df['discharge'] = [x[0] for x in vals]
        df['loading'] = [x[1] for x in vals]
        df['shift'] = [x[2] for x in vals]
    df = df.rename(columns={c: COL_RENAME[norm(c)] for c in df.columns if norm(c) in COL_RENAME})
    # rename 후 같은 표준명이 여러 개 생길 수 있음(부두별 구조 차이) -> 첫 번째만 사용
    df = df.loc[:, ~df.columns.duplicated()]

    def col(name):
        """표준명 컬럼을 항상 Series로 안전하게 반환 (없으면 None Series)."""
        if name in df.columns:
            v = df[name]
            return v.iloc[:, 0] if hasattr(v, 'columns') else v
        return pd.Series([None] * len(df), index=df.index)

    out = pd.DataFrame(index=df.index)
    for c in STD_TEXT:
        s = col(c)
        out[c] = s.astype(str) if c in df.columns else None
    out['eta'] = pd.to_datetime(col('eta'), errors='coerce')
    out['etd'] = pd.to_datetime(col('etd'), errors='coerce')
    out['closing'] = pd.to_datetime(col('closing'), errors='coerce')
    for c in ['discharge', 'loading', 'shift']:
        out[c] = pd.to_numeric(col(c), errors='coerce').fillna(0).astype(int)

    out = out.dropna(subset=['eta', 'etd'])
    stay = (out['etd'] - out['eta']).dt.total_seconds() / 3600.0
    out = out[(stay >= 0) & (stay < 240.0)].reset_index(drop=True)

    fy = (re.search(r'_(\d{4})', os.path.basename(path)) or [None, "0000"])[1]
    out['terminal_id'] = f"terminal_{terminal_no}"
    out['terminal_name'] = TERMINAL_NAME.get(terminal_no)
    out['snapshot_id'] = f"hist_{terminal_no}_{fy}"
    out['bronze_row_id'] = [f"hist_{terminal_no:02d}_{fy}_{i:05d}" for i in range(len(out))]
    out['source_file_name'] = os.path.basename(path)
    out['source_row_number'] = [str(i + 1) for i in range(len(out))]
    out['parsed_at'] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    out['eta_work_date'] = (out['eta'] - timedelta(hours=6)).dt.strftime('%Y-%m-%d')
    out['business_key'] = None  # Spark enrichment에서 재계산 (02와 동일)
    out['is_valid'] = out['berth'].notna() | out['vessel_name'].notna()
    out['error_msg'] = None
    out['row_hash'] = out.apply(
        lambda r: hashlib.md5(f"{r['berth']}{r['vessel_name']}{r['eta']}{r['etd']}".encode()).hexdigest(),
        axis=1)
    return out


def main():
    from pyspark.sql import functions as F
    from spark_session import get_spark

    paths = sorted(glob.glob(os.path.join(RAW_HIST, "**", "*부두*.xls*"), recursive=True))
    print(f"historical 파일 {len(paths)}개")

    frames, errors = [], []
    for p in paths:
        tno = terminal_of(p)
        if tno is None:
            errors.append((os.path.basename(p), "부두번호 추출 실패")); continue
        try:
            d = build_one(p, tno)
            frames.append(d)
            print(f"  OK  {os.path.basename(p)} -> terminal_{tno}, {len(d)}행")
        except Exception as e:
            errors.append((os.path.basename(p), str(e)))
            print(f"  ERR {os.path.basename(p)} -> {e}")

    if errors:
        print(f"[경고] 실패 {len(errors)}건: " + ", ".join(f"{f}({e})" for f, e in errors))
    if not frames:
        print("적재할 데이터가 없습니다."); return

    pdf = pd.concat(frames, ignore_index=True)
    print(f"\n결합: {len(pdf)}행 / 연도 {sorted(pdf['etd'].dt.year.dropna().unique().tolist())}")

    spark = get_spark("historical-to-silver")
    spark.sparkContext.setLogLevel("ERROR")

    from pyspark.sql.types import (StructType, StructField, StringType,
                                   IntegerType, BooleanType, TimestampType)
    # 전 행이 None인 컬럼도 타입이 정해지도록 명시 스키마 사용
    schema = StructType([
        StructField("berth", StringType(), True),
        StructField("carrier", StringType(), True),
        StructField("vessel_name", StringType(), True),
        StructField("mother_vessel", StringType(), True),
        StructField("sun_vessel", StringType(), True),
        StructField("head_bridge_stern", StringType(), True),
        StructField("route", StringType(), True),
        StructField("amp", StringType(), True),
        StructField("status", StringType(), True),
        StructField("eta", TimestampType(), True),
        StructField("etd", TimestampType(), True),
        StructField("closing", TimestampType(), True),
        StructField("discharge", IntegerType(), True),
        StructField("loading", IntegerType(), True),
        StructField("shift", IntegerType(), True),
        StructField("terminal_id", StringType(), True),
        StructField("terminal_name", StringType(), True),
        StructField("snapshot_id", StringType(), True),
        StructField("bronze_row_id", StringType(), True),
        StructField("source_file_name", StringType(), True),
        StructField("source_row_number", StringType(), True),
        StructField("parsed_at", StringType(), True),
        StructField("eta_work_date", StringType(), True),
        StructField("business_key", StringType(), True),
        StructField("is_valid", BooleanType(), True),
        StructField("error_msg", StringType(), True),
        StructField("row_hash", StringType(), True),
    ])
    # Spark 명시 스키마에 맞게 각 셀을 파이썬 기본형으로 정리
    str_cols = [f.name for f in schema.fields if isinstance(f.dataType, StringType)]
    ts_cols  = ['eta', 'etd', 'closing']
    int_cols = ['discharge', 'loading', 'shift']

    for c in str_cols:                       # NaN -> None
        pdf[c] = pdf[c].astype(object).where(pd.notnull(pdf[c]), None)
    for c in int_cols:                       # NaN -> 0, 파이썬 int
        pdf[c] = pd.to_numeric(pdf[c], errors='coerce').fillna(0).astype(int)
    pdf['is_valid'] = pdf['is_valid'].astype(bool)
    for c in ts_cols:                        # pandas Timestamp/NaT -> 파이썬 datetime/None
        col = pd.to_datetime(pdf[c], errors='coerce')
        # dtype=object로 고정해야 pandas가 datetime64로 되돌리지 않음 (Spark가 Timestamp 거부)
        pdf[c] = pd.Series([None if pd.isna(x) else x.to_pydatetime() for x in col],
                           index=pdf.index, dtype=object)

    pdf = pdf[[f.name for f in schema.fields]]
    sdf = spark.createDataFrame(pdf, schema=schema)
    # 02와 동일한 enrichment (스키마 일치)
    sdf = (sdf
        .withColumn("eta", F.to_timestamp("eta"))
        .withColumn("etd", F.to_timestamp("etd"))
        .withColumn("closing", F.to_timestamp("closing"))
        .withColumn("discharge", F.col("discharge").cast("int"))
        .withColumn("loading", F.col("loading").cast("int"))
        .withColumn("shift", F.col("shift").cast("int"))
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

    (sdf.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").save(OUT_PATH))
    print(f"\nsilver_berth_schedule_historical 저장: {sdf.count()}행 -> {OUT_PATH}")
    sdf.groupBy("terminal_id").count().orderBy("terminal_id").show()

    spark.stop()
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0)


if __name__ == "__main__":
    main()