"""
03_container_to_silver.py
-------------------------
'월별 컨테이너 실적 데이터 전처리' 노트북(cell 3)을 로컬 PySpark로 포팅.

하는 일:
  data/raw/container/ 의 연도별 엑셀(월별 컨테이너 처리실적)을 읽어
  - 상단 헤더 다음의 Full/Empty 서브헤더 행 제거
  - 영문 11개 컬럼명으로 통일
  - data/lakehouse/silver/silver_monthly_container_{year} (Delta) 로 저장

이걸 만들어두면 04_silver_to_gold.py 가 자동으로 읽어
gold_monthly_container / gold_ml_feature_table 를 생성합니다.

원본 대비 변경점:
  - Blob/ADLS 연결, account key, dbutils.fs.cp 제거 (파일이 로컬에 있음)
  - saveAsTable(Unity Catalog) -> 경로 기반 Delta .save()
  - 원본 cell 5(자체 gold_monthly_container)는 포팅 안 함:
    03_silver_to_gold 의 gold 정의와 다른 옛 버전이라 04와 충돌 방지.
  컬럼 매핑/행 제거 로직은 원본 그대로.

실행: 프로젝트 루트에서  python 03_container_to_silver.py
"""
import os
import sys
import glob
import pandas as pd

RAW_DIR     = "data/raw/container"
SILVER_PATH = "data/lakehouse/silver/silver_monthly_container_{year}"
FNAME       = "월별 컨테이너 처리실적(확정)_{year}.xlsx"  # 정확한 이름 우선, 없으면 *{year}*.xlsx
YEARS       = range(2022, 2027)

# 컬럼명 매핑 (Excel 원본 → 정리된 영문 컬럼명) — 원본 COLUMN_NAMES 그대로
COLUMN_NAMES = [
    "month",
    "total_full", "total_empty",
    "import_full", "import_empty",
    "export_full", "export_empty",
    "import_transship_full", "import_transship_empty",
    "export_transship_full", "export_transship_empty",
]


def find_file(year: int):
    """연도별 raw 파일 경로 찾기 (정확한 이름 → 패턴 매칭 순)."""
    exact = os.path.join(RAW_DIR, FNAME.format(year=year))
    if os.path.exists(exact):
        return exact
    hits = glob.glob(os.path.join(RAW_DIR, f"*{year}*.xls*"))
    return hits[0] if hits else None


def read_one_year(path: str) -> pd.DataFrame:
    """원본 cell 3 로직: 서브헤더 행 제거 + 11개 컬럼 매핑."""
    pdf = pd.read_excel(path, sheet_name=0, dtype=str)
    # 첫 데이터 행은 Full/Empty 서브헤더이므로 제거
    pdf = pdf.iloc[1:].reset_index(drop=True)
    if pdf.shape[1] != len(COLUMN_NAMES):
        raise ValueError(
            f"컬럼 수가 {pdf.shape[1]}개로 예상({len(COLUMN_NAMES)})과 다릅니다. "
            f"원본 엑셀 구조를 확인하세요: {path}")
    pdf.columns = COLUMN_NAMES
    pdf = pdf.where(pd.notnull(pdf), None)   # NaN -> None (Spark null)
    return pdf


def main():
    from spark_session import get_spark   # Spark는 여기서만

    spark = get_spark("container-to-silver")
    spark.sparkContext.setLogLevel("ERROR")

    made = 0
    for year in YEARS:
        path = find_file(year)
        if path is None:
            print(f"  [건너뜀] {year}: data/raw/container/ 에 파일 없음")
            continue
        try:
            pdf = read_one_year(path)
        except Exception as e:
            print(f"  [오류]   {year}: {e}")
            continue

        out = SILVER_PATH.format(year=year)
        sdf = spark.createDataFrame(pdf)
        (sdf.write.format("delta").mode("overwrite")
            .option("overwriteSchema", "true").save(out))
        print(f"  [OK]     {year}: {sdf.count()}행 -> {out}  (파일: {os.path.basename(path)})")
        made += 1

    print(f"\n완료: silver_monthly_container 테이블 {made}개 생성.")
    if made:
        print("이제 python 04_silver_to_gold.py 를 다시 돌리면 "
              "gold_monthly_container / gold_ml_feature_table 가 포함됩니다.")

    spark.stop()
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0)


if __name__ == "__main__":
    main()