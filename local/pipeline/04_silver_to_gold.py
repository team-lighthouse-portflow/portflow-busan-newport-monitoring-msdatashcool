"""
04_silver_to_gold.py
--------------------
Azure 노트북(03_silver_to_gold)을 로컬 PySpark로 포팅.

만드는 Gold 테이블 (data/lakehouse/gold/ 에 Delta로 저장):
  [스케줄 대시보드 - silver_berth_schedule 기반, 지금 바로 동작]
    - gold_integrated_schedule        7개 부두 선석스케줄 통합
    - gold_schedule_change_history    스케줄 변동 감지
    - gold_hourly_terminal_workload   부두/시간대별 예상 작업량
    - gold_today_terminal_schedule    오늘(CURRENT_DATE) 스케줄
  [물동량 예측 - silver_monthly_container_* 필요, 있을 때만]
    - gold_monthly_container          2022~2026 월별 물동량 통합
    - gold_ml_feature_table           ML feature table

원본 대비 변경점:
  - USE CATALOG/SCHEMA, GRANT, current_user() 등 Databricks 종속 셀 제거
  - Unity Catalog 테이블 참조 -> 로컬 Delta를 임시뷰로 등록 후 spark.sql
  - CREATE OR REPLACE TABLE x AS <쿼리>  ->  spark.sql(<쿼리>) 결과를 Delta로 .save()
  - ★변동감지/오늘스케줄 윈도우 정렬: parsed_at -> snapshot_id
    (로컬은 여러 스냅샷을 한 번에 처리해 parsed_at이 동일 -> snapshot_id가 올바른 시간키)
  SQL 본문(집계/파생 로직)은 원본 그대로.

실행: 프로젝트 루트에서  python 04_silver_to_gold.py  (02 먼저 실행되어 있어야 함)
"""
import os
import sys
from pyspark.sql import functions as F
from spark_session import get_spark

SILVER_BERTH = "data/lakehouse/silver/silver_berth_schedule"
GOLD_DIR     = "data/lakehouse/gold"
MONTHLY_YEARS = [2022, 2023, 2024, 2025, 2026]
MONTHLY_PATH = "data/lakehouse/silver/silver_monthly_container_{year}"


def write_gold(spark, name, df):
    """Delta로 저장 + 임시뷰 등록(다음 쿼리에서 참조 가능) + 건수 출력."""
    path = os.path.join(GOLD_DIR, name)
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").save(path))
    df.createOrReplaceTempView(name)
    print(f"  [OK] {name:32} -> {path}  ({spark.read.format('delta').load(path).count()}행)")


def main():
    spark = get_spark("silver-to-gold")
    spark.sparkContext.setLogLevel("ERROR")

    # silver_berth_schedule 를 임시뷰로 등록 (SQL에서 이름으로 참조)
    SILVER_HIST = "data/lakehouse/silver/silver_berth_schedule_historical"
    spark.read.format("delta").load(SILVER_BERTH).createOrReplaceTempView("_silver_rt")
    if os.path.exists(SILVER_HIST):
        spark.read.format("delta").load(SILVER_HIST).createOrReplaceTempView("_silver_hist")
        cols = ", ".join(spark.table("_silver_rt").columns)   # 실시간 컬럼 기준으로 정렬
        spark.sql(f"SELECT {cols} FROM _silver_rt "
                  f"UNION ALL SELECT {cols} FROM _silver_hist") \
             .createOrReplaceTempView("silver_berth_schedule")
        print("  (과거 이력 포함하여 gold 생성)")
    else:
        spark.table("_silver_rt").createOrReplaceTempView("silver_berth_schedule")

    # =====================================================================
    # 스케줄 대시보드
    # =====================================================================
    # ----- [1] gold_integrated_schedule -----
    write_gold(spark, "gold_integrated_schedule", spark.sql("""
        WITH base AS (
            SELECT
                terminal_id, terminal_name, snapshot_id, bronze_row_id,
                source_file_name, row_hash,
                TRY_CAST(parsed_at AS TIMESTAMP) AS parsed_at,
                berth, carrier, vessel_name, mother_vessel, sun_vessel,
                head_bridge_stern, route,
                TRY_CAST(eta AS TIMESTAMP) AS eta,
                TRY_CAST(etd AS TIMESTAMP) AS etd,
                TRY_CAST(closing AS TIMESTAMP) AS closing,
                TRY_CAST(eta_work_date AS DATE) AS eta_work_date,
                discharge, loading, shift, amp,
                status, is_valid, error_msg, business_key
            FROM silver_berth_schedule
        )
        SELECT
            terminal_id, berth, vessel_name, eta, etd, closing,
            DATE(eta) AS eta_date, eta_work_date,
            discharge, loading, shift,
            COALESCE(discharge,0)+COALESCE(loading,0)+COALESCE(shift,0) AS total_workload,
            CASE WHEN eta IS NULL OR etd IS NULL THEN NULL
                 ELSE ROUND((UNIX_TIMESTAMP(etd)-UNIX_TIMESTAMP(eta))/3600,2) END AS stay_hours,
            ROUND((UNIX_TIMESTAMP(etd)-UNIX_TIMESTAMP(eta))/86400,2) AS stay_days,
            CASE WHEN closing IS NULL OR eta IS NULL THEN NULL
                 ELSE ROUND((UNIX_TIMESTAMP(eta)-UNIX_TIMESTAMP(closing))/3600,2) END AS closing_to_eta_hours,
            business_key, source_file_name, row_hash, snapshot_id, bronze_row_id, parsed_at,
            is_valid, error_msg,
            current_timestamp() AS created_at
        FROM base
    """))

    # ----- [2] gold_schedule_change_history  (정렬: parsed_at -> snapshot_id) -----
    write_gold(spark, "gold_schedule_change_history", spark.sql("""
        WITH ordered AS (
            SELECT
                terminal_id, berth, vessel_name,
                eta, etd, closing, eta_date, eta_work_date,
                discharge, loading, shift, total_workload,
                stay_hours, stay_days, closing_to_eta_hours,
                is_valid, error_msg,
                snapshot_id, source_file_name, parsed_at, business_key, row_hash,
                LAG(row_hash)    OVER (PARTITION BY business_key ORDER BY snapshot_id) AS prev_row_hash,
                LAG(snapshot_id) OVER (PARTITION BY business_key ORDER BY snapshot_id) AS prev_snapshot_id,
                LAG(parsed_at)   OVER (PARTITION BY business_key ORDER BY snapshot_id) AS prev_parsed_at,
                LAG(eta)         OVER (PARTITION BY business_key ORDER BY snapshot_id) AS prev_eta,
                LAG(etd)         OVER (PARTITION BY business_key ORDER BY snapshot_id) AS prev_etd,
                LAG(closing)     OVER (PARTITION BY business_key ORDER BY snapshot_id) AS prev_closing,
                LAG(berth)       OVER (PARTITION BY business_key ORDER BY snapshot_id) AS prev_berth,
                LAG(discharge)   OVER (PARTITION BY business_key ORDER BY snapshot_id) AS prev_discharge,
                LAG(loading)     OVER (PARTITION BY business_key ORDER BY snapshot_id) AS prev_loading,
                LAG(shift)       OVER (PARTITION BY business_key ORDER BY snapshot_id) AS prev_shift
            FROM gold_integrated_schedule
        ),
        changed AS (
            SELECT
                terminal_id, berth, prev_berth, vessel_name,
                eta, prev_eta, etd, prev_etd, closing, prev_closing,
                eta_date, eta_work_date,
                discharge, prev_discharge, loading, prev_loading, shift, prev_shift,
                total_workload, stay_hours, stay_days, closing_to_eta_hours,
                is_valid, error_msg,
                prev_snapshot_id, snapshot_id AS current_snapshot_id,
                prev_parsed_at,   parsed_at   AS current_parsed_at,
                source_file_name, business_key,
                CASE WHEN prev_row_hash IS NULL THEN 'NEW'
                     WHEN prev_row_hash <> row_hash THEN 'CHANGED'
                     ELSE 'NO_CHANGE' END AS change_type,
                CASE WHEN prev_row_hash IS NULL THEN 'new_schedule'
                     WHEN prev_eta      <> eta      THEN 'ETA_change'
                     WHEN prev_etd      <> etd      THEN 'ETD_chaged'
                     WHEN prev_closing  <> closing  THEN 'closing_change'
                     WHEN prev_berth    <> berth    THEN 'berth_change'
                     WHEN prev_discharge<> discharge THEN 'discharge_change'
                     WHEN prev_loading  <> loading  THEN 'loading_change'
                     WHEN prev_shift    <> shift    THEN 'Shift_change'
                     ELSE 'else_change' END AS change_detail,
                current_timestamp() AS created_at
            FROM ordered
        )
        SELECT * FROM changed WHERE change_type IN ('NEW','CHANGED')
    """))

    # ----- [3] gold_hourly_terminal_workload -----
    write_gold(spark, "gold_hourly_terminal_workload", spark.sql("""
        WITH valid_schedule AS (
            SELECT terminal_id, berth, vessel_name, eta, etd, eta_date, eta_work_date,
                   discharge, loading, shift, total_workload, stay_hours, stay_days,
                   snapshot_id, business_key, parsed_at
            FROM gold_integrated_schedule
            WHERE eta IS NOT NULL AND etd IS NOT NULL AND etd > eta
              AND total_workload IS NOT NULL AND total_workload > 0
              AND stay_hours IS NOT NULL AND stay_hours > 0
        ),
        hourly_expanded AS (
            SELECT *,
                EXPLODE(SEQUENCE(DATE_TRUNC('HOUR', eta), DATE_TRUNC('HOUR', etd), INTERVAL 1 HOUR)) AS work_hour
            FROM valid_schedule
        ),
        hourly_calculated AS (
            SELECT
                terminal_id, work_hour,
                DATE(work_hour) AS work_date,
                HOUR(work_hour) AS work_hour_of_day,
                COUNT(DISTINCT business_key) AS vessel_count,
                SUM(total_workload / stay_hours) AS estimated_hourly_workload,
                SUM(discharge / stay_hours)      AS estimated_hourly_discharge,
                SUM(loading / stay_hours)        AS estimated_hourly_loading,
                SUM(shift / stay_hours)          AS estimated_hourly_shift,
                COLLECT_SET(berth)       AS berth_list,
                COLLECT_SET(vessel_name) AS vessel_list,
                MAX(parsed_at) AS latest_parsed_at
            FROM hourly_expanded
            GROUP BY terminal_id, work_hour
        )
        SELECT
            terminal_id, work_date, work_hour, work_hour_of_day, vessel_count,
            ROUND(estimated_hourly_workload, 2)  AS estimated_hourly_workload,
            ROUND(estimated_hourly_discharge, 2) AS estimated_hourly_discharge,
            ROUND(estimated_hourly_loading, 2)   AS estimated_hourly_loading,
            ROUND(estimated_hourly_shift, 2)     AS estimated_hourly_shift,
            berth_list, vessel_list, latest_parsed_at,
            current_timestamp() AS created_at
        FROM hourly_calculated
        ORDER BY terminal_id, work_hour
    """))

    # ----- [4] gold_today_terminal_schedule  (정렬: parsed_at -> snapshot_id) -----
    # 테스트 시 CURRENT_DATE() 대신 특정 날짜로 보려면 아래 WHERE를 바꾸세요.
    write_gold(spark, "gold_today_terminal_schedule", spark.sql("""
        WITH latest_schedule AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY business_key ORDER BY snapshot_id DESC) AS rn
            FROM gold_integrated_schedule
            WHERE eta_work_date = CURRENT_DATE()
            -- WHERE eta_work_date = DATE('2026-06-29')
        )
        SELECT
            terminal_id, berth, vessel_name, eta, etd, closing, eta_date, eta_work_date,
            discharge, loading, shift, total_workload,
            stay_hours, stay_days, closing_to_eta_hours,
            is_valid, error_msg, snapshot_id, source_file_name, parsed_at, business_key,
            current_timestamp() AS created_at
        FROM latest_schedule
        WHERE rn = 1
        ORDER BY terminal_id, eta
    """))

    # =====================================================================
    # 물동량 예측 (silver_monthly_container_* 있을 때만)
    # =====================================================================
    available = [y for y in MONTHLY_YEARS if os.path.exists(MONTHLY_PATH.format(year=y))]
    if not available:
        print("\n[건너뜀] silver_monthly_container_* 테이블이 로컬에 없어 "
              "gold_monthly_container / gold_ml_feature_table 는 생성하지 않습니다.")
        print("         (월별 물동량 silver를 만들면 자동으로 포함됩니다.)")
    else:
        for y in available:
            spark.read.format("delta").load(MONTHLY_PATH.format(year=y)) \
                 .createOrReplaceTempView(f"silver_monthly_container_{y}")
        union_sql = "\n    UNION ALL\n".join(
            f"    SELECT '{y}' AS year, * FROM silver_monthly_container_{y}" for y in available)

        write_gold(spark, "gold_monthly_container", spark.sql(f"""
            WITH unioned AS (
            {union_sql}
            ),
            cleaned AS (
                SELECT
                    CAST(year AS INT) AS year, CAST(month AS INT) AS month,
                    TRY_CAST(REPLACE(total_full,  ',', '') AS DOUBLE) AS total_full,
                    TRY_CAST(REPLACE(total_empty, ',', '') AS DOUBLE) AS total_empty,
                    TRY_CAST(REPLACE(import_full, ',', '') AS DOUBLE) AS import_full,
                    TRY_CAST(REPLACE(import_empty,',', '') AS DOUBLE) AS import_empty,
                    TRY_CAST(REPLACE(export_full, ',', '') AS DOUBLE) AS export_full,
                    TRY_CAST(REPLACE(export_empty,',', '') AS DOUBLE) AS export_empty,
                    TRY_CAST(REPLACE(import_transship_full, ',','') AS DOUBLE) AS import_transship_full,
                    TRY_CAST(REPLACE(import_transship_empty,',','') AS DOUBLE) AS import_transship_empty,
                    TRY_CAST(REPLACE(export_transship_full, ',','') AS DOUBLE) AS export_transship_full,
                    TRY_CAST(REPLACE(export_transship_empty,',','') AS DOUBLE) AS export_transship_empty
                FROM unioned
                WHERE month RLIKE '^[0-9]{{1,2}}$'
            )
            SELECT
                year, month, MAKE_DATE(year, month, 1) AS month_date,
                total_full, total_empty, import_full, import_empty,
                export_full, export_empty,
                import_transship_full, import_transship_empty,
                export_transship_full, export_transship_empty,
                COALESCE(total_full,0)+COALESCE(total_empty,0) AS total_container,
                COALESCE(import_full,0)+COALESCE(import_empty,0) AS import_total,
                COALESCE(export_full,0)+COALESCE(export_empty,0) AS export_total,
                COALESCE(import_transship_full,0)+COALESCE(import_transship_empty,0)
                + COALESCE(export_transship_full,0)+COALESCE(export_transship_empty,0) AS transship_total,
                current_timestamp() AS created_at
            FROM cleaned
        """))

        write_gold(spark, "gold_ml_feature_table", spark.sql("""
            WITH base AS (
                SELECT year, month, month_date,
                    total_full, total_empty, total_container,
                    import_total, export_total, transship_total,
                    import_full, import_empty, export_full, export_empty,
                    CASE WHEN total_container=0 THEN NULL ELSE total_empty/total_container END AS empty_ratio,
                    CASE WHEN total_container=0 THEN NULL ELSE transship_total/total_container END AS transship_ratio,
                    CASE WHEN export_total=0 THEN NULL ELSE import_total/export_total END AS import_export_ratio
                FROM gold_monthly_container
            ),
            features AS (
                SELECT *,
                    LAG(total_container,1)  OVER (ORDER BY month_date) AS prev_1month_total_container,
                    LAG(total_container,12) OVER (ORDER BY month_date) AS prev_12month_total_container,
                    AVG(total_container) OVER (ORDER BY month_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_3month_avg_total_container,
                    AVG(total_container) OVER (ORDER BY month_date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS rolling_6month_avg_total_container
                FROM base
            )
            SELECT
                year, month, month_date,
                total_container, total_full, total_empty,
                import_total, export_total, transship_total,
                empty_ratio, transship_ratio, import_export_ratio,
                prev_1month_total_container, prev_12month_total_container,
                rolling_3month_avg_total_container, rolling_6month_avg_total_container,
                CASE WHEN prev_1month_total_container IS NULL OR prev_1month_total_container=0 THEN NULL
                     ELSE (total_container-prev_1month_total_container)/prev_1month_total_container END AS mom_growth_rate,
                CASE WHEN prev_12month_total_container IS NULL OR prev_12month_total_container=0 THEN NULL
                     ELSE (total_container-prev_12month_total_container)/prev_12month_total_container END AS yoy_growth_rate,
                current_timestamp() AS created_at
            FROM features
            ORDER BY month_date
        """))

    # 요약
    print("\n변동 유형 요약:")
    spark.sql("""
        SELECT change_type, change_detail, COUNT(*) AS cnt
        FROM gold_schedule_change_history
        GROUP BY change_type, change_detail ORDER BY change_type, cnt DESC
    """).show(truncate=False)

    spark.stop()
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0)


if __name__ == "__main__":
    main()