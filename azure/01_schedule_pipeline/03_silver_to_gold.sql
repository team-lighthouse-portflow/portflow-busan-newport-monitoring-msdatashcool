/* =============================================================================
  03_silver_to_gold

  목적
  -----------------------------------------------------------------------------
  Silver Layer의 표준화된 부산신항 선석 스케줄 데이터를 기반으로
  Power BI 대시보드와 ML 예측 모델에서 사용할 Gold Layer 서비스 테이블을 생성한다.

  생성 테이블
  -----------------------------------------------------------------------------
  1. gold_integrated_schedule
     - 7개 부두 선석 스케줄 통합 테이블

  2. gold_schedule_change_history
     - ETA, ETD, Closing, 선석, 작업량 등 스케줄 변경 이력 테이블

  3. gold_today_terminal_schedule
     - 운영일 기준 최신 스케줄만 남긴 오늘의 스케줄 테이블

  4. gold_hourly_terminal_workload
     - 부두별·시간대별 예상 작업량 집계 테이블

  비고
  -----------------------------------------------------------------------------
  개발 당시 각 Gold Table 생성 후 display(table.limit(10))으로 검증했다.
  GitHub에는 실행 결과를 제외하고 SQL 로직 중심으로 관리한다.
============================================================================= */

-- =====================================
-- 1. gold_integrated_schedule
-- =====================================
CREATE OR REPLACE TABLE gold_integrated_schedule AS

WITH base AS (
    --  silver_berth_schedule 에 있는 컬럼들 조회
    SELECT
        terminal_id,
        terminal_name,
        snapshot_id,
        bronze_row_id,
        source_file_name,
        row_hash,

        TRY_CAST(parsed_at AS TIMESTAMP) AS parsed_at,

        berth,
        carrier,
        vessel_name,
        mother_vessel,
        sun_vessel,
        head_bridge_stern,
        route,

        TRY_CAST(eta AS TIMESTAMP) AS eta,
        TRY_CAST(etd AS TIMESTAMP) AS etd,
        TRY_CAST(closing AS TIMESTAMP) AS closing,
        TRY_CAST(eta_work_date AS DATE) AS eta_work_date,

        discharge,
        loading,
        shift,
        amp,

        status,
        is_valid,
        error_msg,
        business_key

    FROM silver.silver_berth_schedule
),

final AS (
    SELECT
        -- 현직에서 쓰고 있는 컬럼
        terminal_id,
        berth,
        vessel_name,
        eta,
        etd,
        closing,
        
        -- 스케줄에 대한 추가정보
        DATE(eta) AS eta_date,
        eta_work_date,
        discharge,
        loading,
        shift,
        
        -- 총 작업시간 : total_workload = discharge + loading + shift
        COALESCE(discharge, 0)+ COALESCE(loading, 0) + COALESCE(shift, 0) AS total_workload,

         -- 체류시간: stay_hours = etd - eta
        CASE
            WHEN eta IS NULL OR etd IS NULL THEN NULL
            ELSE ROUND((UNIX_TIMESTAMP(etd) - UNIX_TIMESTAMP(eta)) / 3600, 2)
        END AS stay_hours,

        -- 체류 기간: stay_days = edt - eta
        ROUND((UNIX_TIMESTAMP(etd) - UNIX_TIMESTAMP(eta)) / 86400, 2) AS stay_days,

        -- 컨테이너 반입마감시간부터 선박 입항 예정시간까지 남은시간 : closingt_to_eta_hours = eta - closing
        CASE
            WHEN closing IS NULL OR eta IS NULL THEN NULL
            ELSE ROUND((UNIX_TIMESTAMP(eta) - UNIX_TIMESTAMP(closing)) / 3600, 2)
        END AS closing_to_eta_hours,

        -- 스케줄 변동 감지에 필요한 컬럼

        business_key,
        source_file_name,
        row_hash,
        snapshot_id,
        bronze_row_id,
        parsed_at,
        
        mother_vessel,
        sun_vessel,
        carrier,
        route,

        is_valid,
        error_msg,

        -- gold 변경 감지용 key : 부두 변경 감지를 위해 terminal_id를 넣지 않음
        CONCAT_WS(
            '_',
            CASE
                WHEN NULLIF(TRIM(mother_vessel), '') IS NOT NULL
                    THEN CONCAT('MV_', NULLIF(TRIM(mother_vessel), ''))

                WHEN NULLIF(TRIM(vessel_name), '') IS NOT NULL
                    THEN CONCAT('VN_', NULLIF(TRIM(vessel_name), ''))

                WHEN NULLIF(TRIM(sun_vessel), '') IS NOT NULL
                    THEN CONCAT('SV_', NULLIF(TRIM(sun_vessel), ''))

                WHEN NULLIF(TRIM(route), '') IS NOT NULL
                    THEN CONCAT('RT_', NULLIF(TRIM(route), ''))

                WHEN NULLIF(TRIM(carrier), '') IS NOT NULL
                    THEN CONCAT(
                        'WEAK_',
                        NULLIF(TRIM(carrier), ''),
                        '_',
                        COALESCE(CAST(discharge AS STRING), '0'),
                        '_',
                        COALESCE(CAST(loading AS STRING), '0')
                    )

                ELSE CONCAT(
                    'UNKNOWN_',
                    COALESCE(CAST(discharge AS STRING), '0'),
                    '_',
                    COALESCE(CAST(loading AS STRING), '0')
                )
            END,
            COALESCE(CAST(eta_work_date AS STRING), 'UNKNOWN_DATE')
        ) AS gold_key,

        -- gold_key 에 쓰인 컬럼 확인용
        CASE
            WHEN NULLIF(TRIM(mother_vessel), '') IS NOT NULL THEN 'mother_vessel'
            WHEN NULLIF(TRIM(vessel_name), '') IS NOT NULL THEN 'vessel_name'
            WHEN NULLIF(TRIM(sun_vessel), '') IS NOT NULL THEN 'sun_vessel'
            WHEN NULLIF(TRIM(route), '') IS NOT NULL THEN 'route'
            WHEN NULLIF(TRIM(carrier), '') IS NOT NULL THEN 'weak_carrier_workload'
            ELSE 'unknown'
        END AS gold_key_type,


        -- 현재시각 
        current_timestamp() AS created_at

    FROM base
)

SELECT *
FROM final;


-- =====================================
-- 2. gold_schedule_change_history
-- =====================================
CREATE OR REPLACE TABLE gold_schedule_change_history AS

WITH ordered AS (
    SELECT
        -- 기본컬럼
        terminal_id,
        berth,
        vessel_name,

        eta,
        etd,
        closing,
        eta_date,
        eta_work_date,

        -- 작업량 컬럼
        discharge,
        loading,
        shift,
        total_workload,

        -- 체류기간 등 스케줄 컬럼
        stay_hours,
        stay_days,
        closing_to_eta_hours,

        -- 에러 컬럼
        is_valid,
        error_msg,

        -- 스냅샷 등 컬럼
        snapshot_id,
        source_file_name,
        parsed_at,
        business_key,
        row_hash,
        gold_key,
        gold_key_type,

        -- business_key별 최신/이전 스냅샷 비교 컬럼
        LAG(row_hash) OVER (
            PARTITION BY gold_key
            ORDER BY parsed_at, snapshot_id
        ) AS prev_row_hash,

        LAG(snapshot_id) OVER (
            PARTITION BY gold_key
            ORDER BY parsed_at, snapshot_id
        ) AS prev_snapshot_id,

        LAG(parsed_at) OVER (
            PARTITION BY gold_key
            ORDER BY parsed_at, snapshot_id
        ) AS prev_parsed_at,

        LAG(eta) OVER (
            PARTITION BY gold_key
            ORDER BY parsed_at, snapshot_id
        ) AS prev_eta,

        LAG(etd) OVER (
            PARTITION BY gold_key
            ORDER BY parsed_at, snapshot_id
        ) AS prev_etd,

        LAG(closing) OVER (
            PARTITION BY gold_key
            ORDER BY parsed_at, snapshot_id
        ) AS prev_closing,

        LAG(berth) OVER (
            PARTITION BY gold_key
            ORDER BY parsed_at, snapshot_id
        ) AS prev_berth,

        LAG(discharge) OVER (
            PARTITION BY gold_key
            ORDER BY parsed_at, snapshot_id
        ) AS prev_discharge,

        LAG(loading) OVER (
            PARTITION BY gold_key
            ORDER BY parsed_at, snapshot_id
        ) AS prev_loading,

        LAG(shift) OVER (
            PARTITION BY gold_key
            ORDER BY parsed_at, snapshot_id
        ) AS prev_shift,

        LAG(terminal_id) OVER (
            PARTITION BY gold_key
            ORDER BY parsed_at, snapshot_id
        ) AS prev_terminal_id

    FROM gold_integrated_schedule
),

changed AS (
    SELECT
        terminal_id,
        prev_terminal_id,
        berth,
        prev_berth,
        vessel_name,

        eta,
        prev_eta,
        etd,
        prev_etd,
        closing,
        prev_closing,

        eta_date,
        eta_work_date,

        discharge,
        prev_discharge,
        loading,
        prev_loading,
        shift,
        prev_shift,

        total_workload,
        stay_hours,
        stay_days,
        closing_to_eta_hours,
        is_valid,
        error_msg,

        prev_snapshot_id,
        snapshot_id AS current_snapshot_id,

        prev_parsed_at,
        parsed_at AS current_parsed_at,

        source_file_name,
        business_key,
        gold_key,
        gold_key_type,
        CASE
            WHEN prev_row_hash IS NULL THEN 'NEW'
            WHEN prev_row_hash <> row_hash THEN 'CHANGED'
            ELSE 'NO_CHANGE'
        END AS change_type,

        CASE
            WHEN prev_row_hash IS NULL THEN 'new_schedule'
            WHEN NOT (prev_terminal_id <=> terminal_id) THEN 'terminal_change'
            WHEN NOT (prev_berth <=> berth) THEN 'berth_change'
            WHEN NOT (prev_eta <=> eta) THEN 'ETA_change'
            WHEN NOT (prev_etd <=> etd) THEN 'ETD_change'
            WHEN NOT (prev_closing <=> closing) THEN 'closing_change'
            WHEN NOT (prev_discharge <=> discharge) THEN 'discharge_change'
            WHEN NOT (prev_loading <=> loading) THEN 'loading_change'
            WHEN NOT (prev_shift <=> shift) THEN 'shift_change'
            ELSE 'else_change'
        END AS change_detail,

        current_timestamp() AS created_at

    FROM ordered
)

SELECT *
FROM changed
WHERE change_type IN ('NEW', 'CHANGED');


-- ======================================
-- 3. gold_today_terminal_schedule
-- ======================================
CREATE OR REPLACE TABLE gold_today_terminal_schedule AS

WITH latest_schedule AS (
    -- gold_key 기준으로 가장 최신 parsed_at 만 남기기
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY gold_key
            ORDER BY parsed_at DESC
        ) AS rn
    FROM gold_integrated_schedule
    WHERE eta_work_date = CURRENT_DATE()
    --WHERE eta_work_date = DATE('2026-06-22')
)

SELECT 
    -- 기본 컬럼
    terminal_id,
    berth,
    vessel_name,
    COALESCE(
        NULLIF(TRIM(vessel_name), ''),
        NULLIF(TRIM(mother_vessel), ''),
        NULLIF(TRIM(sun_vessel), ''),
        CONCAT('미확인 선박(', COALESCE(carrier, 'UNKNOWN'), ')')
    ) AS vessel_display_name,
    
    carrier,
    eta,
    etd,
    closing,
    eta_date,
    eta_work_date,

    -- 작업량 컬럼
    discharge,
    loading,
    shift,
    total_workload,

    stay_hours,
    stay_days,
    closing_to_eta_hours,

    -- 스케줄 변경 감지 컬럼
    is_valid,
    error_msg,

    snapshot_id,
    source_file_name,
    parsed_at,
    business_key,
    gold_key,
    gold_key_type,

    current_timestamp() AS created_at

FROM latest_schedule
WHERE rn = 1
ORDER BY terminal_id, eta;

-- =======================================
-- 4. gold_hourly_terminal_workload
-- =======================================
CREATE OR REPLACE TABLE gold_hourly_terminal_workload AS

WITH latest AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY gold_key
            ORDER BY parsed_at DESC, snapshot_id DESC
        ) AS rn
    FROM gold_integrated_schedule
),

valid_schedule AS (
    -- 오류 없는 유효한 스케줄만 조회하기 
    SELECT
        terminal_id,
        berth,
        vessel_name,

        eta,
        etd,
        eta_date,
        eta_work_date,

        discharge,
        loading,
        shift,
        total_workload,

        stay_hours,
        stay_days,

        snapshot_id,
        business_key,
        gold_key,
        gold_key_type,
        parsed_at

    FROM latest
    WHERE rn = 1
      AND eta IS NOT NULL
      AND etd IS NOT NULL
      AND etd > eta
      AND total_workload IS NOT NULL
      AND total_workload > 0
      AND stay_hours IS NOT NULL
      AND stay_hours > 0
),

hourly_expanded AS (
    SELECT
        terminal_id,
        berth,
        vessel_name,

        eta,
        etd,
        eta_date,
        eta_work_date,

        discharge,
        loading,
        shift,
        total_workload,
        stay_hours,
        stay_days,

        snapshot_id,
        business_key,
        gold_key,
        gold_key_type,
        parsed_at,

        -- work_hour : 한시간 단위로 eta, etd 나누기 위한 작업 
        EXPLODE(
            SEQUENCE(
                DATE_TRUNC('HOUR', eta),
                DATE_TRUNC('HOUR', etd),
                INTERVAL 1 HOUR
            )
        ) AS work_hour

    FROM valid_schedule
),

hourly_calculated AS (
    SELECT
        terminal_id,
        work_hour,

        -- work_date: 작업시간대 (work_hour)를 날짜 단위로 변환
        DATE(work_hour) AS work_date,

        -- work_hour_of_day: 작업시간대 (work_hour)에서 추출한 0~23시 시간값 
        HOUR(work_hour) AS work_hour_of_day,

        -- vessel_count: 선박 수
        COUNT(DISTINCT gold_key) AS vessel_count,

        -- ETA~ETD 사이에 작업량이 균등하게 진행된다고 가정한 예상값 --
        -- estimated_hourly_workload: 시간당 예상 총 작업량.  총 작업량 / 체류시간
        SUM(total_workload / stay_hours) AS estimated_hourly_workload,

        -- estimated_hourly_discharge: 시간당 예상 양하 작업량. 양하 작업량 / 체류시간
        SUM(discharge / stay_hours) AS estimated_hourly_discharge,

        -- estimated_hourly_loading: 시간당 예상 적하 작업량. 적하 작업량 / 체류시간
        SUM(loading / stay_hours) AS estimated_hourly_loading,

        -- estimated_hourly_shift: 시간당 예상 Shift 작업량. Shift 작업량 / 체류시간
        SUM(shift / stay_hours) AS estimated_hourly_shift,

        -- berth_list, vessel_list: 해당 시간대에 작업중인 선석 목록, 선명 목록
        CONCAT_WS(', ', COLLECT_SET(berth)) AS berth_list,
        CONCAT_WS(', ', COLLECT_SET(vessel_name)) AS vessel_list,

        -- 해당 시간대 집계에 포함된 스케줄 데이터 중 가장 최근 파싱 시각 
        MAX(parsed_at) AS latest_parsed_at

    FROM hourly_expanded
    GROUP BY
        terminal_id,
        work_hour
)

SELECT
    terminal_id,
    work_date,
    work_hour,
    work_hour_of_day,

    vessel_count,

    ROUND(estimated_hourly_workload, 2) AS estimated_hourly_workload,
    ROUND(estimated_hourly_discharge, 2) AS estimated_hourly_discharge,
    ROUND(estimated_hourly_loading, 2) AS estimated_hourly_loading,
    ROUND(estimated_hourly_shift, 2) AS estimated_hourly_shift,

    berth_list,
    vessel_list,

    latest_parsed_at,
    current_timestamp() AS created_at

FROM hourly_calculated
ORDER BY terminal_id, work_hour;
