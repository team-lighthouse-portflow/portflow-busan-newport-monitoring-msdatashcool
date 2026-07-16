-- 2시간 이내 선박만 조회하는 뷰테이블
CREATE OR REPLACE VIEW [YOUR_CATALOG].[YOUR_SCHEMA_GOLD].v_demo_arrival_within_2h AS
SELECT
  terminal_id,
  berth,
  vessel_name,
  eta,
  etd,
  total_workload
FROM [YOUR_CATALOG].[YOUR_SCHEMA_GOLD].[gold_today_terminal_schedule]
WHERE eta >= current_timestamp()
  AND eta < current_timestamp() + INTERVAL 2 HOURS;



-- 변경된 스케줄의 상세내용과 변경횟수를 함께 조회하는 뷰테이블 
CREATE OR REPLACE VIEW [YOUR_CATALOG].[YOUR_SCHEMA_GOLD].v_demo_schedule_change_summary AS
SELECT 
  any_value(created_at) as created_at,
  change_detail,
  COUNT(*) AS change_count
FROM [YOUR_CATALOG].[YOUR_SCHEMA_GOLD].[gold_schedule_change_history]
GROUP BY change_detail
ORDER BY created_at;



-- 각 부두별 선박 수와 총 작업량만 조회하는 뷰테이블
CREATE OR REPLACE VIEW [YOUR_CATALOG].[YOUR_SCHEMA_GOLD].v_demo_terminal_vessel_count AS
SELECT
  terminal_id,
  COUNT(*) AS vessel_count,
  SUM(total_workload) AS total_workload
FROM [YOUR_CATALOG].[YOUR_SCHEMA_GOLD].[gold_today_terminal_schedule]
GROUP BY terminal_id;



-- 각 부두별 선박 수와 총 작업량, 총 하역량, 총 적재량을 조회하는 뷰테이블
-- 부두별 선박 작업량을 보다 상세히 조회한다.
CREATE OR REPLACE VIEW [YOUR_CATALOG].[YOUR_SCHEMA_GOLD].v_demo_terminal_status AS
SELECT
  terminal_id,
  COUNT(*) AS vessel_count,
  SUM(total_workload) AS total_workload,
  SUM(discharge) AS total_discharge,
  SUM(loading) AS total_loading
FROM [YOUR_CATALOG].[YOUR_SCHEMA_GOLD].[gold_today_terminal_schedule]
GROUP BY terminal_id;