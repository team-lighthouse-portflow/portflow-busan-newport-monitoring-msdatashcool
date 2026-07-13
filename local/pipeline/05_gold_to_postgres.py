"""
05_gold_to_postgres.py
----------------------
Gold Delta 테이블들을 로컬 PostgreSQL로 적재 (Spark JDBC).
Azure의 'Gold -> Azure SQL DB' 단계를 로컬 'Gold -> PostgreSQL'로 포팅.

준비물:
  1) PostgreSQL에 DB 한 개 미리 생성 (1회):
       CREATE DATABASE busan_port;
  2) pip install psycopg2-binary    (스키마 자동 생성용)
  3) PostgreSQL JDBC 드라이버는 Maven에서 자동 다운로드(인터넷 필요, 최초 1회)

하는 일:
  data/lakehouse/gold/ 의 6개 테이블을 읽어
  - array 컬럼(berth_list, vessel_list)은 ", " 로 합쳐 문자열로 변환
  - PostgreSQL 의 gold 스키마에 테이블별 overwrite 적재

실행: 프로젝트 루트에서  python 05_gold_to_postgres.py
"""
import os
import sys
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType
from spark_session import get_spark


# ====================== 접속 정보 (본인 환경에 맞게 수정) ======================
PG_HOST   = "localhost"
PG_PORT   = "5432"
PG_DB     = "busan_port"     # 미리 만들어 둔 데이터베이스
PG_USER   = "postgres"
PG_PWD = os.environ.get("PG_PASSWORD", "")
PG_SCHEMA = "gold"           # 적재할 스키마 (public 쓰려면 "public")
# ============================================================================

GOLD_DIR     = "data/lakehouse/gold"
PG_DRIVER_PKG = "org.postgresql:postgresql:42.7.4"
JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

TABLES = [
    "gold_integrated_schedule",
    "gold_schedule_change_history",
    "gold_hourly_terminal_workload",
    "gold_today_terminal_schedule",
    "gold_monthly_container",
    "gold_ml_feature_table",
]


def ensure_schema():
    """psycopg2로 스키마 없으면 생성. (없으면 안내만 하고 진행)"""
    try:
        import psycopg2
    except ImportError:
        print(f"[안내] psycopg2 미설치 → 스키마 자동생성 건너뜀. "
              f"PostgreSQL에서 'CREATE SCHEMA IF NOT EXISTS {PG_SCHEMA};' 를 먼저 실행하세요.")
        return
    try:
        conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                                user=PG_USER, password=PG_PWD)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{PG_SCHEMA}";')
        conn.close()
        print(f"[준비] 스키마 확인/생성 완료: {PG_SCHEMA}")
    except Exception as e:
        print(f"[오류] PostgreSQL 연결 실패: {e}")
        print(f"       DB '{PG_DB}' 가 존재하는지, 접속정보가 맞는지 확인하세요 "
              f"(CREATE DATABASE {PG_DB};).")
        sys.exit(1)


def flatten_arrays(df):
    """array 컬럼은 JDBC 적재가 까다로우니 ', ' 로 합쳐 문자열로."""
    for field in df.schema.fields:
        if isinstance(field.dataType, ArrayType):
            df = df.withColumn(field.name, F.concat_ws(", ", F.col(field.name)))
    return df


def main():
    ensure_schema()

    spark = get_spark("gold-to-postgres", extra_packages=[PG_DRIVER_PKG])
    spark.sparkContext.setLogLevel("ERROR")

    props = {"user": PG_USER, "password": PG_PWD, "driver": "org.postgresql.Driver"}

    loaded = 0
    for t in TABLES:
        path = os.path.join(GOLD_DIR, t)
        if not os.path.exists(path):
            print(f"  [건너뜀] {t}: Delta 테이블 없음")
            continue
        df = flatten_arrays(spark.read.format("delta").load(path))
        (df.write
            .mode("overwrite")          # 기존 테이블 drop 후 재생성
            .jdbc(JDBC_URL, f"{PG_SCHEMA}.{t}", properties=props))
        print(f"  [OK] {t}: {df.count()}행 -> {PG_SCHEMA}.{t}")
        loaded += 1

    print(f"\n완료: PostgreSQL {PG_DB}.{PG_SCHEMA} 에 {loaded}개 테이블 적재.")

    # 적재 검증: 한 테이블 읽어서 건수 확인
    if loaded:
        check = (spark.read.jdbc(
            JDBC_URL, f"{PG_SCHEMA}.gold_integrated_schedule", properties=props))
        print(f"검증(gold_integrated_schedule 읽기): {check.count()}행")

    spark.stop()
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0)


if __name__ == "__main__":
    main()