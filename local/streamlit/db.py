"""
db.py
-----
PostgreSQL(busan_port) 연결 + 쿼리 캐싱 공용 모듈.

연결 정보 우선순위:
  1) .streamlit/secrets.toml 의 [postgres] 섹션
  2) 환경변수 (PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD)
  3) 기본값 (localhost:5432 / busan_port / postgres / postgres)

모든 gold 테이블은 gold 스키마에 있다고 가정한다.
"""
import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text


@st.cache_resource(show_spinner=False)
def get_engine():
    cfg = {}
    try:
        cfg = dict(st.secrets["postgres"])  # secrets.toml 이 있으면 사용
    except Exception:
        pass
    host = cfg.get("host", os.getenv("PGHOST", "localhost"))
    port = cfg.get("port", os.getenv("PGPORT", "5432"))
    dbnm = cfg.get("dbname", os.getenv("PGDATABASE", "busan_port"))
    user = cfg.get("user", os.getenv("PGUSER", "postgres"))
    pw = cfg.get("password", os.getenv("PGPASSWORD", "postgres"))
    url = f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{dbnm}"
    return create_engine(url, pool_pre_ping=True)


@st.cache_data(ttl=600, show_spinner="데이터를 불러오는 중...")
def run_query(sql: str, params: dict | None = None) -> pd.DataFrame:
    """SELECT 결과를 DataFrame으로. ttl=600초(10분) 캐시."""
    eng = get_engine()
    with eng.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


def distinct_values(table: str, col: str, schema: str = "gold") -> list:
    """필터 옵션용 DISTINCT 값. col 이 한글이면 호출부에서 따옴표 포함해 전달."""
    df = run_query(
        f'SELECT DISTINCT {col} AS v FROM {schema}.{table} '
        f'WHERE {col} IS NOT NULL ORDER BY v'
    )
    return df["v"].tolist()


# terminal_id → 부두 이름 매핑
# (메모리 기준 부두 순서 PNIT,PNC,HJNC,HPNT,BNCT,BCT,DGT + BCT=terminal_6 단서로 추정)
# 실제 매핑이 다르면 이 dict 만 수정하면 전 화면에 반영됨.
TERMINAL_NAMES = {
    "terminal_1": "PNIT",
    "terminal_2": "PNC",
    "terminal_3": "HJNC",
    "terminal_4": "HPNT",
    "terminal_5": "BNCT",
    "terminal_6": "BCT",
    "terminal_7": "DGT",
}


def tname(v):
    """terminal_id(예: 'terminal_1', '1', 1)를 부두 이름으로. 미매핑이면 원값 반환."""
    if v is None:
        return v
    s = str(v).strip().lower()
    if s in TERMINAL_NAMES:
        return TERMINAL_NAMES[s]
    key = f"terminal_{s}"
    if key in TERMINAL_NAMES:
        return TERMINAL_NAMES[key]
    return str(v)
