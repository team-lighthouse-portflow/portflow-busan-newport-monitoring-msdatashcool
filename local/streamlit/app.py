"""
부산신항 선석 대시보드 — 진입/라우터
실행: streamlit run app.py

st.navigation 으로 페이지를 명시 등록 → 사이드바에 'app' 같은 진입파일명이 뜨지 않고,
pages/ 자동탐색을 쓰지 않으므로 폴더에 남은 옛 파일이 있어도 로드되지 않음.
"""
import streamlit as st

st.set_page_config(page_title="부산신항 선석 대시보드", page_icon="🚢", layout="wide")

pages = [
    st.Page("views/1_schedule.py", title="통합 선석 스케줄", icon="🚢", default=True),
    st.Page("views/2_changes.py", title="스케줄 변경 알림", icon="🔔"),
    st.Page("views/3_workload.py", title="부두별 작업현황", icon="🏗️"),
    st.Page("views/4_forecast.py", title="물동량 예측", icon="📈"),
]
st.navigation(pages).run()
