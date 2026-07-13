import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.linear_model import Ridge
import calendar
import os
import sys
import glob
from sqlalchemy import create_engine

# 터미널 한글 깨짐 방지
try:
    sys.stdout.reconfigure(encoding='utf-8')
except AttributeError:
    pass

BASE_DIR = "."
SCFI_DIR = os.path.join("data", "raw", "container")   # SCFI 원본(.xls) 위치

PG_HOST = "localhost"
PG_PORT = "5432"
PG_DB   = "busan_port"     # 05에서 쓴 값과 동일하게
PG_USER = "postgres"
PG_PWD  = os.environ.get("PG_PASSWORD", "")       # 05에서 쓴 비밀번호와 동일하게

PG_URI = f"postgresql+psycopg2://{PG_USER}:{PG_PWD}@{PG_HOST}:{PG_PORT}/{PG_DB}?client_encoding=utf8"
engine = create_engine(PG_URI)

# [역사적 가중치 & 환적 비중 상수 정의]  (원본 그대로)
seasonality_weights = {
    1: 1.0087, 2: 0.9362, 3: 1.0504, 4: 1.0302, 5: 1.0615, 6: 0.9980,
    7: 1.0317, 8: 0.9875, 9: 0.9393, 10: 0.9823, 11: 0.9871, 12: 0.9870
}
monthly_trans_avg = {
    1: 64.2, 2: 64.1, 3: 64.3, 4: 64.4, 5: 64.5, 6: 64.3,
    7: 64.2, 8: 64.0, 9: 64.1, 10: 64.2, 11: 64.3, 12: 64.4
}


def load_base_data():
    """원본 load_base_data 대체: gold.gold_integrated_schedule에서 부두별 선석 레코드 구성.
    다운스트림이 기대하는 컬럼을 동일하게 만들어 반환."""
    print("1. gold_integrated_schedule 로드 및 전처리 중...")
    df = pd.read_sql(
        'SELECT terminal_id, berth, vessel_name, eta, etd, stay_hours, '
        'discharge, loading, shift, business_key, snapshot_id, is_valid '
        'FROM gold.gold_integrated_schedule', engine)

    df = df[df['is_valid'] == True].copy()
    df['eta'] = pd.to_datetime(df['eta'], errors='coerce')
    df['etd'] = pd.to_datetime(df['etd'], errors='coerce')
    df = df.dropna(subset=['eta', 'etd'])

    # 시간당 중복 스냅샷 제거: 같은 선박 콜은 최신 snapshot_id만 사용
    df = df.sort_values('snapshot_id').drop_duplicates(
        subset=['terminal_id', 'berth', 'vessel_name', 'eta'], keep='last')

    df['부두'] = df['terminal_id'].str.extract(r'terminal_(\d+)')[0].astype(int).astype(str) + '부두'
    df = df.rename(columns={
        'berth': '선석', 'vessel_name': '모선명',
        'eta': '접안예정일시_dt', 'etd': '출항예정일시_dt',
        'stay_hours': '체류시간_시간', 'shift': '이적수량'})

    for c in ['discharge', 'loading', '이적수량']:
        df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
    df['양하수량'] = df['discharge']
    df['적하수량'] = df['loading']
    df['총물동량'] = df['양하수량'] + df['적하수량']

    df['체류시간_시간'] = pd.to_numeric(df['체류시간_시간'], errors='coerce')
    df = df.dropna(subset=['체류시간_시간'])
    df = df[df['체류시간_시간'] < 240.0]

    df = df[~df['모선명'].astype(str).str.upper().str.contains(
        'STORAGE|DUMMY|공컨|테스트|TEST', na=False)]

    df['연도'] = df['출항예정일시_dt'].dt.year
    df['월'] = df['출항예정일시_dt'].dt.month
    df = df[((df['연도'] >= 2022) & (df['연도'] <= 2025)) |
            ((df['연도'] == 2026) & (df['월'] <= 6))]

    # 부두별 특수 보정 (원본 유지)
    df = df[~((df['부두'] == '6부두') & (df['연도'] == 2022))].copy()
    df = df[~((df['부두'] == '7부두') & (df['출항예정일시_dt'] < '2025-07-01'))].copy()

    print(f"   [데이터 범위] 연도={sorted(df['연도'].dropna().unique().tolist())}, 행수={len(df)}")
    return df


def load_trans_ratio():
    """원본의 '월별 환적비' xlsx 직접읽기 대체: gold.gold_monthly_container에서 환적비 계산."""
    g = pd.read_sql(
        'SELECT year AS "연도", month AS "월", import_total, export_total, transship_total '
        'FROM gold.gold_monthly_container', engine)
    denom = g['import_total'].fillna(0) + g['export_total'].fillna(0) + g['transship_total'].fillna(0)
    g['TransshipmentRatio'] = np.where(denom > 0, g['transship_total'].fillna(0) / denom * 100, 0.0)
    return g[['연도', '월', 'TransshipmentRatio']]


df_base = load_base_data()

# 2. 선석 점유율 계산 (구간 병합)
print("2. 부두별 선석 점유율 계산 중...")
df_occ_records = []
for (budu, yr, m), group in df_base.groupby(['부두', '연도', '월']):
    unique_berths = group['선석'].unique()
    num_berths = len(unique_berths) if len(unique_berths) > 0 else 1
    days = calendar.monthrange(yr, m)[1]
    total_avail = num_berths * days * 24.0
    
    total_merged = 0.0
    for berth in unique_berths:
        intervals = sorted(list(zip(group[group['선석'] == berth]['접안예정일시_dt'], group[group['선석'] == berth]['출항예정일시_dt'])))
        if not intervals: continue
        merged = [intervals[0]]
        for curr in intervals[1:]:
            if curr[0] <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(merged[-1][1], curr[1]))
            else:
                merged.append(curr)
        total_merged += sum((end - start).total_seconds() / 3600.0 for start, end in merged)
        
    df_occ_records.append({'부두': budu, '연도': yr, '월': m, 'BerthOccupancy': (total_merged / total_avail * 100) if total_avail > 0 else 0.0})
    
df_occ = pd.DataFrame(df_occ_records)
terminal_monthly_occ_avg = df_occ[df_occ['연도'] < 2026].groupby(['부두', '월'])['BerthOccupancy'].mean().to_dict()

# 3. SCFI 로드 및 스케일링
print("3. SCFI 운임 지수 로드 및 스케일링 중...")
def _read_scfi(path):
    try:
        return pd.read_excel(path).iloc[:, :3]
    except Exception:
        return pd.read_html(path, encoding='utf-8')[0].iloc[:, :3]
scfi_dfs = [_read_scfi(f) for f in glob.glob(os.path.join(SCFI_DIR, "SCFI지수_*.xls"))]
df_scfi = pd.concat(scfi_dfs, ignore_index=True)
df_scfi.columns = ['구분', '운임지수', '등록일']
df_scfi = df_scfi[df_scfi['구분'] == 'SCFI'].copy()
df_scfi['등록일'] = pd.to_datetime(df_scfi['등록일'])
df_scfi['SCFI_raw'] = df_scfi['운임지수'].astype(str).str.replace(',', '').astype(float)
df_scfi['연도'] = df_scfi['등록일'].dt.year
df_scfi['월'] = df_scfi['등록일'].dt.month

df_scfi_m = df_scfi.groupby(['연도', '월'])['SCFI_raw'].mean().reset_index()
df_scfi_m = df_scfi_m.sort_values(['연도', '월']).reset_index(drop=True)
df_scfi_m['SCFI_MoM'] = ((df_scfi_m['SCFI_raw'] - df_scfi_m['SCFI_raw'].shift(1)) / df_scfi_m['SCFI_raw'].shift(1) * 100).fillna(0.0)

# 4. 피처 가공 및 학습 데이터 조립 (Trend-Adjusted YoY 기법 적용)
df_monthly_ops = df_base.groupby(['부두', '연도', '월']).agg(
    총물동량=('총물동량', 'sum'),
    이적수량=('이적수량', 'sum'),
    VesselCount=('모선명', 'count'),
    AvgDwellTime=('체류시간_시간', 'mean'),
    CarrierCount=('선사', 'nunique') if '선사' in df_base.columns else ('모선명', 'count'),
    RouteCount=('ROUTE', 'nunique') if 'ROUTE' in df_base.columns else ('모선명', 'count')
).reset_index()

df_monthly_ops['ShiftRatio'] = (df_monthly_ops['이적수량'] / df_monthly_ops['총물동량'] * 100).fillna(0.0)
df_monthly_ops['ShiftRatio'] = df_monthly_ops['ShiftRatio'].replace([np.inf, -np.inf], 0.0).fillna(0.0)

for col in ['CarrierCount', 'RouteCount']:
    df_monthly_ops[col] = df_monthly_ops[col].replace(0, 1)

# 1부두 2026년 6월 데이터 결손 보정 (24일치만 유입되어 30/24 = 1.25배 증강)
cond_1terminal_june = (df_monthly_ops['부두'] == '1부두') & (df_monthly_ops['연도'] == 2026) & (df_monthly_ops['월'] == 6)
df_monthly_ops.loc[cond_1terminal_june, '총물동량'] = df_monthly_ops.loc[cond_1terminal_june, '총물동량'] * 1.25
df_monthly_ops.loc[cond_1terminal_june, 'VesselCount'] = (df_monthly_ops.loc[cond_1terminal_june, 'VesselCount'] * 1.25).round()

df_ratios = load_trans_ratio()

monthly_agg = pd.merge(df_monthly_ops, df_occ, on=['부두', '연도', '월'], how='left')

# 선석 점유율(BerthOccupancy) 결측치를 부두별, 월별 역사적 평균값으로 보완
df_occ_mean = df_occ[df_occ['연도'] < 2026].groupby(['부두', '월'])['BerthOccupancy'].mean().reset_index()
df_occ_mean = df_occ_mean.rename(columns={'BerthOccupancy': 'BerthOccupancy_mean'})
monthly_agg = pd.merge(monthly_agg, df_occ_mean, on=['부두', '월'], how='left')
monthly_agg['BerthOccupancy'] = monthly_agg['BerthOccupancy'].fillna(monthly_agg['BerthOccupancy_mean']).fillna(50.0)
monthly_agg = monthly_agg.drop(columns=['BerthOccupancy_mean'])

monthly_agg['Weight'] = monthly_agg['월'].map(seasonality_weights)

# 실젯값 병합 후 결측치는 역사적 월평균 딕셔너리로 보안
monthly_agg = pd.merge(monthly_agg, df_ratios, on=['연도', '월'], how='left')
monthly_agg['TransshipmentRatio'] = monthly_agg['TransshipmentRatio'].fillna(monthly_agg['월'].map(monthly_trans_avg)).fillna(64.0)

monthly_agg = pd.merge(monthly_agg, df_scfi_m[['연도', '월', 'SCFI_raw', 'SCFI_MoM']], on=['연도', '월'], how='left').ffill().bfill()

# 월별 삼각함수(Sin/Cos) 변환 피처 추가
monthly_agg['Month_Sin'] = np.sin(2 * np.pi * monthly_agg['월'] / 12.0)
monthly_agg['Month_Cos'] = np.cos(2 * np.pi * monthly_agg['월'] / 12.0)

def compute_trend_adj_yoy_features(group):
    group = group.sort_values(['연도', '월']).reset_index(drop=True)
    group['TimeIndex'] = group.index + 1
    
    lag12_adj_vals = []
    rolling_yoy_vals = []
    vol_dict = {(r['연도'], r['월']): r['총물동량'] for _, r in group.iterrows()}
    
    mean_val = group['총물동량'].mean()
    
    for idx, row in group.iterrows():
        yr, m = row['연도'], row['월']
        
        # 12개월 전 실적 (Lag12)
        yoy_yr = yr - 1
        lag12 = vol_dict.get((yoy_yr, m), np.nan)
        if np.isnan(lag12):
            lag12 = mean_val * seasonality_weights[m]
            
        # 최근 3개월 추세 대비 전년 동기 배율(Trend Ratio) 계산
        trend_ratio = 1.0
        if idx >= 3:
            recent_curr = [group.iloc[idx - i]['총물동량'] for i in range(1, 4)]
            recent_yoy = []
            for i in range(1, 4):
                curr_row = group.iloc[idx - i]
                y_y = curr_row['연도'] - 1
                m_y = curr_row['월']
                val_y = vol_dict.get((y_y, m_y), np.nan)
                if np.isnan(val_y):
                    val_y = mean_val * seasonality_weights[m_y]
                recent_yoy.append(val_y)
                
            mean_curr = sum(recent_curr) / 3.0
            mean_yoy = sum(recent_yoy) / 3.0
            if mean_yoy > 0:
                trend_ratio = mean_curr / mean_yoy
                trend_ratio = max(0.5, min(2.0, trend_ratio)) # 극단적인 급변동 방지
                
        lag12_adj = lag12 * trend_ratio
        
        # RollingAvg3_YoY: 전년도 동월 주변 3개월 평균 (t-11, t-12, t-13)
        m_plus = m + 1 if m < 12 else 1
        yr_plus = yoy_yr if m < 12 else yr
        m_minus = m - 1 if m > 1 else 12
        yr_minus = yoy_yr if m > 1 else yoy_yr - 1
        
        v_11 = vol_dict.get((yr_plus, m_plus), np.nan)
        v_12 = lag12
        v_13 = vol_dict.get((yr_minus, m_minus), np.nan)
        
        if np.isnan(v_11): v_11 = mean_val * seasonality_weights[m_plus]
        if np.isnan(v_13): v_13 = mean_val * seasonality_weights[m_minus]
        
        rolling_yoy = (v_11 + v_12 + v_13) / 3.0
        
        lag12_adj_vals.append(lag12_adj)
        rolling_yoy_vals.append(rolling_yoy)
        
    group['Lag12_Adj'] = lag12_adj_vals
    group['RollingAvg3_YoY'] = rolling_yoy_vals
    return group

advanced_dfs = []
for budu, group in monthly_agg.groupby('부두'):
    advanced_dfs.append(compute_trend_adj_yoy_features(group))
df_all_features = pd.concat(advanced_dfs, ignore_index=True)

# 5. XGBoost + Ridge 앙상블 학습 및 시나리오별 재귀 예측
print("4. XGBoost + Ridge 앙상블 모델 훈련 및 시나리오별 재귀 예측 중...")
feature_cols = ['Weight', 'TransshipmentRatio', 'SCFI_MoM', 'BerthOccupancy', 'Lag12_Adj', 'RollingAvg3_YoY', 'VesselCount', 'AvgDwellTime', 'ShiftRatio', 'Month_Sin', 'Month_Cos']

june_scfi_raw = df_scfi_m.sort_values(['연도', '월']).iloc[-1]['SCFI_raw']

scenarios = {
    'Flat': lambda m_idx: june_scfi_raw,
    'Rise': lambda m_idx: june_scfi_raw * (1.0 + 0.05 * m_idx),
    'Fall': lambda m_idx: june_scfi_raw * (1.0 - 0.05 * m_idx)
}

forecast_results = []
plot_data = {}

# 과거 조업 지표의 월별 평균 구하기
terminal_monthly_ops_avg = {}
for (budu, m), group in df_all_features[df_all_features['연도'] < 2026].groupby(['부두', '월']):
    for col in ['VesselCount', 'AvgDwellTime', 'ShiftRatio', 'TransshipmentRatio']:
        terminal_monthly_ops_avg[(budu, m, col)] = group[col].mean()

for budu, group in df_all_features.groupby('부두'):
    group = group.sort_values(['연도', '월']).reset_index(drop=True)
    
    # Train / Validation 분리 (검증 평가용)
    train_data_eval = group[((group['연도'] < 2026) | ((group['연도'] == 2026) & (group['월'] <= 2)))].dropna(subset=feature_cols).copy()
    val_data_eval = group[((group['연도'] == 2026) & (group['월'] >= 3) & (group['월'] <= 6))].dropna(subset=feature_cols).copy()
    
    # 1~6부두에 대해 검증 세트의 조업 피처를 완전 미래형 조건인 Trend-Adjusted YoY로 대체 (7부두 제외)
    if budu != '7부두' and len(val_data_eval) > 0:
        val_data_eval = val_data_eval.astype({
            'VesselCount': 'float64',
            'AvgDwellTime': 'float64',
            'BerthOccupancy': 'float64',
            'ShiftRatio': 'float64',
            'TransshipmentRatio': 'float64'
        })
        
        # 1. 과거/검증용 조업 지표 딕셔너리 사전 구성
        ops_dict_eval = {}
        for _, r in group.iterrows():
            y_r, m_r = r['연도'], r['월']
            for c_col in ['VesselCount', 'AvgDwellTime', 'BerthOccupancy', 'ShiftRatio', 'TransshipmentRatio']:
                ops_dict_eval[(y_r, m_r, c_col)] = r[c_col]
                
        # 3월 ~ 6월 검증 세트 순차 연산
        for idx_eval, row_eval in val_data_eval.iterrows():
            m_eval = row_eval['월']
            
            # 최근 3개월의 시점 구하기 (예: 3월 예측 시 최근 3개월은 26년 2월, 1월, 25년 12월)
            recent_y_m_eval = []
            curr_yr = 2026
            for delta in range(1, 4):
                target_m = m_eval - delta
                if target_m >= 1:
                    recent_y_m_eval.append((curr_yr, target_m))
                else:
                    recent_y_m_eval.append((curr_yr - 1, 12 + target_m))
                    
            for col_eval in ['VesselCount', 'AvgDwellTime', 'BerthOccupancy', 'ShiftRatio', 'TransshipmentRatio']:
                # 1) 전년 동월값
                val_yoy_eval = ops_dict_eval.get((curr_yr - 1, m_eval, col_eval), np.nan)
                if np.isnan(val_yoy_eval):
                    if col_eval == 'BerthOccupancy':
                        val_yoy_eval = df_occ[(df_occ['부두'] == budu) & (df_occ['월'] == m_eval) & (df_occ['연도'] < 2026)]['BerthOccupancy'].mean()
                    elif col_eval == 'TransshipmentRatio':
                        val_yoy_eval = df_ratios[(df_ratios['월'] == m_eval) & (df_ratios['연도'] < 2026)]['TransshipmentRatio'].mean() if len(df_ratios) > 0 else np.nan
                        if np.isnan(val_yoy_eval):
                            val_yoy_eval = monthly_trans_avg.get(m_eval, 64.0)
                    else:
                        val_yoy_eval = terminal_monthly_ops_avg.get((budu, m_eval, col_eval), 10.0 if 'Count' in col_eval else (0.0 if 'Ratio' in col_eval else 24.0))
                
                # 2) 최근 3개월 추세 비율 계산
                recent_curr_list = []
                recent_yoy_list = []
                for ym_t in recent_y_m_eval:
                    val_curr_t = ops_dict_eval.get((ym_t[0], ym_t[1], col_eval), np.nan)
                    if np.isnan(val_curr_t):
                        if col_eval == 'BerthOccupancy':
                            val_curr_t = df_occ[(df_occ['부두'] == budu) & (df_occ['월'] == ym_t[1]) & (df_occ['연도'] < 2026)]['BerthOccupancy'].mean()
                        elif col_eval == 'TransshipmentRatio':
                            val_curr_t = df_ratios[(df_ratios['월'] == ym_t[1]) & (df_ratios['연도'] < 2026)]['TransshipmentRatio'].mean() if len(df_ratios) > 0 else np.nan
                            if np.isnan(val_curr_t):
                                val_curr_t = monthly_trans_avg.get(ym_t[1], 64.0)
                        else:
                            val_curr_t = terminal_monthly_ops_avg.get((budu, ym_t[1], col_eval), 10.0 if 'Count' in col_eval else (0.0 if 'Ratio' in col_eval else 24.0))
                    recent_curr_list.append(val_curr_t)
                    
                    val_yoy_t = ops_dict_eval.get((ym_t[0] - 1, ym_t[1], col_eval), np.nan)
                    if np.isnan(val_yoy_t):
                        if col_eval == 'BerthOccupancy':
                            val_yoy_t = df_occ[(df_occ['부두'] == budu) & (df_occ['월'] == ym_t[1]) & (df_occ['연도'] < 2026)]['BerthOccupancy'].mean()
                        elif col_eval == 'TransshipmentRatio':
                            val_yoy_t = df_ratios[(df_ratios['월'] == ym_t[1]) & (df_ratios['연도'] < 2026)]['TransshipmentRatio'].mean() if len(df_ratios) > 0 else np.nan
                            if np.isnan(val_yoy_t):
                                val_yoy_t = monthly_trans_avg.get(ym_t[1], 64.0)
                        else:
                            val_yoy_t = terminal_monthly_ops_avg.get((budu, ym_t[1], col_eval), 10.0 if 'Count' in col_eval else (0.0 if 'Ratio' in col_eval else 24.0))
                    recent_yoy_list.append(val_yoy_t)
                    
                mean_curr_ops = sum(recent_curr_list) / 3.0
                mean_yoy_ops = sum(recent_yoy_list) / 3.0
                tr_ops = 1.0
                if mean_yoy_ops > 0:
                    tr_ops = mean_curr_ops / mean_yoy_ops
                    tr_ops = max(0.5, min(2.0, tr_ops))
                    
                val_adj_eval = val_yoy_eval * tr_ops
                val_data_eval.loc[idx_eval, col_eval] = val_adj_eval
                
                # 다음 스텝(예: 4월 예측)을 위해 ops_dict_eval에 대입 결과 저장
                ops_dict_eval[(curr_yr, m_eval, col_eval)] = val_adj_eval
    
    if len(train_data_eval) > 0 and len(val_data_eval) > 0:
        eval_xgb = xgb.XGBRegressor(n_estimators=30, max_depth=3, learning_rate=0.08, reg_alpha=1.0, reg_lambda=1.0, random_state=42)
        eval_ridge = Ridge(alpha=50.0)
        
        eval_xgb.fit(train_data_eval[feature_cols], train_data_eval['총물동량'])
        eval_ridge.fit(train_data_eval[feature_cols], train_data_eval['총물동량'])
        
        val_preds_xgb = eval_xgb.predict(val_data_eval[feature_cols])
        val_preds_ridge = eval_ridge.predict(val_data_eval[feature_cols])
        val_preds_ens = 0.5 * val_preds_xgb + 0.5 * val_preds_ridge
        
        ss_res = np.sum((val_data_eval['총물동량'] - val_preds_ens) ** 2)
        ss_tot = np.sum((val_data_eval['총물동량'] - np.mean(val_data_eval['총물동량'])) ** 2)
        val_r2 = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else np.nan
        val_mae = np.mean(np.abs(val_data_eval['총물동량'] - val_preds_ens))
        
        print(f"  - {budu} 앙상블 검증 (학습: {len(train_data_eval)}행) | 훈련(XGB R²): {eval_xgb.score(train_data_eval[feature_cols], train_data_eval['총물동량']):.4f} | 검증 R²: {val_r2:.4f} | 검증 MAE: {val_mae:,.0f} TEU")
    else:
        print(f"  - {budu} 데이터 부족으로 검증 생략")
    
    # 최종 미래 예측용 모델 학습 (전체 2026년 6월까지 데이터로 재학습 / Refit)
    full_train_data = group.dropna(subset=feature_cols)
    X_train, y_train = full_train_data[feature_cols], full_train_data['총물동량']
    actuals_sum_2026_h1 = group[group['연도'] == 2026]['총물동량'].sum()
    
    model_xgb = xgb.XGBRegressor(n_estimators=30, max_depth=3, learning_rate=0.08, reg_alpha=1.0, reg_lambda=1.0, random_state=42)
    model_ridge = Ridge(alpha=50.0)
    
    model_xgb.fit(X_train, y_train)
    model_ridge.fit(X_train, y_train)
    print(f"    -> [Refit 완료] {budu} 최종 앙상블 모델 학습 완료 (XGB R²: {model_xgb.score(X_train, y_train):.4f}, Ridge R²: {model_ridge.score(X_train, y_train):.4f})")
    
    # [Shapley Value 분석 수행]
    try:
        import shap
        import matplotlib.pyplot as plt
        
        # XGBoost 모델용 TreeExplainer 생성 및 SHAP 값 산출
        explainer = shap.TreeExplainer(model_xgb)
        shap_values = explainer.shap_values(X_train)
        
        # 각 독립변수의 평균 절대 SHAP 중요도 연산
        mean_shap = np.abs(shap_values).mean(axis=0)
        shap_df = pd.DataFrame({
            'Feature': feature_cols,
            'SHAP_Value': mean_shap
        }).sort_values(by='SHAP_Value', ascending=False)
        
        print(f"    -> [{budu} Shapley Feature Importance (Target: 총물동량)]")
        for _, row in shap_df.iterrows():
            print(f"       * {row['Feature']}: {row['SHAP_Value']:.2f}")
            
        # SHAP Summary Plot (Dot/Beeswarm Plot) 시각화 파일 저장
        plt.figure(figsize=(10, 6))
        shap.summary_plot(shap_values, X_train, feature_names=feature_cols, plot_type="dot", show=False)
        plt.title(f"{budu} Shapley Feature Importance Summary (Dot Plot)", fontsize=12, pad=15)
        plt.tight_layout()
        shap_img_name = f"{budu}_shapley_importance.png"
        plt.savefig(os.path.join(BASE_DIR, shap_img_name), dpi=150)
        plt.close()
        print(f"    -> [SHAP 시각화 저장 완료] {shap_img_name}")
    except ImportError:
        print("    -> [안내] shap 라이브러리가 설치되지 않아 Shapley 분석을 건너뜁니다. (pip install shap 실행 필요)")
    except Exception as shap_err:
        print(f"    -> [오류] {budu} Shapley 분석 중 에러 발생: {shap_err}")
        
    scen_forecasts = {}
    for scen_name in ['Flat', 'Rise', 'Fall']:
        last_known_row = group[group['연도'] == 2026].iloc[-1]
        last_time_idx = last_known_row['TimeIndex']
        
        # 미래 장기 예측 루프 구동용 딕셔너리 생성
        vol_dict_proj = {(r['연도'], r['월']): r['총물동량'] for _, r in group.iterrows()}
        scfi_raw_hist = group['SCFI_raw'].tolist()
        
        # 조업 지표 역추적 및 보존용 딕셔너리 사전 빌드
        ops_dict_proj = {}
        for _, r in group.iterrows():
            y_r, m_r = r['연도'], r['월']
            for c_col in ['VesselCount', 'AvgDwellTime', 'BerthOccupancy', 'ShiftRatio', 'TransshipmentRatio']:
                ops_dict_proj[(y_r, m_r, c_col)] = r[c_col]
        
        pred_vols_2026_h2, pred_vols_2027 = [], []
        mean_val = group['총물동량'].mean()
        
        for m_idx in range(1, 19):
            t_idx = last_time_idx + m_idx
            yr, m = (2026, 6 + m_idx) if m_idx <= 6 else (2027, m_idx - 6)
            
            # SCFI MoM 증감률 계산
            scfi_raw_val = scenarios[scen_name](m_idx)
            prev_scfi_raw_val = scfi_raw_hist[-1]
            scfi_mom_val = (scfi_raw_val - prev_scfi_raw_val) / prev_scfi_raw_val * 100
            
            # Lag12: 12개월 전 실적/예측값
            yoy_yr = yr - 1
            lag12 = vol_dict_proj.get((yoy_yr, m), np.nan)
            if np.isnan(lag12):
                lag12 = mean_val * seasonality_weights[m]
                
            # Trend Ratio 계산
            recent_y_m = []
            for i in range(1, 4):
                target_m_idx = m_idx - i
                if target_m_idx >= 1:
                    y_t, m_t = (2026, 6 + target_m_idx) if target_m_idx <= 6 else (2027, target_m_idx - 6)
                else:
                    real_row = group.iloc[len(group) - 1 + target_m_idx]
                    y_t, m_t = real_row['연도'], real_row['월']
                recent_y_m.append((y_t, m_t))
                
            recent_curr = [vol_dict_proj.get(ym) for ym in recent_y_m]
            recent_yoy = []
            for ym in recent_y_m:
                y_y = ym[0] - 1
                m_y = ym[1]
                val_y = vol_dict_proj.get((y_y, m_y), np.nan)
                if np.isnan(val_y):
                    val_y = mean_val * seasonality_weights[m_y]
                recent_yoy.append(val_y)
                
            mean_curr = sum(recent_curr) / 3.0
            mean_yoy = sum(recent_yoy) / 3.0
            trend_ratio = 1.0
            if mean_yoy > 0:
                trend_ratio = mean_curr / mean_yoy
                trend_ratio = max(0.5, min(2.0, trend_ratio))
                
            lag12_adj = lag12 * trend_ratio
            
            # RollingAvg3_YoY
            m_plus = m + 1 if m < 12 else 1
            yr_plus = yoy_yr if m < 12 else yr
            m_minus = m - 1 if m > 1 else 12
            yr_minus = yoy_yr if m > 1 else yoy_yr - 1
            
            v_11 = vol_dict_proj.get((yr_plus, m_plus), np.nan)
            v_12 = lag12
            v_13 = vol_dict_proj.get((yr_minus, m_minus), np.nan)
            
            if np.isnan(v_11): v_11 = mean_val * seasonality_weights[m_plus]
            if np.isnan(v_13): v_13 = mean_val * seasonality_weights[m_minus]
            rolling_yoy = (v_11 + v_12 + v_13) / 3.0
            
            # 5대 조업 변수에 대해 Trend-Adjusted YoY 동적 연산 적용 (7부두 제외)
            pred_ops_vals = {}
            for c_col in ['VesselCount', 'AvgDwellTime', 'BerthOccupancy', 'ShiftRatio', 'TransshipmentRatio']:
                if budu == '7부두':
                    if c_col == 'BerthOccupancy':
                        pred_ops_vals[c_col] = terminal_monthly_occ_avg.get((budu, m), 50.0)
                    else:
                        pred_ops_vals[c_col] = terminal_monthly_ops_avg.get((budu, m, c_col), 10.0 if 'Count' in c_col else (0.0 if 'Ratio' in c_col else 24.0))
                else:
                    # 1) 전년 동월값
                    val_yoy_val = ops_dict_proj.get((yoy_yr, m, c_col), np.nan)
                    if np.isnan(val_yoy_val):
                        if c_col == 'BerthOccupancy':
                            val_yoy_val = df_occ[(df_occ['부두'] == budu) & (df_occ['월'] == m) & (df_occ['연도'] < 2026)]['BerthOccupancy'].mean()
                        elif c_col == 'TransshipmentRatio':
                            val_yoy_val = df_ratios[(df_ratios['월'] == m) & (df_ratios['연도'] < 2026)]['TransshipmentRatio'].mean() if len(df_ratios) > 0 else np.nan
                            if np.isnan(val_yoy_val):
                                val_yoy_val = monthly_trans_avg.get(m, 64.0)
                        else:
                            val_yoy_val = terminal_monthly_ops_avg.get((budu, m, c_col), 10.0 if 'Count' in c_col else (0.0 if 'Ratio' in c_col else 24.0))
                    
                    # 2) 최근 3개월 추세 비율 계산
                    recent_curr_list = []
                    recent_yoy_list = []
                    for ym_t in recent_y_m:
                        val_curr_t = ops_dict_proj.get((ym_t[0], ym_t[1], c_col), np.nan)
                        if np.isnan(val_curr_t):
                            if c_col == 'BerthOccupancy':
                                val_curr_t = df_occ[(df_occ['부두'] == budu) & (df_occ['월'] == ym_t[1]) & (df_occ['연도'] < 2026)]['BerthOccupancy'].mean()
                            elif c_col == 'TransshipmentRatio':
                                val_curr_t = df_ratios[(df_ratios['월'] == ym_t[1]) & (df_ratios['연도'] < 2026)]['TransshipmentRatio'].mean() if len(df_ratios) > 0 else np.nan
                                if np.isnan(val_curr_t):
                                    val_curr_t = monthly_trans_avg.get(ym_t[1], 64.0)
                            else:
                                val_curr_t = terminal_monthly_ops_avg.get((budu, ym_t[1], c_col), 10.0 if 'Count' in c_col else (0.0 if 'Ratio' in c_col else 24.0))
                        recent_curr_list.append(val_curr_t)
                        
                        val_yoy_t = ops_dict_proj.get((ym_t[0] - 1, ym_t[1], c_col), np.nan)
                        if np.isnan(val_yoy_t):
                            if c_col == 'BerthOccupancy':
                                val_yoy_t = df_occ[(df_occ['부두'] == budu) & (df_occ['월'] == ym_t[1]) & (df_occ['연도'] < 2026)]['BerthOccupancy'].mean()
                            elif c_col == 'TransshipmentRatio':
                                val_yoy_t = df_ratios[(df_ratios['월'] == ym_t[1]) & (df_ratios['연도'] < 2026)]['TransshipmentRatio'].mean() if len(df_ratios) > 0 else np.nan
                                if np.isnan(val_yoy_t):
                                    val_yoy_t = monthly_trans_avg.get(ym_t[1], 64.0)
                            else:
                                val_yoy_t = terminal_monthly_ops_avg.get((budu, ym_t[1], c_col), 10.0 if 'Count' in c_col else (0.0 if 'Ratio' in c_col else 24.0))
                        recent_yoy_list.append(val_yoy_t)
                        
                    mean_curr_ops = sum(recent_curr_list) / 3.0
                    mean_yoy_ops = sum(recent_yoy_list) / 3.0
                    tr_ops = 1.0
                    if mean_yoy_ops > 0:
                        tr_ops = mean_curr_ops / mean_yoy_ops
                        tr_ops = max(0.5, min(2.0, tr_ops))
                        
                    val_adj_val = val_yoy_val * tr_ops
                    pred_ops_vals[c_col] = val_adj_val
                
                # 미래 예측 과정 데이터 업데이트를 위해 딕셔너리에 대입 결과 저장
                ops_dict_proj[(yr, m, c_col)] = pred_ops_vals[c_col]
            
            pred_input = pd.DataFrame([{
                'TimeIndex': t_idx,
                'Weight': seasonality_weights[m],
                'TransshipmentRatio': pred_ops_vals['TransshipmentRatio'],
                'SCFI_MoM': scfi_mom_val,
                'BerthOccupancy': pred_ops_vals['BerthOccupancy'],
                'Lag12_Adj': lag12_adj,
                'RollingAvg3_YoY': rolling_yoy,
                'VesselCount': pred_ops_vals['VesselCount'],
                'AvgDwellTime': pred_ops_vals['AvgDwellTime'],
                'ShiftRatio': pred_ops_vals['ShiftRatio'],
                'Month_Sin': np.sin(2 * np.pi * m / 12.0),
                'Month_Cos': np.cos(2 * np.pi * m / 12.0)
            }])
            
            pred_vol_xgb = model_xgb.predict(pred_input[feature_cols])[0]
            pred_vol_ridge = model_ridge.predict(pred_input[feature_cols])[0]
            pred_vol = max(0, int(0.5 * pred_vol_xgb + 0.5 * pred_vol_ridge))
            
            vol_dict_proj[(yr, m)] = pred_vol
            scfi_raw_hist.append(scfi_raw_val)
            
            if yr == 2026:
                pred_vols_2026_h2.append(pred_vol)
            else:
                pred_vols_2027.append(pred_vol)
                
        forecast_results.append({
            '부두': budu, '시나리오': scen_name,
            '2026실적(H1)': int(actuals_sum_2026_h1),
            '2026하반기예측': sum(pred_vols_2026_h2),
            '2026연간합계': int(actuals_sum_2026_h1 + sum(pred_vols_2026_h2)),
            '2027연간합계': sum(pred_vols_2027)
        })
        scen_forecasts[scen_name] = pred_vols_2026_h2 + pred_vols_2027
        
    plot_data[budu] = (group.copy(), scen_forecasts)

df_forecast = pd.DataFrame(forecast_results)

# 6. 예측 결과 터미널 화면 요약 출력
print("\n" + "="*80)
print("=== [부산신항 부두별 SCFI 시나리오 물동량 예측 결과 종합 - Trend-Adjusted YoY 모델] ===")
print("="*80)
for scen in ['Flat', 'Rise', 'Fall']:
    print(f"\n[시나리오: SCFI {scen}]")
    df_scen = df_forecast[df_forecast['시나리오'] == scen]
    df_scen_formatted = df_scen[['부두', '2026실적(H1)', '2026하반기예측', '2026연간합계', '2027연간합계']].copy()
    for col in ['2026실적(H1)', '2026하반기예측', '2026연간합계', '2027연간합계']:
        df_scen_formatted[col] = df_scen_formatted[col].apply(lambda x: f"{x:,.0f} TEU")
    print(df_scen_formatted.to_string(index=False))
print("\n" + "="*80)
print("예측 및 터미널 출력이 완료되었습니다!")
print("="*80)

# 7. 각 부두별 물동량 예측 추이 그래프 시각화 및 저장 (총 7장)
try:
    import matplotlib.pyplot as plt
    plt.rcParams['font.family'] = 'Malgun Gothic'
    plt.rcParams['axes.unicode_minus'] = False
    
    print("\n5. 각 부두별 물동량 예측 추이 그래프(총 7장) 저장 중...")
    scen_colors = {'Flat': 'orange', 'Rise': 'crimson', 'Fall': 'forestgreen'}
    
    for budu, (actuals, forecasts) in plot_data.items():
        actuals = actuals.sort_values(['연도', '월']).reset_index(drop=True)
        actuals['Date'] = actuals['연도'].astype(str) + '-' + actuals['월'].astype(str).str.zfill(2)
        
        pred_dates = []
        for m_idx in range(1, 19):
            yr, m = (2026, 6 + m_idx) if m_idx <= 6 else (2027, m_idx - 6)
            pred_dates.append(f"{yr}-{str(m).zfill(2)}")
            
        plt.figure(figsize=(10, 5.5))
        plt.plot(actuals['Date'], actuals['총물동량'], marker='o', label='실적 (Actual)', color='royalblue', linewidth=2)
        
        last_actual = actuals.iloc[-1]
        
        for scen_name, pred_vols in forecasts.items():
            plt.plot([last_actual['Date'], pred_dates[0]], [last_actual['총물동량'], pred_vols[0]], color=scen_colors[scen_name], linestyle='--', alpha=0.5)
            plt.plot(pred_dates, pred_vols, marker='x', label=f"예측 ({scen_name})", color=scen_colors[scen_name], linestyle='--')
            
        plt.title(f"{budu} 물동량 시나리오 예측 추이 (2022 - 2027)", fontsize=13, weight='bold', pad=15)
        plt.xlabel("연월", labelpad=10)
        plt.ylabel("물동량 (TEU)", labelpad=10)
        plt.gca().get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, loc: f"{int(x):,}"))
        plt.xticks(rotation=45)
        plt.grid(True, linestyle='--', alpha=0.5)
        plt.legend()
        plt.tight_layout()
        
        img_name = f"{budu}_물동량_예측_추이.png"
        plt.savefig(os.path.join(BASE_DIR, img_name), dpi=150)
        plt.close()
        print(f"  - 저장 완료: {img_name}")
except Exception as e:
    print(f"\n[오류] 시각화 이미지 생성 실패: {e}")


# === 예측 결과 PostgreSQL(gold) 적재 (Power BI 연결용) ===  [로컬 추가]
print("\n6. 예측 결과 PostgreSQL(gold) 적재 중...")
df_forecast.to_sql("cargo_forecast", engine, schema="gold", if_exists="replace", index=False)

_rows = []
for budu, (actuals, forecasts) in plot_data.items():
    for scen, vols in forecasts.items():
        for i, vol in enumerate(vols, start=1):
            yr, m = (2026, 6 + i) if i <= 6 else (2027, i - 6)
            _rows.append({"부두": budu, "시나리오": scen, "연도": yr, "월": m,
                          "month_date": f"{yr}-{m:02d}-01", "예측물동량": int(vol)})
pd.DataFrame(_rows).to_sql("cargo_forecast_monthly", engine, schema="gold",
                           if_exists="replace", index=False)
print("   -> gold.cargo_forecast / gold.cargo_forecast_monthly 적재 완료")