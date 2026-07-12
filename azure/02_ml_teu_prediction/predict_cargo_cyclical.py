import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.linear_model import Ridge
import calendar
import glob
import os
import sys

# 터미널 한글 깨짐 방지
try:
    sys.stdout.reconfigure(encoding='utf-8')
except AttributeError:
    pass

BASE_DIR = "."

# [역사적 가중치 & 환적 비중 상수 정의]
seasonality_weights = {
    1: 1.0087, 2: 0.9362, 3: 1.0504, 4: 1.0302, 5: 1.0615, 6: 0.9980,
    7: 1.0317, 8: 0.9875, 9: 0.9393, 10: 0.9823, 11: 0.9871, 12: 0.9870
}
monthly_trans_avg = {
    1: 64.2, 2: 64.1, 3: 64.3, 4: 64.4, 5: 64.5, 6: 64.3,
    7: 64.2, 8: 64.0, 9: 64.1, 10: 64.2, 11: 64.3, 12: 64.4
}

# 1. 7개 부두 데이터 로드 및 전처리
print("1. 부두별 데이터 로드 및 전처리 중...")
col_rename = {
    '접안(예정)일시': '접안예정일시', '접안예정시간': '접안예정일시', '접안일시': '접안예정일시', '입항일시': '접안예정일시', 'atb': '접안예정일시', 'arrival': '접안예정일시',
    '접안예정시간(etb)': '접안예정일시', '출항예정시간(etd)': '출항예정일시',
    '출항(예정)일시': '출항예정일시', '출항예정시간': '출항예정일시', '출항일시': '출항예정일시', 'atd': '출항예정일시', 'departure': '출항예정일시',
    '선석': '선석', 'berth': '선석', '선명': '모선명', '모선명': '모선명', '선박명': '모선명', 'vessel': '모선명',
    '양하': '양하수량', 'discharge': '양하수량', 'import': '양하수량', '양하수량': '양하수량',
    '적하': '적하수량', '선적': '적하수량', 'load': '적하수량', 'export': '적하수량', '선적수량': '적하수량', '적하수량': '적하수량',
    '상태': '상태', 'status': '상태',
    '선사': '선사', '운항선사': '선사', '선사코드': '선사',
    'route': 'ROUTE', '항로': 'ROUTE', 'route명': 'ROUTE',
    'shift': '이적수량', 's/h': '이적수량', '이적': '이적수량'
}

def load_base_data():
    all_rows = []
    for i in range(1, 8):
        for file in glob.glob(os.path.join(BASE_DIR, f"{i}부두", "*.xls*")):
            try:
                try:
                    df = pd.read_html(file, encoding='utf-8')[0]
                except:
                    df = pd.read_excel(file)
            except:
                continue
                
            # 헤더 보정
            keywords = ['선석', '입항', '출항', '양하', '적하', '선적', '접안', '모선명', '선명']
            has_keywords = any(any(k in str(col).lower() for k in keywords) for col in df.columns)
            if (not has_keywords or 'unnamed' in str(df.columns[0]).lower()) and len(df) > 0:
                df.columns = df.iloc[0]
                df = df.iloc[1:].reset_index(drop=True)
                
            # 작업량 통합 컬럼 파싱 (5부두, 7부두 대응)
            workload_col = None
            for col in df.columns:
                col_str = str(col).lower().replace(" ", "").replace("\n", "")
                if '작업량' in col_str:
                    workload_col = col
                    break
            if workload_col is not None:
                def parse_workload(val):
                    if pd.isna(val): return 0.0, 0.0, 0.0
                    parts = str(val).split('/')
                    if len(parts) < 2:
                        parts = str(val).split()
                        if len(parts) < 2: return 0.0, 0.0, 0.0
                    to_num = lambda s: float(''.join(c for c in s if c.isdigit())) if ''.join(c for c in s if c.isdigit()) else 0.0
                    
                    shift_val = 0.0
                    if len(parts) >= 3:
                        shift_val = to_num(parts[2])
                    return to_num(parts[0]), to_num(parts[1]), shift_val
                workload_vals = df[workload_col].apply(parse_workload)
                df['양하수량'] = [x[0] for x in workload_vals]
                df['적하수량'] = [x[1] for x in workload_vals]
                df['이적수량'] = [x[2] for x in workload_vals]

            # 컬럼 표준화
            df = df.rename(columns={col: col_rename[str(col).lower().replace(" ", "").replace("\n", "")] for col in df.columns if str(col).lower().replace(" ", "").replace("\n", "") in col_rename})
            df['부두'] = f"{i}부두"
            
            # 시간 형식 변환 및 이상치 필터
            df['접안예정일시_dt'] = pd.to_datetime(df['접안예정일시'], errors='coerce')
            df['출항예정일시_dt'] = pd.to_datetime(df['출항예정일시'], errors='coerce')
            df = df.dropna(subset=['접안예정일시_dt', '출항예정일시_dt'])
            df['체류시간_시간'] = (df['출항예정일시_dt'] - df['접안예정일시_dt']).dt.total_seconds() / 3600.0
            
            df = df[df['체류시간_시간'] < 240.0]
            if '상태' in df.columns:
                df = df[~df['상태'].astype(str).str.upper().isin(['CANCELLED', 'CANCELED'])]
            if '모선명' in df.columns:
                df = df[~df['모선명'].astype(str).str.upper().str.contains('STORAGE|DUMMY|공컨|테스트|TEST', na=False)]
                
            for c in ['양하수량', '적하수량']:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
                else:
                    df[c] = 0.0
                    
            if '이적수량' in df.columns:
                df['이적수량'] = pd.to_numeric(df['이적수량'], errors='coerce').fillna(0)
            else:
                df['이적수량'] = 0.0
                
            df['총물동량'] = df['양하수량'] + df['적하수량']
            df['연도'] = df['출항예정일시_dt'].dt.year
            df['월'] = df['출항예정일시_dt'].dt.month
            
            df = df[((df['연도'] >= 2022) & (df['연도'] <= 2025)) | ((df['연도'] == 2026) & (df['월'] <= 6))]
            all_rows.append(df)
            
    df_all = pd.concat(all_rows, ignore_index=True)
    df_filtered = df_all[~((df_all['부두'] == '6부두') & (df_all['연도'] == 2022))].copy()
    df_filtered = df_filtered[~((df_filtered['부두'] == '7부두') & (df_filtered['출항예정일시_dt'] < '2025-07-01'))].copy()
    return df_filtered

df_base = load_base_data()
df_base['Week'] = df_base['출항예정일시_dt'].dt.isocalendar().week
df_base['Biweek'] = ((df_base['Week'] - 1) // 2 + 1).clip(1, 26).astype(int)

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
scfi_dfs = [pd.read_excel(f).iloc[:, :3] for f in glob.glob(os.path.join(BASE_DIR, "SCFI지수", "*.xls"))]
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

# 연도별 환적비 실젯값 수집 및 매핑
trans_records = []
trans_dir = os.path.join(BASE_DIR, "월별 환적비")
for yr in [2022, 2023, 2024, 2025, 2026]:
    filename = f"월별 컨테이너 처리실적(확정)_{yr}.xlsx"
    path = os.path.join(trans_dir, filename)
    if not os.path.exists(path):
        continue
    try:
        df_trans = pd.read_excel(path, header=None)
        data_rows = df_trans.iloc[2:]
        for idx, row in data_rows.iterrows():
            m_val = str(row.iloc[0]).strip().zfill(2)
            if m_val in ['nan', '합계']:
                continue
            try:
                m = int(m_val)
            except ValueError:
                continue
            imp_exp_total = float(row.iloc[3]) + float(row.iloc[5]) + float(row.iloc[4]) + float(row.iloc[6])
            trans_total = float(row.iloc[7]) + float(row.iloc[9]) + float(row.iloc[8]) + float(row.iloc[10])
            total_cargo = imp_exp_total + trans_total
            ratio = (trans_total / total_cargo * 100) if total_cargo > 0 else 0.0
            trans_records.append({'연도': yr, '월': m, 'TransshipmentRatio': ratio})
    except:
        continue

df_ratios = pd.DataFrame(trans_records) if trans_records else pd.DataFrame(columns=['연도', '월', 'TransshipmentRatio'])

# 4. 피처 가공 및 학습 데이터 조립 (Trend-Adjusted YoY 기법 적용)
# 3부두만 2주 단위로 쪼개기 위한 데이터 분리
df_monthly_base = df_base[df_base['부두'] != '3부두'].copy()
df_biweekly_base = df_base[df_base['부두'] == '3부두'].copy()

# --- 1) 월간 터미널 (1, 2, 4, 5, 6, 7부두) 피처 가공 ---
df_monthly_ops = df_monthly_base.groupby(['부두', '연도', '월']).agg(
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

# 1부두 6월 데이터 보정
cond_1terminal_june = (df_monthly_ops['부두'] == '1부두') & (df_monthly_ops['연도'] == 2026) & (df_monthly_ops['월'] == 6)
df_monthly_ops.loc[cond_1terminal_june, '총물동량'] = df_monthly_ops.loc[cond_1terminal_june, '총물동량'] * 1.25
df_monthly_ops.loc[cond_1terminal_june, 'VesselCount'] = (df_monthly_ops.loc[cond_1terminal_june, 'VesselCount'] * 1.25).round()

df_occ_monthly = df_occ[df_occ['부두'] != '3부두'].copy()
monthly_agg = pd.merge(df_monthly_ops, df_occ_monthly, on=['부두', '연도', '월'], how='left')

# 선석 점유율(BerthOccupancy) 결측치 보완
df_occ_mean = df_occ_monthly[df_occ_monthly['연도'] < 2026].groupby(['부두', '월'])['BerthOccupancy'].mean().reset_index()
df_occ_mean = df_occ_mean.rename(columns={'BerthOccupancy': 'BerthOccupancy_mean'})
monthly_agg = pd.merge(monthly_agg, df_occ_mean, on=['부두', '월'], how='left')
monthly_agg['BerthOccupancy'] = monthly_agg['BerthOccupancy'].fillna(monthly_agg['BerthOccupancy_mean']).fillna(50.0)
monthly_agg = monthly_agg.drop(columns=['BerthOccupancy_mean'])

monthly_agg['Weight'] = monthly_agg['월'].map(seasonality_weights)
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
        yoy_yr = yr - 1
        lag12 = vol_dict.get((yoy_yr, m), np.nan)
        if np.isnan(lag12):
            lag12 = mean_val * seasonality_weights[m]
            
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
                trend_ratio = max(0.5, min(2.0, trend_ratio))
                
        lag12_adj = lag12 * trend_ratio
        
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
df_all_features_monthly = pd.concat(advanced_dfs, ignore_index=True)


# --- 2) 3부두 (2주 단위) 피처 가공 ---
# 3부두 2주 단위 선석 점유율 계산 (기간: 14일)
df_occ_3_bw_records = []
for (budu, yr, bw), group in df_biweekly_base.groupby(['부두', '연도', 'Biweek']):
    unique_berths = group['선석'].unique()
    num_berths = len(unique_berths) if len(unique_berths) > 0 else 1
    total_avail = num_berths * 14 * 24.0
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
    df_occ_3_bw_records.append({'부두': budu, '연도': yr, 'Biweek': bw, 'BerthOccupancy': (total_merged / total_avail * 100) if total_avail > 0 else 0.0})
df_occ_3_bw = pd.DataFrame(df_occ_3_bw_records)

df_3_bw_ops = df_biweekly_base.groupby(['부두', '연도', 'Biweek']).agg(
    총물동량=('총물동량', 'sum'),
    이적수량=('이적수량', 'sum'),
    VesselCount=('모선명', 'count'),
    AvgDwellTime=('체류시간_시간', 'mean'),
    CarrierCount=('선사', 'nunique') if '선사' in df_base.columns else ('모선명', 'count'),
    RouteCount=('ROUTE', 'nunique') if 'ROUTE' in df_base.columns else ('모선명', 'count'),
    MedianMonth=('월', lambda x: int(x.median()))
).reset_index()

df_3_bw_ops['ShiftRatio'] = (df_3_bw_ops['이적수량'] / df_3_bw_ops['총물동량'] * 100).fillna(0.0)
df_3_bw_ops['ShiftRatio'] = df_3_bw_ops['ShiftRatio'].replace([np.inf, -np.inf], 0.0).fillna(0.0)

for col in ['CarrierCount', 'RouteCount']:
    df_3_bw_ops[col] = df_3_bw_ops[col].replace(0, 1)

df_3_bw_features = pd.merge(df_3_bw_ops, df_occ_3_bw, on=['부두', '연도', 'Biweek'], how='left')

# 3부두 점유율 결측치 보완
df_occ_3_bw_mean = df_occ_3_bw[df_occ_3_bw['연도'] < 2026].groupby(['Biweek'])['BerthOccupancy'].mean().reset_index()
df_occ_3_bw_mean = df_occ_3_bw_mean.rename(columns={'BerthOccupancy': 'BerthOccupancy_mean'})
df_3_bw_features = pd.merge(df_3_bw_features, df_occ_3_bw_mean, on=['Biweek'], how='left')
df_3_bw_features['BerthOccupancy'] = df_3_bw_features['BerthOccupancy'].fillna(df_3_bw_features['BerthOccupancy_mean']).fillna(50.0)
df_3_bw_features = df_3_bw_features.drop(columns=['BerthOccupancy_mean'])

# SCFI 2주 평균 매핑
df_scfi_3 = df_scfi.copy()
df_scfi_3['Week'] = df_scfi_3['등록일'].dt.isocalendar().week
df_scfi_3['Biweek'] = ((df_scfi_3['Week'] - 1) // 2 + 1).clip(1, 26).astype(int)
df_scfi_3_bw = df_scfi_3.groupby(['연도', 'Biweek'])['SCFI_raw'].mean().reset_index()
df_scfi_3_bw['SCFI_MoM'] = ((df_scfi_3_bw['SCFI_raw'] - df_scfi_3_bw['SCFI_raw'].shift(1)) / df_scfi_3_bw['SCFI_raw'].shift(1) * 100).fillna(0.0)
df_3_bw_features = pd.merge(df_3_bw_features, df_scfi_3_bw[['연도', 'Biweek', 'SCFI_raw', 'SCFI_MoM']], on=['연도', 'Biweek'], how='left').ffill().bfill()

# 환적비 및 가중치 매핑
df_3_bw_features = pd.merge(df_3_bw_features, df_ratios.rename(columns={'월': 'MedianMonth'}), on=['연도', 'MedianMonth'], how='left')
df_3_bw_features['TransshipmentRatio'] = df_3_bw_features['TransshipmentRatio'].fillna(df_3_bw_features['MedianMonth'].map(monthly_trans_avg)).fillna(64.0)
df_3_bw_features['Weight'] = df_3_bw_features['MedianMonth'].map(seasonality_weights)

# 2주 단위 cyclical features (Period = 26)
df_3_bw_features['Month_Sin'] = np.sin(2 * np.pi * df_3_bw_features['Biweek'] / 26.0)
df_3_bw_features['Month_Cos'] = np.cos(2 * np.pi * df_3_bw_features['Biweek'] / 26.0)

# 3부두 YoY Lag 및 Rolling 피처 계산 함수 (W=6)
def compute_trend_adj_features_3_bw(group):
    group = group.sort_values(['연도', 'Biweek']).reset_index(drop=True)
    group['TimeIndex'] = group.index + 1
    lag26_adj_vals = []
    rolling_yoy_vals = []
    vol_dict = {(r['연도'], r['Biweek']): r['총물동량'] for _, r in group.iterrows()}
    mean_val = group['총물동량'].mean()
    W = 6
    for idx, row in group.iterrows():
        yr, bw = row['연도'], row['Biweek']
        yoy_yr = yr - 1
        lag26 = vol_dict.get((yoy_yr, bw), np.nan)
        if np.isnan(lag26):
            lag26 = mean_val * seasonality_weights[row['MedianMonth']]
            
        trend_ratio = 1.0
        if idx >= W:
            recent_curr = [group.iloc[idx - i]['총물동량'] for i in range(1, W + 1)]
            recent_yoy = []
            for i in range(1, W + 1):
                curr_row = group.iloc[idx - i]
                y_y = curr_row['연도'] - 1
                bw_y = curr_row['Biweek']
                val_y = vol_dict.get((y_y, bw_y), np.nan)
                if np.isnan(val_y):
                    val_y = mean_val * seasonality_weights[curr_row['MedianMonth']]
                recent_yoy.append(val_y)
            mean_curr = sum(recent_curr) / float(W)
            mean_yoy = sum(recent_yoy) / float(W)
            if mean_yoy > 0:
                trend_ratio = mean_curr / mean_yoy
                trend_ratio = max(0.5, min(2.0, trend_ratio))
        lag26_adj = lag26 * trend_ratio
        
        bw_plus = bw + 1 if bw < 26 else 1
        yr_plus = yoy_yr if bw < 26 else yr
        bw_minus = bw - 1 if bw > 1 else 26
        yr_minus = yoy_yr if bw > 1 else yoy_yr - 1
        v_25 = vol_dict.get((yr_plus, bw_plus), np.nan)
        v_26 = lag26
        v_27 = vol_dict.get((yr_minus, bw_minus), np.nan)
        if np.isnan(v_25): v_25 = mean_val * seasonality_weights[row['MedianMonth']]
        if np.isnan(v_27): v_27 = mean_val * seasonality_weights[row['MedianMonth']]
        rolling_yoy = (v_25 + v_26 + v_27) / 3.0
        
        lag26_adj_vals.append(lag26_adj)
        rolling_yoy_vals.append(rolling_yoy)
    group['Lag12_Adj'] = lag26_adj_vals
    group['RollingAvg3_YoY'] = rolling_yoy_vals
    return group

df_all_features_3 = compute_trend_adj_features_3_bw(df_3_bw_features)
df_all_features_3 = df_all_features_3.rename(columns={'Biweek': '월'})

# 컬럼 일체화 및 병합
cols_to_keep = list(df_all_features_monthly.columns) + ['MedianMonth']
df_all_features_monthly['MedianMonth'] = df_all_features_monthly['월']

df_all_features = pd.concat([
    df_all_features_monthly[cols_to_keep],
    df_all_features_3[cols_to_keep]
], ignore_index=True)

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
xai_records = []

# 과거 조업 지표의 월별 평균 구하기
terminal_monthly_ops_avg = {}
for (budu, m), group in df_all_features[df_all_features['연도'] < 2026].groupby(['부두', '월']):
    for col in ['VesselCount', 'AvgDwellTime', 'ShiftRatio', 'TransshipmentRatio']:
        terminal_monthly_ops_avg[(budu, m, col)] = group[col].mean()

for budu, group in df_all_features.groupby('부두'):
    group = group.sort_values(['연도', '월']).reset_index(drop=True)
    
    # Train / Validation 분리 (MedianMonth 활용으로 3부두 2주 단위 및 타부두 월간 단위 통일)
    train_data_eval = group[((group['연도'] < 2026) | ((group['연도'] == 2026) & (group['MedianMonth'] <= 3)))].dropna(subset=feature_cols).copy()
    val_data_eval = group[((group['연도'] == 2026) & (group['MedianMonth'] >= 4) & (group['MedianMonth'] <= 6))].dropna(subset=feature_cols).copy()
    
    W = 6 if budu == '3부두' else 3
    period_limit = 26 if budu == '3부두' else 12
    
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
                
        # 검증 세트 순차 연산 (3부두는 2주 단위, 타부두는 월간 단위)
        for idx_eval, row_eval in val_data_eval.iterrows():
            m_eval = row_eval['월']
            
            # 최근 W개 시점 구하기
            recent_y_m_eval = []
            curr_yr = 2026
            for delta in range(1, W + 1):
                target_m = m_eval - delta
                if target_m >= 1:
                    recent_y_m_eval.append((curr_yr, target_m))
                else:
                    recent_y_m_eval.append((curr_yr - 1, period_limit + target_m))
                    
            for col_eval in ['VesselCount', 'AvgDwellTime', 'BerthOccupancy', 'ShiftRatio', 'TransshipmentRatio']:
                # 1) 전년 동월/동주기값
                val_yoy_eval = ops_dict_eval.get((curr_yr - 1, m_eval, col_eval), np.nan)
                if np.isnan(val_yoy_eval):
                    if col_eval == 'BerthOccupancy':
                        if budu == '3부두':
                            val_yoy_eval = df_occ_3_bw[df_occ_3_bw['Biweek'] == m_eval]['BerthOccupancy'].mean()
                        else:
                            val_yoy_eval = df_occ[(df_occ['부두'] == budu) & (df_occ['월'] == m_eval) & (df_occ['연도'] < 2026)]['BerthOccupancy'].mean()
                    elif col_eval == 'TransshipmentRatio':
                        if budu == '3부두':
                            approx_m = (m_eval - 1) * 12 // 26 + 1
                            val_yoy_eval = df_ratios[(df_ratios['월'] == approx_m) & (df_ratios['연도'] < 2026)]['TransshipmentRatio'].mean() if len(df_ratios) > 0 else np.nan
                            if np.isnan(val_yoy_eval):
                                val_yoy_eval = monthly_trans_avg.get(approx_m, 64.0)
                        else:
                            val_yoy_eval = df_ratios[(df_ratios['월'] == m_eval) & (df_ratios['연도'] < 2026)]['TransshipmentRatio'].mean() if len(df_ratios) > 0 else np.nan
                            if np.isnan(val_yoy_eval):
                                val_yoy_eval = monthly_trans_avg.get(m_eval, 64.0)
                    else:
                        if budu == '3부두':
                            approx_m = (m_eval - 1) * 12 // 26 + 1
                            val_yoy_eval = terminal_monthly_ops_avg.get((budu, approx_m, col_eval), 10.0 if 'Count' in col_eval else (0.0 if 'Ratio' in col_eval else 24.0))
                        else:
                            val_yoy_eval = terminal_monthly_ops_avg.get((budu, m_eval, col_eval), 10.0 if 'Count' in col_eval else (0.0 if 'Ratio' in col_eval else 24.0))
                
                # 2) 최근 W-기간 추세 비율 계산
                recent_curr_list = []
                recent_yoy_list = []
                for ym_t in recent_y_m_eval:
                    val_curr_t = ops_dict_eval.get((ym_t[0], ym_t[1], col_eval), np.nan)
                    if np.isnan(val_curr_t):
                        if col_eval == 'BerthOccupancy':
                            if budu == '3부두':
                                val_curr_t = df_occ_3_bw[df_occ_3_bw['Biweek'] == ym_t[1]]['BerthOccupancy'].mean()
                            else:
                                val_curr_t = df_occ[(df_occ['부두'] == budu) & (df_occ['월'] == ym_t[1]) & (df_occ['연도'] < 2026)]['BerthOccupancy'].mean()
                        elif col_eval == 'TransshipmentRatio':
                            if budu == '3부두':
                                approx_m_t = (ym_t[1] - 1) * 12 // 26 + 1
                                val_curr_t = df_ratios[(df_ratios['월'] == approx_m_t) & (df_ratios['연도'] < 2026)]['TransshipmentRatio'].mean() if len(df_ratios) > 0 else np.nan
                                if np.isnan(val_curr_t):
                                    val_curr_t = monthly_trans_avg.get(approx_m_t, 64.0)
                            else:
                                val_curr_t = df_ratios[(df_ratios['월'] == ym_t[1]) & (df_ratios['연도'] < 2026)]['TransshipmentRatio'].mean() if len(df_ratios) > 0 else np.nan
                                if np.isnan(val_curr_t):
                                    val_curr_t = monthly_trans_avg.get(ym_t[1], 64.0)
                        else:
                            if budu == '3부두':
                                approx_m_t = (ym_t[1] - 1) * 12 // 26 + 1
                                val_curr_t = terminal_monthly_ops_avg.get((budu, approx_m_t, col_eval), 10.0 if 'Count' in col_eval else (0.0 if 'Ratio' in col_eval else 24.0))
                            else:
                                val_curr_t = terminal_monthly_ops_avg.get((budu, ym_t[1], col_eval), 10.0 if 'Count' in col_eval else (0.0 if 'Ratio' in col_eval else 24.0))
                    recent_curr_list.append(val_curr_t)
                    
                    val_yoy_t = ops_dict_eval.get((ym_t[0] - 1, ym_t[1], col_eval), np.nan)
                    if np.isnan(val_yoy_t):
                        if col_eval == 'BerthOccupancy':
                            if budu == '3부두':
                                val_yoy_t = df_occ_3_bw[df_occ_3_bw['Biweek'] == ym_t[1]]['BerthOccupancy'].mean()
                            else:
                                val_yoy_t = df_occ[(df_occ['부두'] == budu) & (df_occ['월'] == ym_t[1]) & (df_occ['연도'] < 2026)]['BerthOccupancy'].mean()
                        elif col_eval == 'TransshipmentRatio':
                            if budu == '3부두':
                                approx_m_t = (ym_t[1] - 1) * 12 // 26 + 1
                                val_yoy_t = df_ratios[(df_ratios['월'] == approx_m_t) & (df_ratios['연도'] < 2026)]['TransshipmentRatio'].mean() if len(df_ratios) > 0 else np.nan
                                if np.isnan(val_yoy_t):
                                    val_yoy_t = monthly_trans_avg.get(approx_m_t, 64.0)
                            else:
                                val_yoy_t = df_ratios[(df_ratios['월'] == ym_t[1]) & (df_ratios['연도'] < 2026)]['TransshipmentRatio'].mean() if len(df_ratios) > 0 else np.nan
                                if np.isnan(val_yoy_t):
                                    val_yoy_t = monthly_trans_avg.get(ym_t[1], 64.0)
                        else:
                            if budu == '3부두':
                                approx_m_t = (ym_t[1] - 1) * 12 // 26 + 1
                                val_yoy_t = terminal_monthly_ops_avg.get((budu, approx_m_t, col_eval), 10.0 if 'Count' in col_eval else (0.0 if 'Ratio' in col_eval else 24.0))
                            else:
                                val_yoy_t = terminal_monthly_ops_avg.get((budu, ym_t[1], col_eval), 10.0 if 'Count' in col_eval else (0.0 if 'Ratio' in col_eval else 24.0))
                    recent_yoy_list.append(val_yoy_t)
                    
                mean_curr_ops = sum(recent_curr_list) / float(W)
                mean_yoy_ops = sum(recent_yoy_list) / float(W)
                tr_ops = 1.0
                if mean_yoy_ops > 0:
                    tr_ops = mean_curr_ops / mean_yoy_ops
                    tr_ops = max(0.5, min(2.0, tr_ops))
                    
                val_adj_eval = val_yoy_eval * tr_ops
                val_data_eval.loc[idx_eval, col_eval] = val_adj_eval
                
                # 다음 스텝을 위해 ops_dict_eval에 대입 결과 저장
                ops_dict_eval[(curr_yr, m_eval, col_eval)] = val_adj_eval
    
    if len(train_data_eval) > 0 and len(val_data_eval) > 0:
        eval_xgb = xgb.XGBRegressor(n_estimators=30, max_depth=3, learning_rate=0.08, reg_alpha=1.0, reg_lambda=1.0, random_state=42)
        eval_ridge = Ridge(alpha=50.0)
        
        eval_xgb.fit(train_data_eval[feature_cols], train_data_eval['총물동량'])
        eval_ridge.fit(train_data_eval[feature_cols], train_data_eval['총물동량'])
        
        val_preds_xgb = eval_xgb.predict(val_data_eval[feature_cols])
        val_preds_ridge = eval_ridge.predict(val_data_eval[feature_cols])
        val_preds_ens = 0.5 * val_preds_xgb + 0.5 * val_preds_ridge
        
        if budu == '3부두':
            # 3부두는 2주 단위 예측값을 실제 월별 합산(Roll-up)하여 검증 R² 및 MAE 산출
            val_data_eval['Pred'] = val_preds_ens
            monthly_rollup = val_data_eval.groupby('MedianMonth')[['총물동량', 'Pred']].sum().reset_index()
            ss_res = np.sum((monthly_rollup['총물동량'] - monthly_rollup['Pred']) ** 2)
            ss_tot = np.sum((monthly_rollup['총물동량'] - np.mean(monthly_rollup['총물동량'])) ** 2)
            val_r2 = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else np.nan
            val_mae = np.mean(np.abs(monthly_rollup['총물동량'] - monthly_rollup['Pred']))
            print(f"  - {budu} 앙상블 검증 (2주 단위 학습: {len(train_data_eval)}행) | 훈련(XGB R²): {eval_xgb.score(train_data_eval[feature_cols], train_data_eval['총물동량']):.4f} | 검증 R²(월간합산): {val_r2:.4f} | 검증 MAE(월간합산): {val_mae:,.0f} TEU")
        else:
            ss_res = np.sum((val_data_eval['총물동량'] - val_preds_ens) ** 2)
            ss_tot = np.sum((val_data_eval['총물동량'] - np.mean(val_data_eval['총물동량'])) ** 2)
            val_r2 = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else np.nan
            val_mae = np.mean(np.abs(val_data_eval['총물동량'] - val_preds_ens))
            print(f"  - {budu} 앙상블 검증 (학습: {len(train_data_eval)}행) | 훈련(XGB R²): {eval_xgb.score(train_data_eval[feature_cols], train_data_eval['총물동량']):.4f} | 검증 R²: {val_r2:.4f} | 검증 MAE: {val_mae:,.0f} TEU")
    else:
        print(f"  - {budu} 데이터 부족으로 검증 생략")
    explainer = None
    # 최종 미래 예측용 모델 학습 (전체 데이터로 재학습 / Refit)
    full_train_data = group.dropna(subset=feature_cols)
    X_train, y_train = full_train_data[feature_cols], full_train_data['총물동량']
    
    if budu == '3부두':
        # 3부두는 2주 단위 실제 물동량을 집계
        actuals_sum_2026_h1 = group[group['연도'] == 2026]['총물동량'].sum()
    else:
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
        explainer = shap.TreeExplainer(model_xgb, feature_perturbation='tree_path_dependent')
        shap_values = explainer.shap_values(X_train)
        mean_shap = np.abs(shap_values).mean(axis=0)
        shap_df = pd.DataFrame({
            'Feature': feature_cols,
            'SHAP_Value': mean_shap
        }).sort_values(by='SHAP_Value', ascending=False)
        
        print(f"    -> [{budu} Shapley Feature Importance (Target: 총물동량)]")
        for _, row in shap_df.iterrows():
            print(f"       * {row['Feature']}: {row['SHAP_Value']:.2f}")
            
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
        
        proj_steps = 39 if budu == '3부두' else 18
        
        for m_idx in range(1, proj_steps + 1):
            t_idx = last_time_idx + m_idx
            
            if budu == '3부두':
                bw = 13 + m_idx
                if bw <= 26:
                    yr, m = 2026, bw
                else:
                    yr, m = 2027, bw - 26
            else:
                yr, m = (2026, 6 + m_idx) if m_idx <= 6 else (2027, m_idx - 6)
            
            # SCFI MoM 계산 (3부두 2주 단위 변화율은 한 달 변화율의 절반으로 스케일링 적용)
            scfi_raw_val = scenarios[scen_name](m_idx / 2.0 if budu == '3부두' else m_idx)
            prev_scfi_raw_val = scfi_raw_hist[-1]
            scfi_mom_val = (scfi_raw_val - prev_scfi_raw_val) / prev_scfi_raw_val * 100
            
            # Lag12 (또는 Lag26)
            yoy_yr = yr - 1
            lag12 = vol_dict_proj.get((yoy_yr, m), np.nan)
            if np.isnan(lag12):
                if budu == '3부두':
                    approx_m = (m - 1) * 12 // 26 + 1
                    lag12 = mean_val * seasonality_weights[approx_m]
                else:
                    lag12 = mean_val * seasonality_weights[m]
                
            # Trend Ratio 계산
            recent_y_m = []
            for i in range(1, W + 1):
                target_m_idx = m_idx - i
                if target_m_idx >= 1:
                    if budu == '3부두':
                        bw_t = 13 + target_m_idx
                        if bw_t <= 26:
                            y_t, m_t = 2026, bw_t
                        else:
                            y_t, m_t = 2027, bw_t - 26
                    else:
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
                    if budu == '3부두':
                        approx_m_t = (m_y - 1) * 12 // 26 + 1
                        val_y = mean_val * seasonality_weights[approx_m_t]
                    else:
                        val_y = mean_val * seasonality_weights[m_y]
                recent_yoy.append(val_y)
                
            mean_curr = sum(recent_curr) / float(W)
            mean_yoy = sum(recent_yoy) / float(W)
            trend_ratio = 1.0
            if mean_yoy > 0:
                trend_ratio = mean_curr / mean_yoy
                trend_ratio = max(0.5, min(2.0, trend_ratio))
                
            lag12_adj = lag12 * trend_ratio
            
            # RollingAvg3_YoY
            m_plus = m + 1 if m < period_limit else 1
            yr_plus = yoy_yr if m < period_limit else yr
            m_minus = m - 1 if m > 1 else period_limit
            yr_minus = yoy_yr if m > 1 else yoy_yr - 1
            
            v_11 = vol_dict_proj.get((yr_plus, m_plus), np.nan)
            v_12 = lag12
            v_13 = vol_dict_proj.get((yr_minus, m_minus), np.nan)
            
            if np.isnan(v_11):
                if budu == '3부두':
                    approx_m_p = (m_plus - 1) * 12 // 26 + 1
                    v_11 = mean_val * seasonality_weights[approx_m_p]
                else:
                    v_11 = mean_val * seasonality_weights[m_plus]
            if np.isnan(v_13):
                if budu == '3부두':
                    approx_m_m = (m_minus - 1) * 12 // 26 + 1
                    v_13 = mean_val * seasonality_weights[approx_m_m]
                else:
                    v_13 = mean_val * seasonality_weights[m_minus]
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
                    # 1) 전년 동기값
                    val_yoy_val = ops_dict_proj.get((yoy_yr, m, c_col), np.nan)
                    if np.isnan(val_yoy_val):
                        if c_col == 'BerthOccupancy':
                            if budu == '3부두':
                                val_yoy_val = df_occ_3_bw[df_occ_3_bw['Biweek'] == m]['BerthOccupancy'].mean()
                            else:
                                val_yoy_val = df_occ[(df_occ['부두'] == budu) & (df_occ['월'] == m) & (df_occ['연도'] < 2026)]['BerthOccupancy'].mean()
                        elif c_col == 'TransshipmentRatio':
                            if budu == '3부두':
                                approx_m = (m - 1) * 12 // 26 + 1
                                val_yoy_val = df_ratios[(df_ratios['월'] == approx_m) & (df_ratios['연도'] < 2026)]['TransshipmentRatio'].mean() if len(df_ratios) > 0 else np.nan
                                if np.isnan(val_yoy_val):
                                    val_yoy_val = monthly_trans_avg.get(approx_m, 64.0)
                            else:
                                val_yoy_val = df_ratios[(df_ratios['월'] == m) & (df_ratios['연도'] < 2026)]['TransshipmentRatio'].mean() if len(df_ratios) > 0 else np.nan
                                if np.isnan(val_yoy_val):
                                    val_yoy_val = monthly_trans_avg.get(m, 64.0)
                        else:
                            if budu == '3부두':
                                approx_m = (m - 1) * 12 // 26 + 1
                                val_yoy_val = terminal_monthly_ops_avg.get((budu, approx_m, c_col), 10.0 if 'Count' in c_col else (0.0 if 'Ratio' in c_col else 24.0))
                            else:
                                val_yoy_val = terminal_monthly_ops_avg.get((budu, m, c_col), 10.0 if 'Count' in c_col else (0.0 if 'Ratio' in c_col else 24.0))
                    
                    # 2) 최근 W-기간 추세 비율 계산
                    recent_curr_list = []
                    recent_yoy_list = []
                    for ym_t in recent_y_m:
                        val_curr_t = ops_dict_proj.get((ym_t[0], ym_t[1], c_col), np.nan)
                        if np.isnan(val_curr_t):
                            if c_col == 'BerthOccupancy':
                                if budu == '3부두':
                                    val_curr_t = df_occ_3_bw[df_occ_3_bw['Biweek'] == ym_t[1]]['BerthOccupancy'].mean()
                                else:
                                    val_curr_t = df_occ[(df_occ['부두'] == budu) & (df_occ['월'] == ym_t[1]) & (df_occ['연도'] < 2026)]['BerthOccupancy'].mean()
                            elif c_col == 'TransshipmentRatio':
                                if budu == '3부두':
                                    approx_m_t = (ym_t[1] - 1) * 12 // 26 + 1
                                    val_curr_t = df_ratios[(df_ratios['월'] == approx_m_t) & (df_ratios['연도'] < 2026)]['TransshipmentRatio'].mean() if len(df_ratios) > 0 else np.nan
                                    if np.isnan(val_curr_t):
                                        val_curr_t = monthly_trans_avg.get(approx_m_t, 64.0)
                                else:
                                    val_curr_t = df_ratios[(df_ratios['월'] == ym_t[1]) & (df_ratios['연도'] < 2026)]['TransshipmentRatio'].mean() if len(df_ratios) > 0 else np.nan
                                    if np.isnan(val_curr_t):
                                        val_curr_t = monthly_trans_avg.get(ym_t[1], 64.0)
                            else:
                                if budu == '3부두':
                                    approx_m_t = (ym_t[1] - 1) * 12 // 26 + 1
                                    val_curr_t = terminal_monthly_ops_avg.get((budu, approx_m_t, c_col), 10.0 if 'Count' in c_col else (0.0 if 'Ratio' in c_col else 24.0))
                                else:
                                    val_curr_t = terminal_monthly_ops_avg.get((budu, ym_t[1], c_col), 10.0 if 'Count' in c_col else (0.0 if 'Ratio' in c_col else 24.0))
                        recent_curr_list.append(val_curr_t)
                        
                        val_yoy_t = ops_dict_proj.get((ym_t[0] - 1, ym_t[1], c_col), np.nan)
                        if np.isnan(val_yoy_t):
                            if c_col == 'BerthOccupancy':
                                if budu == '3부두':
                                    val_yoy_t = df_occ_3_bw[df_occ_3_bw['Biweek'] == ym_t[1]]['BerthOccupancy'].mean()
                                else:
                                    val_yoy_t = df_occ[(df_occ['부두'] == budu) & (df_occ['월'] == ym_t[1]) & (df_occ['연도'] < 2026)]['BerthOccupancy'].mean()
                            elif c_col == 'TransshipmentRatio':
                                if budu == '3부두':
                                    approx_m_t = (ym_t[1] - 1) * 12 // 26 + 1
                                    val_yoy_t = df_ratios[(df_ratios['월'] == approx_m_t) & (df_ratios['연도'] < 2026)]['TransshipmentRatio'].mean() if len(df_ratios) > 0 else np.nan
                                    if np.isnan(val_yoy_t):
                                        val_yoy_t = monthly_trans_avg.get(approx_m_t, 64.0)
                                else:
                                    val_yoy_t = df_ratios[(df_ratios['월'] == ym_t[1]) & (df_ratios['연도'] < 2026)]['TransshipmentRatio'].mean() if len(df_ratios) > 0 else np.nan
                                    if np.isnan(val_yoy_t):
                                        val_yoy_t = monthly_trans_avg.get(ym_t[1], 64.0)
                            else:
                                if budu == '3부두':
                                    approx_m_t = (ym_t[1] - 1) * 12 // 26 + 1
                                    val_yoy_t = terminal_monthly_ops_avg.get((budu, approx_m_t, c_col), 10.0 if 'Count' in c_col else (0.0 if 'Ratio' in c_col else 24.0))
                                else:
                                    val_yoy_t = terminal_monthly_ops_avg.get((budu, ym_t[1], c_col), 10.0 if 'Count' in c_col else (0.0 if 'Ratio' in c_col else 24.0))
                        recent_yoy_list.append(val_yoy_t)
                        
                    mean_curr_ops = sum(recent_curr_list) / float(W)
                    mean_yoy_ops = sum(recent_yoy_list) / float(W)
                    tr_ops = 1.0
                    if mean_yoy_ops > 0:
                        tr_ops = mean_curr_ops / mean_yoy_ops
                        tr_ops = max(0.5, min(2.0, tr_ops))
                        
                    val_adj_val = val_yoy_val * tr_ops
                    pred_ops_vals[c_col] = val_adj_val
                
                ops_dict_proj[(yr, m, c_col)] = pred_ops_vals[c_col]
            
            if budu == '3부두':
                approx_m = (m - 1) * 12 // 26 + 1
                w_val = seasonality_weights[approx_m]
                month_sin_val = np.sin(2 * np.pi * m / 26.0)
                month_cos_val = np.cos(2 * np.pi * m / 26.0)
            else:
                w_val = seasonality_weights[m]
                month_sin_val = np.sin(2 * np.pi * m / 12.0)
                month_cos_val = np.cos(2 * np.pi * m / 12.0)
                
            pred_input = pd.DataFrame([{
                'TimeIndex': t_idx,
                'Weight': w_val,
                'TransshipmentRatio': pred_ops_vals['TransshipmentRatio'],
                'SCFI_MoM': scfi_mom_val,
                'BerthOccupancy': pred_ops_vals['BerthOccupancy'],
                'Lag12_Adj': lag12_adj,
                'RollingAvg3_YoY': rolling_yoy,
                'VesselCount': pred_ops_vals['VesselCount'],
                'AvgDwellTime': pred_ops_vals['AvgDwellTime'],
                'ShiftRatio': pred_ops_vals['ShiftRatio'],
                'Month_Sin': month_sin_val,
                'Month_Cos': month_cos_val
            }])
            
            # --- 26년 7월 ~ 12월 및 전체 예측 기간 XAI 수치 수집 ---
            try:
                if explainer is not None:
                    step_shap = explainer.shap_values(pred_input[feature_cols])[0]
                    budu_en = f"terminal_{budu.replace('부두', '')}"
                    for f_idx, col_name in enumerate(feature_cols):
                        xai_records.append({
                            'terminal': budu_en,
                            'scenario': scen_name.lower(),
                            'ym': f"{yr}-{str(m).zfill(2)}",
                            'feature': col_name.lower(),
                            'feature_value': float(pred_input[col_name].iloc[0]),
                            'shap_value': float(step_shap[f_idx])
                        })
            except Exception as xai_err:
                pass
                
            pred_vol_xgb = model_xgb.predict(pred_input[feature_cols])[0]
            pred_vol_ridge = model_ridge.predict(pred_input[feature_cols])[0]
            pred_vol = max(0, int(0.5 * pred_vol_xgb + 0.5 * pred_vol_ridge))
            
            vol_dict_proj[(yr, m)] = pred_vol
            scfi_raw_hist.append(scfi_raw_val)
            
            if yr == 2026:
                pred_vols_2026_h2.append(pred_vol)
            else:
                pred_vols_2027.append(pred_vol)
                
        if budu == '3부두':
            # 3부두 예측값(2주 단위)을 월별 합산하여 forecast_results 및 plot_data에 연동
            bw_to_m = {1:1, 2:1, 3:2, 4:2, 5:3, 6:3, 7:4, 8:4, 9:5, 10:5, 11:6, 12:6, 13:6, 14:7, 15:7, 16:8, 17:8, 18:9, 19:9, 20:10, 21:10, 22:11, 23:11, 24:12, 25:12, 26:12}
            
            # 2026 H2 (m_idx: 1~13)
            pred_records_26 = []
            for idx_bw in range(1, 14):
                bw = 13 + idx_bw
                m = bw_to_m[bw]
                pred_records_26.append({'월': m, 'Value': pred_vols_2026_h2[idx_bw - 1]})
            df_pred_26 = pd.DataFrame(pred_records_26).groupby('월')['Value'].sum().reset_index()
            sum_2026_h2_m = df_pred_26['Value'].tolist()
            
            # 2027 (m_idx: 14~39)
            pred_records_27 = []
            for idx_bw in range(1, 27):
                bw = idx_bw
                m = bw_to_m[bw]
                pred_records_27.append({'월': m, 'Value': pred_vols_2027[idx_bw - 1]})
            df_pred_27 = pd.DataFrame(pred_records_27).groupby('월')['Value'].sum().reset_index()
            sum_2027_m = df_pred_27['Value'].tolist()
            
            forecast_results.append({
                '부두': budu, '시나리오': scen_name,
                '2026실적(H1)': int(actuals_sum_2026_h1),
                '2026하반기예측': sum(sum_2026_h2_m),
                '2026연간합계': int(actuals_sum_2026_h1 + sum(sum_2026_h2_m)),
                '2027연간합계': sum(sum_2027_m)
            })
            scen_forecasts[scen_name] = sum_2026_h2_m + sum_2027_m
        else:
            forecast_results.append({
                '부두': budu, '시나리오': scen_name,
                '2026실적(H1)': int(actuals_sum_2026_h1),
                '2026하반기예측': sum(pred_vols_2026_h2),
                '2026연간합계': int(actuals_sum_2026_h1 + sum(pred_vols_2026_h2)),
                '2027연간합계': sum(pred_vols_2027)
            })
            scen_forecasts[scen_name] = pred_vols_2026_h2 + pred_vols_2027
            
    if budu == '3부두':
        # 3부두 actuals(실적)도 MedianMonth 기준으로 월별 합산하여 시각화에 연동
        actuals_monthly = group.groupby(['연도', 'MedianMonth']).agg(
            총물동량=('총물동량', 'sum')
        ).reset_index().rename(columns={'MedianMonth': '월'})
        plot_data[budu] = (actuals_monthly, scen_forecasts)
    else:
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
