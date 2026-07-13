"""
run_pipeline.py
---------------
01 -> 02 -> 04 -> 05 를 순차 실행하는 오케스트레이터.
Windows 작업 스케줄러(Task Scheduler)에서 매시간 호출하는 용도.

- 어디서 호출되든 프로젝트 루트 기준으로 동작 (상대경로 깨짐 방지)
- 각 단계 결과/소요시간을 logs/ 에 기록
- 한 단계라도 실패하면 즉시 중단

실행: python run_pipeline.py
"""
import os
import sys
import subprocess
import time
from datetime import datetime

# 호출 위치와 무관하게 항상 이 파일이 있는 폴더(프로젝트 루트)에서 실행
ROOT = os.path.dirname(os.path.abspath(__file__))
os.chdir(ROOT)

STEPS = [
    "01_raw_to_bronze.py",      # raw(xlsx/xls/xml) -> bronze
    "02_bronze_to_silver.py",   # bronze -> silver (컬럼통일/변경감지)
    # "03_container_to_silver.py",  # 월별 물동량 raw가 새로 들어올 때만 켜기 (매시간 X)
    "04_silver_to_gold.py",     # silver -> gold 6종
    "05_gold_to_postgres.py",   # gold -> PostgreSQL
]


def main():
    os.makedirs("logs", exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    logpath = os.path.join("logs", f"pipeline_{stamp}.log")

    def log(msg):
        line = f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}"
        print(line)
        with open(logpath, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    log(f"파이프라인 시작 (python={sys.executable})")
    t_all = time.time()

    for step in STEPS:
        log(f"--- {step} 시작 ---")
        t0 = time.time()
        result = subprocess.run([sys.executable, step])
        dt = time.time() - t0
        if result.returncode != 0:
            log(f"!! {step} 실패 (returncode={result.returncode}, {dt:.1f}s) → 중단")
            sys.exit(1)
        log(f"--- {step} 완료 ({dt:.1f}s) ---")

    log(f"파이프라인 전체 완료 ✅ (총 {time.time()-t_all:.1f}s)")


if __name__ == "__main__":
    main()