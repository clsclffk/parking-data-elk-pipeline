#!/bin/bash

# 가상환경의 파이썬 경로 직접 지정 (절대경로)
VENV_PYTHON=/mnt/c/Users/jisu/Desktop/log_analysis/venv/bin/python

# 로그 시작
echo "=== Run start: $(date) ===" >> /mnt/c/Users/jisu/Desktop/log_analysis/logs/cron.log

# 파킹 데이터 업로드
if $VENV_PYTHON /mnt/c/Users/jisu/Desktop/log_analysis/scripts/upload_parking_data.py >> /mnt/c/Users/jisu/Desktop/log_analysis/logs/parking.log 2>&1; then
    echo "[SUCCESS] parking_data 업로드 완료 ($(date))" >> /mnt/c/Users/jisu/Desktop/log_analysis/logs/cron.log
else
    echo "[ERROR] parking_data 업로드 실패 ($(date))" >> /mnt/c/Users/jisu/Desktop/log_analysis/logs/cron.log
fi

# 상권 데이터 업로드
if $VENV_PYTHON /mnt/c/Users/jisu/Desktop/log_analysis/scripts/upload_commercial_data.py >> /mnt/c/Users/jisu/Desktop/log_analysis/logs/commercial.log 2>&1; then
    echo "[SUCCESS] commercial_data 업로드 완료 ($(date))" >> /mnt/c/Users/jisu/Desktop/log_analysis/logs/cron.log
else
    echo "[ERROR] commercial_data 업로드 실패 ($(date))" >> /mnt/c/Users/jisu/Desktop/log_analysis/logs/cron.log
fi

# 로그 종료
echo "=== Run end: $(date) ===" >> /mnt/c/Users/jisu/Desktop/log_analysis/logs/cron.log
