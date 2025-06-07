#!/bin/bash

# 가상환경 활성화
source venv/bin/activate

echo "=== Run start: $(date) ===" >> /mnt/c/Users/jisu/Desktop/log_analysis/logs/cron.log

python3 /mnt/c/Users/jisu/Desktop/log_analysis/scripts/upload_parking_data.py >> /mnt/c/Users/jisu/Desktop/log_analysis/logs/parking.log 2>&1
python3 /mnt/c/Users/jisu/Desktop/log_analysis/scripts/upload_commercial_data.py >> /mnt/c/Users/jisu/Desktop/log_analysis/logs/commercial.log 2>&1

echo "=== Run end: $(date) ===" >> /mnt/c/Users/jisu/Desktop/log_analysis/logs/cron.log
