python 01_preprocessing/main_parallel.py histories_raw \
    -o 01_preprocessing/outputs \
    -w 8 \
    --marker-dir 01_preprocessing/.markers \
    --log-dir 01_preprocessing/logs \
    --log-every 100000

# Quick run (isolated outputs, small limits)
python 01_preprocessing/main_parallel.py histories_raw --quick-run