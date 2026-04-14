# Optional cleanup from previous run: remove markers and old logs.
rm .markers_ranking_cite/*
rm logs_cite/*

# Execute full ranking pipeline with 8 workers and default ProbReview S2 setup.
python main.py --input_dir ../histories_filtered --output_dir outputs --parts_dir outputs/parts --marker_dir .markers_ranking_cite --log_dir logs_cite --workers 8 --scheme S2 --alpha 7 --max_iter 100 --tol 1e-6