from utils.bz2_stream import open_bz2_stream
import json
import os

def test_bz2_stream():
    file_path = "/home/rmits/project-wiki/histories_filtered_cite"
    files = [os.path.join(file_path, f) for f in os.listdir(file_path) if f.endswith('.jsonl.bz2')]

    try:
        for test_file in files:
            with open_bz2_stream(test_file) as f:
                for i, line in enumerate(f):
                    data = json.loads(line)
                if data['page_id'] == 3526633:  # Thay bằng page_id bạn muốn kiểm tra
                    print(f"Found page: {data['title']} (ID: {data['page_id']}) in file: {test_file}")
                    print(f"Number of revisions: {len(data['revisions'])}")
                    break
    except Exception as e:
        print(f"Error reading bz2 file: {e}")

if __name__ == "__main__":
    test_bz2_stream()