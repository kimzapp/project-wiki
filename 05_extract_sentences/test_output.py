import json
from pprint import pprint

def main():
    output_path = '/home/rmits/project-wiki/sentences/output_method1.jsonl'
    preview_path = '/home/rmits/project-wiki/sentences/preview_output.json'
    with open(output_path, 'r', encoding='utf-8') as f:
        with open(preview_path, 'w', encoding='utf-8') as preview_f:
            for line in f.readlines():
                page = json.loads(line)
                preview_f.write(json.dumps(page, ensure_ascii=False) + "\n")
                print(f"Page ID: {page['page_id']}, Title: {page['title']}, Paragraphs: {len(page['paragraphs'])}")
                for p in page['paragraphs']:
                    print(f"  Paragraph ID: {p['paragraph_id']}, Sentences: {len(p['sentences'])}")
                    for s in p['sentences']:
                        print(f"    Sentence ID: {s['sentence_id']}, Label: {s['label']}, Text: {s['text'][:50]}...")
                print("-" * 80)

                break  # chỉ xem trước 1 page để kiểm tra định dạng

if __name__ == "__main__":
    main()
