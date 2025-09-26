#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV -> JSONL/JSON exporter
- 默认输出整个 CSV
- 可选参数 --limit N 只导出前 N 行
- 空白字段自动跳过
"""

import csv, json, argparse, sys

def is_blank(s):
    return s is None or str(s).strip() == ""

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Path to the CSV file")
    ap.add_argument("--out-prefix", default="output", help="Output prefix (default: output)")
    ap.add_argument("--limit", type=int, default=None, help="Limit number of rows (default: all rows)")
    args = ap.parse_args()

    out_jsonl = args.out_prefix + ".jsonl"
    out_json  = args.out_prefix + ".json"

    records = []
    with open(args.csv, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            print("Empty CSV.", file=sys.stderr)
            sys.exit(2)

        for i, row in enumerate(reader):
            if args.limit is not None and i >= args.limit:
                break
            obj = {}
            for col_name, val in zip(header, row):
                if not is_blank(val):
                    obj[col_name] = val
            records.append(obj)

    # 写 JSONL
    with open(out_jsonl, "w", encoding="utf-8") as f:
        for obj in records:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")

    # 写 JSON 数组
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print("Wrote:", out_jsonl, "and", out_json)

if __name__ == "__main__":
    main()
