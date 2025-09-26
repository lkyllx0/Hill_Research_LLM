1. UKB 字段解码与重命名 (ukb_decode_cells_final_fix.py)

功能：

解析 UK Biobank 官方 HTML 数据字典

将 CSV 表头重命名为 snake_case + 序号 + 实例描述

根据 coding 表自动解码字段值

python ukb_decode_cells_final_fix.py \
  -H UKB_data_field_explanation.html \
  -i input.csv \
  -o output.csv \
  --cache-json cache.json \
  --instance-json instance_mapping.json

2. 患者叙述与问答生成 (patient_narrative_qa_v3_2.py)

功能：

输入：CSV 或 JSONL 格式的患者结构化字段

输出：JSONL，包含：

narrative（专业临床叙述）

qa（问答对）

used_fields（使用过的字段）

默认调用 OpenAI 接口（需设置 OPENAI_API_KEY 环境变量）

支持 dryrun 模式，生成模拟结果

# OpenAI 模式
export OPENAI_API_KEY=your_api_key
python patient_narrative_qa_v3_2.py \
  --in patients.csv \
  --out patients_narrative.jsonl \
  --provider openai \
  --model gpt-4o-mini \
  --qa-count 5

# Dryrun 模式
python patient_narrative_qa_v3_2.py \
  --in patients.csv \
  --out dryrun.jsonl \
  --provider dryrun

3. CSV 转 JSON/JSONL (csv_to_json_exporter.py)

功能：

将 CSV 文件转换为 JSONL 和 JSON 数组

支持 --limit 参数，限制输出行数

自动跳过空字段

示例命令：

python csv_to_json_exporter.py \
  --csv patients.csv \
  --out-prefix output \
  --limit 100

1.使用 ukb_decode_cells_final_fix.py 对字段进行解码和重命名
2.使用 csv_to_json_exporter.py 将原始 CSV 转换为 JSONL
3.使用 patient_narrative_qa_v3_2.py 生成临床叙述与问答
