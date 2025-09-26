#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Patient Narrative + Q/A pipeline (CSV/JSONL -> JSONL), OpenAI, single-step generation
- Safe + professional: no fabrication, strict JSON output
- Uses ALL non-empty fields per patient (with truncation option)
- English only; narrative in professional clinical style
- Q/A avoids trivial lookups, must be grounded strictly in given fields
"""

import os, sys, json, argparse, time, re, csv
from typing import Dict, Any, List, Tuple

SYSTEM_PROMPT = """You are a medical data summarization assistant.
Rules:
- Use ONLY the provided facts (fields and values) from the input. Do NOT add external knowledge, assumptions, or invented details.
- The narrative must be concise, professional, and written in a clinical style. Integrate multiple fields if possible, but never add facts not explicitly present.
- Q/A pairs must:
  * Be grounded strictly in the provided facts.
  * Avoid trivial one-to-one lookups (e.g., "What is the patient's age?").
  * Prefer questions that combine or compare multiple fields, as long as those fields exist.
  * Do NOT ask about fields that are not present.
- If some information is missing, state "unknown/not recorded" without speculation.
- Return STRICT JSON with the exact keys requested.
"""

USER_TEMPLATE = """Given the following structured patient facts from a record, produce in one step:
1) A concise professional clinical narrative in English, integrating as many relevant facts as possible into one coherent paragraph. The narrative must not introduce any information not explicitly listed in the facts.
2) {qa_count} Q/A pairs. Each question must:
   - Be based ONLY on the provided facts.
   - Avoid trivial questions that just restate a single field.
   - Focus on relationships, comparisons, or contextual meaning between the fields that exist.
   - Provide answers strictly from the given facts; if the answer is not available, say "unknown/not recorded".
3) Return STRICT JSON with this shape:
{{
  "narrative": "<one paragraph clinical narrative>",
  "qa": [{{"q":"...","a":"..."}}, ... {qa_count} items],
  "used_fields": ["list the field names you used, ordered by importance"]
}}

[FACTS] (use ONLY these, never invent or assume anything beyond them):
{facts}
"""

def truncate_value(v: Any, limit: int) -> str:
    if isinstance(v, (dict, list)):
        s = json.dumps(v, ensure_ascii=False)
    else:
        s = str(v)
    if limit and limit > 0 and len(s) > limit:
        return s[:limit] + "…"
    return s

def facts_from_record(rec: Dict[str, Any], truncate: int) -> Tuple[str, List[str]]:
    lines = []
    used = []
    keys = list(rec.keys())
    if "eid" in rec:
        keys.remove("eid")
        keys = ["eid"] + keys
    for k in keys:
        v = rec.get(k, None)
        if v is None or (isinstance(v, str) and v.strip() == ""):
            continue
        s = truncate_value(v, truncate)
        lines.append(f"- {k}: {s}")
        used.append(k)
    return "\n".join(lines) if lines else "- no_facts: none", used

def extract_json_object(text: str) -> Dict[str, Any]:
    try:
        return json.loads(text)
    except Exception:
        pass
    fence = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, flags=re.S|re.I)
    if fence:
        try:
            return json.loads(fence.group(1))
        except Exception:
            pass
    m = re.search(r"\{.*\}", text, flags=re.S)
    if m:
        try:
            return json.loads(m.group(0))
        except Exception:
            pass
    raise ValueError("Model did not return valid JSON.")

def validate_schema(obj: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(obj, dict):
        raise ValueError("Not a JSON object.")
    for k in ["narrative", "qa", "used_fields"]:
        if k not in obj:
            obj[k] = "" if k == "narrative" else []
    if not isinstance(obj["narrative"], str):
        obj["narrative"] = str(obj["narrative"])
    if not isinstance(obj["qa"], list):
        obj["qa"] = []
    clean_qa = []
    for item in obj["qa"]:
        if isinstance(item, dict):
            q = item.get("q", "")
            a = item.get("a", "")
            clean_qa.append({"q": str(q), "a": str(a)})
        elif isinstance(item, list) and len(item) >= 2:
            clean_qa.append({"q": str(item[0]), "a": str(item[1])})
    obj["qa"] = clean_qa
    uf = obj.get("used_fields", [])
    if not isinstance(uf, list):
        uf = [str(uf)]
    obj["used_fields"] = [str(x) for x in uf]
    return obj

def call_openai(model: str, system_prompt: str, user_prompt: str, api_key: str) -> str:
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        resp = client.chat.completions.create(
            model=model,
            temperature=0.2,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        return resp.choices[0].message.content
    except Exception:
        import openai
        openai.api_key = api_key
        resp = openai.ChatCompletion.create(
            model=model,
            temperature=0.2,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        return resp["choices"][0]["message"]["content"]

def dryrun_answer(rec: Dict[str, Any], qa_count: int) -> str:
    keys = [k for k in rec.keys() if rec.get(k) not in (None, "", [])]
    ks = keys[:max(1, min(qa_count, 5))]
    narrative = "This is a dry-run narrative built from the provided fields: " + ", ".join(ks) + "."
    qa = [{"q": f"What is the recorded value of {k}?", "a": f"{rec[k]}"} for k in ks]
    return json.dumps({"narrative": narrative, "qa": qa, "used_fields": ks}, ensure_ascii=False)

def process_file(inp: str, outp: str, model: str, qa_count: int, truncate: int,
                 provider: str, limit: int, sleep: float) -> None:
    api_key = os.getenv("OPENAI_API_KEY", "")
    if provider == "openai" and not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")

    processed = 0
    succeeded = 0

    # 支持 CSV 输入
    if inp.lower().endswith(".csv"):
        with open(inp, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            records = list(reader)
    else:  # JSONL
        with open(inp, "r", encoding="utf-8") as f:
            records = [json.loads(line) for line in f if line.strip()]

    with open(outp, "w", encoding="utf-8") as fout:
        for rec in records:
            if limit is not None and processed >= limit:
                break
            processed += 1

            facts, used_fields = facts_from_record(rec, truncate)
            user_prompt = USER_TEMPLATE.format(facts=facts, qa_count=qa_count)

            try:
                if provider == "openai":
                    raw = call_openai(model, SYSTEM_PROMPT, user_prompt, api_key)
                else:
                    raw = dryrun_answer(rec, qa_count)
                obj = validate_schema(extract_json_object(raw))
            except Exception:
                stricter_user = user_prompt + "\n\nReturn ONLY a valid JSON object with no extra text, no markdown."
                if provider == "openai":
                    raw = call_openai(model, SYSTEM_PROMPT, stricter_user, api_key)
                else:
                    raw = dryrun_answer(rec, qa_count)
                obj = validate_schema(extract_json_object(raw))

            out_row = {
                "eid": rec.get("eid", rec.get("EID", rec.get("f_eid_00", None))),
                "narrative": obj["narrative"],
                "qa": obj["qa"],
                "used_fields": obj.get("used_fields", used_fields) or used_fields,
            }
            fout.write(json.dumps(out_row, ensure_ascii=False) + "\n")
            succeeded += 1
            if sleep and sleep > 0:
                time.sleep(sleep)

    print(f"Done. processed={processed}, succeeded={succeeded}, output={outp}")

def main():
    ap = argparse.ArgumentParser(description="CSV/JSONL -> LLM narratives + Q/A (English only, OpenAI or dryrun)")
    ap.add_argument("--in", dest="inp", required=True, help="Input CSV or JSONL")
    ap.add_argument("--out", dest="out", required=True, help="Output JSONL (narrative + qa)")
    ap.add_argument("--provider", choices=["openai", "dryrun"], default="openai")
    ap.add_argument("--model", default="gpt-4o-mini")
    ap.add_argument("--qa-count", type=int, default=5)
    ap.add_argument("--truncate", type=int, default=240)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--sleep", type=float, default=0.3)
    args = ap.parse_args()

    process_file(args.inp, args.out, args.model, args.qa_count, args.truncate,
                 args.provider, args.limit, args.sleep)

if __name__ == "__main__":
    main()
