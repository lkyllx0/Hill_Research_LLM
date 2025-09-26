#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
UK Biobank CSV header renamer + universal decoder
(final_fix: stable base names + instance descriptions + field-wide coding fallback)

What this does
--------------
1) Parse Columns HTML to get, per UDI (e.g., "3-0.0"):
   - field description (e.g., "Verbal interview duration")
   - coding id (if stated)

2) Rename headers
   - Base name = <description> in your chosen style (default snake_case)
   - Index per base: _00, _01, ... (per description bucket, NOT per original column string)
   - Append instance description if available from instance mapping JSON:
        verbal_interview_duration_01 (First repeat assessment visit (2012-13))

3) Decode all cells
   - If *any* UDI row of a field mentions a coding id, the same coding id is used as fallback
     for every column of that field (so all instances/arrays get decoded, not just the first one).

4) Cache
   - --cache-json path will be used to read/write coding maps, avoiding repeated fetches.

Usage
-----
python ukb_decode_cells_final_fix.py -H UKB_data_field_explanation.html -i input.csv -o output.csv \
  --cache-json cache.json --instance-json instance_mapping.json
"""

import argparse, csv, io, os, re, sys, json, time
from typing import Dict, Tuple, Optional, List, Set
from collections import defaultdict

import requests
from bs4 import BeautifulSoup

FALLBACK_BASES = [
    "https://biobank.ndph.ox.ac.uk/ukb/",
    "https://biobank.ctsu.ox.ac.uk/crystal/",
]

def eprint(*a, **k): print(*a, file=sys.stderr, **k)

# ---------------- Coding fetching & parsing ----------------
def fetch_coding_html_variants(coding_id: int, url_hint: Optional[str] = None, timeout=20):
    urls = []
    for base in FALLBACK_BASES:
        urls.append(f"{base}coding.cgi?id={coding_id}&nl=1")
    if url_hint:
        urls.append(url_hint)
    for base in FALLBACK_BASES:
        urls.append(f"{base}coding.cgi?id={coding_id}")
    seen = set(); out = []
    for u in urls:
        if u in seen: continue
        seen.add(u)
        try:
            r = requests.get(u, timeout=timeout)
            if r.status_code == 200 and ("<table" in r.text.lower() or "coding" in r.text.lower()):
                out.append((u, r.text))
        except Exception:
            continue
    return out

def parse_coding_table_to_strmap_from_html(html: str) -> Dict[str,str]:
    soup = BeautifulSoup(html, "lxml")
    best = None
    for tbl in soup.find_all("table"):
        ths = [th.get_text(strip=True).lower() for th in tbl.find_all("th")]
        if not ths: continue
        if (any("coding" in h or "value" in h or "code" in h for h in ths)
            and any("meaning" in h or "description" in h for h in ths)):
            best = tbl; break
    if best is None:
        tlist = soup.find_all("table")
        if not tlist: return {}
        best = tlist[0]
    mapping = {}
    for tr in best.find_all("tr"):
        tds = tr.find_all(["td","th"])
        if len(tds) < 2: continue
        v = tds[0].get_text(" ", strip=True)
        m = tds[1].get_text(" ", strip=True)
        if not v or not m: continue
        mapping[str(v)] = m
    return mapping

def try_download_tsv_to_strmap(page_html: str, base_url: str) -> Dict[str,str]:
    soup = BeautifulSoup(page_html, "lxml")
    a = None
    for link in soup.find_all("a"):
        txt = (link.get_text() or "").strip().lower()
        href = link.get("href", "")
        if "download" in txt and href:
            a = href; break
    if not a: return {}
    if not a.startswith("http"):
        if "ctsu.ox.ac.uk" in base_url:
            a = "https://biobank.ctsu.ox.ac.uk/crystal/" + a.lstrip("/")
        else:
            a = "https://biobank.ndph.ox.ac.uk/ukb/" + a.lstrip("/")
    try:
        r = requests.get(a, timeout=20)
        if r.status_code != 200: return {}
        text = r.text
        import csv as _csv, io as _io
        try:
            dialect = _csv.Sniffer().sniff(text[:1000], delimiters="\t,;")
            delim = dialect.delimiter
        except Exception:
            delim = "\t"
        reader = _csv.reader(_io.StringIO(text), delimiter=delim)
        header = next(reader, None)
        def row_to_pair(row):
            if len(row) < 2: return None
            c = row[0].strip(); d = row[1].strip()
            if not c or not d: return None
            return (c, d)
        mapping = {}
        if header and (re.search(r"code|coding|value", " ".join(header), re.I) or re.search(r"meaning|description", " ".join(header), re.I)):
            pass
        else:
            if header:
                pr = row_to_pair(header)
                if pr: mapping[pr[0]] = pr[1]
        for row in reader:
            pr = row_to_pair(row)
            if pr: mapping[pr[0]] = pr[1]
        return mapping
    except Exception:
        return {}

def build_one_coding_map_str(coding_id: int, url_hint: Optional[str] = None) -> Dict[str,str]:
    variants = fetch_coding_html_variants(coding_id, url_hint=url_hint)
    # nl=1 first
    for url, html in variants:
        if "&nl=1" in url:
            mp = parse_coding_table_to_strmap_from_html(html)
            if mp: return mp
    # try TSV
    for url, html in variants:
        tsv_map = try_download_tsv_to_strmap(html, base_url=url)
        if tsv_map: return tsv_map
    # raw page last
    for url, html in variants:
        mp = parse_coding_table_to_strmap_from_html(html)
        if mp: return mp
    return {}

# ---------------- HTML dictionary parsing ----------------
def clean_desc(desc: str) -> str:
    # remove trailing "Uses data-coding NNNNN"
    return re.sub(r"\s*Uses\s+data-coding\s+\d+\s*$", "", desc.strip(), flags=re.I)

def find_columns_table(soup: BeautifulSoup):
    for tbl in soup.find_all("table"):
        ths = [th.get_text(strip=True).lower() for th in tbl.find_all("th")]
        if not ths: continue
        if any("udi" in h for h in ths) and any("description" in h for h in ths):
            return tbl
    return soup.find("table")

def parse_dictionary_html(html_path: str):
    if not os.path.isfile(html_path):
        raise FileNotFoundError(f"Dictionary HTML not found: {html_path}")
    with open(html_path, "r", encoding="utf-8", errors="ignore") as f:
        soup = BeautifulSoup(f, "lxml")
    table = find_columns_table(soup)
    if not table:
        raise RuntimeError("Could not find a Columns table in the HTML.")

    # udi_info: key UDI -> dict(field, inst, arr, desc, coding_id)
    udi_info: Dict[str, dict] = {}
    # coding_urls for potential downloads
    coding_urls: Dict[int, str] = {}

    for tr in table.find_all("tr")[1:]:
        tds = tr.find_all("td")
        if len(tds) < 5: continue
        udi_text = tds[1].get_text(strip=True)
        desc_text = clean_desc(tds[4].get_text(" ", strip=True))
        coding_id = None
        a = tds[4].find("a", href=re.compile(r"coding\.cgi\?id=(\d+)"))
        if a:
            m = re.search(r"id=(\d+)", a["href"])
            if m:
                coding_id = int(m.group(1))
                href = a["href"]
                if href.startswith("http"):
                    coding_urls[coding_id] = href
                else:
                    coding_urls[coding_id] = "https://biobank.ndph.ox.ac.uk/ukb/" + href.lstrip("/")

        m = re.match(r"^(\d+)-(\d+)\.(\d+)$", udi_text)
        if not m: 
            # skip malformed rows
            continue
        field, inst, arr = m.groups()
        udi_info[udi_text] = {
            "field": field, "inst": inst, "arr": arr,
            "desc": desc_text, "coding_id": coding_id
        }

    # Build field-level fallbacks
    field_base_desc: Dict[str, str] = {}
    field_coding_map: Dict[str, int] = {}
    for udi, info in udi_info.items():
        fld = info["field"]
        if fld not in field_base_desc and info["desc"]:
            field_base_desc[fld] = info["desc"]
        if info["coding_id"] is not None and fld not in field_coding_map:
            field_coding_map[fld] = info["coding_id"]

    return udi_info, field_base_desc, field_coding_map, coding_urls

# ---------------- helpers ----------------
def snake(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", text).strip("_").lower()

def parse_header_to_parts(header: str) -> Optional[Tuple[str,str,str]]:
    m = re.match(r"^f\.(\d+)\.(\d+)\.(\d+)$", header)
    if m: return m.groups()
    m2 = re.match(r"^(\d+)-(\d+)\.(\d+)$", header)
    if m2: return m2.groups()
    return None

def build_coding_maps(coding_ids: Set[int], coding_urls: Dict[int,str], cache_json: Optional[str]):
    cache = {}
    if cache_json and os.path.isfile(cache_json):
        try:
            with open(cache_json, "r", encoding="utf-8") as f:
                cache = json.load(f)
        except Exception:
            cache = {}
    out: Dict[int, Dict[str,str]] = {}
    for cid in sorted(coding_ids):
        if str(cid) in cache and isinstance(cache[str(cid)], dict):
            out[cid] = {str(k): v for k, v in cache[str(cid)].items()}
            continue
        mp = build_one_coding_map_str(cid, coding_urls.get(cid))
        if not mp:
            eprint(f"[warn] no mapping parsed for coding {cid}, values kept raw")
            continue
        out[cid] = mp
        time.sleep(0.3)
    if cache_json:
        try:
            with open(cache_json, "w", encoding="utf-8") as f:
                json.dump(out, f, ensure_ascii=False, indent=2)
        except Exception:
            pass
    return out

# ---------------- main ----------------
def main():
    ap = argparse.ArgumentParser(description="UKB CSV renamer + decoder (final_fix)")
    ap.add_argument("-H","--html", required=True, help="Columns HTML")
    ap.add_argument("-i","--input", required=True, help="Input CSV")
    ap.add_argument("-o","--output", required=True, help="Output CSV")
    ap.add_argument("--cache-json", default="", help="Path to coding maps cache JSON")
    ap.add_argument("--instance-json", default="", help="Path to instance mapping JSON (field->inst->desc)")
    ap.add_argument("--style", default="snake", choices=["snake"], help="Naming style for base (default snake)")
    args = ap.parse_args()

    udi_info, field_base_desc, field_coding_map, coding_urls = parse_dictionary_html(args.html)

    # instance mapping (user-provided) is authoritative for instance descriptions
    inst_map = {}
    if args.instance_json and os.path.isfile(args.instance_json):
        try:
            with open(args.instance_json, "r", encoding="utf-8") as f:
                m = json.load(f)
                # accept either {"<field>":{"0":"...","1":"..."}}
                # or {"__instances__": {"<field>": {...}}}
                inst_map = m.get("__instances__", m)
        except Exception:
            inst_map = {}

    # Read header
    with open(args.input, "r", encoding="utf-8", newline="") as fin:
        sample = fin.read(2048); fin.seek(0)
        try:
            delim = csv.Sniffer().sniff(sample, delimiters=",\t;").delimiter
        except Exception:
            delim = ","
        reader = csv.reader(fin, delimiter=delim)
        rows = list(reader)
    if not rows:
        raise SystemExit("Empty CSV.")
    header = rows[0]; data_rows = rows[1:]

    # Build base names for each column using description (fallback by field)
    bases: List[str] = []
    base_counts: Dict[str, int] = defaultdict(int)
    parsed_parts: List[Optional[Tuple[str,str,str]]] = []

    for col in header:
        if col.lower() in ("eid","f.eid"):
            bases.append("eid"); parsed_parts.append(None); base_counts["eid"] += 1
            continue
        parts = parse_header_to_parts(col)
        parsed_parts.append(parts)
        if parts:
            field, inst, arr = parts
            key = f"{field}-{inst}.{arr}"
            desc = (udi_info.get(key) or {}).get("desc", "") if key in udi_info else ""
            if not desc:
                desc = field_base_desc.get(field, col)
            base = snake(desc) if args.style == "snake" else snake(desc)
        else:
            base = snake(col)
        bases.append(base); base_counts[base] += 1

    # zero-pad width per base (min 2)
    pad_by_base = {b: max(2, len(str(n-1))) for b, n in base_counts.items() if b != "eid"}
    running_by_base: Dict[str, int] = defaultdict(int)

    # Prepare header rename list and decide coding per column
    new_header: List[str] = []
    header_coding_idx: Dict[int, int] = {}
    all_needed_coding_ids: Set[int] = set()

    for idx, (col, parts, base) in enumerate(zip(header, parsed_parts, bases)):
        if base == "eid":
            new_header.append("eid")
            continue

        idx_for_base = running_by_base[base]; running_by_base[base] += 1
        name = f"{base}_{str(idx_for_base).zfill(pad_by_base.get(base, 2))}"

        # Instance description append (from instance mapping)
        inst_desc = None
        if parts:
            field, inst, arr = parts
            inst_desc = inst_map.get(str(field), {}).get(str(inst))

        if inst_desc:
            name = f"{name} ({inst_desc})"

        new_header.append(name)

        # Coding id for decoding: prefer exact UDI, else field fallback
        coding_id = None
        if parts:
            field, inst, arr = parts
            key = f"{field}-{inst}.{arr}"
            info = udi_info.get(key)
            if info and info.get("coding_id") is not None:
                coding_id = info["coding_id"]
            else:
                coding_id = field_coding_map.get(field)
        if coding_id is not None:
            header_coding_idx[idx] = coding_id
            all_needed_coding_ids.add(coding_id)

    # Build coding maps
    coding_maps = build_coding_maps(all_needed_coding_ids, coding_urls, args.cache_json or None)

    # Write output with decoding
    with open(args.output, "w", encoding="utf-8", newline="") as fout:
        writer = csv.writer(fout, delimiter=delim)
        writer.writerow(new_header)
        for r in data_rows:
            if len(r) < len(header):
                r = r + [""] * (len(header) - len(r))
            out = list(r)
            for j, cid in header_coding_idx.items():
                raw = out[j]
                if raw is None or raw == "":
                    continue
                mp = coding_maps.get(cid)
                if not mp:
                    continue
                parts = re.split(r"[;,|]", str(raw))
                decoded = []
                for p in parts:
                    k = p.strip().strip('"')
                    if not k: 
                        continue
                    decoded.append(mp.get(k, k))
                out[j] = ";".join(decoded)
            writer.writerow(out)

    eprint(f"[done] wrote: {args.output}")

if __name__ == "__main__":
    main()
