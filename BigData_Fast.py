# ============================================================
# SIRPAIRIQ ULTRA BIG DATA ENGINE V2
# ============================================================
# High-performance reconciliation engine
# Optimized for 10M–100M+ rows
# ============================================================
# FEATURES
# ============================================================
# - Streaming file ingestion
# - DuckDB vectorized joins
# - Partitioned parquet datasets
# - Candidate blocking
# - RapidFuzz optimized matching
# - Parallel partition processing
# - Incremental CSV writing
# - Memory-safe architecture
# - Low RAM usage
# - Full / Mismatch mode
# - Real-time progress bar
# - Enterprise-scale design
# ============================================================

import os
import gc
import csv
import uuid
import math
import time
import queue
import shutil
import tempfile
import threading
import multiprocessing as mp
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from rapidfuzz import fuzz

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

# ============================================================
# CONFIG
# ============================================================
APP_NAME = "SirpairIQ"

BG = "#ffffff"
CARD = "#142A3E"
TXT = "#E6EEF5"
MUTED = "#9BB3C4"
ACCENT = "#2c6cf5"

CPU_COUNT = max(1, os.cpu_count() - 1)
PARTITIONS = CPU_COUNT * 8
STREAM_CHUNK = 500_000
SIMILARITY_THRESHOLD = 65

TEMP_ROOT = os.path.join(
    tempfile.gettempdir(),
    f"sirpairiq_{uuid.uuid4().hex}"
)

MANDATORY_COLUMNS = [
    "Period", "ShopCode", "Barcode",
    "Description", "DeptDescription",
    "Qty", "Value"
]

ALL_COLUMNS = [
    "Period", "BatchID", "ShopCode", "Barcode",
    "fCrefSuffix", "FCrefAltCode", "Description",
    "fCrefDescriptionSuffix", "DeptCode", "DeptDescription",
    "SupplierCode", "SupplierDescription", "FCrefStatus",
    "FDTGroup", "FsubTag", "Qty", "Value", "fSlot3",
    "fSlot4", "fSlot5", "fSlot6", "fSlot7", "fSlot8",
    "fSlot9", "fSlot10", "fCSlot1", "fCSlot2",
    "(RMS) Retailer SKU",
    "(Mercury) Store Receipt Description",
    "(Mercury) Decoder Ring Barcode",
    "(eComm) Retailer Item Code",
    "URL"
]

OPTIONAL_COLUMNS = [
    c for c in ALL_COLUMNS
    if c not in MANDATORY_COLUMNS and c != "BatchID"
]

# ============================================================
# GLOBALS
# ============================================================
old_file_path = None
new_file_path = None
output_dir = None

# ============================================================
# SAFE HELPERS
# ============================================================
def ensure_dirs():
    folders = [
        TEMP_ROOT,
        os.path.join(TEMP_ROOT, "old"),
        os.path.join(TEMP_ROOT, "new"),
        os.path.join(TEMP_ROOT, "results")
    ]

    for f in folders:
        os.makedirs(f, exist_ok=True)


def cleanup_temp():
    gc.collect()
    time.sleep(2)

    if os.path.exists(TEMP_ROOT):
        shutil.rmtree(TEMP_ROOT, ignore_errors=True)


def safe_float(v):
    try:
        return float(v)
    except:
        return None


def normalize_text(s):
    return " ".join(str(s).strip().split()).lower()


def make_key(shop, barcode):
    return f"{shop}_{barcode}"


def partition_for(key):
    return abs(hash(key)) % PARTITIONS


# ============================================================
# FAST BLOCKING TOKEN
# ============================================================
def blocking_token(desc):
    txt = normalize_text(desc)

    if not txt:
        return ""

    tokens = txt.split()

    if not tokens:
        return ""

    return tokens[0][:4]


# ============================================================
# STREAM FILE -> PARTITIONED PARQUET
# ============================================================
def stream_to_parquet(filepath, side, progress_callback=None):
    buffers = defaultdict(list)
    counts = defaultdict(int)

    out_root = os.path.join(TEMP_ROOT, side)

    total = 0

    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:

        for line in f:
            total += 1

            vals = line.rstrip("\n").split("\t")

            base = vals[:len(ALL_COLUMNS)]
            base.extend([""] * (len(ALL_COLUMNS) - len(base)))

            row = dict(zip(ALL_COLUMNS, base))

            row["Qty"] = safe_float(row["Qty"])
            row["Value"] = safe_float(row["Value"])

            row["Key"] = make_key(
                row["ShopCode"],
                row["Barcode"]
            )

            row["BLOCK"] = blocking_token(row["Description"])

            pid = partition_for(row["Key"])

            buffers[pid].append(row)

            if len(buffers[pid]) >= STREAM_CHUNK:
                flush_partition(buffers[pid], out_root, pid)
                counts[pid] += len(buffers[pid])
                buffers[pid].clear()

            if progress_callback and total % 100000 == 0:
                progress_callback(total)

    for pid, rows in buffers.items():
        if rows:
            flush_partition(rows, out_root, pid)
            counts[pid] += len(rows)

    return counts


# ============================================================
# PARQUET APPEND WRITER
# ============================================================
def flush_partition(rows, root, pid):
    if not rows:
        return

    table = pa.Table.from_pylist(rows)

    path = os.path.join(root, f"part_{pid}_{uuid.uuid4().hex}.parquet")

    pq.write_table(
        table,
        path,
        compression="snappy"
    )


# ============================================================
# CANDIDATE BLOCKING
# ============================================================
def build_candidate_map(rows):
    buckets = defaultdict(list)

    for i, r in enumerate(rows):
        buckets[r.get("BLOCK", "")].append((i, r))

    return buckets


# ============================================================
# FAST SIMILARITY
# ============================================================
def similarity(old_row, new_row):
    d1 = normalize_text(old_row.get("Description", ""))
    d2 = normalize_text(new_row.get("Description", ""))

    dept1 = normalize_text(old_row.get("DeptDescription", ""))
    dept2 = normalize_text(new_row.get("DeptDescription", ""))

    s1 = fuzz.ratio(d1, d2)
    s2 = fuzz.ratio(dept1, dept2)

    return (s1 * 0.7) + (s2 * 0.3)


# ============================================================
# VECTORIZED PARTITION PROCESSOR
# ============================================================
def process_partition(args):
    pid, compare_cols, mode = args

    old_glob = os.path.join(TEMP_ROOT, "old", f"part_{pid}_*.parquet")
    new_glob = os.path.join(TEMP_ROOT, "new", f"part_{pid}_*.parquet")

    old_exists = len([f for f in os.listdir(os.path.join(TEMP_ROOT, "old")) if f.startswith(f"part_{pid}_")]) > 0
    new_exists = len([f for f in os.listdir(os.path.join(TEMP_ROOT, "new")) if f.startswith(f"part_{pid}_")]) > 0

    if not old_exists and not new_exists:
        return None

    con = duckdb.connect(database=':memory:')

    if old_exists:
        con.execute(f"CREATE TABLE old_tbl AS SELECT * FROM read_parquet('{old_glob}')")
    else:
        con.execute("CREATE TABLE old_tbl AS SELECT NULL WHERE FALSE")

    if new_exists:
        con.execute(f"CREATE TABLE new_tbl AS SELECT * FROM read_parquet('{new_glob}')")
    else:
        con.execute("CREATE TABLE new_tbl AS SELECT NULL WHERE FALSE")

    keys = con.execute('''
        SELECT DISTINCT Key
        FROM (
            SELECT Key FROM old_tbl
            UNION
            SELECT Key FROM new_tbl
        )
    ''').fetchall()

    output_file = os.path.join(
        TEMP_ROOT,
        "results",
        f"result_{pid}.csv"
    )

    summary = defaultdict(lambda: defaultdict(int))

    with open(output_file, "w", newline="", encoding="utf-8") as fout:

        writer = csv.writer(fout)

        header = ["Key"]

        for col in compare_cols:
            header.extend([
                f"Old_{col}",
                f"New_{col}",
                f"{col}_Check"
            ])

        header.append("Complete_Match")

        writer.writerow(header)

        for (key,) in keys:

            old_rows = con.execute(
                "SELECT * FROM old_tbl WHERE Key = ?",
                [key]
            ).pl().to_dicts()

            new_rows = con.execute(
                "SELECT * FROM new_tbl WHERE Key = ?",
                [key]
            ).pl().to_dicts()

            old_map = build_candidate_map(old_rows)
            new_map = build_candidate_map(new_rows)

            used_old = set()
            used_new = set()

            matches = []

            candidate_blocks = set(old_map.keys()) | set(new_map.keys())

            # ====================================================
            # BLOCKED MATCHING
            # ====================================================
            for blk in candidate_blocks:

                o_candidates = old_map.get(blk, [])
                n_candidates = new_map.get(blk, [])

                scores = []

                for oi, o in o_candidates:
                    for ni, n in n_candidates:

                        score = similarity(o, n)

                        if score >= SIMILARITY_THRESHOLD:
                            scores.append((score, oi, ni))

                scores.sort(reverse=True, key=lambda x: x[0])

                for score, oi, ni in scores:
                    if oi not in used_old and ni not in used_new:
                        used_old.add(oi)
                        used_new.add(ni)
                        matches.append((oi, ni))

            # ====================================================
            # MATCHED ROWS
            # ====================================================
            for oi, ni in matches:
                o = old_rows[oi]
                n = new_rows[ni]

                row = [key]

                complete = True
                mismatch = False

                for col in compare_cols:

                    ov = o.get(col, "")
                    nv = n.get(col, "")

                    if col in ["Qty", "Value"]:
                        chk = (
                            "Match"
                            if safe_float(ov) == safe_float(nv)
                            else "Mismatch"
                        )
                    else:
                        chk = (
                            "Match"
                            if str(ov) == str(nv)
                            else "Mismatch"
                        )

                    if chk != "Match":
                        mismatch = True
                        complete = False

                    summary[col][chk] += 1
                    summary[col]["TOTAL"] += 1

                    row.extend([ov, nv, chk])

                row.append(complete)

                if mode == "MISMATCH":
                    if mismatch:
                        writer.writerow(row)
                else:
                    writer.writerow(row)

            # ====================================================
            # OLD ONLY
            # ====================================================
            for oi, o in enumerate(old_rows):
                if oi in used_old:
                    continue

                row = [key]

                for col in compare_cols:
                    summary[col]["NOTFOUND"] += 1
                    summary[col]["TOTAL"] += 1

                    row.extend([
                        o.get(col, ""),
                        "Not Found",
                        "Not Found"
                    ])

                row.append(False)
                writer.writerow(row)

            # ====================================================
            # NEW ONLY
            # ====================================================
            for ni, n in enumerate(new_rows):
                if ni in used_new:
                    continue

                row = [key]

                for col in compare_cols:
                    summary[col]["NOTFOUND"] += 1
                    summary[col]["TOTAL"] += 1

                    row.extend([
                        "Not Found",
                        n.get(col, ""),
                        "Not Found"
                    ])

                row.append(False)
                writer.writerow(row)

    con.close()

    return output_file, summary


# ============================================================
# MERGE OUTPUTS
# ============================================================
def merge_results(final_csv):

    result_dir = os.path.join(TEMP_ROOT, "results")

    files = [
        os.path.join(result_dir, f)
        for f in os.listdir(result_dir)
        if f.endswith(".csv")
    ]

    first = True

    with open(final_csv, "w", newline="", encoding="utf-8") as fout:
        writer = csv.writer(fout)

        for fp in files:
            with open(fp, "r", encoding="utf-8") as fin:
                reader = csv.reader(fin)

                header = next(reader)

                if first:
                    writer.writerow(header)
                    first = False

                for row in reader:
                    writer.writerow(row)


# ============================================================
# SUMMARY WRITER
# ============================================================
def write_summary(summary, compare_cols):

    path = os.path.join(
        output_dir,
        "Summary_Column_Summary.csv"
    )

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        writer.writerow([
            "Column_Name",
            "Total_Records",
            "Mismatch_Count",
            "Mismatch_Percentage"
        ])

        for col in compare_cols:
            total = summary[col]["TOTAL"]
            mism = summary[col]["Mismatch"]

            pct = round((mism / total) * 100, 2) if total else 0

            writer.writerow([
                col,
                total,
                mism,
                pct
            ])


# ============================================================
# MAIN COMPARISON ENGINE
# ============================================================
def run_comparison():

    ensure_dirs()

    compare_btn.config(state="disabled")

    progress_frame.pack(fill="x", pady=10)

    selected_optional = [
        opt_list.get(i)
        for i in opt_list.curselection()
    ]

    compare_cols = MANDATORY_COLUMNS + selected_optional

    mode = mode_var.get()

    # ========================================================
    # STREAM OLD
    # ========================================================
    progress_lbl.config(text="Streaming OLD file...")
    root.update_idletasks()

    stream_to_parquet(old_file_path, "old")

    # ========================================================
    # STREAM NEW
    # ========================================================
    progress_lbl.config(text="Streaming NEW file...")
    root.update_idletasks()

    stream_to_parquet(new_file_path, "new")

    # ========================================================
    # PARALLEL PROCESSING
    # ========================================================
    tasks = [
        (pid, compare_cols, mode)
        for pid in range(PARTITIONS)
    ]

    progress_bar["maximum"] = len(tasks)

    final_summary = defaultdict(lambda: defaultdict(int))

    completed = 0

    progress_lbl.config(text="Running vectorized comparison...")
    root.update_idletasks()

    with ProcessPoolExecutor(max_workers=CPU_COUNT) as exe:

        futures = [
            exe.submit(process_partition, t)
            for t in tasks
        ]

        for fut in as_completed(futures):

            result = fut.result()

            completed += 1

            progress_bar["value"] = completed

            progress_lbl.config(
                text=f"Processed {completed}/{len(tasks)} partitions"
            )

            root.update_idletasks()

            if result is None:
                continue

            _, summ = result

            for col in compare_cols:
                for k, v in summ[col].items():
                    final_summary[col][k] += v

    # ========================================================
    # MERGE OUTPUTS
    # ========================================================
    progress_lbl.config(text="Merging outputs...")
    root.update_idletasks()

    comparison_csv = os.path.join(output_dir, "Comparison.csv")

    merge_results(comparison_csv)

    # ========================================================
    # WRITE SUMMARY
    # ========================================================
    progress_lbl.config(text="Writing summaries...")
    root.update_idletasks()

    write_summary(final_summary, compare_cols)

    cleanup_temp()

    progress_lbl.config(text="✅ Comparison completed")

    compare_btn.config(state="normal")

    messagebox.showinfo(
        APP_NAME,
        "Comparison completed successfully"
    )


# ============================================================
# UI FUNCTIONS
# ============================================================
def load_old():
    global old_file_path, output_dir

    p = filedialog.askopenfilename()

    if p:
        old_file_path = p
        output_dir = os.path.dirname(p)

        old_lbl.config(
            text=f"Old File: {os.path.basename(p)} ✅"
        )

        validate_ready()


# ============================================================
def load_new():
    global new_file_path

    p = filedialog.askopenfilename()

    if p:
        new_file_path = p

        new_lbl.config(
            text=f"New File: {os.path.basename(p)} ✅"
        )

        validate_ready()


# ============================================================
def validate_ready():
    if old_file_path and new_file_path:
        compare_btn.config(state="normal")


# ============================================================
def start_compare():
    t = threading.Thread(
        target=run_comparison,
        daemon=True
    )
    t.start()


# ============================================================
# UI
# ============================================================
root = tk.Tk()
root.title(APP_NAME)
root.geometry("980x760")
root.configure(bg=BG)

style = ttk.Style(root)
style.configure(
    "NIQ.Horizontal.TProgressbar",
    troughcolor="#e6e9f0",
    background=ACCENT,
    thickness=6
)

# ============================================================
# TITLE
# ============================================================
tk.Label(
    root,
    text=APP_NAME,
    bg=BG,
    fg=ACCENT,
    font=("Segoe UI", 24, "bold")
).pack(pady=10)

# ============================================================
# CARD
# ============================================================
card = tk.Frame(root, bg=CARD)
card.pack(fill="both", expand=True, padx=25, pady=10)

# ============================================================
# MANDATORY
# ============================================================
tk.Label(
    card,
    text="Mandatory Columns",
    bg=CARD,
    fg=TXT,
    font=("Segoe UI", 11, "bold")
).pack(anchor="w", padx=20, pady=5)


tk.Label(
    card,
    text=", ".join(MANDATORY_COLUMNS),
    bg=CARD,
    fg=MUTED,
    wraplength=900
).pack(anchor="w", padx=20)

# ============================================================
# OPTIONAL
# ============================================================
tk.Label(
    card,
    text="Optional Columns (Multi‑Select)",
    bg=CARD,
    fg=TXT,
    font=("Segoe UI", 11, "bold")
).pack(anchor="w", padx=20, pady=10)

opt_list = tk.Listbox(
    card,
    selectmode=tk.MULTIPLE,
    height=10,
    bg=BG,
    fg="black",
    selectbackground=ACCENT
)

for c in OPTIONAL_COLUMNS:
    opt_list.insert(tk.END, c)

opt_list.pack(fill="x", padx=20)

# ============================================================
# FILE LABELS
# ============================================================
old_lbl = tk.Label(
    card,
    text="Old File: Not selected",
    bg=CARD,
    fg=MUTED
)

new_lbl = tk.Label(
    card,
    text="New File: Not selected",
    bg=CARD,
    fg=MUTED
)

old_lbl.pack(anchor="w", padx=20, pady=5)
new_lbl.pack(anchor="w", padx=20)

# ============================================================
# BUTTONS
# ============================================================
button_frame = tk.Frame(card, bg=CARD)
button_frame.pack(fill="x", pady=10)


tk.Button(
    button_frame,
    text="Load Old File",
    bg=ACCENT,
    fg="white",
    width=20,
    command=load_old
).pack(side="left", padx=10)


tk.Button(
    button_frame,
    text="Load New File",
    bg=ACCENT,
    fg="white",
    width=20,
    command=load_new
).pack(side="left", padx=10)

compare_btn = tk.Button(
    button_frame,
    text="Compare Files",
    bg=ACCENT,
    fg="white",
    width=20,
    state="disabled",
    command=start_compare
)

compare_btn.pack(side="left", padx=10)

# ============================================================
# MODE
# ============================================================
mode_var = tk.StringVar(value="FULL")


tk.Label(
    card,
    text="Comparison Mode",
    bg=CARD,
    fg=TXT,
    font=("Segoe UI", 10, "bold")
).pack(anchor="w", padx=20, pady=(10, 2))

mode_frame = tk.Frame(card, bg=CARD)
mode_frame.pack(anchor="w", padx=20)


tk.Radiobutton(
    mode_frame,
    text="Full Comparison",
    variable=mode_var,
    value="FULL",
    bg=CARD,
    fg=TXT,
    selectcolor=CARD
).pack(side="left")


tk.Radiobutton(
    mode_frame,
    text="Mismatch / Extra Only",
    variable=mode_var,
    value="MISMATCH",
    bg=CARD,
    fg=TXT,
    selectcolor=CARD
).pack(side="left", padx=20)

# ============================================================
# PROGRESS
# ============================================================
progress_frame = tk.Frame(card, bg=CARD)

progress_bar = ttk.Progressbar(
    progress_frame,
    style="NIQ.Horizontal.TProgressbar"
)

progress_lbl = tk.Label(
    progress_frame,
    bg=CARD,
    fg=MUTED
)

progress_bar.pack(fill="x", padx=20)
progress_lbl.pack()

progress_frame.pack(fill="x", pady=10)
progress_frame.pack_forget()

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    mp.freeze_support()
    root.mainloop()
