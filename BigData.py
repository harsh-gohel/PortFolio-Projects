import os
import csv
import math
import time
import queue
import shutil
import threading
import multiprocessing as mp
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
import gc
import stat

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl
from rapidfuzz.fuzz import ratio


import tempfile
import uuid


import tkinter as tk
from tkinter import filedialog, messagebox, ttk

# ============================================================
# SIRPAIRIQ BIG DATA ENGINE
# ============================================================
# Enterprise-scale streaming reconciliation engine
# Supports:
# - Huge files
# - Partition-based processing
# - Duplicate-aware fuzzy matching
# - Incremental summaries
# - Low memory execution
# - Streaming outputs
# ============================================================

# ============================================================
# UI PALETTE
# ============================================================
BG = "#ffffff"
CARD = "#142A3E"
TXT = "#E6EEF5"
MUTED = "#9BB3C4"
ACCENT = "#2c6cf5"

# ============================================================
# CONFIG
# ============================================================
APP_NAME = "SirpairIQ"
NUM_PARTITIONS = max(16, (os.cpu_count() or 4) * 4)
CHUNK_SIZE = 250_000
BUFFER_SIZE = 1024 * 1024 * 8


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

CANCEL = mp.Event()

# ============================================================
# HELPERS
# ============================================================

# ============================================================
# SAFE TEMP DIRECTORY CREATION
# ============================================================
def ensure_dirs():
    global TEMP_ROOT

    dirs = [
        TEMP_ROOT,
        os.path.join(TEMP_ROOT, "old"),
        os.path.join(TEMP_ROOT, "new"),
        os.path.join(TEMP_ROOT, "logs"),
        os.path.join(TEMP_ROOT, "comparison")
    ]

    for d in dirs:
        try:
            os.makedirs(d, exist_ok=True)

        except PermissionError as e:
            raise Exception(
                f"Cannot create temp directory:\n{d}\n\n"
                f"Windows denied access.\n"
                f"Try running from another folder or as Administrator."
            ) from e






def cleanup_temp(retries=10, delay=1):
    """
    Safely remove temp folder on Windows.
    Handles:
    - locked parquet files
    - delayed file handles
    - readonly files
    - multiprocessing cleanup delays
    """

    gc.collect()

    if not os.path.exists(TEMP_ROOT):
        return

    def on_rm_error(func, path, exc_info):
        try:
            os.chmod(path, stat.S_IWRITE)
            func(path)
        except:
            pass

    for attempt in range(retries):
        try:
            shutil.rmtree(
                TEMP_ROOT,
                onerror=on_rm_error
            )

            return

        except PermissionError:
            time.sleep(delay)

        except Exception:
            time.sleep(delay)

    print(f"WARNING: Could not fully remove temp folder: {TEMP_ROOT}")

def safe_float(v):
    try:
        return float(v)
    except:
        return None


def normalize_text(s):
    return " ".join(str(s).strip().split()).lower()


def similarity(a, b):
    return ratio(a, b) / 100.0


def make_key(shop, barcode):
    return f"{shop}_{barcode}"


def partition_id(key):
    return abs(hash(key)) % NUM_PARTITIONS


# ============================================================
# STREAMING PARTITION WRITER
# ============================================================
def stream_partition_file(filepath, side, progress_queue=None):
    partition_buffers = defaultdict(list)
    partition_counts = defaultdict(int)

    out_root = os.path.join(TEMP_ROOT, side)

    total_rows = 0

    with open(filepath, "r", encoding="utf-8", errors="ignore", buffering=BUFFER_SIZE) as f:
        for line in f:
            if CANCEL.is_set():
                break

            total_rows += 1

            values = line.rstrip("\n").split("\t")

            base = values[:len(ALL_COLUMNS)]
            extra = values[len(ALL_COLUMNS):]

            base.extend([""] * (len(ALL_COLUMNS) - len(base)))

            row = dict(zip(ALL_COLUMNS, base))

            for i, ex in enumerate(extra):
                row[f"EXTRA_{i+1}"] = ex

            row["Qty"] = safe_float(row["Qty"])
            row["Value"] = safe_float(row["Value"])

            key = make_key(row["ShopCode"], row["Barcode"])
            row["Key"] = key

            pid = partition_id(key)
            partition_buffers[pid].append(row)

            if len(partition_buffers[pid]) >= CHUNK_SIZE:
                flush_partition(pid, partition_buffers[pid], out_root)
                partition_counts[pid] += len(partition_buffers[pid])
                partition_buffers[pid].clear()

            if progress_queue and total_rows % 100000 == 0:
                progress_queue.put(("ROWS", total_rows))

    for pid, rows in partition_buffers.items():
        if rows:
            flush_partition(pid, rows, out_root)
            partition_counts[pid] += len(rows)

    return partition_counts


def flush_partition(pid, rows, out_root):
    if not rows:
        return

    table = pa.Table.from_pylist(rows)

    path = os.path.join(out_root, f"part_{pid}.parquet")

    if os.path.exists(path):
        existing = pq.read_table(path)
        combined = pa.concat_tables([existing, table])
        pq.write_table(combined, path)
    else:
        pq.write_table(table, path)


# ============================================================
# MATCH ENGINE
# ============================================================
def row_similarity(o, n):
    desc_score = similarity(
        normalize_text(o.get("Description", "")),
        normalize_text(n.get("Description", ""))
    )

    dept_score = similarity(
        normalize_text(o.get("DeptDescription", "")),
        normalize_text(n.get("DeptDescription", ""))
    )

    return desc_score * 0.7 + dept_score * 0.3


# ============================================================
# SUMMARY AGGREGATOR
# ============================================================
def create_summary(compare_cols):
    summary = {}

    for col in compare_cols:
        summary[col] = {
            "Total": 0,
            "Mismatch": 0,
            "Match": 0,
            "NotFound": 0
        }

    return summary


# ============================================================
# WORKER
# ============================================================
def process_partition(args):
    pid, compare_cols, mode = args

    old_path = os.path.join(TEMP_ROOT, "old", f"part_{pid}.parquet")
    new_path = os.path.join(TEMP_ROOT, "new", f"part_{pid}.parquet")

    old_rows = []
    new_rows = []

    if os.path.exists(old_path):
        old_rows = pq.read_table(old_path).to_pylist()

    if os.path.exists(new_path):
        new_rows = pq.read_table(new_path).to_pylist()

    old_groups = defaultdict(list)
    new_groups = defaultdict(list)

    for r in old_rows:
        old_groups[r["Key"]].append(r)

    for r in new_rows:
        new_groups[r["Key"]].append(r)

    keys = sorted(set(old_groups.keys()) | set(new_groups.keys()))

    output_path = os.path.join(
        TEMP_ROOT,
        "comparison",
        f"comparison_{pid}.csv"
    )

    summary = create_summary(compare_cols)

    with open(output_path, "w", newline="", encoding="utf-8") as out:
        writer = csv.writer(out)

        header = ["Key"]

        for col in compare_cols:
            header.extend([
                f"Old_{col}",
                f"New_{col}",
                f"{col}_Check"
            ])

        header.append("Complete_Match")

        writer.writerow(header)

        for key in keys:
            if CANCEL.is_set():
                break

            o_rows = old_groups.get(key, [])
            n_rows = new_groups.get(key, [])

            used_o = set()
            used_n = set()

            pairs = []

            # ====================================================
            # BIDIRECTIONAL GREEDY MATCHING
            # ====================================================
            for oi, o in enumerate(o_rows):
                for ni, n in enumerate(n_rows):
                    score = row_similarity(o, n)
                    pairs.append((score, oi, ni))

            pairs.sort(reverse=True, key=lambda x: x[0])

            final_pairs = []

            for score, oi, ni in pairs:
                if oi not in used_o and ni not in used_n:
                    used_o.add(oi)
                    used_n.add(ni)
                    final_pairs.append((oi, ni))

            # ====================================================
            # PAIRED ROWS
            # ====================================================
            for oi, ni in final_pairs:
                o = o_rows[oi]
                n = n_rows[ni]

                row_out = [key]
                complete = True
                mismatch_exists = False
                notfound_exists = False

                for col in compare_cols:
                    ov = o.get(col, "")
                    nv = n.get(col, "")

                    if ov is None:
                        ov = "Not Found"

                    if nv is None:
                        nv = "Not Found"

                    if col in ["Qty", "Value"]:
                        status = (
                            "Match"
                            if safe_float(ov) == safe_float(nv)
                            else "Mismatch"
                        )
                    else:
                        status = (
                            "Match"
                            if str(ov) == str(nv)
                            else "Mismatch"
                        )

                    if status != "Match":
                        complete = False
                        mismatch_exists = True

                    summary[col]["Total"] += 1
                    summary[col][status] += 1

                    row_out.extend([ov, nv, status])

                row_out.append(complete)

                if mode == "MISMATCH":
                    if mismatch_exists or notfound_exists:
                        writer.writerow(row_out)
                else:
                    writer.writerow(row_out)

            # ====================================================
            # OLD ONLY
            # ====================================================
            for oi, o in enumerate(o_rows):
                if oi in used_o:
                    continue

                row_out = [key]

                for col in compare_cols:
                    ov = o.get(col, "")

                    summary[col]["Total"] += 1
                    summary[col]["NotFound"] += 1

                    row_out.extend([
                        ov,
                        "Not Found",
                        "Not Found"
                    ])

                row_out.append(False)
                writer.writerow(row_out)

            # ====================================================
            # NEW ONLY
            # ====================================================
            for ni, n in enumerate(n_rows):
                if ni in used_n:
                    continue

                row_out = [key]

                for col in compare_cols:
                    nv = n.get(col, "")

                    summary[col]["Total"] += 1
                    summary[col]["NotFound"] += 1

                    row_out.extend([
                        "Not Found",
                        nv,
                        "Not Found"
                    ])

                row_out.append(False)
                writer.writerow(row_out)

    return output_path, summary


# ============================================================
# FINAL MERGE
# ============================================================
def merge_outputs(final_output, compare_cols, mode):
    files = sorted([
        os.path.join(TEMP_ROOT, "comparison", f)
        for f in os.listdir(os.path.join(TEMP_ROOT, "comparison"))
        if f.endswith(".csv")
    ])

    with open(final_output, "w", newline="", encoding="utf-8") as fout:
        writer = None

        for i, fp in enumerate(files):
            with open(fp, "r", encoding="utf-8") as fin:
                reader = csv.reader(fin)
                header = next(reader)

                if writer is None:
                    writer = csv.writer(fout)
                    writer.writerow(header)

                for row in reader:
                    writer.writerow(row)


# ============================================================
# SUMMARY FILES
# ============================================================
def write_summary(summary_all, compare_cols):
    path = os.path.join(output_dir, "Summary_Column_Summary.csv")

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        writer.writerow([
            "Column_Name",
            "Total_Records",
            "Mismatch_Count",
            "Mismatch_Percentage"
        ])

        for col in compare_cols:
            total = summary_all[col]["Total"]
            mismatch = summary_all[col]["Mismatch"]

            pct = round((mismatch / total) * 100, 2) if total else 0

            writer.writerow([
                col,
                total,
                mismatch,
                pct
            ])


# ============================================================
# MAIN ENGINE
# ============================================================
def run_comparison():

    global output_dir

    if not old_file_path or not new_file_path:
        messagebox.showerror(APP_NAME, "Please select both files")
        return

    ensure_dirs()

    compare_btn.config(state="disabled")

    progress_frame.pack(fill="x", pady=10)
    progress_bar["value"] = 0

    selected_optional = [
        opt_list.get(i)
        for i in opt_list.curselection()
    ]

    compare_cols = MANDATORY_COLUMNS + selected_optional

    mode = mode_var.get()

    progress_lbl.config(text="Partitioning OLD file...")
    root.update_idletasks()

    q = queue.Queue()

    stream_partition_file(old_file_path, "old")

    progress_lbl.config(text="Partitioning NEW file...")
    root.update_idletasks()

    stream_partition_file(new_file_path, "new")

    progress_lbl.config(text="Running comparison...")
    root.update_idletasks()

    tasks = [
        (pid, compare_cols, mode)
        for pid in range(NUM_PARTITIONS)
    ]

    progress_bar["maximum"] = len(tasks)

    summary_all = create_summary(compare_cols)

    completed = 0

    with ProcessPoolExecutor(max_workers=max(1, os.cpu_count() - 1)) as exe:
        futures = [
            exe.submit(process_partition, t)
            for t in tasks
        ]

        for fut in futures:
            if CANCEL.is_set():
                break

            out_path, summ = fut.result()

            completed += 1

            progress_bar["value"] = completed

            progress_lbl.config(
                text=f"Processed {completed}/{len(tasks)} partitions"
            )

            root.update_idletasks()

            for col in compare_cols:
                for k, v in summ[col].items():
                    summary_all[col][k] += v

    comparison_output = os.path.join(output_dir, "Comparison.csv")

    progress_lbl.config(text="Merging outputs...")
    root.update_idletasks()

    merge_outputs(comparison_output, compare_cols, mode)

    progress_lbl.config(text="Writing summaries...")
    root.update_idletasks()

    write_summary(summary_all, compare_cols)
    # Ensure all workers/files are released
    time.sleep(2)
    gc.collect()

    cleanup_temp()

    progress_lbl.config(text="✅ Comparison completed")

    compare_btn.config(state="normal")

    messagebox.showinfo(APP_NAME, "Comparison completed successfully")


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
def threaded_compare():
    t = threading.Thread(target=run_comparison, daemon=True)
    t.start()


# ============================================================
# UI
# ============================================================
root = tk.Tk()
root.title(APP_NAME)
root.geometry("960x740")
root.configure(bg=BG)

style = ttk.Style(root)

style.configure(
    "NIQ.Horizontal.TProgressbar",
    troughcolor="#e6e9f0",
    background=ACCENT,
    thickness=6
)

# ============================================================
# HEADER
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
    wraplength=880
).pack(anchor="w", padx=20)

# ============================================================
# OPTIONAL
# ============================================================
tk.Label(
    card,
    text="Optional Columns (Multi-Select)",
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
    command=load_old,
    width=20
).pack(side="left", padx=10)


tk.Button(
    button_frame,
    text="Load New File",
    bg=ACCENT,
    fg="white",
    command=load_new,
    width=20
).pack(side="left", padx=10)

compare_btn = tk.Button(
    button_frame,
    text="Compare Files",
    bg=ACCENT,
    fg="white",
    state="disabled",
    command=threaded_compare,
    width=20
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
    selectcolor=CARD,
    activebackground=CARD,
    activeforeground=TXT
).pack(side="left")


tk.Radiobutton(
    mode_frame,
    text="Mismatch / Extra Only",
    variable=mode_var,
    value="MISMATCH",
    bg=CARD,
    fg=TXT,
    selectcolor=CARD,
    activebackground=CARD,
    activeforeground=TXT
).pack(side="left", padx=15)

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
