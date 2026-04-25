import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import pandas as pd
import os
import multiprocessing as mp
import math
import duckdb
from rapidfuzz import fuzz
import glob

# =====================================================
# PALETTE
# =====================================================
BG = "#ffffff"
CARD = "#142A3E"
TXT = "#E6EEF5"
MUTED = "#9BB3C4"
ACCENT = "#2c6cf5"

# =====================================================
# CONFIG
# =====================================================
DB_FILE = "temp_compare.duckdb"

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

old_file_path = None
new_file_path = None
output_dir = None

progress_counter = None
total_keys_global = 0

# =====================================================
# HELPERS
# =====================================================
def safe_float(v):
    try:
        return float(v)
    except:
        return None

def normalize_text(s):
    return " ".join(str(s).strip().split())

def similarity(a, b):
    return fuzz.ratio(a, b) / 100

# =====================================================
# INIT DB
# =====================================================
def init_db():
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)

    conn = duckdb.connect(DB_FILE)
    conn.execute(f"PRAGMA threads={os.cpu_count()};")

    conn.execute(f"""
        CREATE TABLE old_table AS
        SELECT *,
               ShopCode || '_' || Barcode AS Key
        FROM read_csv_auto('{old_file_path}', delim='\t', ignore_errors=true)
    """)

    conn.execute(f"""
        CREATE TABLE new_table AS
        SELECT *,
               ShopCode || '_' || Barcode AS Key
        FROM read_csv_auto('{new_file_path}', delim='\t', ignore_errors=true)
    """)

    conn.close()

# =====================================================
# WORKER
# =====================================================
def compare_chunk(args):
    keys, compare_cols, progress_counter = args

    conn = duckdb.connect(DB_FILE)

    output_file = f"temp_{os.getpid()}.csv"
    first_write = not os.path.exists(output_file)

    for key in keys:
        o_rows = conn.execute(
            "SELECT * FROM old_table WHERE Key = ?", [key]
        ).fetchdf()

        n_rows = conn.execute(
            "SELECT * FROM new_table WHERE Key = ?", [key]
        ).fetchdf()

        used_o, used_n = set(), set()

        def row_similarity(o, n):
            return (
                similarity(normalize_text(o["Description"]),
                           normalize_text(n["Description"])) * 0.7 +
                similarity(normalize_text(o["DeptDescription"]),
                           normalize_text(n["DeptDescription"])) * 0.3
            )

        pairs = []
        for oi, o in o_rows.iterrows():
            for ni, n in n_rows.iterrows():
                pairs.append((row_similarity(o, n), oi, ni))

        pairs.sort(reverse=True)

        rows_out = []

        for sim, oi, ni in pairs:
            if oi in used_o or ni in used_n:
                continue

            used_o.add(oi)
            used_n.add(ni)

            rec = {"Key": key}
            o = o_rows.loc[oi]
            n = n_rows.loc[ni]

            for col in compare_cols:
                ov, nv = o.get(col), n.get(col)

                rec[f"Old_{col}"] = ov if pd.notna(ov) else "Not Found"
                rec[f"New_{col}"] = nv if pd.notna(nv) else "Not Found"

                if pd.isna(ov) or pd.isna(nv):
                    rec[f"{col}_Check"] = "Not Found"
                elif col in ["Qty", "Value"]:
                    rec[f"{col}_Check"] = (
                        "Match" if safe_float(ov) == safe_float(nv)
                        else "Mismatch"
                    )
                else:
                    rec[f"{col}_Check"] = (
                        "Match" if str(ov) == str(nv)
                        else "Mismatch"
                    )

            rec["Complete_Match"] = all(
                rec[f"{c}_Check"] == "Match" for c in compare_cols
            )

            rows_out.append(rec)

        if rows_out:
            pd.DataFrame(rows_out).to_csv(
                output_file,
                mode="a",
                index=False,
                header=first_write
            )
            first_write = False

        # update progress
        with progress_counter.get_lock():
            progress_counter.value += 1

    conn.close()
    return output_file

# =====================================================
# CONTROLLER
# =====================================================
def compare_files():
    global progress_counter, total_keys_global

    progress_frame.pack(fill="x", pady=10)
    progress_bar["value"] = 0
    progress_lbl.config(text="Initializing...")

    selected_optional = [opt_list.get(i) for i in opt_list.curselection()]
    compare_cols = MANDATORY_COLUMNS + selected_optional

    init_db()

    conn = duckdb.connect(DB_FILE)

    all_keys = conn.execute("""
        SELECT DISTINCT Key FROM old_table
        UNION
        SELECT DISTINCT Key FROM new_table
    """).fetchall()

    conn.close()

    all_keys = [k[0] for k in all_keys]
    total_keys_global = len(all_keys)

    manager = mp.Manager()
    progress_counter = manager.Value('i', 0)

    cpu = max(1, os.cpu_count() - 1)
    chunk_size = math.ceil(len(all_keys) / cpu)
    chunks = [all_keys[i:i + chunk_size] for i in range(0, len(all_keys), chunk_size)]

    pool = mp.Pool(cpu)

    async_results = [
        pool.apply_async(compare_chunk, ((chunk, compare_cols, progress_counter),))
        for chunk in chunks
    ]

    pool.close()

    temp_files = []

    def poll():
        done = progress_counter.value
        percent = (done / total_keys_global) * 100 if total_keys_global else 0

        progress_bar["value"] = percent
        progress_bar["maximum"] = 100
        progress_lbl.config(text=f"{percent:.2f}% ({done}/{total_keys_global})")

        for r in async_results[:]:
            if r.ready():
                try:
                    temp_files.append(r.get())
                except Exception as e:
                    print("Worker error:", e)
                async_results.remove(r)

        if async_results:
            root.after(100, poll)
        else:
            pool.join()
            finalize_files(compare_cols)

    poll()

# =====================================================
# FINALIZE
# =====================================================
def finalize_files(compare_cols):
    files = glob.glob("temp_*.csv")

    comp_df = pd.concat((pd.read_csv(f) for f in files), ignore_index=True)

    check_cols = [f"{c}_Check" for c in compare_cols]

    # -------------------------------------------------
    # Apply comparison mode filter (RESTORED)
    # -------------------------------------------------
    if mode_var.get() == "MISMATCH":
        mask = (
            comp_df[check_cols].eq("Mismatch").any(axis=1) |
            comp_df[check_cols].eq("Not Found").any(axis=1)
        )
        output_df = comp_df.loc[mask]
    else:
        output_df = comp_df

    output_df.to_csv(
        os.path.join(output_dir, "Comparison.csv"),
        index=False
    )

    # cleanup temp files
    for f in files:
        os.remove(f)

    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)

    progress_lbl.config(text="✅ Completed")
    messagebox.showinfo("SirpairIQ", "Comparison completed successfully")
# =====================================================
# LOADERS
# =====================================================
def load_old():
    global old_file_path, output_dir
    p = filedialog.askopenfilename()
    if p:
        old_file_path = p
        output_dir = os.path.dirname(p)
        old_lbl.config(text=f"Old File: {os.path.basename(p)} ✅")
        validate_ready()

def load_new():
    global new_file_path
    p = filedialog.askopenfilename()
    if p:
        new_file_path = p
        new_lbl.config(text=f"New File: {os.path.basename(p)} ✅")
        validate_ready()

def validate_ready():
    if old_file_path and new_file_path:
        compare_btn.config(state="normal")

# =====================================================
# UI (UNCHANGED)
# =====================================================
root = tk.Tk()
root.title("SirpairIQ")
root.geometry("960x740")
root.configure(bg=BG)

style = ttk.Style(root)
style.configure(
    "NIQ.Horizontal.TProgressbar",
    troughcolor="#e6e9f0",
    background=ACCENT,
    thickness=6
)

tk.Label(
    root, text="SirpairIQ",
    bg=BG, fg=ACCENT,
    font=("Segoe UI", 24, "bold")
).pack(pady=10)

card = tk.Frame(root, bg=CARD)
card.pack(fill="both", expand=True, padx=25, pady=10)

tk.Label(card, text="Mandatory Columns", bg=CARD, fg=TXT,
         font=("Segoe UI", 11, "bold")).pack(anchor="w", padx=20, pady=5)

tk.Label(card, text=", ".join(MANDATORY_COLUMNS),
         bg=CARD, fg=MUTED, wraplength=880).pack(anchor="w", padx=20)

tk.Label(card, text="Optional Columns (Multi-Select)", bg=CARD, fg=TXT,
         font=("Segoe UI", 11, "bold")).pack(anchor="w", padx=20, pady=10)

opt_list = tk.Listbox(
    card, selectmode=tk.MULTIPLE, height=10,
    bg=BG, fg="black", selectbackground=ACCENT
)
for c in OPTIONAL_COLUMNS:
    opt_list.insert(tk.END, c)
opt_list.pack(fill="x", padx=20)

old_lbl = tk.Label(card, text="Old File: Not selected", bg=CARD, fg=MUTED)
new_lbl = tk.Label(card, text="New File: Not selected", bg=CARD, fg=MUTED)
old_lbl.pack(anchor="w", padx=20, pady=5)
new_lbl.pack(anchor="w", padx=20)

tk.Button(card, text="Load Old File", bg=ACCENT, fg="white",
          command=load_old).pack(pady=4)

tk.Button(card, text="Load New File", bg=ACCENT, fg="white",
          command=load_new).pack(pady=4)

compare_btn = tk.Button(
    card, text="Compare Files",
    bg=ACCENT, fg="white",
    state="disabled",
    command=compare_files
)
compare_btn.pack(pady=10)

# --- Comparison Mode ---
mode_var = tk.StringVar(value="FULL")

tk.Label(card, text="Comparison Mode", bg=CARD, fg=TXT,
         font=("Segoe UI", 10, "bold")).pack(anchor="w", padx=20, pady=(10, 2))

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



progress_frame = tk.Frame(card, bg=CARD)
progress_bar = ttk.Progressbar(progress_frame, style="NIQ.Horizontal.TProgressbar")
progress_lbl = tk.Label(progress_frame, bg=CARD, fg=MUTED)
progress_bar.pack(fill="x", padx=20)
progress_lbl.pack()
progress_frame.pack(fill="x", pady=10)
progress_frame.pack_forget()

if __name__ == "__main__":
    mp.freeze_support()
    root.mainloop()
