import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import duckdb
import os
import threading
from difflib import SequenceMatcher
import time
import csv

# =====================================================
# CONTROL FLAGS
# =====================================================
PAUSE = False
STOP = False
START_TIME = None

# =====================================================
# CONFIG
# =====================================================
BG = "#ffffff"
CARD = "#142A3E"
TXT = "#E6EEF5"
MUTED = "#9BB3C4"
ACCENT = "#2c6cf5"

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

# =====================================================
# HELPERS
# =====================================================
def normalize_text(s):
    return " ".join(str(s).split())

def similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()

def wait_if_paused():
    global PAUSE, STOP
    while PAUSE:
        time.sleep(0.2)
    if STOP:
        raise Exception("Stopped by user")

def update_status(msg):
    root.after(0, lambda: progress_lbl.config(text=msg))

def format_time(seconds):
    return time.strftime("%H:%M:%S", time.gmtime(seconds))

# =====================================================
# LOAD INTO DUCKDB (FAST)
# =====================================================
def load_into_duck(con, file_path, table_name):

    # Step 1: load raw
    con.execute(f"""
        CREATE OR REPLACE TABLE {table_name}_raw AS
        SELECT *
        FROM read_csv(
            '{file_path}',
            delim='\\t',
            header=False,
            ignore_errors=True
        )
    """)

    # Step 2: get actual column names
    cols = con.execute(f"PRAGMA table_info('{table_name}_raw')").fetchall()
    real_cols = [c[1] for c in cols]

    total_cols = len(real_cols)

    select_parts = []

    # Step 3: safe mapping
    for i, col_name in enumerate(ALL_COLUMNS):
        if i < total_cols:
            select_parts.append(f'"{real_cols[i]}" AS "{col_name}"')
        else:
            select_parts.append(f'NULL AS "{col_name}"')

    # Step 4: safe Qty & Value
    if total_cols > 15:
        select_parts.append(f'TRY_CAST("{real_cols[15]}" AS DOUBLE) AS Qty')
    else:
        select_parts.append("NULL AS Qty")

    if total_cols > 16:
        select_parts.append(f'TRY_CAST("{real_cols[16]}" AS DOUBLE) AS Value')
    else:
        select_parts.append("NULL AS Value")

    # Step 5: SAFE KEY CREATION
    if total_cols > 3:
        select_parts.append(
            f'CAST("{real_cols[2]}" AS VARCHAR) || \'_\' || CAST("{real_cols[3]}" AS VARCHAR) AS "Key"'
        )
    else:
        select_parts.append("NULL AS \"Key\"")

    # Step 6: final table
    con.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT {", ".join(select_parts)}
        FROM {table_name}_raw
    """)

    # Step 7: index (big speed boost)
    con.execute(f'CREATE INDEX idx_{table_name}_key ON {table_name}("Key")')






def safe_float(x):
    try:
        if x is None:
            return None
        x = str(x).replace(",", "").strip()
        if x == "":
            return None
        return float(x)
    except:
        return None
# =====================================================
# CORE ENGINE (ULTRA FAST)
# =====================================================


def run_compare():
    global START_TIME

    try:
        START_TIME = time.time()
        progress_frame.pack(fill="x", pady=10)

        db_path = os.path.join(output_dir,"sirpairiq.db")
        con = duckdb.connect(db_path)

        load_into_duck(con, old_file_path, "old_tbl")
        load_into_duck(con, new_file_path, "new_tbl")

        keys = con.execute("""
            SELECT DISTINCT "Key" FROM old_tbl
            UNION
            SELECT DISTINCT "Key" FROM new_tbl
        """).fetchall()

        keys = [k[0] for k in keys if k[0] is not None]

        progress_bar["maximum"] = len(keys)

        selected_optional = [opt_list.get(i) for i in opt_list.curselection()]
        compare_cols = MANDATORY_COLUMNS + selected_optional

        # -------- SUMMARY TRACKERS --------
        summary = {
            col: {"total": 0, "mismatch": 0}
            for col in compare_cols
        }

        # ================= FILE WRITE START =================
        output_file = os.path.join(output_dir, "Comparison.csv")

        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = None

            for i, key in enumerate(keys, 1):
                wait_if_paused()

                o_rows = con.execute(
                    'SELECT * FROM old_tbl WHERE "Key"=?', [key]
                ).fetchall()

                n_rows = con.execute(
                    'SELECT * FROM new_tbl WHERE "Key"=?', [key]
                ).fetchall()

                used_o, used_n = set(), set()
                pairs = []

                o_norm = {
                    i: (normalize_text(r[6]), normalize_text(r[9]))
                    for i, r in enumerate(o_rows)
                }

                n_norm = {
                    i: (normalize_text(r[6]), normalize_text(r[9]))
                    for i, r in enumerate(n_rows)
                }

                for oi in o_norm:
                    for ni in n_norm:
                        sim = similarity(o_norm[oi][0], n_norm[ni][0]) * 0.7 + \
                              similarity(o_norm[oi][1], n_norm[ni][1]) * 0.3
                        pairs.append((sim, oi, ni))

                pairs.sort(reverse=True)

                final_pairs = []
                for _, oi, ni in pairs:
                    if oi not in used_o and ni not in used_n:
                        used_o.add(oi)
                        used_n.add(ni)
                        final_pairs.append((oi, ni))

                def process(o, n):
                    rec = {"Key": key}
                    for idx, col in enumerate(compare_cols):
                        ov = o[idx] if o else None
                        nv = n[idx] if n else None

                        rec[f"Old_{col}"] = ov if ov is not None else "Not Found"
                        rec[f"New_{col}"] = nv if nv is not None else "Not Found"

                        summary[col]["total"] += 1

                        if ov is None or nv is None:
                            rec[f"{col}_Check"] = "Not Found"

                        elif col in ["Qty", "Value"]:
                            of = safe_float(ov)
                            nf = safe_float(nv)

                            if of is None or nf is None:
                                rec[f"{col}_Check"] = "Not Found"
                            else:
                                if of == nf:
                                    rec[f"{col}_Check"] = "Match"
                                else:
                                    rec[f"{col}_Check"] = "Mismatch"
                                    summary[col]["mismatch"] += 1
                        else:
                            if str(ov) == str(nv):
                                rec[f"{col}_Check"] = "Match"
                            else:
                                rec[f"{col}_Check"] = "Mismatch"
                                summary[col]["mismatch"] += 1

                    rec["Complete_Match"] = all(
                        rec[f"{c}_Check"] == "Match" for c in compare_cols
                    )
                    return rec

                for oi, ni in final_pairs:
                    row = process(o_rows[oi], n_rows[ni])

                    if writer is None:
                        writer = csv.DictWriter(f, fieldnames=row.keys())
                        writer.writeheader()

                    writer.writerow(row)

                progress_bar["value"] = i

                elapsed = time.time() - START_TIME
                rate = i / elapsed if elapsed else 0
                eta = (len(keys) - i) / rate if rate else 0

                update_status(f"{i}/{len(keys)} | {format_time(elapsed)} | ETA {format_time(eta)}")

        # ================= SUMMARY WRITE =================
        summary_file = os.path.join(output_dir, "Enhanced_File_Level_Summary.csv")

        with open(summary_file, "w", newline="", encoding="utf-8") as sf:
            writer = csv.writer(sf)
            writer.writerow(["Column", "Total", "Mismatch"])

            for col in compare_cols:
                writer.writerow([
                    col,
                    summary[col]["total"],
                    summary[col]["mismatch"]
                ])

        elapsed = time.time() - START_TIME
        formatted_time = format_time(elapsed)

        update_status(f"Completed in {formatted_time} ✅")
        messagebox.showinfo("SirpairIQ", f"Completed in {formatted_time}")

    except Exception as e:
        messagebox.showerror("Error", str(e))





# =====================================================
# UI (EXACT SAME AS YOURS)
# =====================================================
root = tk.Tk()
root.title("SirpairIQ")
root.geometry("960x740")
root.configure(bg=BG)

tk.Label(root, text="SirpairIQ", bg=BG, fg=ACCENT,
    font=("Segoe UI", 24, "bold")).pack(pady=10)

card = tk.Frame(root, bg=CARD)
card.pack(fill="both", expand=True, padx=25, pady=10)

tk.Label(card, text="Mandatory Columns", bg=CARD, fg=TXT,
         font=("Segoe UI", 10, "bold")).pack(anchor="w", padx=20)

tk.Label(card, text=", ".join(MANDATORY_COLUMNS),
         bg=CARD, fg=MUTED, wraplength=880).pack(anchor="w", padx=20)

opt_list = tk.Listbox(card, selectmode=tk.MULTIPLE, height=10,
                      bg=BG, fg="black", selectbackground=ACCENT)
for c in OPTIONAL_COLUMNS:
    opt_list.insert(tk.END, c)
opt_list.pack(fill="x", padx=20, pady=10)

old_lbl = tk.Label(card, text="Old File: Not selected", bg=CARD, fg=MUTED)
new_lbl = tk.Label(card, text="New File: Not selected", bg=CARD, fg=MUTED)
old_lbl.pack(anchor="w", padx=20)
new_lbl.pack(anchor="w", padx=20)

btn_frame = tk.Frame(card, bg=CARD)
btn_frame.pack(pady=10)

def load_old():
    global old_file_path, output_dir
    p = filedialog.askopenfilename()
    if p:
        old_file_path = p
        output_dir = os.path.dirname(p)
        old_lbl.config(text=f"Old File: {os.path.basename(p)} ✅")
        validate_buttons()

def load_new():
    global new_file_path
    p = filedialog.askopenfilename()
    if p:
        new_file_path = p
        new_lbl.config(text=f"New File: {os.path.basename(p)} ✅")
        validate_buttons()

def start_compare():
    global STOP, PAUSE
    STOP = False
    PAUSE = False
    threading.Thread(target=run_compare).start()

def pause_resume():
    global PAUSE
    PAUSE = not PAUSE
    pause_btn.config(text="Resume" if PAUSE else "Pause")

def stop_process():
    global STOP
    STOP = True

def validate_buttons():
    if old_file_path and new_file_path:
        compare_btn.config(state="normal")
        pause_btn.config(state="normal")
        stop_btn.config(state="normal")

tk.Button(btn_frame, text="Load Old File", command=load_old).grid(row=0, column=0, padx=5)
tk.Button(btn_frame, text="Load New File", command=load_new).grid(row=0, column=1, padx=5)

compare_btn = tk.Button(btn_frame, text="Compare Files", command=start_compare, state="disabled")
compare_btn.grid(row=0, column=2, padx=5)

pause_btn = tk.Button(btn_frame, text="Pause", command=pause_resume, state="disabled")
pause_btn.grid(row=0, column=3, padx=5)

stop_btn = tk.Button(btn_frame, text="Stop", command=stop_process, state="disabled")
stop_btn.grid(row=0, column=4, padx=5)

mode_var = tk.StringVar(value="FULL")
tk.Radiobutton(card, text="Full Comparison", variable=mode_var, value="FULL",
               bg=CARD, fg=TXT, selectcolor=CARD).pack(anchor="w", padx=20)
tk.Radiobutton(card, text="Mismatch / Extra Only", variable=mode_var, value="MISMATCH",
               bg=CARD, fg=TXT, selectcolor=CARD).pack(anchor="w", padx=20)

progress_frame = tk.Frame(card, bg=CARD)
progress_bar = ttk.Progressbar(progress_frame)
progress_lbl = tk.Label(progress_frame, bg=CARD, fg=MUTED)
progress_bar.pack(fill="x", padx=20)
progress_lbl.pack()
progress_frame.pack(fill="x", pady=10)

root.mainloop()
