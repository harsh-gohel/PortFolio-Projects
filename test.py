import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import duckdb
import os
import threading
import tempfile
import shutil
from rapidfuzz.process import cdist
from rapidfuzz import fuzz

# =====================================================
# UI COLORS
# =====================================================
BG = "#ffffff"
CARD = "#142A3E"
TXT = "#E6EEF5"
MUTED = "#9BB3C4"
ACCENT = "#2c6cf5"

DB_FILE = "temp_compare.duckdb"

# =====================================================
# COLUMN ORDER (HEADERLESS SAFE)
# =====================================================
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

MANDATORY_COLUMNS = [
    "Period", "ShopCode", "Barcode",
    "Description", "DeptDescription",
    "Qty", "Value"
]

OPTIONAL_COLUMNS = [
    c for c in ALL_COLUMNS if c not in MANDATORY_COLUMNS and c != "BatchID"
]

old_file_path = None
new_file_path = None
output_dir = None

# =====================================================
# HELPERS
# =====================================================
def safe_float(v):
    try:
        return float(v)
    except:
        return None

def norm(x):
    return " ".join(str(x).lower().strip().split())

def safe_copy(path):
    try:
        path = os.path.abspath(path)
        temp_path = os.path.join(tempfile.gettempdir(), os.path.basename(path))
        shutil.copy2(path, temp_path)
        return temp_path
    except Exception as e:
        raise Exception(
            "Cannot access file.\n\n"
            "Please ensure:\n"
            "- File is NOT open in Excel\n"
            "- You have read permission\n\n"
            f"Details: {str(e)}"
        )

# =====================================================
# INIT DB
# =====================================================
def init_db():
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)

    conn = duckdb.connect(DB_FILE)
    conn.execute(f"PRAGMA threads={os.cpu_count()};")

    col_def = ", ".join([f'"{c}" VARCHAR' for c in ALL_COLUMNS])

    old_safe = safe_copy(old_file_path)
    new_safe = safe_copy(new_file_path)

    try:
        conn.execute(f"CREATE TABLE old_table ({col_def});")
        conn.execute(f"""
            INSERT INTO old_table
            SELECT * FROM read_csv('{old_safe}', delim='\t', header=False);
        """)

        conn.execute(f"CREATE TABLE new_table ({col_def});")
        conn.execute(f"""
            INSERT INTO new_table
            SELECT * FROM read_csv('{new_safe}', delim='\t', header=False);
        """)

        conn.execute("ALTER TABLE old_table ADD COLUMN Key VARCHAR;")
        conn.execute("UPDATE old_table SET Key = ShopCode || '_' || Barcode;")

        conn.execute("ALTER TABLE new_table ADD COLUMN Key VARCHAR;")
        conn.execute("UPDATE new_table SET Key = ShopCode || '_' || Barcode;")

    except Exception as e:
        raise Exception(
            "File format issue.\n\n"
            "Ensure:\n"
            "- Tab-separated file\n"
            "- No header row\n"
            "- Correct column order\n\n"
            f"Details: {str(e)}"
        )

    return conn

# =====================================================
# CORE PROCESS
# =====================================================
def process(compare_cols):
    conn = init_db()

    try:
        keys = conn.execute("""
            SELECT DISTINCT Key FROM old_table
            UNION
            SELECT DISTINCT Key FROM new_table
        """).fetchall()

        keys = [k[0] for k in keys]
        total = len(keys)

        output_file = os.path.join(output_dir, "Comparison.csv")

        with open(output_file, "w", encoding="utf-8") as f:
            header_written = False

            for i, key in enumerate(keys):

                rows = conn.execute("""
                    SELECT o.*, n.*
                    FROM old_table o
                    JOIN new_table n
                    ON o.Key = n.Key
                    AND o.DeptDescription = n.DeptDescription
                    WHERE o.Key = ?
                """, [key]).fetchall()

                if not rows:
                    continue

                o_desc = [norm(r[6]) for r in rows]
                n_desc = [norm(r[len(ALL_COLUMNS)+6]) for r in rows]

                scores = cdist(o_desc, n_desc, scorer=fuzz.ratio)

                used_o = set()
                used_n = set()

                pairs = []
                for oi in range(len(o_desc)):
                    for ni in range(len(n_desc)):
                        pairs.append((scores[oi][ni], oi, ni))

                pairs.sort(reverse=True)

                for score, oi, ni in pairs:
                    if oi in used_o or ni in used_n:
                        continue

                    used_o.add(oi)
                    used_n.add(ni)

                    r = rows[oi]
                    o = r[:len(ALL_COLUMNS)]
                    n = r[len(ALL_COLUMNS):]

                    row = [key]
                    checks = []

                    for col in compare_cols:
                        idx = ALL_COLUMNS.index(col)

                        ov = o[idx] or "Not Found"
                        nv = n[idx] or "Not Found"

                        row.extend([ov, nv])

                        if ov == "Not Found" or nv == "Not Found":
                            chk = "Not Found"
                        elif col in ["Qty", "Value"]:
                            chk = "Match" if safe_float(ov) == safe_float(nv) else "Mismatch"
                        else:
                            chk = "Match" if str(ov) == str(nv) else "Mismatch"

                        row.append(chk)
                        checks.append(chk)

                    complete_match = all(c == "Match" for c in checks)

                    # MODE FILTER
                    if mode_var.get() == "MISMATCH":
                        if not any(c in ("Mismatch", "Not Found") for c in checks):
                            continue

                    row.append("TRUE" if complete_match else "FALSE")

                    if not header_written:
                        header = ["Key"]
                        for c in compare_cols:
                            header += [f"Old_{c}", f"New_{c}", f"{c}_Check"]
                        header.append("Complete_Match")
                        f.write(",".join(header) + "\n")
                        header_written = True

                    f.write(",".join(map(str, row)) + "\n")

                percent = (i + 1) / total * 100
                progress_bar["value"] = percent
                progress_lbl.config(text=f"{percent:.2f}% ({i+1}/{total})")
                root.update_idletasks()

        conn.close()

    except Exception as e:
        raise Exception("Processing failed.\n\n" + str(e))

# =====================================================
# THREAD WRAPPER
# =====================================================
def compare_files():
    try:
        progress_frame.pack(fill="x", pady=10)
        progress_bar["value"] = 0
        progress_lbl.config(text="Starting...")

        selected_optional = [opt_list.get(i) for i in opt_list.curselection()]
        compare_cols = MANDATORY_COLUMNS + selected_optional

        threading.Thread(target=run_process, args=(compare_cols,)).start()

    except Exception as e:
        messagebox.showerror("Error", str(e))

def run_process(compare_cols):
    try:
        process(compare_cols)
        progress_lbl.config(text="✅ Completed")
        messagebox.showinfo("SirpairIQ", "Comparison completed successfully")
    except Exception as e:
        messagebox.showerror("Error", str(e))

# =====================================================
# FILE LOADERS
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
style.configure("NIQ.Horizontal.TProgressbar",
                troughcolor="#e6e9f0",
                background=ACCENT,
                thickness=6)

tk.Label(root, text="SirpairIQ",
         bg=BG, fg=ACCENT,
         font=("Segoe UI", 24, "bold")).pack(pady=10)

card = tk.Frame(root, bg=CARD)
card.pack(fill="both", expand=True, padx=25, pady=10)

tk.Label(card, text="Mandatory Columns", bg=CARD, fg=TXT,
         font=("Segoe UI", 11, "bold")).pack(anchor="w", padx=20, pady=5)

tk.Label(card, text=", ".join(MANDATORY_COLUMNS),
         bg=CARD, fg=MUTED, wraplength=880).pack(anchor="w", padx=20)

tk.Label(card, text="Optional Columns (Multi-Select)", bg=CARD, fg=TXT,
         font=("Segoe UI", 11, "bold")).pack(anchor="w", padx=20, pady=10)

opt_list = tk.Listbox(card, selectmode=tk.MULTIPLE, height=10,
                      bg=BG, fg="black", selectbackground=ACCENT)
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

compare_btn = tk.Button(card, text="Compare Files",
                        bg=ACCENT, fg="white",
                        state="disabled",
                        command=compare_files)
compare_btn.pack(pady=10)

mode_var = tk.StringVar(value="FULL")

mode_frame = tk.Frame(card, bg=CARD)
mode_frame.pack(anchor="w", padx=20, pady=10)

tk.Radiobutton(mode_frame, text="Full Comparison",
               variable=mode_var, value="FULL",
               bg=CARD, fg=TXT, selectcolor=CARD).pack(side="left")

tk.Radiobutton(mode_frame, text="Mismatch / Extra Only",
               variable=mode_var, value="MISMATCH",
               bg=CARD, fg=TXT, selectcolor=CARD).pack(side="left", padx=15)

progress_frame = tk.Frame(card, bg=CARD)
progress_bar = ttk.Progressbar(progress_frame, style="NIQ.Horizontal.TProgressbar")
progress_lbl = tk.Label(progress_frame, bg=CARD, fg=MUTED)
progress_bar.pack(fill="x", padx=20)
progress_lbl.pack()
progress_frame.pack(fill="x", pady=10)
progress_frame.pack_forget()

if __name__ == "__main__":
    root.mainloop()
