import os
import threading
import tempfile
import heapq
import pandas as pd
from tkinter import *
from tkinter import filedialog, ttk, messagebox

# ================= CONFIG =================
HEADERS = [
    "Period","BatchID","ShopCode","fCrefSuffix","FCrefAltCode","Description",
    "FCrefDecriptionSuffix","DeptCode","DeptDescription","SupplierCode",
    "SuplierDescription","FCrefStatus","FDTGroup","FsubTag","Qty","Value",
    "fslot3","fslot4","fslot5","fslot6","fslot7","fslot8","fslot9","fslot10",
    "fcslot1","fcslot2","RMS_Retailer_SKU","Mercury_Store_ReceiptDescription",
    "Mercury_Decoder_Ring_Barcode","Barcode","URL"
]

MANDATORY = ["Period","ShopCode","Barcode","Description","DeptDescription","Qty","Value"]
SORT_COLS = ["ShopCode","Barcode","Description","DeptDescription"]
KEY_COLS = ["ShopCode","Barcode"]

CHUNK_SIZE = 200000  # tune based on RAM

# ================= GLOBAL STATE =================
old_file = ""
new_file = ""
paused = False

# ================= UI HELPERS =================
def select_old():
    global old_file
    old_file = filedialog.askopenfilename(filetypes=[("DETAIL files","*.DETAIL"),("All files","*.*")])
    old_label.config(text=os.path.basename(old_file))

def select_new():
    global new_file
    new_file = filedialog.askopenfilename(filetypes=[("DETAIL files","*.DETAIL"),("All files","*.*")])
    new_label.config(text=os.path.basename(new_file))

def toggle_pause():
    global paused
    paused = not paused
    pause_btn.config(text="Resume" if paused else "Pause")

def wait_if_paused():
    while paused:
        root.update()

# ================= EXTERNAL SORT (STREAMING) =================
def _chunk_sort_to_temp(input_file, temp_dir, prefix):
    paths = []
    for i, chunk in enumerate(pd.read_csv(input_file, sep="\t", names=HEADERS, chunksize=CHUNK_SIZE, dtype=str, keep_default_na=False)):
        chunk = chunk.sort_values(by=SORT_COLS, kind="mergesort")
        p = os.path.join(temp_dir, f"{prefix}_chunk_{i}.tsv")
        chunk.to_csv(p, sep="\t", index=False, header=False)
        paths.append(p)
    return paths

def _merge_chunks(paths, output_file):
    # k-way merge using heap
    files = [open(p, "r", encoding="utf-8", errors="ignore") for p in paths]

    def parse(line):
        parts = line.rstrip("\n").split("\t")
        # pad/truncate to HEADERS length
        if len(parts) < len(HEADERS):
            parts += [""] * (len(HEADERS) - len(parts))
        elif len(parts) > len(HEADERS):
            parts = parts[:len(HEADERS)]
        return parts

    def sort_key(parts):
        # tuple for SORT_COLS
        return tuple(parts[HEADERS.index(c)] for c in SORT_COLS)

    heap = []
    for idx, f in enumerate(files):
        line = f.readline()
        if line:
            parts = parse(line)
            heapq.heappush(heap, (sort_key(parts), idx, parts))

    with open(output_file, "w", encoding="utf-8") as out:
        while heap:
            _, idx, parts = heapq.heappop(heap)
            out.write("\t".join(parts) + "\n")
            nxt = files[idx].readline()
            if nxt:
                parts2 = parse(nxt)
                heapq.heappush(heap, (sort_key(parts2), idx, parts2))

    for f in files:
        f.close()
    for p in paths:
        try: os.remove(p)
        except: pass

def external_sort_stream(input_file, output_file, temp_dir, prefix):
    paths = _chunk_sort_to_temp(input_file, temp_dir, prefix)
    _merge_chunks(paths, output_file)

# ================= STREAMING GROUP READERS =================
def _parse_line(line):
    parts = line.rstrip("\n").split("\t")
    if len(parts) < len(HEADERS):
        parts += [""] * (len(HEADERS) - len(parts))
    elif len(parts) > len(HEADERS):
        parts = parts[:len(HEADERS)]
    return parts

def _key_from_parts(parts):
    return f"{parts[HEADERS.index('ShopCode')]}_{parts[HEADERS.index('Barcode')]}"

def _group_reader(path):
    f = open(path, "r", encoding="utf-8", errors="ignore")
    current_key = None
    group = []
    for line in f:
        parts = _parse_line(line)
        k = _key_from_parts(parts)
        if current_key is None:
            current_key = k
            group = [parts]
        elif k == current_key:
            group.append(parts)
        else:
            yield current_key, group
            current_key = k
            group = [parts]
    if current_key is not None:
        yield current_key, group
    f.close()

def _next_group(it):
    try:
        return next(it)
    except StopIteration:
        return None

# ================= COMPARISON (STREAMING, DUP-SAFE) =================
def compare_files():
    global paused

    if not old_file or not new_file:
        messagebox.showerror("Error", "Select both files")
        return

    # columns to compare
    selected_cols = [listbox.get(i) for i in listbox.curselection()]
    cols = list(dict.fromkeys(MANDATORY + selected_cols))

    # temp workspace
    temp_dir = tempfile.mkdtemp(prefix="cmp_")

    sorted_old = os.path.join(temp_dir, "old_sorted.tsv")
    sorted_new = os.path.join(temp_dir, "new_sorted.tsv")

    status.set("Sorting OLD (streaming)...")
    external_sort_stream(old_file, sorted_old, temp_dir, "old")

    status.set("Sorting NEW (streaming)...")
    external_sort_stream(new_file, sorted_new, temp_dir, "new")

    # output path = same directory as OLD file
    out_dir = os.path.dirname(old_file) if old_file else os.getcwd()
    output_file = os.path.join(out_dir, "comparison_output.csv")

    status.set("Comparing (streaming)...")

    # prepare writers
    with open(output_file, "w", encoding="utf-8") as out:

        # header
        header = ["Key"]
        for c in cols:
            header += [f"Old_{c}", f"New_{c}", f"{c}_check"]
        header.append("Complete_match")
        out.write(",".join(header) + "\n")

        # iterators
        it_old = _group_reader(sorted_old)
        it_new = _group_reader(sorted_new)

        g_old = _next_group(it_old)
        g_new = _next_group(it_new)

        processed = 0

        while g_old is not None or g_new is not None:
            wait_if_paused()

            if g_old is None:
                key = g_new[0]
                old_rows = []
                new_rows = g_new[1]
                g_new = _next_group(it_new)
            elif g_new is None:
                key = g_old[0]
                old_rows = g_old[1]
                new_rows = []
                g_old = _next_group(it_old)
            else:
                key_old, rows_old = g_old
                key_new, rows_new = g_new

                if key_old == key_new:
                    key = key_old
                    old_rows = rows_old
                    new_rows = rows_new
                    g_old = _next_group(it_old)
                    g_new = _next_group(it_new)
                elif key_old < key_new:
                    key = key_old
                    old_rows = rows_old
                    new_rows = []
                    g_old = _next_group(it_old)
                else:
                    key = key_new
                    old_rows = []
                    new_rows = rows_new
                    g_new = _next_group(it_new)

            max_len = max(len(old_rows), len(new_rows))

            for i in range(max_len):
                old_parts = old_rows[i] if i < len(old_rows) else [""] * len(HEADERS)
                new_parts = new_rows[i] if i < len(new_rows) else [""] * len(HEADERS)

                row_out = [key]
                complete = True

                for c in cols:
                    idx = HEADERS.index(c)
                    old_val = old_parts[idx]
                    new_val = new_parts[idx]

                    check = "match" if str(old_val) == str(new_val) else "mismatch"
                    if check == "mismatch":
                        complete = False

                    row_out += [str(old_val), str(new_val), check]

                row_out.append(str(complete))

                if output_option.get() == "Mismatch Only" and complete:
                    continue

                out.write(",".join(row_out) + "\n")

            processed += 1
            if processed % 100 == 0:
                prog_label.config(text=f"Groups processed: {processed}")
                progress['value'] = (processed % 1000) / 10  # rolling progress (streaming)
                root.update_idletasks()

    status.set(f"Done! Output saved at: {output_file}")
    messagebox.showinfo("Completed", f"Output saved at:\n{output_file}")

# ================= THREAD =================
def start_compare():
    threading.Thread(target=compare_files, daemon=True).start()

# ================= UI =================
root = Tk()
root.title("DETAIL File Comparator (Streaming, Duplicate-Safe)")
root.geometry("920x680")

Label(root, text="DETAIL File Comparator", font=("Arial", 16, "bold")).pack(pady=10)

frame = Frame(root)
frame.pack(pady=10)

Button(frame, text="Select Old File", width=20, command=select_old).grid(row=0, column=0, padx=10, pady=5)
old_label = Label(frame, text="No file selected")
old_label.grid(row=0, column=1)

Button(frame, text="Select New File", width=20, command=select_new).grid(row=1, column=0, padx=10, pady=5)
new_label = Label(frame, text="No file selected")
new_label.grid(row=1, column=1)

Label(root, text="Mandatory Columns:", font=("Arial", 10, "bold")).pack()
Label(root, text=", ".join(MANDATORY)).pack(pady=5)

Label(root, text="Select Additional Columns:").pack()
listbox = Listbox(root, selectmode=MULTIPLE, width=65, height=10)
for col in HEADERS:
    if col not in MANDATORY:
        listbox.insert(END, col)
listbox.pack(pady=5)

output_option = StringVar(value="Full Comparison")
opt_frame = Frame(root)
opt_frame.pack()
Radiobutton(opt_frame, text="Full Comparison", variable=output_option, value="Full Comparison").pack(side=LEFT, padx=10)
Radiobutton(opt_frame, text="Mismatch Only", variable=output_option, value="Mismatch Only").pack(side=LEFT, padx=10)

progress = ttk.Progressbar(root, length=520)
progress.pack(pady=10)

prog_label = Label(root, text="Groups processed: 0")
prog_label.pack()

status = StringVar(value="Idle")
Label(root, textvariable=status, fg="blue").pack(pady=5)

btn_frame = Frame(root)
btn_frame.pack(pady=10)

Button(btn_frame, text="Compare Files", width=20, command=start_compare).grid(row=0, column=0, padx=10)
pause_btn = Button(btn_frame, text="Pause", width=20, command=toggle_pause)
pause_btn.grid(row=0, column=1, padx=10)

root.mainloop()