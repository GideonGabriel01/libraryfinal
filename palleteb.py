from __future__ import annotations
"""
library_system_fixed.py
Single-file library system — Notebook (tabs) layout, CustomTkinter, bcrypt
Requirements: customtkinter, bcrypt
Optional: matplotlib, openpyxl, reportlab
Run: python library_system_fixed.py
"""
import os
import sys
import sqlite3
import threading
import random
import string
import smtplib
from datetime import datetime, timedelta
from typing import Optional, Any, Callable, List, Dict
import csv
import json
import re

# GUI
import tkinter as tk
from tkinter import ttk, messagebox, filedialog, Toplevel
import customtkinter as ctk

# bcrypt (required)
try:
    import bcrypt
except Exception:
    try:
        tk.Tk().withdraw()
        messagebox.showerror("Missing dependency", "bcrypt is required. Install with:\n\npip install bcrypt")
    except Exception:
        print("bcrypt is required. Install with: pip install bcrypt", file=sys.stderr)
    sys.exit(1)

# Optional libs
try:
    import openpyxl
except Exception:
    openpyxl = None
try:
    import matplotlib
    matplotlib.use("TkAgg")
    from matplotlib.figure import Figure
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
except Exception:
    Figure = None
try:
    from reportlab.lib.pagesizes import letter
    from reportlab.pdfgen import canvas as pdfcanvas
except Exception:
    pdfcanvas = None

# -------------------------
# Config & Palette
# -------------------------
DB_PATH = os.environ.get("LIB_DB_PATH", "library.db")
DB_TIMEOUT = 30
SQLITE_DT_FMT = "%Y-%m-%d %H:%M:%S"
DB_LOCK = threading.Lock()

PALETTE = {
    "bg": "#f0f6ff",
    "panel": "#ffffff",
    "accent": "#0ea5e9",
    "accent2": "#7c3aed",
    "muted": "#475569",
    "success": "#10b981",
    "warning": "#f59e0b",
    "danger": "#ef4444"
}

ctk.set_appearance_mode("Light")
ctk.set_default_color_theme("blue")

HEADER_FONT = ("Arial", 18, "bold")
SMALL_FONT = ("Segoe UI", 10)

# -------------------------
# Utilities
# -------------------------
def _conn():
    con = sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    con.row_factory = sqlite3.Row
    return con

def to_sql_dt(dt: datetime) -> str:
    return dt.strftime(SQLITE_DT_FMT)

def parse_any_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except Exception:
        try:
            return datetime.strptime(s, SQLITE_DT_FMT)
        except Exception:
            try:
                return datetime.strptime(s.replace("T", " "), SQLITE_DT_FMT)
            except Exception:
                return None

def _rand_code(n=6):
    return "".join(random.choices(string.digits, k=n))

EMAIL_RE = re.compile(r"^[\w\.-]+@[\w\.-]+\.\w+$")
def is_valid_email(email: str) -> bool:
    return bool(EMAIL_RE.match(email.strip()))

# -------------------------
# bcrypt helpers
# -------------------------
def _hash_pw_bcrypt(pw: str) -> str:
    return bcrypt.hashpw(pw.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

def _verify_pw_bcrypt(pw: str, h: str) -> bool:
    try:
        return bcrypt.checkpw(pw.encode("utf-8"), h.encode("utf-8"))
    except Exception:
        return False

# -------------------------
# DB init & helpers
# -------------------------
def init_db():
    con = _conn(); cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS books (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        author TEXT,
        category TEXT,
        isbn TEXT,
        copies_total INTEGER NOT NULL DEFAULT 1,
        copies_available INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS members (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        email TEXT,
        phone TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS loans (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        book_id INTEGER NOT NULL,
        member_id INTEGER NOT NULL,
        date_borrowed TEXT NOT NULL,
        date_due TEXT NOT NULL,
        date_returned TEXT,
        late_fee REAL NOT NULL DEFAULT 0.0,
        created_by TEXT,
        returned_by TEXT,
        FOREIGN KEY(book_id) REFERENCES books(id),
        FOREIGN KEY(member_id) REFERENCES members(id)
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        pw_hash TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'staff',
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS audit (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        actor TEXT,
        action TEXT NOT NULL,
        details TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS password_resets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL,
        code TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        used INTEGER NOT NULL DEFAULT 0
    )""")
    # default admin if none
    cur.execute("SELECT COUNT(*) FROM users")
    if cur.fetchone()[0] == 0:
        cur.execute("INSERT INTO users (username, pw_hash, role) VALUES (?,?,?)",
                    ("admin", _hash_pw_bcrypt("admin"), "admin"))
    cur.execute("INSERT OR IGNORE INTO settings(key, value) VALUES('late_fee_per_day','0.50')")
    con.commit(); con.close()

def log_audit(actor: Optional[str], action: str, details: Optional[str] = ""):
    try:
        con = _conn(); cur = con.cursor()
        cur.execute("INSERT INTO audit(actor, action, details) VALUES (?,?,?)", (actor, action, details))
        con.commit(); con.close()
    except Exception:
        pass

def get_setting(key: str) -> Optional[str]:
    con = _conn(); cur = con.cursor()
    cur.execute("SELECT value FROM settings WHERE key=?", (key,))
    r = cur.fetchone(); con.close()
    return r[0] if r else None

def set_setting(key: str, value: str, actor: Optional[str] = None):
    con = _conn(); cur = con.cursor()
    cur.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value))
    con.commit(); con.close()
    log_audit(actor, "set_setting", f"{key}={value}")

def _late_fee_per_day() -> float:
    v = get_setting("late_fee_per_day") or "0.5"
    try: return float(v)
    except: return 0.5

# -------------------------
# Users
# -------------------------
def create_user(username: str, password: str, role: str = "staff"):
    con = _conn(); cur = con.cursor()
    cur.execute("INSERT INTO users(username, pw_hash, role) VALUES(?,?,?)",
                (username, _hash_pw_bcrypt(password), role))
    con.commit(); con.close()
    log_audit(username, "create_user", role)

def verify_user(username: str, password: str) -> bool:
    con = _conn(); cur = con.cursor()
    cur.execute("SELECT pw_hash FROM users WHERE username=?", (username,))
    r = cur.fetchone(); con.close()
    return bool(r and _verify_pw_bcrypt(password, r[0]))

def get_user_role(username: str) -> Optional[str]:
    con = _conn(); cur = con.cursor()
    cur.execute("SELECT role FROM users WHERE username=?", (username,))
    r = cur.fetchone(); con.close()
    return r[0] if r else None

def change_user_password(username: str, current_pw: str, new_pw: str):
    con = _conn(); cur = con.cursor()
    cur.execute("SELECT pw_hash FROM users WHERE username=?", (username,))
    r = cur.fetchone()
    if not r or not _verify_pw_bcrypt(current_pw, r[0]):
        con.close()
        return {"success": False, "message": "Current password incorrect"}
    cur.execute("UPDATE users SET pw_hash=? WHERE username=?", (_hash_pw_bcrypt(new_pw), username))
    con.commit(); con.close()
    log_audit(username, "change_password", "self")
    return {"success": True, "message": "Password changed"}

def admin_reset_password(admin_user: str, target_username: str, new_pw: str):
    if get_user_role(admin_user) != "admin":
        return {"success": False, "message": "Not authorized"}
    con = _conn(); cur = con.cursor()
    cur.execute("SELECT id FROM users WHERE username=?", (target_username,))
    if not cur.fetchone():
        con.close()
        return {"success": False, "message": "User not found"}
    cur.execute("UPDATE users SET pw_hash=? WHERE username=?", (_hash_pw_bcrypt(new_pw), target_username))
    con.commit(); con.close()
    log_audit(admin_user, "admin_reset_password", target_username)
    return {"success": True, "message": "Password reset"}

def create_reset_code(username: str):
    con = _conn(); cur = con.cursor()
    cur.execute("SELECT id FROM users WHERE username=?", (username,))
    if not cur.fetchone():
        con.close()
        return {"success": False, "message": "User not found"}

    code = _rand_code()

    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cur.execute("""
        INSERT INTO password_resets(username, code, created_at, used)
        VALUES (?, ?, ?, 0)
    """, (username, code, created_at))

    con.commit(); con.close()

    log_audit(username, "create_reset_code", code)

    return {"success": True, "code": code}


def safe_parse_dt(s):
    """Parse timestamp safely in known formats."""
    if isinstance(s, datetime):
        return s
    
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except:
            pass
    
    return None   # let caller handle fallback


def get_reset_record(username: str, code: str):
    con = _conn(); cur = con.cursor()
    cur.execute("""
        SELECT id, created_at, used 
        FROM password_resets
        WHERE username=? AND code=?
        ORDER BY id DESC LIMIT 1
    """, (username, code))

    r = cur.fetchone()
    con.close()

    if not r:
        return None

    rid, created_at_raw, used = r

    # ---- FIXED: parse using safe parser ----
    created_dt = safe_parse_dt(created_at_raw)

    if not created_dt:
        # NEVER auto-expire incorrectly; assume created now (safe)
        created_dt = datetime.now()

    # ---- FIXED: correct expiration logic ----
    expires_at = created_dt + timedelta(minutes=30)
    expired = datetime.now() > expires_at

    return {"id": rid, "used": used == 1, "expired": expired}
def mark_reset_used(reset_id: int, new_pw: str, username: str):
    con = _conn(); cur = con.cursor()
    cur.execute("UPDATE password_resets SET used=1 WHERE id=?", (reset_id,))
    cur.execute("UPDATE users SET pw_hash=? WHERE username=?", (_hash_pw_bcrypt(new_pw), username))
    con.commit(); con.close()
    log_audit(username, "reset_password", f"via_code id={reset_id}")

# -------------------------
# SMTP helpers
# -------------------------
def _smtp_conn():
    host = get_setting("smtp_host") or ""
    port_s = get_setting("smtp_port") or "587"
    user = get_setting("smtp_user") or ""
    pw = get_setting("smtp_password") or ""
    try:
        port = int(port_s)
    except Exception:
        return {"success": False, "message": "SMTP port invalid"}
    if not host or not user:
        return {"success": False, "message": "SMTP not configured"}
    try:
        server = smtplib.SMTP(host, port, timeout=15)
        server.starttls()
        if pw:
            server.login(user, pw)
        return {"success": True, "server": server, "from": user}
    except Exception as e:
        return {"success": False, "message": str(e)}

def send_email(to_addr: str, subject: str, body: str):
    conn = _smtp_conn()
    if not conn.get("success"):
        return conn
    server = conn["server"]; from_addr = conn["from"]
    try:
        from email.mime.text import MIMEText
        msg = MIMEText(body, "plain", "utf-8")
        msg["From"] = from_addr
        msg["To"] = to_addr
        msg["Subject"] = subject
        server.sendmail(from_addr, [to_addr], msg.as_string())
        server.quit()
        return {"success": True}
    except Exception as e:
        try: server.quit()
        except Exception: pass
        return {"success": False, "message": str(e)}

def send_reset_code_email(username: str, recipient: str, code: str):
    subject = "Your library password reset code"
    body = f"Hello {username},\n\nUse this code to reset your password: {code}\nThis code expires in 30 minutes."
    return send_email(recipient, subject, body)

# -------------------------
# Books / Members / Loans
# -------------------------
def list_books(search_text: Optional[str]=None):
    con = _conn(); cur = con.cursor()
    if search_text:
        like = f"%{search_text}%"
        cur.execute("""SELECT id, title, author, category, isbn, copies_available FROM books
                       WHERE title LIKE ? OR author LIKE ? OR category LIKE ? OR isbn LIKE ?
                       ORDER BY id DESC""", (like, like, like, like))
    else:
        cur.execute("SELECT id, title, author, category, isbn, copies_available FROM books ORDER BY id DESC")
    rows = cur.fetchall(); con.close()
    return [{"id": r[0], "title": r[1], "author": r[2], "category": r[3], "isbn": r[4], "available": bool(r[5])} for r in rows]

def get_book(book_id: int):
    con = _conn(); cur = con.cursor()
    cur.execute("SELECT id, title, author, category, isbn, copies_available FROM books WHERE id=?", (book_id,))
    r = cur.fetchone(); con.close()
    if not r: return None
    return {"id": r[0], "title": r[1], "author": r[2], "category": r[3], "isbn": r[4], "available": bool(r[5])}

def add_book(title: str, author: str, category: str, isbn: str, copies:int=1, actor: Optional[str]=None):
    con = _conn(); cur = con.cursor()
    with DB_LOCK:
        cur.execute("INSERT INTO books(title,author,category,isbn,copies_total,copies_available) VALUES(?,?,?,?,?,?)",
                    (title, author, category, isbn, copies, copies))
        con.commit()
    con.close()
    log_audit(actor, "add_book", title)

def update_book(book_id: int, title: str, author: str, category: str, isbn: str, available: bool, actor: Optional[str]=None):
    con = _conn(); cur = con.cursor()
    with DB_LOCK:
        cur.execute("UPDATE books SET title=?, author=?, category=?, isbn=? WHERE id=?", (title, author, category, isbn, book_id))
        con.commit()
    con.close()
    log_audit(actor, "update_book", f"id={book_id}")

def delete_book(book_id: int, actor: Optional[str]=None):
    con = _conn(); cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM loans WHERE book_id=? AND date_returned IS NULL", (book_id,))
    if cur.fetchone()[0] > 0:
        con.close()
        return {"success": False, "message": "Cannot delete: book has active loans"}
    with DB_LOCK:
        cur.execute("DELETE FROM books WHERE id=?", (book_id,))
        con.commit()
    con.close()
    log_audit(actor, "delete_book", f"id={book_id}")
    return {"success": True, "message": "Book deleted"}

def list_members():
    con = _conn(); cur = con.cursor()
    cur.execute("SELECT id, name, email, phone FROM members ORDER BY id DESC")
    rows = cur.fetchall(); con.close()
    return [{"id": r[0], "name": r[1], "email": r[2], "phone": r[3]} for r in rows]

def add_member(name: str, email: str, phone: str = "", actor: Optional[str]=None):
    con = _conn(); cur = con.cursor()
    with DB_LOCK:
        cur.execute("INSERT INTO members(name,email,phone) VALUES(?,?,?)", (name, email, phone))
        con.commit()
    con.close()
    log_audit(actor, "add_member", name)

def search_members_by_text(txt: str):
    con = _conn(); cur = con.cursor()
    like = f"%{txt}%"
    cur.execute("SELECT id, name, email FROM members WHERE name LIKE ? OR email LIKE ? ORDER BY name ASC LIMIT 300",
                (like, like))
    rows = cur.fetchall(); con.close()
    return [{"id": r[0], "name": r[1], "email": r[2]} for r in rows]

def borrow_book(book_id: int, member_id: int, days_due: Optional[int]=None, actor: Optional[str]=None):
    con = _conn(); cur = con.cursor()
    cur.execute("SELECT copies_available FROM books WHERE id=?", (book_id,))
    r = cur.fetchone()
    if not r:
        con.close(); return {"success": False, "message": "Book not found"}
    if r[0] <= 0:
        con.close(); return {"success": False, "message": "Book not available"}
    now = datetime.now()
    due = now + timedelta(days=(int(days_due) if days_due else 14))
    now_str = to_sql_dt(now); due_str = to_sql_dt(due)
    with DB_LOCK:
        cur.execute("INSERT INTO loans(book_id, member_id, date_borrowed, date_due, created_by) VALUES(?,?,?,?,?)",
                    (book_id, member_id, now_str, due_str, actor))
        cur.execute("UPDATE books SET copies_available = copies_available - 1 WHERE id=?", (book_id,))
        con.commit()
    con.close()
    log_audit(actor, "borrow_book", f"book_id={book_id} member_id={member_id}")
    return {"success": True, "message": "Book borrowed"}

def return_book(loan_id: int, actor: Optional[str]=None):
    con = _conn(); cur = con.cursor()
    cur.execute("SELECT id, book_id, date_due FROM loans WHERE id=? AND date_returned IS NULL", (loan_id,))
    row = cur.fetchone()
    if not row:
        con.close(); return {"success": False, "message": "Active loan not found"}
    loan_id_, book_id, date_due = row["id"], row["book_id"], row["date_due"]
    now = datetime.now()
    due_dt = parse_any_dt(date_due)
    days_late = 0
    if due_dt:
        days_late = (now.date() - due_dt.date()).days
    fee = round(days_late * _late_fee_per_day(), 2) if days_late > 0 else 0.0
    now_str = to_sql_dt(now)
    with DB_LOCK:
        cur.execute("UPDATE loans SET date_returned=?, late_fee=?, returned_by=? WHERE id=?", (now_str, fee, actor, loan_id_))
        cur.execute("UPDATE books SET copies_available = copies_available + 1 WHERE id=?", (book_id,))
        con.commit()
    con.close()
    log_audit(actor, "return_book", f"loan_id={loan_id_} fee={fee}")
    return {"success": True, "message": f"Book returned. Late fee: {fee:.2f}"}

def list_loans(show_all: bool=False):
    con = _conn(); cur = con.cursor()
    if show_all:
        cur.execute("""
        SELECT l.id AS loan_id, b.title AS book_title, m.name AS member_name, l.member_id, l.date_borrowed, l.date_due, l.date_returned, l.late_fee
        FROM loans l JOIN books b ON l.book_id=b.id JOIN members m ON l.member_id=m.id
        ORDER BY l.id DESC
        """)
    else:
        cur.execute("""
        SELECT l.id AS loan_id, b.title AS book_title, m.name AS member_name, l.member_id, l.date_borrowed, l.date_due
        FROM loans l JOIN books b ON l.book_id=b.id JOIN members m ON l.member_id=m.id
        WHERE l.date_returned IS NULL
        ORDER BY l.id DESC
        """)
    rows = cur.fetchall(); con.close()
    if show_all:
        return [{"loan_id": r[0], "book_title": r[1], "member_name": r[2], "member_id": r[3], "date_borrowed": r[4], "date_due": r[5], "date_returned": r[6], "late_fee": r[7]} for r in rows]
    else:
        return [{"loan_id": r[0], "book_title": r[1], "member_name": r[2], "member_id": r[3], "date_borrowed": r[4], "date_due": r[5]} for r in rows]

def get_all_loans_for_export():
    con = _conn(); cur = con.cursor()
    cur.execute("""
    SELECT l.id AS loan_id, b.title AS book_title, m.name AS member_name, l.date_borrowed, l.date_due, l.date_returned, l.late_fee
    FROM loans l JOIN books b ON l.book_id=b.id JOIN members m ON l.member_id=m.id
    ORDER BY l.id DESC
    """)
    rows = cur.fetchall(); con.close()
    return [{"loan_id": r[0], "book_title": r[1], "member_name": r[2], "date_borrowed": r[3], "date_due": r[4], "date_returned": r[5], "late_fee": r[6]} for r in rows]

def query_audit(limit=500, since: Optional[datetime]=None):
    con = _conn(); cur = con.cursor()
    if since:
        cur.execute("SELECT id, actor, action, details, created_at FROM audit WHERE datetime(created_at) >= datetime(?) ORDER BY id DESC LIMIT ?", (since.isoformat(), limit))
    else:
        cur.execute("SELECT id, actor, action, details, created_at FROM audit ORDER BY id DESC LIMIT ?", (limit,))
    rows = cur.fetchall(); con.close()
    return [{"id": r[0], "actor": r[1], "action": r[2], "details": r[3], "created_at": r[4]} for r in rows]

def get_loan(loan_id: int):
    con = _conn(); cur = con.cursor()
    cur.execute("""
    SELECT l.id, l.book_id, b.title as book_title, l.member_id, m.name as member_name, l.date_borrowed, l.date_due, l.date_returned, l.late_fee
    FROM loans l
    JOIN books b ON l.book_id=b.id
    JOIN members m ON l.member_id=m.id
    WHERE l.id=?
    """, (loan_id,))
    r = cur.fetchone(); con.close()
    if not r: return None
    return {"id": r["id"], "book_id": r["book_id"], "book_title": r["book_title"], "member_id": r["member_id"], "member_name": r["member_name"], "date_borrowed": r["date_borrowed"], "date_due": r["date_due"], "date_returned": r["date_returned"], "late_fee": r["late_fee"]}

# -------------------------
# UI helpers
# -------------------------
from difflib import SequenceMatcher
def fuzzy_score(needle: str, haystack: str) -> float:
    if not needle: return 0.0
    return SequenceMatcher(None, needle.lower(), haystack.lower()).ratio()

def make_tree(parent, columns, headings):
    # use pack for the container and widgets inside it (no mixing with grid for that parent)
    container = tk.Frame(parent, bg=PALETTE["panel"])
    container.pack(fill="both", expand=True)
    tree = ttk.Treeview(container, columns=columns, show="headings", selectmode="browse")
    for col, head in zip(columns, headings):
        tree.heading(col, text=head)
        width = 140 if col not in ('title','book_title','name') else 300
        tree.column(col, width=width, anchor="w")
    vsb = ttk.Scrollbar(container, orient="vertical", command=tree.yview)
    hsb = ttk.Scrollbar(container, orient="horizontal", command=tree.xview)
    tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
    tree.pack(side="top", fill="both", expand=True)
    hsb.pack(side="bottom", fill="x")
    vsb.pack(side="right", fill="y")
    def tagger(iid, idx):
        try:
            if idx % 2 == 0: tree.item(iid, tags=("even",))
            else: tree.item(iid, tags=("odd",))
            tree.tag_configure("even", background="#ffffff")
            tree.tag_configure("odd", background="#f7fbff")
        except Exception:
            pass
    def auto_size():
        try:
            fnt = tk.font.nametofont("TkDefaultFont")
            for col in columns:
                w = fnt.measure(col) + 30
                tree.column(col, width=w)
        except Exception:
            pass
    tree.auto_size = auto_size  # type: ignore
    return tree, vsb, hsb, tagger

# -------------------------
# SearchableDropdown (modal)
# -------------------------
class SearchableDropdown:
    def __init__(self, parent, fetch_fn: Callable[[str], List[Any]], on_select: Callable[[Any], None], title="Select item", max_results=200):
        self.parent = parent; self.fetch_fn = fetch_fn; self.on_select = on_select; self.max_results = max_results
        self.win = Toplevel(parent)
        self.win.title(title); self.win.geometry("520x380"); self.win.resizable(False, False); self.win.grab_set()
        top = tk.Frame(self.win, bg=PALETTE["panel"]); top.pack(fill="x", padx=12, pady=(12,6))
        tk.Label(top, text=title, anchor="w", bg=PALETTE["panel"], fg=PALETTE["accent"], font=("Helvetica", 12, "bold")).pack(fill="x")
        self.entry_var = tk.StringVar(); self.entry = tk.Entry(top, textvariable=self.entry_var, font=("Segoe UI", 11)); self.entry.pack(fill="x", pady=(6,4)); self.entry.focus_set()
        self.entry.bind("<KeyRelease>", self.on_key); self.entry.bind("<Return>", lambda e: self.confirm_selection()); self.entry.bind("<Escape>", lambda e: self.close())
        lb_frame = tk.Frame(self.win, bg=PALETTE["panel"]); lb_frame.pack(fill="both", expand=True, padx=12, pady=(6,12))
        self.listbox = tk.Listbox(lb_frame, activestyle="none", selectmode="browse", font=("Segoe UI", 10)); self.listbox.pack(side="left", fill="both", expand=True)
        self.listbox.bind("<Double-Button-1>", lambda e: self.confirm_selection()); self.listbox.bind("<Return>", lambda e: self.confirm_selection()); self.listbox.bind("<Escape>", lambda e: self.close())
        scrollbar = tk.Scrollbar(lb_frame, orient="vertical", command=self.listbox.yview); scrollbar.pack(side="right", fill="y"); self.listbox.config(yscrollcommand=scrollbar.set)
        btn_frame = tk.Frame(self.win, bg=PALETTE["panel"]); btn_frame.pack(fill="x", padx=12, pady=(0,12))
        tk.Button(btn_frame, text="Select", command=self.confirm_selection, bg=PALETTE["accent"], fg="white").pack(side="left", padx=(0,6))
        tk.Button(btn_frame, text="Cancel", command=self.close).pack(side="right", padx=(6,0))
        self.results = []
        self.update_list("")
    def normalize_item(self, raw):
        if isinstance(raw, dict) and "label" in raw and "value" in raw:
            return raw["label"], raw["value"]
        if isinstance(raw, dict):
            label = raw.get("label") or f"{raw.get('id','?')} — {raw.get('name','')}"
            return label, raw
        return (str(raw), raw)
    def update_list(self, txt: str):
        txt = txt or ""
        fetched = self.fetch_fn(txt) or []
        normalized = [self.normalize_item(r) for r in fetched]
        scored = []
        for label, value in normalized:
            score = fuzzy_score(txt, label) if txt else 0.5
            scored.append((score, label, value))
        scored.sort(key=lambda x: (-x[0], x[1]))
        scored = scored[:self.max_results]
        self.results = [val for _, _, val in scored]
        self.listbox.delete(0, tk.END)
        for idx, (score, label, val) in enumerate([(s,l,v) for s,l,v in scored]):
            self.listbox.insert(tk.END, label)
            try:
                fg = PALETTE["muted"]
                if score > 0.8: fg = PALETTE["accent"]
                elif score > 0.6: fg = PALETTE["accent2"]
                self.listbox.itemconfig(idx, fg=fg)
            except Exception:
                pass
        if self.results:
            self.listbox.selection_clear(0, tk.END); self.listbox.selection_set(0); self.listbox.activate(0); self.listbox.see(0)
    def on_key(self, event):
        self.update_list(self.entry_var.get().strip())
    def confirm_selection(self):
        sel = self.listbox.curselection()
        if not sel:
            messagebox.showwarning("Select", "Please select an item."); return
        selected = self.results[sel[0]]
        try:
            self.on_select(selected)
        except Exception as e:
            messagebox.showerror("Error", f"Selection handler error: {e}")
        finally:
            self.close()
    def close(self):
        try: self.win.grab_release()
        except Exception: pass
        try: self.win.destroy()
        except Exception: pass

# -------------------------
# Main app — Notebook tabs
# -------------------------
class LibraryApp:
    def __init__(self, root: tk.Tk, username: str):
        self.root = root; self.username = username; self.role = get_user_role(username) or "staff"
        self.root.title(f"Library — {username} ({self.role})"); self.root.geometry("1150x750")
        # top controls (optional)
        topbar = ctk.CTkFrame(self.root); topbar.pack(side="top", fill="x")
        ctk.CTkLabel(topbar, text=f"BAC Library — user: {username} ({self.role})", font=("Helvetica", 14, "bold")).pack(side="left", padx=12, pady=8)
        ctk.CTkButton(topbar, text="Logout", fg_color=PALETTE["danger"], command=self.logout).pack(side="right", padx=12, pady=8)
        # Notebook
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill="both", expand=True)
        # create frames for tabs
        self.frames = {}
        names = ["Dashboard","Books","Borrow","Manage","Members","Loans","Transactions","Audit","Settings","Admin"]
        for n in names:
            f = tk.Frame(self.notebook, bg=PALETTE["panel"])
            self.notebook.add(f, text=n)
            key = n.lower()
            self.frames[key] = f
        # build each tab page
        self._build_dashboard(); self._build_books(); self._build_borrow(); self._build_manage()
        self._build_members(); self._build_loans(); self._build_transactions(); self._build_audit(); self._build_settings()
        if self.role == "admin":
            self._build_admin()
        # bind tab change for refresh
        self.notebook.bind("<<NotebookTabChanged>>", lambda e: self.on_tab_change())
        # initial refresh
        self.draw_dashboard(); self.load_books_table(); self.load_borrow_list(); self.load_manage_table(); self.load_members(); self.load_loans_table(); self.load_transactions(); self.load_audit()

    def on_tab_change(self):
        tab = self.notebook.tab(self.notebook.select(), "text").lower()
        if tab == "dashboard": self.draw_dashboard()
        elif tab == "books": self.load_books_table()
        elif tab == "borrow": self.load_borrow_list()
        elif tab == "manage": self.load_manage_table()
        elif tab == "members": self.load_members()
        elif tab == "loans": self.load_loans_table()
        elif tab == "transactions": self.load_transactions()
        elif tab == "audit": self.load_audit()
        elif tab == "settings": self.load_settings()

    def logout(self):
        if not messagebox.askyesno("Confirm", "Logout and return to login?"): return
        try: self.root.destroy()
        except Exception: pass
        try: os.execl(sys.executable, sys.executable, *sys.argv)
        except Exception: sys.exit(0)

    # -------------------------
    # Dashboard tab
    # -------------------------
    def _build_dashboard(self):
        f = self.frames["dashboard"]
        # clear
        for w in f.winfo_children(): w.destroy()
        top = ctk.CTkFrame(f); top.pack(fill="x", padx=12, pady=12)
        ctk.CTkLabel(top, text="Dashboard", font=HEADER_FONT).pack(side="left")
        ctk.CTkButton(top, text="Refresh", width=100, command=self.draw_dashboard).pack(side="right", padx=12)
        metrics = ctk.CTkFrame(f); metrics.pack(fill="x", padx=12, pady=(6,12))
        self.lbl_books = ctk.CTkLabel(metrics, text="Books: -", font=("Inter",14,"bold")); self.lbl_members = ctk.CTkLabel(metrics, text="Members: -", font=("Inter",14,"bold")); self.lbl_active = ctk.CTkLabel(metrics, text="Active loans: -", font=("Inter",14,"bold"))
        self.lbl_books.grid(row=0,column=0,padx=10); self.lbl_members.grid(row=0,column=1,padx=10); self.lbl_active.grid(row=0,column=2,padx=10)
        chart_frame = ctk.CTkFrame(f); chart_frame.pack(fill="both", expand=True, padx=12, pady=12)
        if Figure:
            self.fig = Figure(figsize=(7,3), dpi=90); self.ax = self.fig.add_subplot(111)
            self.canvas = FigureCanvasTkAgg(self.fig, master=chart_frame); self.canvas.get_tk_widget().pack(fill="both", expand=True)
        else:
            ctk.CTkLabel(chart_frame, text="Matplotlib not available").pack(padx=12, pady=12)
        bottom = ctk.CTkFrame(f); bottom.pack(fill="x", padx=12, pady=(0,12))
        ctk.CTkLabel(bottom, text="Recent loans:").pack(anchor="w", padx=6)
        self.recent_loans_box = tk.Listbox(bottom, height=4, font=SMALL_FONT); self.recent_loans_box.pack(fill="x", padx=6, pady=(6,0))

    def draw_dashboard(self):
        try:
            con = _conn(); cur = con.cursor()
            cur.execute("SELECT COUNT(*) FROM books"); books_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM members"); members_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM loans WHERE date_returned IS NULL"); active_loans = cur.fetchone()[0]
            con.close()
            self.lbl_books.configure(text=f"Books: {books_count}"); self.lbl_members.configure(text=f"Members: {members_count}"); self.lbl_active.configure(text=f"Active loans: {active_loans}")
        except Exception:
            pass
        if Figure:
            try:
                self.ax.clear()
                cur = _conn().cursor()
                cur.execute("SELECT strftime('%Y-%m', date_borrowed) m, COUNT(*) c FROM loans GROUP BY m ORDER BY m DESC LIMIT 6")
                rows = cur.fetchall()[::-1]
                labels = [r[0] for r in rows]; vals = [r[1] for r in rows]
                if labels:
                    self.ax.plot(labels, vals, marker="o"); self.ax.fill_between(labels, vals, alpha=0.12)
                self.ax.set_title("Loans (recent months)")
                self.canvas.draw()
            except Exception:
                pass
        try:
            self.recent_loans_box.delete(0, tk.END)
            for r in list_loans(show_all=True)[:8]:
                b = parse_any_dt(r.get("date_borrowed")); s = b.strftime("%Y-%m-%d") if b else ""
                self.recent_loans_box.insert(tk.END, f"{r['book_title']} — {r['member_name']} ({s})")
            if not list_loans(show_all=True):
                self.recent_loans_box.insert(tk.END, "No recent loans")
        except Exception:
            pass

    # -------------------------
    # Books tab (search)
    # -------------------------
    def _build_books(self):
        f = self.frames["books"]
        for w in f.winfo_children(): w.destroy()
        top = ctk.CTkFrame(f); top.pack(fill="x", padx=12, pady=8)
        ctk.CTkLabel(top, text="Search Books", font=HEADER_FONT).pack(side="left")
        bar = ctk.CTkFrame(f); bar.pack(fill="x", padx=12, pady=(6,8))
        self.search_var = ctk.StringVar()
        ctk.CTkEntry(bar, textvariable=self.search_var, placeholder_text="Search by title/author/category/ISBN").pack(side="left", fill="x", expand=True, padx=(8,6))
        ctk.CTkButton(bar, text="Search", width=100, command=lambda: self.load_books_table(self.search_var.get().strip())).pack(side="left", padx=6)
        ctk.CTkButton(bar, text="Refresh", width=100, command=lambda: self.load_books_table(self.search_var.get().strip())).pack(side="left", padx=6)
        table_frame = tk.Frame(f, bg=PALETTE["panel"]); table_frame.pack(fill="both", expand=True, padx=12, pady=8)
        cols = ("id","title","author","category","isbn","available"); heads = ("ID","Title","Author","Category","ISBN","Available")
        self.books_tree, self.books_vsb, self.books_hsb, self.books_tagger = make_tree(table_frame, cols, heads)
        self.books_tree.bind("<Double-1>", self.on_books_double)
        self.books_tree.bind("<Button-3>", self.on_books_right_click)

    def load_books_table(self, search_text: Optional[str]=None):
        try: self.books_tree.delete(*self.books_tree.get_children())
        except Exception: pass
        rows = list_books(search_text)
        for idx, r in enumerate(rows):
            vals = (r["id"], r["title"], r.get("author",""), r.get("category",""), r.get("isbn",""), "Yes" if r["available"] else "No")
            iid = self.books_tree.insert("", "end", values=vals)
            try: self.books_tagger(iid, idx)
            except: pass
        try: self.books_tree.auto_size()
        except: pass

    def on_books_double(self, event):
        sel = self.books_tree.selection()
        if not sel: return
        vals = self.books_tree.item(sel[0], "values")
        try:
            book_id = int(vals[0]); self.open_manage_edit(book_id)
            # switch to Manage tab
            self.notebook.select(self.frames["manage"])
        except Exception:
            pass

    def on_books_right_click(self, event):
        iid = self.books_tree.identify_row(event.y)
        if not iid: return
        self.books_tree.selection_set(iid)
        vals = self.books_tree.item(iid, "values"); book_id = int(vals[0])
        menu = tk.Menu(self.root, tearoff=0)
        menu.add_command(label="Borrow", command=lambda b=book_id: self.open_borrow_modal(b))
        menu.add_command(label="Edit", command=lambda b=book_id: (self.open_manage_edit(b), self.notebook.select(self.frames["manage"])))
        menu.add_command(label="Delete", command=lambda b=book_id: self._confirm_delete_book(b))
        try:
            menu.tk_popup(event.x_root, event.y_root)
        finally:
            try: menu.grab_release()
            except: pass

    # -------------------------
    # Borrow tab
    # -------------------------
    def _build_borrow(self):
        f = self.frames["borrow"]
        for w in f.winfo_children(): w.destroy()
        top = ctk.CTkFrame(f); top.pack(fill="x", padx=12, pady=8)
        ctk.CTkLabel(top, text="Borrow / Return", font=HEADER_FONT).pack(side="left")
        mid = ctk.CTkFrame(f); mid.pack(fill="x", padx=12, pady=(6,8))
        ctk.CTkLabel(mid, text="Search book to borrow:").pack(side="left", padx=6)
        self.borrow_search_var = ctk.StringVar(); ctk.CTkEntry(mid, textvariable=self.borrow_search_var, width=300).pack(side="left", padx=6)
        ctk.CTkButton(mid, text="Search & Borrow", command=self.search_and_borrow).pack(side="left", padx=6)
        table_frame = tk.Frame(f, bg=PALETTE["panel"]); table_frame.pack(fill="both", expand=True, padx=12, pady=8)
        cols = ("loan_id","book","member","borrowed","due"); heads = ("ID","Book","Member","Borrowed","Due")
        self.borrow_tree, self.borrow_vsb, self.borrow_hsb, self.borrow_tagger = make_tree(table_frame, cols, heads)
        self.borrow_tree.bind("<Double-1>", lambda e: self.open_return_from_table())

    def search_and_borrow(self):
        txt = self.borrow_search_var.get().strip()
        if not txt: messagebox.showwarning("Input","Enter search text"); return
        rows = list_books(txt)
        if not rows: messagebox.showinfo("No match","No book found"); return
        book = rows[0]
        self.open_borrow_modal(book["id"])

    def load_borrow_list(self):
        try: self.borrow_tree.delete(*self.borrow_tree.get_children())
        except Exception: pass
        rows = list_loans(show_all=False)
        for i, l in enumerate(rows):
            b = parse_any_dt(l.get("date_borrowed")); d = parse_any_dt(l.get("date_due"))
            b_s = b.strftime("%Y-%m-%d") if b else ""; d_s = d.strftime("%Y-%m-%d") if d else ""
            iid = self.borrow_tree.insert("", "end", values=(l.get("loan_id"), l.get("book_title"), l.get("member_name"), b_s, d_s))
            try: self.borrow_tagger(iid, i)
            except: pass

    # -------------------------
    # Manage tab
    # -------------------------
    def _build_manage(self):
        f = self.frames["manage"]
        for w in f.winfo_children(): w.destroy()
        top = ctk.CTkFrame(f); top.pack(fill="x", padx=12, pady=8)
        ctk.CTkLabel(top, text="Manage Books", font=HEADER_FONT).pack(side="left")
        split = tk.Frame(f, bg=PALETTE["panel"]); split.pack(fill="both", expand=True, padx=12, pady=8)
        left = tk.Frame(split, bg=PALETTE["panel"]); left.pack(side="left", fill="both", expand=True, padx=(0,8))
        right = tk.Frame(split, bg=PALETTE["panel"], width=360); right.pack(side="right", fill="y")
        # form on right
        frm = ctk.CTkFrame(right); frm.pack(fill="x", padx=12, pady=12)
        self.m_title = ctk.StringVar(); self.m_author = ctk.StringVar(); self.m_category = ctk.StringVar(); self.m_isbn = ctk.StringVar(); self.m_copies = ctk.IntVar(value=1); self.selected_book = ctk.IntVar(value=0)
        ctk.CTkLabel(frm,text="Title").grid(row=0,column=0,sticky="w",padx=6,pady=6); ctk.CTkEntry(frm,textvariable=self.m_title,width=260).grid(row=0,column=1,padx=6,pady=6)
        ctk.CTkLabel(frm,text="Author").grid(row=1,column=0,sticky="w",padx=6,pady=6); ctk.CTkEntry(frm,textvariable=self.m_author).grid(row=1,column=1,padx=6,pady=6)
        ctk.CTkLabel(frm,text="Category").grid(row=2,column=0,sticky="w",padx=6,pady=6); ctk.CTkEntry(frm,textvariable=self.m_category).grid(row=2,column=1,padx=6,pady=6)
        ctk.CTkLabel(frm,text="ISBN").grid(row=3,column=0,sticky="w",padx=6,pady=6); ctk.CTkEntry(frm,textvariable=self.m_isbn).grid(row=3,column=1,padx=6,pady=6)
        ctk.CTkLabel(frm,text="Copies").grid(row=4,column=0,sticky="w",padx=6,pady=6); ctk.CTkEntry(frm,textvariable=self.m_copies).grid(row=4,column=1,padx=6,pady=6)
        btns = ctk.CTkFrame(frm); btns.grid(row=5,column=0,columnspan=2,pady=12)
        ctk.CTkButton(btns,text="Add Book",command=self.add_book_action).pack(side="left",padx=6)
        ctk.CTkButton(btns,text="Update Selected",command=self.update_book_action).pack(side="left",padx=6)
        ctk.CTkButton(btns,text="Delete Selected",command=self.delete_book_action).pack(side="left",padx=6)
        ctk.CTkButton(btns,text="Clear",command=self.clear_manage_form).pack(side="left",padx=6)
        # table on left
        cols = ("id","title","author","category","isbn","available"); heads = ("ID","Title","Author","Category","ISBN","Available")
        self.manage_tree, self.manage_vsb, self.manage_hsb, self.manage_tagger = make_tree(left, cols, heads)
        self.manage_tree.bind("<Double-1>", lambda e: self._load_selected_book_to_form())

    def load_manage_table(self):
        try: self.manage_tree.delete(*self.manage_tree.get_children())
        except Exception: pass
        rows = list_books()
        for idx,r in enumerate(rows):
            iid = self.manage_tree.insert("", "end", values=(r["id"], r["title"], r.get("author",""), r.get("category",""), r.get("isbn",""), "Yes" if r["available"] else "No"))
            try: self.manage_tagger(iid, idx)
            except: pass
        try: self.manage_tree.auto_size()
        except: pass

    def _load_selected_book_to_form(self):
        sel = self.manage_tree.selection(); 
        if not sel: return
        vals = self.manage_tree.item(sel[0], "values")
        try:
            book_id = int(vals[0]); self.open_manage_edit(book_id)
        except Exception:
            pass

    def open_manage_edit(self, book_id:int):
        b = get_book(book_id)
        if not b: return
        self.m_title.set(b["title"]); self.m_author.set(b.get("author","")); self.m_category.set(b.get("category","")); self.m_isbn.set(b.get("isbn",""))
        self.selected_book.set(book_id)
        # switch to Manage tab
        self.notebook.select(self.frames["manage"])

    def add_book_action(self):
        title = self.m_title.get().strip()
        if not title: messagebox.showwarning("Input","Title required"); return
        try:
            add_book(title, self.m_author.get().strip(), self.m_category.get().strip(), self.m_isbn.get().strip(), copies=max(1,self.m_copies.get()), actor=self.username)
            messagebox.showinfo("Added","Book added"); self.clear_manage_form(); self.load_books_table(); self.load_manage_table()
        except Exception as e:
            messagebox.showerror("Error", f"Could not add book: {e}")

    def update_book_action(self):
        bid = self.selected_book.get()
        if not bid: messagebox.showwarning("Select","Select a book first"); return
        try:
            update_book(bid, self.m_title.get().strip(), self.m_author.get().strip(), self.m_category.get().strip(), self.m_isbn.get().strip(), True, actor=self.username)
            messagebox.showinfo("Updated","Book updated"); self.clear_manage_form(); self.load_books_table(); self.load_manage_table()
        except Exception as e:
            messagebox.showerror("Error", f"Could not update book: {e}")

    def delete_book_action(self):
        bid = self.selected_book.get()
        if not bid: messagebox.showwarning("Select","Select a book first"); return
        if not messagebox.askyesno("Confirm","Delete this book?"): return
        try:
            res = delete_book(bid, actor=self.username)
            if res.get("success"):
                messagebox.showinfo("Deleted",res["message"]); self.clear_manage_form(); self.load_books_table(); self.load_manage_table()
            else:
                messagebox.showerror("Could not delete",res.get("message"))
        except Exception as e:
            messagebox.showerror("Error", f"Could not delete book: {e}")

    def _confirm_delete_book(self, book_id:int):
        if not messagebox.askyesno("Confirm","Delete this book?"): return
        try:
            res = delete_book(book_id, actor=self.username)
            if res.get("success"):
                messagebox.showinfo("Deleted",res["message"]); self.load_books_table(); self.load_manage_table()
            else:
                messagebox.showerror("Could not delete",res.get("message"))
        except Exception as e:
            messagebox.showerror("Error", f"Could not delete book: {e}")

    def clear_manage_form(self):
        self.selected_book.set(0); self.m_title.set(""); self.m_author.set(""); self.m_category.set(""); self.m_isbn.set(""); self.m_copies.set(1)

    # -------------------------
    # Members tab
    # -------------------------
    def _build_members(self):
        f = self.frames["members"]
        for w in f.winfo_children(): w.destroy()
        top = ctk.CTkFrame(f); top.pack(fill="x", padx=12, pady=8)
        ctk.CTkLabel(top, text="Members", font=HEADER_FONT).pack(side="left")
        frm = ctk.CTkFrame(f); frm.pack(fill="x", padx=12, pady=6)
        self.mname_var = ctk.StringVar(); self.memail_var = ctk.StringVar(); self.mphone_var = ctk.StringVar()
        ctk.CTkLabel(frm, text="Name").grid(row=0,column=0,sticky="w",padx=6,pady=6); ctk.CTkEntry(frm, textvariable=self.mname_var, width=400).grid(row=0,column=1,padx=6,pady=6)
        ctk.CTkLabel(frm, text="Email").grid(row=1,column=0,sticky="w",padx=6,pady=6); ctk.CTkEntry(frm, textvariable=self.memail_var).grid(row=1,column=1,padx=6,pady=6)
        ctk.CTkLabel(frm, text="Phone").grid(row=2,column=0,sticky="w",padx=6,pady=6); ctk.CTkEntry(frm, textvariable=self.mphone_var).grid(row=2,column=1,padx=6,pady=6)
        ctk.CTkButton(frm, text="Add Member", command=self.add_member_action).grid(row=3,column=0,columnspan=2,pady=8)
        list_frame = tk.Frame(f, bg=PALETTE["panel"]); list_frame.pack(fill="both", expand=True, padx=12, pady=6)
        cols = ("id","name","email","phone"); heads = ("ID","Name","Email","Phone")
        self.member_tree, self.member_vsb, self.member_hsb, _ = make_tree(list_frame, cols, heads)
        self.load_members()

    def add_member_action(self):
        name = self.mname_var.get().strip(); email = self.memail_var.get().strip(); phone = self.mphone_var.get().strip()
        if not name: messagebox.showwarning("Input","Member name required"); return
        if email and not is_valid_email(email): messagebox.showwarning("Invalid","Email looks invalid"); return
        add_member(name,email,phone, actor=self.username); messagebox.showinfo("Added","Member added"); self.mname_var.set(""); self.memail_var.set(""); self.mphone_var.set(""); self.load_members()

    def load_members(self):
        try: self.member_tree.delete(*self.member_tree.get_children())
        except Exception: pass
        for m in list_members():
            self.member_tree.insert("", "end", values=(m["id"], m["name"], m.get("email",""), m.get("phone","")))

    # -------------------------
    # Loans tab
    # -------------------------
    def _build_loans(self):
        f = self.frames["loans"]
        for w in f.winfo_children(): w.destroy()
        top = ctk.CTkFrame(f); top.pack(fill="x", padx=12, pady=8)
        ctk.CTkLabel(top, text="Loans", font=HEADER_FONT).pack(side="left")
        ctk.CTkButton(top, text="Refresh", command=self.load_loans_table).pack(side="right", padx=6)
        mid = ctk.CTkFrame(f); mid.pack(fill="x", padx=12, pady=(4,8))
        self.show_all_loans_var = ctk.BooleanVar(value=False)
        ctk.CTkCheckBox(mid, text="Show returned loans", variable=self.show_all_loans_var, command=self.load_loans_table).pack(side="left", padx=6)
        table_frame = tk.Frame(f, bg=PALETTE["panel"]); table_frame.pack(fill="both", expand=True, padx=12, pady=8)
        cols = ("id","member","book","borrowed","due","returned"); heads = ("ID","Member","Book","Borrowed","Due","Returned")
        self.loans_tree, self.loans_vsb, self.loans_hsb, self.loans_tagger = make_tree(table_frame, cols, heads)
        self.loans_tree.bind("<Button-3>", self.on_loan_right_click)
        self.loans_tree.bind("<Double-1>", lambda e: self.open_return_from_table())
        self.load_loans_table()

    def load_loans_table(self):
        try: self.loans_tree.delete(*self.loans_tree.get_children())
        except Exception: pass
        rows = list_loans(show_all=self.show_all_loans_var.get())
        for i,l in enumerate(rows):
            b = parse_any_dt(l.get("date_borrowed")); d = parse_any_dt(l.get("date_due")); r = parse_any_dt(l.get("date_returned"))
            b_s = b.strftime("%Y-%m-%d") if b else ""; d_s = d.strftime("%Y-%m-%d") if d else ""; r_s = r.strftime("%Y-%m-%d") if r else ""
            iid = self.loans_tree.insert("", "end", values=(l.get("loan_id"), l.get("member_name"), l.get("book_title"), b_s, d_s, r_s))
            try: self.loans_tagger(iid, i)
            except: pass

    def on_loan_right_click(self, event):
        iid = self.loans_tree.identify_row(event.y)
        if not iid: return
        self.loans_tree.selection_set(iid)
        vals = self.loans_tree.item(iid,"values"); loan_id = int(vals[0])
        menu = tk.Menu(self.root, tearoff=0)
        menu.add_command(label="Return this loan", command=lambda: self.open_return_modal(loan_id))
        try:
            menu.tk_popup(event.x_root, event.y_root)
        finally:
            try: menu.grab_release()
            except: pass

    def open_return_from_table(self):
        sel = self.loans_tree.selection(); 
        if not sel: return
        vals = self.loans_tree.item(sel[0],"values"); loan_id = int(vals[0])
        self.open_return_modal(loan_id)

    # Borrow & Return modals
    def open_borrow_modal(self, book_id:int):
        b = get_book(book_id)
        if not b: messagebox.showerror("Error","Book not found"); return
        if not b["available"]: messagebox.showwarning("Unavailable","No copies available"); return
        win = ctk.CTkToplevel(self.root); win.title("Borrow"); win.geometry("420x320"); win.grab_set()
        ctk.CTkLabel(win, text=f"Borrow: {b['title']}", font=("Inter",16,"bold")).pack(pady=10)
        def fetch_members(txt):
            out = []
            for m in search_members_by_text(txt):
                out.append({"id": m["id"], "name": m["name"], "label": f"{m['id']} — {m['name']}", "value": m})
            return out
        def on_select_member(item):
            member_id = item["value"]["id"] if isinstance(item, dict) and "value" in item else (item.get("id") if isinstance(item, dict) else None)
            if not member_id:
                messagebox.showerror("Error","Could not parse member"); return
            res = borrow_book(book_id, member_id, actor=self.username)
            if res.get("success"):
                messagebox.showinfo("Borrowed", "Book borrowed"); win.destroy(); self.load_books_table(); self.load_manage_table(); self.load_loans_table(); self.load_borrow_list()
            else:
                messagebox.showerror("Error", res.get("message"))
        SearchableDropdown(win, fetch_members, on_select_member, title="Select Member")

    def open_return_modal(self, loan_id:int):
        loan = get_loan(loan_id)
        if not loan: messagebox.showerror("Error","Loan not found"); return
        if loan.get("date_returned"): messagebox.showinfo("Info","Already returned"); return
        win = ctk.CTkToplevel(self.root); win.title("Return"); win.geometry("420x240"); win.grab_set()
        ctk.CTkLabel(win, text=f"Return: {loan['book_title']}", font=("Inter",16,"bold")).pack(pady=12)
        def do_return():
            res = return_book(loan_id, actor=self.username)
            if res.get("success"):
                messagebox.showinfo("Returned", res.get("message")); win.destroy(); self.load_books_table(); self.load_manage_table(); self.load_loans_table(); self.load_borrow_list()
            else:
                messagebox.showerror("Error", res.get("message"))
        ctk.CTkButton(win, text="Return Book", width=200, command=do_return).pack(pady=18)

    # -------------------------
    # Transactions / export
    # -------------------------
    def _build_transactions(self):
        f = self.frames["transactions"]
        for w in f.winfo_children(): w.destroy()
        ctk.CTkLabel(f, text="Transactions / Export", font=HEADER_FONT).pack(pady=8)
        controls = ctk.CTkFrame(f); controls.pack(fill="x", padx=12, pady=6)
        ctk.CTkLabel(controls, text="From (YYYY-MM-DD)").grid(row=0,column=0,padx=6,pady=6)
        self.export_from = ctk.StringVar(); ctk.CTkEntry(controls, textvariable=self.export_from).grid(row=0,column=1,padx=6,pady=6)
        ctk.CTkLabel(controls, text="To (YYYY-MM-DD)").grid(row=0,column=2,padx=6,pady=6)
        self.export_to = ctk.StringVar(); ctk.CTkEntry(controls, textvariable=self.export_to).grid(row=0,column=3,padx=6,pady=6)
        ctk.CTkButton(controls, text="Export CSV", command=lambda: self.export_transactions_range("csv")).grid(row=0,column=4,padx=6)
        ctk.CTkButton(controls, text="Export XLSX", command=lambda: self.export_transactions_range("xlsx")).grid(row=0,column=5,padx=6)
        ctk.CTkButton(controls, text="Export JSON", command=lambda: self.export_transactions_range("json")).grid(row=0,column=6,padx=6)
        ctk.CTkButton(controls, text="Export PDF", command=lambda: self.export_transactions_range("pdf")).grid(row=0,column=7,padx=6)
        table_frame = tk.Frame(f, bg=PALETTE["panel"]); table_frame.pack(fill="both", expand=True, padx=12, pady=6)
        cols = ("loan_id","book","member","borrowed","due","returned","late_fee"); heads = ("Loan ID","Book","Member","Borrowed","Due","Returned","Late Fee")
        self.trans_tree, self.trans_vsb, self.trans_hsb, _ = make_tree(table_frame, cols, heads)

    def export_transactions_range(self, fmt="csv"):
        rows = get_all_loans_for_export()
        dfrom = self.export_from.get().strip(); dto = self.export_to.get().strip()
        if dfrom or dto:
            try:
                dfrom_dt = datetime.fromisoformat(dfrom) if dfrom else None
                dto_dt = datetime.fromisoformat(dto) if dto else None
            except Exception:
                messagebox.showerror("Invalid date","Dates must be YYYY-MM-DD"); return
            filtered = []
            for r in rows:
                borrowed = parse_any_dt(r["date_borrowed"])
                if dfrom_dt and borrowed and borrowed < dfrom_dt: continue
                if dto_dt and borrowed and borrowed > dto_dt: continue
                filtered.append(r)
            rows = filtered
        if fmt == "csv":
            filename = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files","*.csv")])
            if not filename: return
            with open(filename, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f); writer.writerow(["Loan ID","Book","Member","Borrowed","Due","Returned","Late Fee"])
                for r in rows:
                    borrowed = parse_any_dt(r["date_borrowed"]); due = parse_any_dt(r["date_due"]); returned = parse_any_dt(r["date_returned"])
                    writer.writerow([r["loan_id"], r["book_title"], r["member_name"], borrowed.strftime("%Y-%m-%d") if borrowed else "", due.strftime("%Y-%m-%d") if due else "", returned.strftime("%Y-%m-%d") if returned else "", r["late_fee"]])
            log_audit(self.username, "export_csv", f"exported {len(rows)} transactions"); messagebox.showinfo("Exported", f"CSV saved to {filename}")
        elif fmt == "xlsx":
            if openpyxl is None:
                messagebox.showerror("Missing lib", "openpyxl required for XLSX"); return
            filename = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel files","*.xlsx")])
            if not filename: return
            wb = openpyxl.Workbook(); ws = wb.active; ws.title="Transactions"
            ws.append(["Loan ID","Book","Member","Borrowed","Due","Returned","Late Fee"])
            for r in rows:
                borrowed = parse_any_dt(r["date_borrowed"]); due = parse_any_dt(r["date_due"]); returned = parse_any_dt(r["date_returned"])
                ws.append([r["loan_id"], r["book_title"], r["member_name"], borrowed.strftime("%Y-%m-%d") if borrowed else "", due.strftime("%Y-%m-%d") if due else "", returned.strftime("%Y-%m-%d") if returned else "", r["late_fee"]])
            wb.save(filename); log_audit(self.username, "export_xlsx", f"exported {len(rows)} transactions"); messagebox.showinfo("Exported", f"XLSX saved to {filename}")
        elif fmt == "json":
            filename = filedialog.asksaveasfilename(defaultextension=".json", filetypes=[("JSON files","*.json")])
            if not filename: return
            out = []
            for r in rows:
                out.append({
                    "loan_id": r["loan_id"], "book_title": r["book_title"], "member_name": r["member_name"],
                    "date_borrowed": r["date_borrowed"], "date_due": r["date_due"], "date_returned": r["date_returned"], "late_fee": r["late_fee"]
                })
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(out, f, indent=2, default=str)
            log_audit(self.username, "export_json", f"exported {len(rows)} transactions"); messagebox.showinfo("Exported", f"JSON saved to {filename}")
        elif fmt == "pdf":
            if pdfcanvas is None:
                messagebox.showerror("Missing lib", "reportlab required for PDF"); return
            filename = filedialog.asksaveasfilename(defaultextension=".pdf", filetypes=[("PDF files","*.pdf")])
            if not filename: return
            c = pdfcanvas.Canvas(filename, pagesize=letter); width, height = letter; y = height - 36
            c.setFont("Helvetica-Bold", 12); c.drawString(36, y, "Loans Export"); y -= 24
            for r in rows:
                borrowed = parse_any_dt(r["date_borrowed"]); due = parse_any_dt(r["date_due"]); returned = parse_any_dt(r["date_returned"])
                line = f"{r['loan_id']}: {r['book_title']} | {r['member_name']} | Borrowed:{borrowed.strftime('%Y-%m-%d') if borrowed else ''} Due:{due.strftime('%Y-%m-%d') if due else ''} Returned:{returned.strftime('%Y-%m-%d') if returned else ''} Fee:{r['late_fee']}"
                c.setFont("Helvetica", 9); c.drawString(36, y, line); y -= 14
                if y < 56: c.showPage(); y = height - 36
            c.save(); log_audit(self.username, "export_pdf", f"exported {len(rows)} transactions"); messagebox.showinfo("Exported", f"PDF saved to {filename}")

    def load_transactions(self):
        try: self.trans_tree.delete(*self.trans_tree.get_children())
        except Exception: pass
        rows = get_all_loans_for_export()
        for r in rows:
            borrowed = parse_any_dt(r["date_borrowed"]); due = parse_any_dt(r["date_due"]); returned = parse_any_dt(r["date_returned"])
            self.trans_tree.insert("", "end", values=(r["loan_id"], r["book_title"], r["member_name"], borrowed.strftime("%Y-%m-%d") if borrowed else "", due.strftime("%Y-%m-%d") if due else "", returned.strftime("%Y-%m-%d") if returned else "", r["late_fee"]))

    # -------------------------
    # Audit tab
    # -------------------------
    def _build_audit(self):
        f = self.frames["audit"]
        for w in f.winfo_children(): w.destroy()
        ctk.CTkLabel(f, text="Audit Log", font=HEADER_FONT).pack(pady=8)
        top = ctk.CTkFrame(f); top.pack(fill="x", padx=12, pady=6)
        ctk.CTkLabel(top, text="Since (YYYY-MM-DD)").grid(row=0,column=0,padx=6,pady=6)
        self.audit_since = ctk.StringVar(); ctk.CTkEntry(top, textvariable=self.audit_since).grid(row=0,column=1,padx=6)
        ctk.CTkButton(top, text="Refresh", command=self.load_audit).grid(row=0,column=2,padx=6)
        table_frame = tk.Frame(f, bg=PALETTE["panel"]); table_frame.pack(fill="both", expand=True, padx=12, pady=6)
        cols = ("id","actor","action","details","created_at"); heads = ("ID","Actor","Action","Details","When")
        self.audit_tree, self.audit_vsb, self.audit_hsb, _ = make_tree(table_frame, cols, heads)

    def load_audit(self):
        since_txt = self.audit_since.get().strip(); since = None
        if since_txt:
            try: since = datetime.fromisoformat(since_txt)
            except Exception: messagebox.showerror("Invalid", "Since date must be YYYY-MM-DD"); return
        rows = query_audit(limit=500, since=since)
        try: self.audit_tree.delete(*self.audit_tree.get_children())
        except Exception: pass
        for a in rows:
            self.audit_tree.insert("", "end", values=(a["id"], a["actor"], a["action"], a["details"], a["created_at"]))

    # -------------------------
    # Settings & Admin tabs
    # -------------------------
    def _build_settings(self):
        f = self.frames["settings"]
        for w in f.winfo_children(): w.destroy()
        ctk.CTkLabel(f, text="Settings", font=HEADER_FONT).pack(pady=8)
        sfrm = ctk.CTkFrame(f); sfrm.pack(padx=12, pady=12, fill="x")
        ctk.CTkLabel(sfrm, text="Late fee per day").grid(row=0,column=0,sticky="w",padx=6,pady=6)
        self.late_fee_var = ctk.DoubleVar(value=float(get_setting("late_fee_per_day") or 0.50))
        ctk.CTkEntry(sfrm, textvariable=self.late_fee_var).grid(row=0,column=1,padx=6,pady=6)
        ctk.CTkButton(sfrm, text="Save", command=self.save_settings).grid(row=0,column=2,padx=6)
        ctk.CTkLabel(sfrm, text="SMTP Host").grid(row=1,column=0,sticky="w",padx=6,pady=6)
        self.smtp_host_var = ctk.StringVar(value=get_setting("smtp_host") or ""); ctk.CTkEntry(sfrm, textvariable=self.smtp_host_var).grid(row=1,column=1,padx=6,pady=6)
        ctk.CTkLabel(sfrm, text="SMTP Port").grid(row=2,column=0,sticky="w",padx=6,pady=6); self.smtp_port_var = ctk.StringVar(value=get_setting("smtp_port") or "587"); ctk.CTkEntry(sfrm, textvariable=self.smtp_port_var).grid(row=2,column=1,padx=6,pady=6)
        ctk.CTkLabel(sfrm, text="SMTP User (from address)").grid(row=3,column=0,sticky="w",padx=6,pady=6); self.smtp_user_var = ctk.StringVar(value=get_setting("smtp_user") or ""); ctk.CTkEntry(sfrm, textvariable=self.smtp_user_var).grid(row=3,column=1,padx=6,pady=6)
        ctk.CTkLabel(sfrm, text="SMTP Password").grid(row=4,column=0,sticky="w",padx=6,pady=6); self.smtp_pw_var = ctk.StringVar(value=get_setting("smtp_password") or ""); ctk.CTkEntry(sfrm, textvariable=self.smtp_pw_var, show="*").grid(row=4,column=1,padx=6,pady=6)
        ctk.CTkButton(sfrm, text="Save SMTP Settings", command=self.save_smtp_settings).grid(row=5, column=0,padx=6,pady=(8,0))
        ctk.CTkButton(sfrm, text="Test SMTP (send to SMTP user)", command=self.test_smtp_connection).grid(row=5, column=1,padx=6,pady=(8,0))
        # Change password UI
        ctk.CTkLabel(sfrm, text="Change Password", font=("Arial",12,"bold")).grid(row=6,column=0,sticky="w",padx=6,pady=(12,6))
        ctk.CTkLabel(sfrm, text="Current").grid(row=7,column=0,sticky="w",padx=6,pady=6)
        self.current_pw = ctk.StringVar(); ctk.CTkEntry(sfrm, textvariable=self.current_pw, show="*").grid(row=7,column=1,padx=6)
        ctk.CTkLabel(sfrm, text="New").grid(row=8,column=0,sticky="w",padx=6,pady=6)
        self.new_pw = ctk.StringVar(); ctk.CTkEntry(sfrm, textvariable=self.new_pw, show="*").grid(row=8,column=1,padx=6)
        ctk.CTkLabel(sfrm, text="Confirm").grid(row=9,column=0,sticky="w",padx=6,pady=6)
        self.confirm_pw = ctk.StringVar(); ctk.CTkEntry(sfrm, textvariable=self.confirm_pw, show="*").grid(row=9,column=1,padx=6)
        ctk.CTkButton(sfrm, text="Change Password", command=self.change_password_action).grid(row=10,column=0,columnspan=2,pady=8)

    def save_settings(self):
        v = self.late_fee_var.get()
        try: float(v)
        except Exception:
            messagebox.showerror("Invalid","Late fee must be a number"); return
        set_setting("late_fee_per_day", str(v), actor=self.username); messagebox.showinfo("Saved","Settings saved"); log_audit(self.username,"save_settings",f"late_fee_per_day={v}")

    def save_smtp_settings(self):
        host = self.smtp_host_var.get().strip(); port = self.smtp_port_var.get().strip(); user = self.smtp_user_var.get().strip(); pw = self.smtp_pw_var.get().strip()
        if not host or not port or not user:
            if not messagebox.askyesno("Confirm","Host/Port/User empty — this will clear SMTP settings. Continue?"): return
        set_setting("smtp_host", host, actor=self.username); set_setting("smtp_port", port, actor=self.username); set_setting("smtp_user", user, actor=self.username); set_setting("smtp_password", pw, actor=self.username)
        messagebox.showinfo("Saved","SMTP settings saved"); log_audit(self.username, "save_smtp", f"host={host} user={user}")

    def test_smtp_connection(self):
        cfg_user = self.smtp_user_var.get().strip()
        if not cfg_user: messagebox.showwarning("No recipient","Set SMTP User before testing"); return
        def _do_test():
            res = send_email(cfg_user, "Library SMTP test", "This is a test email from Library System.")
            if res.get("success"): messagebox.showinfo("SMTP Test", "Test email sent successfully")
            else: messagebox.showerror("SMTP Test failed", f"Failed: {res.get('message')}")
        threading.Thread(target=_do_test, daemon=True).start()

    def change_password_action(self):
        cur = self.current_pw.get(); new = self.new_pw.get(); conf = self.confirm_pw.get()
        if not cur or not new or not conf: messagebox.showwarning("Input","All fields required"); return
        if new != conf: messagebox.showwarning("Mismatch","New passwords do not match"); return
        res = change_user_password(self.username, cur, new)
        if res["success"]:
            def _send_confirm():
                recipient = None
                for m in list_members():
                    if m.get("email") and m["email"].lower() == self.username.lower():
                        recipient = m["email"]; break
                    if m.get("name") and m["name"].lower() == self.username.lower():
                        recipient = m.get("email"); break
                if recipient:
                    send_email(recipient, "Your library password was changed", f"Hello {self.username},\n\nYour password was changed.")
            threading.Thread(target=_send_confirm, daemon=True).start()
            messagebox.showinfo("Changed", res["message"]); log_audit(self.username,"change_password","user changed own password"); self.current_pw.set(""); self.new_pw.set(""); self.confirm_pw.set("")
        else:
            messagebox.showerror("Error", res["message"])

    # -------------------------
    # Admin tab (if admin)
    # -------------------------
    def _build_admin(self):
        f = self.frames["admin"]
        for w in f.winfo_children(): w.destroy()
        ctk.CTkLabel(f, text="Admin", font=HEADER_FONT).pack(pady=8)
        frm = ctk.CTkFrame(f); frm.pack(fill="x", padx=12, pady=12)
        ctk.CTkLabel(frm, text="Create Admin User", font=("Arial",12,"bold")).grid(row=0,column=0,sticky="w",padx=6,pady=(6,12))
        self.new_admin_user = ctk.StringVar(); self.new_admin_pw = ctk.StringVar()
        ctk.CTkLabel(frm, text="Username").grid(row=1,column=0,sticky="w",padx=6); ctk.CTkEntry(frm, textvariable=self.new_admin_user).grid(row=1,column=1)
        ctk.CTkLabel(frm, text="Password").grid(row=2,column=0,sticky="w",padx=6); ctk.CTkEntry(frm, textvariable=self.new_admin_pw, show="*").grid(row=2,column=1)
        ctk.CTkButton(frm, text="Create Admin", command=self.create_admin_action).grid(row=3,column=0,columnspan=2,pady=8)
        ctk.CTkLabel(frm, text="Admin Reset User Password", font=("Arial",12,"bold")).grid(row=4,column=0,sticky="w",padx=6,pady=(12,6))
        self.reset_user_var = ctk.StringVar(); self.reset_pw_var = ctk.StringVar()
        ctk.CTkLabel(frm, text="Username").grid(row=5,column=0,sticky="w",padx=6); ctk.CTkEntry(frm, textvariable=self.reset_user_var).grid(row=5,column=1)
        ctk.CTkLabel(frm, text="New password").grid(row=6,column=0,sticky="w",padx=6); ctk.CTkEntry(frm, textvariable=self.reset_pw_var, show="*").grid(row=6,column=1)
        ctk.CTkButton(frm, text="Reset Password", command=self.admin_reset_action).grid(row=7,column=0,columnspan=2,pady=8)

    def create_admin_action(self):
        u = self.new_admin_user.get().strip(); p = self.new_admin_pw.get()
        if not u or not p: messagebox.showwarning("Input","Username & password required"); return
        try:
            create_user(u,p,role="admin"); messagebox.showinfo("Created", f"Admin '{u}' created"); self.new_admin_user.set(""); self.new_admin_pw.set("")
        except Exception as e:
            messagebox.showerror("Error", f"Could not create user: {e}")

    def admin_reset_action(self):
        target = self.reset_user_var.get().strip(); new = self.reset_pw_var.get()
        if not target or not new: messagebox.showwarning("Input","Username & new password required"); return
        res = admin_reset_password(self.username, target, new)
        if res["success"]:
            def _send_reset():
                recipient = None
                for m in list_members():
                    if (m.get("email") and m["email"].lower() == target.lower()) or (m.get("name") and m["name"].lower() == target.lower()):
                        recipient = m.get("email"); break
                if recipient:
                    send_email(recipient, "Your library password was reset by admin", f"Hello {target},\n\nYour password was reset by {self.username}.")
            threading.Thread(target=_send_reset, daemon=True).start()
            messagebox.showinfo("Reset", res["message"]); log_audit(self.username, "admin_reset_password", f"reset for {target}"); self.reset_user_var.set(""); self.reset_pw_var.set("")
        else:
            messagebox.showerror("Error", res["message"])

# -------------------------
# Signup / Forgot / Login
# -------------------------
class SignupWindow:
    def __init__(self, root, on_done: Callable[[],None]):
        self.root = root; self.on_done = on_done
        self.win = Toplevel(root); self.win.title("Sign up"); self.win.geometry("360x260"); self.win.resizable(False, False); self.win.grab_set()
        frm = ctk.CTkFrame(self.win); frm.pack(fill="both", expand=True, padx=12, pady=12)
        ctk.CTkLabel(frm, text="Create account", font=("Arial", 16, "bold")).grid(row=0, column=0, columnspan=2, pady=(6,12))
        ctk.CTkLabel(frm, text="Username (email)").grid(row=1, column=0, sticky="w", padx=6, pady=6)
        self.user_var = ctk.StringVar(); ctk.CTkEntry(frm, textvariable=self.user_var).grid(row=1, column=1, padx=6, pady=6)
        ctk.CTkLabel(frm, text="Password").grid(row=2, column=0, sticky="w", padx=6, pady=6)
        self.pw_var = ctk.StringVar(); ctk.CTkEntry(frm, textvariable=self.pw_var, show="*").grid(row=2, column=1, padx=6, pady=6)
        ctk.CTkLabel(frm, text="Name (optional)").grid(row=3, column=0, sticky="w", padx=6, pady=6)
        self.name_var = ctk.StringVar(); ctk.CTkEntry(frm, textvariable=self.name_var).grid(row=3, column=1, padx=6, pady=6)
        btns = ctk.CTkFrame(frm); btns.grid(row=4, column=0, columnspan=2, pady=12)
        ctk.CTkButton(btns, text="Sign up", command=self.signup).pack(side="left", padx=6)
        ctk.CTkButton(btns, text="Cancel", command=self.close).pack(side="left", padx=6)
    def signup(self):
        u = self.user_var.get().strip(); p = self.pw_var.get().strip(); name = self.name_var.get().strip()
        if not u or not p:
            messagebox.showwarning("Input","Username and password required"); return
        if "@" in u and not is_valid_email(u):
            messagebox.showwarning("Invalid","Username looks like email but invalid"); return
        try:
            create_user(u,p,role="staff")
            if name or is_valid_email(u):
                display_name = name if name else u
                add_member(display_name, u if is_valid_email(u) else "", actor=u)
            messagebox.showinfo("Created","Account created. You can login now.")
            self.close(); self.on_done()
        except Exception as e:
            messagebox.showerror("Error", f"Could not create user: {e}")
    def close(self):
        try: self.win.grab_release()
        except: pass
        try: self.win.destroy()
        except: pass

class ForgotPasswordWindow:
    def __init__(self, root):
        self.root = root
        self.win = Toplevel(root); self.win.title("Forgot password"); self.win.geometry("380x280"); self.win.resizable(False, False); self.win.grab_set()
        frm = ctk.CTkFrame(self.win); frm.pack(fill="both", expand=True, padx=12, pady=12)
        ctk.CTkLabel(frm, text="Reset password", font=("Arial", 16, "bold")).grid(row=0,column=0,columnspan=2,pady=(6,12))
        ctk.CTkLabel(frm, text="Username").grid(row=1,column=0,sticky="w",padx=6,pady=6)
        self.user_var = ctk.StringVar(); ctk.CTkEntry(frm, textvariable=self.user_var).grid(row=1,column=1,padx=6,pady=6)
        ctk.CTkButton(frm, text="Send code", command=self.send_code).grid(row=1,column=2,padx=6,pady=6)
        ctk.CTkLabel(frm, text="Code").grid(row=2,column=0,sticky="w",padx=6,pady=6)
        self.code_var = ctk.StringVar(); ctk.CTkEntry(frm, textvariable=self.code_var).grid(row=2,column=1,padx=6,pady=6)
        ctk.CTkLabel(frm, text="New password").grid(row=3,column=0,sticky="w",padx=6,pady=6)
        self.newpw_var = ctk.StringVar(); ctk.CTkEntry(frm, textvariable=self.newpw_var, show="*").grid(row=3,column=1,padx=6,pady=6)
        ctk.CTkButton(frm, text="Reset", command=self.reset_now).grid(row=4,column=0,columnspan=3,pady=12)
    def send_code(self):
        username = self.user_var.get().strip()
        if not username: messagebox.showwarning("Input","Enter username"); return
        rec = create_reset_code(username)
        if not rec.get("success"): messagebox.showerror("Error", rec.get("message","Failed")); return
        recipient = None
        for m in list_members():
            if m.get("email") and m["email"].lower() == username.lower():
                recipient = m["email"]; break
            if m.get("name") and m["name"].lower() == username.lower():
                recipient = m.get("email"); break
        if not recipient: messagebox.showwarning("No email","No member email found to send code"); return
        code = rec["code"]
        def _send():
            send_reset_code_email(username, recipient, code)
        threading.Thread(target=_send, daemon=True).start()
        messagebox.showinfo("Sent","Reset code sent to your email.")
    def reset_now(self):
        username = self.user_var.get().strip(); code = self.code_var.get().strip(); newpw = self.newpw_var.get().strip()
        if not username or not code or not newpw: messagebox.showwarning("Input","Fill all fields"); return
        rec = get_reset_record(username, code)
        if not rec: messagebox.showerror("Invalid","Code not found"); return
        if rec["used"]: messagebox.showerror("Invalid","Code already used"); return
        if rec["expired"]: messagebox.showerror("Expired","Code expired. Request a new one"); return
        mark_reset_used(rec["id"], newpw, username)
        messagebox.showinfo("Reset","Password has been reset. You can login.")
        try: log_audit(username, "reset_password", "via code")
        except Exception: pass
        self.win.destroy()

class LoginWindow:
    def __init__(self, root, on_success: Callable[[str], None]):
        self.on_success = on_success; self.root = root
        self.win = Toplevel(root); self.win.title("Library Login"); self.win.geometry("420x300"); self.win.resizable(False, False); self.win.grab_set()
        self.win.protocol("WM_DELETE_WINDOW", self.on_close)
        lbl = ctk.CTkLabel(self.win, text="Library Login", font=("Arial", 18, "bold")); lbl.pack(pady=(12,4))
        frm = ctk.CTkFrame(self.win); frm.pack(padx=12, pady=6, fill="x")
        ctk.CTkLabel(frm, text="Username").grid(row=0,column=0,sticky="w",padx=6,pady=6)
        self.user_var = ctk.StringVar(); ctk.CTkEntry(frm, textvariable=self.user_var).grid(row=0,column=1,padx=6,pady=6)
        ctk.CTkLabel(frm, text="Password").grid(row=1,column=0,sticky="w",padx=6,pady=6)
        self.pw_var = ctk.StringVar(); ctk.CTkEntry(frm, textvariable=self.pw_var, show="*").grid(row=1,column=1,padx=6,pady=6)
        btns = ctk.CTkFrame(self.win); btns.pack(fill="x", padx=12, pady=(6,6))
        ctk.CTkButton(btns, text="Login", command=self.try_login).pack(side="left", padx=6)
        ctk.CTkButton(btns, text="Exit", command=self.on_close).pack(side="right", padx=6)
        links = ctk.CTkFrame(self.win); links.pack(fill="x", padx=12, pady=(0,8))
        ctk.CTkButton(links, text="Sign up", command=self.open_signup).pack(side="left", padx=6)
        ctk.CTkButton(links, text="Forgot password", command=self.open_forgot).pack(side="left", padx=6)
        ctk.CTkLabel(self.win, text="(default admin/admin if first run)", font=("Arial",9,"italic")).pack(pady=(0,6))

    def open_signup(self): SignupWindow(self.root, on_done=lambda: None)
    def open_forgot(self): ForgotPasswordWindow(self.root)
    def try_login(self):
        username = self.user_var.get().strip(); password = self.pw_var.get()
        if not username or not password: messagebox.showwarning("Input","Enter username and password"); return
        ok = verify_user(username, password)
        if ok:
            role = get_user_role(username) or "staff"
            messagebox.showinfo("Welcome", f"Welcome, {username} ({role})")
            try: self.win.grab_release()
            except: pass
            try: self.win.destroy()
            except: pass
            self.on_success(username)
        else:
            messagebox.showerror("Login failed","Invalid username or password")
    def on_close(self):
        try: self.win.grab_release()
        except: pass
        try: self.win.destroy()
        except: pass
        try: self.root.quit()
        except: pass

# -------------------------
# Entrypoint
# -------------------------
def main():
    init_db()
    root = tk.Tk(); root.withdraw()
    def on_login_success(user):
        root.deiconify(); app = LibraryApp(root, user); root.mainloop()
    LoginWindow(root, on_login_success)
    root.mainloop()

if __name__ == "__main__":
    main()
