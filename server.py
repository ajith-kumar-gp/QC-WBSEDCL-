"""
Meter OCR Annotation Server  —  Production-ready version
=========================================================
Run:          python server.py
Annotator:    http://localhost:5000
Dashboard:    http://localhost:5000/dashboard
Exports:      ./exports/

WHAT SURVIVES A FULL SHUTDOWN:
  ✅ All annotations      → annotations.db  (SQLite on disk)
  ✅ Completed Excels     → exports/*.xlsx  (files on disk)
  ✅ Same image sample    → RANDOM_SEED=42  (deterministic every restart)
  ✅ User selection       → browser localStorage (per browser)
  ✅ Agency progress      → rebuilt from DB on first request

PERFORMANCE DESIGN:
  • UI boots with only agency metadata (~2 KB) — NOT 64k rows
  • Rows are fetched per-agency on demand and cached in the browser
  • Server caches sampled rows in memory after first load
  • SQLite runs in WAL mode (concurrent reads + 1 write, no blocking)
  • DB indexes on agency + row_id for fast lookups
  • Gzip compression on all API responses
  • Thread-safe DB connections (one per request)
"""

import os, hashlib, threading, time, gzip
from functools import wraps
import datetime

from flask import Flask, jsonify, request, send_from_directory, send_file, Response
import pandas as pd
import sqlite3

# ─────────────────────────────────────────────────────────
#  CONFIG  —  edit these
# ─────────────────────────────────────────────────────────
DATA_FILE         = "data.xlsx"      # or .xlsx
AGENCY_COLUMN     = "Agency"        # column name for agency/division
SAMPLE_PER_AGENCY = 150             # 150 or 200 images per agency
RANDOM_SEED       = 42              # never change this — keeps sample identical across restarts
DB_FILE           = "annotations.db"
EXPORTS_DIR       = "exports"
PORT              = 5000
HOST              = "0.0.0.0"

USERS = [
    {"id": f"u{i}", "name": f"User {i}", "color": "#5b8dee"}
    for i in range(1, 8)
]
# ─────────────────────────────────────────────────────────

app = Flask(__name__, static_folder=".", static_url_path="")
os.makedirs(EXPORTS_DIR, exist_ok=True)

# ─── GZIP MIDDLEWARE ──────────────────────────────────────
def gzip_response(f):
    """Decorator: gzip any JSON response if client accepts it."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        rv = f(*args, **kwargs)
        # only compress Response objects with JSON content
        if not isinstance(rv, Response):
            rv = app.make_response(rv)
        if (rv.content_type and 'json' in rv.content_type and
                'gzip' in request.headers.get('Accept-Encoding', '')):
            compressed = gzip.compress(rv.get_data())
            rv.set_data(compressed)
            rv.headers['Content-Encoding'] = 'gzip'
            rv.headers['Content-Length']   = len(compressed)
        return rv
    return wrapper

# ─── DATABASE ─────────────────────────────────────────────
# SQLite in WAL mode: multiple readers + 1 writer, no blocking.
# Each request gets its own connection (thread-safe).

def get_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")   # write-ahead logging
    conn.execute("PRAGMA synchronous=NORMAL") # fast but safe
    conn.execute("PRAGMA cache_size=-32000")  # 32 MB page cache
    conn.execute("PRAGMA temp_store=MEMORY")
    return conn

def init_db():
    with get_db() as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS annotations (
                row_id       TEXT PRIMARY KEY,
                agency       TEXT NOT NULL,
                consumer_id  TEXT,
                img_url      TEXT,
                label        TEXT DEFAULT '',
                notes        TEXT DEFAULT '',
                skipped      INTEGER DEFAULT 0,
                annotated_by TEXT DEFAULT '',
                created_at   TEXT,
                updated_at   TEXT
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS completed_agencies (
                agency       TEXT PRIMARY KEY,
                completed_at TEXT,
                excel_path   TEXT,
                total_rows   INTEGER
            )
        """)
        # Indexes — critical for speed on 64k rows
        db.execute("CREATE INDEX IF NOT EXISTS idx_ann_agency       ON annotations(agency)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_ann_by           ON annotations(annotated_by)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_ann_updated      ON annotations(updated_at DESC)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_ann_agency_label ON annotations(agency, label)")

        # Safe migrations for older databases
        for col, definition in [("annotated_by", "TEXT DEFAULT ''"),
                                 ("notes",        "TEXT DEFAULT ''")]:
            try:
                db.execute(f"ALTER TABLE annotations ADD COLUMN {col} {definition}")
            except Exception:
                pass
        db.commit()
    print("[db] Initialized with WAL mode + indexes")

# ─── DATA LOADING ─────────────────────────────────────────
# Loaded once at startup, stays in memory for the server's lifetime.
# On restart the same rows are produced (RANDOM_SEED is fixed).

_lock         = threading.Lock()
_rows_cache   = None   # flat list of all sampled rows
_agency_index = None   # dict: agency_name -> [row, ...]
_agency_names = None   # sorted list of agency names
_total_rows   = 0

def load_and_sample():
    global _rows_cache, _agency_index, _agency_names, _total_rows
    with _lock:
        if _rows_cache is not None:
            return _rows_cache

        t0 = time.time()
        print(f"[data] Loading {DATA_FILE} ...")
        ext = os.path.splitext(DATA_FILE)[1].lower()
        df  = (pd.read_excel(DATA_FILE, engine="openpyxl")
               if ext in (".xlsx", ".xls") else pd.read_csv(DATA_FILE))
        df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

        rename_map = {
            "actual_reading": "actual_reading",
            "actualreading": "actual_reading",

            "ai_based_meter_reading": "ai_meter_reading",
            "ai_meter_reading": "ai_meter_reading",

            "ai_based_confidence_level": "confidence_score",
            "confidence_score": "confidence_score",

            "consumerid": "consumer_id",
        }

        df.rename(columns=rename_map, inplace=True)

        for old, new in rename_map.items():
            if old in df.columns:
                df.rename(columns={old: new}, inplace=True)

        agency_col = next((c for c in df.columns if c.lower() == AGENCY_COLUMN.lower()), None)
        if not agency_col:
            raise ValueError(
                f"Agency column '{AGENCY_COLUMN}' not found.\n"
                f"Available columns: {list(df.columns)}"
            )

        parts = []
        for agency, group in df.groupby(agency_col, sort=True):
            if len(group) <= SAMPLE_PER_AGENCY:
                parts.append(group)
            else:
                parts.append(group.sample(n=SAMPLE_PER_AGENCY, random_state=RANDOM_SEED))

        sampled = pd.concat(parts, ignore_index=True)
        n_agencies = sampled[agency_col].nunique()
        print(f"[data] {len(sampled):,} rows across {n_agencies} agencies "
              f"({time.time()-t0:.1f}s)")

        rows, idx = [], {}
        for _, row in sampled.iterrows():
            d = {
                k: (
                    None if pd.isna(v)
                    else str(v) if isinstance(v, (datetime.datetime, datetime.date, datetime.time, pd.Timestamp))
                    else v
                )
                for k, v in row.items()
            }
            # Stable row ID — same every restart for the same data
            raw = (f"{d.get('consumer_id','')}"
                   f"|{d.get('img_url', d.get('Img_url', d.get('IMG_URL', d.get('image_url',''))))}")
            d["row_id"]  = hashlib.md5(raw.encode()).hexdigest()
            d["_agency"] = str(d.get(agency_col) or "Unknown")
            rows.append(d)
            idx.setdefault(d["_agency"], []).append(d)

        _rows_cache   = rows
        _agency_index = idx
        _agency_names = sorted(idx.keys())
        _total_rows   = len(rows)
        print(f"[data] Ready — {_total_rows:,} rows cached in memory")
        return rows

USER_ASSIGNMENTS = {}

def assign_agencies_to_users():
    global USER_ASSIGNMENTS

    agencies = sorted(_agency_names)
    users = [u["id"] for u in USERS]

    USER_ASSIGNMENTS = {u: [] for u in users}

    for i, agency in enumerate(agencies):
        user = users[i % len(users)]
        USER_ASSIGNMENTS[user].append(agency)

    print("Agency assignment complete")
    
def get_agency_rows(agency: str):
    load_and_sample()
    return _agency_index.get(agency, [])

def get_agency_names():
    load_and_sample()
    return _agency_names

# ─── AGENCY STATS HELPER ──────────────────────────────────
# Called often — uses indexed DB queries, never touches the full row cache.

def get_agency_stats(db):
    """Returns dict: agency -> {annotated, by_user, labels}"""
    rows = db.execute(
        "SELECT agency, label, skipped, annotated_by, COUNT(*) as cnt "
        "FROM annotations "
        "GROUP BY agency, label, skipped, annotated_by"
    ).fetchall()

    result = {}
    for r in rows:
        ag = r["agency"]
        if ag not in result:
            result[ag] = {"annotated": 0, "by_user": {}, "labels": {}}
        result[ag]["annotated"] += r["cnt"]
        uid = r["annotated_by"] or "unknown"
        result[ag]["by_user"][uid] = result[ag]["by_user"].get(uid, 0) + r["cnt"]
        lbl = "skipped" if r["skipped"] else (r["label"] or "unlabeled")
        result[ag]["labels"][lbl] = result[ag]["labels"].get(lbl, 0) + r["cnt"]
    return result

# ─── EXCEL BUILDER ────────────────────────────────────────

def build_agency_excel(agency, agency_rows, ann_map, user_map):
    safe      = "".join(c if c.isalnum() or c in " ._-" else "_" for c in agency)
    ts        = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename  = f"{safe}_{ts}.xlsx"
    filepath  = os.path.join(EXPORTS_DIR, filename)

    src_keys  = [k for k in (agency_rows[0].keys() if agency_rows else [])
                 if not k.startswith("_") and k != "row_id"]
    ann_cols  = ["annotation_label","annotation_notes","skipped",
                 "annotated_by_id","annotated_by_name",
                 "annotated_at","created_at","row_id"]

    records = []
    for row in agency_rows:
        ann   = ann_map.get(row["row_id"], {})
        uid   = ann.get("annotated_by","")
        rec   = {k: ("" if row.get(k) is None else row.get(k)) for k in src_keys}
        rec.update({
            "annotation_label":  ann.get("label",""),
            "annotation_notes":  ann.get("notes",""),
            "skipped":           "Yes" if ann.get("skipped") else "No",
            "annotated_by_id":   uid,
            "annotated_by_name": user_map.get(uid, uid),
            "annotated_at":      ann.get("updated_at",""),
            "created_at":        ann.get("created_at",""),
            "row_id":            row["row_id"],
        })
        records.append(rec)

    df_all = pd.DataFrame(records, columns=src_keys + ann_cols)

    lc = df_all["annotation_label"].value_counts().reset_index()
    lc.columns = ["Label","Count"]
    lc["Pct"] = (lc["Count"] / max(len(df_all),1) * 100).round(1).astype(str) + "%"

    by_ann = (df_all.groupby("annotated_by_name")
                    .agg(Count=("row_id","count"))
                    .reset_index()
                    .rename(columns={"annotated_by_name":"Annotator"}))

    with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
        df_all.to_excel(writer, sheet_name="All Rows", index=False)

        meta = pd.DataFrame([
            ["Agency",       agency],
            ["Generated",    datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")],
            ["Total Rows",   len(df_all)],
            ["Correct",      int((df_all["annotation_label"]=="actual_correct").sum())],
            ["Wrong",        int((df_all["annotation_label"]=="actual_wrong").sum())],
            ["Skipped",      int((df_all["skipped"]=="Yes").sum())],
            ["Other",        int(~df_all["annotation_label"].isin(
                                  ["actual_correct","actual_wrong",""]).sum())],
        ], columns=["Field","Value"])
        meta.to_excel(writer, sheet_name="Summary", index=False, startrow=0)
        lc.to_excel(writer, sheet_name="Summary", index=False, startrow=len(meta)+2)
        by_ann.to_excel(writer, sheet_name="Summary", index=False,
                        startrow=len(meta)+len(lc)+5)

        df_all[df_all["annotation_label"]=="actual_correct"].to_excel(
            writer, sheet_name="Correct", index=False)
        df_all[df_all["annotation_label"]=="actual_wrong"].to_excel(
            writer, sheet_name="Wrong", index=False)
        df_all[~df_all["annotation_label"].isin(["actual_correct","actual_wrong"]) |
               (df_all["skipped"]=="Yes")].to_excel(
            writer, sheet_name="Other", index=False)

        try:
            from openpyxl.styles import PatternFill, Font, Alignment
            from openpyxl.utils import get_column_letter

            def style_ws(ws, hdr_hex, font_hex="F5A623"):
                fill  = PatternFill("solid", fgColor=hdr_hex)
                font  = Font(bold=True, color=font_hex)
                align = Alignment(horizontal="center", vertical="center")
                for cell in ws[1]:
                    cell.fill = fill; cell.font = font; cell.alignment = align
                ws.freeze_panes = "A2"
                for i, col in enumerate(ws.columns, 1):
                    w = max((len(str(c.value or "")) for c in col), default=8)
                    ws.column_dimensions[get_column_letter(i)].width = min(w + 4, 44)

            style_ws(writer.sheets["All Rows"], "161719")
            style_ws(writer.sheets["Summary"],  "161719")
            style_ws(writer.sheets["Correct"],  "1B4332", "FFFFFF")
            style_ws(writer.sheets["Wrong"],    "4B1C1C", "FFFFFF")
            style_ws(writer.sheets["Other"],    "1A1A2E")
        except Exception as e:
            print(f"[excel] Styling skipped: {e}")

    print(f"[excel] Saved → {filepath}")
    return filepath, filename

# ─── COMPLETION CHECK ─────────────────────────────────────

def check_and_complete_agency(agency, db):
    """Returns (just_completed, filename_or_None)"""
    if db.execute("SELECT 1 FROM completed_agencies WHERE agency=?", (agency,)).fetchone():
        return False, None   # already done

    total_needed = len(get_agency_rows(agency))
    if total_needed == 0:
        return False, None

    ann_count = db.execute(
        "SELECT COUNT(*) FROM annotations WHERE agency=?", (agency,)
    ).fetchone()[0]

    if ann_count < total_needed:
        return False, None

    # 🎉 Just finished this agency
    ann_map  = {r["row_id"]: dict(r) for r in
                db.execute("SELECT * FROM annotations WHERE agency=?", (agency,)).fetchall()}
    user_map = {u["id"]: u["name"] for u in USERS}
    try:
        filepath, filename = build_agency_excel(
            agency, get_agency_rows(agency), ann_map, user_map)
        db.execute(
            "INSERT INTO completed_agencies (agency,completed_at,excel_path,total_rows) "
            "VALUES (?,?,?,?)",
            (agency, datetime.datetime.utcnow().isoformat(), filepath, total_needed))
        db.commit()
        return True, filename
    except Exception as e:
        print(f"[server] Excel build failed for '{agency}': {e}")
        return False, None

# ─── ROUTES ───────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(".", "ui.html")

@app.route("/dashboard")
def dashboard():
    return send_from_directory(".", "dashboard.html")

# ── Static data (users never change at runtime)
@app.route("/api/users")
def api_users():
    resp = jsonify(USERS)
    resp.headers["Cache-Control"] = "public, max-age=3600"
    return resp

# ── Agency list  (lightweight — no row data, just names + counts)
# This is what the UI fetches on boot. Tiny payload.
@app.route("/api/agencies")
@gzip_response
def api_agencies():
    load_and_sample()   # ensure cache is warm
    db    = get_db()
    stats = get_agency_stats(db)
    completed_set = {r["agency"] for r in
                     db.execute("SELECT agency FROM completed_agencies").fetchall()}

    result = []
    user = request.args.get("user")

    if user and user in USER_ASSIGNMENTS:
        agencies = USER_ASSIGNMENTS[user]
    else:
        agencies = _agency_names

    for ag in agencies:
        total = len(_agency_index[ag])
        s     = stats.get(ag, {"annotated":0, "by_user":{}, "labels":{}})
        result.append({
            "agency":    ag,
            "total":     total,
            "annotated": s["annotated"],
            "pending":   total - s["annotated"],
            "by_user":   s["by_user"],
            "labels":    s["labels"],
            "completed": ag in completed_set,
        })
    return jsonify(result)

# ── Per-agency images  (fetched only when user clicks an agency)
# Returns only the rows for one agency — typically 150–200 rows.
@app.route("/api/images/<path:agency>")
@gzip_response
def api_images_for_agency(agency):
    rows = get_agency_rows(agency)
    if not rows:
        return jsonify([])
    # Merge saved annotations so the UI can restore state
    db      = get_db()
    ann_map = {r["row_id"]: dict(r) for r in
               db.execute("SELECT * FROM annotations WHERE agency=?", (agency,)).fetchall()}
    result  = []
    for row in rows:
        d   = {k: v for k, v in row.items() if not k.startswith("_")}
        ann = ann_map.get(row["row_id"], {})
        d["_saved_label"]   = ann.get("label","")
        d["_saved_notes"]   = ann.get("notes","")
        d["_saved_skipped"] = bool(ann.get("skipped"))
        result.append(d)
    return jsonify(result)

# ── Kept for backward-compat / bulk export  (not used by UI boot)
@app.route("/api/images")
@gzip_response
def api_images_all():
    agency = request.args.get("agency")
    if agency:
        return api_images_for_agency(agency)
    rows = load_and_sample()
    return jsonify([{k:v for k,v in r.items() if not k.startswith("_")} for r in rows])

# ── Save annotation
@app.route("/api/annotate", methods=["POST"])
def api_annotate():
    data   = request.get_json(force=True)
    row_id = data.get("row_id")
    if not row_id:
        return jsonify({"error": "row_id required"}), 400

    agency = data.get("agency","")
    now    = datetime.datetime.utcnow().isoformat()

    with get_db() as db:
        if db.execute("SELECT 1 FROM annotations WHERE row_id=?", (row_id,)).fetchone():
            db.execute(
                "UPDATE annotations "
                "SET label=?,notes=?,skipped=?,annotated_by=?,updated_at=? "
                "WHERE row_id=?",
                (data.get("label",""), data.get("notes",""),
                 1 if data.get("skipped") else 0,
                 data.get("annotated_by",""), now, row_id))
        else:
            db.execute(
                "INSERT INTO annotations "
                "(row_id,agency,consumer_id,img_url,label,notes,skipped,annotated_by,created_at,updated_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?)",
                (row_id, agency, data.get("consumer_id",""),
                 data.get("img_url",""), data.get("label",""), data.get("notes",""),
                 1 if data.get("skipped") else 0,
                 data.get("annotated_by",""), now, now))
        db.commit()
        just_completed, excel_filename = check_and_complete_agency(agency, db)

    resp = {"ok": True, "row_id": row_id, "updated_at": now}
    if just_completed:
        resp.update({"agency_completed": True,
                     "excel_filename":   excel_filename,
                     "excel_url":        f"/api/exports/{excel_filename}"})
    return jsonify(resp)

# ── Download completed Excel
@app.route("/api/exports/<filename>")
def api_download_export(filename):
    safe = os.path.basename(filename)
    path = os.path.join(EXPORTS_DIR, safe)
    if not os.path.exists(path):
        return jsonify({"error": "File not found"}), 404
    return send_file(path, as_attachment=True, download_name=safe,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ── List completed agencies
@app.route("/api/completed_agencies")
@gzip_response
def api_completed_agencies():
    db   = get_db()
    rows = db.execute(
        "SELECT agency,completed_at,excel_path,total_rows "
        "FROM completed_agencies ORDER BY completed_at DESC"
    ).fetchall()
    result = []
    for r in rows:
        fname = os.path.basename(r["excel_path"]) if r["excel_path"] else None
        result.append({
            "agency":       r["agency"],
            "completed_at": r["completed_at"],
            "total_rows":   r["total_rows"],
            "excel_url":    f"/api/exports/{fname}" if fname else None,
            "excel_name":   fname,
        })
    return jsonify(result)

# ── Force-rebuild Excel for an agency
@app.route("/api/regenerate_excel/<path:agency>", methods=["POST"])
def api_regenerate_excel(agency):
    agency_rows = get_agency_rows(agency)
    if not agency_rows:
        return jsonify({"error":"Agency not found"}), 404
    db      = get_db()
    ann_map = {r["row_id"]: dict(r) for r in
               db.execute("SELECT * FROM annotations WHERE agency=?", (agency,)).fetchall()}
    try:
        filepath, filename = build_agency_excel(
            agency, agency_rows, ann_map, {u["id"]:u["name"] for u in USERS})
        db.execute(
            "INSERT OR REPLACE INTO completed_agencies (agency,completed_at,excel_path,total_rows)"
            " VALUES (?,?,?,?)",
            (agency, datetime.datetime.utcnow().isoformat(), filepath, len(agency_rows)))
        db.commit()
        return jsonify({"ok":True, "excel_url":f"/api/exports/{filename}", "excel_name":filename})
    except Exception as e:
        return jsonify({"error":str(e)}), 500

# ── Annotations list (dashboard / export)
@app.route("/api/annotations")
@gzip_response
def api_annotations():
    agency = request.args.get("agency")
    user   = request.args.get("user")
    limit  = int(request.args.get("limit", 500))
    db     = get_db()
    q, params = "SELECT * FROM annotations WHERE 1=1", []
    if agency: q += " AND agency=?";       params.append(agency)
    if user:   q += " AND annotated_by=?"; params.append(user)
    q += " ORDER BY updated_at DESC LIMIT ?"
    params.append(limit)
    return jsonify([dict(r) for r in db.execute(q, params).fetchall()])

# ── Dashboard summary (per-user stats)
@app.route("/api/dashboard")
@gzip_response
def api_dashboard():
    load_and_sample()
    db             = get_db()
    total_agencies = len(_agency_names)

    user_data = {u["id"]: {**u, "total":0, "agencies":set(), "labels":{}, "recent":[]}
                 for u in USERS}

    for r in db.execute(
        "SELECT annotated_by,agency,label,skipped,updated_at "
        "FROM annotations ORDER BY updated_at DESC"
    ).fetchall():
        uid = r["annotated_by"] or "unknown"
        if uid not in user_data:
            continue
        ud = user_data[uid]
        ud["total"] += 1
        ud["agencies"].add(r["agency"])
        lbl = "skipped" if r["skipped"] else (r["label"] or "unlabeled")
        ud["labels"][lbl] = ud["labels"].get(lbl, 0) + 1
        if len(ud["recent"]) < 8:
            ud["recent"].append({"agency":r["agency"],"label":lbl,"at":r["updated_at"]})

    completed_count = db.execute(
        "SELECT COUNT(*) FROM completed_agencies"
    ).fetchone()[0]
    total_annotated = db.execute(
        "SELECT COUNT(*) FROM annotations"
    ).fetchone()[0]

    result = [{"id":ud["id"],"name":ud["name"],"color":ud["color"],
               "total_annotations":ud["total"],
               "agencies_touched":len(ud["agencies"]),
               "agencies_list":sorted(ud["agencies"]),
               "labels":ud["labels"],"recent":ud["recent"]}
              for ud in user_data.values()]
    result.sort(key=lambda x: x["total_annotations"], reverse=True)

    return jsonify({
        "users": result,
        "global": {
            "total_images":       _total_rows,
            "total_agencies":     total_agencies,
            "completed_agencies": completed_count,
            "total_annotated":    total_annotated,
        }
    })

# ── Full CSV export
@app.route("/api/export/csv")
def api_export_csv():
    import io, csv
    rows    = load_and_sample()
    db      = get_db()
    ann_map = {r["row_id"]: dict(r) for r in
               db.execute("SELECT * FROM annotations").fetchall()}
    out     = io.StringIO()
    if not rows:
        return Response("", mimetype="text/csv")
    src_keys   = [k for k in rows[0].keys() if not k.startswith("_") and k != "row_id"]
    extra_keys = ["annotation_label","annotation_notes","skipped",
                  "annotated_by","annotated_at","row_id"]
    writer = csv.DictWriter(out, fieldnames=src_keys+extra_keys, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        ann = ann_map.get(row["row_id"], {})
        r2  = {k: row.get(k,"") for k in src_keys}
        r2.update({"annotation_label": ann.get("label",""),
                   "annotation_notes": ann.get("notes",""),
                   "skipped":          "yes" if ann.get("skipped") else "no",
                   "annotated_by":     ann.get("annotated_by",""),
                   "annotated_at":     ann.get("updated_at",""),
                   "row_id":           row["row_id"]})
        writer.writerow(r2)
    fname = f"export_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    return Response(out.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename={fname}"})

# ─── STARTUP ──────────────────────────────────────────────
if __name__ == "__main__":
    init_db()
    try:
        load_and_sample()
        assign_agencies_to_users()
    except Exception as e:
        print(f"\n[ERROR] {e}")
        print("Fix DATA_FILE / AGENCY_COLUMN at the top of server.py and retry.\n")
        raise SystemExit(1)
    print(f"\n  Annotator  →  http://localhost:{PORT}")
    print(f"  Dashboard  →  http://localhost:{PORT}/dashboard")
    print(f"  Exports    →  ./{EXPORTS_DIR}/\n")
    # threaded=True: each request gets its own thread → concurrent users work fine
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
