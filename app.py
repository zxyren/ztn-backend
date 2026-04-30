import os, threading, time, glob, json, logging, shutil, subprocess, uuid, sqlite3
from flask import Flask, request, jsonify, send_file, Response, session as flask_session
from flask_cors import CORS
from yt_dlp import YoutubeDL

app = Flask(__name__)
CORS(app, supports_credentials=True)
app.secret_key = os.environ.get("FLASK_SECRET_KEY") or os.urandom(32)
logging.getLogger('werkzeug').setLevel(logging.ERROR)

DOWNLOAD_FOLDER = os.environ.get("DOWNLOAD_FOLDER") or os.path.join(os.path.dirname(__file__), "downloads")
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

# Detect ffmpeg
FFMPEG = shutil.which("ffmpeg")
if not FFMPEG:
    try:
        subprocess.run(["ffmpeg", "-version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        FFMPEG = "ffmpeg"
    except Exception:
        pass
print(f"ffmpeg: {FFMPEG or 'NOT FOUND'}")

MAX_CONCURRENT = 1
DB_PATH = os.environ.get("DB_PATH") or os.path.join(os.path.dirname(__file__), "downloads.db")
db_claim_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def init_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS downloads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT,
            url TEXT NOT NULL,
            format TEXT NOT NULL,
            status TEXT NOT NULL,
            progress REAL DEFAULT 0.0,
            error TEXT,
            filename TEXT,
            filepath TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_downloads_session ON downloads(session_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_downloads_status ON downloads(status);")
    conn.commit()
    conn.close()


def get_db_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_session_downloads(session_id: str):
    session_id = session_id or "unknown"
    conn = get_db_conn()
    try:
        total = conn.execute(
            "SELECT COUNT(*) FROM downloads WHERE session_id = ?",
            (session_id,),
        ).fetchone()[0]
        completed = conn.execute(
            "SELECT COUNT(*) FROM downloads WHERE session_id = ? AND status = 'Completed'",
            (session_id,),
        ).fetchone()[0]
        downloading = conn.execute(
            """
            SELECT COUNT(*) FROM downloads
            WHERE session_id = ?
              AND status IN ('Starting','Downloading','Converting','Merging')
            """,
            (session_id,),
        ).fetchone()[0]

        rows = conn.execute(
            """
            SELECT id, url, status, progress, format, filename, error
            FROM downloads
            WHERE session_id = ?
            ORDER BY created_at DESC
            """,
            (session_id,),
        ).fetchall()

        queue = [
            {
                "id": int(r["id"]),
                "session_id": session_id,
                "url": r["url"],
                "status": r["status"],
                "progress": float(r["progress"] or 0.0),
                "format": r["format"],
                "filename": r["filename"],
                "error": r["error"],
            }
            for r in rows
        ]
        return {"total": total, "completed": completed, "downloading": downloading, "queue": queue}
    finally:
        conn.close()


def get_session_id(create_if_missing: bool = True) -> str:
    sid = flask_session.get("session_id")
    if sid:
        return str(sid)
    provided = (request.headers.get("X-Session-ID") or request.args.get("session_id", "")).strip()
    if provided:
        flask_session["session_id"] = provided
        return provided
    if not create_if_missing:
        return ""
    flask_session["session_id"] = uuid.uuid4().hex
    return flask_session["session_id"]


def build_ydl_opts(progress_hook, format_type):
    base = {
        "outtmpl": os.path.join(DOWNLOAD_FOLDER, "%(title)s.%(ext)s"),
        "progress_hooks": [progress_hook],
        "restrictfilenames": True,
        "windowsfilenames": True,
        "updatetime": False,
        "noverifyhttpscert": True,
        "buffersize": 1024 * 64,
        "http_headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        },
    }

    if format_type == "audio":
        if not FFMPEG:
            raise RuntimeError("FFmpeg not found — required for MP3 extraction")
        # Prefer m4a (AAC) — FFmpeg stream-copies it instantly into MP3.
        # /best fallback ensures TikTok and sites without m4a still work.
        base["format"] = "bestaudio[ext=m4a]/bestaudio[ext=mp3]/bestaudio/best"
        base["postprocessors"] = [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": "192",
        }]
        # Stream-copy when source is AAC/MP3 (near-instant), re-encode otherwise
        base["postprocessor_args"] = {"ffmpegextractaudio": ["-c:a", "copy"]}
    else:
        base["format"] = "bestvideo+bestaudio/best" if FFMPEG else "best"
        if FFMPEG:
            base["merge_output_format"] = "mp4"

    return base


# ---------------------------------------------------------------------------
# Download worker
# ---------------------------------------------------------------------------

def download_one(item_id: int, session_id: str, url: str, format_type: str):
    """
    Download + (optional) extract audio, then persist output + state in SQLite.
    """
    conn = get_db_conn()
    final_file = None
    last_pct = None
    last_update_ts = 0.0

    def hook(d):
        nonlocal final_file, last_pct, last_update_ts
        if d["status"] == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate")
            if not total:
                return
            pct = round(d["downloaded_bytes"] * 100.0 / total, 1)
            now = time.time()

            # Throttle DB writes so SQLite isn't hammered during large downloads.
            if last_pct is not None and pct == last_pct and (now - last_update_ts) < 1.0:
                return

            last_pct = pct
            last_update_ts = now

            current = conn.execute("SELECT status FROM downloads WHERE id = ?", (item_id,)).fetchone()
            if current and current["status"] == "Cancelled":
                raise Exception("Cancelled")

            conn.execute(
                "UPDATE downloads SET status=?, progress=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                ("Downloading", max(0.0, min(100.0, pct)), item_id),
            )
            conn.commit()

        elif d["status"] == "finished":
            status = "Converting" if format_type == "audio" else "Merging"
            conn.execute(
                "UPDATE downloads SET status=?, progress=100.0, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (status, item_id),
            )
            conn.commit()
            final_file = d.get("filename")

    try:
        try:
            opts = build_ydl_opts(hook, format_type)
        except RuntimeError as e:
            conn.execute(
                "UPDATE downloads SET status='Error', error=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (str(e), item_id),
            )
            conn.commit()
            return

        with YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=False)
            expected = ydl.prepare_filename(info)
            ydl.download([url])

        # Resolve output file (same heuristics as your original code).
        if format_type == "audio":
            base = os.path.splitext(expected)[0]
            mp3 = base + ".mp3"
            if os.path.exists(mp3):
                final_file = mp3
            else:
                candidates = glob.glob(os.path.join(DOWNLOAD_FOLDER, "*.mp3"))
                final_file = max(candidates, key=os.path.getmtime) if candidates else None
        else:
            final_file = expected if os.path.exists(expected) else None
            if not final_file:
                all_files = [f for f in glob.glob(os.path.join(DOWNLOAD_FOLDER, "*")) if os.path.isfile(f)]
                final_file = max(all_files, key=os.path.getmtime) if all_files else None

        if not final_file or not os.path.exists(final_file):
            raise Exception("Output file not found after download")
        if os.path.getsize(final_file) < 1024:
            raise Exception("Output file too small — download likely failed")

        conn.execute(
            """
            UPDATE downloads
            SET status='Completed',
                progress=100.0,
                filename=?,
                filepath=?,
                error=NULL,
                updated_at=CURRENT_TIMESTAMP
            WHERE id=?
            """,
            (os.path.basename(final_file), final_file, item_id),
        )
        conn.commit()

    except Exception as e:
        msg = str(e)
        is_cancel = "cancelled" in msg.lower()

        conn.execute(
            "UPDATE downloads SET status=?, error=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
            ("Cancelled" if is_cancel else "Error", msg, item_id),
        )
        conn.commit()

        # Cleanup partial output when cancelling.
        if is_cancel and final_file and os.path.exists(final_file):
            try:
                os.remove(final_file)
            except Exception:
                pass
    finally:
        conn.close()


def worker_loop():
    while True:
        item = None
        try:
            with db_claim_lock:
                conn = get_db_conn()
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    """
                    SELECT id, session_id, url, format
                    FROM downloads
                    WHERE status='Queued'
                    ORDER BY created_at
                    LIMIT 1
                    """
                ).fetchone()
                if not row:
                    conn.commit()
                    conn.close()
                    time.sleep(0.5)
                    continue

                conn.execute(
                    "UPDATE downloads SET status='Starting', progress=0.0, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                    (row["id"],),
                )
                conn.commit()
                item = row
                conn.close()
        except sqlite3.OperationalError:
            # Database may be locked if you have multiple instances; wait and retry.
            time.sleep(0.5)
            continue

        if item:
            download_one(int(item["id"]), item["session_id"], item["url"], item["format"])
        time.sleep(0.1)


def start_workers():
    for _ in range(MAX_CONCURRENT):
        threading.Thread(target=worker_loop, daemon=True).start()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

def sid():
    return get_session_id(True)


@app.route("/api/queue", methods=["POST"])
def queue_download():
    data = request.get_json(force=True, silent=True) or {}
    format_type = data.get("format", "video")
    session_id = sid()
    conn = get_db_conn()
    try:
        for url in [u.strip() for u in data.get("urls", []) if u.strip()]:
            conn.execute(
                "INSERT INTO downloads(session_id, url, format, status, progress) VALUES (?, ?, ?, 'Queued', 0.0)",
                (session_id, url, format_type),
            )
        conn.commit()
    finally:
        conn.close()
    return jsonify(ensure_session_downloads(session_id))


@app.route("/api/upload", methods=["POST"])
def upload_file():
    session_id = sid()
    f = request.files.get("file")
    if not f:
        return jsonify({"error": "No file"}), 400
    format_type = request.form.get("format", "video")
    lines = [l.strip() for l in f.read().decode("utf-8", errors="ignore").splitlines() if l.strip()]
    conn = get_db_conn()
    try:
        for url in lines:
            conn.execute(
                "INSERT INTO downloads(session_id, url, format, status, progress) VALUES (?, ?, ?, 'Queued', 0.0)",
                (session_id, url, format_type),
            )
        conn.commit()
    finally:
        conn.close()
    return jsonify(ensure_session_downloads(session_id))


@app.route("/api/status")
def status():
    session_id = sid()
    return jsonify(ensure_session_downloads(session_id))


@app.route("/api/cancel/<int:item_id>", methods=["POST"])
def cancel(item_id):
    session_id = sid()
    conn = get_db_conn()
    try:
        row = conn.execute(
            "SELECT session_id, status FROM downloads WHERE id=?",
            (item_id,),
        ).fetchone()
        if not row:
            return jsonify({"error": "Not found"}), 404
        if row["session_id"] != session_id:
            return jsonify({"error": "Not allowed"}), 403
        if row["status"] in ("Completed", "Error", "Cancelled"):
            return jsonify({"error": f"Cannot cancel — status is {row['status']}"}), 400

        conn.execute(
            "UPDATE downloads SET status='Cancelled', error='Cancelled by user', updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (item_id,),
        )
        conn.commit()
    finally:
        conn.close()
    return jsonify(ensure_session_downloads(session_id))


@app.route("/api/download/<int:item_id>")
def download_file(item_id):
    conn = get_db_conn()
    try:
        item = conn.execute(
            """
            SELECT status, filepath, filename
            FROM downloads
            WHERE id=?
            """,
            (item_id,),
        ).fetchone()
    finally:
        conn.close()

    if not item:
        return jsonify({"error": "Not found"}), 404

    # Important: do NOT require the same session cookie for downloads.
    # This fixes "copied link disappears" after production where session cookies can differ.
    if item["status"] != "Completed":
        return jsonify({"error": "Not ready"}), 400
    fp = item.get("filepath")
    if not fp or not os.path.exists(fp):
        return jsonify({"error": "File missing"}), 404
    return send_file(fp, as_attachment=True, download_name=item.get("filename", "download"))


@app.route("/api/clear", methods=["POST"])
def clear():
    session_id = sid()
    conn = get_db_conn()
    try:
        rows = conn.execute(
            "SELECT id, filepath, status FROM downloads WHERE session_id=?",
            (session_id,),
        ).fetchall()
        for r in rows:
            fp = r["filepath"]
            if fp and os.path.exists(fp):
                try:
                    os.remove(fp)
                except Exception:
                    pass

        # Mimic your original behavior: remove queued/completed items from the UI.
        conn.execute("DELETE FROM downloads WHERE session_id=?", (session_id,))
        conn.commit()
    finally:
        conn.close()
    return jsonify(ensure_session_downloads(session_id))


@app.route("/api/events")
def events():
    session_id = sid()
    def stream():
        # Simple DB-polling SSE. This works across multiple app instances
        # (your previous in-memory broadcast could break on production).
        last_payload = None
        while True:
            st = ensure_session_downloads(session_id)
            payload = json.dumps(st, sort_keys=True)
            if payload != last_payload:
                last_payload = payload
                yield f"data: {payload}\n\n"
            time.sleep(1)
    return Response(stream(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/thumbnail", methods=["POST"])
def thumbnail():
    url = (request.get_json(force=True, silent=True) or {}).get("url", "").strip()
    if not url:
        return jsonify({"error": "No URL"}), 400
    try:
        info = YoutubeDL({"quiet": True, "skip_download": True, "socket_timeout": 10}).extract_info(url, download=False)
        if not info:
            return jsonify({"error": "No info"}), 404
        thumbs = [t for t in info.get("thumbnails", []) if t.get("url")]
        thumb = None
        if thumbs:
            low_q = [t for t in thumbs if 240 <= t.get("width", 0) <= 640]
            thumb = (min(low_q, key=lambda x: abs(x.get("width", 0) - 480))["url"]
                     if low_q else min(thumbs, key=lambda x: x.get("width", 999999))["url"])
        thumb = thumb or info.get("thumbnail")
        return jsonify({"thumbnail": thumb, "title": info.get("title", "")}) if thumb else (jsonify({"error": "No thumbnail"}), 404)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/")
def index():
    try:
        with open(os.path.join(os.path.dirname(__file__), "index.html")) as f:
            return f.read()
    except Exception:
        return jsonify({"status": "Video Downloader API running"})


init_db()
start_workers()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))