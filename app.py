import os, threading, time, tempfile, glob, json, logging, shutil, subprocess
from queue import Queue
from flask import Flask, request, jsonify, send_file, Response
from flask_cors import CORS
from yt_dlp import YoutubeDL

app = Flask(__name__)
CORS(app)
logging.getLogger('werkzeug').setLevel(logging.ERROR)

DOWNLOAD_FOLDER = tempfile.mkdtemp(prefix="ydl_")
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

FFMPEG = shutil.which("ffmpeg")
if not FFMPEG:
    try:
        subprocess.run(["ffmpeg", "-version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        FFMPEG = "ffmpeg"
    except Exception:
        pass
print(f"✓ ffmpeg: {FFMPEG or 'NOT FOUND'}")

MAX_CONCURRENT = 2

# ---------------------------------------------------------------------------
# Per-session in-memory state — no database, no files, no login needed.
# Each browser gets a UUID from localStorage, sent as ?session_id=xxx.
# Users only ever see their own queue.
# ---------------------------------------------------------------------------
lock = threading.Lock()
sse_lock = threading.Lock()
task_queue = Queue()          # items: (session_id, item_id)
sessions = {}                 # sid -> { total, completed, downloading, queue }
next_ids = {}                 # sid -> int
cancelled = set()             # (sid, item_id)
sse_clients = {}              # sid -> [Queue, ...]


def get_session(sid):
    if sid not in sessions:
        sessions[sid] = {"total": 0, "completed": 0, "downloading": 0, "queue": []}
        next_ids[sid] = 1
    return sessions[sid]


def get_item(sid, item_id):
    d = sessions.get(sid)
    return next((x for x in d["queue"] if x["id"] == item_id), None) if d else None


def broadcast(sid):
    with lock:
        data = json.dumps(sessions.get(sid, {}))
    with sse_lock:
        for q in sse_clients.get(sid, [])[:]:
            try:
                q.put(data)
            except Exception:
                sse_clients[sid].remove(q)


def sid_from_request():
    return (request.args.get("session_id") or
            request.headers.get("X-Session-ID") or "").strip()


def require_sid():
    s = sid_from_request()
    if not s:
        return None, (jsonify({"error": "Missing session_id"}), 400)
    return s, None


# ---------------------------------------------------------------------------
# yt-dlp options
# ---------------------------------------------------------------------------

def build_opts(hook, format_type):
    base = {
        "outtmpl": os.path.join(DOWNLOAD_FOLDER, "%(title)s.%(ext)s"),
        "progress_hooks": [hook],
        "restrictfilenames": True,
        "windowsfilenames": True,
        "updatetime": False,
        "noverifyhttpscert": True,
        "buffersize": 1024 * 64,
        "http_headers": {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
    }
    if format_type == "audio":
        if not FFMPEG:
            raise RuntimeError("FFmpeg not found — required for MP3 extraction")
        base["format"] = "bestaudio[ext=m4a]/bestaudio[ext=mp3]/bestaudio/best"
        base["postprocessors"] = [{"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "192"}]
        base["postprocessor_args"] = {"ffmpegextractaudio": ["-c:a", "copy"]}
    else:
        base["format"] = "bestvideo+bestaudio/best" if FFMPEG else "best"
        if FFMPEG:
            base["merge_output_format"] = "mp4"
    return base


# ---------------------------------------------------------------------------
# Download worker
# ---------------------------------------------------------------------------

def download_one(sid, item_id):
    with lock:
        item = get_item(sid, item_id)
        if not item or (sid, item_id) in cancelled:
            return

    format_type = item.get("format", "video")
    final_file = None

    def hook(d):
        nonlocal final_file
        with lock:
            if (sid, item_id) in cancelled:
                raise Exception("Cancelled")
        if d["status"] == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate")
            if total:
                pct = round(d["downloaded_bytes"] * 100.0 / total, 1)
                with lock:
                    item["progress"] = max(0.0, min(100.0, pct))
                    item["status"] = "Downloading"
                broadcast(sid)
        elif d["status"] == "finished":
            with lock:
                item["progress"] = 100.0
                item["status"] = "Converting" if format_type == "audio" else "Merging"
            broadcast(sid)
            final_file = d.get("filename")

    try:
        opts = build_opts(hook, format_type)
    except RuntimeError as e:
        with lock:
            item["status"], item["error"] = "Error", str(e)
        broadcast(sid)
        return

    try:
        with YoutubeDL(opts) as ydl:
            info = ydl.extract_info(item["url"], download=False)
            expected = ydl.prepare_filename(info)
            ydl.download([item["url"]])

        # Resolve final file path
        if format_type == "audio":
            base = os.path.splitext(expected)[0]
            mp3 = base + ".mp3"
            final_file = mp3 if os.path.exists(mp3) else None
            if not final_file:
                candidates = glob.glob(os.path.join(DOWNLOAD_FOLDER, "*.mp3"))
                final_file = max(candidates, key=os.path.getmtime) if candidates else None
        else:
            final_file = expected if os.path.exists(expected) else None
            if not final_file:
                files = [f for f in glob.glob(os.path.join(DOWNLOAD_FOLDER, "*")) if os.path.isfile(f)]
                final_file = max(files, key=os.path.getmtime) if files else None

        if not final_file or not os.path.exists(final_file):
            raise Exception("Output file not found after download")
        if os.path.getsize(final_file) < 1024:
            raise Exception("Output file too small — download likely failed")

        with lock:
            item.update({"status": "Completed", "filename": os.path.basename(final_file), "filepath": final_file})
            sessions[sid]["completed"] += 1
        broadcast(sid)

    except Exception as e:
        msg = str(e)
        is_cancel = "cancelled" in msg.lower()
        with lock:
            item["status"] = "Cancelled" if is_cancel else "Error"
            item["error"] = msg
            if is_cancel and final_file and os.path.exists(final_file):
                try: os.remove(final_file)
                except Exception: pass
        broadcast(sid)


def worker_loop():
    while True:
        sid, item_id = task_queue.get()
        with lock:
            if (sid, item_id) in cancelled:
                task_queue.task_done()
                continue
            d = sessions.get(sid)
            if d:
                d["downloading"] += 1
            item = get_item(sid, item_id)
            if item and item["status"] == "Queued":
                item["status"] = "Starting"
        broadcast(sid)
        if item:
            download_one(sid, item_id)
        time.sleep(0.1)
        with lock:
            d = sessions.get(sid)
            if d:
                d["downloading"] = max(0, d["downloading"] - 1)
            cancelled.discard((sid, item_id))
        broadcast(sid)
        task_queue.task_done()


def start_workers():
    for _ in range(MAX_CONCURRENT):
        threading.Thread(target=worker_loop, daemon=True).start()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/api/queue", methods=["POST"])
def queue_download():
    sid, err = require_sid()
    if err: return err
    data = request.get_json(force=True, silent=True) or {}
    fmt = data.get("format", "video")
    with lock:
        d = get_session(sid)
        for url in [u.strip() for u in data.get("urls", []) if u.strip()]:
            iid = next_ids[sid]
            next_ids[sid] += 1
            d["queue"].append({"id": iid, "url": url, "status": "Queued", "progress": 0.0, "format": fmt})
            d["total"] += 1
            task_queue.put((sid, iid))
    broadcast(sid)
    return jsonify(sessions[sid])


@app.route("/api/upload", methods=["POST"])
def upload_file():
    sid, err = require_sid()
    if err: return err
    f = request.files.get("file")
    if not f: return jsonify({"error": "No file"}), 400
    fmt = request.form.get("format", "video")
    lines = [l.strip() for l in f.read().decode("utf-8", errors="ignore").splitlines() if l.strip()]
    with lock:
        d = get_session(sid)
        for url in lines:
            iid = next_ids[sid]
            next_ids[sid] += 1
            d["queue"].append({"id": iid, "url": url, "status": "Queued", "progress": 0.0, "format": fmt})
            d["total"] += 1
            task_queue.put((sid, iid))
    broadcast(sid)
    return jsonify(sessions[sid])


@app.route("/api/status")
def status():
    sid, err = require_sid()
    if err: return err
    with lock:
        return jsonify(get_session(sid))


@app.route("/api/cancel/<int:item_id>", methods=["POST"])
def cancel_item(item_id):
    sid, err = require_sid()
    if err: return err
    with lock:
        item = get_item(sid, item_id)
        if not item: return jsonify({"error": "Not found"}), 404
        if item["status"] in ("Completed", "Error", "Cancelled"):
            return jsonify({"error": f"Cannot cancel — status is {item['status']}"}), 400
        cancelled.add((sid, item_id))
        item["status"] = "Cancelling"
    broadcast(sid)
    return jsonify(sessions[sid])


@app.route("/api/download/<int:item_id>")
def download_file(item_id):
    sid, err = require_sid()
    if err: return err
    with lock:
        item = get_item(sid, item_id)
    if not item: return jsonify({"error": "Not found"}), 404
    if item["status"] != "Completed": return jsonify({"error": "Not ready"}), 400
    fp = item.get("filepath")
    if not fp or not os.path.exists(fp): return jsonify({"error": "File missing"}), 404
    return send_file(fp, as_attachment=True, download_name=item.get("filename", "download"))


@app.route("/api/clear", methods=["POST"])
def clear():
    sid, err = require_sid()
    if err: return err
    with lock:
        d = sessions.get(sid, {})
        for item in d.get("queue", []):
            fp = item.get("filepath")
            if fp and os.path.exists(fp):
                try: os.remove(fp)
                except Exception: pass
        sessions[sid] = {"total": 0, "completed": 0, "downloading": 0, "queue": []}
        next_ids[sid] = 1
        cancelled.difference_update({k for k in cancelled if k[0] == sid})
    broadcast(sid)
    return jsonify(sessions[sid])


@app.route("/api/events")
def events():
    sid = sid_from_request()
    if not sid:
        return Response("data: {}\n\n", mimetype="text/event-stream")

    def stream():
        q = Queue()
        with sse_lock:
            sse_clients.setdefault(sid, []).append(q)
        try:
            with lock:
                yield f"data: {json.dumps(get_session(sid))}\n\n"
            while True:
                yield f"data: {q.get()}\n\n"
        except GeneratorExit:
            with sse_lock:
                if q in sse_clients.get(sid, []):
                    sse_clients[sid].remove(q)

    return Response(stream(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/thumbnail", methods=["POST"])
def thumbnail():
    url = (request.get_json(force=True, silent=True) or {}).get("url", "").strip()
    if not url: return jsonify({"error": "No URL"}), 400
    try:
        info = YoutubeDL({"quiet": True, "skip_download": True, "socket_timeout": 10}).extract_info(url, download=False)
        if not info: return jsonify({"error": "No info"}), 404
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


start_workers()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))