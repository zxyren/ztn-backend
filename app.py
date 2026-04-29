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

# Detect ffmpeg
FFMPEG = shutil.which("ffmpeg")
if not FFMPEG:
    try:
        subprocess.run(["ffmpeg", "-version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        FFMPEG = "ffmpeg"
    except Exception:
        pass
print(f"✓ ffmpeg: {FFMPEG or 'NOT FOUND'}")

MAX_CONCURRENT = 1
state_lock = threading.Lock()
sse_lock = threading.Lock()
task_queue = Queue()
sse_clients = []
cancelled_ids = set()
next_id = 1
downloads = {"total": 0, "completed": 0, "downloading": 0, "queue": []}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def broadcast():
    with state_lock:
        data = json.dumps(downloads)
    with sse_lock:
        for q in sse_clients[:]:
            try:
                q.put(data)
            except Exception:
                sse_clients.remove(q)


def get_item(item_id):
    return next((x for x in downloads["queue"] if x["id"] == item_id), None)


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

def download_one(item):
    item_id = item["id"]
    format_type = item.get("format", "video")
    final_file = None

    def hook(d):
        nonlocal final_file
        with state_lock:
            if item_id in cancelled_ids:
                raise Exception("Cancelled")
        if d["status"] == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate")
            if total:
                pct = round(d["downloaded_bytes"] * 100.0 / total, 1)
                with state_lock:
                    item["progress"] = max(0.0, min(100.0, pct))
                    item["status"] = "Downloading"
                broadcast()
        elif d["status"] == "finished":
            with state_lock:
                item["progress"] = 100.0
                item["status"] = "Converting" if format_type == "audio" else "Merging"
            broadcast()
            final_file = d.get("filename")

    try:
        opts = build_ydl_opts(hook, format_type)
    except RuntimeError as e:
        with state_lock:
            item["status"], item["error"] = "Error", str(e)
        broadcast()
        return

    try:
        with YoutubeDL(opts) as ydl:
            info = ydl.extract_info(item["url"], download=False)
            expected = ydl.prepare_filename(info)
            ydl.download([item["url"]])

        # Resolve output file
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

        with state_lock:
            item["status"] = "Completed"
            item["filename"] = os.path.basename(final_file)
            item["filepath"] = final_file
            downloads["completed"] += 1
        broadcast()

    except Exception as e:
        msg = str(e)
        with state_lock:
            item["status"] = "Cancelled" if "cancelled" in msg.lower() else "Error"
            item["error"] = msg
            if "cancelled" in msg.lower() and final_file and os.path.exists(final_file):
                try: os.remove(final_file)
                except Exception: pass
        broadcast()


def worker_loop():
    while True:
        item_id = task_queue.get()
        if item_id is None:
            break

        with state_lock:
            if item_id in cancelled_ids:
                task_queue.task_done()
                continue
            downloads["downloading"] += 1
            item = get_item(item_id)
            if item and item["status"] == "Queued":
                item["status"] = "Starting"
        broadcast()

        if item:
            download_one(item)

        time.sleep(0.1)
        with state_lock:
            downloads["downloading"] = max(0, downloads["downloading"] - 1)
            cancelled_ids.discard(item_id)
        broadcast()
        task_queue.task_done()


def start_workers():
    for _ in range(MAX_CONCURRENT):
        threading.Thread(target=worker_loop, daemon=True).start()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

def sid():
    """Get session_id from header or query param."""
    return (request.headers.get("X-Session-ID") or request.args.get("session_id", "")).strip()


@app.route("/api/queue", methods=["POST"])
def queue_download():
    global next_id
    data = request.get_json(force=True, silent=True) or {}
    format_type = data.get("format", "video")
    with state_lock:
        for url in [u.strip() for u in data.get("urls", []) if u.strip()]:
            item = {"id": next_id, "url": url, "status": "Queued", "progress": 0.0, "format": format_type}
            downloads["queue"].append(item)
            downloads["total"] += 1
            task_queue.put(next_id)
            next_id += 1
    broadcast()
    return jsonify(downloads)


@app.route("/api/upload", methods=["POST"])
def upload_file():
    global next_id
    f = request.files.get("file")
    if not f:
        return jsonify({"error": "No file"}), 400
    format_type = request.form.get("format", "video")
    lines = [l.strip() for l in f.read().decode("utf-8", errors="ignore").splitlines() if l.strip()]
    with state_lock:
        for url in lines:
            item = {"id": next_id, "url": url, "status": "Queued", "progress": 0.0, "format": format_type}
            downloads["queue"].append(item)
            downloads["total"] += 1
            task_queue.put(next_id)
            next_id += 1
    broadcast()
    return jsonify(downloads)


@app.route("/api/status")
def status():
    with state_lock:
        return jsonify(downloads)


@app.route("/api/cancel/<int:item_id>", methods=["POST"])
def cancel(item_id):
    with state_lock:
        item = get_item(item_id)
        if not item:
            return jsonify({"error": "Not found"}), 404
        if item["status"] in ("Completed", "Error", "Cancelled"):
            return jsonify({"error": f"Cannot cancel — status is {item['status']}"}), 400
        cancelled_ids.add(item_id)
        item["status"] = "Cancelling"
    broadcast()
    return jsonify(downloads)


@app.route("/api/download/<int:item_id>")
def download_file(item_id):
    with state_lock:
        item = get_item(item_id)
    if not item:
        return jsonify({"error": "Not found"}), 404
    if item["status"] != "Completed":
        return jsonify({"error": "Not ready"}), 400
    fp = item.get("filepath")
    if not fp or not os.path.exists(fp):
        return jsonify({"error": "File missing"}), 404
    return send_file(fp, as_attachment=True, download_name=item.get("filename", "download"))


@app.route("/api/clear", methods=["POST"])
def clear():
    global next_id
    with state_lock:
        for item in downloads["queue"]:
            fp = item.get("filepath")
            if fp and os.path.exists(fp):
                try: os.remove(fp)
                except Exception: pass
        downloads.update({"total": 0, "completed": 0, "downloading": 0, "queue": []})
        next_id = 1
        cancelled_ids.clear()
        while not task_queue.empty():
            try: task_queue.get_nowait()
            except Exception: break
    broadcast()
    return jsonify(downloads)


@app.route("/api/events")
def events():
    def stream():
        q = Queue()
        with sse_lock:
            sse_clients.append(q)
        try:
            with state_lock:
                yield f"data: {json.dumps(downloads)}\n\n"
            while True:
                yield f"data: {q.get()}\n\n"
        except GeneratorExit:
            with sse_lock:
                if q in sse_clients:
                    sse_clients.remove(q)
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


start_workers()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))