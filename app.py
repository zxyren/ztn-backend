import os, threading, time, tempfile, glob, json, logging, shutil, subprocess, zipfile
from queue import Queue
from flask import Flask, request, jsonify, send_file, Response
from flask_cors import CORS
from yt_dlp import YoutubeDL

app = Flask(__name__)
CORS(app)
logging.getLogger('werkzeug').setLevel(logging.ERROR)

DOWNLOAD_FOLDER = tempfile.mkdtemp(prefix="ydl_")
FFMPEG  = shutil.which("ffmpeg")  or "ffmpeg"
FFPROBE = shutil.which("ffprobe") or "ffprobe"
MAX_CONCURRENT = 2

print(f"✓ ffmpeg : {FFMPEG}")
print(f"✓ ffprobe: {FFPROBE}")

# ── State ──────────────────────────────────────────────────────────────────
lock = threading.Lock()
sse_lock = threading.Lock()
task_queue = Queue()
sessions, next_ids, cancelled, sse_clients = {}, {}, set(), {}


def get_session(sid):
    if sid not in sessions:
        sessions[sid] = {"total": 0, "completed": 0, "downloading": 0, "queue": []}
        next_ids[sid] = 1
    return sessions[sid]


def get_item(sid, iid):
    d = sessions.get(sid, {})
    return next((x for x in d.get("queue", []) if x["id"] == iid), None)


def broadcast(sid):
    data = json.dumps(sessions.get(sid, {}))
    with sse_lock:
        for q in sse_clients.get(sid, [])[:]:
            try: q.put(data)
            except: sse_clients[sid].remove(q)


def sid_from_request():
    return (request.args.get("session_id") or request.headers.get("X-Session-ID") or "").strip()


def require_sid():
    s = sid_from_request()
    return (s, None) if s else (None, (jsonify({"error": "Missing session_id"}), 400))


def item_dir(sid, item_id):
    d = os.path.join(DOWNLOAD_FOLDER, f"{sid[:8]}_{item_id}")
    os.makedirs(d, exist_ok=True)
    return d


def ffmpeg_dir():
    return os.path.dirname(FFMPEG) if os.path.isabs(FFMPEG) else None


# ── yt-dlp option builders ─────────────────────────────────────────────────

def build_video_opts(hook, out_dir):
    return {
        "outtmpl": os.path.join(out_dir, "%(title)s.%(ext)s"),
        "progress_hooks": [hook],
        "restrictfilenames": True, "windowsfilenames": True,
        "updatetime": False, "noverifyhttpscert": True,
        "retries": 10, "fragment_retries": 10,
        "socket_timeout": 15,
        "concurrent_fragment_downloads": 4,
        "ffmpeg_location": ffmpeg_dir(),
        "format": "bestvideo+bestaudio/best",
        "merge_output_format": "mp4",
        "postprocessors": [{"key": "FFmpegVideoConvertor", "preferedformat": "mp4"},
                           {"key": "FFmpegMetadata"}],
        "postprocessor_args": {
            "ffmpegvideoconvertor": ["-c:v", "copy", "-c:a", "aac", "-b:a", "192k"],
        },
        "http_headers": {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
    }
    

def build_audio_opts(hook, out_dir):
    return {
        "outtmpl": os.path.join(out_dir, "%(title)s.%(ext)s"),
        "progress_hooks": [hook],
        "restrictfilenames": True, "windowsfilenames": True,
        "updatetime": False, "noverifyhttpscert": True,
        "retries": 10, "fragment_retries": 10,
        "socket_timeout": 15,
        "ffmpeg_location": ffmpeg_dir(),
        "format": "bestaudio/best",
        "postprocessors": [{"key": "FFmpegExtractAudio",
                            "preferredcodec": "mp3", "preferredquality": "192"}],
        "postprocessor_args": {
            "ffmpegextractaudio": ["-vn", "-c:a", "libmp3lame", "-b:a", "192k"],
        },
        "http_headers": {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
    }


def find_output(out_dir, ext):
    cands = [f for f in glob.glob(os.path.join(out_dir, f"*.{ext}"))
             if not f.endswith(".part")]
    if not cands:
        cands = [f for f in glob.glob(os.path.join(out_dir, "*"))
                 if os.path.isfile(f) and not f.endswith(".part")]
    return max(cands, key=os.path.getmtime) if cands else None


# ── Image download (gallery-dl) — images only, no audio ───────────────────

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif", ".bmp", ".tiff", ".heic"}

def download_image(sid, item_id):
    with lock:
        item = get_item(sid, item_id)
        if not item or (sid, item_id) in cancelled: return

    out_dir = item_dir(sid, item_id)

    def upd(status, progress=None, **kw):
        with lock:
            item["status"] = status
            if progress is not None: item["progress"] = progress
            item.update(kw)
        broadcast(sid)

    upd("Downloading", 10)
    try:
        proc = subprocess.Popen(
            ["gallery-dl", "--directory", out_dir, item["url"]],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        while proc.poll() is None:
            time.sleep(0.5)
            with lock:
                if (sid, item_id) in cancelled:
                    proc.kill()
                    raise Exception("Cancelled")
        
        out, err = proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(err.strip() or "Gallery-dl failed")

        # Collect only image files — skip any audio/video gallery-dl pulled in
        all_files = [
            os.path.join(r, f)
            for r, _, fs in os.walk(out_dir) for f in fs
            if not f.endswith((".tmp", ".part"))
        ]
        files = sorted(
            [f for f in all_files if os.path.splitext(f)[1].lower() in IMAGE_EXTS],
            key=os.path.getmtime,
        )
        if not files:
            raise RuntimeError(err.strip() or "No images downloaded")

        if len(files) == 1:
            fp, fname = files[0], os.path.basename(files[0])
        else:
            fname = f"images_{item_id}.zip"
            fp = os.path.join(DOWNLOAD_FOLDER, fname)
            with zipfile.ZipFile(fp, "w", zipfile.ZIP_DEFLATED) as z:
                for f in files:
                    z.write(f, os.path.relpath(f, out_dir))

        with lock:
            item.update(status="Completed", progress=100.0,
                        filename=fname, filepath=fp, image_count=len(files))
            sessions[sid]["completed"] += 1
        broadcast(sid)

    except Exception as e:
        upd("Cancelled" if "Cancelled" in str(e) else "Error", error=str(e))


# ── Media download (yt-dlp) ────────────────────────────────────────────────

def download_media(sid, item_id):
    with lock:
        item = get_item(sid, item_id)
        if not item or (sid, item_id) in cancelled: return

    fmt = item.get("format", "video")
    out_dir = item_dir(sid, item_id)
    out_ext = "mp3" if fmt == "audio" else "mp4"

    def hook(d):
        with lock:
            if (sid, item_id) in cancelled: raise Exception("Cancelled")
        if d["status"] == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate")
            if total:
                pct = round(d["downloaded_bytes"] * 100 / total, 1)
                with lock: item.update(progress=pct, status="Downloading")
                broadcast(sid)
        elif d["status"] == "finished":
            with lock: item.update(progress=99.0,
                                   status="Converting" if fmt == "audio" else "Merging")
            broadcast(sid)

    try:
        opts = build_audio_opts(hook, out_dir) if fmt == "audio" else build_video_opts(hook, out_dir)
        with YoutubeDL(opts) as ydl:
            ydl.download([item["url"]])

        time.sleep(1.5)

        fp = find_output(out_dir, out_ext)
        if not fp or not os.path.exists(fp) or os.path.getsize(fp) < 1024:
            raise Exception("Output file missing or too small after download")

        with lock:
            item.update(status="Completed", progress=100.0,
                        filename=os.path.basename(fp), filepath=fp)
            sessions[sid]["completed"] += 1
        broadcast(sid)

    except Exception as e:
        msg = str(e)
        with lock:
            item.update(status="Cancelled" if "cancelled" in msg.lower() else "Error",
                        error=msg)
        broadcast(sid)


# ── Worker ─────────────────────────────────────────────────────────────────

def worker_loop():
    while True:
        sid, iid = task_queue.get()
        with lock:
            if (sid, iid) in cancelled:
                task_queue.task_done(); continue
            d = sessions.get(sid)
            if d: d["downloading"] += 1
            item = get_item(sid, iid)
            if item and item["status"] == "Queued": item["status"] = "Starting"
        broadcast(sid)
        if item:
            if item.get("format") == "image":
                download_image(sid, iid)
            else:
                download_media(sid, iid)
        time.sleep(0.2)
        with lock:
            if d: d["downloading"] = max(0, d["downloading"] - 1)
            cancelled.discard((sid, iid))
        broadcast(sid)
        task_queue.task_done()


for _ in range(MAX_CONCURRENT):
    threading.Thread(target=worker_loop, daemon=True).start()


# ── Routes ─────────────────────────────────────────────────────────────────

@app.route("/api/queue", methods=["POST"])
def queue_download():
    sid, err = require_sid()
    if err: return err
    data = request.get_json(force=True, silent=True) or {}
    fmt = data.get("format", "video")
    with lock:
        d = get_session(sid)
        for url in [u.strip() for u in data.get("urls", []) if u.strip()]:
            iid = next_ids[sid]; next_ids[sid] += 1
            d["queue"].append({"id": iid, "url": url, "status": "Queued",
                               "progress": 0.0, "format": fmt})
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
            iid = next_ids[sid]; next_ids[sid] += 1
            d["queue"].append({"id": iid, "url": url, "status": "Queued",
                               "progress": 0.0, "format": fmt})
            d["total"] += 1
            task_queue.put((sid, iid))
    broadcast(sid)
    return jsonify(sessions[sid])


@app.route("/api/cancel/<int:item_id>", methods=["POST"])
def cancel_item(item_id):
    sid, err = require_sid()
    if err: return err
    with lock:
        item = get_item(sid, item_id)
        if not item: return jsonify({"error": "Not found"}), 404
        if item["status"] in ("Completed", "Error", "Cancelled"):
            return jsonify({"error": f"Cannot cancel: {item['status']}"}), 400
        cancelled.add((sid, item_id))
        item["status"] = "Cancelled"
    broadcast(sid)
    return jsonify(sessions[sid])


@app.route("/api/download/<int:item_id>")
def download_file(item_id):
    sid, err = require_sid()
    if err: return err
    with lock: item = get_item(sid, item_id)
    if not item: return jsonify({"error": "Not found"}), 404
    if item["status"] != "Completed": return jsonify({"error": "Not ready"}), 400
    fp = item.get("filepath")
    if not fp or not os.path.exists(fp): return jsonify({"error": "File missing"}), 404
    return send_file(fp, as_attachment=True, download_name=item.get("filename", "download"))


@app.route("/api/remove/<int:item_id>", methods=["POST"])
def remove_item(item_id):
    sid, err = require_sid()
    if err: return err
    with lock:
        item = get_item(sid, item_id)
        if not item: return jsonify({"error": "Not found"}), 404
        
        d = sessions.get(sid)
        if item in d.get("queue", []):
            d["queue"].remove(item)
            if item["status"] == "Completed":
                d["completed"] = max(0, d["completed"] - 1)
            d["total"] = max(0, d["total"] - 1)
            
            if item["status"] in ("Queued", "Starting", "Downloading", "Converting", "Merging"):
                cancelled.add((sid, item_id))
            
            fp = item.get("filepath")
            if fp and os.path.exists(fp):
                try: os.remove(fp)
                except: pass
                
            out_dir = item_dir(sid, item_id)
            if os.path.exists(out_dir):
                try: shutil.rmtree(out_dir)
                except: pass
    broadcast(sid)
    return jsonify(sessions[sid])


@app.route("/api/clear", methods=["POST"])
def clear():
    sid, err = require_sid()
    if err: return err
    with lock:
        for item in sessions.get(sid, {}).get("queue", []):
            if item["status"] in ("Queued", "Starting", "Downloading", "Converting", "Merging"):
                cancelled.add((sid, item["id"]))

            fp = item.get("filepath")
            if fp and os.path.exists(fp):
                try: os.remove(fp)
                except: pass

            out_dir = item_dir(sid, item["id"])
            if os.path.exists(out_dir):
                try: shutil.rmtree(out_dir)
                except: pass

        sessions[sid] = {"total": 0, "completed": 0, "downloading": 0, "queue": []}
        next_ids[sid] = 1
    broadcast(sid)
    return jsonify(sessions[sid])


@app.route("/api/events")
def events():
    sid = sid_from_request()
    if not sid: return Response("data: {}\n\n", mimetype="text/event-stream")

    def stream():
        q = Queue()
        with sse_lock: sse_clients.setdefault(sid, []).append(q)
        try:
            with lock: yield f"data: {json.dumps(get_session(sid))}\n\n"
            while True: yield f"data: {q.get()}\n\n"
        except GeneratorExit:
            with sse_lock:
                if q in sse_clients.get(sid, []): sse_clients[sid].remove(q)

    return Response(stream(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

# ── Thumbnail retrieval ─────────────────────────────────────────────────────
from handlers import resolve_thumbnail

@app.route("/api/thumbnail", methods=["POST"])
def thumbnail():
    url = (request.get_json(force=True, silent=True) or {}).get("url", "").strip()
    if not url:
        return jsonify({"error": "No URL"}), 400
    result = resolve_thumbnail(url, ffmpeg_dir())
    if result:
        return jsonify(result)
    return jsonify({"error": "No thumbnail found"}), 404


@app.route("/")
def index():
    try:
        with open(os.path.join(os.path.dirname(__file__), "index.html")) as f: return f.read()
    except: return jsonify({"status": "API running"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8001)))