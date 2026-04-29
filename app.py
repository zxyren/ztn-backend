import os
import threading
import time
import tempfile
import glob
from queue import Queue
from flask import Flask, request, jsonify, send_file, Response
from flask_cors import CORS
from yt_dlp import YoutubeDL
import shutil
import subprocess
import json
import logging

app = Flask(__name__)

# Allow X-Session-ID header from any origin
CORS(app, supports_credentials=True, allow_headers=["Content-Type", "X-Session-ID"])

logging.getLogger('werkzeug').setLevel(logging.ERROR)

DOWNLOAD_FOLDER = tempfile.mkdtemp(prefix="video_downloader_")
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
print(f"✓ Download folder: {DOWNLOAD_FOLDER}")


def detect_ffmpeg():
    path = shutil.which("ffmpeg")
    if path:
        print(f"✓ FFmpeg: {path}")
        return path
    try:
        if subprocess.run(["ffmpeg", "-version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE).returncode == 0:
            return "ffmpeg"
    except Exception:
        pass
    print("⚠ FFmpeg not found")
    return None


FFMPEG_PATH = detect_ffmpeg()
MAX_CONCURRENT = 2

# ---------------------------------------------------------------------------
# Per-session state  { session_id -> { total, completed, downloading, queue } }
# ---------------------------------------------------------------------------
sessions = {}
session_lock = threading.Lock()

task_queue = Queue()
next_ids = {}

sse_clients = {}
sse_lock = threading.Lock()


def get_session(session_id):
    with session_lock:
        if session_id not in sessions:
            sessions[session_id] = {"total": 0, "completed": 0, "downloading": 0, "queue": []}
            next_ids[session_id] = 1
        return sessions[session_id]


def broadcast(session_id):
    with session_lock:
        data = json.dumps(sessions.get(session_id, {}))
    with sse_lock:
        for q in sse_clients.get(session_id, [])[:]:
            try:
                q.put(data)
            except Exception:
                pass


def ydl_opts(progress_cb, format_type):
    opts = {
        'outtmpl': os.path.join(DOWNLOAD_FOLDER, '%(title)s.%(ext)s'),
        'progress_hooks': [progress_cb],
        'restrictfilenames': True,
        'windowsfilenames': True,
        'updatetime': False,
        'noverifyhttpscert': True,
        'buffersize': 1024 * 64,
        'http_headers': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        },
    }
    if format_type == 'audio':
        if not FFMPEG_PATH:
            raise Exception("FFmpeg not installed — required for MP3 conversion")
        opts['format'] = 'bestaudio[ext=m4a]/bestaudio[ext=mp3]/bestaudio'
        opts['postprocessors'] = [{'key': 'FFmpegExtractAudio', 'preferredcodec': 'mp3', 'preferredquality': '0'}]
        opts['postprocessor_args'] = {'ffmpegextractaudio': ['-c:a', 'copy']}
    else:
        opts['format'] = 'bestvideo+bestaudio/best' if FFMPEG_PATH else 'best'
        if FFMPEG_PATH:
            opts['merge_output_format'] = 'mp4'
    return opts


def find_item(session_id, item_id):
    d = sessions.get(session_id)
    if not d:
        return None
    return next((x for x in d['queue'] if x['id'] == item_id), None)


def download_one(session_id, item_id):
    downloaded_filename = None

    with session_lock:
        item = find_item(session_id, item_id)
        if not item or item['status'] == 'Cancelled':
            return

    format_type = item.get('format', 'video')

    def progress_hook(d):
        nonlocal downloaded_filename
        with session_lock:
            it = find_item(session_id, item_id)
            if not it or it['status'] == 'Cancelled':
                raise Exception("Cancelled by user")
        if d['status'] == 'downloading':
            total = d.get('total_bytes') or d.get('total_bytes_estimate')
            if total:
                pct = round(d.get('downloaded_bytes', 0) * 100.0 / total, 1)
                with session_lock:
                    it = find_item(session_id, item_id)
                    if it:
                        it['progress'] = max(0.0, min(100.0, pct))
                        it['status'] = 'Downloading'
                broadcast(session_id)
        elif d['status'] == 'finished':
            with session_lock:
                it = find_item(session_id, item_id)
                if it:
                    it['progress'] = 100.0
                    it['status'] = 'Converting' if format_type == 'audio' else 'Merging'
            broadcast(session_id)
            if 'filename' in d:
                downloaded_filename = d['filename']

    try:
        opts = ydl_opts(progress_hook, format_type)
    except Exception as e:
        with session_lock:
            it = find_item(session_id, item_id)
            if it:
                it['status'] = 'Error'
                it['error'] = str(e)
        broadcast(session_id)
        return

    try:
        with YoutubeDL(opts) as ydl:
            with session_lock:
                it = find_item(session_id, item_id)
                if not it or it['status'] == 'Cancelled':
                    return
            info = ydl.extract_info(item['url'], download=False)
            expected_filename = ydl.prepare_filename(info)
            ydl.download([item['url']])

        if format_type == 'audio':
            base = os.path.splitext(expected_filename)[0]
            mp3_path = base + '.mp3'
            if os.path.exists(mp3_path):
                downloaded_filename = mp3_path
            else:
                mp3s = glob.glob(os.path.join(DOWNLOAD_FOLDER, '*.mp3'))
                downloaded_filename = max(mp3s, key=os.path.getmtime) if mp3s else None
        else:
            if os.path.exists(expected_filename):
                downloaded_filename = expected_filename
            else:
                files = [f for f in glob.glob(os.path.join(DOWNLOAD_FOLDER, '*')) if os.path.isfile(f)]
                downloaded_filename = max(files, key=os.path.getmtime) if files else None

        if not downloaded_filename or not os.path.exists(str(downloaded_filename)):
            raise Exception("Output file not found after download")
        if os.path.getsize(str(downloaded_filename)) < 1024:
            raise Exception("Output file too small — download likely failed")

        with session_lock:
            it = find_item(session_id, item_id)
            if it and it['status'] != 'Cancelled':
                it['status'] = 'Completed'
                it['filename'] = os.path.basename(str(downloaded_filename))
                it['filepath'] = str(downloaded_filename)
                sessions[session_id]['completed'] += 1
        broadcast(session_id)

    except Exception as e:
        err = str(e)
        with session_lock:
            it = find_item(session_id, item_id)
            if it and it['status'] != 'Cancelled':
                it['status'] = 'Error'
                it['error'] = err
        broadcast(session_id)
        print(f"FAILED [{session_id[:8]}] {err}")


def worker_loop():
    while True:
        session_id, item_id = task_queue.get()
        with session_lock:
            d = sessions.get(session_id)
            if d:
                d['downloading'] += 1
            it = find_item(session_id, item_id)
            if it and it['status'] == 'Queued':
                it['status'] = 'Starting'
        broadcast(session_id)

        with session_lock:
            it = find_item(session_id, item_id)
        if it and it['status'] != 'Cancelled':
            download_one(session_id, item_id)

        time.sleep(0.1)
        with session_lock:
            d = sessions.get(session_id)
            if d:
                d['downloading'] = max(0, d['downloading'] - 1)
        broadcast(session_id)
        task_queue.task_done()


def start_workers():
    for _ in range(MAX_CONCURRENT):
        threading.Thread(target=worker_loop, daemon=True).start()


def session_id_from_request():
    # Accept from header OR query param (query param fallback for SSE/EventSource)
    return (request.headers.get('X-Session-ID') or '').strip() or request.args.get('session_id', '').strip()


def enqueue_urls(session_id, urls, format_type):
    added = []
    with session_lock:
        d = get_session(session_id)
        for raw in urls:
            url = (raw or '').strip()
            if not url:
                continue
            item_id = next_ids[session_id]
            next_ids[session_id] += 1
            item = {'id': item_id, 'url': url, 'status': 'Queued', 'progress': 0.0, 'format': format_type}
            d['queue'].append(item)
            d['total'] += 1
            added.append(item_id)
    for item_id in added:
        task_queue.put((session_id, item_id))
    broadcast(session_id)
    with session_lock:
        return dict(sessions[session_id])


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/api/queue", methods=["POST"])
def queue_download():
    sid = session_id_from_request()
    if not sid:
        return jsonify({"error": "Missing X-Session-ID"}), 400
    data = request.get_json(force=True, silent=True) or {}
    return jsonify(enqueue_urls(sid, data.get("urls", []), data.get("format", "video")))


@app.route("/api/upload", methods=["POST"])
def upload_file():
    sid = session_id_from_request()
    if not sid:
        return jsonify({"error": "Missing X-Session-ID"}), 400
    f = request.files.get("file")
    if not f:
        return jsonify({"error": "No file"}), 400
    format_type = request.form.get("format", "video")
    lines = [l.strip() for l in f.read().decode("utf-8", errors="ignore").splitlines() if l.strip()]
    return jsonify(enqueue_urls(sid, lines, format_type))


@app.route("/api/status")
def status():
    sid = session_id_from_request()
    if not sid:
        return jsonify({"error": "Missing X-Session-ID"}), 400
    return jsonify(get_session(sid))


@app.route("/api/cancel/<int:item_id>", methods=["POST"])
def cancel_download(item_id):
    sid = session_id_from_request()
    if not sid:
        return jsonify({"error": "Missing X-Session-ID"}), 400
    with session_lock:
        it = find_item(sid, item_id)
        if not it:
            return jsonify({"error": "Item not found"}), 404
        if it['status'] in ('Completed', 'Error', 'Cancelled'):
            return jsonify({"error": f"Cannot cancel — status is {it['status']}"}), 400
        it['status'] = 'Cancelled'
    broadcast(sid)
    with session_lock:
        return jsonify(dict(sessions[sid]))


@app.route("/api/events")
def events():
    sid = session_id_from_request()
    if not sid:
        return Response("data: {}\n\n", mimetype="text/event-stream")

    def stream():
        q = Queue()
        with sse_lock:
            sse_clients.setdefault(sid, []).append(q)
        try:
            with session_lock:
                initial = json.dumps(sessions.get(sid, {}))
            yield f"data: {initial}\n\n"
            while True:
                yield f"data: {q.get()}\n\n"
        except GeneratorExit:
            with sse_lock:
                clients = sse_clients.get(sid, [])
                if q in clients:
                    clients.remove(q)

    return Response(stream(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/download/<int:item_id>")
def download_file(item_id):
    sid = session_id_from_request()
    if not sid:
        return jsonify({"error": "Missing session"}), 400
    with session_lock:
        it = find_item(sid, item_id)
    if not it:
        return jsonify({"error": "Item not found"}), 404
    if it['status'] != 'Completed':
        return jsonify({"error": "Not ready"}), 400
    fp = it.get('filepath')
    if not fp or not os.path.exists(fp):
        return jsonify({"error": "File missing on disk"}), 404
    return send_file(fp, as_attachment=True, download_name=it.get('filename', 'download'))


@app.route("/api/clear", methods=["POST"])
def clear_downloads():
    sid = session_id_from_request()
    if not sid:
        return jsonify({"error": "Missing session"}), 400
    with session_lock:
        d = sessions.get(sid, {})
        for it in d.get('queue', []):
            fp = it.get('filepath')
            if fp and os.path.exists(fp):
                try:
                    os.remove(fp)
                except Exception:
                    pass
        sessions[sid] = {"total": 0, "completed": 0, "downloading": 0, "queue": []}
        next_ids[sid] = 1
    broadcast(sid)
    with session_lock:
        return jsonify(sessions[sid])


@app.route("/api/thumbnail", methods=["POST"])
def get_thumbnail():
    url = (request.get_json(force=True, silent=True) or {}).get("url", "").strip()
    if not url:
        return jsonify({"error": "No URL"}), 400
    try:
        info = YoutubeDL({'quiet': True, 'skip_download': True, 'socket_timeout': 10}).extract_info(url, download=False)
        if not info:
            return jsonify({"error": "No info"}), 404
        thumbs = [t for t in info.get('thumbnails', []) if t.get('url')]
        thumbnail = None
        if thumbs:
            low_q = [t for t in thumbs if 240 <= t.get('width', 0) <= 640]
            thumbnail = (min(low_q, key=lambda x: abs(x.get('width', 0) - 480))['url']
                         if low_q else min(thumbs, key=lambda x: x.get('width', 999999))['url'])
        thumbnail = thumbnail or info.get('thumbnail')
        if thumbnail:
            return jsonify({"thumbnail": thumbnail, "title": info.get('title', '')})
        return jsonify({"error": "No thumbnail"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/")
def index():
    try:
        with open(os.path.join(os.path.dirname(__file__), 'index.html')) as f:
            return f.read()
    except Exception:
        return jsonify({"message": "Video Downloader API running"})


start_workers()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))