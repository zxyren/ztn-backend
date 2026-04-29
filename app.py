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
CORS(app)

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

DOWNLOAD_FOLDER = tempfile.mkdtemp(prefix="video_downloader_")
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
print(f"✓ Temporary download folder: {DOWNLOAD_FOLDER}")


def detect_ffmpeg():
    ffmpeg_path = shutil.which("ffmpeg")
    if ffmpeg_path:
        print(f"✓ FFmpeg detected at {ffmpeg_path}")
        return ffmpeg_path
    try:
        result = subprocess.run(
            ["ffmpeg", "-version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        if result.returncode == 0:
            return "ffmpeg"
    except Exception:
        pass
    print("⚠ FFmpeg not found — MP3 conversion and video merging unavailable")
    return None


FFMPEG_PATH = detect_ffmpeg()
MAX_CONCURRENT = 1

downloads = {
    "total": 0,
    "completed": 0,
    "downloading": 0,
    "queue": []
}
next_id = 1
task_queue = Queue()
state_lock = threading.Lock()
cancelled_ids = set()

sse_clients = []
sse_lock = threading.Lock()


def broadcast_update():
    with state_lock:
        data = json.dumps(downloads)
    with sse_lock:
        for client_queue in sse_clients[:]:
            try:
                client_queue.put(data)
            except Exception:
                sse_clients.remove(client_queue)


def ydl_options(progress_cb, format_type='video'):
    opts = {
        'outtmpl': os.path.join(DOWNLOAD_FOLDER, '%(title)s.%(ext)s'),
        'progress_hooks': [progress_cb],
        'restrictfilenames': True,
        'windowsfilenames': True,
        'updatetime': False,
        'noverifyhttpscert': True,
        'buffersize': 1024 * 64,
        'continuedl': True,
        '--no-check-certificate': True,
        'http_headers': {
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/120.0.0.0 Safari/537.36'
            ),
        },
    }

    if format_type == 'audio':
        if not FFMPEG_PATH:
            raise Exception("FFmpeg is required for audio extraction.")

        opts['format'] = 'bestaudio/best'   # ← KEY FIX (fallback included)

        opts['postprocessors'] = [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }]
    else:
        if FFMPEG_PATH:
            opts['format'] = 'bestvideo+bestaudio/best'
            opts['merge_output_format'] = 'mp4'
        else:
            opts['format'] = 'best'

    return opts


def download_one(item):
    downloaded_filename = None
    format_type = item.get('format', 'video')

    def progress_hook(d):
        nonlocal downloaded_filename
        # Check if download was cancelled
        with state_lock:
            if item['id'] in cancelled_ids:
                raise Exception("Download cancelled by user")
        
        if d['status'] == 'downloading':
            total = d.get('total_bytes') or d.get('total_bytes_estimate')
            if total:
                pct = d.get('downloaded_bytes', 0) * 100.0 / total
                with state_lock:
                    item['progress'] = max(0.0, min(100.0, round(pct, 1)))
                    item['status'] = 'Downloading'
                broadcast_update()
        elif d['status'] == 'finished':
            with state_lock:
                item['progress'] = 100.0
                # Show "Converting" instead of "Processing" — more accurate
                item['status'] = 'Converting' if format_type == 'audio' else 'Merging'
            broadcast_update()
            if 'filename' in d:
                downloaded_filename = d['filename']

    try:
        opts = ydl_options(progress_hook, format_type)
    except Exception as e:
        with state_lock:
            item['status'] = 'Error'
            item['error'] = str(e)
        broadcast_update()
        return

    try:
        with YoutubeDL(opts) as ydl:
            info = ydl.extract_info(item['url'], download=False)
            expected_filename = ydl.prepare_filename(info)
            ydl.download([item['url']])

        if format_type == 'audio':
            base = os.path.splitext(expected_filename)[0]
            mp3_path = base + '.mp3'
            if os.path.exists(mp3_path):
                downloaded_filename = mp3_path
            else:
                # Fallback: find newest mp3 in folder
                mp3_files = glob.glob(os.path.join(DOWNLOAD_FOLDER, '*.mp3'))
                if mp3_files:
                    downloaded_filename = max(mp3_files, key=os.path.getmtime)
                else:
                    raise Exception("MP3 file was not created — FFmpeg post-processor may have failed")
        else:
            if os.path.exists(expected_filename):
                downloaded_filename = expected_filename
            else:
                files = [f for f in glob.glob(os.path.join(DOWNLOAD_FOLDER, '*')) if os.path.isfile(f)]
                if files:
                    downloaded_filename = max(files, key=os.path.getmtime)

        if not downloaded_filename or not os.path.exists(str(downloaded_filename)):
            raise Exception("Downloaded file not found")

        file_size = os.path.getsize(str(downloaded_filename))
        if file_size < 1024:
            raise Exception(f"File too small ({file_size} bytes) — download may have failed")

        with state_lock:
            item['status'] = 'Completed'
            item['filename'] = os.path.basename(str(downloaded_filename))
            item['filepath'] = str(downloaded_filename)
            downloads['completed'] += 1
        broadcast_update()

    except Exception as e:
        error_msg = str(e)
        is_cancelled = "cancelled" in error_msg.lower()
        print(f"--- DOWNLOAD FAILED: {e} ---")
        with state_lock:
            item['status'] = 'Cancelled' if is_cancelled else 'Error'
            item['error'] = error_msg
            # Clean up partial file if download was cancelled
            if is_cancelled and downloaded_filename and os.path.exists(str(downloaded_filename)):
                try:
                    os.remove(str(downloaded_filename))
                except Exception:
                    pass
        broadcast_update()


def worker_loop():
    while True:
        item_id = task_queue.get()
        if item_id is None:
            break

        with state_lock:
            # Skip if already cancelled before even starting
            if item_id in cancelled_ids:
                downloads['downloading'] -= 1 if item_id not in cancelled_ids or downloads['downloading'] > 0 else 0
                task_queue.task_done()
                broadcast_update()
                continue
            
            downloads['downloading'] += 1
            item = next(
                (x for x in downloads['queue'] if x['id'] == item_id and x['status'] == 'Queued'),
                None
            )
            if item:
                item['status'] = 'Starting'
        broadcast_update()

        if item:
            download_one(item)

        time.sleep(0.2)

        with state_lock:
            downloads['downloading'] -= 1
            # Remove from cancelled set after download is done
            cancelled_ids.discard(item_id)
        broadcast_update()
        task_queue.task_done()


def start_workers():
    for _ in range(MAX_CONCURRENT):
        threading.Thread(target=worker_loop, daemon=True).start()


# ---------------------------------------------------------------------------
# API Routes
# ---------------------------------------------------------------------------

@app.route("/api/queue", methods=["POST"])
def queue_download():
    global next_id
    data = request.get_json(force=True, silent=True) or {}
    urls = data.get("urls", [])
    format_type = data.get("format", "video")

    with state_lock:
        for raw_url in urls:
            url = (raw_url or "").strip()
            if not url:
                continue
            item = {
                "id": next_id,
                "url": url,
                "status": "Queued",
                "progress": 0.0,
                "format": format_type,
            }
            next_id += 1
            downloads['queue'].append(item)
            downloads['total'] += 1
            task_queue.put(item["id"])

    broadcast_update()
    return jsonify(downloads)


@app.route("/api/upload", methods=["POST"])
def upload_file():
    global next_id
    f = request.files.get("file")
    if not f:
        return jsonify({"error": "No file"}), 400

    format_type = request.form.get("format", "video")
    lines = [
        ln.strip()
        for ln in f.read().decode("utf-8", errors="ignore").splitlines()
        if ln.strip()
    ]

    with state_lock:
        for url in lines:
            item = {
                "id": next_id,
                "url": url,
                "status": "Queued",
                "progress": 0.0,
                "format": format_type,
            }
            next_id += 1
            downloads['queue'].append(item)
            downloads['total'] += 1
            task_queue.put(item["id"])

    broadcast_update()
    return jsonify(downloads)


@app.route("/api/status")
def status():
    with state_lock:
        return jsonify(downloads)


@app.route("/api/events")
def events():
    def event_stream():
        client_queue = Queue()
        with sse_lock:
            sse_clients.append(client_queue)
        try:
            with state_lock:
                data = json.dumps(downloads)
            yield f"data: {data}\n\n"
            while True:
                data = client_queue.get()
                yield f"data: {data}\n\n"
        except GeneratorExit:
            with sse_lock:
                if client_queue in sse_clients:
                    sse_clients.remove(client_queue)

    return Response(
        event_stream(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/api/download/<int:item_id>")
def download_file(item_id):
    with state_lock:
        item = next((x for x in downloads['queue'] if x['id'] == item_id), None)

    if not item:
        return jsonify({"error": "Item not found"}), 404
    if item['status'] != 'Completed':
        return jsonify({"error": "File not ready yet"}), 400

    filepath = item.get('filepath')
    if not filepath or not os.path.exists(filepath):
        return jsonify({"error": "File missing on disk"}), 404

    return send_file(
        filepath,
        as_attachment=True,
        download_name=item.get('filename', 'download')
    )


@app.route("/api/cancel/<int:item_id>", methods=["POST"])
def cancel_download(item_id):
    with state_lock:
        item = next((x for x in downloads['queue'] if x['id'] == item_id), None)
        if not item:
            return jsonify({"error": "Item not found"}), 404
        
        # Mark as cancelled
        cancelled_ids.add(item_id)
        
        # If already completed or failed, just mark as cancelled
        if item['status'] in ['Completed', 'Error', 'Cancelled']:
            item['status'] = 'Cancelled'
        else:
            item['status'] = 'Cancelling'
    
    broadcast_update()
    return jsonify({"success": True, "message": "Download cancelled"})


@app.route("/api/clear", methods=["POST"])
def clear_downloads():
    global next_id

    with state_lock:
        for item in downloads['queue']:
            fp = item.get('filepath')
            if fp and os.path.exists(fp):
                try:
                    os.remove(fp)
                except Exception:
                    pass

        downloads['total'] = 0
        downloads['completed'] = 0
        downloads['downloading'] = 0
        downloads['queue'] = []
        next_id = 1
        cancelled_ids.clear()

        while not task_queue.empty():
            try:
                task_queue.get_nowait()
            except Exception:
                break

    broadcast_update()
    return jsonify(downloads)


@app.route("/api/thumbnail", methods=["POST"])
def get_thumbnail():
    url = (request.get_json(force=True, silent=True) or {}).get("url", "").strip()
    if not url:
        return jsonify({"error": "No URL provided"}), 400
    try:
        opts = {
            'quiet': True,
            'skip_download': True,
            'socket_timeout': 10,
            'http_headers': {'User-Agent': 'Mozilla/5.0'},
        }
        info = YoutubeDL(opts).extract_info(url, download=False)
        if not info:
            return jsonify({"error": "No info returned"}), 404

        thumbnail = None
        thumbs = [t for t in info.get('thumbnails', []) if t.get('url')]
        if thumbs:
            low_q = [t for t in thumbs if 240 <= t.get('width', 0) <= 640]
            thumbnail = (
                min(low_q, key=lambda x: abs(x.get('width', 0) - 480))['url']
                if low_q
                else min(thumbs, key=lambda x: x.get('width', 999999))['url']
            )
        thumbnail = thumbnail or info.get('thumbnail')
        if thumbnail:
            return jsonify({"thumbnail": thumbnail, "title": info.get('title', '')})
        return jsonify({"error": "No thumbnail found"}), 404
    except Exception as e:
        print(f"Thumbnail error [{url}]: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/")
def index():
    try:
        with open(os.path.join(os.path.dirname(__file__), 'index.html'), 'r') as f:
            return f.read()
    except Exception:
        return jsonify({
            "message": "Video Downloader API is running",
            "endpoints": [
                "GET  /api/status",
                "GET  /api/events  (SSE)",
                "POST /api/queue",
                "POST /api/upload",
                "GET  /api/download/<id>",
                "POST /api/clear",
                "POST /api/thumbnail",
            ]
        })


start_workers()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)