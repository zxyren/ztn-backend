import os
import threading
import time
import glob
from queue import Queue
from flask import Flask, request, jsonify, send_file, Response
from flask_cors import CORS
from yt_dlp import YoutubeDL
from yt_dlp.networking.impersonate import ImpersonateTarget
import shutil
import subprocess
import json
import logging
from datetime import datetime

app = Flask(__name__)
CORS(app)  # Enable CORS for React frontend

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Disable Flask's request logging for the SSE endpoint
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

# Use persistent directory for downloads
# In production (Koyeb), use /tmp/downloads or mount a persistent volume
DOWNLOAD_FOLDER = os.environ.get("DOWNLOAD_FOLDER", "/tmp/downloads")
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
print(f"✓ Download folder: {DOWNLOAD_FOLDER}")

def detect_ffmpeg():
    # Check if ffmpeg exists in PATH
    ffmpeg_path = shutil.which("ffmpeg")
    if ffmpeg_path:
        print(f"✓ FFmpeg detected at {ffmpeg_path}")
        return ffmpeg_path
    
    # fallback: try running ffmpeg
    try:
        result = subprocess.run(
            ["ffmpeg", "-version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        if result.returncode == 0:
            return "ffmpeg"
    except Exception:
        pass

    print("⚠ FFmpeg not found!")
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

# SSE client management
sse_clients = []
sse_lock = threading.Lock()

def broadcast_update():
    """Send update to all connected SSE clients"""
    with state_lock:
        data = json.dumps(downloads)
    
    with sse_lock:
        for client_queue in sse_clients[:]:
            try:
                client_queue.put(data)
            except:
                sse_clients.remove(client_queue)

def ydl_options(progress_cb):
    opts = {
        'outtmpl': os.path.join(DOWNLOAD_FOLDER, '%(title)s.%(ext)s'),
        'progress_hooks': [progress_cb],
        'restrictfilenames': True,
        'windowsfilenames': True,
        'updatetime': False,
        'noverifyhttpscert': True,
        'buffersize': 1024 * 64,
        'continuedl': True,
    }
    
    if FFMPEG_PATH:
        opts['format'] = 'bestvideo+bestaudio/best'
        opts['merge_output_format'] = 'mp4'
    else:
        # Fallback to single format if FFmpeg not available
        print("⚠ FFmpeg not available - downloading single format only")
        opts['format'] = 'best'  # Download best single format (no merging needed)

    try:
        opts['impersonate'] = ImpersonateTarget.from_str('chrome')
    except Exception:
        opts['http_headers'] = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        }

    return opts

def download_one(item):
    downloaded_filename = None
    logger.info(f"Starting download for item {item['id']}: {item['url']}")
    
    def progress_hook(d):
        if d['status'] == 'downloading':
            total = d.get('total_bytes') or d.get('total_bytes_estimate')
            if total:
                pct = d.get('downloaded_bytes', 0) * 100.0 / total
                with state_lock:
                    item['progress'] = max(0.0, min(100.0, pct))
                    item['status'] = 'Downloading'
                broadcast_update()  # Send update to clients
        elif d['status'] == 'finished':
            with state_lock:
                item['progress'] = 100.0
                item['status'] = 'Merging'
            broadcast_update()  # Send update to clients
            # Capture the filename when download finishes
            if 'filename' in d:
                nonlocal downloaded_filename
                downloaded_filename = d['filename']
                logger.info(f"Download finished, filename: {downloaded_filename}")

    opts = ydl_options(progress_hook)
    try:
        with YoutubeDL(opts) as ydl:
            # Extract info first to get the expected filename
            logger.info(f"Extracting info from: {item['url']}")
            info = ydl.extract_info(item['url'], download=False)
            expected_filename = ydl.prepare_filename(info)
            logger.info(f"Expected filename: {expected_filename}")
            
            logger.info(f"Starting download to: {DOWNLOAD_FOLDER}")
            ydl.download([item['url']])
        
        # Use the expected filename, or find the most recently modified file as fallback
        if os.path.exists(expected_filename):
            downloaded_filename = expected_filename
            logger.info(f"File found at expected location: {expected_filename}")
        else:
            # Fallback: Get the most recently modified file in download folder
            files = glob.glob(os.path.join(DOWNLOAD_FOLDER, '*'))
            if files:
                downloaded_filename = max(files, key=os.path.getmtime)
                logger.info(f"File found via fallback: {downloaded_filename}")
            else:
                logger.warning(f"No files found in {DOWNLOAD_FOLDER}")
        
        with state_lock:
            item['status'] = 'Completed'
            item['filename'] = os.path.basename(downloaded_filename) if downloaded_filename else None
            item['filepath'] = downloaded_filename if downloaded_filename else None
            downloads['completed'] += 1
        logger.info(f"Download completed for item {item['id']}: {item['filename']}")
        broadcast_update()  # Send update to clients
    except Exception as e:
        error_msg = f"Download failed: {str(e)}"
        logger.error(f"Item {item['id']}: {error_msg}", exc_info=True)
        with state_lock:
            item['status'] = 'Failed'
            item['error'] = str(e)
        broadcast_update()  # Send update to clients

def worker_loop():
    while True:
        url = task_queue.get()
        if url is None:
            break
        with state_lock:
            downloads['downloading'] += 1
            item = next((x for x in downloads['queue'] if x['url']==url and x['status']=='Queued'), None)
            if item:
                item['status'] = 'Starting'
        broadcast_update()  # Send update to clients
        if item:
            download_one(item)
        time.sleep(0.2)
        with state_lock:
            downloads['downloading'] -= 1
        broadcast_update()  # Send update to clients
        task_queue.task_done()

def start_workers():
    for _ in range(MAX_CONCURRENT):
        threading.Thread(target=worker_loop, daemon=True).start()

# API Routes
@app.route("/api/queue", methods=["POST"])
def queue_download():
    global next_id
    data = request.get_json(force=True, silent=True) or {}
    urls = data.get("urls", [])
    logger.info(f"Queue request received with {len(urls)} URLs")
    with state_lock:
        for raw_url in urls:
            url = (raw_url or "").strip()
            if not url:
                continue
            item = {"id": next_id, "url": url, "status": "Queued", "progress": 0.0}
            next_id += 1
            downloads['queue'].append(item)
            downloads['total'] += 1
            task_queue.put(url)
            logger.info(f"Added item {item['id']} to queue: {url}")
    broadcast_update()  # Send update to clients
    return jsonify(downloads)

@app.route("/api/upload", methods=["POST"])
def upload_file():
    global next_id
    f = request.files.get("file")
    if not f:
        logger.warning("Upload request with no file")
        return jsonify({"error": "No file"}), 400
    lines = [ln.strip() for ln in f.read().decode("utf-8", errors="ignore").splitlines() if ln.strip()]
    logger.info(f"File uploaded with {len(lines)} URLs")
    with state_lock:
        for url in lines:
            item = {"id": next_id, "url": url, "status": "Queued", "progress": 0.0}
            next_id += 1
            downloads['queue'].append(item)
            downloads['total'] += 1
            task_queue.put(url)
            logger.info(f"Added item {item['id']} from upload: {url}")
    broadcast_update()  # Send update to clients
    return jsonify(downloads)

@app.route("/api/status")
def status():
    with state_lock:
        return jsonify(downloads)

@app.route("/api/events")
def events():
    """Server-Sent Events endpoint for real-time updates"""
    def event_stream():
        client_queue = Queue()
        with sse_lock:
            sse_clients.append(client_queue)
        
        try:
            # Send initial state
            with state_lock:
                data = json.dumps(downloads)
            yield f"data: {data}\n\n"
            
            # Send updates as they occur
            while True:
                data = client_queue.get()
                yield f"data: {data}\n\n"
        except GeneratorExit:
            with sse_lock:
                if client_queue in sse_clients:
                    sse_clients.remove(client_queue)
    
    # return Response(event_stream(), mimetype="text/event-stream")
    return Response(
        event_stream(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no"
        }
)


@app.route("/api/download/<int:item_id>")
def download_file(item_id):
    """Serve the downloaded file to trigger browser download"""
    logger.info(f"Download request for item {item_id}")
    with state_lock:
        item = next((x for x in downloads['queue'] if x['id'] == item_id), None)
    
    if not item:
        logger.warning(f"Item {item_id} not found")
        return jsonify({"error": "Item not found"}), 404
    
    if item['status'] != 'Completed':
        logger.warning(f"Item {item_id} not ready. Status: {item['status']}")
        return jsonify({"error": f"File not ready. Status: {item['status']}"}), 400
    
    filepath = item.get('filepath')
    if not filepath or not os.path.exists(filepath):
        logger.error(f"File not found for item {item_id}. Filepath: {filepath}")
        return jsonify({"error": "File not found on server"}), 404
    
    filename = item.get('filename', 'video.mp4')
    logger.info(f"Serving file {filename} for item {item_id}")
    return send_file(filepath, as_attachment=True, download_name=filename)

@app.route("/api/clear", methods=["POST"])
def clear_downloads():
    global next_id
    
    with state_lock:
        # Clean up old files before clearing
        for item in downloads['queue']:
            filepath = item.get('filepath')
            if filepath and os.path.exists(filepath):
                try:
                    os.remove(filepath)
                except:
                    pass
        
        downloads['total'] = 0
        downloads['completed'] = 0
        downloads['downloading'] = 0
        downloads['queue'] = []
        next_id = 1
        # Clear the task queue
        while not task_queue.empty():
            try:
                task_queue.get_nowait()
            except:
                break
    broadcast_update()  # Send update to clients
    return jsonify(downloads)

# Add a root route for testing
@app.route("/")
def index():
    return jsonify({
        "message": "Video Downloader API",
        "endpoints": [
            "/api/status",
            "/api/events (SSE)",
            "/api/queue",
            "/api/upload",
            "/api/download/<id>",
            "/api/clear"
        ]
    })
    
# Start workers for Gunicorn & production
start_workers()
logger.info(f"✓ Workers started. Download folder: {DOWNLOAD_FOLDER}")
logger.info(f"✓ FFmpeg: {'Available' if FFMPEG_PATH else 'Not available'}")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    logger.info(f"Starting Flask app on port {port}")
    app.run(host="0.0.0.0", port=port)
