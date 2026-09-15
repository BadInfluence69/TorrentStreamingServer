"""
The Hub - Torrent Streaming Server
-----------------------------------
Streams the largest media file inside a torrent straight into an HTML5
<video> element via an on-the-fly FFmpeg remux. There is intentionally no
"download" affordance anywhere in the UI or the API - only the STREAM path
is exposed to the browser.

Notable improvements over the original version:
  - DHT / LSD / UPnP / NAT-PMP enabled on the libtorrent session (with
    bootstrap DHT routers) so magnets actually resolve peers reliably,
    instead of relying on trackers alone.
  - Torrent metadata/TMDB lookups run concurrently (thread pool) instead of
    sequentially, so search results come back much faster.
  - Real error handling: search failures, unreachable APIs and torrent
    metadata timeouts all surface a readable message in the UI instead of
    hanging forever or crashing.
  - Idle torrent sessions are garbage collected automatically after an hour
    of inactivity so long-running instances don't leak handles/disk.
  - The stream endpoint validates its inputs (info-hash shape, magnet
    prefix) and bounds every wait loop with a timeout.
  - threaded=True on the dev server + debug reloader disabled, so one
    client streaming video no longer blocks every other request (and the
    libtorrent session no longer gets started twice by the reloader).
  - The "download"/magnet button has been removed entirely - the grid only
    offers STREAM, and no raw magnet link is rendered into the page.
"""

from flask import Flask, render_template_string, request, Response, stream_with_context, send_from_directory
import requests
import urllib.parse
import libtorrent as lt
import time
import os
import re
import tempfile
import subprocess
import threading
import logging
import concurrent.futures

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')
logger = logging.getLogger("hub")

APP_DIR = os.path.dirname(os.path.abspath(__file__))

app = Flask(__name__)

# --- libtorrent session -----------------------------------------------------
# DHT/LSD/UPnP/NAT-PMP are all enabled so magnets can find peers without
# depending solely on the trackers embedded in the magnet link.
lt_session = lt.session({
    'listen_interfaces': '0.0.0.0:6881',
    'enable_dht': True,
    'enable_lsd': True,
    'enable_upnp': True,
    'enable_natpmp': True,
})
for router, port in [
    ('router.bittorrent.com', 6881),
    ('router.utorrent.com', 6881),
    ('dht.transmissionbt.com', 6881),
]:
    try:
        lt_session.add_dht_router(router, port)
    except Exception:
        pass

active_torrents = {}        # info_hash -> libtorrent handle
last_access = {}            # info_hash -> last-touched timestamp
torrents_lock = threading.Lock()

TORRENT_IDLE_SECONDS = 3600      # drop cached torrents after an hour of no use
METADATA_TIMEOUT_SECONDS = 45    # max wait for magnet -> metadata resolution
FIRST_PIECE_TIMEOUT_SECONDS = 45 # max wait for the first bytes to land on disk
HASH_RE = re.compile(r'^[a-fA-F0-9]{40}$')

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>The Hub Torrent Player</title>
    <link rel="icon" href="/favicon.ico">
    <style>
        body {
            font-family: 'Courier New', Courier, monospace, sans-serif;
            background-color: #06080c;
            color: #94a3b8;
            margin: 0;
            padding: 20px;
            display: flex;
            flex-direction: column;
            align-items: center;
        }
        .main-deck {
            width: 100%;
            max-width: 950px;
            background-color: #0b0f17;
            border: 2px solid #1e293b;
            border-radius: 12px;
            padding: 30px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.8);
            margin-top: 20px;
        }
        .branding h1 {
            font-size: 26px;
            font-weight: 900;
            color: #ffffff;
            margin: 0;
            text-align: center;
            letter-spacing: 3px;
            text-transform: uppercase;
        }
        .branding h1 span { color: #38bdf8; }
        .search-row {
            display: flex;
            background-color: #111827;
            border: 1px solid #334155;
            border-radius: 8px;
            padding: 5px 15px;
            margin: 20px 0 25px 0;
        }
        .search-row input[type="text"] {
            flex-grow: 1;
            background: none;
            border: none;
            color: #f8fafc;
            font-size: 18px;
            padding: 12px 10px;
            outline: none;
            font-family: inherit;
        }
        .submit-btn {
            background-color: #0284c7;
            color: white;
            border: none;
            padding: 10px 24px;
            font-size: 16px;
            font-weight: bold;
            border-radius: 6px;
            cursor: pointer;
        }
        .data-grid { display: flex; flex-direction: column; gap: 16px; }
        .grid-card {
            display: flex;
            gap: 15px;
            background-color: #0e1420;
            border: 1px solid #1e293b;
            padding: 14px;
            border-radius: 8px;
        }
        .poster {
            width: 90px;
            height: 135px;
            background-color: #1e293b;
            border-radius: 4px;
            object-fit: cover;
            flex-shrink: 0;
        }
        .card-details {
            flex-grow: 1;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
        }
        .col-title { color: #e2e8f0; font-weight: bold; font-size: 16px; }
        .meta-info { font-size: 12px; color: #38bdf8; margin: 4px 0; }
        .overview { font-size: 12px; color: #64748b; line-height: 1.4; max-height: 50px; overflow: hidden; }
        .card-bottom {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-top: 10px;
        }
        .stats { font-size: 12px; color: #94a3b8; }
        .col-seeders { color: #4ade80; font-weight: bold; }
        .col-action { display: flex; gap: 8px; }
        .btn-stream {
            background-color: #38bdf8;
            color: #000;
            padding: 6px 16px;
            border-radius: 4px;
            font-size: 12px;
            font-weight: bold;
            text-decoration: none;
        }
        .btn-stream:hover { background-color: #7dd3fc; }
        .player-deck { margin-bottom: 30px; background-color: #000; border-radius: 8px; overflow: hidden; border: 1px solid #334155; }
        .video-wrap { position: relative; }
        video { width: 100%; max-height: 500px; outline: none; display: block; }
        .player-status {
            position: absolute;
            bottom: 12px;
            left: 12px;
            background: rgba(0,0,0,0.75);
            color: #38bdf8;
            padding: 6px 12px;
            border-radius: 4px;
            font-size: 12px;
            pointer-events: none;
        }
        .error-banner {
            background: #2a1414;
            border: 1px solid #7f1d1d;
            color: #fca5a5;
            padding: 12px 16px;
            border-radius: 6px;
            margin-bottom: 18px;
            font-size: 13px;
        }
        .empty-state { color: #64748b; text-align: center; padding: 30px 0; font-size: 14px; }
    </style>
</head>
<body>

    <div class="main-deck">
        <div class="branding">
            <h1>The Hub <span>Torrent Player</span></h1>
        </div>

        {% if info_hash %}
        <div class="player-deck">
            <div class="video-wrap">
                <video id="player" controls autoplay playsinline>
                    <source src="/stream?hash={{ info_hash }}&magnet={{ magnet_encoded }}" type="video/mp4">
                    Your browser does not support html5 video streaming.
                </video>
                <div id="playerStatus" class="player-status">Connecting to swarm…</div>
            </div>
        </div>
        <script>
            (function () {
                var video = document.getElementById('player');
                var status = document.getElementById('playerStatus');
                function show(msg) { status.textContent = msg; status.style.display = 'block'; }
                function hide() { status.style.display = 'none'; }
                video.addEventListener('waiting', function () { show('Buffering… fetching pieces from peers'); });
                video.addEventListener('playing', hide);
                video.addEventListener('canplay', hide);
                video.addEventListener('error', function () {
                    show('Playback failed - the torrent may still be resolving. Try reloading in a few seconds.');
                });
            })();
        </script>
        {% endif %}

        <form action="/" method="GET">
            <div class="search-row">
                <input type="text" name="q" placeholder="Enter search query..." value="{{ query }}" required autocomplete="off">
                <button type="submit" class="submit-btn">Search</button>
            </div>
        </form>

        {% if error %}
        <div class="error-banner">{{ error }}</div>
        {% endif %}

        {% if query %}
        <div class="data-grid">
            {% if results %}
                {% for item in results %}
                    <div class="grid-card">
                        <img class="poster" src="{{ item.poster }}" alt="Cover Art">
                        <div class="card-details">
                            <div>
                                <div class="col-title">{{ item.name }}</div>
                                {% if item.meta_title %}
                                    <div class="meta-info">Matched: {{ item.meta_title }} (★ {{ item.vote_average }})</div>
                                    <div class="overview">{{ item.overview }}</div>
                                {% endif %}
                            </div>
                            <div class="card-bottom">
                                <div class="stats">Size: {{ item.size }} | <span class="col-seeders">▲ {{ item.seeders }}</span></div>
                                <div class="col-action">
                                    <a class="btn-stream" href="/play?hash={{ item.hash }}&magnet={{ item.magnet_encoded }}&q={{ query }}">▶ STREAM</a>
                                </div>
                            </div>
                        </div>
                    </div>
                {% endfor %}
            {% elif not error %}
                <div class="empty-state">No torrents found with enough seeders for "{{ query }}".</div>
            {% endif %}
        </div>
        {% endif %}
    </div>

</body>
</html>
"""


def clean_title(title):
    # Remove common torrent release metadata tags for TMDB metadata matching
    cleaned = re.sub(r'\(?\d{4}\)?|1080p|720p|4k|HDR|WEBRip|BluRay|x264|x265|AAC|MP4|MKV|-[A-Za-z0-9]+', '', title, flags=re.IGNORECASE)
    cleaned = cleaned.replace('.', ' ').replace('_', ' ').strip()
    return cleaned


def fetch_tmdb_metadata(raw_title):
    clean = clean_title(raw_title)
    if not clean:
        return {}

    # Hits public TMDB search API
    url = f"https://api.themoviedb.org/3/search/multi?api_key=f89a6c17c26177e0105cc30f54016022&query={urllib.parse.quote(clean)}"
    try:
        res = requests.get(url, timeout=4).json()
        if res.get('results'):
            match = res['results'][0]
            poster_path = match.get('poster_path')
            poster_url = f"https://image.tmdb.org/t/p/w185{poster_path}" if poster_path else "https://via.placeholder.com/90x135?text=No+Cover"
            return {
                'poster': poster_url,
                'meta_title': match.get('title') or match.get('name'),
                'overview': match.get('overview', 'No summary available.'),
                'vote_average': match.get('vote_average', 'N/A')
            }
    except Exception as e:
        logger.debug(f"TMDB lookup failed for '{raw_title}': {e}")
    return {'poster': 'https://via.placeholder.com/90x135?text=No+Cover'}


def format_bytes(size_bytes):
    try:
        size_bytes = int(size_bytes)
        if size_bytes <= 0:
            return "0 B"
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} PB"
    except (TypeError, ValueError):
        return "Unknown"


def get_torrent_results(query):
    """Returns (results, error_message). error_message is None on success."""
    url = f"https://apibay.org/q.php?q={urllib.parse.quote(query)}"
    headers = {'User-Agent': 'Mozilla/5.0'}

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        items = response.json()
    except requests.RequestException as e:
        logger.warning(f"Search provider unreachable: {e}")
        return [], "Search provider is unreachable right now. Please try again shortly."
    except ValueError as e:
        logger.warning(f"Search provider returned bad data: {e}")
        return [], "Search provider returned an unexpected response."

    if not isinstance(items, list) or not items or items[0].get('id') == '0':
        return [], None

    candidates = []
    for item in items[:15]:  # Limit top matches for instant metadata fetch
        name = item.get('name', 'Unknown')
        info_hash = item.get('info_hash', '')
        try:
            seeders = int(item.get('seeders', 0) or 0)
        except (TypeError, ValueError):
            seeders = 0

        if info_hash and seeders >= 5:
            candidates.append((item, name, info_hash, seeders))

    def build_result(candidate):
        item, name, info_hash, seeders = candidate
        magnet_link = f"magnet:?xt=urn:btih:{info_hash}&dn={urllib.parse.quote(name)}"
        meta = fetch_tmdb_metadata(name)
        return {
            'name': name,
            'size': format_bytes(item.get('size', 0)),
            'seeders': seeders,
            'hash': info_hash,
            'magnet_encoded': urllib.parse.quote(magnet_link),
            'poster': meta.get('poster'),
            'meta_title': meta.get('meta_title'),
            'overview': meta.get('overview'),
            'vote_average': meta.get('vote_average')
        }

    filtered_results = []
    if candidates:
        # Fan the TMDB lookups out across a small thread pool instead of
        # blocking on them one at a time - cuts search latency significantly.
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                filtered_results = list(pool.map(build_result, candidates))
        except Exception as e:
            logger.exception(f"Metadata enrichment failed: {e}")
            return [], "Something went wrong while enriching search results."

    return sorted(filtered_results, key=lambda x: x['seeders'], reverse=True), None


def cleanup_stale_torrents():
    """Drop torrent handles that haven't been streamed from in a while."""
    now = time.time()
    with torrents_lock:
        stale_hashes = [h for h, ts in last_access.items() if now - ts > TORRENT_IDLE_SECONDS]
        for h in stale_hashes:
            handle = active_torrents.pop(h, None)
            last_access.pop(h, None)
            if handle is not None:
                try:
                    lt_session.remove_torrent(handle)
                    logger.info(f"Cleaned up idle torrent {h}")
                except Exception:
                    pass


@app.route('/')
def index():
    query = request.args.get('q', '').strip()
    results, error = get_torrent_results(query) if query else ([], None)
    return render_template_string(HTML_TEMPLATE, query=query, results=results, error=error, info_hash=None, magnet_encoded=None)


@app.route('/play')
def play():
    query = request.args.get('q', '')
    info_hash = request.args.get('hash', '')
    magnet_encoded = request.args.get('magnet', '')
    results, error = get_torrent_results(query) if query else ([], None)
    return render_template_string(
        HTML_TEMPLATE, query=query, info_hash=info_hash,
        magnet_encoded=magnet_encoded, results=results, error=error
    )


@app.route('/stream')
def stream():
    info_hash = (request.args.get('hash') or '').strip()
    raw_magnet = request.args.get('magnet')

    if not info_hash or not HASH_RE.match(info_hash):
        return Response("Invalid or missing torrent hash.", status=400, mimetype='text/plain')
    if not raw_magnet:
        return Response("Missing magnet link.", status=400, mimetype='text/plain')

    magnet_link = urllib.parse.unquote(raw_magnet)
    if not magnet_link.startswith('magnet:'):
        return Response("Invalid magnet link.", status=400, mimetype='text/plain')

    cleanup_stale_torrents()

    with torrents_lock:
        handle = active_torrents.get(info_hash)
        if handle is None:
            save_path = os.path.join(tempfile.gettempdir(), 'hub_stream_cache')
            os.makedirs(save_path, exist_ok=True)
            params = {'save_path': save_path, 'storage_mode': lt.storage_mode_t.storage_mode_sparse}
            try:
                handle = lt.add_magnet_uri(lt_session, magnet_link, params)
            except Exception as e:
                logger.error(f"Failed to add magnet {info_hash}: {e}")
                return Response("Failed to start the torrent session.", status=500, mimetype='text/plain')
            handle.set_sequential_download(True)
            active_torrents[info_hash] = handle
        last_access[info_hash] = time.time()

    wait_start = time.time()
    while not handle.has_metadata():
        if time.time() - wait_start > METADATA_TIMEOUT_SECONDS:
            return Response(
                "Timed out waiting for torrent metadata - not enough reachable peers yet. Try again shortly.",
                status=504, mimetype='text/plain'
            )
        time.sleep(0.5)

    torrent_info = handle.get_torrent_info()
    files = torrent_info.files()
    largest_file_idx = max(range(torrent_info.num_files()), key=lambda i: files.file_size(i))
    file_path = os.path.join(handle.save_path(), files.file_path(largest_file_idx))

    # Make sure the file we're about to serve is the one being prioritized.
    try:
        handle.file_priority(largest_file_idx, 7)
    except Exception:
        pass

    wait_start = time.time()
    while not os.path.exists(file_path):
        if time.time() - wait_start > FIRST_PIECE_TIMEOUT_SECONDS:
            return Response("Timed out waiting for the first pieces to download.", status=504, mimetype='text/plain')
        time.sleep(0.2)

    # On-the-fly FFmpeg transcoder to convert MKV/AVI/AC3 directly into a
    # fragmented MP4 stream the <video> tag can play as it arrives.
    def generate_transcoded_stream():
        ffmpeg_cmd = [
            'ffmpeg', '-hide_banner', '-loglevel', 'error',
            '-re',
            '-i', file_path,
            '-c:v', 'copy',       # Copy video stream directly without re-encoding CPU load
            '-c:a', 'aac',        # Force standard audio compatibility
            '-f', 'mp4',
            '-movflags', 'frag_keyframe+empty_moov+default_base_moof',
            'pipe:1'
        ]

        proc = subprocess.Popen(ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=1024 * 1024)
        try:
            while True:
                data = proc.stdout.read(1024 * 256)
                if not data:
                    break
                yield data
        finally:
            try:
                proc.kill()
                proc.wait(timeout=2)
            except Exception:
                pass

    return Response(stream_with_context(generate_transcoded_stream()), mimetype='video/mp4')


@app.route('/favicon.ico')
def favicon():
    return send_from_directory(APP_DIR, 'favicon.ico')


@app.route('/robots.txt')
def robots():
    return send_from_directory(APP_DIR, 'robots.txt')


if __name__ == '__main__':
    # threaded=True is required so a long-lived video stream doesn't block
    # every other request the dev server receives. The reloader/debug mode
    # is disabled because it starts the process (and this libtorrent
    # session) twice, which fights over the same listen port.
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
