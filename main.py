import os
import time
import json
import signal
import sys
import sqlite3
import logging
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import requests
from dotenv import load_dotenv
from groq import Groq
import asyncio
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())
from pyrogram import Client

load_dotenv()

PID = os.getpid()
logging.basicConfig(level=logging.INFO, format=f'%(asctime)s - PID:{PID} - %(levelname)s - %(message)s')

# ================= CONFIGURATION =================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
SUBREDDIT = os.getenv("SUBREDDIT", "kpopfap")
CHECK_INTERVAL_SECONDS = int(os.getenv("CHECK_INTERVAL_SECONDS", "300"))
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
PHONE_NUMBER = os.getenv("PHONE_NUMBER", "")


CHANNELS = {
    "hot": ["-1003981379214", "-1001958598110"],
    "new": "-1003944214147",
    "top": "-1003932396172",
    "rising": "-1003947731924",
    "photo": "-1003932976389",
    "video": "-1003896668440"
}


def _get_chat_ids(channels, key):
    """Retrieve a list of chat IDs for a given key from the channels dictionary."""
    val = channels.get(key)
    if not val:
        return []
    if isinstance(val, list):
        return [str(v).strip() for v in val if v]
    if isinstance(val, str):
        return [v.strip() for v in val.split(",") if v.strip()]
    return [str(val)]
# =================================================

DATABASE_FILE = "history.db"
HISTORY_CLEANUP_DAYS = 7
USER_AGENT = "KpopTelegramBot/1.0"
FEED_TYPES = ["hot", "new", "top", "rising"]
MAX_VIDEO_SIZE = 50 * 1024 * 1024

# Reusable session with connection pooling to prevent WinError 10013 (port exhaustion)
_http = requests.Session()
_adapter = HTTPAdapter(
    pool_connections=20,
    pool_maxsize=20,
    max_retries=Retry(total=2, backoff_factor=1, status_forcelist=[429, 502, 503, 504])
)
_http.mount("https://", _adapter)
_http.mount("http://", _adapter)
_http.headers.update({"User-Agent": USER_AGENT})

# Groq client instance (reuse, jangan new per AI check)
_groq_client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None

_shutdown_requested = False
_db_conn = None
_send_counts = {}  # diagnostic: tracks sends per post_id within this process


def _signal_handler(signum, frame):
    global _shutdown_requested
    logging.info(f"Received signal {signum}, shutting down gracefully...")
    _shutdown_requested = True


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

PID_FILE = "bot.pid"


def _acquire_pid_lock():
    """Acquire a PID-based lock to prevent multiple bot instances from running.
    Returns True if this is the only instance, False if another instance is already running."""
    if os.path.exists(PID_FILE):
        old_pid = None
        try:
            with open(PID_FILE, "r") as f:
                old_pid = int(f.read().strip())
            # Check if the old process is still running
            os.kill(old_pid, 0)  # signal 0 = no-op, but raises OSError if process doesn't exist
            # If we reach here, old process is still alive
            logging.error(
                f"Another bot instance is already running (PID {old_pid}). "
                f"Remove {PID_FILE} if the old instance has crashed."
            )
            return False
        except (ValueError, OSError):
            # Old PID file exists but process is dead or file is corrupted
            if old_pid is not None:
                logging.warning(f"Removing stale PID lock from dead process (PID {old_pid})")
            else:
                logging.warning(f"Removing corrupted/unreadable PID lock file: {PID_FILE}")
            try:
                os.remove(PID_FILE)
            except OSError:
                pass

    try:
        with open(PID_FILE, "w") as f:
            f.write(str(PID))
        return True
    except OSError as e:
        logging.error(f"Failed to create PID lock file: {e}")
        return False


def _release_pid_lock():
    """Remove the PID lock file on shutdown."""
    try:
        if os.path.exists(PID_FILE):
            with open(PID_FILE, "r") as f:
                lock_pid = int(f.read().strip())
            if lock_pid == PID:
                os.remove(PID_FILE)
                logging.info("PID lock released.")
    except (OSError, ValueError):
        pass


# ================= DATABASE =================

def init_db(db_path=DATABASE_FILE):
    global _db_conn
    _db_conn = sqlite3.connect(db_path)
    _db_conn.execute("PRAGMA journal_mode=WAL")
    _db_conn.execute("PRAGMA synchronous=NORMAL")
    _db_conn.execute("""
        CREATE TABLE IF NOT EXISTS processed_posts (
            post_id TEXT NOT NULL,
            feed_type TEXT NOT NULL,
            is_ad INTEGER DEFAULT 0,
            media_url TEXT,
            processed_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (post_id, feed_type)
        )
    """)
    # Migration: add media_url column for older DBs that lack it
    try:
        _db_conn.execute("ALTER TABLE processed_posts ADD COLUMN media_url TEXT")
    except sqlite3.OperationalError:
        pass  # column already exists (SQLite doesn't support IF NOT EXISTS for ALTER)
    _db_conn.execute("CREATE INDEX IF NOT EXISTS idx_feed_type ON processed_posts(feed_type)")
    _db_conn.execute("CREATE INDEX IF NOT EXISTS idx_processed_at ON processed_posts(processed_at)")
    _db_conn.execute("CREATE INDEX IF NOT EXISTS idx_media_url_feed ON processed_posts(media_url, feed_type)")
    _db_conn.commit()

    if os.path.exists("history.json"):
        _migrate_from_json()
        try:
            os.rename("history.json", "history.json.bak")
        except OSError:
            pass

    return _db_conn


def _migrate_from_json():
    try:
        with open("history.json", "r") as f:
            old = json.load(f)
    except Exception as e:
        logging.error(f"Failed to read history.json for migration: {e}")
        return

    migrated = 0
    try:
        for feed in FEED_TYPES:
            for pid in old.get(feed, []):
                _db_conn.execute(
                    "INSERT OR IGNORE INTO processed_posts(post_id, feed_type) VALUES(?, ?)",
                    (pid, feed)
                )
                migrated += 1

        for pid in old.get("media", []):
            _db_conn.execute(
                "INSERT OR IGNORE INTO processed_posts(post_id, feed_type) VALUES(?, 'media')",
                (pid,)
            )
            migrated += 1

        for pid in old.get("ads", []):
            _db_conn.execute(
                "INSERT OR IGNORE INTO processed_posts(post_id, feed_type, is_ad) VALUES(?, 'ad', 1)",
                (pid,)
            )
            migrated += 1

        _db_conn.commit()
        logging.info(f"Migrated {migrated} entries from history.json to SQLite")
    except Exception as e:
        logging.error(f"Migration failed: {e}")
        _db_conn.rollback()


def is_processed(post_id, feed_type):
    try:
        row = _db_conn.execute(
            "SELECT 1 FROM processed_posts WHERE post_id = ? AND feed_type = ?",
            (post_id, feed_type)
        ).fetchone()
        return row is not None
    except sqlite3.Error as e:
        logging.error(f"DB error in is_processed({post_id}, {feed_type}): {e}")
        return False


def is_known_ad(post_id):
    try:
        row = _db_conn.execute(
            "SELECT 1 FROM processed_posts WHERE post_id = ? AND feed_type = 'ad'",
            (post_id,)
        ).fetchone()
        return row is not None
    except sqlite3.Error as e:
        logging.error(f"DB error in is_known_ad({post_id}): {e}")
        return False


def is_media_url_processed(media_url, feed_type):
    """Check if a media URL has already been posted in the given feed_type (crosspost dedup)."""
    if not media_url or (isinstance(media_url, list) and not media_url):
        return False
    try:
        # For galleries, check the first image URL as representative
        check_url = media_url[0] if isinstance(media_url, list) else media_url
        row = _db_conn.execute(
            "SELECT 1 FROM processed_posts WHERE media_url = ? AND feed_type = ? AND media_url IS NOT NULL",
            (check_url, feed_type)
        ).fetchone()
        return row is not None
    except sqlite3.Error as e:
        logging.error(f"DB error in is_media_url_processed({feed_type}): {e}")
        return False


def mark_processed(post_id, feed_type, is_ad=False, also_mark_ad=False, media_url=None):
    url = ((media_url[0] if isinstance(media_url, list) else media_url) if media_url else None)
    _db_conn.execute(
        "INSERT OR REPLACE INTO processed_posts(post_id, feed_type, is_ad, media_url, processed_at) VALUES(?, ?, ?, ?, datetime('now'))",
        (post_id, feed_type, 1 if is_ad else 0, url)
    )
    if also_mark_ad:
        _db_conn.execute(
            "INSERT OR REPLACE INTO processed_posts(post_id, feed_type, is_ad, processed_at) VALUES(?, 'ad', 1, datetime('now'))",
            (post_id,)
        )
    _db_conn.commit()


def track_send(post_id, feed_type, channel_name):
    """Diagnostic: track and log sends per post to detect duplicates."""
    key = f"{post_id}@{feed_type}"
    count = _send_counts.get(key, 0)
    _send_counts[key] = count + 1
    if count > 0:
        logging.warning(
            f"DUPLICATE SEND DETECTED: post={post_id}, feed={feed_type}, "
            f"channel={channel_name}, send_number={count + 1} within this process!"
        )


def cleanup_old_entries(days=HISTORY_CLEANUP_DAYS):
    cur = _db_conn.execute(
        "DELETE FROM processed_posts WHERE processed_at < datetime('now', ?)",
        (f'-{days} days',)
    )
    if cur.rowcount > 0:
        logging.info(f"Cleaned up {cur.rowcount} old history entries (> {days} days)")
    _db_conn.commit()


def get_stats():
    """Return row counts for monitoring."""
    row = _db_conn.execute("SELECT feed_type, COUNT(*) FROM processed_posts GROUP BY feed_type").fetchall()
    return {r[0]: r[1] for r in row}


# ================= AI FILTER =================

def check_is_ad_with_ai(title):
    if not _groq_client:
        return False
    try:
        chat_completion = _groq_client.chat.completions.create(
            messages=[
                {
                    "role": "system",
                    "content": "You are a content filter. Respond ONLY with 'YES' or 'NO'. Is this Reddit post title an advertisement, spam, or a community announcement? Title:"
                },
                {
                    "role": "user",
                    "content": title,
                }
            ],
            model="llama-3.3-70b-versatile",
        )
        answer = chat_completion.choices[0].message.content.strip().upper()
        is_ad = "YES" in answer
        if is_ad:
            logging.info(f"AI detected ad/announcement: {title}")
        time.sleep(1)
        return is_ad
    except Exception as e:
        logging.error(f"Error AI filter: {e}")
        return False


# ================= TELEGRAM SENDERS =================

def send_message(token, chat_id, text):
    if not chat_id or chat_id.startswith("YOUR_"):
        return None
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    try:
        res = _http.post(url, json=payload, timeout=20)
        return res.json()
    except Exception as e:
        logging.error(f"Error sending message: {e}")
        return None


def send_photo(token, chat_id, photo_url, caption):
    if not chat_id or chat_id.startswith("YOUR_"):
        return None
    url = f"https://api.telegram.org/bot{token}/sendPhoto"

    # Check if photo_url is already a file_id (not starting with http)
    if isinstance(photo_url, str) and not photo_url.startswith(("http://", "https://")):
        logging.info(f"Sending photo by file_id to {chat_id}...")
        try:
            payload = {"chat_id": chat_id, "photo": photo_url, "caption": caption, "parse_mode": "HTML"}
            res = _http.post(url, json=payload, timeout=30)
            if res.status_code == 200 and res.json().get("ok"):
                return photo_url
            logging.warning(f"Failed to send photo by file_id. Response: {res.text[:200]}")
        except Exception as e:
            logging.error(f"Error sending photo by file_id: {e}")
        return None

    logging.info(f"Downloading photo: {photo_url}")
    photo_data = None
    for attempt in range(3):
        try:
            res = _http.get(photo_url, timeout=30)
            if res.status_code == 200:
                photo_data = res.content
                break
            logging.warning(f"Attempt {attempt+1} failed to fetch photo: Status {res.status_code}")
        except Exception as e:
            logging.error(f"Attempt {attempt+1} error downloading photo: {e}")
        time.sleep(5)

    if not photo_data:
        logging.error(f"Failed to download photo after 3 attempts: {photo_url}")
        return None

    logging.info(f"Uploading photo to Telegram ({len(photo_data)} bytes) to {chat_id}...")
    try:
        files = {'photo': ('photo.jpg', photo_data)}
        payload = {"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"}
        r = _http.post(url, data=payload, files=files, timeout=120)
        if r.status_code == 200 and r.json().get("ok"):
            res_json = r.json()
            try:
                file_id = res_json.get("result", {}).get("photo", [])[-1].get("file_id")
                logging.info(f"Photo uploaded successfully. file_id: {file_id}")
                return file_id
            except:
                return True
        logging.warning(f"Telegram returned non-ok for photo. Response: {r.text[:200]}")
    except Exception as e:
        logging.error(f"Error uploading photo to Telegram: {e}")
    return None


def send_gallery(token, chat_id, urls, caption):
    if not chat_id or chat_id.startswith("YOUR_"):
        return None
    url_api = f"https://api.telegram.org/bot{token}/sendMediaGroup"

    # Check if urls are already file_ids (first element is not starting with http)
    if urls and isinstance(urls, list) and not urls[0].startswith(("http://", "https://")):
        logging.info(f"Sending gallery by file_ids to {chat_id}...")
        try:
            media_group = []
            for i, file_id in enumerate(urls):
                media_item = {"type": "photo", "media": file_id}
                if i == 0:
                    media_item["caption"] = caption
                    media_item["parse_mode"] = "HTML"
                media_group.append(media_item)
            
            payload = {"chat_id": chat_id, "media": json.dumps(media_group)}
            res = _http.post(url_api, json=payload, timeout=120)
            if res.status_code == 200 and res.json().get("ok"):
                return urls
            logging.warning(f"Failed to send gallery by file_ids. Response: {res.text[:200]}")
        except Exception as e:
            logging.error(f"Error sending gallery by file_ids: {e}")
        return None

    chunks = [urls[i:i + 10] for i in range(0, len(urls), 10)]
    uploaded_file_ids = []

    for chunk_idx, chunk_urls in enumerate(chunks):
        files = {}
        media_group = []
        for i, img_url in enumerate(chunk_urls):
            logging.info(f"Downloading gallery image ({chunk_idx * 10 + i + 1}/{len(urls)}): {img_url}")
            photo_data = None
            for attempt in range(3):
                try:
                    res = _http.get(img_url, timeout=30)
                    if res.status_code == 200:
                        photo_data = res.content
                        break
                except Exception as e:
                    logging.error(f"Attempt {attempt+1} error downloading gallery img: {e}")
                time.sleep(2)

            if photo_data:
                file_key = f'photo_{chunk_idx}_{i}'
                files[file_key] = (f'{file_key}.jpg', photo_data)
                media_item = {"type": "photo", "media": f"attach://{file_key}"}
                if i == 0:
                    media_item["caption"] = caption
                    media_item["parse_mode"] = "HTML"
                media_group.append(media_item)

        if not media_group:
            continue

        logging.info(f"Uploading gallery chunk ({len(media_group)} images) to Telegram to {chat_id}...")
        try:
            payload = {"chat_id": chat_id, "media": json.dumps(media_group)}
            r = _http.post(url_api, data=payload, files=files, timeout=300)
            if r.status_code == 200 and r.json().get("ok"):
                res_json = r.json()
                try:
                    results = res_json.get("result", [])
                    for msg in results:
                        photo_list = msg.get("photo", [])
                        if photo_list:
                            uploaded_file_ids.append(photo_list[-1].get("file_id"))
                except Exception as ex:
                    logging.warning(f"Could not parse file_ids from gallery response: {ex}")
            else:
                logging.warning(f"Telegram returned non-ok for gallery chunk. Response: {r.text[:200]}")
        except Exception as e:
            logging.error(f"Error uploading gallery chunk: {e}")

        time.sleep(3)
    return uploaded_file_ids if uploaded_file_ids else True


def download_hls_stream(hls_url, output_path, timeout=300):
    """Download an HLS (.m3u8) stream and mux to MP4 using ffmpeg (no re-encode).
    
    Requires ffmpeg to be installed on the VPS: apt install ffmpeg
    Returns True on success, False on failure.
    """
    import subprocess
    try:
        cmd = [
            'ffmpeg', '-y',
            '-i', hls_url,
            '-c', 'copy',            # remux only — no re-encode
            '-movflags', '+faststart',
            output_path
        ]
        result = subprocess.run(cmd, capture_output=True, timeout=timeout)
        if result.returncode == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            logging.info(f"HLS downloaded: {output_path} ({os.path.getsize(output_path):,} bytes)")
            return True
        stderr = result.stderr.decode(errors='replace')[-400:]
        logging.error(f"ffmpeg failed (code {result.returncode}): {stderr}")
        return False
    except subprocess.TimeoutExpired:
        logging.error("ffmpeg timed out downloading HLS stream")
        return False
    except FileNotFoundError:
        logging.error("ffmpeg not found — install it on the VPS: apt install ffmpeg")
        return False
    except Exception as e:
        logging.error(f"HLS download error: {e}")
        return False


def upload_large_video_with_pyrogram(video_url, caption, chat_id, video_data=None, photo_preview_url=None, video_path=None):
    if not API_ID or not API_HASH:
        logging.error("API_ID or API_HASH not set. Cannot use Pyrogram for large files.")
        return None

    # If a pre-built file path is provided, use it directly (caller is responsible for cleanup).
    # Otherwise create our own temp file.
    owns_file = video_path is None
    if video_path is None:
        video_path = f"temp_video_{int(time.time())}.mp4"

    try:
        if owns_file:
            if video_data:
                with open(video_path, "wb") as f:
                    f.write(video_data)
            else:
                logging.info(f"Downloading large video directly for Pyrogram: {video_url}")
                with _http.get(video_url, timeout=120, stream=True) as res:
                    if res.status_code == 200:
                        with open(video_path, "wb") as f:
                            for chunk in res.iter_content(chunk_size=1024 * 1024):
                                f.write(chunk)
                    else:
                        logging.error(f"Failed to download large video: Status {res.status_code}")
                        return None

        async def _upload():
            async with Client("user_session", api_id=API_ID, api_hash=API_HASH, phone_number=PHONE_NUMBER) as app:
                logging.info(f"Pyrogram logged in. Uploading large video to {chat_id}...")
                target_chat_id = int(chat_id) if chat_id.lstrip('-').isdigit() else chat_id
                send_kwargs = dict(
                    chat_id=target_chat_id,
                    video=video_path,
                    caption=caption,
                    supports_streaming=True,
                )
                # Attach thumbnail if available so mobile shows a proper preview
                if photo_preview_url:
                    import tempfile, urllib.request
                    thumb_tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg")
                    try:
                        urllib.request.urlretrieve(photo_preview_url, thumb_tmp.name)
                        send_kwargs["thumb"] = thumb_tmp.name
                    except Exception as thumb_err:
                        logging.warning(f"Could not fetch thumbnail for Pyrogram: {thumb_err}")
                msg = await app.send_video(**send_kwargs)
                return msg.video.file_id if msg.video else True

        return asyncio.run(_upload())
    except Exception as e:
        logging.error(f"Error in Pyrogram upload: {e}")
        return None
    finally:
        if owns_file and os.path.exists(video_path):
            os.remove(video_path)


def send_video(token, chat_id, video_url, caption, moving_preview_url=None, photo_preview_url=None):
    if not chat_id or chat_id.startswith("YOUR_"):
        return None
    url = f"https://api.telegram.org/bot{token}/sendVideo"

    # Check if video_url is already a file_id (not starting with http)
    if isinstance(video_url, str) and not video_url.startswith(("http://", "https://")):
        logging.info(f"Sending video by file_id to {chat_id}...")
        try:
            payload = {"chat_id": chat_id, "video": video_url, "caption": caption, "parse_mode": "HTML", "supports_streaming": True}
            if photo_preview_url:
                payload["thumbnail"] = photo_preview_url
            res = _http.post(url, json=payload, timeout=30)
            if res.status_code == 200 and res.json().get("ok"):
                return video_url
            logging.warning(f"Failed to send video by file_id. Response: {res.text[:200]}")
        except Exception as e:
            logging.error(f"Error sending video by file_id: {e}")
        return None

    logging.info(f"Downloading video from: {video_url}")

    # ── HLS stream (.m3u8) — must be downloaded via ffmpeg first ──
    if isinstance(video_url, str) and '.m3u8' in video_url:
        temp_hls = f"temp_hls_{int(time.time())}.mp4"
        try:
            logging.info(f"HLS stream detected. Downloading via ffmpeg: {video_url}")
            if not download_hls_stream(video_url, temp_hls):
                logging.error("HLS download failed.")
                if photo_preview_url:
                    return send_photo(token, chat_id, photo_preview_url, caption)
                return None

            file_size = os.path.getsize(temp_hls)
            if file_size > MAX_VIDEO_SIZE:
                logging.warning(f"HLS result too large ({file_size:,} bytes). Using Pyrogram...")
                # Pass path directly so Pyrogram doesn't re-download
                result = upload_large_video_with_pyrogram(
                    video_url, caption, chat_id,
                    video_path=temp_hls,
                    photo_preview_url=photo_preview_url
                )
                temp_hls = None  # ownership transferred — do NOT delete
                return result

            # Small enough for Bot API — read into memory then send
            with open(temp_hls, 'rb') as fh:
                hls_video_data = fh.read()
        finally:
            if temp_hls and os.path.exists(temp_hls):
                os.remove(temp_hls)

        logging.info(f"Uploading HLS-converted video ({len(hls_video_data):,} bytes) to {chat_id}...")
        try:
            files = {'video': ('video.mp4', hls_video_data)}
            payload = {"chat_id": chat_id, "caption": caption, "parse_mode": "HTML", "supports_streaming": True}
            if photo_preview_url:
                try:
                    tr = _http.get(photo_preview_url, timeout=15)
                    if tr.status_code == 200:
                        files['thumbnail'] = ('thumb.jpg', tr.content)
                except Exception as te:
                    logging.warning(f"Thumbnail fetch error: {te}")
            r = _http.post(url, data=payload, files=files, timeout=300)
            if r.status_code == 200 and r.json().get("ok"):
                try:
                    file_id = r.json().get("result", {}).get("video", {}).get("file_id")
                    logging.info(f"HLS video uploaded. file_id: {file_id}")
                    return file_id
                except:
                    return True
            logging.warning(f"Telegram error for HLS video: {r.text[:200]}")
        except Exception as e:
            logging.error(f"Error uploading HLS video: {e}")
        return None
    # ── end HLS block ──

    video_data = None
    for attempt in range(3):
        try:
            with _http.get(video_url, timeout=60, stream=True) as res:
                if res.status_code != 200:
                    logging.warning(f"Attempt {attempt+1} failed to fetch video: Status {res.status_code}")
                    time.sleep(5)
                    continue

                content_length = res.headers.get("Content-Length")
                if content_length and int(content_length) > MAX_VIDEO_SIZE:
                    logging.warning(f"Video too large ({content_length} bytes) for Bot API. Using Pyrogram user account...")
                    pyrogram_result = upload_large_video_with_pyrogram(video_url, caption, chat_id, photo_preview_url=photo_preview_url)
                    if pyrogram_result:
                        return pyrogram_result
                    logging.warning(f"Pyrogram upload failed. Trying moving preview.")
                    if moving_preview_url and moving_preview_url != video_url:
                        return send_video(token, chat_id, moving_preview_url, caption)
                    elif photo_preview_url:
                        return send_photo(token, chat_id, photo_preview_url, caption)
                    return None

                chunks = []
                downloaded = 0
                for chunk in res.iter_content(chunk_size=1024 * 1024):
                    downloaded += len(chunk)
                    if downloaded > MAX_VIDEO_SIZE:
                        break
                    chunks.append(chunk)

                if downloaded > MAX_VIDEO_SIZE:
                    logging.warning(f"Video exceeded limit during download. Using Pyrogram user account...")
                    pyrogram_result = upload_large_video_with_pyrogram(video_url, caption, chat_id, photo_preview_url=photo_preview_url)
                    if pyrogram_result:
                        return pyrogram_result
                    logging.warning(f"Pyrogram upload failed. Trying preview.")
                    if moving_preview_url and moving_preview_url != video_url:
                        return send_video(token, chat_id, moving_preview_url, caption)
                    elif photo_preview_url:
                        return send_photo(token, chat_id, photo_preview_url, caption)
                    return None

                video_data = b"".join(chunks)
                break
        except Exception as e:
            logging.error(f"Attempt {attempt+1} error downloading video: {e}")
            time.sleep(5)

    if not video_data:
        logging.error(f"Failed to download video after 3 attempts: {video_url}")
        return None

    logging.info(f"Uploading video to Telegram ({len(video_data)} bytes) to {chat_id}...")
    try:
        files = {'video': ('video.mp4', video_data)}
        payload = {"chat_id": chat_id, "caption": caption, "parse_mode": "HTML", "supports_streaming": True}

        # Attach thumbnail so mobile clients show a proper preview instead of a black frame
        if photo_preview_url:
            try:
                thumb_res = _http.get(photo_preview_url, timeout=15)
                if thumb_res.status_code == 200:
                    files['thumbnail'] = ('thumb.jpg', thumb_res.content)
                    logging.info("Thumbnail attached to video upload.")
                else:
                    logging.warning(f"Could not fetch thumbnail ({thumb_res.status_code}), skipping.")
            except Exception as thumb_err:
                logging.warning(f"Error fetching thumbnail: {thumb_err}")

        r = _http.post(url, data=payload, files=files, timeout=300)
        if r.status_code == 200 and r.json().get("ok"):
            res_json = r.json()
            try:
                file_id = res_json.get("result", {}).get("video", {}).get("file_id")
                logging.info(f"Video uploaded successfully. file_id: {file_id}")
                return file_id
            except:
                return True
        logging.warning(f"Telegram returned non-ok for video. Response: {r.text[:200]}")
    except Exception as e:
        logging.error(f"Error uploading video to Telegram: {e}")

    return None


# ================= MEDIA PARSER =================


def get_redgifs_direct_url(page_url):
    """Fetch the real direct MP4 URL from Redgifs API v2 (no watermark).
    
    Redgifs embeds a watermark when you use the watch-page URL directly.
    This function calls the API to get the actual source file.
    """
    try:
        # Extract gif ID from URLs like:
        #   https://www.redgifs.com/watch/somegifid
        #   https://redgifs.com/watch/SomeGifId-extra
        gif_id = page_url.rstrip('/').split('/')[-1].split('-')[0].lower()
        if not gif_id:
            logging.warning("Redgifs: could not extract gif ID from URL")
            return None

        # Step 1: get a short-lived anonymous token
        token_res = _http.get("https://api.redgifs.com/v2/auth/temporary", timeout=10)
        if token_res.status_code != 200:
            logging.warning(f"Redgifs: failed to get auth token (HTTP {token_res.status_code})")
            return None
        token = token_res.json().get("token")
        if not token:
            logging.warning("Redgifs: auth token missing in response")
            return None

        # Step 2: fetch gif metadata
        headers = {"Authorization": f"Bearer {token}"}
        gif_res = _http.get(f"https://api.redgifs.com/v2/gifs/{gif_id}", headers=headers, timeout=10)
        if gif_res.status_code != 200:
            logging.warning(f"Redgifs: gif metadata request failed (HTTP {gif_res.status_code})")
            return None

        urls = gif_res.json().get("gif", {}).get("urls", {})
        # Prefer HD, fall back to SD
        direct_url = urls.get("hd") or urls.get("sd")
        if direct_url:
            logging.info(f"Redgifs: resolved direct URL → {direct_url}")
        else:
            logging.warning(f"Redgifs: no hd/sd URL in response: {urls}")
        return direct_url
    except Exception as e:
        logging.warning(f"Redgifs API error: {e}")
        return None

def get_media_type_and_url(post):
    data = post['data']
    url = data.get('url', '')
    domain = data.get('domain', '')
    is_video = data.get('is_video', False)

    photo_preview = ""
    try:
        photo_preview = data.get('preview', {}).get('images', [{}])[0].get('source', {}).get('url', '')
        photo_preview = photo_preview.replace('&amp;', '&')
    except:
        photo_preview = data.get('thumbnail', '')

    moving_preview = ""
    try:
        variants = data.get('preview', {}).get('images', [{}])[0].get('variants', {})
        moving_preview = variants.get('mp4', {}).get('source', {}).get('url', '')
        if not moving_preview:
            moving_preview = variants.get('gif', {}).get('source', {}).get('url', '')
        if not moving_preview:
            moving_preview = data.get('preview', {}).get('reddit_video_preview', {}).get('fallback_url', '')
        moving_preview = moving_preview.replace('&amp;', '&')
    except:
        pass

    if domain == 'redgifs.com':
        direct_url = get_redgifs_direct_url(url)
        if direct_url:
            return "video", direct_url, moving_preview, photo_preview
        # Fallback: try the old thumbnail trick
        try:
            thumb = data.get('secure_media', {}).get('oembed', {}).get('thumbnail_url', '')
            if '-poster.jpg' in thumb:
                direct_mp4 = thumb.replace('-poster.jpg', '.mp4')
                return "video", direct_mp4, moving_preview, photo_preview
        except:
            pass
        logging.warning(f"Redgifs: all resolution methods failed for {url}")

    if is_video:
        try:
            fallback = data['secure_media']['reddit_video']['fallback_url']
            return "video", fallback, moving_preview, photo_preview
        except:
            return "video", url, moving_preview, photo_preview
    elif domain in ['redgifs.com', 'gfycat.com'] or url.endswith(('.gif', '.gifv', '.mp4')):
        return "video", url, moving_preview, photo_preview
    elif url.endswith(('.jpg', '.jpeg', '.png')):
        return "photo", url.replace('&amp;', '&'), moving_preview, photo_preview
    elif 'gallery_data' in data and 'media_metadata' in data:
        try:
            urls = []
            for item in data['gallery_data']['items']:
                media_id = item['media_id']
                meta = data['media_metadata'].get(media_id, {})
                img_url = meta.get('s', {}).get('u', '')
                if img_url:
                    urls.append(img_url.replace('&amp;', '&'))
            if len(urls) > 1:
                return "gallery", urls, moving_preview, photo_preview
            elif len(urls) == 1:
                return "photo", urls[0], moving_preview, photo_preview
            else:
                return "link", url, moving_preview, photo_preview
        except:
            return "link", url, moving_preview, photo_preview
    elif data.get('is_gallery'):
        return "photo", url, moving_preview, photo_preview
    else:
        return "link", url, moving_preview, photo_preview


# ================= FEED PROCESSOR =================

def process_feed(feed_type):
    token = BOT_TOKEN
    channels = CHANNELS

    url = f"https://old.reddit.com/r/{SUBREDDIT}/{feed_type}.json?limit=50"

    try:
        response = _http.get(url, timeout=30)
        if response.status_code != 200:
            logging.error(f"Failed to fetch {feed_type}: HTTP {response.status_code}")
            return
    except Exception as e:
        logging.error(f"Error fetching {feed_type}: {e}")
        return

    try:
        data = response.json()
    except requests.exceptions.JSONDecodeError:
        logging.error(f"Failed to parse JSON for {feed_type}. Response text: {response.text[:100]}")
        return

    posts = data.get('data', {}).get('children', [])
    new_posts = 0

    for post in reversed(posts):
        if _shutdown_requested:
            return

        post_data = post['data']
        post_id = post_data['id']
        title = post_data.get('title', '')

        if post_data.get('stickied', False):
            continue

        if is_processed(post_id, feed_type):
            continue

        if is_known_ad(post_id):
            mark_processed(post_id, feed_type)
            logging.info(f"Skipping known ad in {feed_type}: {post_id}")
            continue

        if check_is_ad_with_ai(title):
            mark_processed(post_id, feed_type, is_ad=True, also_mark_ad=True)
            continue

        media_type, media_url, moving_preview, photo_preview = get_media_type_and_url(post)

        if media_type == "link":
            mark_processed(post_id, feed_type)
            continue

        if is_media_url_processed(media_url, feed_type):
            mark_processed(post_id, feed_type, media_url=media_url)
            logging.info(f"Skipping crosspost/duplicate URL in {feed_type}: {post_id}")
            continue

        caption = f"<b>{title}</b>"
        chat_ids = _get_chat_ids(channels, feed_type)

        mark_processed(post_id, feed_type, media_url=media_url)
        new_posts += 1

        for chat_id in chat_ids:
            logging.info(f"→ SENDING to channel {feed_type}({chat_id}) | post={post_id}")
            if media_type == "photo":
                res = send_photo(token, chat_id, media_url, caption)
                if isinstance(res, str):
                    media_url = res
                track_send(post_id, feed_type, feed_type)
            elif media_type == "video":
                res = send_video(token, chat_id, media_url, caption, moving_preview, photo_preview)
                if isinstance(res, str):
                    media_url = res
                track_send(post_id, feed_type, feed_type)
            elif media_type == "gallery":
                res = send_gallery(token, chat_id, media_url, caption)
                if isinstance(res, list) and res:
                    media_url = res
                track_send(post_id, feed_type, feed_type)
            else:
                send_message(token, chat_id, f"{caption}\n{media_url}")
                track_send(post_id, feed_type, feed_type)
            time.sleep(2)

        if feed_type == "new" and not is_processed(post_id, "media"):
            if is_media_url_processed(media_url, "media"):
                mark_processed(post_id, "media", media_url=media_url)
                logging.info(f"Skipping crosspost routing to media channel: {post_id}")
                continue

            mark_processed(post_id, "media", media_url=media_url)

            photo_chat_ids = _get_chat_ids(channels, "photo")
            video_chat_ids = _get_chat_ids(channels, "video")

            if media_type == "photo" and photo_chat_ids:
                for chat_id in photo_chat_ids:
                    logging.info(f"→ ROUTING to channel photo({chat_id}) | post={post_id}")
                    res = send_photo(token, chat_id, media_url, caption)
                    if isinstance(res, str):
                        media_url = res
                    track_send(post_id, "media", "photo")
                    time.sleep(2)
            elif media_type == "gallery" and photo_chat_ids:
                for chat_id in photo_chat_ids:
                    logging.info(f"→ ROUTING to channel photo({chat_id}) | post={post_id}")
                    res = send_gallery(token, chat_id, media_url, caption)
                    if isinstance(res, list) and res:
                        media_url = res
                    track_send(post_id, "media", "photo")
                    time.sleep(2)
            elif media_type == "video" and video_chat_ids:
                for chat_id in video_chat_ids:
                    logging.info(f"→ ROUTING to channel video({chat_id}) | post={post_id}")
                    res = send_video(token, chat_id, media_url, caption, moving_preview, photo_preview)
                    if isinstance(res, str):
                        media_url = res
                    track_send(post_id, "media", "video")
                    time.sleep(2)

    if new_posts > 0:
        logging.info(f"Processed {new_posts} new posts from {feed_type}")


# ================= MAIN =================

def main():
    if not BOT_TOKEN:
        logging.error("BOT_TOKEN not set. Please configure .env file.")
        return
    if not GROQ_API_KEY:
        logging.warning("GROQ_API_KEY not set. AI ad filtering will be disabled.")

    if not _acquire_pid_lock():
        logging.error("Exiting to prevent duplicate instances.")
        sys.exit(1)

    logging.info(f"Starting Reddit Telegram Bot for r/{SUBREDDIT}")
    init_db()
    logging.info(f"DB stats: {get_stats()}")
    logging.info(f"Send count tracker active — duplicate sends will be logged as WARNING")

    iteration = 0
    while not _shutdown_requested:
        logging.info("Checking feeds...")
        for feed in FEED_TYPES:
            if _shutdown_requested:
                break
            process_feed(feed)

        iteration += 1
        if iteration % 288 == 0:
            cleanup_old_entries()

        if not _shutdown_requested:
            logging.info(f"Sleeping for {CHECK_INTERVAL_SECONDS} seconds...")
            for _ in range(CHECK_INTERVAL_SECONDS):
                if _shutdown_requested:
                    break
                time.sleep(1)

    _release_pid_lock()
    if _db_conn:
        _db_conn.close()
    logging.info("Bot stopped gracefully.")


if __name__ == "__main__":
    main()
