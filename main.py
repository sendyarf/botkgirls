import os
import time
import json
import signal
import tempfile
import requests
import logging
import re
from dotenv import load_dotenv
from groq import Groq

# Load environment variables from .env file
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ================= CONFIGURATION =================
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
SUBREDDIT = os.getenv("SUBREDDIT", "kpopfap")
CHECK_INTERVAL_SECONDS = int(os.getenv("CHECK_INTERVAL_SECONDS", "300"))

CHANNELS = {
    "hot": "-1003981379214",     # kpop1 (Semua media dari Hot)
    "new": "-1003944214147",     # kpop2 (Semua media dari New)
    "top": "-1003932396172",     # kpop3 (Semua media dari Top)
    "rising": "-1003947731924",  # kpop4 (Semua media dari Rising)
    "photo": "-1003932976389",   # kpop5 (Hanya Foto)
    "video": "-1003896668440"    # kpop6 (Hanya Video/GIF)
}
# =================================================

HISTORY_FILE = "history.json"
USER_AGENT = "KpopTelegramBot/1.0"
FEED_TYPES = ["hot", "new", "top", "rising"]
MAX_VIDEO_SIZE = 50 * 1024 * 1024  # 50MB Telegram Bot API limit

# Graceful shutdown flag
_shutdown_requested = False


def _signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    global _shutdown_requested
    logging.info(f"Received signal {signum}, shutting down gracefully...")
    _shutdown_requested = True


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


def check_is_ad_with_ai(title):
    """Menggunakan Groq AI untuk mengecek apakah judul postingan adalah iklan atau pengumuman."""
    if not GROQ_API_KEY:
        return False

    try:
        client = Groq(api_key=GROQ_API_KEY)
        chat_completion = client.chat.completions.create(
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
        # Simple rate limiting for Groq free tier
        time.sleep(1)
        return is_ad
    except Exception as e:
        logging.error(f"Error AI filter: {e}")
        return False


def load_history():
    default_history = {
        "hot": [],
        "new": [],
        "top": [],
        "rising": [],
        "media": []
    }
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, "r") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    # Gabungkan dengan default agar key selalu ada
                    for key in default_history:
                        if key in data:
                            default_history[key] = data[key]
        except Exception as e:
            logging.error(f"Error loading history file: {e}")
    return default_history


def save_history(history):
    """Save history atomically: write to temp file then replace."""
    try:
        dir_name = os.path.dirname(os.path.abspath(HISTORY_FILE))
        fd, tmp_path = tempfile.mkstemp(dir=dir_name, suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(history, f)
            os.replace(tmp_path, HISTORY_FILE)
        except Exception:
            # Clean up temp file on failure
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
    except Exception as e:
        logging.error(f"Error saving history: {e}")


def send_message(token, chat_id, text):
    if not chat_id or chat_id.startswith("YOUR_"):
        return None
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    try:
        res = requests.post(url, json=payload, timeout=20)
        return res.json()
    except Exception as e:
        logging.error(f"Error sending message: {e}")
        return None


def send_photo(token, chat_id, photo_url, caption):
    if not chat_id or chat_id.startswith("YOUR_"):
        return
    url = f"https://api.telegram.org/bot{token}/sendPhoto"

    for attempt in range(3):  # Coba hingga 3 kali
        try:
            res = requests.get(photo_url, headers={"User-Agent": USER_AGENT}, timeout=30)
            if res.status_code == 200:
                files = {'photo': ('photo.jpg', res.content)}
                payload = {"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"}
                r = requests.post(url, data=payload, files=files, timeout=60)
                if r.json().get("ok"):
                    return
            logging.warning(f"Attempt {attempt+1} failed to send photo: Status {res.status_code}")
        except Exception as e:
            logging.error(f"Attempt {attempt+1} error sending photo: {e}")
        time.sleep(5)


def send_video(token, chat_id, video_url, caption, moving_preview_url=None, photo_preview_url=None):
    if not chat_id or chat_id.startswith("YOUR_"):
        return
    url = f"https://api.telegram.org/bot{token}/sendVideo"

    for attempt in range(3):  # Coba hingga 3 kali
        try:
            # Stream download: check Content-Length first to avoid buffering huge files
            with requests.get(video_url, headers={"User-Agent": USER_AGENT}, timeout=60, stream=True) as res:
                if res.status_code != 200:
                    logging.warning(f"Attempt {attempt+1} failed to fetch video: Status {res.status_code}")
                    time.sleep(5)
                    continue

                # Check Content-Length header if available
                content_length = res.headers.get("Content-Length")
                if content_length and int(content_length) > MAX_VIDEO_SIZE:
                    logging.warning(f"Video too large ({content_length} bytes). Trying moving preview.")
                    if moving_preview_url and moving_preview_url != video_url:
                        # Try to send the smaller moving preview instead
                        send_video(token, chat_id, moving_preview_url, caption)
                    elif photo_preview_url:
                        send_photo(token, chat_id, photo_preview_url, caption)
                    return

                # Download in chunks with size limit
                chunks = []
                downloaded = 0
                for chunk in res.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
                    downloaded += len(chunk)
                    if downloaded > MAX_VIDEO_SIZE:
                        logging.warning(f"Video exceeded {MAX_VIDEO_SIZE} bytes during download. Trying moving preview.")
                        if moving_preview_url and moving_preview_url != video_url:
                            send_video(token, chat_id, moving_preview_url, caption)
                        elif photo_preview_url:
                            send_photo(token, chat_id, photo_preview_url, caption)
                        return
                    chunks.append(chunk)

                video_data = b"".join(chunks)

            files = {'video': ('video.mp4', video_data)}
            payload = {"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"}
            r = requests.post(url, data=payload, files=files, timeout=120)
            if r.json().get("ok"):
                return
            logging.warning(f"Attempt {attempt+1} failed to upload video to Telegram")
        except Exception as e:
            logging.error(f"Attempt {attempt+1} error sending video: {e}")
        time.sleep(5)


def get_media_type_and_url(post):
    data = post['data']
    url = data.get('url', '')
    domain = data.get('domain', '')
    is_video = data.get('is_video', False)

    # 1. Ambil photo preview URL (diam)
    photo_preview = ""
    try:
        photo_preview = data.get('preview', {}).get('images', [{}])[0].get('source', {}).get('url', '')
        photo_preview = photo_preview.replace('&amp;', '&')
    except:
        photo_preview = data.get('thumbnail', '')

    # 2. Ambil moving preview URL (media bergerak resolusi rendah/gif)
    moving_preview = ""
    try:
        # Coba ambil mp4 variant dari preview (biasanya lebih kecil dari main video)
        variants = data.get('preview', {}).get('images', [{}])[0].get('variants', {})
        moving_preview = variants.get('mp4', {}).get('source', {}).get('url', '')
        if not moving_preview:
            moving_preview = variants.get('gif', {}).get('source', {}).get('url', '')
        
        # Coba ambil reddit_video_preview
        if not moving_preview:
            moving_preview = data.get('preview', {}).get('reddit_video_preview', {}).get('fallback_url', '')
            
        moving_preview = moving_preview.replace('&amp;', '&')
    except:
        pass

    # Logika deteksi media utama
    if domain == 'redgifs.com':
        try:
            thumb = data.get('secure_media', {}).get('oembed', {}).get('thumbnail_url', '')
            if '-poster.jpg' in thumb:
                direct_mp4 = thumb.replace('-poster.jpg', '.mp4')
                return "video", direct_mp4, moving_preview, photo_preview
        except:
            pass

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
            first_item_id = data['gallery_data']['items'][0]['media_id']
            img_url = data['media_metadata'][first_item_id]['s']['u']
            return "photo", img_url.replace('&amp;', '&'), moving_preview, photo_preview
        except:
            return "photo", url, moving_preview, photo_preview
    elif data.get('is_gallery'):
        return "photo", url, moving_preview, photo_preview
    else:
        return "link", url, moving_preview, photo_preview


def _is_in_any_feed_history(history, post_id):
    """Check if a post ID exists in any of the feed histories."""
    return any(post_id in history[feed] for feed in FEED_TYPES)


def process_feed(history, feed_type):
    token = BOT_TOKEN
    channels = CHANNELS

    url = f"https://old.reddit.com/r/{SUBREDDIT}/{feed_type}.json?limit=50"
    headers = {"User-Agent": USER_AGENT}

    try:
        response = requests.get(url, headers=headers, timeout=30)
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

    for post in reversed(posts):
        if _shutdown_requested:
            return

        post_data = post['data']
        post_id = post_data['id']
        title = post_data.get('title', '')

        if post_data.get('stickied', False):
            continue

        if not _is_in_any_feed_history(history, post_id):
            if check_is_ad_with_ai(title):
                history[feed_type].append(post_id)
                continue

        media_type, media_url, moving_preview, photo_preview = get_media_type_and_url(post)

        if media_type == "link":
            continue

        caption = f"<b>{title}</b>"

        # 1. Send to the specific feed channel
        if post_id not in history[feed_type]:
            logging.info(f"New post found in {feed_type}: {post_id}")
            chat_id = channels.get(feed_type)
            if chat_id:
                if media_type == "photo":
                    send_photo(token, chat_id, media_url, caption)
                elif media_type == "video":
                    send_video(token, chat_id, media_url, caption, moving_preview, photo_preview)
                else:
                    send_message(token, chat_id, f"{caption}\n{media_url}")
                time.sleep(2)

            history[feed_type].append(post_id)
            if len(history[feed_type]) > 200:
                history[feed_type] = history[feed_type][-200:]

        # 2. Route to specialized media channels
        if post_id not in history["media"]:
            if media_type == "photo" and channels.get("photo"):
                logging.info(f"Sending photo to photo channel: {post_id}")
                send_photo(token, channels["photo"], media_url, caption)
                history["media"].append(post_id)
                time.sleep(2)
            elif media_type == "video" and channels.get("video"):
                logging.info(f"Sending video to video channel: {post_id}")
                send_video(token, channels["video"], media_url, caption, moving_preview, photo_preview)
                history["media"].append(post_id)
                time.sleep(2)

            if len(history["media"]) > 1000:
                history["media"] = history["media"][-1000:]


def main():
    if not BOT_TOKEN:
        logging.error("BOT_TOKEN not set. Please configure .env file.")
        return
    if not GROQ_API_KEY:
        logging.warning("GROQ_API_KEY not set. AI ad filtering will be disabled.")

    logging.info(f"Starting Reddit Telegram Bot for r/{SUBREDDIT}")
    history = load_history()

    while not _shutdown_requested:
        logging.info("Checking feeds...")
        for feed in FEED_TYPES:
            if _shutdown_requested:
                break
            process_feed(history, feed)
            save_history(history)

        if not _shutdown_requested:
            logging.info(f"Sleeping for {CHECK_INTERVAL_SECONDS} seconds...")
            for _ in range(CHECK_INTERVAL_SECONDS):
                if _shutdown_requested:
                    break
                time.sleep(1)

    save_history(history)
    logging.info("Bot stopped gracefully. History saved.")


if __name__ == "__main__":
    main()
