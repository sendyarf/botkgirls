import os
import time
import json
import signal
import tempfile
import requests
import logging
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
        "media": [],
        "ads": []
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
    """Kirim foto ke Telegram."""
    if not chat_id or chat_id.startswith("YOUR_"):
        return False
    url = f"https://api.telegram.org/bot{token}/sendPhoto"

    for attempt in range(3):
        try:
            res = requests.get(photo_url, headers={"User-Agent": USER_AGENT}, timeout=30)
            if res.status_code == 200:
                files = {'photo': ('photo.jpg', res.content)}
                payload = {"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"}
                r = requests.post(url, data=payload, files=files, timeout=120)
                if r.json().get("ok"):
                    return True
            logging.warning(f"Attempt {attempt+1} failed to send photo: Status {res.status_code}")
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            logging.warning(f"Timeout/connection error sending photo. Assuming success to prevent duplicates.")
            return True
        except Exception as e:
            logging.error(f"Attempt {attempt+1} error sending photo: {e}")
        time.sleep(5)

    logging.error(f"All attempts failed to send photo: {photo_url}")
    return False


def send_gallery(token, chat_id, urls, caption):
    """Kirim album/galeri ke Telegram (dipecah per 10 media)."""
    if not chat_id or chat_id.startswith("YOUR_"):
        return False
    url_api = f"https://api.telegram.org/bot{token}/sendMediaGroup"

    # Split URLs into chunks of 10
    chunks = [urls[i:i + 10] for i in range(0, len(urls), 10)]
    
    overall_success = True
    for chunk_idx, chunk_urls in enumerate(chunks):
        for attempt in range(3):
            try:
                files = {}
                media_group = []
                for i, img_url in enumerate(chunk_urls):
                    res = requests.get(img_url, headers={"User-Agent": USER_AGENT}, timeout=30)
                    if res.status_code == 200:
                        file_key = f'photo_{chunk_idx}_{i}'
                        files[file_key] = (f'{file_key}.jpg', res.content)
                        media_item = {"type": "photo", "media": f"attach://{file_key}"}
                        # Pasang caption hanya di foto pertama pada chunk pertama
                        if i == 0 and chunk_idx == 0:
                            media_item["caption"] = caption
                            media_item["parse_mode"] = "HTML"
                        media_group.append(media_item)
                
                if not media_group:
                    break # Semua download gagal di chunk ini

                payload = {"chat_id": chat_id, "media": json.dumps(media_group)}
                r = requests.post(url_api, data=payload, files=files, timeout=300)
                if r.json().get("ok"):
                    break # Sukses kirim chunk ini
                else:
                    logging.warning(f"Attempt {attempt+1} failed to send gallery chunk: {r.text}")
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
                logging.warning(f"Timeout/connection error sending gallery chunk. Assuming success to prevent duplicates.")
                break
            except Exception as e:
                logging.error(f"Attempt {attempt+1} error sending gallery: {e}")
            time.sleep(5)
        else:
            overall_success = False
            
        time.sleep(3) # Jeda agar tidak terkena limit rate Telegram
    return overall_success


def send_video(token, chat_id, video_url, caption, moving_preview_url=None, photo_preview_url=None):
    """Kirim video ke Telegram."""
    if not chat_id or chat_id.startswith("YOUR_"):
        return False
    url = f"https://api.telegram.org/bot{token}/sendVideo"

    for attempt in range(3):
        try:
            with requests.get(video_url, headers={"User-Agent": USER_AGENT}, timeout=60, stream=True) as res:
                if res.status_code != 200:
                    logging.warning(f"Attempt {attempt+1} failed to fetch video: Status {res.status_code}")
                    time.sleep(5)
                    continue

                content_length = res.headers.get("Content-Length")
                if content_length and int(content_length) > MAX_VIDEO_SIZE:
                    logging.warning(f"Video too large ({content_length} bytes). Trying moving preview.")
                    if moving_preview_url and moving_preview_url != video_url:
                        return send_video(token, chat_id, moving_preview_url, caption)
                    elif photo_preview_url:
                        return send_photo(token, chat_id, photo_preview_url, caption)
                    return False

                chunks = []
                downloaded = 0
                for chunk in res.iter_content(chunk_size=1024 * 1024):
                    downloaded += len(chunk)
                    if downloaded > MAX_VIDEO_SIZE:
                        logging.warning(f"Video exceeded {MAX_VIDEO_SIZE} bytes during download. Trying moving preview.")
                        if moving_preview_url and moving_preview_url != video_url:
                            return send_video(token, chat_id, moving_preview_url, caption)
                        elif photo_preview_url:
                            return send_photo(token, chat_id, photo_preview_url, caption)
                        return False
                    chunks.append(chunk)

                video_data = b"".join(chunks)

            files = {'video': ('video.mp4', video_data)}
            payload = {"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"}
            r = requests.post(url, data=payload, files=files, timeout=300)
            if r.json().get("ok"):
                return True
            logging.warning(f"Attempt {attempt+1} failed to upload video to Telegram")
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            logging.warning(f"Timeout/connection error sending video. Assuming success to prevent duplicates.")
            return True
        except Exception as e:
            logging.error(f"Attempt {attempt+1} error sending video: {e}")
        time.sleep(5)

    logging.error(f"All attempts failed to send video: {video_url}")
    return False


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
    
    # Deteksi Gallery (Album Reddit)
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

        # AI Filter
        if post_id not in history[feed_type]:
            if post_id not in history["ads"]:
                if check_is_ad_with_ai(title):
                    history["ads"].append(post_id)
                    if len(history["ads"]) > 500:
                        history["ads"] = history["ads"][-500:]
                    history[feed_type].append(post_id)
                    save_history(history)
                    continue
            else:
                logging.info(f"Skipping known ad in {feed_type}: {post_id}")
                history[feed_type].append(post_id)
                save_history(history)
                continue

        media_type, media_url, moving_preview, photo_preview = get_media_type_and_url(post)
        if media_type == "link":
            continue

        caption = f"<b>{title}</b>"

        # 1. Kirim ke channel feed (hot, new, top, atau rising)
        if post_id not in history[feed_type]:
            logging.info(f"New post found in {feed_type}: {post_id}")
            chat_id = channels.get(feed_type)
            if chat_id:
                if media_type == "photo":
                    send_photo(token, chat_id, media_url, caption)
                elif media_type == "video":
                    send_video(token, chat_id, media_url, caption, moving_preview, photo_preview)
                elif media_type == "gallery":
                    send_gallery(token, chat_id, media_url, caption)
                else:
                    send_message(token, chat_id, f"{caption}\n{media_url}")
                time.sleep(2)

            # [FIX ANTI-DUPLIKAT] Fire and Forget: 
            # Selalu catat ID ke history meskipun API Telegram error/timeout.
            # Ini memastikan bot tidak pernah terjebak dalam loop mengirim foto/ID yang sama.
            history[feed_type].append(post_id)
            if len(history[feed_type]) > 200:
                history[feed_type] = history[feed_type][-200:]
            save_history(history)

        # 2. Kirim ke channel kpop5 (Photo) & kpop6 (Video)
        if feed_type == "new" and post_id not in history["media"]:
            if media_type == "photo" and channels.get("photo"):
                logging.info(f"Routing new photo to kpop5: {post_id}")
                send_photo(token, channels["photo"], media_url, caption)
                time.sleep(2)
            elif media_type == "gallery" and channels.get("photo"):
                logging.info(f"Routing new gallery to kpop5: {post_id}")
                send_gallery(token, channels["photo"], media_url, caption)
                time.sleep(2)
            elif media_type == "video" and channels.get("video"):
                logging.info(f"Routing new video to kpop6: {post_id}")
                send_video(token, channels["video"], media_url, caption, moving_preview, photo_preview)
                time.sleep(2)

            # [FIX ANTI-DUPLIKAT] Fire and Forget untuk history media
            history["media"].append(post_id)
            if len(history["media"]) > 1000:
                history["media"] = history["media"][-1000:]
            save_history(history)


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