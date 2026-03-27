import os
import json
import time
from datetime import datetime, timedelta, timezone
from googleapiclient.discovery import build
from google.oauth2 import service_account
import gspread
from youtube_transcript_api import YouTubeTranscriptApi
from apscheduler.schedulers.blocking import BlockingScheduler

# ─── Config ───────────────────────────────────────────────────────────────────
YOUTUBE_API_KEY  = os.environ["YOUTUBE_API_KEY"]
SPREADSHEET_ID   = "1KwkW7DHzDuvmbhNHEeMqdamjelYzQOexfYM2iIO5R7s"
SHEET_NAME = "midnight script"

CHANNELS = [
    "@babynojamie",
    "@Snook_YT",
    "@tuchniyzhab",
    "@upvotemedia",
]

# ─── Google Sheets client ─────────────────────────────────────────────────────
def get_sheets_client():
    info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT"])
    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)

# ─── YouTube helpers ──────────────────────────────────────────────────────────
def get_channel_id(youtube, handle):
    handle_clean = handle.lstrip("@")
    r = youtube.search().list(
        part="snippet", q=handle_clean, type="channel", maxResults=1
    ).execute()
    items = r.get("items", [])
    return items[0]["snippet"]["channelId"] if items else None

def get_transcript(video_id):
    try:
        data = YouTubeTranscriptApi.get_transcript(
            video_id, languages=["en", "ru", "uk"]
        )
        text = " ".join(t["text"] for t in data)
        return text[:6000]
    except Exception:
        return "Транскрипция недоступна"

def fetch_new_videos(youtube, channel_id, handle):
    since = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    r = youtube.search().list(
        part="snippet",
        channelId=channel_id,
        publishedAfter=since,
        type="video",
        order="date",
        maxResults=10,
    ).execute()

    videos = []
    for item in r.get("items", []):
        vid        = item["id"]["videoId"]
        title      = item["snippet"]["title"]
        date       = item["snippet"]["publishedAt"][:10]
        url        = f"https://www.youtube.com/watch?v={vid}"
        transcript = get_transcript(vid)
        videos.append((title, url, handle, date, transcript))
    return videos

# ─── Main job ─────────────────────────────────────────────────────────────────
def run():
    print(f"[{datetime.now()}] Старт сбора видео...")
    youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
    gc      = get_sheets_client()
    sheet   = gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)

    rows = []
    for handle in CHANNELS:
        print(f"  → {handle}")
        ch_id = get_channel_id(youtube, handle)
        if not ch_id:
            print(f"    Канал не найден: {handle}")
            continue
        videos = fetch_new_videos(youtube, ch_id, handle)
        print(f"    Найдено видео: {len(videos)}")

        for (title, url, ch, date, transcript) in videos:
            # Колонки:
            # A Название | B Ссылка | C Канал | D Дата | E Транскрипция
            # F Вирусный триггер | G Идея 1 | H да/нет | I Идея 2 | J да/нет
            # K Идея 3 | L да/нет | M Идея 4 | N да/нет | O Идея 5 | P да/нет
            # Q Outliner
            rows.append([
                title, url, ch, date, transcript,
                "", "", "нет", "", "нет",
                "", "нет", "", "нет", "", "нет",
                "",
            ])
        time.sleep(0.5)

    if rows:
        sheet.append_rows(rows, value_input_option="USER_ENTERED")
        print(f"  Добавлено строк: {len(rows)}")
    else:
        print("  Новых видео нет.")
    print("  Готово.")

# ─── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run()  # запуск сразу при старте контейнера

    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(run, "cron", hour=7, minute=0)  # 07:00 UTC = 09:00 Киев
    print("Планировщик активен. Следующий запуск — 07:00 UTC")
    scheduler.start()
