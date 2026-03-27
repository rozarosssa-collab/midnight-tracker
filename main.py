import os
import json
import time
from datetime import datetime, timedelta, timezone
from googleapiclient.discovery import build
from google.oauth2 import service_account
import gspread
from youtube_transcript_api import YouTubeTranscriptApi
from apscheduler.schedulers.blocking import BlockingScheduler
import anthropic

YOUTUBE_API_KEY   = os.environ["YOUTUBE_API_KEY"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
SPREADSHEET_ID    = "1KwkW7DHzDuvmbhNHEeMqdamjelYzQOexfYM2iIO5R7s"
SHEET_NAME        = "midnight script"

CHANNELS = [
    "@babynojamie",
    "@Snook_YT",
    "@tuchniyzhab",
    "@upvotemedia",
]

NICHE_BENDING_PROMPT = """You are a viral content strategist for a Reddit-style YouTube channel called Midnight Archive.
The channel targets American audience with betrayal, revenge, justice, and outrage stories in Reddit narration format.
Emotions targeted: outrage, justice, recognition, betrayal.

Analyze the competitor video and generate ideas using Niche Bending:
- Same viral triggers + same structure, but different story content
- Like: stole apple from monkey -> stole banana from turtle (same mechanic, different content)

Idea 1: SAME story adapted for American audience (same triggers, Americanized names/context/culture)
Ideas 2-5: Different Reddit stories with the SAME viral triggers and structure

Respond in EXACT format:
VIRAL_TRIGGER: [why this works - hook, escalation, payoff, comment bait]
OUTLINER: [Yes/No]
IDEA_1: Idea: [text] | Twist: [text] | Watch-till-end: [text]
IDEA_2: Idea: [text] | Twist: [text] | Watch-till-end: [text]
IDEA_3: Idea: [text] | Twist: [text] | Watch-till-end: [text]
IDEA_4: Idea: [text] | Twist: [text] | Watch-till-end: [text]
IDEA_5: Idea: [text] | Twist: [text] | Watch-till-end: [text]"""

def generate_ideas(title, transcript):
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    content = f"Video title: {title}\n\nTranscript:\n{transcript if transcript != 'Транскрипция недоступна' else '[No transcript, analyze by title only]'}"
    message = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1500,
        system=NICHE_BENDING_PROMPT,
        messages=[{"role": "user", "content": content}]
    )
    response = message.content[0].text
    result = {"viral_trigger": "", "outliner": "", "ideas": ["", "", "", "", ""]}
    for line in response.split("\n"):
        line = line.strip()
        if line.startswith("VIRAL_TRIGGER:"):
            result["viral_trigger"] = line.replace("VIRAL_TRIGGER:", "").strip()
        elif line.startswith("OUTLINER:"):
            result["outliner"] = line.replace("OUTLINER:", "").strip()
        elif line.startswith("IDEA_1:"):
            result["ideas"][0] = line.replace("IDEA_1:", "").strip()
        elif line.startswith("IDEA_2:"):
            result["ideas"][1] = line.replace("IDEA_2:", "").strip()
        elif line.startswith("IDEA_3:"):
            result["ideas"][2] = line.replace("IDEA_3:", "").strip()
        elif line.startswith("IDEA_4:"):
            result["ideas"][3] = line.replace("IDEA_4:", "").strip()
        elif line.startswith("IDEA_5:"):
            result["ideas"][4] = line.replace("IDEA_5:", "").strip()
    return result

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

def get_channel_id(youtube, handle):
    handle_clean = handle.lstrip("@")
    r = youtube.search().list(
        part="snippet", q=handle_clean, type="channel", maxResults=1
    ).execute()
    items = r.get("items", [])
    return items[0]["snippet"]["channelId"] if items else None

def get_transcript(video_id):
    try:
        data = YouTubeTranscriptApi.get_transcript(video_id, languages=["en", "ru", "uk"])
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

def run():
    print(f"[{datetime.now()}] Старт сбора видео...")
    youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
    gc      = get_sheets_client()
    sheet   = gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)

    rows = []
    for handle in CHANNELS:
        print(f"  -> {handle}")
        ch_id = get_channel_id(youtube, handle)
        if not ch_id:
            print(f"    Канал не найден: {handle}")
            continue
        videos = fetch_new_videos(youtube, ch_id, handle)
        print(f"    Найдено видео: {len(videos)}")
        for (title, url, ch, date, transcript) in videos:
            print(f"    Генерирую идеи: {title[:50]}...")
            try:
                ideas = generate_ideas(title, transcript)
                row = [
                    title, url, ch, date, transcript,
                    ideas["viral_trigger"],
                    ideas["ideas"][0], "нет",
                    ideas["ideas"][1], "нет",
                    ideas["ideas"][2], "нет",
                    ideas["ideas"][3], "нет",
                    ideas["ideas"][4], "нет",
                    ideas["outliner"],
                ]
            except Exception as e:
                print(f"    Ошибка: {e}")
                row = [
                    title, url, ch, date, transcript,
                    "", "", "нет", "", "нет",
                    "", "нет", "", "нет", "", "нет",
                    "",
                ]
            rows.append(row)
            time.sleep(1)

    if rows:
        sheet.append_rows(rows, value_input_option="USER_ENTERED")
        print(f"  Добавлено строк: {len(rows)}")
    else:
        print("  Новых видео нет.")
    print("  Готово.")

if __name__ == "__main__":
    run()
    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(run, "cron", hour=7, minute=0)
    print("Планировщик активен. Следующий запуск — 07:00 UTC")
    scheduler.start()
