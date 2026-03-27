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
import re

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
The channel targets AMERICAN audience with betrayal, revenge, justice, and outrage stories in Reddit narration format.
ALL OUTPUT MUST BE IN ENGLISH.

Analyze the competitor video and generate ideas using Niche Bending system:
- Keep the same viral triggers and emotional structure, but use different story content
- Example: "I stole an apple from a monkey" -> "I stole a banana from a turtle" (same mechanic, different content)

IDEA 1: Same story concept adapted for American audience (Americanized names, places, cultural context)
IDEAS 2-5: Different Reddit stories using the SAME viral triggers and emotional structure

You MUST respond using EXACTLY this format (one item per line, no line breaks within items):
VIRAL_TRIGGER: [explain in 1-2 sentences why this video works - hook, escalation, payoff, comment bait]
OUTLINER: [Yes or No]
IDEA_1: Idea: [pitch] | Twist: [unexpected element] | Watch-till-end: [reason]
IDEA_2: Idea: [pitch] | Twist: [unexpected element] | Watch-till-end: [reason]
IDEA_3: Idea: [pitch] | Twist: [unexpected element] | Watch-till-end: [reason]
IDEA_4: Idea: [pitch] | Twist: [unexpected element] | Watch-till-end: [reason]
IDEA_5: Idea: [pitch] | Twist: [unexpected element] | Watch-till-end: [reason]"""

def generate_ideas(title, transcript):
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    has_transcript = transcript != "Транскрипция недоступна"
    content = f"Video title: {title}\n\n"
    if has_transcript:
        content += f"Transcript:\n{transcript}"
    else:
        content += "No transcript available. Analyze based on the title only and generate ideas."

    message = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1500,
        system=NICHE_BENDING_PROMPT,
        messages=[{"role": "user", "content": content}]
    )
    response = message.content[0].text
    print(f"    Claude response:\n{response[:300]}...")

    result = {"viral_trigger": "", "outliner": "", "ideas": ["", "", "", "", ""]}

    # More robust parsing - search anywhere in text
    vt = re.search(r'VIRAL_TRIGGER:\s*(.+?)(?=\nOUTLINER:|$)', response, re.DOTALL)
    if vt:
        result["viral_trigger"] = vt.group(1).strip().replace("\n", " ")

    ol = re.search(r'OUTLINER:\s*(.+?)(?=\nIDEA_|$)', response, re.DOTALL)
    if ol:
        result["outliner"] = ol.group(1).strip().split("\n")[0]

    for i in range(1, 6):
        idea = re.search(rf'IDEA_{i}:\s*(.+?)(?=\nIDEA_{i+1}:|$)', response, re.DOTALL)
        if idea:
            result["ideas"][i-1] = idea.group(1).strip().replace("\n", " ")

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

def get_existing_urls(sheet):
    try:
        urls = sheet.col_values(2)
        return set(urls[1:])  # skip header
    except:
        return set()

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
    existing_urls = get_existing_urls(sheet)
    print(f"  Уже в таблице: {len(existing_urls)} видео")

    rows = []
    for handle in CHANNELS:
        print(f"  -> {handle}")
        ch_id = get_channel_id(youtube, handle)
        if not ch_id:
            print(f"    Канал не найден: {handle}")
            continue
        videos = fetch_new_videos(youtube, ch_id, handle)
        print(f"    Найдено новых: {len(videos)}")

        for (title, url, ch, date, transcript) in videos:
            if url in existing_urls:
                print(f"    Пропускаю дубль: {title[:40]}")
                continue
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
                    "ERROR", "", "нет", "", "нет",
                    "", "нет", "", "нет", "", "нет",
                    "",
                ]
            rows.append(row)
            existing_urls.add(url)
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
