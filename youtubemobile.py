import os
import re
import sqlite3
from io import BytesIO
from datetime import datetime, timedelta, UTC
from zoneinfo import ZoneInfo
import random
import base64
import time

import requests
import pandas as pd
import streamlit as st
from dateutil import parser as dateparser

# --- Page Configuration ---
st.set_page_config(
    page_title="CISF YouTube Monitor",
    page_icon="logo.jpeg",
    layout="centered"  # Changed from "wide" to "centered" for mobile
)

# -------------------------
# CONFIG
# -------------------------
API_KEYS = [
    "AIzaSyBKYB1F1M8T-QtjtICMC4hGk10v1qYxCs0",
    "AIzaSyBoIlS8vpSGmb-kEQ6PNNwJuvFbtEOyOv4",
    "AIzaSyAi23dPzb6FGYxzZi0UqIXz6iz90Kdv4R8",
    "AIzaSyDneStGWthm9BuWkpB12iz0LXQsTrJkuYI",
    "AIzaSyA4BwZQ1x1XE0gZRDpItsQjAVljlDosB0Y",
    "AIzaSyDAr-Xic0Is_WMqA-JuDYQaQcVSsli9fl8",
    "AIzaSyD61qtlbCDVeky6f6fIKPm6dRRjHS-yszw",
    "AIzaSyAqj9yxsW_RkYiIEBZvKx1_OL3PuolEjrc",
    "AIzaSyBaXD7DQs7gLgsPEmzD0K6PzsugnATuWR4",
    "AIzaSyDfbbMUf3F1ltRHelYicygGchJdmBPHl4A",
    "AIzaSyDAGWhWH5wBMBo-tG7l5zSGXKYLjfvinso",
    "AIzaSyAPbRK5fOo5m37zHGbFlm5HwO9H49_Kxtc",
]

YOUTUBE_SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
YOUTUBE_VIDEO_URL = "https://www.googleapis.com/youtube/v3/videos"
YOUTUBE_CHANNEL_URL = "https://www.googleapis.com/youtube/v3/channels"
DB_PATH = "cisf_youtube.db"
WATCHLIST_FILE_PATH = "watchlist.txt"
SHORTS_MAX_DURATION = 60  # seconds
CLEAR_ON_STARTUP = True
ALERT_VIEW_THRESHOLD = 25000 # High view count for an alert
IST = ZoneInfo("Asia/Kolkata")

# -------------------------
# STREAMLIT: clear cache on each run
# -------------------------
try:
    st.cache_data.clear()
except Exception:
    pass
try:
    st.cache_resource.clear()
except Exception:
    pass

# -------------------------
# UTILITIES
# -------------------------
def utcnow():
    """Returns the current time in UTC."""
    return datetime.now(UTC)

def to_rfc3339_z(dt: datetime) -> str:
    """Return RFC3339 UTC Z format: YYYY-MM-DDTHH:MM:SSZ"""
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

def to_sql_utc_string(dt: datetime) -> str:
    """Return SQLite-friendly UTC string: YYYY-MM-DD HH:MM:SS"""
    return dt.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")

def ensure_sql_utc_string(val) -> str:
    """
    Accepts datetime or string. If datetime -> normalize to UTC 'YYYY-MM-DD HH:MM:SS'.
    If string -> return as-is (assumed pre-normalized). If None -> now.
    """
    if isinstance(val, datetime):
        return to_sql_utc_string(val)
    if isinstance(val, str) and val:
        return val
    return to_sql_utc_string(utcnow())

def parse_duration_iso8601(duration: str) -> int:
    """Parses an ISO 8601 duration string into seconds."""
    m = re.match(r"^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$", duration or "")
    if not m:
        return 0
    h = int(m.group(1) or 0)
    m_ = int(m.group(2) or 0)
    s = int(m.group(3) or 0)
    return h * 3600 + m_ * 60 + s

def try_request(url, params, timeout=15):
    """Cycles through API keys to make a request. Returns response object on success or last failure."""
    last_response = None
    for key in API_KEYS:
        p = dict(params)
        p["key"] = key
        try:
            r = requests.get(url, params=p, timeout=timeout)
            if r.status_code == 200:
                return r
            last_response = r  # Store the last failed response
        except requests.exceptions.RequestException as e:
            # Create a mock response for connection errors or other request exceptions
            last_response = requests.Response()
            last_response.status_code = 500  # Internal Server Error or similar
            last_response.reason = "Request Exception"
            last_response._content = str(e).encode('utf-8')
            continue # Try next key
    return last_response # Return the last response, whether success or fail

def chunked(iterable, n):
    """Yield successive n-sized chunks from iterable."""
    for i in range(0, len(iterable), n):
        yield iterable[i:i + n]

def simulate_sentiment_analysis(likes, comments):
    """Simulates sentiment based on engagement stats."""
    if likes == 0 and comments == 0: return "Neutral"
    ratio = likes / (comments + 1)
    if ratio > 10: return "Positive"
    if ratio < 1: return "Negative"
    return "Neutral"

# -------------------------
# SQLITE DATABASE
# -------------------------
def db_connect():
    """Connects to the SQLite database."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def db_init(conn):
    """Initializes the database schema if it doesn't exist."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS videos (
            videoId TEXT PRIMARY KEY, title TEXT, channel TEXT, channelId TEXT,
            publishedAt TEXT, views INTEGER, likes INTEGER, comments INTEGER,
            category TEXT, duration INTEGER, liveStatus TEXT, url TEXT,
            thumbnail TEXT, firstSeenSource TEXT, lastUpdated TEXT, 
            sentiment TEXT, sourceKeyword TEXT, serial INTEGER UNIQUE
        );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_publishedAt ON videos(publishedAt);")
    conn.commit()

def db_migrate(conn):
    """Adds new columns to the table if they don't exist."""
    cur = conn.cursor()
    try: cur.execute("ALTER TABLE videos ADD COLUMN sentiment TEXT;")
    except: pass
    try: cur.execute("ALTER TABLE videos ADD COLUMN sourceKeyword TEXT;")
    except: pass
    conn.commit()

def db_upsert_videos(conn, rows):
    """Inserts or updates video data in the database."""
    cur = conn.cursor()
    now_str = to_sql_utc_string(utcnow())
    
    video_ids = [r["videoId"] for r in rows]
    qmarks = ",".join("?" for _ in video_ids)
    cur.execute(f"SELECT videoId FROM videos WHERE videoId IN ({qmarks})", video_ids)
    existing_ids = {row[0] for row in cur.fetchall()}

    for r in rows:
        vid = r["videoId"]
        if vid in existing_ids:
            cur.execute("""
                UPDATE videos SET title=?, channel=?, channelId=?, publishedAt=?, views=?, likes=?,
                comments=?, category=?, duration=?, liveStatus=?, url=?, thumbnail=?,
                lastUpdated=?, sentiment=?, sourceKeyword=COALESCE(sourceKeyword, ?) WHERE videoId=?;
            """, (r.get("title",""), r.get("channel",""), r.get("channelId",""),
                  ensure_sql_utc_string(r.get("publishedAt")), int(r.get("views",0) or 0),
                  int(r.get("likes",0) or 0), int(r.get("comments",0) or 0), r.get("category",""),
                  int(r.get("duration",0) or 0), r.get("liveStatus",""), r.get("url",""),
                  r.get("thumbnail",""), now_str, r.get("sentiment"), r.get("sourceKeyword"), vid))
        else:
            cur.execute("SELECT COALESCE(MAX(serial), 0) FROM videos;")
            serial = cur.fetchone()[0] + 1
            cur.execute("""
                INSERT INTO videos (videoId, title, channel, channelId, publishedAt, views, likes, comments,
                category, duration, liveStatus, url, thumbnail, firstSeenSource, lastUpdated, sentiment, sourceKeyword, serial)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
            """, (vid, r.get("title",""), r.get("channel",""), r.get("channelId",""),
                  ensure_sql_utc_string(r.get("publishedAt")), int(r.get("views",0) or 0),
                  int(r.get("likes",0) or 0), int(r.get("comments",0) or 0), r.get("category",""),
                  int(r.get("duration",0) or 0), r.get("liveStatus",""), r.get("url",""),
                  r.get("thumbnail",""), r.get("sourceKeyword", "pull"), now_str, r.get("sentiment"), r.get("sourceKeyword"), serial))
    conn.commit()

def db_fetch_all_videos(conn):
    """Fetches all video data from the database into a DataFrame."""
    return pd.read_sql_query("SELECT * FROM videos", conn)

def db_get_video_ids(conn, published_after: datetime|None, limit: int|None = None, order_by_published: bool = False):
    """Gets video IDs from the database, with optional filtering, ordering, and limit."""
    cur = conn.cursor()
    query = "SELECT videoId FROM videos"
    params = []

    if published_after:
        query += " WHERE datetime(publishedAt) >= datetime(?)"
        params.append(to_sql_utc_string(published_after))
    
    if order_by_published:
        query += " ORDER BY datetime(publishedAt) DESC"
        
    if limit:
        query += " LIMIT ?"
        params.append(limit)

    cur.execute(query, tuple(params))
    return [r[0] for r in cur.fetchall()]

# -------------------------
# YOUTUBE API HELPERS
# -------------------------
def youtube_search(query, **kwargs):
    """Searches YouTube for videos matching a query."""
    ids_with_keyword = []
    page_token = None
    params = { "part": "snippet", "q": query, "type": "video", "maxResults": 50, "order": "date", **kwargs }
    
    while True:
        if page_token: params["pageToken"] = page_token
        r = try_request(YOUTUBE_SEARCH_URL, params)
        if not r or r.status_code != 200:
            st.error(f"API Error: YouTube search for '{query}' failed. Status: {r.status_code if r else 'Connection Error'}. Please check API keys/quota.", icon="🚨")
            break
        
        data = r.json()
        for item in data.get("items", []):
            if "videoId" in item.get("id", {}):
                ids_with_keyword.append({"videoId": item["id"]["videoId"], "sourceKeyword": query})
        
        page_token = data.get("nextPageToken")
        if not page_token or len(ids_with_keyword) >= params.get("maxResults", 50): break
            
    return ids_with_keyword

def get_video_details(video_objs):
    """Fetches detailed information for a list of video IDs."""
    video_ids = [v['videoId'] for v in video_objs]
    source_keywords = {v['videoId']: v.get('sourceKeyword') for v in video_objs}
    
    results = []
    for batch_ids in chunked(video_ids, 50):
        params = {"part": "snippet,statistics,liveStreamingDetails,contentDetails", "id": ",".join(batch_ids)}
        r = try_request(YOUTUBE_VIDEO_URL, params)
        if not r or r.status_code != 200:
            st.error(f"API Error: Could not fetch video details. Status: {r.status_code if r else 'Connection Error'}.", icon="🚨")
            continue
        
        data = r.json()
        for item in data.get("items", []):
            snippet, stats = item.get("snippet", {}), item.get("statistics", {})
            live, content = item.get("liveStreamingDetails", {}), item.get("contentDetails", {})
            
            duration = parse_duration_iso8601(content.get("duration", "PT0S"))
            category = "Short" if duration <= SHORTS_MAX_DURATION else "Video"
            likes = int(stats.get("likeCount", 0) or 0)
            comments = int(stats.get("commentCount", 0) or 0)
            
            results.append({
                "videoId": item["id"], "title": snippet.get("title"), "channel": snippet.get("channelTitle"),
                "channelId": snippet.get("channelId"), "publishedAt": snippet.get("publishedAt"),
                "views": int(stats.get("viewCount", 0) or 0), "likes": likes, "comments": comments,
                "category": category, "duration": duration,
                "liveStatus": "LIVE" if "actualStartTime" in live else ("UPCOMING" if "scheduledStartTime" in live else "NORMAL"),
                "url": f"https://www.youtube.com/watch?v={item['id']}",
                "thumbnail": snippet.get("thumbnails", {}).get("high", {}).get("url"),
                "sourceKeyword": source_keywords.get(item["id"]),
                "sentiment": simulate_sentiment_analysis(likes, comments)
            })
    return results

def get_channel_details(channel_ids):
    """Fetches detailed information for a list of channel IDs."""
    if not channel_ids: return []
    results = []
    for batch_ids in chunked(list(set(channel_ids)), 50): # Use set to avoid duplicate API calls
        params = {"part": "snippet,statistics", "id": ",".join(batch_ids)}
        r = try_request(YOUTUBE_CHANNEL_URL, params)
        if not r or r.status_code != 200:
            st.error(f"API Error: Could not fetch channel details. Status: {r.status_code if r else 'Connection Error'}.", icon="🚨")
            continue
        
        data = r.json()
        for item in data.get("items", []):
            snippet = item.get("snippet", {})
            stats = item.get("statistics", {})
            results.append({
                "channelId": item.get("id"),
                "channelName": snippet.get("title"),
                "description": snippet.get("description"),
                "thumbnail": snippet.get("thumbnails", {}).get("default", {}).get("url"),
                "subscriberCount": int(stats.get("subscriberCount", 0) or 0),
                "videoCount": int(stats.get("videoCount", 0) or 0),
                "url": f"https://www.youtube.com/channel/{item.get('id')}"
            })
    return results

@st.cache_data(ttl=3600) # Cache handle lookups for an hour
def get_channel_id_from_handle(handle):
    """Fetches a channel ID from a custom handle/URL (@handle) using the search API."""
    params = {"part": "id", "q": handle, "type": "channel", "maxResults": 1}
    r = try_request(YOUTUBE_SEARCH_URL, params)
    if r and r.status_code == 200:
        data = r.json()
        items = data.get("items", [])
        if items and items[0].get("id", {}).get("kind") == "youtube#channel":
            return items[0]["id"]["channelId"]
    # Don't show error in function, handle it in the main app logic
    return None

# ==============================
# 🔹 STREAMLIT APP
# ==============================

# --- CSS & STYLING ---
st.markdown("""<style>
/* Base styles for dark mode (default) */
:root {
    --bg-color: #121212; --text-color: #e0e0e0; --sidebar-bg: #1e1e1e; --sidebar-border: #333333;
    --header-color: #f0f0f0; --subheader-color: #a0a0a0; --card-bg: #1e1e1e; --table-bg: #1e1e1e;
    --table-header-bg: #282828; --table-text-color: #e0e0e0; --table-header-text-color: #a0a0a0;
    --table-row-hover: #2a2a2a; --table-border-color: #333333; --button-bg: #ff671f; /* Saffron */
    --button-hover-bg: #046a38; /* Green */ --button-text: white; --accent-color: #046a38; /* Green */
}
/* Light mode overrides using Streamlit's data-theme attribute */
body[data-theme="light"] :root {
    --bg-color: #f0f2f6; --text-color: #333; --sidebar-bg: #ffffff; --sidebar-border: #e0e0e0;
    --header-color: #333; --subheader-color: #6c757d; --card-bg: #ffffff; --table-bg: #ffffff;
    --table-header-bg: #f8f9fa; --table-text-color: #4a4a4a; --table-header-text-color: #6c757d;
    --table-row-hover: #f0f8ff; --table-border-color: #f0f2f6;
}
/* Main app container */
.stApp { background-color: var(--bg-color); color: var(--text-color); }
/* Thematic Button styling */
div.stButton > button {
    background-color: var(--button-bg); color: var(--button-text); border-radius: 25px; font-size: 14px;
    font-weight: bold; padding: 10px 24px; border: 2px solid #046a38; /* Green border */
    transition: all 0.3s ease-in-out; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
}
div.stButton > button:hover {
    background-color: var(--button-hover-bg); transform: translateY(-2px);
    box-shadow: 0 6px 10px rgba(0, 0, 0, 0.2); border-color: #ff671f; /* Saffron border on hover */
}
/* Make metric labels smaller */
.stMetric > div[data-testid="metric-container"] > div[data-testid="stMetricLabel"] {
    font-size: 0.85em;
    color: var(--subheader-color);
}
/* Ensure containers in columns have consistent height (helps alignment) */
.st-emotion-cache-1n6n36p { 
    height: 100%; 
}
</style>""", unsafe_allow_html=True)

# --- HEADER ---
# NOTE: You need to have a 'logo.png' file in the same directory for this to work.
logo_path = "logo.png"
if os.path.exists(logo_path):
    with open(logo_path, "rb") as f:
        logo_b64 = base64.b64encode(f.read()).decode()
    st.markdown(f"""<div style="text-align: center;">
        <img src="data:image/jpeg;base64,{logo_b64}" alt="CISF Logo" style="width: 40%; max-width: 150px;">
    </div>""", unsafe_allow_html=True)

st.markdown("<h1 style='text-align: center; font-size: 1.8em;'>CISF YouTube Monitoring Tool</h1>", unsafe_allow_html=True)
st.markdown("<h3 style='text-align: center; color: var(--subheader-color); font-size: 1.2em;'>YouTube Dashboard</h3>", unsafe_allow_html=True)

# --- INITIALIZATION ---
if "started" not in st.session_state:
    st.session_state.started = True
    conn = db_connect()
    db_init(conn)
    db_migrate(conn)
    if CLEAR_ON_STARTUP:
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM videos;")
            conn.commit()
        except Exception: pass
    conn.close()
    st.session_state.last_updated_api = None
    st.session_state.video_to_play = None
    st.session_state.last_updated = datetime.now(IST)
    st.session_state.pinned_inputs = [""]
    
    # Load Watchlist from file on first run
    initial_watchlist = [""]
    if os.path.exists(WATCHLIST_FILE_PATH):
        try:
            with open(WATCHLIST_FILE_PATH, "r") as f:
                saved_watchlist = [line.strip() for line in f if line.strip()]
                if saved_watchlist:
                    initial_watchlist = saved_watchlist
        except Exception as e:
            st.error(f"Could not load watchlist file: {e}")
    if not initial_watchlist or initial_watchlist[-1] != "":
        initial_watchlist.append("")
    st.session_state.watchlist_inputs = initial_watchlist

    st.session_state.pinned_video_ids = []
    st.session_state.queries = "CISF, सीआईएसएफ, #cisf"
    st.session_state.search_query = ""
    st.session_state.selected_channel = "All Channels"
    st.session_state.time_filter_option = "All Time"
    st.session_state.sort_by = "Newest"
    st.session_state.category_filter = "All"

conn = db_connect()
df = db_fetch_all_videos(conn)
if not df.empty:
    df['publishedAt'] = pd.to_datetime(df['publishedAt'], errors='coerce', utc=True)
    df.dropna(subset=['publishedAt'], inplace=True)
all_channels = sorted(df['channel'].unique()) if not df.empty else []


# --- SETTINGS EXPANDER (MOVED FROM SIDEBAR) ---
with st.expander("⚙️ Settings & Watchlist", expanded=False):
    st.markdown(f"**Time:** {datetime.now(IST).strftime('%d %b %Y, %I:%M %p')}")
    st.text_area("Keyword Queries (comma-separated):", key="queries")
    st.markdown("---")
    
    st.markdown("## 📌 Pinned Videos")
    st.info("Add video URLs or IDs here to pin them to the Pinned Videos tab.")
    new_values = []
    for i, val in enumerate(st.session_state.pinned_inputs):
        v = st.text_input(f"Pinned Video {i+1} (ID or URL):", val, key=f"pinned_{i}")
        new_values.append(v)
    if new_values and new_values[-1].strip() != "":
        new_values.append("")
    st.session_state.pinned_inputs = new_values
    
    st.markdown("---")
    st.markdown("## 📺 Watch List")
    st.info("Add channel URLs or IDs, then click Update.")
    new_watchlist_values = []
    for i, val in enumerate(st.session_state.watchlist_inputs):
        v = st.text_input(f"Channel {i+1} (ID or URL):", val, key=f"watchlist_{i}")
        new_watchlist_values.append(v)
    if new_watchlist_values and new_watchlist_values[-1].strip() != "":
        new_watchlist_values.append("")
    st.session_state.watchlist_inputs = new_watchlist_values
    
    if st.button("Update Watch List", key="update_watchlist"):
        # Save the current watchlist to a file
        try:
            with open(WATCHLIST_FILE_PATH, "w") as f:
                for entry in st.session_state.watchlist_inputs:
                    if entry.strip():
                        f.write(f"{entry.strip()}\n")
            st.toast("Watch list saved!", icon="💾")
        except Exception as e:
            st.error(f"Could not save watchlist: {e}")
        # The button click will also trigger a rerun


# Consolidate pins from settings into the main list of pinned IDs
manual_pins = set(st.session_state.pinned_video_ids)
for part in st.session_state.pinned_inputs:
    part = part.strip()
    if not part: continue
    m = re.search(r"(?:v=|/)([a-zA-Z0-9_-]{11})", part)
    video_id_to_add = None
    if m: video_id_to_add = m.group(1)
    elif len(part) == 11: video_id_to_add = part
    
    if video_id_to_add:
        manual_pins.add(video_id_to_add)
st.session_state.pinned_video_ids = list(manual_pins)


# --- FILTER WIDGETS AND LOGIC (BEFORE TABS) ---
with st.expander("🎛️ Filter Videos", expanded=False):
    # Responsive filter layout
    f_cols_1 = st.columns(2)
    search_query = f_cols_1[0].text_input("🔍 Search", key="search_query")
    selected_channel = f_cols_1[1].selectbox("Channel", ["All Channels"] + all_channels, key="selected_channel")
    
    f_cols_2 = st.columns(3)
    time_filter_option = f_cols_2[0].selectbox("Timeframe", ["All Time", "Last 1h", "Last 6h", "Last 24h", "Last 7d"], key="time_filter_option")
    sort_by = f_cols_2[1].selectbox("Sort by", ["Newest", "Most Viewed", "Most Commented"], key="sort_by")
    category_filter = f_cols_2[2].selectbox("Type", ["All", "Short", "Video", "LIVE", "UPCOMING"], key="category_filter")

# Apply filtering
df_filtered = df.copy()
if time_filter_option != "All Time":
    hours_map = {"Last 1h": 1, "Last 6h": 6, "Last 24h": 24, "Last 7d": 168}
    cutoff = utcnow() - timedelta(hours=hours_map[time_filter_option])
    df_filtered = df_filtered[df_filtered['publishedAt'] >= cutoff]
if category_filter != "All":
    if category_filter in ["Short", "Video"]: df_filtered = df_filtered[df_filtered["category"] == category_filter]
    else: df_filtered = df_filtered[df_filtered["liveStatus"] == category_filter]
if selected_channel != "All Channels":
    df_filtered = df_filtered[df_filtered['channel'] == selected_channel]
if search_query:
    df_filtered = df_filtered[df_filtered['title'].str.contains(search_query, case=False, na=False) | df_filtered['channel'].str.contains(search_query, case=False, na=False)]
if sort_by == "Newest": df_filtered = df_filtered.sort_values(by="publishedAt", ascending=False)
elif sort_by == "Most Viewed": df_filtered = df_filtered.sort_values(by="views", ascending=False)
elif sort_by == "Most Commented": df_filtered = df_filtered.sort_values(by="comments", ascending=False)


# ==============================
# 🔹 TABS & LAYOUT
# ==============================
tab1, tab2, tab3, tab4 = st.tabs(["📊 Main", "📈 Analytics", "📌 Pinned", "📺 Watch List"])

# Helper functions for formatting
def format_published_time(utc_dt):
    try: return utc_dt.astimezone(IST).strftime("%d %b %Y, %I:%M %p")
    except: return ""

def get_status_icon(row):
    if row['liveStatus'] == 'LIVE': return "<span style='color:red;'>🔴 LIVE</span>"
    if row['liveStatus'] == 'UPCOMING': return "<span style='color:orange;'>🟠 UPCOMING</span>"
    return "🎬 Short" if row['category'] == 'Short' else "▶️ Video"

# Function to render a video card (re-usable for tab1 and tab3)
def render_video_card(row, is_pinned_view=False):
    
    # Format data for display
    title = row.get('title', 'No Title')
    channel = row.get('channel', 'No Channel')
    published_time = format_published_time(row.get('publishedAt'))
    status = get_status_icon(row)
    views = f"{int(row.get('views', 0)):,}"
    likes = f"{int(row.get('likes', 0)):,}"
    comments = f"{int(row.get('comments', 0)):,}"
    thumbnail_url = row.get('thumbnail')
    video_url = row.get('url', '#')
    video_id = row.get('videoId')

    with st.container(border=True):
        # --- Row 1: Thumbnail + Info ---
        col1, col2 = st.columns([1, 2])
        with col1:
            if thumbnail_url:
                # *** FIXED DEPRECATION WARNING HERE ***
                st.image(thumbnail_url, use_container_width=True)
        with col2:
            st.markdown(f"**{title}**")
            st.markdown(f"_{channel}_")
            st.caption(f"{published_time}")
            st.markdown(status, unsafe_allow_html=True)
        
        # --- Row 2: Stats (Collapsible) ---
        # *** MODIFIED THIS SECTION TO BE COLLAPSIBLE ***
        with st.expander("Show Stats"):
            col3, col4, col5 = st.columns(3)
            col3.metric("Views", views)
            col4.metric("Likes", likes)
            col5.metric("Comments", comments)
        
        # --- Row 3: Actions ---
        col6, col7 = st.columns([1,1])
        with col6:
            # Pin/Unpin Button
            if is_pinned_view:
                if st.button("❌ Unpin", key=f"unpin_{video_id}", use_container_width=True):
                    st.session_state.pinned_video_ids.remove(video_id)
                    st.rerun()
            else:
                is_pinned = video_id in st.session_state.pinned_video_ids
                button_label = "✅ Pinned" if is_pinned else "📌 Pin"
                if st.button(button_label, key=f"pin_{video_id}", use_container_width=True):
                    if is_pinned:
                        st.session_state.pinned_video_ids.remove(video_id)
                    else:
                        st.session_state.pinned_video_ids.append(video_id)
                    st.rerun()
        
        with col7:
            # Link Button
            st.link_button("🔗 Watch", video_url, use_container_width=True)


with tab1:
    with st.expander("⚙️ Actions & Data Management", expanded=False):
        # --- ACTION BUTTONS ---
        action_cols = st.columns(3)
        if action_cols[0].button("🔄 Refresh", use_container_width=True, help="Full Refresh"):
            with st.spinner('Refreshing...'):
                # Step 1 - Delete old videos
                seven_days_ago = to_sql_utc_string(utcnow() - timedelta(days=7))
                cur = conn.cursor()
                cur.execute("DELETE FROM videos WHERE datetime(publishedAt) < datetime(?)", (seven_days_ago,))
                deleted_count = cur.rowcount
                conn.commit()
                if deleted_count > 0:
                    st.toast(f"🧹 Cleared {deleted_count} videos older than 7 days.", icon="🧹")

                # Step 2 - Search for new videos
                queries = [q.strip() for q in st.session_state.queries.split(',') if q.strip()]
                all_video_objs = []
                for q in queries:
                    all_video_objs.extend(youtube_search(q))
                
                # Step 3 - Get the last 100 video IDs to update
                ids_to_update = db_get_video_ids(conn, None, limit=100, order_by_published=True)
                for vid in ids_to_update:
                    all_video_objs.append({'videoId': vid, 'sourceKeyword': None})

                # Step 4 - De-duplicate and fetch details
                seen_ids = set()
                unique_video_objs = []
                for video_obj in all_video_objs:
                    if video_obj['videoId'] not in seen_ids:
                        unique_video_objs.append(video_obj)
                        seen_ids.add(video_obj['videoId'])
                
                if unique_video_objs:
                    details = get_video_details(unique_video_objs)
                    if details:
                        db_upsert_videos(conn, details)

                st.session_state.last_updated = datetime.now(IST)
                st.rerun()

        if action_cols[1].button("🗑️ Clear All", use_container_width=True):
            cur = conn.cursor()
            cur.execute("DELETE FROM videos;")
            conn.commit()
            st.success("All videos cleared from database.")
            st.rerun()

        if action_cols[2].button("⚡ Quick", use_container_width=True, help="Quick Update (Last Hour)"):
            with st.spinner('Fetching latest videos...'):
                queries = [q.strip() for q in st.session_state.queries.split(',') if q.strip()]
                latest_video_objs = []
                published_after_dt = utcnow() - timedelta(hours=1)
                published_after_str = to_rfc3339_z(published_after_dt)

                for q in queries:
                    latest_video_objs.extend(youtube_search(q, publishedAfter=published_after_str))
                
                if latest_video_objs:
                    seen_ids = set()
                    unique_latest_objs = [obj for obj in latest_video_objs if obj['videoId'] not in seen_ids and not seen_ids.add(obj['videoId'])]
                    
                    if unique_latest_objs:
                        details = get_video_details(unique_latest_objs)
                        if details:
                            db_upsert_videos(conn, details)
                
                st.session_state.last_updated = datetime.now(IST)
                st.rerun()

    # --- VIDEO LIST ---
    st.markdown("### Video List")
    st.markdown(f"<p style='font-size: 0.9em; margin-top: -10px;'>Last updated: {st.session_state.last_updated.strftime('%d %b %Y, %I:%M %p IST')}</p>", unsafe_allow_html=True)
    
    display_df = df_filtered.copy()
    display_df["Published Time"] = display_df["publishedAt"].apply(format_published_time)
    display_df["Status"] = display_df.apply(get_status_icon, axis=1)

    if display_df.empty:
        st.info("No videos found matching your criteria. Try refreshing or adjusting filters.")
    else:
        st.markdown(f"**Showing {len(display_df)} videos**")
        for i, row in display_df.iterrows():
            render_video_card(row, is_pinned_view=False)


with tab2:
    st.header("📈 Analytics")
    st.markdown("In-depth analysis of the currently filtered video data.")
    
    if df_filtered.empty:
        st.warning("No data matches the current filters. Please adjust filters on the Main tab.")
    else:
        # --- KPI CARDS ---
        total_videos, total_views, total_comments = len(df_filtered), df_filtered['views'].sum(), df_filtered['comments'].sum()
        kpi1, kpi2, kpi3 = st.columns(3)
        kpi1.metric("Videos Found", f"{total_videos}")
        kpi2.metric("Total Views", f"{total_views:,}")
        kpi3.metric("Total Comments", f"{total_comments:,}")
        st.markdown("---")
        
        # --- SENTIMENT & KEYWORD ANALYSIS ---
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### Sentiment Analysis")
            sentiment_counts = df_filtered['sentiment'].value_counts()
            st.bar_chart(sentiment_counts)
        with col2:
            st.markdown("#### Keyword Performance")
            keyword_counts = df_filtered['sourceKeyword'].value_counts()
            st.bar_chart(keyword_counts)
        st.markdown("---")
        
        # --- CHARTS ---
        st.markdown("#### Top 10 Most Viewed Videos")
        top_10 = df_filtered.sort_values(by='views', ascending=False).head(10)
        top_10['label'] = top_10['title'].str[:30] + '...'
        st.bar_chart(top_10, x='label', y='views')
        
        st.markdown("#### Videos Published Over Time")
        df_daily = df_filtered.set_index('publishedAt').resample('D').size().reset_index(name='count')
        st.line_chart(df_daily.rename(columns={'publishedAt':'Date', 'count':'Videos'}), x='Date', y='Videos')

with tab3:
    st.header("📌 Pinned Videos")
    st.markdown("All videos you have pinned for quick access.")

    pinned_ids = st.session_state.get('pinned_video_ids', [])

    if not pinned_ids:
        st.info("No videos have been pinned yet. Click the 📌 icon on the Main tab to pin a video.")
    else:
        with st.spinner("Fetching details for pinned videos..."):
            pinned_details = get_video_details([{'videoId': v_id} for v_id in pinned_ids])
        
        if pinned_details:
            pinned_display_df = pd.DataFrame(pinned_details)
            pinned_display_df['publishedAt'] = pd.to_datetime(pinned_display_df['publishedAt'], errors='coerce', utc=True)
            
            pinned_display_df["Published Time"] = pinned_display_df["publishedAt"].apply(format_published_time)
            pinned_display_df["Status"] = pinned_display_df.apply(get_status_icon, axis=1)

            st.markdown(f"**Showing {len(pinned_display_df)} pinned videos**")
            for i, row in pinned_display_df.iterrows():
                render_video_card(row, is_pinned_view=True)
        else:
            st.warning("Could not fetch details for pinned videos.")

with tab4:
    st.header("📺 Channel Watch List")
    st.markdown("A list of channels to monitor. Add channels from the Settings expander at the top.")

    # Parse inputs to get channel IDs
    watchlist_ids = set()
    unparsed_entries = []
    
    with st.spinner("Resolving channel handles..."):
        for entry in st.session_state.watchlist_inputs:
            entry = entry.strip()
            if not entry: continue
            
            # Regex for standard UC... channel ID
            uc_match = re.search(r'(UC[a-zA-Z0-9_\-]{22})', entry)
            # Regex for new @handle format from URL
            handle_match = re.search(r'@([a-zA-Z0-9_.-]+)', entry)

            if uc_match:
                watchlist_ids.add(uc_match.group(1))
            elif handle_match:
                handle = handle_match.group(1)
                channel_id = get_channel_id_from_handle(handle)
                if channel_id:
                    watchlist_ids.add(channel_id)
                else:
                    unparsed_entries.append(entry)
            else:
                unparsed_entries.append(entry)

    if unparsed_entries:
        st.warning(f"Could not parse or resolve the following entries: `{'`, `'.join(unparsed_entries)}`")
        st.info("Please provide a valid full channel URL (e.g., .../channel/UC... or .../@handle) or just the Channel ID/Handle itself.")

    if not watchlist_ids:
        st.info("No channels added to the watch list yet. Add a channel from the Settings expander.")
    else:
        with st.spinner("Fetching channel details..."):
            channel_details = get_channel_details(list(watchlist_ids))
        
        if not channel_details and watchlist_ids:
            st.warning("Could not fetch details for the provided channel IDs. Please check the IDs/URLs or your API key quota.")
        elif channel_details:
            for channel in sorted(channel_details, key=lambda x: x['channelName']):
                with st.container(border=True):
                    col1, col2 = st.columns([1, 3])
                    with col1:
                        if channel.get('thumbnail'):
                            st.image(channel['thumbnail'], width=88)
                    with col2:
                        st.subheader(channel['channelName'])
                        st.markdown(f"**Subs:** {channel.get('subscriberCount', 0):,} | **Videos:** {channel.get('videoCount', 0):,}")
                        st.markdown(f"[Go to Channel]({channel['url']})", unsafe_allow_html=True)
                    with st.expander("Show Description"):
                        st.write(channel.get('description', 'No description available.'))


# --- PLAYER & DOWNLOAD ---
if st.session_state.video_to_play:
    st.markdown("---")
    st.markdown("### Video Player")
    st.video(f"https://www.youtube.com/watch?v={st.session_state.video_to_play}")
    if st.button("Close Player"):
        st.session_state.video_to_play = None
        st.rerun()

df_csv = df_filtered.copy()
# Clean HTML tags for CSV export
df_csv["title"] = df_csv["title"].str.replace(r'<[^>]+>', '', regex=True)
df_csv["Link"] = df_csv["url"] # Use the raw URL for the link

# Re-apply formatting for export DF
if not df_csv.empty:
    df_csv["Published Time"] = df_csv["publishedAt"].apply(format_published_time)
    df_csv["Status"] = df_csv.apply(get_status_icon, axis=1)

export_cols = ["title", "channel", "Published Time", "views", "likes", "comments", "category", "duration", "liveStatus", "Link", "sentiment", "sourceKeyword"]

# Ensure all columns exist before trying to export
for ccol in export_cols:
    if ccol not in df_csv.columns: 
        if ccol in df.columns:
            id_to_val_map = df.set_index('videoId')[ccol]
            df_csv[ccol] = df_csv['videoId'].map(id_to_val_map)
        else:
            df_csv[ccol] = ""

# Only show download button if there is data
if not df_csv.empty:
    buf = BytesIO()
    df_csv.to_csv(buf, index=False, columns=export_cols, encoding='utf-8')
    buf.seek(0)
    st.download_button("⬇️ Download Data as CSV", data=buf, file_name="cisf_youtube_data.csv", mime="text/csv", use_container_width=True)

# Close the connection for the main page render
conn.close()