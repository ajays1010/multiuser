import os

# Patch httpx to support 'proxy' kwarg by remapping to 'proxies' for older httpx versions
try:
    import httpx as _httpx
    _OrigClient = _httpx.Client
    class _PatchedClient(_OrigClient):
        def __init__(self, *args, **kwargs):
            if 'proxy' in kwargs:
                proxy_val = kwargs.pop('proxy')
                if proxy_val is not None and 'proxies' not in kwargs:
                    kwargs['proxies'] = proxy_val
            super().__init__(*args, **kwargs)
    _httpx.Client = _PatchedClient

    _OrigAsyncClient = _httpx.AsyncClient
    class _PatchedAsyncClient(_OrigAsyncClient):
        def __init__(self, *args, **kwargs):
            if 'proxy' in kwargs:
                proxy_val = kwargs.pop('proxy')
                if proxy_val is not None and 'proxies' not in kwargs:
                    kwargs['proxies'] = proxy_val
            super().__init__(*args, **kwargs)
    _httpx.AsyncClient = _PatchedAsyncClient
    if os.environ.get("YAHOO_VERBOSE", "0") == "1":
        print("Applied httpx proxy compatibility patch (database.py).")
except Exception:
    pass

from supabase import create_client, Client
from gotrue.errors import AuthApiError
import firebase_admin
from firebase_admin import credentials, auth
from datetime import datetime, timezone, timedelta

# --- Firebase Admin SDK Initialization ---
firebase_app = None

def initialize_firebase():
    """Initializes the Firebase Admin SDK.
    Supports either a file path via GOOGLE_APPLICATION_CREDENTIALS or raw JSON
    in FIREBASE_SERVICE_ACCOUNT_JSON (written to /tmp/firebase_sa.json)."""
    global firebase_app
    if firebase_app:
        return

    key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    # If the path is missing or file doesn't exist, try JSON env var -> write to /tmp
    if not key_path or not os.path.exists(key_path):
        json_blob = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON")
        if json_blob:
            try:
                tmp_path = "/tmp/firebase_sa.json"
                with open(tmp_path, "w", encoding="utf-8") as f:
                    f.write(json_blob)
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = tmp_path
                key_path = tmp_path
            except Exception as e:
                print(f"Failed to write FIREBASE_SERVICE_ACCOUNT_JSON to /tmp: {e}")

    # Fallback to local service account file in repo if still missing
    if (not key_path or not os.path.exists(key_path)) and os.path.exists(
        "bsemonitoring-64a8e-firebase-adminsdk-fbsvc-cb5ca4b412.json"
    ):
        key_path = "bsemonitoring-64a8e-firebase-adminsdk-fbsvc-cb5ca4b412.json"
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

    if not key_path or not os.path.exists(key_path):
        print("CRITICAL ERROR: Firebase service account key not found.")
        return

    try:
        cred = credentials.Certificate(key_path)
        firebase_app = firebase_admin.initialize_app(cred)
        print("Firebase Admin SDK initialized successfully.")
    except Exception as e:
        print(f"CRITICAL ERROR: Failed to initialize Firebase Admin SDK: {e}")

# --- Supabase Client Initialization ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

# Workaround for environments where proxy env vars cause supabase/httpx init issues
# e.g., "Client.__init__() got an unexpected keyword argument 'proxy'"
_PROXY_ENV_VARS = [
    "HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"
]

def _suppress_proxy_env_for_supabase():
    changed = []
    for k in _PROXY_ENV_VARS:
        if os.environ.pop(k, None) is not None:
            changed.append(k)
    if changed:
        print(f"Notice: Temporarily ignoring proxy env vars for Supabase client: {', '.join(changed)}")

# --- Telegram Bot Configuration ---
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

# Yahoo Finance session and cache
_YAHOO_SESSION = None
_YAHOO_CACHE_SERIES = {}
_YAHOO_CACHE_TTL = int(os.environ.get("YAHOO_CACHE_TTL", "60"))

def get_yahoo_session():
    global _YAHOO_SESSION
    if _YAHOO_SESSION is None:
        import requests
        s = requests.Session()
        s.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36'})
        _YAHOO_SESSION = s
    return _YAHOO_SESSION

def yahoo_chart_series_cached(symbol: str, range_str: str, interval: str):
    # Returns pandas Series of closes indexed by datetime, or None
    import time
    import pandas as pd
    session = get_yahoo_session()
    key = (symbol, range_str, interval)
    # Check cache
    cached = _YAHOO_CACHE_SERIES.get(key)
    now = time.time()
    if cached is not None:
        ts, series = cached
        if now - ts < _YAHOO_CACHE_TTL:
            return series
    # Fetch
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range={range_str}&interval={interval}"
        r = session.get(url, timeout=10)
        if r.status_code != 200:
            if os.environ.get("YAHOO_VERBOSE", "0") == "1":
                print(f"Chart API HTTP {r.status_code} for {symbol} {range_str}/{interval}: {r.text[:120]}")
            return None
        data = r.json()
        result = (data or {}).get('chart', {}).get('result')
        if not result:
            return None
        result = result[0]
        closes = result.get('indicators', {}).get('quote', [{}])[0].get('close') or []
        timestamps = result.get('timestamp') or []
        if not closes or not timestamps:
            return None
        s = pd.Series(closes, index=pd.to_datetime(timestamps, unit='s')).dropna()
        _YAHOO_CACHE_SERIES[key] = (now, s)
        return s
    except Exception as e:
        if os.environ.get("YAHOO_VERBOSE", "0") == "1":
            print(f"Chart API error for {symbol} {range_str}/{interval}: {e}")
        return None

supabase_anon: Client = None
supabase_service: Client = None

def get_supabase_client(service_role=False):
    """Initializes and returns the appropriate Supabase client.
    Returns None if configuration is missing or initialization fails.
    """
    global supabase_anon, supabase_service
    if service_role:
        if supabase_service is None:
            if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
                print("CRITICAL: Supabase Service Key not set.")
                return None
            try:
                _suppress_proxy_env_for_supabase()
                supabase_service = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
            except Exception as e:
                print(f"CRITICAL: Failed to initialize Supabase service client: {e}")
                supabase_service = None
                return None
        return supabase_service
    else:
        if supabase_anon is None:
            if not SUPABASE_URL or not SUPABASE_KEY:
                print("CRITICAL: Supabase Anon Key not set.")
                return None
            try:
                _suppress_proxy_env_for_supabase()
                supabase_anon = create_client(SUPABASE_URL, SUPABASE_KEY)
            except Exception as e:
                print(f"CRITICAL: Failed to initialize Supabase anon client: {e}")
                supabase_anon = None
                return None
        return supabase_anon

# --- Unified User Authentication Logic ---
def find_or_create_supabase_user(decoded_token):
    """
    Finds a user in Supabase by their Firebase/Google UID or email.
    If not found, creates a new user. Returns a new Supabase session.
    """
    # Ensure Firebase Admin SDK is initialized
    initialize_firebase()

    sb_admin = get_supabase_client(service_role=True)
    if not sb_admin:
        return {"session": None, "error": "Admin client not configured."}

    provider_uid = decoded_token['uid']
    
    # Prefer values present in the verified token
    email = decoded_token.get('email')
    phone_number = decoded_token.get('phone_number')

    try:
        # Only call Admin API if we still miss fields
        if not email or not phone_number:
            firebase_user_record = auth.get_user(provider_uid)
            email = email or firebase_user_record.email
            phone_number = phone_number or firebase_user_record.phone_number

        if not email and firebase_user_record.provider_data:
            for provider_info in firebase_user_record.provider_data:
                if provider_info.email:
                    email = provider_info.email
                    break
    except Exception:
        # Ignore Admin lookup failures; we keep whatever we have from the token
        pass

    provider = decoded_token['firebase']['sign_in_provider']
    uid_column = 'google_uid' if provider == 'google.com' else 'firebase_uid'

    # 1. Try to find an existing user
    profile_response = sb_admin.table('profiles').select('id, email').eq(uid_column, provider_uid).execute()
    profile = profile_response.data[0] if profile_response.data else None
    
    if not profile and email:
        profile_response = sb_admin.table('profiles').select('id, email').eq('email', email).execute()
        profile = profile_response.data[0] if profile_response.data else None
        if profile:
            sb_admin.table('profiles').update({uid_column: provider_uid}).eq('id', profile['id']).execute()

    # If we found an existing profile, return identifiers and allow app session login
    if profile:
        # If we have a better email now, update profiles and auth.users when placeholder is present
        if email and (not profile.get('email') or profile.get('email', '').endswith('@yourapp.com')):
            try:
                sb_admin.table('profiles').update({'email': email}).eq('id', profile['id']).execute()
                try:
                    sb_admin.auth.admin.update_user(profile['id'], {'email': email})
                except Exception:
                    # Non-fatal if auth update fails
                    pass
                profile['email'] = email
            except Exception:
                pass
        return {
            "session": None,
            "email": profile['email'],
            "user_id": profile['id'],
            "phone": phone_number,
            "error": None,
        }

    # 3. If no user is found, create a new one
    try:
        user_attrs = {}
        if email:
            user_attrs['email'] = email
        elif phone_number:
            user_attrs['phone'] = phone_number
            user_attrs['email'] = f"{phone_number}@yourapp.com"
        else:
            user_attrs['email'] = f"{provider_uid}@yourapp.com"

        new_user_response = sb_admin.auth.admin.create_user(user_attrs)
        new_user = new_user_response.user
        
        sb_admin.table('profiles').update({uid_column: provider_uid}).eq('id', new_user.id).execute()
        
        # Skip generating Supabase session links; authenticate app-side via Flask session
        return {
            "session": None,
            "email": new_user.email,
            "user_id": new_user.id,
            "phone": phone_number,
            "error": None,
        }

    except Exception as e:
        return {"session": None, "email": email, "user_id": None, "phone": phone_number, "error": str(e)}


# --- User-Specific Data Functions (Remain the same) ---
def get_user_scrips(user_client, user_id: str):
    return (
        user_client
        .table('monitored_scrips')
        .select('bse_code, company_name')
        .eq('user_id', user_id)
        .execute()
        .data or []
    )

def get_user_recipients(user_client, user_id: str):
    return (
        user_client
        .table('telegram_recipients')
        .select('chat_id')
        .eq('user_id', user_id)
        .execute()
        .data or []
    )

def add_user_scrip(user_client, user_id: str, bse_code: str, company_name: str):
    user_client.table('monitored_scrips').insert({'user_id': user_id, 'bse_code': bse_code, 'company_name': company_name}).execute()

def delete_user_scrip(user_client, user_id: str, bse_code: str):
    user_client.table('monitored_scrips').delete().eq('user_id', user_id).eq('bse_code', bse_code).execute()

def add_user_recipient(user_client, user_id: str, chat_id: str):
    """
    Add or assign a Telegram chat_id to the current user.
    Handles the case where chat_id is globally unique and may already exist for another user.
    Behavior:
    - If chat_id already exists for this user: no-op
    - If chat_id exists for a different user: reassign to this user
    - If chat_id doesn't exist: insert
    """
    # Normalize chat_id to string for consistency
    chat_id_str = str(chat_id).strip()

    # Look for existing record by chat_id (unique)
    existing = user_client.table('telegram_recipients').select('user_id').eq('chat_id', chat_id_str).limit(1).execute()
    existing_row = (existing.data or [None])[0]

    if existing_row:
        if str(existing_row.get('user_id')) == str(user_id):
            # Already assigned to this user; nothing to do
            return
        # Reassign this chat_id to the current user
        user_client.table('telegram_recipients').update({'user_id': user_id}).eq('chat_id', chat_id_str).execute()
        return

    # Not found; insert new
    user_client.table('telegram_recipients').insert({'user_id': user_id, 'chat_id': chat_id_str}).execute()

def delete_user_recipient(user_client, user_id: str, chat_id: str):
    user_client.table('telegram_recipients').delete().eq('user_id', user_id).eq('chat_id', chat_id).execute()


# --- Admin helpers ---
def admin_get_all_users():
    sb_admin = get_supabase_client(service_role=True)
    resp = sb_admin.table('profiles').select('id, email').order('email').execute()
    return resp.data or []

def admin_get_user_details(user_id: str):
    sb_admin = get_supabase_client(service_role=True)
    profile = sb_admin.table('profiles').select('id, email').eq('id', user_id).single().execute().data
    scrips = sb_admin.table('monitored_scrips').select('bse_code, company_name').eq('user_id', user_id).execute().data or []
    recipients = sb_admin.table('telegram_recipients').select('chat_id').eq('user_id', user_id).execute().data or []
    return {
        'id': profile['id'],
        'email': profile.get('email', ''),
        'scrips': scrips,
        'recipients': recipients,
    }

def admin_add_scrip_for_user(user_id: str, bse_code: str, company_name: str):
    sb_admin = get_supabase_client(service_role=True)
    sb_admin.table('monitored_scrips').insert({'user_id': user_id, 'bse_code': bse_code, 'company_name': company_name}).execute()

def admin_delete_scrip_for_user(user_id: str, bse_code: str):
    sb_admin = get_supabase_client(service_role=True)
    sb_admin.table('monitored_scrips').delete().eq('user_id', user_id).eq('bse_code', bse_code).execute()

def admin_add_recipient_for_user(user_id: str, chat_id: str):
    sb_admin = get_supabase_client(service_role=True)
    chat_id_str = str(chat_id).strip()
    existing = sb_admin.table('telegram_recipients').select('user_id').eq('chat_id', chat_id_str).limit(1).execute()
    existing_row = (existing.data or [None])[0]
    if existing_row:
        if str(existing_row.get('user_id')) == str(user_id):
            return
        sb_admin.table('telegram_recipients').update({'user_id': user_id}).eq('chat_id', chat_id_str).execute()
        return
    sb_admin.table('telegram_recipients').insert({'user_id': user_id, 'chat_id': chat_id_str}).execute()

def admin_delete_recipient_for_user(user_id: str, chat_id: str):
    sb_admin = get_supabase_client(service_role=True)
    sb_admin.table('telegram_recipients').delete().eq('user_id', user_id).eq('chat_id', chat_id).execute()

# --- Telegram Helper Functions ---
def send_telegram_message(chat_id: str, message: str):
    """
    Sends a message to a Telegram chat using the bot API.
    Returns True if successful, False otherwise.
    """
    import requests
    import json

    if not TELEGRAM_BOT_TOKEN:
        print("❌ Telegram bot token missing. Set TELEGRAM_BOT_TOKEN in your .env and restart the app.")
        return False
    
    try:
        payload = {
            'chat_id': chat_id,
            'text': message,
            'parse_mode': 'Markdown'
        }
        
        response = requests.post(
            f"{TELEGRAM_API_URL}/sendMessage",
            json=payload,
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get('ok'):
                print(f"✅ Message sent successfully to Telegram {chat_id}")
                return True
            else:
                print(f"❌ Telegram API error: {result.get('description', 'Unknown error')}")
                return False
        else:
            print(f"❌ HTTP error {response.status_code}: {response.text}")
            if response.status_code == 404:
                print("Hint: 404 from Telegram often means an invalid bot token or malformed URL. Double-check TELEGRAM_BOT_TOKEN and ensure you started a chat with the bot.")
            return False
            
    except Exception as e:
        print(f"❌ Error sending Telegram message: {e}")
        return False

# --- Script Message Functions ---

# --- BSE Announcements Integration ---
BSE_API_URL = "https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w"
PDF_BASE_URL = "https://www.bseindia.com/xml-data/corpfiling/AttachLive/"
BSE_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
    'Referer': 'https://www.bseindia.com/'
}

IST_OFFSET = timedelta(hours=5, minutes=30)
IST_TZ = timezone(IST_OFFSET, name="IST")

def ist_now():
    # Return timezone-aware IST datetime
    return datetime.now(IST_TZ)

def db_seen_announcement_exists(user_client, user_id: str, news_id: str) -> bool:
    try:
        resp = (
            user_client
            .table('seen_announcements')
            .select('news_id', count='exact')
            .eq('news_id', news_id)
            .eq('user_id', user_id)
            .execute()
        )
        return (getattr(resp, 'count', 0) or 0) > 0
    except Exception as e:
        # If the table doesn't exist yet or any error occurs, do NOT block sending.
        # We return False so announcements are treated as new.
        try:
            print(f"seen_announcements lookup failed, treating as new: {e}")
        except Exception:
            pass
        return False

def db_save_seen_announcement(user_client, user_id: str, news_id: str, scrip_code: str, headline: str, pdf_name: str, ann_dt_iso: str, caption: str):
    try:
        user_client.table('seen_announcements').insert({
            'user_id': user_id,
            'news_id': news_id,
            'scrip_code': scrip_code,
            'headline': headline,
            'pdf_name': pdf_name,
            'ann_date': ann_dt_iso,
            'caption': caption,
        }).execute()
    except Exception:
        pass

def fetch_bse_announcements_for_scrip(scrip_code: str, since_dt) -> list[dict]:
    import requests
    results = []
    try:
        from_date_str = (ist_now() - timedelta(days=7)).strftime('%Y%m%d')
        to_date_str = ist_now().strftime('%Y%m%d')
        params = {
            'strCat': '-1', 'strPrevDate': from_date_str, 'strToDate': to_date_str,
            'strScrip': scrip_code, 'strSearch': 'P', 'strType': 'C'
        }
        r = requests.get(BSE_API_URL, headers=BSE_HEADERS, params=params, timeout=30)
        data = r.json() if r.status_code == 200 else {}
        table = data.get('Table') or []
        for ann in table:
            news_id = ann.get('NEWSID')
            pdf_name = ann.get('ATTACHMENTNAME')
            if not news_id or not pdf_name:
                continue
            ann_date_str = ann.get('NEWS_DT') or ann.get('DissemDT')
            if not ann_date_str:
                continue
            # Parse announcement date (several formats observed)
            dt_parsed = None
            for fmt in ('%d %b %Y %I:%M:%S %p', '%Y-%m-%d %I:%M %p', '%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S'):
                try:
                    dt_parsed = datetime.strptime(ann_date_str, fmt)
                    break
                except ValueError:
                    continue
            if not dt_parsed:
                # Try to strip potential timezone suffixes or extra parts
                try:
                    dt_parsed = datetime.fromisoformat(ann_date_str.split('.')[0])
                except Exception:
                    continue
            # Localize to IST if naive
            if dt_parsed.tzinfo is None:
                ann_dt = dt_parsed.replace(tzinfo=IST_TZ)
            else:
                ann_dt = dt_parsed.astimezone(IST_TZ)
            if ann_dt < since_dt:
                continue
            headline = ann.get('NEWSSUB') or ann.get('HEADLINE', 'N/A')
            results.append({
                'news_id': news_id,
                'scrip_code': scrip_code,
                'headline': headline,
                'pdf_name': pdf_name,
                'ann_dt': ann_dt,
            })
    except Exception:
        pass
    return results

def send_bse_announcements_consolidated(user_client, user_id: str, monitored_scrips, telegram_recipients, hours_back: int = 24) -> int:
    # Build a lookup from bse_code to company_name for friendly messages
    code_to_name = {}
    try:
        for s in monitored_scrips:
            code_to_name[str(s.get('bse_code'))] = s.get('company_name') or str(s.get('bse_code'))
    except Exception:
        pass
    import requests
    messages_sent = 0
    since_dt = ist_now() - timedelta(hours=hours_back)

    # Fetch announcements for all scrips
    all_new = []
    for scrip in monitored_scrips:
        scrip_code = scrip['bse_code']
        ann = fetch_bse_announcements_for_scrip(scrip_code, since_dt)
        for item in ann:
            if not db_seen_announcement_exists(user_client, user_id, item['news_id']):
                all_new.append(item)

    if not all_new:
        # Optionally send a small notice
        return 0

    # Build a consolidated message + attach PDFs individually
    header = [
        "📰 BSE Announcements",
        f"🕐 {ist_now().strftime('%Y-%m-%d %H:%M:%S')} IST",
        "",
    ]

    # Group items per scrip for nicer formatting
    from collections import defaultdict
    by_scrip = defaultdict(list)
    for item in sorted(all_new, key=lambda x: x['ann_dt'], reverse=True):
        by_scrip[item['scrip_code']].append(item)

    # Create text summary
    lines = header[:]
    for scrip_code, items in by_scrip.items():
        company_name = code_to_name.get(str(scrip_code)) or str(scrip_code)
        lines.append(f"• {company_name}")
        for it in items[:5]:
            lines.append(f"  - {it['ann_dt'].strftime('%d-%m %H:%M')} — {it['headline']}")
        lines.append("")
    summary_text = "\n".join(lines).strip()

    # Send summary first
    for rec in telegram_recipients:
        chat_id = rec['chat_id']
        from requests import post
        post(f"{TELEGRAM_API_URL}/sendMessage", json={'chat_id': chat_id, 'text': summary_text, 'parse_mode': 'HTML'}, timeout=10)
        messages_sent += 1

    # Send documents (PDFs)
    for item in all_new:
        friendly_name = code_to_name.get(str(item['scrip_code'])) or str(item['scrip_code'])
        caption = f"Company: {friendly_name}\nAnnouncement: {item['headline']}\nDate: {item['ann_dt'].strftime('%d-%m-%Y %H:%M')} IST"
        pdf_url = f"{PDF_BASE_URL}{item['pdf_name']}"
        try:
            resp = requests.get(pdf_url, headers=BSE_HEADERS, timeout=30)
            if resp.status_code == 200 and resp.content:
                for rec in telegram_recipients:
                    files = {"document": (item['pdf_name'], resp.content, "application/pdf")}
                    data = {"chat_id": rec['chat_id'], "caption": caption, "parse_mode": "HTML"}
                    requests.post(f"{TELEGRAM_API_URL}/sendDocument", data=data, files=files, timeout=45)
                # Record as seen for this user
                db_save_seen_announcement(user_client, user_id, item['news_id'], item['scrip_code'], item['headline'], item['pdf_name'], item['ann_dt'].isoformat(), caption)
            else:
                # Could not fetch PDF, still mark as seen to avoid repeated attempts
                db_save_seen_announcement(user_client, user_id, item['news_id'], item['scrip_code'], item['headline'], item['pdf_name'], item['ann_dt'].isoformat(), caption)
        except Exception:
            # On errors, we still mark as seen to limit retries (could adjust behavior)
            db_save_seen_announcement(user_client, user_id, item['news_id'], item['scrip_code'], item['headline'], item['pdf_name'], item['ann_dt'].isoformat(), caption)

    return messages_sent
def send_script_messages_to_telegram(user_client, user_id: str, monitored_scrips, telegram_recipients):
    """
    Sends a single consolidated Telegram message with current price and moving averages
    for all monitored scrips. Uses batch requests to Yahoo Finance to reduce rate limits.
    Returns the number of messages sent (one per recipient).
    """
    import yfinance as yf
    import pandas as pd
    from datetime import datetime

    def safe_fmt(val):
        try:
            return f"₹{float(val):.2f}"
        except Exception:
            return "N/A"

    # Helper to safely pull a series from a yfinance.download DataFrame
    def get_series(df, symbol, field='Close'):
        if df is None or df.empty:
            return None
        try:
            if isinstance(df.columns, pd.MultiIndex):
                # Try group_by='ticker' layout first
                if (symbol, field) in df.columns:
                    return df[(symbol, field)].dropna()
                # Some versions may return the inverse
                if (field, symbol) in df.columns:
                    return df[(field, symbol)].dropna()
                return None
            else:
                if field in df.columns:
                    return df[field].dropna()
                return None
        except Exception:
            return None

    try:
        # Load the stock tickers CSV to get Yahoo Finance symbols
        company_df = pd.read_csv('indian_stock_tickers.csv')

        # Map BSE codes -> Yahoo symbols and keep order/context
        symbol_map = {}
        ordered_symbols = []
        for scrip in monitored_scrips:
            bse_code = scrip['bse_code']
            company_name = scrip['company_name']

            # Find symbol for BSE code
            try:
                bse_code_int = int(bse_code)
                ticker_match = company_df[company_df['BSE Code'] == bse_code_int]
            except (ValueError, TypeError):
                ticker_match = company_df[company_df['BSE Code'].astype(str) == str(bse_code)]

            if ticker_match.empty:
                print(f"Warning: No Yahoo Finance symbol found for BSE code {bse_code}")
                continue

            symbol = str(ticker_match.iloc[0]['Yahoo Symbol']).strip()
            if not symbol:
                print(f"Warning: Empty Yahoo Finance symbol for BSE code {bse_code}")
                continue

            if os.environ.get("YAHOO_VERBOSE", "0") == "1":
                print(f"Using Yahoo symbol: {symbol} for {company_name} ({bse_code})")
            symbol_map[symbol] = {'bse_code': bse_code, 'company_name': company_name}
            ordered_symbols.append(symbol)

        if not ordered_symbols:
            print("No valid Yahoo symbols found for monitored scrips.")
            return 0

        # Prepare session
        session = get_yahoo_session()

        # Chunk symbols in groups of 10
        def chunks(lst, size):
            for i in range(0, len(lst), size):
                yield lst[i:i+size]

        # Batch current prices via Yahoo Chart API (prefer chart API to avoid Quote API 401)
        prices = {}
        for batch in chunks(ordered_symbols, 10):
            for sym in batch:
                s_intraday = yahoo_chart_series_cached(sym, '1d', '1m')
                if s_intraday is not None and not s_intraday.empty:
                    prices[sym] = s_intraday.iloc[-1]
                else:
                    s_daily = yahoo_chart_series_cached(sym, '5d', '1d')
                    if s_daily is not None and not s_daily.empty:
                        prices[sym] = s_daily.iloc[-1]

        # Build consolidated message
        lines = []
        lines.append("📊 Market Update")
        lines.append(f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")
        failed_symbols = []

        for symbol in ordered_symbols:
            meta = symbol_map[symbol]
            bse_code = meta['bse_code']
            company_name = meta['company_name']

            # Current price from chart cache
            current_price = prices.get(symbol, 'N/A')

            # Moving averages from daily history via chart API (cached)
            ma_50 = 'N/A'
            ma_200 = 'N/A'
            s_hist = yahoo_chart_series_cached(symbol, '1y', '1d')
            if s_hist is not None and not s_hist.empty:
                closes = s_hist.dropna()
                if len(closes) >= 50:
                    ma_50 = closes.tail(50).mean()
                if len(closes) >= 200:
                    ma_200 = closes.tail(200).mean()

            if current_price == 'N/A' and ma_50 == 'N/A' and ma_200 == 'N/A':
                failed_symbols.append(f"{company_name} ({symbol})")

            # Append section for this symbol
            lines.append(f"• {company_name} ({bse_code})")
            lines.append(f"  - Price: {safe_fmt(current_price)}")
            lines.append(f"  - MA50: {safe_fmt(ma_50)} | MA200: {safe_fmt(ma_200)}")
            lines.append("")

        if failed_symbols:
            lines.append("⚠️ Could not fetch data for: " + ", ".join(failed_symbols))

        consolidated_message = "\n".join(lines).strip()

        # Send one message per recipient
        messages_sent = 0
        for recipient in telegram_recipients:
            chat_id = recipient['chat_id']
            try:
                if send_telegram_message(chat_id, consolidated_message):
                    messages_sent += 1
                else:
                    print(f"❌ Failed to send consolidated message to Telegram {chat_id}")
            except Exception as e:
                print(f"❌ Error sending consolidated message to Telegram {chat_id}: {e}")

        return messages_sent

    except Exception as e:
        print(f"Error in send_script_messages_to_telegram: {e}")
        raise e
