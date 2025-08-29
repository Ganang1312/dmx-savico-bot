import os
import json
import threading, time, requests
from flask import Flask, request, abort
from linebot import (
    LineBotApi, WebhookHandler
)
from linebot.exceptions import InvalidSignatureError
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage, FlexSendMessage,
    SourceGroup
)
import gspread
from google.oauth2.service_account import Credentials
import collections
from datetime import datetime
import pytz
import math

# --- CẤU HÌNH LINE & GOOGLE SHEETS ---
CHANNEL_ACCESS_TOKEN = os.environ.get('CHANNEL_ACCESS_TOKEN')
CHANNEL_SECRET = os.environ.get('CHANNEL_SECRET')
GOOGLE_CREDS_JSON = os.environ.get('GOOGLE_CREDENTIALS_JSON')

if not all([CHANNEL_ACCESS_TOKEN, CHANNEL_SECRET, GOOGLE_CREDS_JSON]):
    raise ValueError("Thiếu biến môi trường: CHANNEL_ACCESS_TOKEN, CHANNEL_SECRET, GOOGLE_CREDENTIALS_JSON")

SCOPE = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
google_creds_dict = json.loads(GOOGLE_CREDS_JSON)
CREDS = Credentials.from_service_account_info(google_creds_dict, scopes=SCOPE)
CLIENT = gspread.authorize(CREDS)

SHEET_NAME = 'DATA REATIME'
WORKSHEET_NAME = 'chi_tiet_cum'

# --- APP ---
app = Flask(__name__)
line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)

# --- KEEP ALIVE ---
@app.route("/ping", methods=["GET"])
def ping():
    return "pong", 200

def keep_alive():
    def run():
        while True:
            try:
                url = os.environ.get("KEEP_ALIVE_URL")
                if url:
                    requests.get(url)
                    print(f"Ping {url} at {datetime.now().strftime('%H:%M:%S')}")
            except Exception as e:
                print("Ping lỗi:", e)
            time.sleep(300)
    threading.Thread(target=run, daemon=True).start()

# --- HÀM HỖ TRỢ ---
def parse_float_from_string(s):
    if s is None: return 0.0
    if not isinstance(s, str): s = str(s)
    clean_s = s.strip()
    if not clean_s or clean_s == '-': return 0.0
    try:
        return float(clean_s.replace(',', '.'))
    except ValueError:
        return 0.0

def handle_percentage_string(percent_str):
    if not percent_str: return 0.0, "0%"
    clean_str = str(percent_str).strip()
    if '%' in clean_str:
        try:
            value = float(clean_str.replace('%', '')) / 100
            return value, f"{round(value * 100)}%"
        except: return 0.0, "0%"
    else:
        try:
            value = float(clean_str)
            return value, f"{round(value * 100)}%"
        except: return 0.0, "0%"

def parse_competition_data(header_row, data_row):
    start_column_index = 6
    category_indices = collections.defaultdict(list)
    for i, header in enumerate(header_row[start_column_index:], start=start_column_index):
        if header: category_indices[header].append(i)
    results = []
    for category_name, indices in category_indices.items():
        if len(indices) == 3:
            try:
                percent_ht_val = data_row[indices[0]]
                realtime_val_str = data_row[indices[1]] if data_row[indices[1]] and data_row[indices[1]].strip() != '-' else "0"
                target_val_str = data_row[indices[2]] if data_row[indices[2]] and data_row[indices[2]].strip() != '-' else "0"
                percent_float, percent_ht_formatted = handle_percentage_string(percent_ht_val)
                results.append({
                    "name": category_name,
                    "realtime": parse_float_from_string(realtime_val_str),
                    "target": target_val_str,
                    "percent_ht": percent_ht_formatted,
                    "percent_val": percent_float
                })
            except: continue
    results.sort(key=lambda x: x['percent_val'], reverse=True)
    return results

def calculate_ranking(all_data, current_row):
    try:
        current_channel = (current_row[1] or "").strip()
        current_revenue = parse_float_from_string(current_row[4])
        channel_stores = []
        for row in all_data[1:]:
            if len(row) > 4 and (row[1] or "").strip() == current_channel:
                try:
                    revenue = parse_float_from_string(row[4])
                    channel_stores.append({'revenue': revenue, 'full_row': row})
                except: continue
        channel_stores.sort(key=lambda x: x['revenue'], reverse=True)
        for i, store in enumerate(channel_stores):
            if store['full_row'] == current_row:
                return f"{i+1}/{len(channel_stores)}"
        return "-/-"
    except: return "-/-"

def find_supermarket_row(all_data, supermarket_code):
    supermarket_code = supermarket_code.strip().upper()
    for row in all_data[1:]:
        if row and len(row) > 2 and row[2]:
            value = row[2].upper().replace(" ", "")
            if supermarket_code in value:
                return row
    return None

# --- placeholder cho Flex (giữ nguyên logic cũ của bạn) ---
def create_flex_message(row, competition_results, ranking):
    return {"contents": {"type": "bubble", "body": {"type": "box", "layout": "vertical", "contents":[{"type":"text","text":f"Báo cáo {row[2]} - Hạng {ranking}"}]}}}

def create_summary_text_message(row, competition_results):
    return TextSendMessage(text=f"Tóm tắt: {row[2]} doanh thu {row[4]}")

def create_leaderboard_flex_message(all_data, cluster_name=None):
    return [{"altText":"BXH Cụm","contents":{"type":"bubble","body":{"type":"box","layout":"vertical","contents":[{"type":"text","text":"BXH cụm demo"}]}}}]

# --- CALLBACK ---
@app.route("/callback", methods=['POST'])
def callback():
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return 'OK'

# --- HANDLE MESSAGE ---
@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    user_message = event.message.text.strip()
    reply_messages = []

    # Nếu gõ 'id' trong group LINE -> trả groupId
    if user_message.lower() == "id" and isinstance(event.source, SourceGroup):
        reply_messages.append(TextSendMessage(text=f"🆔 Group ID: {event.source.group_id}"))
        line_bot_api.reply_message(event.reply_token, reply_messages)
        return

    try:
        sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_NAME)
        all_data = sheet.get_all_values()
        header_row = all_data[0]
        user_msg_upper = user_message.upper()

        # --- LỆNH ST <mã> ---
        if user_msg_upper.startswith("ST "):
            supermarket_code = user_message.split(" ", 1)[1].strip()
            found_row = find_supermarket_row(all_data, supermarket_code)

            if found_row:
                ranking = calculate_ranking(all_data, found_row)
                competition_results = parse_competition_data(header_row, found_row)

                # Báo cáo realtime
                flex_message = create_flex_message(found_row, competition_results, ranking)
                reply_messages.append(FlexSendMessage(
                    alt_text=f"Báo cáo realtime ST {supermarket_code}",
                    contents=flex_message['contents']
                ))

                # BXH cụm
                cluster_name = (found_row[0] or "").strip()
                if cluster_name:
                    bxh_messages = create_leaderboard_flex_message(all_data, cluster_name=cluster_name)
                    for flex_data in bxh_messages:
                        reply_messages.append(FlexSendMessage(
                            alt_text=flex_data['altText'],
                            contents=flex_data['contents']
                        ))
            else:
                reply_messages.append(TextSendMessage(text=f"❌ Không tìm thấy dữ liệu cho siêu thị {supermarket_code}"))

        # --- fallback: các lệnh khác ---
        else:
            reply_messages.append(TextSendMessage(text="⚠️ Hãy nhập theo cú pháp: ST <mã siêu thị>"))

        if reply_messages:
            line_bot_api.reply_message(event.reply_token, reply_messages)

    except Exception as e:
        print("Lỗi:", repr(e))
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text='Đã có lỗi xảy ra khi truy vấn dữ liệu.'))

# --- MAIN ---
if __name__ == "__main__":
    keep_alive()
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
