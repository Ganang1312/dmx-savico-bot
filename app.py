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

# --- PHẦN CẤU HÌNH: ĐỌC TỪ BIẾN MÔI TRƯỜNG ---
CHANNEL_ACCESS_TOKEN = os.environ.get('CHANNEL_ACCESS_TOKEN')
CHANNEL_SECRET = os.environ.get('CHANNEL_SECRET')
GOOGLE_CREDS_JSON = os.environ.get('GOOGLE_CREDENTIALS_JSON')

if not all([CHANNEL_ACCESS_TOKEN, CHANNEL_SECRET, GOOGLE_CREDS_JSON]):
    raise ValueError("Lỗi: Hãy kiểm tra lại các biến môi trường trên Render.")

# --- CẤU HÌNH GOOGLE SHEETS TỪ BIẾN MÔI TRƯỜNG ---
SCOPE = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
google_creds_dict = json.loads(GOOGLE_CREDS_JSON)
CREDS = Credentials.from_service_account_info(google_creds_dict, scopes=SCOPE)
CLIENT = gspread.authorize(CREDS)

# Tên file và trang tính cần đọc
SHEET_NAME = 'DATA REATIME'
WORKSHEET_NAME = 'chi_tiet_cum'

# --- KHỞI TẠO ỨNG DỤNG ---
app = Flask(__name__)
line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)

# --- ROUTE PING RIÊNG ---
@app.route("/ping", methods=["GET"])
def ping():
    return "pong", 200

# --- HÀM GIỮ CHO APP LUÔN SỐNG ---
def keep_alive():
    def run():
        while True:
            try:
                url = os.environ.get("KEEP_ALIVE_URL")
                if url:
                    requests.get(url)
                    print(f"Ping thành công: {url} lúc {datetime.now().strftime('%H:%M:%S')}")
            except Exception as e:
                print("Ping lỗi:", e)
            time.sleep(300)  # ping mỗi 5 phút
    thread = threading.Thread(target=run)
    thread.daemon = True
    thread.start()

# --- ĐỊNH NGHĨA CÁC HÀM XỬ LÝ ---
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
        except (ValueError, TypeError): return 0.0, "0%"
    else:
        try:
            value = float(clean_str)
            return value, f"{round(value * 100)}%"
        except (ValueError, TypeError): return 0.0, "0%"

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
            except (ValueError, TypeError, IndexError): continue
    results.sort(key=lambda x: x['percent_val'], reverse=True)
    return results

def format_currency(value_str, remove_decimal=False):
    if not value_str or str(value_str).strip() == '-': return "-"
    try:
        value = parse_float_from_string(value_str)
        if remove_decimal:
            if value >= 1000: return f"{math.floor(value / 1000)} Tỷ"
            return f"{math.floor(value)} Tr"
        else:
            if value >= 1000: return f"{round(value / 1000, 2)} Tỷ"
            return f"{round(value, 2)} Tr"
    except (ValueError, TypeError): return "-"

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
                except (ValueError, TypeError): continue
        channel_stores.sort(key=lambda x: x['revenue'], reverse=True)
        rank = -1
        for i, store in enumerate(channel_stores):
            if store['full_row'] == current_row:
                rank = i + 1
                break
        if rank != -1: return f"{rank}/{len(channel_stores)}"
        return "-/-"
    except (IndexError, ValueError, TypeError): return "-/-"

# --- create_flex_message, create_summary_text_message, create_leaderboard_flex_message ---
# 👉 Giữ nguyên code gốc của bạn (không thay đổi logic)

# --- ĐIỂM TIẾP NHẬN WEBHOOK TỪ LINE ---
@app.route("/callback", methods=['POST'])
def callback():
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return 'OK'

# --- HÀM XỬ LÝ TIN NHẮN CHÍNH ---
@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    user_message = event.message.text.strip()
    reply_messages = []

    # --- Nếu gõ 'id' trong group LINE -> trả groupId ---
    if user_message.lower() == "id" and isinstance(event.source, SourceGroup):
        group_id = event.source.group_id
        reply_messages.append(TextSendMessage(text=f"🆔 Group ID: {group_id}"))
        line_bot_api.reply_message(event.reply_token, reply_messages)
        return

    try:
        sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_NAME)
        all_data = sheet.get_all_values()
        header_row = all_data[0]
        cluster_names = {row[0].strip().upper() for row in all_data[1:] if len(row) > 0 and row[0]}
        user_msg_upper = user_message.upper()

        # --- Nếu nhập ST <mã siêu thị> ---
        if user_msg_upper.startswith("ST "):
            supermarket_code = user_message.split(" ", 1)[1].strip()
            found_row = None
            for row in all_data[1:]:
                if row and len(row) > 2 and row[2]:
                    if row[2].startswith(f"ST {supermarket_code}"):
                        found_row = row
                        break
            if found_row:
                ranking = calculate_ranking(all_data, found_row)
                competition_results = parse_competition_data(header_row, found_row)

                flex_message = create_flex_message(found_row, competition_results, ranking)
                reply_messages.append(FlexSendMessage(alt_text='Báo cáo Realtime', contents=flex_message['contents']))

                summary_message = create_summary_text_message(found_row, competition_results)
                if summary_message:
                    reply_messages.append(summary_message)

                cluster_name = (found_row[0] or "").strip()
                if cluster_name:
                    list_of_flex_messages = create_leaderboard_flex_message(all_data, cluster_name=cluster_name)
                    for flex_data in list_of_flex_messages:
                        reply_messages.append(FlexSendMessage(alt_text=flex_data['altText'], contents=flex_data['contents']))
            else:
                reply_messages.append(TextSendMessage(text=f"❌ Không tìm thấy dữ liệu cho siêu thị {supermarket_code}"))

        # --- Nếu nhập BXH ---
        elif user_msg_upper == "BXH":
            list_of_flex_messages = create_leaderboard_flex_message(all_data)
            for flex_data in list_of_flex_messages:
                reply_messages.append(FlexSendMessage(alt_text=flex_data['altText'], contents=flex_data['contents']))

        # --- Nếu nhập tên cụm ---
        elif user_msg_upper in cluster_names:
            list_of_flex_messages = create_leaderboard_flex_message(all_data, cluster_name=user_msg_upper)
            for flex_data in list_of_flex_messages:
                reply_messages.append(FlexSendMessage(alt_text=flex_data['altText'], contents=flex_data['contents']))

        # --- Nếu nhập mã siêu thị (không có ST) ---
        else:
            found_row = None
            for row in all_data[1:]:
                if row and len(row) > 2 and row[2]:
                    supermarket_code_parts = row[2].strip().split(' ')
                    if supermarket_code_parts and supermarket_code_parts[0] == user_message:
                        found_row = row
                        break
            if found_row:
                ranking = calculate_ranking(all_data, found_row)
                competition_results = parse_competition_data(header_row, found_row)
                flex_message = create_flex_message(found_row, competition_results, ranking)
                reply_messages.append(FlexSendMessage(alt_text='Báo cáo Realtime', contents=flex_message['contents']))
                summary_message = create_summary_text_message(found_row, competition_results)
                if summary_message:
                    reply_messages.append(summary_message)
            else:
                reply_messages.append(TextSendMessage(text=f"Không tìm thấy dữ liệu cho: {user_message}"))

        if reply_messages:
            line_bot_api.reply_message(event.reply_token, reply_messages)

    except Exception as e:
        print(f"!!! GẶP LỖI NGHIÊM TRỌNG: {repr(e)}")
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text='Đã có lỗi xảy ra khi truy vấn dữ liệu.'))

# --- CHẠY ỨNG DỤNG ---
if __name__ == "__main__":
    keep_alive()  # bật keep-alive
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
