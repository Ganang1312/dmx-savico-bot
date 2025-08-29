import os
import json
from flask import Flask, request, abort
from linebot import (
    LineBotApi, WebhookHandler
)
from linebot.exceptions import InvalidSignatureError
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage, FlexSendMessage
)
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import collections
from datetime import datetime
import pytz
import math
import threading
import time
import requests

# --- PHẦN CẤU HÌNH: ĐỌC TỪ BIẾN MÔI TRƯỜNG ---
CHANNEL_ACCESS_TOKEN = os.environ.get('CHANNEL_ACCESS_TOKEN')
CHANNEL_SECRET = os.environ.get('CHANNEL_SECRET')
GOOGLE_CREDS_JSON = os.environ.get('GOOGLE_CREDENTIALS_JSON')
# BIẾN MÔI TRƯỜNG MỚI: ID của quản trị viên bot
ADMIN_USER_ID = os.environ.get('ADMIN_USER_ID')

if not all([CHANNEL_ACCESS_TOKEN, CHANNEL_SECRET, GOOGLE_CREDS_JSON]):
    raise ValueError("Lỗi: Hãy kiểm tra lại các biến môi trường trên Render.")

# --- CẤU HÌNH GOOGLE SHEETS ---
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
google_creds_dict = json.loads(GOOGLE_CREDS_JSON)
CREDS = ServiceAccountCredentials.from_json_keyfile_dict(google_creds_dict, SCOPE)
CLIENT = gspread.authorize(CREDS)

# Tên các trang tính
SHEET_NAME = 'DATA REATIME'
WORKSHEET_NAME = 'chi_tiet_cum'
# TRANG TÍNH MỚI: Chứa danh sách các ID được phép sử dụng bot
WORKSHEET_NAME_USERS = 'allowed_users'

# Cache để lưu danh sách ID được phép
allowed_ids_cache = set()

# --- KHỞI TẠO ỨNG DỤNG ---
app = Flask(__name__)
line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)

# --- CÁC HÀM TIỆN ÍCH ---

def load_allowed_ids():
    """
    Tải danh sách các user/group ID được phép từ trang tính 'allowed_users'.
    Nếu không tìm thấy trang tính, bot sẽ ở chế độ công khai.
    """
    global allowed_ids_cache
    try:
        print("Đang tải danh sách ID được phép...")
        sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_NAME_USERS)
        # Lấy tất cả ID từ cột đầu tiên
        list_of_ids = sheet.col_values(1)
        # Bỏ qua tiêu đề (nếu có) và các ô trống
        allowed_ids_cache = set(filter(None, list_of_ids[1:] if list_of_ids and list_of_ids[0].lower() == 'id' else list_of_ids))
        print(f"Đã tải thành công {len(allowed_ids_cache)} ID vào danh sách cho phép.")
    except gspread.exceptions.WorksheetNotFound:
        print(f"!!! CẢNH BÁO: Không tìm thấy trang tính '{WORKSHEET_NAME_USERS}'.")
        print("Bot sẽ hoạt động ở chế độ công khai cho tất cả người dùng.")
        allowed_ids_cache = set() # Xóa cache để tắt kiểm tra
    except Exception as e:
        print(f"Lỗi nghiêm trọng khi tải danh sách ID: {e}")
        allowed_ids_cache = set()

def keep_alive():
    ping_url = os.environ.get("KEEP_ALIVE_URL")
    if not ping_url:
        app_url = os.environ.get("RENDER_EXTERNAL_URL")
        if app_url:
            ping_url = f"{app_url}/ping"
    if not ping_url:
        print("Bỏ qua chức năng keep-alive.")
        return
    while True:
        try:
            requests.get(ping_url, timeout=10)
        except requests.exceptions.RequestException as e:
            print(f"Lỗi khi ping: {e}")
        time.sleep(600)

# Khởi động các tác vụ nền
if 'RENDER' in os.environ:
    keep_alive_thread = threading.Thread(target=keep_alive, daemon=True)
    keep_alive_thread.start()

# Tải danh sách ID được phép khi khởi động
load_allowed_ids()

# --- CÁC HÀM XỬ LÝ DỮ LIỆU (không thay đổi) ---
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

def create_flex_message(store_data, competition_results, ranking):
    cum = store_data[0] or "-"
    kenh = (store_data[1] or "").strip()
    sieu_thi_full = store_data[2] or "Không có tên"
    ten_sieu_thi_parts = sieu_thi_full.split(' - ')
    short_name_part = ten_sieu_thi_parts[-1] if len(ten_sieu_thi_parts) > 1 else sieu_thi_full
    ten_sieu_thi_rut_gon = f"{kenh} {short_name_part}"
    realtime_tong = format_currency(store_data[4], remove_decimal=True)
    target_tong = format_currency(store_data[3], remove_decimal=True)
    percent_float, percent_ht_tong = handle_percentage_string(store_data[5])
    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    now = datetime.now(tz_vietnam)
    thoi_gian = f"{now.hour}h Ngày {now.day}/{now.month}"
    sold_items = [item for item in competition_results if item['realtime'] > 0]
    unsold_items = [item for item in competition_results if item['realtime'] == 0]
    nh_thi_dua_dat = sum(1 for item in sold_items if item.get("percent_val", 0) >= 1)
    channel_styles = {"ĐML": {"bg": "#1E88E5", "text": "#FFFFFF"}, "ĐMM": {"bg": "#43A047", "text": "#FFFFFF"},"TGD": {"bg": "#FDD835", "text": "#000000"}, "AAR": {"bg": "#212121", "text": "#FFFFFF"}}
    style = channel_styles.get(kenh, {"bg": "#006c83", "text": "#FFFFFF"})
    TEXT_COLOR = "#000000"
    SEPARATOR_COLOR = "#EEEEEE"
    sold_components = []
    for item in sold_items:
        percent_val = item.get("percent_val", 0)
        color = "#00B33C" if percent_val >= 1 else ("#FFC400" if percent_val > 0.7 else "#FF3B30")
        target_display = str(parse_float_from_string(item["target"]))
        component = {"type": "box", "layout": "horizontal", "margin": "md", "paddingTop": "sm", "paddingBottom": "sm", "contents": [{"type": "text", "text": item["name"], "wrap": True, "size": "sm", "color": TEXT_COLOR, "flex": 4, "gravity": "center"},{"type": "separator", "color": SEPARATOR_COLOR},{"type": "text", "text": str(round(item["realtime"], 2)), "size": "sm", "color": TEXT_COLOR, "align": "center", "flex": 2, "gravity": "center"},{"type": "separator", "color": SEPARATOR_COLOR},{"type": "text", "text": target_display, "size": "sm", "color": TEXT_COLOR, "align": "center", "flex": 2, "gravity": "center"},{"type": "separator", "color": SEPARATOR_COLOR},{"type": "box", "layout": "vertical", "flex": 2, "contents": [{"type": "text", "text": item["percent_ht"], "size": "sm", "color": color, "align": "end", "weight": "bold", "gravity": "center"}]}]}
        sold_components.append(component)
        sold_components.append({"type": "separator", "margin": "md", "color": SEPARATOR_COLOR})
    unsold_components = []
    if unsold_items:
        unsold_components.extend([{"type": "separator", "margin": "xl", "color": SEPARATOR_COLOR}, {"type": "text", "text": "NGÀNH HÀNG CHƯA CÓ SỐ:", "color": "#888888", "size": "sm", "align": "center", "margin": "lg", "weight": "bold"}])
        num_columns = 3
        columns = [[] for _ in range(num_columns)]
        for i, item in enumerate(unsold_items):
            columns[i % num_columns].append({"type": "text", "text": f"• {item['name']}", "color": TEXT_COLOR, "size": "xs", "margin": "sm", "wrap": True})
        column_boxes = [{"type": "box", "layout": "vertical", "flex": 1, "contents": col} for col in columns]
        unsold_components.append({"type": "box", "layout": "horizontal", "margin": "md", "spacing": "md", "contents": column_boxes})
    percent_color = "#00B33C" if percent_float >= 1 else ("#FFC400" if percent_float > 0.7 else "#FF3B30")
    flex_json = {"type": "flex", "altText": f"Báo cáo cho {ten_sieu_thi_rut_gon}", "contents": { "type": "bubble", "size": "giga", "header": { "type": "box", "layout": "vertical", "paddingAll": "20px", "backgroundColor": style["bg"], "contents": [ {"type": "text", "text": "Báo cáo Realtime", "color": style["text"], "size": "lg", "align": "center", "weight": "bold"}, {"type": "text", "text": f"🏪 {ten_sieu_thi_rut_gon.upper()}", "color": style["text"], "weight": "bold", "size": "xl", "align": "center", "margin": "md", "wrap": True}, {"type": "box", "layout": "vertical", "margin": "lg", "spacing": "sm", "contents": [ {"type": "text", "text": f"⭐ Cụm: {cum}", "size": "sm", "color": style["text"]}, {"type": "text", "text": f"🕒 Thời gian: {thoi_gian}", "size": "sm", "color": style["text"]}, {"type": "text", "text": f"🏆 NH thi đua đạt: {nh_thi_dua_dat}", "size": "sm", "color": style["text"]} ]} ] }, "body": { "type": "box", "layout": "vertical", "paddingAll": "20px", "backgroundColor": "#FFFFFF", "contents": [ {"type": "box", "layout": "horizontal", "contents": [ {"type": "box", "layout": "vertical", "flex": 1, "spacing": "sm", "contents": [ {"type": "text", "text": "💰 DOANH THU", "color": "#007BFF", "size": "md", "align": "center", "weight":"bold"}, {"type": "text", "text": realtime_tong, "color": "#007BFF", "size": "xxl", "weight": "bold", "align": "center"} ]}, {"type": "box", "layout": "vertical", "flex": 1, "spacing": "sm", "contents": [ {"type": "text", "text": "🎯 TARGET", "color": "#DC3545", "size": "md", "align": "center", "weight":"bold"}, {"type": "text", "text": target_tong, "color": "#DC3545", "size": "xxl", "weight": "bold", "align": "center"} ]} ]}, {"type": "text", "text": "% HOÀN THÀNH", "color": TEXT_COLOR, "size": "md", "align": "center", "margin": "xl"}, {"type": "text", "text": percent_ht_tong, "color": percent_color, "size": "4xl", "weight": "bold", "align": "center"}, {"type": "box", "layout": "vertical", "backgroundColor": "#DDDDDD", "height": "8px", "cornerRadius": "md", "margin": "md", "contents": [ {"type": "box", "layout": "vertical", "backgroundColor": percent_color, "height": "8px", "cornerRadius": "md", "width": f"{min(100, round(percent_float * 100))}%"} ]}, {"type": "box", "layout": "horizontal", "margin": "xl", "contents": [{"type": "text", "text": "XH D.Thu Kênh", "size": "sm", "color": TEXT_COLOR, "align": "center", "flex": 1}]}, {"type": "box", "layout": "horizontal", "contents": [{"type": "text", "text": ranking, "weight": "bold", "size": "lg", "color": TEXT_COLOR, "align": "center", "flex": 1}]}, {"type": "separator", "margin": "xl", "color": SEPARATOR_COLOR}, {"type": "box", "layout": "horizontal", "margin": "md", "contents": [{"type": "text", "text": "Ngành Hàng", "color": "#555555", "size": "sm", "flex": 4, "weight": "bold", "align": "center"}, {"type": "text", "text": "Realtime", "color": "#555555", "size": "sm", "flex": 2, "align": "center", "weight": "bold"}, {"type": "text", "text": "Target", "color": "#555555", "size": "sm", "flex": 2, "align": "center", "weight": "bold"}, {"type": "text", "text": "%HT", "color": "#555555", "size": "sm", "flex": 2, "align": "end", "weight": "bold"}]}, {"type": "separator", "margin": "md", "color": SEPARATOR_COLOR}, *sold_components, *unsold_components ] }, "footer": {"type": "box", "layout": "vertical", "contents": [{"type": "text", "text": "Created By 32859-NH Dương", "color": "#AAAAAA", "size": "xs", "align": "center"}]} }}
    return flex_json

def create_leaderboard_flex_message(all_data, cluster_name=None, channel_filter=None):
    dmx_channels = ['ĐML', 'ĐMM', 'ĐMS']; tgdd_channels = ['TGD', 'AAR']
    dmx_stores, tgdd_stores = [], []
    data_to_process = all_data[1:]
    if cluster_name:
        data_to_process = [row for row in data_to_process if len(row) > 0 and row[0] and row[0].strip().upper() == cluster_name.strip().upper()]
    for row in data_to_process:
        try:
            kenh = (row[1] or "").strip()
            if not kenh: continue
            store_info = {'kenh': kenh, 'sieu_thi': row[2], 'doanh_thu': parse_float_from_string(row[4])}
            if kenh in dmx_channels: dmx_stores.append(store_info)
            elif kenh in tgdd_channels: tgdd_stores.append(store_info)
        except (IndexError, ValueError, TypeError): continue
    dmx_stores.sort(key=lambda x: x['doanh_thu'], reverse=True)
    tgdd_stores.sort(key=lambda x: x['doanh_thu'], reverse=True)
    if not cluster_name: dmx_stores, tgdd_stores = dmx_stores[:20], tgdd_stores[:20]
    def build_leaderboard_bubble(title, stores, header_bg_color, header_text_color):
        header = {"type": "box", "layout": "vertical", "backgroundColor": header_bg_color, "paddingAll": "lg", "contents": [{"type": "text", "text": title, "weight": "bold", "size": "xl", "color": header_text_color, "align": "center", "wrap": True}]}
        body_bg_color, text_color_body, separator_color = "#FFFFFF", "#000000", "#EEEEEE"
        table_header = {"type": "box", "layout": "horizontal", "margin": "md", "paddingAll": "sm", "backgroundColor": header_bg_color, "cornerRadius": "md", "contents": [{"type": "text", "text": "STT", "weight": "bold", "size": "sm", "color": header_text_color, "flex": 1, "align": "center", "gravity":"center"},{"type": "separator", "color": separator_color},{"type": "text", "text": "KÊNH", "weight": "bold", "size": "sm", "color": header_text_color, "flex": 2, "align": "center", "gravity":"center"},{"type": "separator", "color": separator_color},{"type": "text", "text": "SIÊU THỊ", "weight": "bold", "size": "sm", "color": header_text_color, "flex": 6, "align": "center", "gravity":"center"},{"type": "separator", "color": separator_color},{"type": "text", "text": "RT", "weight": "bold", "size": "sm", "color": header_text_color, "flex": 2, "align": "center", "gravity":"center"}]}
        rows = [table_header, {"type": "separator", "margin": "sm", "color": separator_color}]
        for i, store in enumerate(stores):
            name_parts = store['sieu_thi'].split(' - ', 1)
            short_name = name_parts[1] if len(name_parts) > 1 else store['sieu_thi']
            row_component = {"type": "box", "layout": "horizontal", "margin": "md", "paddingTop":"sm", "paddingBottom":"sm", "contents": [{"type": "text", "text": str(i+1), "size": "sm", "color": text_color_body, "flex": 1, "gravity": "center", "align": "center"},{"type": "separator", "color": separator_color},{"type": "text", "text": store['kenh'], "size": "sm", "color": text_color_body, "flex": 2, "gravity": "center", "align": "center"},{"type": "separator", "color": separator_color},{"type": "text", "text": short_name, "size": "xs", "color": text_color_body, "flex": 6, "wrap": True, "gravity": "center"},{"type": "separator", "color": separator_color},{"type": "text", "text": str(round(store['doanh_thu'])), "size": "sm", "color": text_color_body, "flex": 2, "align": "center", "gravity": "center"}]}
            rows.append(row_component)
            if i < len(stores) -1: rows.append({"type": "separator", "margin": "sm", "color": separator_color})
        return {"type": "bubble", "size": "giga", "header": header, "body": { "type": "box", "layout": "vertical", "contents": rows, "paddingAll":"lg", "backgroundColor": body_bg_color }}
    if cluster_name: dmx_title, tgdd_title = f"🏆 BXH CỤM {cluster_name.upper()} - ĐMX 🏆", f"🏆 BXH CỤM {cluster_name.upper()} - TGDD 🏆"
    else: dmx_title, tgdd_title = "🏆 REALTIME TOP 20 ĐMX 🏆", "🏆 REALTIME TOP 20 TGDD 🏆"
    messages_to_return = []
    if not channel_filter or channel_filter in dmx_channels:
        if dmx_stores: messages_to_return.append({ "type": "flex", "altText": dmx_title, "contents": build_leaderboard_bubble(dmx_title, dmx_stores, "#1E88E5", "#FFFFFF") })
    if not channel_filter or channel_filter in tgdd_channels:
        if tgdd_stores: messages_to_return.append({ "type": "flex", "altText": tgdd_title, "contents": build_leaderboard_bubble(tgdd_title, tgdd_stores, "#FDD835", "#000000") })
    return messages_to_return


# --- CÁC ĐIỂM TIẾP NHẬN (ROUTES) ---
@app.route("/callback", methods=['POST'])
def callback():
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return 'OK'

@app.route("/ping")
def ping():
    return "OK", 200

# --- HÀM XỬ LÝ TIN NHẮN CHÍNH ---
@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    user_message = event.message.text.strip()
    user_msg_upper = user_message.upper()
    
    user_id = event.source.user_id
    source_id = event.source.group_id if event.source.type == 'group' else user_id

    # --- LỆNH CÔNG KHAI: LẤY ID ---
    if user_msg_upper == 'ID':
        reply_text = f'User ID của bạn là:\n{user_id}'
        if event.source.type == 'group':
            reply_text = f'ID của nhóm này là:\n{event.source.group_id}\n\n' + reply_text
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))
        return

    # --- LỆNH CỦA ADMIN ---
    if ADMIN_USER_ID and user_id == ADMIN_USER_ID:
        try:
            if user_msg_upper.startswith('ADD '):
                id_to_add = user_message[4:].strip()
                if id_to_add:
                    sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_NAME_USERS)
                    sheet.append_row([id_to_add])
                    load_allowed_ids()
                    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f'Đã thêm ID thành công:\n{id_to_add}'))
                return
            elif user_msg_upper.startswith('REMOVE '):
                id_to_remove = user_message[7:].strip()
                if id_to_remove:
                    sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_NAME_USERS)
                    cell = sheet.find(id_to_remove)
                    if cell:
                        sheet.delete_rows(cell.row)
                        load_allowed_ids()
                        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f'Đã xóa ID thành công:\n{id_to_remove}'))
                    else:
                        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f'Không tìm thấy ID: {id_to_remove}'))
                return
            elif user_msg_upper == 'LIST IDS':
                reply_text = "Danh sách ID được phép:\n" + "\n".join(sorted(list(allowed_ids_cache))) if allowed_ids_cache else "Không có ID nào được cấp phép."
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))
                return
        except Exception as e:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f'Lỗi lệnh admin: {e}'))
            return

    # --- KIỂM TRA QUYỀN TRUY CẬP ---
    # Bot sẽ công khai nếu không có sheet 'allowed_users' hoặc không đặt ADMIN_USER_ID
    is_controlled = bool(allowed_ids_cache) and ADMIN_USER_ID
    if is_controlled and source_id not in allowed_ids_cache:
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text='Bạn không có quyền sử dụng bot này. Vui lòng liên hệ quản trị viên.'))
        return

    # --- CÁC LỆNH CHO NGƯỜI DÙNG ĐÃ ĐƯỢC CẤP QUYỀN ---
    try:
        sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_NAME)
        all_data = sheet.get_all_values()
        reply_messages = []
        cluster_names = {row[0].strip().upper() for row in all_data[1:] if len(row) > 0 and row[0]}
        header_row = all_data[0]
        
        if user_msg_upper.startswith('ST '):
            supermarket_code = user_message[3:].strip()
            found_row = next((row for row in all_data[1:] if row and len(row) > 2 and row[2] and row[2].strip().split(' ')[0] == supermarket_code), None)
            if found_row:
                ranking = calculate_ranking(all_data, found_row)
                competition_results = parse_competition_data(header_row, found_row)
                reply_messages.append(FlexSendMessage(alt_text=f'Báo cáo ST {supermarket_code}', contents=create_flex_message(found_row, competition_results, ranking)['contents']))
                cluster_name = (found_row[0] or "").strip().upper()
                store_channel = (found_row[1] or "").strip()
                if cluster_name in cluster_names:
                    for flex_data in create_leaderboard_flex_message(all_data, cluster_name=cluster_name, channel_filter=store_channel):
                        reply_messages.append(FlexSendMessage(alt_text=flex_data['altText'], contents=flex_data['contents']))
            else:
                reply_messages.append(TextSendMessage(text=f'Không tìm thấy dữ liệu cho mã siêu thị: {supermarket_code}'))

        elif user_msg_upper == 'BXH':
            for flex_data in create_leaderboard_flex_message(all_data):
                reply_messages.append(FlexSendMessage(alt_text=flex_data['altText'], contents=flex_data['contents']))
        
        elif user_msg_upper in cluster_names:
            for flex_data in create_leaderboard_flex_message(all_data, cluster_name=user_msg_upper):
                reply_messages.append(FlexSendMessage(alt_text=flex_data['altText'], contents=flex_data['contents']))
        
        else: # Tìm kiếm mặc định theo mã ST
            found_row = next((row for row in all_data[1:] if row and len(row) > 2 and row[2] and row[2].strip().split(' ')[0] == user_message), None)
            if found_row:
                ranking = calculate_ranking(all_data, found_row)
                competition_results = parse_competition_data(header_row, found_row)
                reply_messages.append(FlexSendMessage(alt_text='Báo cáo Realtime', contents=create_flex_message(found_row, competition_results, ranking)['contents']))
            else:
                reply_messages.append(TextSendMessage(text=f'Không tìm thấy dữ liệu cho mã siêu thị hoặc cụm: "{user_message}"'))
        
        if reply_messages:
            line_bot_api.reply_message(event.reply_token, reply_messages)

    except Exception as e:
        print(f"!!! GẶP LỖI NGHIÊM TRỌNG: {repr(e)}")
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text='Đã có lỗi xảy ra khi truy vấn dữ liệu.'))

# --- CHẠY ỨNG DỤNG ---
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)

