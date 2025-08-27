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

# --- PHẦN CẤU HÌNH: ĐỌC TỪ BIẾN MÔI TRƯỜNG ---
CHANNEL_ACCESS_TOKEN = os.environ.get('CHANNEL_ACCESS_TOKEN')
CHANNEL_SECRET = os.environ.get('CHANNEL_SECRET')
GOOGLE_CREDS_JSON = os.environ.get('GOOGLE_CREDENTIALS_JSON')

if not all([CHANNEL_ACCESS_TOKEN, CHANNEL_SECRET, GOOGLE_CREDS_JSON]):
    raise ValueError("Lỗi: Hãy kiểm tra lại các biến môi trường trên Render.")

# --- CẤU HÌNH GOOGLE SHEETS TỪ BIẾN MÔI TRƯỜNG ---
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
google_creds_dict = json.loads(GOOGLE_CREDS_JSON)
CREDS = ServiceAccountCredentials.from_json_keyfile_dict(google_creds_dict, SCOPE)
CLIENT = gspread.authorize(CREDS)

# Tên file và trang tính cần đọc
SHEET_NAME = 'DATA REATIME'
WORKSHEET_NAME = 'chi_tiet_cum'

# --- KHỞI TẠO ỨNG DỤNG ---
app = Flask(__name__)
line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)

# --- ĐỊNH NGHĨA CÁC HÀM XỬ LÝ ---

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
                realtime_val = data_row[indices[1]] if data_row[indices[1]] and data_row[indices[1]].strip() != '-' else "0"
                target_val = data_row[indices[2]] if data_row[indices[2]] and data_row[indices[2]].strip() != '-' else "0"
                percent_float, percent_ht_formatted = handle_percentage_string(percent_ht_val)
                results.append({
                    "name": category_name, "realtime": float(realtime_val.replace(',', '')), "target": target_val,
                    "percent_ht": percent_ht_formatted, "percent_val": percent_float
                })
            except (ValueError, TypeError, IndexError): continue
    results.sort(key=lambda x: x['percent_val'], reverse=True)
    return results

def format_currency(value_str):
    if not value_str or str(value_str).strip() == '-': return "-"
    try:
        value = float(value_str)
        if value >= 1000: return f"{round(value / 1000)} Tỷ"
        return f"{round(value)} Tr"
    except (ValueError, TypeError): return "-"

def calculate_ranking(all_data, current_row):
    try:
        current_channel = current_row[1] # Cột B: KÊNH
        current_revenue = float(current_row[4]) # Cột E: REALTIME
        
        # Lọc tất cả siêu thị trong cùng kênh
        channel_stores = []
        for row in all_data[1:]: # Bỏ qua header
            if len(row) > 4 and row[1] == current_channel:
                try:
                    revenue = float(row[4])
                    channel_stores.append({'revenue': revenue, 'full_row': row})
                except (ValueError, TypeError):
                    continue
        
        # Sắp xếp theo doanh thu giảm dần
        channel_stores.sort(key=lambda x: x['revenue'], reverse=True)
        
        # Tìm vị trí (rank)
        rank = -1
        for i, store in enumerate(channel_stores):
            # So sánh toàn bộ dòng để xác định đúng siêu thị
            if store['full_row'] == current_row:
                rank = i + 1
                break
        
        if rank != -1:
            return f"{rank}/{len(channel_stores)}"
        return "-/-"

    except (IndexError, ValueError, TypeError):
        return "-/-"

def create_flex_message(store_data, competition_results, ranking):
    # --- Trích xuất và chuẩn bị dữ liệu ---
    cum = store_data[0] or "-"
    kenh = store_data[1] or ""
    sieu_thi_full = store_data[2] or "Không có tên"
    
    # Rút gọn tên siêu thị
    ten_sieu_thi_parts = sieu_thi_full.split(' - ')
    short_name_part = ten_sieu_thi_parts[-1] if len(ten_sieu_thi_parts) > 1 else sieu_thi_full
    ten_sieu_thi_rut_gon = f"{kenh} {short_name_part}"

    realtime_tong = format_currency(store_data[4])
    target_tong = format_currency(store_data[3])
    percent_float, percent_ht_tong = handle_percentage_string(store_data[5])
    
    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    now = datetime.now(tz_vietnam)
    thoi_gian = f"{now.hour}h Ngày {now.day}/{now.month}"

    # Phân loại ngành hàng
    sold_items = [item for item in competition_results if item['realtime'] > 0]
    unsold_items = [item for item in competition_results if item['realtime'] == 0]
    nh_thi_dua_dat = sum(1 for item in sold_items if item.get("percent_val", 0) >= 1)

    # Chọn màu nền theo kênh
    channel_colors = {
        "TGDD": "#FFD600",
        "ĐML": "#006c83",
        "ĐMM": "#006c83",
        "AAR": "#5E5E5E"
    }
    header_color = channel_colors.get(kenh, "#006c83") # Màu mặc định

    # --- Xây dựng các thành phần giao diện ---
    sold_components = []
    for item in sold_items:
        percent_val = item.get("percent_val", 0)
        color = "#4CFF42" if percent_val >= 1 else ("#FFD142" if percent_val > 0.7 else "#FF4242")
        component = {"type": "box", "layout": "horizontal", "margin": "md", "paddingTop": "sm", "paddingBottom": "sm", "contents": [{"type": "text", "text": item["name"], "wrap": True, "size": "sm", "color": "#FFFFFF", "flex": 4, "gravity": "center"}, {"type": "text", "text": str(round(item["realtime"], 2)), "size": "sm", "color": "#FFFFFF", "align": "center", "flex": 2, "gravity": "center"}, {"type": "text", "text": str(item["target"]), "size": "sm", "color": "#FFFFFF", "align": "center", "flex": 2, "gravity": "center"}, {"type": "box", "layout": "vertical", "flex": 2, "contents": [{"type": "text", "text": item["percent_ht"], "size": "sm", "color": color, "align": "end", "weight": "bold", "gravity": "center"}]}]}
        sold_components.append(component)
        sold_components.append({"type": "separator", "margin": "md", "color": "#4A4A4A"})
    
    unsold_components = []
    if unsold_items:
        unsold_components.append({"type": "separator", "margin": "xl", "color": "#4A4A4A"})
        unsold_components.append({"type": "text", "text": "NGÀNH HÀNG CHƯA CÓ SỐ:", "color": "#C0C0C0", "size": "sm", "align": "center", "margin": "lg", "weight": "bold"})
        
        # Chia thành 3 cột
        num_columns = 3
        columns = [[] for _ in range(num_columns)]
        for i, item in enumerate(unsold_items):
            columns[i % num_columns].append({"type": "text", "text": f"• {item['name']}", "color": "#FFFFFF", "size": "xs", "margin": "sm", "wrap": True})

        column_boxes = [{"type": "box", "layout": "vertical", "flex": 1, "contents": col} for col in columns]
        unsold_components.append({"type": "box", "layout": "horizontal", "margin": "md", "spacing": "md", "contents": column_boxes})

    # --- Cấu trúc Flex Message hoàn chỉnh ---
    flex_json = {
      "type": "flex", "altText": f"Báo cáo cho {ten_sieu_thi_rut_gon}",
      "contents": {
        "type": "bubble", "size": "giga", "backgroundColor": "#2E2E2E",
        "body": {
          "type": "box", "layout": "vertical", "paddingAll": "0px",
          "contents": [
            {"type": "box", "layout": "vertical", "paddingAll": "20px", "backgroundColor": header_color, "contents": [
                {"type": "text", "text": "Báo cáo Realtime", "color": "#FFFFFF", "size": "lg", "align": "center", "weight": "bold"},
                {"type": "text", "text": ten_sieu_thi_rut_gon.upper(), "color": "#FFFFFF", "weight": "bold", "size": "xl", "align": "center", "margin": "md", "wrap": True},
                {"type": "box", "layout": "vertical", "margin": "lg", "spacing": "sm", "contents": [
                    {"type": "text", "text": f"⭐ Cụm: {cum}", "color": "#FFFFFF", "size": "sm"},
                    {"type": "text", "text": f"⭐ Thời gian: {thoi_gian}", "color": "#FFFFFF", "size": "sm"},
                    {"type": "text", "text": f"⭐ NH Thi Đua Đạt: {nh_thi_dua_dat}", "color": "#FFFFFF", "size": "sm"}
                ]}
            ]},
            {"type": "box", "layout": "vertical", "paddingAll": "20px", "backgroundColor": "#2E2E2E", "contents": [
                {"type": "box", "layout": "vertical", "backgroundColor": "#1C1C1C", "cornerRadius": "lg", "paddingAll": "lg", "contents": [
                    {"type": "box", "layout": "horizontal", "contents": [
                        {"type": "box", "layout": "vertical", "flex": 1, "contents": [
                            {"type": "text", "text": "DOANH THU", "color": "#C0C0C0", "size": "md", "align": "center"},
                            {"type": "text", "text": realtime_tong, "color": "#FFFFFF", "size": "xxl", "weight": "bold", "align": "center", "margin": "sm"}
                        ]},
                        {"type": "box", "layout": "vertical", "flex": 1, "contents": [
                            {"type": "text", "text": "TARGET", "color": "#C0C0C0", "size": "md", "align": "center"},
                            {"type": "text", "text": target_tong, "color": "#FFFFFF", "size": "xxl", "weight": "bold", "align": "center", "margin": "sm"}
                        ]}
                    ]},
                    {"type": "text", "text": "% HOÀN THÀNH", "color": "#C0C0C0", "size": "md", "align": "center", "margin": "xl"},
                    {"type": "text", "text": percent_ht_tong, "color": "#4CFF42", "size": "4xl", "weight": "bold", "align": "center"},
                    {"type": "box", "layout": "vertical", "backgroundColor": "#4A4A4A", "height": "8px", "cornerRadius": "md", "margin": "md", "contents": [
                        {"type": "box", "layout": "vertical", "backgroundColor": "#4CFF42", "height": "8px", "cornerRadius": "md", "width": f"{min(100, round(percent_float * 100))}%"}
                    ]},
                    {"type": "box", "layout": "horizontal", "margin": "xl", "contents": [{"type": "text", "text": "XH D.Thu Kênh", "size": "sm", "color": "#C0C0C0", "align": "center", "flex": 1}]},
                    {"type": "box", "layout": "horizontal", "contents": [{"type": "text", "text": ranking, "weight": "bold", "size": "lg", "color": "#FFFFFF", "align": "center", "flex": 1}]}
                ]},
                {"type": "separator", "margin": "xl", "color": "#4A4A4A"},
                {"type": "box", "layout": "horizontal", "margin": "md", "contents": [{"type": "text", "text": "Ngành Hàng", "color": "#C0C0C0", "size": "sm", "flex": 4, "weight": "bold"}, {"type": "text", "text": "Realtime", "color": "#C0C0C0", "size": "sm", "flex": 2, "align": "center", "weight": "bold"}, {"type": "text", "text": "Target", "color": "#C0C0C0", "size": "sm", "flex": 2, "align": "center", "weight": "bold"}, {"type": "text", "text": "%HT", "color": "#C0C0C0", "size": "sm", "flex": 2, "align": "end", "weight": "bold"}]},
                {"type": "separator", "margin": "md", "color": "#4A4A4A"},
                *sold_components,
                *unsold_components
            ]}
          ]
        },
        "footer": {"type": "box", "layout": "vertical", "contents": [{"type": "text", "text": "Created By 32859-NH Dương", "color": "#888888", "size": "xs", "align": "center"}]}
      }
    }
    return flex_json

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
    try:
        sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_NAME)
        all_data = sheet.get_all_values()
        header_row, found_row = all_data[0], None
        for row in all_data[1:]:
            if row and len(row) > 2 and row[2]:
                cell_content = row[2].strip()
                supermarket_code = cell_content.split(' ')[0]
                if supermarket_code == user_message:
                    found_row = row
                    break
        if found_row:
            ranking = calculate_ranking(all_data, found_row)
            competition_results = parse_competition_data(header_row, found_row)
            flex_message_data = create_flex_message(found_row, competition_results, ranking)
            reply_message = FlexSendMessage(alt_text='Báo cáo Realtime', contents=flex_message_data['contents'])
        else:
            reply_message = TextSendMessage(text=f'Không tìm thấy dữ liệu cho mã siêu thị: {user_message}')
    except Exception as e:
        print(f"!!! GẶP LỖI NGHIÊM TRỌNG: {repr(e)}")
        reply_message = TextSendMessage(text='Đã có lỗi xảy ra khi truy vấn dữ liệu.')
    line_bot_api.reply_message(event.reply_token, reply_message)

# --- CHẠY ỨNG DỤNG ---
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
