import os
import json
from flask import Flask, request, abort
from linebot import (
    LineBotApi, WebhookHandler
)
from linebot.exceptions import InvalidSignatureError, LineBotApiError
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

def format_currency(value_str, remove_decimal=False):
    if not value_str or str(value_str).strip() == '-': return "-"
    try:
        value = float(str(value_str).replace(',', ''))
        if remove_decimal:
            if value >= 1000: return f"{math.floor(value / 1000)} Tỷ"
            return f"{math.floor(value)} Tr"
        else:
            if value >= 1000: return f"{round(value / 1000, 2)} Tỷ"
            return f"{round(value, 2)} Tr"
    except (ValueError, TypeError): return "-"

def calculate_ranking(all_data, current_row):
    try:
        current_channel = current_row[1]
        current_revenue = float(str(current_row[4]).replace(',', ''))
        channel_stores = []
        for row in all_data[1:]:
            if len(row) > 4 and row[1] == current_channel:
                try:
                    revenue = float(str(row[4]).replace(',', ''))
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
    kenh = store_data[1] or ""
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
    
    channel_styles = {
        "ĐML": {"bg": "#1E88E5", "text": "#FFFFFF"}, "ĐMM": {"bg": "#43A047", "text": "#FFFFFF"},
        "TGD": {"bg": "#FDD835", "text": "#000000"}, "AAR": {"bg": "#212121", "text": "#FFFFFF"}
    }
    style = channel_styles.get(kenh, {"bg": "#006c83", "text": "#FFFFFF"})
    
    # 1. Tạo danh sách các component đã bán
    sold_components = []
    for i, item in enumerate(sold_items):
        percent_val = item.get("percent_val", 0)
        color = "#4CFF42" if percent_val >= 1 else ("#FFD142" if percent_val > 0.7 else "#FF4242")
        component = {"type": "box", "layout": "horizontal", "margin": "md", "paddingTop": "sm", "paddingBottom": "sm", "contents": [{"type": "text", "text": str(i+1), "color": "#C0C0C0", "flex": 0, "margin": "sm", "size": "sm", "gravity": "center"}, {"type": "text", "text": item["name"], "wrap": True, "size": "sm", "color": "#FFFFFF", "flex": 4, "gravity": "center"}, {"type": "text", "text": str(round(item["realtime"], 2)), "size": "sm", "color": "#FFFFFF", "align": "center", "flex": 2, "gravity": "center"}, {"type": "text", "text": str(item["target"]), "size": "sm", "color": "#FFFFFF", "align": "center", "flex": 2, "gravity": "center"}, {"type": "box", "layout": "vertical", "flex": 2, "contents": [{"type": "text", "text": item["percent_ht"], "size": "sm", "color": color, "align": "end", "weight": "bold", "gravity": "center"}]}]}
        sold_components.append(component)
        if i < len(sold_items) - 1:
            sold_components.append({"type": "separator", "margin": "md", "color": "#4A4A4A"})
    
    # 2. Tạo danh sách các component chưa bán
    unsold_components = []
    if unsold_items:
        unsold_components.extend([{"type": "separator", "margin": "xl", "color": "#4A4A4A"}, {"type": "text", "text": "NGÀNH HÀNG CHƯA CÓ SỐ:", "color": "#C0C0C0", "size": "sm", "align": "center", "margin": "lg", "weight": "bold"}])
        num_columns = 3
        columns = [[] for _ in range(num_columns)]
        for i, item in enumerate(unsold_items):
            columns[i % num_columns].append({"type": "text", "text": f"• {item['name']}", "color": "#FFFFFF", "size": "xs", "margin": "sm", "wrap": True})
        column_boxes = [{"type": "box", "layout": "vertical", "flex": 1, "contents": col} for col in columns]
        unsold_components.append({"type": "box", "layout": "horizontal", "margin": "md", "spacing": "md", "contents": column_boxes})
    
    percent_color = "#4CFF42" if percent_float >= 1 else ("#FFD142" if percent_float > 0.7 else "#FF4242")

    # 3. Xây dựng danh sách contents cho body một cách tường minh
    body_contents = [
        {"type": "box", "layout": "horizontal", "contents": [
            {"type": "box", "layout": "vertical", "flex": 1, "spacing": "sm", "contents": [
                {"type": "text", "text": "DOANH THU", "color": "#87CEEB", "size": "md", "align": "center"},
                {"type": "text", "text": realtime_tong, "color": "#87CEEB", "size": "xxl", "weight": "bold", "align": "center"}
            ]},
            {"type": "box", "layout": "vertical", "flex": 1, "spacing": "sm", "contents": [
                {"type": "text", "text": "TARGET", "color": "#FFB6C1", "size": "md", "align": "center"},
                {"type": "text", "text": target_tong, "color": "#FFB6C1", "size": "xxl", "weight": "bold", "align": "center"}
            ]}
        ]},
        {"type": "text", "text": "% HOÀN THÀNH", "color": "#C0C0C0", "size": "md", "align": "center", "margin": "xl"},
        {"type": "text", "text": percent_ht_tong, "color": percent_color, "size": "4xl", "weight": "bold", "align": "center"},
        {"type": "box", "layout": "vertical", "backgroundColor": "#4A4A4A", "height": "8px", "cornerRadius": "md", "margin": "md", "contents": [
            {"type": "box", "layout": "vertical", "backgroundColor": percent_color, "height": "8px", "cornerRadius": "md", "width": f"{min(100, round(percent_float * 100))}%"}
        ]},
        {"type": "box", "layout": "horizontal", "margin": "xl", "contents": [{"type": "text", "text": "XH D.Thu Kênh", "size": "sm", "color": "#C0C0C0", "align": "center", "flex": 1}]},
        {"type": "box", "layout": "horizontal", "contents": [{"type": "text", "text": ranking, "weight": "bold", "size": "lg", "color": "#FFFFFF", "align": "center", "flex": 1}]},
        {"type": "separator", "margin": "xl", "color": "#4A4A4A"},
        {"type": "box", "layout": "horizontal", "margin": "md", "contents": [{"type": "text", "text": "STT", "color": "#C0C0C0", "size": "sm", "flex": 0, "weight": "bold"}, {"type": "text", "text": "Ngành Hàng", "color": "#C0C0C0", "size": "sm", "flex": 4, "weight": "bold", "align": "center"}, {"type": "text", "text": "Realtime", "color": "#C0C0C0", "size": "sm", "flex": 2, "align": "center", "weight": "bold"}, {"type": "text", "text": "Target", "color": "#C0C0C0", "size": "sm", "flex": 2, "align": "center", "weight": "bold"}, {"type": "text", "text": "%HT", "color": "#C0C0C0", "size": "sm", "flex": 2, "align": "end", "weight": "bold"}]},
        {"type": "separator", "margin": "md", "color": "#4A4A4A"}
    ]
    
    # 4. Dùng extend để thêm các component động vào, đảm bảo cấu trúc phẳng
    if sold_components:
        body_contents.extend(sold_components)
    if unsold_components:
        body_contents.extend(unsold_components)

    flex_json = {
      "type": "flex", "altText": f"Báo cáo cho {ten_sieu_thi_rut_gon}",
      "contents": {
        "type": "bubble", "size": "giga",
        "header": {
            "type": "box", "layout": "vertical", "paddingAll": "20px", "backgroundColor": style["bg"],
            "contents": [
                {"type": "text", "text": "Báo cáo Realtime", "color": style["text"], "size": "lg", "align": "center", "weight": "bold"},
                {"type": "text", "text": ten_sieu_thi_rut_gon.upper(), "color": style["text"], "weight": "bold", "size": "xl", "align": "center", "margin": "md", "wrap": True},
                {"type": "box", "layout": "vertical", "margin": "lg", "spacing": "sm", "contents": [
                    {"type": "text", "size": "sm", "color": style["text"], "wrap": True, "contents": [{"type": "span", "text": "⭐ "}, {"type": "span", "text": f"Cụm: {cum}"}]},
                    {"type": "text", "size": "sm", "color": style["text"], "wrap": True, "contents": [{"type": "span", "text": "🕒 "}, {"type": "span", "text": f"Thời gian: {thoi_gian}"}]},
                    {"type": "text", "size": "sm", "color": style["text"], "wrap": True, "contents": [{"type": "span", "text": "🏆 "}, {"type": "span", "text": f"NH Thi Đua Đạt: {nh_thi_dua_dat}"}]}
                ]}
            ]
        },
        # 5. Gán danh sách đã được xây dựng hoàn chỉnh vào đây
        "body": {
            "type": "box", "layout": "vertical", "paddingAll": "20px", "backgroundColor": "#2E2E2E",
            "contents": body_contents
        },
        "footer": {"type": "box", "layout": "vertical", "contents": [{"type": "text", "text": "Created By 32859-NH Dương", "color": "#888888", "size": "xs", "align": "center"}]}
      }
    }
    return flex_json

def create_leaderboard_flex_message(all_data):
    dmx_channels = ['ĐML', 'ĐMM', 'ĐMS']
    tgdd_channels = ['TGD', 'AAR']
    
    dmx_stores = []
    tgdd_stores = []

    for row in all_data[1:]:
        try:
            kenh = row[1]
            sieu_thi = row[2]
            doanh_thu_str = str(row[4]).replace(',', '')
            doanh_thu = float(doanh_thu_str) if doanh_thu_str else 0
            
            store_info = {'kenh': kenh, 'sieu_thi': sieu_thi, 'doanh_thu': doanh_thu}
            
            if kenh in dmx_channels:
                dmx_stores.append(store_info)
            elif kenh in tgdd_channels:
                tgdd_stores.append(store_info)
        except (IndexError, ValueError, TypeError):
            continue
            
    dmx_stores.sort(key=lambda x: x['doanh_thu'], reverse=True)
    tgdd_stores.sort(key=lambda x: x['doanh_thu'], reverse=True)

    top_20_dmx = dmx_stores[:20]
    top_20_tgdd = tgdd_stores[:20]

    def build_leaderboard_bubble(title, stores, color, text_color="#FFFFFF"):
        header = {"type": "box", "layout": "vertical", "backgroundColor": color, "paddingAll": "lg", "contents": [{"type": "text", "text": title, "weight": "bold", "size": "xl", "color": text_color, "align": "center"}]}
        
        table_header = {"type": "box", "layout": "horizontal", "margin": "md", "contents": [
            {"type": "text", "text": "STT", "weight": "bold", "size": "sm", "color": "#C0C0C0", "flex": 1},
            {"type": "text", "text": "KÊNH", "weight": "bold", "size": "sm", "color": "#C0C0C0", "flex": 2},
            {"type": "text", "text": "SIÊU THỊ", "weight": "bold", "size": "sm", "color": "#C0C0C0", "flex": 6},
            {"type": "text", "text": "RT", "weight": "bold", "size": "sm", "color": "#C0C0C0", "flex": 2, "align": "end"}
        ]}
        
        rows = [table_header, {"type": "separator", "margin": "sm", "color": "#4A4A4A"}]
        for i, store in enumerate(stores):
            row_component = {"type": "box", "layout": "horizontal", "margin": "md", "contents": [
                {"type": "text", "text": str(i+1), "size": "sm", "color": "#FFFFFF", "flex": 1},
                {"type": "text", "text": store['kenh'], "size": "sm", "color": "#FFFFFF", "flex": 2},
                {"type": "text", "text": store['sieu_thi'], "size": "xs", "color": "#FFFFFF", "flex": 6, "wrap": True},
                {"type": "text", "text": str(round(store['doanh_thu'])), "size": "sm", "color": "#FFFFFF", "flex": 2, "align": "end"}
            ]}
            rows.append(row_component)

        return {"type": "bubble", "size": "giga", "backgroundColor": "#2E2E2E", "header": header, "body": {"type": "box", "layout": "vertical", "contents": rows}}

    dmx_bubble = build_leaderboard_bubble("REALTIME TOP 20 ĐMX", top_20_dmx, "#1E88E5")
    tgdd_bubble = build_leaderboard_bubble("REALTIME TOP 20 TGDD", top_20_tgdd, "#FDD835", text_color="#000000")

    return {
        "type": "flex",
        "altText": "Bảng xếp hạng Realtime Top 20",
        "contents": {
            "type": "carousel",
            "contents": [dmx_bubble, tgdd_bubble]
        }
    }

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
        
        if user_message.upper() == 'BXH':
            flex_message_data = create_leaderboard_flex_message(all_data)
            reply_message = FlexSendMessage(
                alt_text='Bảng xếp hạng Realtime Top 20',
                contents=flex_message_data['contents']
            )
        else:
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
    
    try:
        line_bot_api.reply_message(event.reply_token, reply_message)
    except LineBotApiError as e:
        print(f"!!! LỖI KHI GỬI TIN NHẮN: {e.status_code}")
        print(e.error.details)


# --- CHẠY ỨNG DỤNG ---
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
