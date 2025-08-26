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
    """Hàm xử lý chuỗi phần trăm, trả về (float_value, formatted_string)."""
    if not percent_str: return 0.0, "0%"
    clean_str = str(percent_str).strip()
    if '%' in clean_str:
        try:
            value = float(clean_str.replace('%', '')) / 100
            return value, clean_str
        except (ValueError, TypeError): return 0.0, "0%"
    else:
        try:
            value = float(clean_str)
            return value, f"{round(value * 100)}%"
        except (ValueError, TypeError): return 0.0, "0%"

def parse_competition_data(header_row, data_row):
    """Hàm này xử lý logic phức tạp để trích xuất dữ liệu thi đua."""
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
                    "name": category_name, "realtime": realtime_val, "target": target_val,
                    "percent_ht": percent_ht_formatted, "percent_val": percent_float
                })
            except (ValueError, TypeError, IndexError): continue
    results.sort(key=lambda x: x['percent_val'], reverse=True)
    return results

def format_currency(value_str):
    """Hàm định dạng số thành chuỗi tiền tệ (Tr, Tỷ)."""
    if not value_str or str(value_str).strip() == '-': return "-"
    try:
        value = float(value_str)
        if value >= 1000: return f"{round(value / 1000, 2)} Tỷ"
        return f"{round(value, 2)} Tr"
    except (ValueError, TypeError): return "-"

def create_flex_message(store_data, competition_results):
    """Hàm tạo giao diện Flex Message."""
    cum = store_data[0] or "-"
    sieu_thi_full = store_data[2] or "Không có tên"
    ten_sieu_thi_parts = sieu_thi_full.split(' - ')
    ten_sieu_thi = " - ".join(ten_sieu_thi_parts[1:]) if len(ten_sieu_thi_parts) > 1 else sieu_thi_full
    realtime_tong = format_currency(store_data[4])
    target_tong = format_currency(store_data[3])
    _, percent_ht_tong = handle_percentage_string(store_data[5])
    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    now = datetime.now(tz_vietnam)
    thoi_gian = f"{now.hour}h Ngày {now.day}/{now.month}"
    nh_thi_dua_dat = sum(1 for item in competition_results if item.get("percent_val", 0) >= 1)
    xh_dthu = "-"
    try:
        if store_data[30] and store_data[31] and store_data[30].strip() != '-' and store_data[31].strip() != '-':
            xh_dthu = f"{store_data[30]}/{store_data[31]}"
    except IndexError: pass
    competition_components = []
    if not competition_results:
        competition_components.append({"type": "text", "text": "Không có dữ liệu thi đua.", "color": "#C0C0C0", "size": "sm", "align": "center", "margin": "md"})
    else:
        for item in competition_results:
            percent_val = item.get("percent_val", 0)
            color = "#4CFF42" if percent_val >= 1 else ("#FFD142" if percent_val > 0.7 else "#FF4242")
            component = {"type": "box", "layout": "horizontal", "margin": "md", "paddingTop": "sm", "paddingBottom": "sm", "contents": [{"type": "text", "text": item["name"], "wrap": True, "size": "sm", "color": "#FFFFFF", "flex": 4, "gravity": "center"}, {"type": "text", "text": str(item["realtime"]), "size": "sm", "color": "#FFFFFF", "align": "center", "flex": 2, "gravity": "center"}, {"type": "text", "text": str(item["target"]), "size": "sm", "color": "#FFFFFF", "align": "center", "flex": 2, "gravity": "center"}, {"type": "box", "layout": "vertical", "flex": 2, "contents": [{"type": "text", "text": item["percent_ht"], "size": "sm", "color": color, "align": "end", "weight": "bold", "gravity": "center"}]}]}
            competition_components.append(component)
            competition_components.append({"type": "separator", "margin": "md", "color": "#4A4A4A"})
    
    # Sửa lỗi cú pháp: Xóa 1 dấu '}' bị thừa ở cuối
    flex_json = {"type": "flex", "altText": f"Báo cáo tổng hợp cho {ten_sieu_thi}", "contents": {"type": "bubble", "size": "giga", "backgroundColor": "#2E2E2E", "body": {"type": "box", "layout": "vertical", "paddingAll": "0px", "contents": [{"type": "box", "layout": "vertical", "paddingAll": "20px", "backgroundColor": "#006c83", "contents": [{"type": "text", "text": "BÁO CÁO TỔNG HỢP", "color": "#FFFFFF", "size": "lg", "align": "center", "weight": "bold"}, {"type": "text", "text": ten_sieu_thi.upper(), "color": "#FFFFFF", "weight": "bold", "size": "xl", "align": "center", "margin": "md", "wrap": True}, {"type": "box", "layout": "vertical", "margin": "lg", "spacing": "sm", "contents": [{"type": "text", "text": f"⭐ Cụm: {cum}", "color": "#FFFFFF", "size": "sm"}, {"type": "text", "text": f"⭐ Thời gian: {thoi_gian}", "color": "#FFFFFF", "size": "sm"}, {"type": "text", "text": f"⭐ NH Thi Đua Đạt: {nh_thi_dua_dat}", "color": "#FFFFFF", "size": "sm"}]}]}, {"type": "box", "layout": "vertical", "paddingAll": "20px", "contents": [{"type": "box", "layout": "horizontal", "contents": [{"type": "box", "layout": "vertical", "flex": 1, "contents": [{"type": "text", "text": "DOANH THU", "color": "#C0C0C0", "size": "md", "align": "center"}, {"type": "text", "text": realtime_tong, "color": "#FFFFFF", "size": "xxl", "weight": "bold", "align": "center", "margin": "sm"}]}, {"type": "box", "layout": "vertical", "flex": 1, "contents": [{"type": "text", "text": "TARGET", "color": "#C0C0C0", "size": "md", "align": "center"}, {"type": "text", "text": target_tong, "color": "#FFFFFF", "size": "xxl", "weight": "bold", "align": "center", "margin": "sm"}]}]}, {"type": "text", "text": "% HOÀN THÀNH", "color": "#C0C0C0", "size": "md", "align": "center", "margin": "xl"}, {"type": "text", "text": percent_ht_tong, "color": "#4CFF42", "size": "4xl", "weight": "bold", "align": "center"}, {"type": "box", "layout": "horizontal", "margin": "xl", "contents": [{"type": "text", "text": "XH D.Thu ĐMX", "size": "sm", "color": "#C0C0C0", "align": "center", "flex": 1}]}, {"type": "box", "layout": "horizontal", "contents": [{"type": "text", "text": xh_dthu, "weight": "bold", "size": "lg", "color": "#FFFFFF", "align": "center", "flex": 1}]}, {"type": "separator", "margin": "xl", "color": "#4A4A4A"}, {"type": "box", "layout": "horizontal", "margin": "md", "contents": [{"type": "text", "text": "Ngành Hàng", "color": "#C0C0C0", "size": "sm", "flex": 4, "weight": "bold"}, {"type": "text", "text": "Realtime", "color": "#C0C0C0", "size": "sm", "flex": 2, "align": "center", "weight": "bold"}, {"type": "text", "text": "Target", "color": "#C0C0C0", "size": "sm", "flex": 2, "align": "center", "weight": "bold"}, {"type": "text", "text": "%HT", "color": "#C0C0C0", "size": "sm", "flex": 2, "align": "end", "weight": "bold"}]}, {"type": "separator", "margin": "md", "color": "#4A4A4A"}, *competition_components]}]}, "footer": {"type": "box", "layout": "vertical", "contents": [{"type": "text", "text": "Created By 32859-NH Dương", "color": "#888888", "size": "xs", "align": "center"}]}}}}
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
            competition_results = parse_competition_data(header_row, found_row)
            flex_message_data = create_flex_message(found_row, competition_results)
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
