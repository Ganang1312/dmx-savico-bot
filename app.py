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
SHEET_NAME = 'chi_tiet_cum'
WORKSHEET_NAME = 'chi_tiet_cum'

# --- KHỞI TẠO ỨNG DỤNG ---
app = Flask(__name__)
line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)

# --- ĐỊNH NGHĨA CÁC HÀM XỬ LÝ ---
def parse_competition_data(header_row, data_row):
    start_column_index = 6
    category_indices = collections.defaultdict(list)
    for i, header in enumerate(header_row[start_column_index:], start=start_column_index):
        if header: category_indices[header].append(i)
    results = []
    for category_name, indices in category_indices.items():
        if len(indices) == 3:
            try:
                percent_ht_val, realtime_val, target_val = data_row[indices[0]], data_row[indices[1]], data_row[indices[2]]
                percent_ht_formatted = "{:.2%}".format(float(percent_ht_val)) if percent_ht_val else "0.00%"
                results.append({"name": category_name, "realtime": realtime_val, "target": target_val, "percent_ht": percent_ht_formatted})
            except (ValueError, TypeError, IndexError): continue
    return results

def create_flex_message(store_data, competition_results):
    sieu_thi_full = store_data[2]
    ma_sieu_thi = sieu_thi_full.split(' ')[0]
    ten_sieu_thi = " - ".join(sieu_thi_full.split(' - ')[1:])
    target, realtime = store_data[3], store_data[4]
    try: percent_ht_tong = "{:.2%}".format(float(store_data[5]))
    except (ValueError, TypeError): percent_ht_tong = "0.00%"
    competition_components = []
    if competition_results:
        competition_components.append({"type": "separator", "margin": "xxl"})
        title_box = {"type": "box", "layout": "horizontal", "margin": "md", "contents": [{"type": "text", "text": "Ngành hàng", "weight": "bold", "size": "sm", "color": "#555555", "flex": 4}, {"type": "text", "text": "Realtime", "weight": "bold", "size": "sm", "color": "#555555", "align": "center", "flex": 2}, {"type": "text", "text": "Target", "weight": "bold", "size": "sm", "color": "#555555", "align": "center", "flex": 2}, {"type": "text", "text": "%HT", "weight": "bold", "size": "sm", "color": "#555555", "align": "end", "flex": 2}]}
        competition_components.append(title_box)
        for item in competition_results:
            component = {"type": "box", "layout": "horizontal", "margin": "md", "contents": [{"type": "text", "text": item["name"], "wrap": True, "size": "xs", "color": "#111111", "flex": 4}, {"type": "text", "text": str(item["realtime"]), "size": "xs", "color": "#111111", "align": "center", "flex": 2}, {"type": "text", "text": str(item["target"]), "size": "xs", "color": "#111111", "align": "center", "flex": 2}, {"type": "text", "text": item["percent_ht"], "size": "xs", "color": "#111111", "align": "end", "flex": 2, "weight": "bold"}]}
            competition_components.append(component)
    flex_json = {"type": "flex", "altText": f"Báo cáo Realtime cho siêu thị {ma_sieu_thi}", "contents": {"type": "bubble", "size": "giga", "header": {"type": "box", "layout": "vertical", "contents": [{"type": "text", "text": "BÁO CÁO REALTIME", "weight": "bold", "size": "xl", "color": "#FFFFFF", "align": "center"}], "backgroundColor": "#006c83", "paddingAll": "20px"}, "body": {"type": "box", "layout": "vertical", "contents": [{"type": "box", "layout": "vertical", "margin": "lg", "spacing": "sm", "contents": [{"type": "text", "text": ten_sieu_thi, "wrap": True, "weight": "bold", "size": "md"}, {"type": "text", "text": f"Mã siêu thị: {ma_sieu_thi}", "size": "sm", "color": "#555555"}]}, {"type": "separator", "margin": "xxl"}, {"type": "box", "layout": "vertical", "margin": "xxl", "spacing": "sm", "contents": [{"type": "box", "layout": "horizontal", "contents": [{"type": "text", "text": "Target Tổng:", "size": "sm", "color": "#555555"}, {"type": "text", "text": str(target), "size": "sm", "color": "#111111", "align": "end"}]}, {"type": "box", "layout": "horizontal", "contents": [{"type": "text", "text": "Realtime Tổng:", "size": "sm", "color": "#555555"}, {"type": "text", "text": str(realtime), "size": "sm", "color": "#111111", "align": "end"}]}, {"type": "box", "layout": "horizontal", "contents": [{"type": "text", "text": "%HT Tổng:", "size": "sm", "color": "#555555"}, {"type": "text", "text": percent_ht_tong, "size": "sm", "color": "#111111", "align": "end", "weight": "bold"}]}]}, *competition_components]}}}
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

# --- HÀM XỬ LÝ TIN NHẮN CHÍNH (CÓ BƯỚC GỠ LỖI ĐẶC BIỆT) ---
@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    user_message = event.message.text.strip()
    print(f"--- Bắt đầu xử lý cho tin nhắn: '{user_message}' ---")
    try:
        # --- BƯỚC GỠ LỖI ĐẶC BIỆT ---
        print("Bước gỡ lỗi: Đang liệt kê tất cả các bảng tính mà bot có thể thấy...")
        # Sửa lại tên hàm cho đúng với phiên bản gspread mới
        accessible_spreadsheets = CLIENT.openall()
        spreadsheet_titles = [s.title for s in accessible_spreadsheets]
        print(f"=> Bot có thể thấy các file sau: {spreadsheet_titles}")

        if SHEET_NAME not in spreadsheet_titles:
            print(f"!!! LỖI QUYỀN TRUY CẬP: File '{SHEET_NAME}' không nằm trong danh sách các file có thể truy cập.")
            raise gspread.SpreadsheetNotFound(f"Không thể tìm thấy file '{SHEET_NAME}' trong danh sách được chia sẻ.")
        # --- KẾT THÚC BƯỚC GỠ LỖI ---

        print("Bước 1: Đang mở file Google Sheet...")
        sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_NAME)
        print("=> Bước 1 THÀNH CÔNG: Đã mở file và trang tính.")

        print("Bước 2: Đang lấy tất cả dữ liệu từ trang tính...")
        all_data = sheet.get_all_values()
        print(f"=> Bước 2 THÀNH CÔNG: Đã lấy được {len(all_data)} dòng dữ liệu.")

        print(f"Bước 3: Đang tìm kiếm mã '{user_message}' trong dữ liệu...")
        header_row, found_row = all_data[0], None
        for i, row in enumerate(all_data[1:]):
            if row and len(row) > 2 and row[2]:
                cell_content = row[2].strip()
                supermarket_code = cell_content.split(' ')[0]
                if supermarket_code == user_message:
                    found_row = row
                    print(f"=> Bước 3 THÀNH CÔNG: Tìm thấy dữ liệu ở dòng {i+2}.")
                    break
        if not found_row: print("=> Bước 3 CẢNH BÁO: Không tìm thấy dòng nào khớp.")

        print("Bước 4: Đang tạo tin nhắn trả lời...")
        if found_row:
            competition_results = parse_competition_data(header_row, found_row)
            flex_message_data = create_flex_message(found_row, competition_results)
            reply_message = FlexSendMessage(alt_text='Báo cáo Realtime', contents=flex_message_data['contents'])
        else:
            reply_message = TextSendMessage(text=f'Không tìm thấy dữ liệu cho mã siêu thị: {user_message}')
        print("=> Bước 4 THÀNH CÔNG: Đã tạo xong tin nhắn.")

    except Exception as e:
        print(f"!!! GẶP LỖI NGHIÊM TRỌNG: {repr(e)}")
        reply_message = TextSendMessage(text='Đã có lỗi xảy ra khi truy vấn dữ liệu.')

    print("Bước 5: Đang gửi tin nhắn trả lời về LINE...")
    line_bot_api.reply_message(event.reply_token, reply_message)
    print("--- Xử lý hoàn tất ---")

# --- CHẠY ỨNG DỤNG ---
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
