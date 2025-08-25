import os
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

# --- PHẦN CẤU HÌNH: THAY THÔNG TIN CỦA BẠN VÀO ĐÂY ---
# LINE Bot credentials
CHANNEL_ACCESS_TOKEN = 'YOUR_CHANNEL_ACCESS_TOKEN' # Dán Channel Access Token của bạn vào đây
CHANNEL_SECRET = 'YOUR_CHANNEL_SECRET'           # Dán Channel Secret của bạn vào đây

# Google Sheets credentials
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
CREDS = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', SCOPE)
CLIENT = gspread.authorize(CREDS)

# Tên của Google Sheet và worksheet
SHEET_NAME = 'Realtime.xlsx' 
WORKSHEET_NAME = 'chi_tiet_cum'

# --- KẾT THÚC PHẦN CẤU HÌNH ---

app = Flask(__name__)
line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)

@app.route("/callback", methods=['POST'])
def callback():
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return 'OK'

def parse_competition_data(header_row, data_row):
    """
    Hàm này xử lý logic phức tạp để trích xuất dữ liệu thi đua.
    """
    # Cột G là index 6
    start_column_index = 6
    
    # Tìm vị trí của mỗi ngành hàng trên dòng tiêu đề
    # Ví dụ: {'Ngành hàng A': [6, 33, 60], 'Ngành hàng B': [7, 34, 61]}
    category_indices = collections.defaultdict(list)
    for i, header in enumerate(header_row[start_column_index:], start=start_column_index):
        if header: # Bỏ qua các ô tiêu đề trống
            category_indices[header].append(i)

    # Xử lý và trích xuất dữ liệu
    results = []
    for category_name, indices in category_indices.items():
        # Chỉ lấy những ngành hàng có đủ 3 cột dữ liệu
        if len(indices) == 3:
            try:
                # Lấy dữ liệu từ dòng của siêu thị dựa trên các index đã tìm được
                percent_ht_val = data_row[indices[0]]
                realtime_val = data_row[indices[1]]
                target_val = data_row[indices[2]]
                
                # Chuyển đổi %HT sang định dạng phần trăm
                try:
                    percent_ht_formatted = "{:.2%}".format(float(percent_ht_val))
                except (ValueError, TypeError):
                    percent_ht_formatted = "0.00%"
                
                results.append({
                    "name": category_name,
                    "realtime": realtime_val,
                    "target": target_val,
                    "percent_ht": percent_ht_formatted
                })
            except IndexError:
                # Bỏ qua nếu có lỗi khi truy cập index (dữ liệu không nhất quán)
                continue
    return results

def create_flex_message(store_data, competition_results):
    """Hàm này tạo cấu trúc JSON cho Flex Message dựa trên dữ liệu đầu vào"""
    sieu_thi_full = store_data[2]
    ma_sieu_thi = sieu_thi_full.split(' - ')[0]
    ten_sieu_thi = " - ".join(sieu_thi_full.split(' - ')[1:])
    target = store_data[3]
    realtime = store_data[4]
    
    try:
        percent_ht_tong = "{:.2%}".format(float(store_data[5]))
    except (ValueError, TypeError):
        percent_ht_tong = "0.00%"

    # --- Tự động tạo các dòng cho ngành hàng thi đua ---
    competition_components = []
    if competition_results:
        # Thêm 1 đường kẻ ngang trước khi liệt kê
        competition_components.append({"type": "separator", "margin": "xxl"})
        
        # Tạo tiêu đề cho phần thi đua
        title_box = {
            "type": "box",
            "layout": "horizontal",
            "margin": "md",
            "contents": [
                {"type": "text", "text": "Ngành hàng", "weight": "bold", "size": "sm", "color": "#555555", "flex": 4},
                {"type": "text", "text": "Realtime", "weight": "bold", "size": "sm", "color": "#555555", "align": "center", "flex": 2},
                {"type": "text", "text": "Target", "weight": "bold", "size": "sm", "color": "#555555", "align": "center", "flex": 2},
                {"type": "text", "text": "%HT", "weight": "bold", "size": "sm", "color": "#555555", "align": "end", "flex": 2}
            ]
        }
        competition_components.append(title_box)

        # Lặp qua kết quả và tạo component
        for item in competition_results:
            component = {
                "type": "box",
                "layout": "horizontal",
                "margin": "md",
                "contents": [
                    {"type": "text", "text": item["name"], "wrap": True, "size": "xs", "color": "#111111", "flex": 4},
                    {"type": "text", "text": str(item["realtime"]), "size": "xs", "color": "#111111", "align": "center", "flex": 2},
                    {"type": "text", "text": str(item["target"]), "size": "xs", "color": "#111111", "align": "center", "flex": 2},
                    {"type": "text", "text": item["percent_ht"], "size": "xs", "color": "#111111", "align": "end", "flex": 2, "weight": "bold"}
                ]
            }
            competition_components.append(component)

    # --- Cấu trúc Flex Message hoàn chỉnh ---
    flex_json = {
        "type": "flex",
        "altText": f"Báo cáo Realtime cho siêu thị {ma_sieu_thi}",
        "contents": {
            "type": "bubble",
            "size": "giga", # Tăng kích thước bubble để chứa nhiều thông tin hơn
            "header": {
                "type": "box",
                "layout": "vertical",
                "contents": [{"type": "text", "text": "BÁO CÁO REALTIME", "weight": "bold", "size": "xl", "color": "#FFFFFF", "align": "center"}],
                "backgroundColor": "#006c83", "paddingAll": "20px"
            },
            "body": {
                "type": "box", "layout": "vertical",
                "contents": [
                    {"type": "box", "layout": "vertical", "margin": "lg", "spacing": "sm", "contents": [
                        {"type": "text", "text": ten_sieu_thi, "wrap": True, "weight": "bold", "size": "md"},
                        {"type": "text", "text": f"Mã siêu thị: {ma_sieu_thi}", "size": "sm", "color": "#555555"}
                    ]},
                    {"type": "separator", "margin": "xxl"},
                    {"type": "box", "layout": "vertical", "margin": "xxl", "spacing": "sm", "contents": [
                        {"type": "box", "layout": "horizontal", "contents": [{"type": "text", "text": "Target Tổng:", "size": "sm", "color": "#555555"}, {"type": "text", "text": str(target), "size": "sm", "color": "#111111", "align": "end"}]},
                        {"type": "box", "layout": "horizontal", "contents": [{"type": "text", "text": "Realtime Tổng:", "size": "sm", "color": "#555555"}, {"type": "text", "text": str(realtime), "size": "sm", "color": "#111111", "align": "end"}]},
                        {"type": "box", "layout": "horizontal", "contents": [{"type": "text", "text": "%HT Tổng:", "size": "sm", "color": "#555555"}, {"type": "text", "text": percent_ht_tong, "size": "sm", "color": "#111111", "align": "end", "weight": "bold"}]}
                    ]},
                    # Chèn các component thi đua đã tạo ở trên vào đây
                    *competition_components
                ]
            }
        }
    }
    return flex_json

@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    user_message = event.message.text.strip()
    try:
        sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_NAME)
        all_data = sheet.get_all_values()
        
        header_row = all_data[0]
        found_row = None
        
        for row in all_data[1:]:
            # Cột Siêu Thị là cột C, index 2
            if row and len(row) > 2 and row[2] and row[2].strip().startswith(user_message):
                found_row = row
                break
        
        if found_row:
            # Phân tích dữ liệu thi đua từ các cột G trở đi
            competition_results = parse_competition_data(header_row, found_row)
            
            # Tạo Flex Message
            flex_message_data = create_flex_message(found_row, competition_results)
            reply_message = FlexSendMessage(
                alt_text='Báo cáo Realtime',
                contents=flex_message_data['contents']
            )
        else:
            reply_message = TextSendMessage(text=f'Không tìm thấy dữ liệu cho mã siêu thị: {user_message}')

    except Exception as e:
        print(f"Lỗi: {e}")
        reply_message = TextSendMessage(text='Đã có lỗi xảy ra khi truy vấn dữ liệu.')

    line_bot_api.reply_message(
        event.reply_token,
        reply_message
    )

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
