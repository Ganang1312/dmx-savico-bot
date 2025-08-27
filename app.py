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
        current_channel = (current_row[1] or "").strip()
        current_revenue = float(str(current_row[4]).replace(',', ''))
        channel_stores = []
        for row in all_data[1:]:
            if len(row) > 4 and (row[1] or "").strip() == current_channel:
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
    
    channel_styles = {
        "ĐML": {"bg": "#1E88E5", "text": "#FFFFFF"}, "ĐMM": {"bg": "#43A047", "text": "#FFFFFF"},
        "TGD": {"bg": "#FDD835", "text": "#000000"}, "AAR": {"bg": "#212121", "text": "#FFFFFF"}
    }
    style = channel_styles.get(kenh, {"bg": "#006c83", "text": "#FFFFFF"})
    
    sold_components = []
    for item in sold_items:
        percent_val = item.get("percent_val", 0)
        color = "#4CFF42" if percent_val >= 1 else ("#FFD142" if percent_val > 0.7 else "#FF4242")
        component = {"type": "box", "layout": "horizontal", "margin": "md", "paddingTop": "sm", "paddingBottom": "sm", "contents": [
            {"type": "text", "text": item["name"], "wrap": True, "size": "sm", "color": "#FFFFFF", "flex": 4, "gravity": "center"},
            {"type": "separator", "color": "#4A4A4A"},
            {"type": "text", "text": str(round(item["realtime"], 2)), "size": "sm", "color": "#FFFFFF", "align": "center", "flex": 2, "gravity": "center"},
            {"type": "separator", "color": "#4A4A4A"},
            {"type": "text", "text": str(item["target"]), "size": "sm", "color": "#FFFFFF", "align": "center", "flex": 2, "gravity": "center"},
            {"type": "separator", "color": "#4A4A4A"},
            {"type": "box", "layout": "vertical", "flex": 2, "contents": [{"type": "text", "text": item["percent_ht"], "size": "sm", "color": color, "align": "end", "weight": "bold", "gravity": "center"}]}
        ]}
        sold_components.append(component)
        sold_components.append({"type": "separator", "margin": "md", "color": "#4A4A4A"})
    
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

    flex_json = {
      "type": "flex", "altText": f"Báo cáo cho {ten_sieu_thi_rut_gon}",
      "contents": {
        "type": "bubble", "size": "giga",
        "header": {
          "type": "box", "layout": "vertical", "paddingAll": "20px", "backgroundColor": style["bg"],
          "contents": [
            {"type": "text", "text": "Báo cáo Realtime", "color": style["text"], "size": "lg", "align": "center", "weight": "bold"},
            {"type": "text", "text": f"🏪 {ten_sieu_thi_rut_gon.upper()}", "color": style["text"], "weight": "bold", "size": "xl", "align": "center", "margin": "md", "wrap": True},
            {"type": "box", "layout": "vertical", "margin": "lg", "spacing": "sm", "contents": [
              {"type": "text", "text": f"⭐ Cụm: {cum}", "size": "sm", "color": style["text"]},
              {"type": "text", "text": f"🕒 Thời gian: {thoi_gian}", "size": "sm", "color": style["text"]},
              {"type": "text", "text": f"🏆 NH Thi Đua Đạt: {nh_thi_dua_dat}", "size": "sm", "color": style["text"]}
            ]}
          ]
        },
        "body": {
          "type": "box", "layout": "vertical", "paddingAll": "20px", "backgroundColor": "#2E2E2E",
          "contents": [
            {"type": "box", "layout": "horizontal", "contents": [
              {"type": "box", "layout": "vertical", "flex": 1, "spacing": "sm", "contents": [
                {"type": "text", "text": "💰 DOANH THU", "color": "#87CEEB", "size": "md", "align": "center"},
                {"type": "text", "text": realtime_tong, "color": "#87CEEB", "size": "xxl", "weight": "bold", "align": "center"}
              ]},
              {"type": "box", "layout": "vertical", "flex": 1, "spacing": "sm", "contents": [
                {"type": "text", "text": "🎯 TARGET", "color": "#FFB6C1", "size": "md", "align": "center"},
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
            {"type": "box", "layout": "horizontal", "margin": "md", "contents": [{"type": "text", "text": "Ngành Hàng", "color": "#C0C0C0", "size": "sm", "flex": 4, "weight": "bold", "align": "center"}, {"type": "text", "text": "Realtime", "color": "#C0C0C0", "size": "sm", "flex": 2, "align": "center", "weight": "bold"}, {"type": "text", "text": "Target", "color": "#C0C0C0", "size": "sm", "flex": 2, "align": "center", "weight": "bold"}, {"type": "text", "text": "%HT", "color": "#C0C0C0", "size": "sm", "flex": 2, "align": "end", "weight": "bold"}]},
            {"type": "separator", "margin": "md", "color": "#4A4A4A"},
            *sold_components,
            *unsold_components
          ]
        },
        "footer": {"type": "box", "layout": "vertical", "contents": [{"type": "text", "text": "Created By 32859-NH Dương", "color": "#888888", "size": "xs", "align": "center"}]}
      }
    }
    return flex_json

# <<<--- THAY ĐỔI BẮT ĐẦU TỪ ĐÂY --->>>
def create_summary_text_message(store_data, competition_results):
    try:
        target_val = float((store_data[3] or "0").replace(',', ''))
        realtime_val = float((store_data[4] or "0").replace(',', ''))
        percent_float, _ = handle_percentage_string(store_data[5])
        
        remaining_val = target_val - realtime_val
        
        finished_items_count = sum(1 for item in competition_results if item['percent_val'] >= 1)
        
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        now = datetime.now(tz_vietnam)
        time_str = now.strftime("%H:%M:%S")

        summary = f"📊 BÁO CÁO NHANH REAL-TIME - {time_str} 📊\n"
        summary += "-------------------\n"
        summary += f"- 🎯 Target Ngày: {math.floor(target_val)}\n"
        summary += f"- 📈 Realtime: {math.floor(realtime_val)} ({round(percent_float*100)}%)\n"
        summary += f"- 📉 Còn lại: {math.floor(remaining_val)}\n"
        summary += f"- 🏆 Thi đua dự kiến đạt: {finished_items_count}/{len(competition_results)}\n"
        summary += "-------------------\n"
        summary += "🏁 TÌNH HÌNH THI ĐUA NGÀNH HÀNG 🏁\n\n"
        
        # Vòng lặp để tạo định dạng mới cho từng ngành hàng
        if competition_results:
            for item in competition_results:
                try:
                    realtime = item.get('realtime', 0)
                    target_str = str(item.get('target', '0')).replace(',', '')
                    target = float(target_str) if target_str.strip() and target_str.strip() != '-' else 0
                    
                    remaining = target - realtime
                    percent_ht = item.get('percent_ht', '0%')
                    
                    # Làm tròn số để hiển thị đẹp hơn
                    realtime_disp = math.floor(realtime) if realtime == math.floor(realtime) else round(realtime, 2)
                    target_disp = math.floor(target) if target == math.floor(target) else round(target, 2)
                    remaining_disp = math.floor(remaining) if remaining == math.floor(remaining) else round(remaining, 2)

                    summary += f"• {item['name']}: {realtime_disp}/{target_disp} ({percent_ht}) còn lại: {remaining_disp}\n"
                except (ValueError, TypeError):
                    # Fallback nếu dữ liệu target không phải là số
                    summary += f"• {item['name']}: {item.get('realtime', 0)} ({item.get('percent_ht', '0%')})\n"
        else:
            summary += "Không có dữ liệu thi đua."
            
        return TextSendMessage(text=summary)
    except Exception as e:
        print(f"Lỗi khi tạo tin nhắn tóm tắt: {e}")
        return None

def create_leaderboard_flex_message(all_data):
    dmx_channels = ['ĐML', 'ĐMM', 'ĐMS']
    tgdd_channels = ['TGD', 'AAR']
    
    dmx_stores, tgdd_stores = [], []

    for row in all_data[1:]:
        try:
            kenh = (row[1] or "").strip()
            if not kenh: continue
            
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
        
        separator_color = "#555555" # Màu cho đường kẻ

        # Tiêu đề bảng với đường kẻ dọc
        table_header = {"type": "box", "layout": "horizontal", "margin": "md", "contents": [
            {"type": "text", "text": "STT", "weight": "bold", "size": "sm", "color": "#FFFFFF", "flex": 1, "align": "center"},
            {"type": "separator", "color": separator_color},
            {"type": "text", "text": "KÊNH", "weight": "bold", "size": "sm", "color": "#FFFFFF", "flex": 2, "align": "center"},
            {"type": "separator", "color": separator_color},
            {"type": "text", "text": "SIÊU THỊ", "weight": "bold", "size": "sm", "color": "#FFFFFF", "flex": 6, "align": "center"},
            {"type": "separator", "color": separator_color},
            {"type": "text", "text": "RT", "weight": "bold", "size": "sm", "color": "#FFFFFF", "flex": 2, "align": "center"}
        ]}
        
        rows = [table_header, {"type": "separator", "margin": "sm", "color": separator_color}]
        for i, store in enumerate(stores):
            # Mỗi hàng dữ liệu với đường kẻ dọc
            row_component = {"type": "box", "layout": "horizontal", "margin": "md", "paddingTop":"sm", "paddingBottom":"sm", "contents": [
                {"type": "text", "text": str(i+1), "size": "sm", "color": "#FFFFFF", "flex": 1, "gravity": "center", "align": "center"},
                {"type": "separator", "color": separator_color},
                {"type": "text", "text": store['kenh'], "size": "sm", "color": "#FFFFFF", "flex": 2, "gravity": "center", "align": "center"},
                {"type": "separator", "color": separator_color},
                {"type": "text", "text": store['sieu_thi'], "size": "xs", "color": "#FFFFFF", "flex": 6, "wrap": True, "gravity": "center"},
                {"type": "separator", "color": separator_color},
                {"type": "text", "text": str(round(store['doanh_thu'])), "size": "sm", "color": "#FFFFFF", "flex": 2, "align": "end", "gravity": "center"}
            ]}
            rows.append(row_component)
            # Thêm đường kẻ ngang sau mỗi hàng
            rows.append({"type": "separator", "margin": "sm", "color": separator_color})

        # Nền đen cho toàn bộ bubble
        return {"type": "bubble", "size": "giga", "backgroundColor": "#000000", "header": header, "body": {"type": "box", "layout": "vertical", "contents": rows, "paddingAll":"lg"}}

    dmx_bubble = build_leaderboard_bubble("🏆 REALTIME TOP 20 ĐMX 🏆", top_20_dmx, "#1E88E5")
    tgdd_bubble = build_leaderboard_bubble("🏆 REALTIME TOP 20 TGDD 🏆", top_20_tgdd, "#FDD835", text_color="#000000")

    return {
        "type": "flex",
        "altText": "Bảng xếp hạng Realtime Top 20",
        "contents": {
            "type": "carousel",
            "contents": [dmx_bubble, tgdd_bubble]
        }
    }
# <<<--- THAY ĐỔI KẾT THÚC TẠI ĐÂY --->>>

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
        
        reply_messages = []
        if user_message.upper() == 'BXH':
            flex_message_data = create_leaderboard_flex_message(all_data)
            reply_messages.append(FlexSendMessage(
                alt_text='Bảng xếp hạng Realtime Top 20',
                contents=flex_message_data['contents']
            ))
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
                flex_message = create_flex_message(found_row, competition_results, ranking)
                reply_messages.append(FlexSendMessage(alt_text='Báo cáo Realtime', contents=flex_message['contents']))
                
                summary_message = create_summary_text_message(found_row, competition_results)
                if summary_message:
                    reply_messages.append(summary_message)
            else:
                reply_messages.append(TextSendMessage(text=f'Không tìm thấy dữ liệu cho mã siêu thị: {user_message}'))
        
        if reply_messages:
            line_bot_api.reply_message(event.reply_token, reply_messages)

    except Exception as e:
        print(f"!!! GẶP LỖI NGHIÊM TRỌNG: {repr(e)}")
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text='Đã có lỗi xảy ra khi truy vấn dữ liệu.'))

# --- CHẠY ỨNG DỤNG ---
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
