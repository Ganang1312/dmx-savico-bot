import os
import json
import collections
import math
import threading
import time
import requests
from datetime import datetime
import pytz

from flask import Flask, request, abort
from linebot import (
    LineBotApi, WebhookHandler
)
from linebot.exceptions import InvalidSignatureError
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage, FlexSendMessage
)
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler

# --- IMPORT TỪ CÁC FILE KHÁC CỦA BẠN ---
from config import CLIENT, SHEET_NAME, WORKSHEET_NAME_USERS, WORKSHEET_NAME
# THÊM MỚI: Import các hàm cần thiết để tra cứu lịch
from schedule_handler import get_vietnamese_day_of_week, create_schedule_flex_message

# --- PHẦN CẤU HÌNH: ĐỌC TỪ BIẾN MÔI TRƯỜNG ---
CHANNEL_ACCESS_TOKEN = os.environ.get('CHANNEL_ACCESS_TOKEN')
CHANNEL_SECRET = os.environ.get('CHANNEL_SECRET')
ADMIN_USER_ID = os.environ.get('ADMIN_USER_ID')
CRON_SECRET_KEY = os.environ.get('CRON_SECRET_KEY')

if not all([CHANNEL_ACCESS_TOKEN, CHANNEL_SECRET]):
    raise ValueError("Lỗi: Hãy kiểm tra lại các biến môi trường trên Render.")

allowed_ids_cache = set()

# --- KHỞI TẠO ỨNG DỤNG ---
app = Flask(__name__)
line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)

# --- CÁC HÀM TIỆN ÍCH ---
def load_allowed_ids():
    global allowed_ids_cache
    try:
        print("Đang tải danh sách ID được phép...")
        sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_NAME_USERS)
        list_of_ids = sheet.col_values(1)
        allowed_ids_cache = set(filter(None, list_of_ids[1:] if list_of_ids and list_of_ids[0].lower() == 'id' else list_of_ids))
        print(f"Đã tải thành công {len(allowed_ids_cache)} ID vào danh sách cho phép.")
    except Exception as e:
        print(f"Lỗi khi tải danh sách ID: {e}")
        allowed_ids_cache = set()

def keep_alive():
    ping_url = os.environ.get("RENDER_EXTERNAL_URL")
    if not ping_url:
        print("Bỏ qua chức năng keep-alive vì không có RENDER_EXTERNAL_URL.")
        return
    while True:
        try:
            requests.get(ping_url.rstrip('/') + "/ping", timeout=10)
        except requests.exceptions.RequestException as e:
            print(f"Lỗi khi ping: {e}")
        time.sleep(600)

# --- HÀM GỬI BÁO CÁO CÔNG VIỆC CỐ ĐỊNH (MỚI) ---
def send_static_report(shift):
    """
    Hàm này gửi một tin nhắn văn bản cố định thông báo công việc
    đến tất cả các group được liệt kê trong Google Sheet.
    """
    print(f"Bắt đầu gửi báo cáo công việc cố định cho ca: {shift}")

    # --- ĐỊNH NGHĨA NỘI DUNG TIN NHẮN ---
    if shift == 'sang':
        report_text = (
            "✅ BÁO CÁO CÔNG VIỆC CA SÁNG\n"
            "1️⃣ 📦 Check lệnh chuyển kho online (09:15)\n"
            "2️⃣ 🚚 Check đơn GHTK chuyển kho (09:30)\n"
            "3️⃣ 🏷️ Chạy tủ, thay giá TBBM, DSD (thứ 2 & 5) (10:00)\n"
            "4️⃣ 🧹 Rà soát tốc kệ (cùng model, nhóm màu, sạch bụi) (10:30)\n"
            "5️⃣ 📑 Check Phiếu CK / NK quá 7 ngày (11:30)\n"
            "6️⃣ 🔧 Đổ tồn hàng T.Thái (lỗi) → Gửi bảo hành, xử lý về 0 (Trước 14:00)"
        )
    elif shift == 'chieu':
        report_text = (
            "🌙 BÁO CÁO CÔNG VIỆC CA CHIỀU\n"
            "1️⃣ 📦 Check lệnh online (15:15)\n"
            "2️⃣ 🚚 Check đơn GHTK (15:30)\n"
            "3️⃣ 📦🧹 Sắp xếp hàng hóa kho & dọn bàn làm việc (16:00)\n"
            "4️⃣ 🖼️ Rà soát tốc kệ (gia dụng / tivi / ụ giá sốc) (16:30)\n"
            "5️⃣ 📊 Xử lý BCNB chiều (17:30)\n"
            "6️⃣ 🔧 Đổ tồn hàng T.Thái (lỗi) → Gửi bảo hành, xử lý về 0 (Trước 19:00)\n"
            "7️⃣ 📦🚚 Check GHTK / Grab (21:00)\n"
            "8️⃣ 📸 Up hình máy cũ / máy trưng bày (21:30)"
        )
    else:
        print(f"Lỗi: Ca làm việc '{shift}' không hợp lệ.")
        return

    # --- LẤY DANH SÁCH GROUP VÀ GỬI TIN NHẮN ---
    try:
        sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_NAME_USERS)
        group_ids = sheet.col_values(1)[1:] 
        
        if not group_ids:
            print("Không tìm thấy group ID nào để gửi thông báo.")
            return

        print(f"Sẽ gửi thông báo đến {len(group_ids)} group.")
        
        for group_id in group_ids:
            if group_id:
                try:
                    line_bot_api.push_message(group_id, TextSendMessage(text=report_text))
                    print(f"Đã gửi thành công đến group: {group_id}")
                except Exception as e:
                    print(f"Lỗi khi gửi đến group {group_id}: {e}")
                    
    except Exception as e:
        print(f"Lỗi nghiêm trọng khi thực hiện send_static_report: {e}")

# --- CÁC HÀM XỬ LÝ DỮ LIỆU BÁO CÁO (KHÔNG THAY ĐỔI) ---
def parse_float_from_string(s):
    if s is None: return 0.0
    if not isinstance(s, str): s = str(s)
    clean_s = s.strip()
    if not clean_s or clean_s == '-': return 0.0
    try:
        return float(clean_s.replace(',', '.'))
    except ValueError: return 0.0

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
                results.append({"name": category_name, "realtime": parse_float_from_string(realtime_val_str), "target": target_val_str, "percent_ht": percent_ht_formatted, "percent_val": percent_float})
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
                rank = i + 1; break
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
    
    flex_json = {"type": "flex", "altText": f"Báo cáo cho {ten_sieu_thi_rut_gon}", "contents": { "type": "bubble", "size": "giga", "header": { "type": "box", "layout": "vertical", "paddingAll": "20px", "backgroundColor": style["bg"], "contents": [ {"type": "text", "text": "Báo cáo Realtime", "color": style["text"], "size": "lg", "align": "center", "weight": "bold"}, {"type": "text", "text": f"🏪 {ten_sieu_thi_rut_gon.upper()}", "color": style["text"], "weight": "bold", "size": "xl", "align": "center", "margin": "md", "wrap": True}, {"type": "box", "layout": "vertical", "margin": "lg", "spacing": "sm", "contents": [ {"type": "text", "text": f"⭐ Cụm: {cum}", "size": "sm", "color": style["text"]}, {"type": "text", "text": f"🕒 Thời gian: {thoi_gian}", "size": "sm", "color": style["text"]}, {"type": "text", "text": f"🏆 NH thi đua đạt: {nh_thi_dua_dat}", "size": "sm", "color": style["text"]} ]} ] }, "body": { "type": "box", "layout": "vertical", "paddingAll": "20px", "backgroundColor": "#FFFFFF", "contents": [ {"type": "box", "layout": "horizontal", "contents": [ {"type": "box", "layout": "vertical", "flex": 1, "spacing": "sm", "contents": [ {"type": "text", "text": "💰 DOANH THU", "color": "#007BFF", "size": "md", "align": "center", "weight":"bold"}, {"type": "text", "text": realtime_tong, "color": "#007BFF", "size": "xxl", "weight": "bold", "align": "center"} ]}, {"type": "box", "layout": "vertical", "flex": 1, "spacing": "sm", "contents": [ {"type": "text", "text": "🎯 TARGET", "color": "#DC3545", "size": "md", "align": "center", "weight":"bold"}, {"type": "text", "text": target_tong, "color": "#DC3545", "size": "xxl", "weight": "bold", "align": "center"} ]} ]}, {"type": "text", "text": "% HOÀN THÀNH", "color": TEXT_COLOR, "size": "md", "align": "center", "margin": "xl"}, {"type": "text", "text": percent_ht_tong, "color": percent_color, "size": "4xl", "weight": "bold", "align": "center"}, {"type": "box", "layout": "vertical", "backgroundColor": "#DDDDDD", "height": "8px", "cornerRadius": "md", "margin": "md", "contents": [ {"type": "box", "layout": "vertical", "backgroundColor": percent_color, "height": "8px", "cornerRadius": "md", "width": f"{min(100, round(percent_float * 100))}%"} ]}, {"type": "box", "layout": "horizontal", "margin": "xl", "contents": [{"type": "text", "text": "XH D.Thu Kênh", "size": "sm", "color": TEXT_COLOR, "align": "center", "flex": 1}]}, {"type": "box", "layout": "horizontal", "contents": [{"type": "text", "text": ranking, "weight": "bold", "size": "lg", "color": TEXT_COLOR, "align": "center", "flex": 1}]}, {"type": "separator", "margin": "xl", "color": SEPARATOR_COLOR}, {"type": "box", "layout": "horizontal", "margin": "md", "contents": [{"type": "text", "text": "Ngành Hàng", "color": "#555555", "size": "sm", "flex": 4, "weight": "bold", "align": "center"}, {"type": "text", "text": "Realtime", "color": "#555555", "size": "sm", "flex": 2, "align": "center", "weight": "bold"}, {"type": "text", "text": "Target", "color": "#555555", "size": "sm", "flex": 2, "align": "center", "weight": "bold"}, {"type": "text", "text": "%HT", "color": "#555555", "size": "sm", "flex": 2, "align": "end", "weight": "bold"}]}, {"type": "separator", "margin": "md", "color": SEPARATOR_COLOR}, *sold_components, *unsold_components ] } }}
    return flex_json

def create_summary_text_message(store_data, competition_results):
    try:
        target_val = parse_float_from_string(store_data[3])
        realtime_val = parse_float_from_string(store_data[4])
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
        summary += f"- 🏆 NH thi đua đạt: {finished_items_count}/{len(competition_results)}\n"
        summary += "-------------------\n"
        summary += "🏁 THI ĐUA NH:\n\n"
        sold_items = [item for item in competition_results if item.get('realtime', 0) > 0]
        if sold_items:
            for item in sold_items:
                try:
                    realtime = item.get('realtime', 0)
                    target = parse_float_from_string(item.get('target', '0'))
                    remaining = target - realtime
                    percent_ht = item.get('percent_ht', '0%')
                    realtime_disp = math.floor(realtime) if realtime == math.floor(realtime) else round(realtime, 2)
                    target_disp = math.floor(target) if target == math.floor(target) else round(target, 2)
                    remaining_disp = math.floor(remaining) if remaining == math.floor(remaining) else round(remaining, 2)
                    summary += f"• {item['name']}: {realtime_disp}/{target_disp} ({percent_ht}) còn lại: {remaining_disp}\n"
                except (ValueError, TypeError):
                    summary += f"• {item['name']}: {item.get('realtime', 0)} ({item.get('percent_ht', '0%')})\n"
        else:
            summary += "Chưa có ngành hàng thi đua nào phát sinh doanh số."
        return TextSendMessage(text=summary)
    except Exception as e:
        print(f"Lỗi khi tạo tin nhắn tóm tắt: {e}")
        return None

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
    
    show_dmx = False
    show_tgdd = False

    if channel_filter is None:
        show_dmx = True
        show_tgdd = True
    elif channel_filter == 'dmx':
        show_dmx = True
    elif channel_filter == 'tgdd':
        show_tgdd = True
    elif channel_filter in dmx_channels:
        show_dmx = True
    elif channel_filter in tgdd_channels:
        show_tgdd = True

    if show_dmx and dmx_stores:
        messages_to_return.append({ "type": "flex", "altText": dmx_title, "contents": build_leaderboard_bubble(dmx_title, dmx_stores, "#1E88E5", "#FFFFFF") })
    if show_tgdd and tgdd_stores:
        messages_to_return.append({ "type": "flex", "altText": tgdd_title, "contents": build_leaderboard_bubble(tgdd_title, tgdd_stores, "#FDD835", "#000000") })
        
    return messages_to_return

# --- KHỞI ĐỘNG CÁC TÁC VỤ NỀN ---
load_allowed_ids()
if 'RENDER' in os.environ:
    keep_alive_thread = threading.Thread(target=keep_alive, daemon=True)
    keep_alive_thread.start()

scheduler = BackgroundScheduler(daemon=True, timezone='Asia/Ho_Chi_Minh')
# ĐÃ XÓA TÁC VỤ NHẮC NHỞ TỰ ĐỘNG (reminder_job)
scheduler.start()

# --- ĐIỂM TIẾP NHẬN (ROUTES) ---
@app.route("/trigger-schedule", methods=['POST'])
def trigger_schedule():
    incoming_secret = request.headers.get('X-Cron-Secret')
    if not CRON_SECRET_KEY or incoming_secret != CRON_SECRET_KEY:
        print("Lỗi bảo mật: Sai hoặc thiếu CRON_SECRET_KEY.")
        abort(403)
    data = request.get_json()
    schedule_type = data.get('type')
    if schedule_type not in ['pg', 'employee']:
        return "Lỗi: 'type' phải là 'pg' hoặc 'employee'.", 400
    try:
        from schedule_handler import send_daily_schedule
        thread = threading.Thread(target=send_daily_schedule, args=(schedule_type,))
        thread.start()
        print(f"Đã kích hoạt thành công gửi lịch cho: {schedule_type}")
        return f"OK, đã kích hoạt gửi lịch cho {schedule_type}.", 200
    except Exception as e:
        print(f"Lỗi khi kích hoạt gửi lịch: {e}")
        return f"Lỗi máy chủ: {e}", 500
    
@app.route("/trigger-checklist", methods=['POST'])
def trigger_checklist():
    """
    Endpoint này được Cron Job gọi để gửi báo cáo công việc cố định.
    Đã được chỉnh sửa để gọi hàm send_static_report.
    """
    incoming_secret = request.headers.get('X-Cron-Secret')
    if not CRON_SECRET_KEY or incoming_secret != CRON_SECRET_KEY:
        print("Lỗi bảo mật: Sai hoặc thiếu CRON_SECRET_KEY.")
        abort(403)
        
    data = request.get_json()
    shift = data.get('shift')
    if shift not in ['sang', 'chieu']:
        return "Lỗi: 'shift' phải là 'sang' hoặc 'chieu'.", 400
        
    try:
        # Chạy hàm gửi báo cáo trong một luồng riêng để không bị timeout
        thread = threading.Thread(target=send_static_report, args=(shift,))
        thread.start()
        
        print(f"Đã kích hoạt gửi báo cáo công việc cố định cho ca: {shift}")
        return f"OK, đã kích hoạt gửi báo cáo ca {shift}.", 200
    except Exception as e:
        print(f"Lỗi khi kích hoạt gửi báo cáo: {e}")
        return f"Lỗi máy chủ: {e}", 500

@app.route("/callback", methods=['POST'])
def callback():
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError: abort(400)
    return 'OK'

@app.route("/ping")
def ping():
    return "OK", 200

# --- XỬ LÝ NÚT BẤM (POSTBACK) ĐÃ BỊ XÓA ---
# Toàn bộ hàm handle_postback và process_task_completion đã được loại bỏ.

# --- XỬ LÝ TIN NHẮN CHÍNH ---
@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    user_message = event.message.text.strip()
    user_msg_upper = user_message.upper()
    user_id = event.source.user_id
    source_id = event.source.group_id if event.source.type == 'group' else user_id

    # --- LỆNH TIỆN ÍCH ---
    if user_msg_upper == 'ID':
        reply_text = f'User ID: {user_id}'
        if event.source.type == 'group':
            reply_text = f'Group ID: {source_id}\n\n' + reply_text
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))
        return

    # --- LỆNH XEM LỊCH LÀM VIỆC ---
    schedule_type_to_send = None
    if user_msg_upper == 'NV':
        schedule_type_to_send = 'employee'
    elif user_msg_upper == 'PG':
        schedule_type_to_send = 'pg'

    if schedule_type_to_send:
        print(f"Nhận lệnh xem lịch '{schedule_type_to_send}'")
        try:
            from config import WORKSHEET_SCHEDULES_NAME
            sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_SCHEDULES_NAME)
            all_schedules = sheet.get_all_records()
            today_str = get_vietnamese_day_of_week()
            column_to_read = 'pg_schedule' if schedule_type_to_send == 'pg' else 'employee_schedule'
            schedule_text_today = next((row.get(column_to_read) for row in all_schedules if row.get('day_of_week') == today_str), None)
            if schedule_text_today:
                flex_content = create_schedule_flex_message(schedule_type_to_send, schedule_text_today)
                message = FlexSendMessage(alt_text=f"Lịch làm việc {schedule_type_to_send} hôm nay", contents=flex_content)
                line_bot_api.reply_message(event.reply_token, message)
            else:
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"Không tìm thấy lịch làm việc cho hôm nay ({today_str})."))
        except Exception as e:
            print(f"Lỗi khi gửi lịch làm việc theo lệnh: {e}")
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="Đã có lỗi xảy ra khi lấy lịch làm việc."))
        return
        
    # --- LỆNH CHECKLIST ĐÃ BỊ XÓA ---
    # Toàn bộ logic xử lý 'TEST SANG', 'RESET CHIEU' đã được loại bỏ.

    # --- KIỂM TRA QUYỀN TRUY CẬP ---
    is_controlled = bool(allowed_ids_cache) and ADMIN_USER_ID
    if is_controlled and source_id not in allowed_ids_cache:
        # Cho phép các lệnh không cần kiểm tra quyền truy cập ở trên chạy qua
        if user_msg_upper not in ['ID', 'NV', 'PG']:
            print(f"Từ chối truy cập từ source_id: {source_id}")
            return

    # --- XỬ LÝ BÁO CÁO REALTIME ---
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
        
        elif user_msg_upper == 'BXH1':
            for flex_data in create_leaderboard_flex_message(all_data, channel_filter='dmx'):
                reply_messages.append(FlexSendMessage(alt_text=flex_data['altText'], contents=flex_data['contents']))

        elif user_msg_upper == 'BXH2':
            for flex_data in create_leaderboard_flex_message(all_data, channel_filter='tgdd'):
                reply_messages.append(FlexSendMessage(alt_text=flex_data['altText'], contents=flex_data['contents']))
        
        elif user_msg_upper == 'MENU BOT':
            menu_text = (
                "🤖 MENU HƯỚNG DẪN BOT 🤖\n"
                "-------------------\n"
                "✅ BÁO CÁO REALTIME:\n"
                "• `ST [Mã ST]` - Xem báo cáo chi tiết.\n"
                "  ↳ Ví dụ: `ST 12345`\n"
                "• `[Tên Cụm]` - BXH doanh thu trong cụm.\n"
                "  ↳ Ví dụ: `CHG`\n"
                "• `BXH` - Top 20 ĐMX & TGDD.\n"
                "• `BXH1` - Top 20 ĐMX.\n"
                "• `BXH2` - Top 20 TGDD.\n"
                "\n✅ LỊCH LÀM VIỆC:\n"
                "• `NV` - Xem lịch làm việc Nhân viên.\n"
                "• `PG` - Xem lịch làm việc PG.\n"
                "\n✅ TIỆN ÍCH KHÁC:\n"
                "• `ID` - Lấy ID cá nhân & ID nhóm.\n"
                "• `MENU BOT` - Hiển thị lại menu này."
            )
            reply_messages.append(TextSendMessage(text=menu_text))
            
        elif user_msg_upper in cluster_names:
            for flex_data in create_leaderboard_flex_message(all_data, cluster_name=user_msg_upper):
                reply_messages.append(FlexSendMessage(alt_text=flex_data['altText'], contents=flex_data['contents']))
        
        else:
            found_row = next((row for row in all_data[1:] if row and len(row) > 2 and row[2] and row[2].strip().split(' ')[0] == user_message), None)
            if found_row:
                ranking = calculate_ranking(all_data, found_row)
                competition_results = parse_competition_data(header_row, found_row)
                reply_messages.append(FlexSendMessage(alt_text='Báo cáo Realtime', contents=create_flex_message(found_row, competition_results, ranking)['contents']))
                summary_message = create_summary_text_message(found_row, competition_results)
                if summary_message:
                    reply_messages.append(summary_message)
        
        if reply_messages:
            line_bot_api.reply_message(event.reply_token, reply_messages)

    except Exception as e:
        print(f"!!! GẶP LỖI NGHIÊM TRỌNG (BÁO CÁO): {repr(e)}")

# --- CHẠY ỨNG DỤNG ---
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
