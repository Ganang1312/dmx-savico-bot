import os
import json
import collections
import math
import threading
import time
import requests
from datetime import datetime, date
import pytz
from dateutil.relativedelta import relativedelta
import re

from flask import Flask, request, abort, jsonify
from linebot import (
    LineBotApi, WebhookHandler
)
from linebot.exceptions import InvalidSignatureError
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage, FlexSendMessage, PostbackEvent, ImageSendMessage
)
import pandas as pd

# --- IMPORT ---
from config import CLIENT, SHEET_NAME, WORKSHEET_NAME_USERS, WORKSHEET_NAME, WORKSHEET_TRACKER_NAME
# CẬP NHẬT IMPORT MỚI
from schedule_handler import send_daily_schedule
from flex_handler import initialize_daily_tasks, generate_checklist_flex
from checklist_scheduler import send_initial_checklist, get_checklist_message 
from meal_handler import generate_meal_flex, update_meal_status

# --- CẤU HÌNH ---
CHANNEL_ACCESS_TOKEN = os.environ.get('CHANNEL_ACCESS_TOKEN')
CHANNEL_SECRET = os.environ.get('CHANNEL_SECRET')
ADMIN_USER_ID = os.environ.get('ADMIN_USER_ID')
CRON_SECRET_KEY = os.environ.get('CRON_SECRET_KEY')

if not all([CHANNEL_ACCESS_TOKEN, CHANNEL_SECRET, ADMIN_USER_ID]):
    print("Cảnh báo: Thiếu biến môi trường quan trọng.")

allowed_ids_cache = set()
app = Flask(__name__)
line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)

# --- UTILS ---
def load_allowed_ids():
    global allowed_ids_cache
    try:
        sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_NAME_USERS)
        records = sheet.get_all_records()
        new_allowed_ids = set()
        today = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).date()
        for record in records:
            user_id = record.get('id')
            exp_date_str = record.get('expiration_date')
            if not user_id or not exp_date_str: continue
            try:
                exp_date = datetime.strptime(exp_date_str, '%Y-%m-%d').date()
                if exp_date >= today: new_allowed_ids.add(str(user_id))
            except ValueError: continue
        allowed_ids_cache = new_allowed_ids
    except Exception as e:
        print(f"Lỗi tải danh sách ID: {e}")
        allowed_ids_cache = set()

def keep_alive():
    ping_url = os.environ.get("RENDER_EXTERNAL_URL")
    if not ping_url: return
    while True:
        try: requests.get(ping_url.rstrip('/') + "/ping", timeout=10)
        except: pass
        time.sleep(600)

def update_expiration_in_sheet(target_id, expiration_date_str):
    sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_NAME_USERS)
    all_ids = sheet.col_values(1)
    try:
        row_to_update = all_ids.index(target_id) + 1
        sheet.update_cell(row_to_update, 2, expiration_date_str)
        return "Cập nhật"
    except ValueError:
        sheet.append_row([target_id, expiration_date_str])
        return "Thêm mới"

def parse_duration(duration_str):
    if duration_str == '0': return relativedelta(years=999), "vĩnh viễn"
    match = re.match(r"^(\d+)([dm])$", duration_str.lower())
    if not match: return None, None
    value = int(match.group(1))
    unit = match.group(2)
    if unit == 'd': return relativedelta(days=value), f"{value} ngày"
    if unit == 'm': return relativedelta(months=value), f"{value} tháng"
    # --- AUTO SYNC UTILS ---
GAS_URL = "https://script.google.com/macros/s/AKfycbxjTGuxxRiX1zY78pRkZ55qhJQF5om1Vht_kC3exy5JsYVBwjH1u7G2crr6dwbFz0lj/exec"

def handle_auto_sync_command(event, cmd_type):
    from flask import request
    source_id = getattr(event.source, 'group_id', event.source.user_id)
    cmd_name = {
        "RT": "Realtime",
        "LK": "Lũy Kế",
        "NV0": "Nhân Viên"
    }.get(cmd_type, cmd_type)
    
    host_url = request.url_root.replace('https://', '').replace('http://', '').strip('/')
    
    line_bot_api.reply_message(
        event.reply_token,
        TextSendMessage(text=f"⏳ Đang xếp hàng lệnh đổ số {cmd_name}.\nTrình duyệt trên máy tính sẽ tự động cào số và gửi báo cáo sau ít phút...")
    )
    
    threading.Thread(target=async_process_bot_command, args=(source_id, cmd_type, cmd_name, host_url)).start()

def post_to_gas(payload):
    try:
        res = requests.post(GAS_URL, json=payload, allow_redirects=False, timeout=15)
        if res.status_code in [301, 302, 307, 308] and 'Location' in res.headers:
            redirect_url = res.headers['Location']
            res = requests.post(redirect_url, json=payload, timeout=15)
        return res
    except Exception as e:
        print(f"Error in post_to_gas: {e}")
        raise e

def async_process_bot_command(source_id, cmd_type, cmd_name, host_url=""):
    import uuid
    # Mã hóa source_id và host_url vào cmd_id bằng dấu phân tách __ để stateless và tránh truy vấn DB/Sheets phức tạp
    cmd_id = f"{cmd_type}_{int(time.time())}_{uuid.uuid4().hex[:6]}__{source_id}__{host_url}"
    
    try:
        payload = {
            "action": "create_command",
            "cmdId": cmd_id,
            "cmdType": cmd_type
        }
        res = post_to_gas(payload)
        print(f"Created command {cmd_id}: {res.text}")
    except Exception as e:
        print(f"Lỗi khi gửi lệnh lên GAS: {e}")
        try:
            line_bot_api.push_message(source_id, TextSendMessage(text=f"❌ Lỗi kết nối Google Sheets khi gửi lệnh {cmd_name}."))
        except Exception as push_err:
            print(f"Lỗi gửi tin báo lỗi kết nối GAS: {push_err}")

# --- REPORT UTILS ---
def parse_float_from_string(s):
    if s is None: return 0.0
    if not isinstance(s, str): s = str(s)
    clean_s = s.strip()
    if not clean_s or clean_s == '-': return 0.0
    try: return float(clean_s.replace(',', '.'))
    except ValueError: return 0.0

def handle_percentage_string(percent_str):
    if not percent_str: return 0.0, "0%"
    clean_str = str(percent_str).strip().replace(',', '.')
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

def calculate_ranking(all_data, current_row):
    try:
        current_channel = (current_row[1] or "").strip()
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
    except: return "-/-"

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
            except: continue
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
    except: return "-"

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
        component = {"type": "box", "layout": "horizontal", "margin": "md", "paddingTop": "sm", "paddingBottom": "sm", "contents": [{"type": "text", "text": item["name"], "wrap": True, "size": "xs", "color": TEXT_COLOR, "flex": 4, "gravity": "center"},{"type": "separator", "color": SEPARATOR_COLOR},{"type": "text", "text": str(round(item["realtime"], 2)), "size": "xs", "color": TEXT_COLOR, "align": "center", "flex": 2, "gravity": "center"},{"type": "separator", "color": SEPARATOR_COLOR},{"type": "text", "text": target_display, "size": "xs", "color": TEXT_COLOR, "align": "center", "flex": 2, "gravity": "center"},{"type": "separator", "color": SEPARATOR_COLOR},{"type": "box", "layout": "vertical", "flex": 2, "contents": [{"type": "text", "text": item["percent_ht"], "size": "xs", "color": color, "align": "end", "weight": "bold", "gravity": "center"}]}]}
        sold_components.append(component)
        sold_components.append({"type": "separator", "margin": "md", "color": SEPARATOR_COLOR})
    unsold_components = []
    if unsold_items:
        unsold_components.extend([{"type": "separator", "margin": "xl", "color": SEPARATOR_COLOR}, {"type": "text", "text": "NGÀNH HÀNG CHƯA CÓ SỐ:", "color": "#888888", "size": "xs", "align": "center", "margin": "lg", "weight": "bold"}])
        num_columns = 3
        columns = [[] for _ in range(num_columns)]
        for i, item in enumerate(unsold_items):
            columns[i % num_columns].append({"type": "text", "text": f"• {item['name']}", "color": TEXT_COLOR, "size": "xxs", "margin": "sm", "wrap": True})
        column_boxes = [{"type": "box", "layout": "vertical", "flex": 1, "contents": col} for col in columns]
        unsold_components.append({"type": "box", "layout": "horizontal", "margin": "md", "spacing": "md", "contents": column_boxes})
    percent_color = "#00B33C" if percent_float >= 1 else ("#FFC400" if percent_float > 0.7 else "#FF3B30")
    flex_json = {"type": "flex", "altText": f"Báo cáo cho {ten_sieu_thi_rut_gon}", "contents": { "type": "bubble", "size": "mega", "header": { "type": "box", "layout": "vertical", "paddingAll": "20px", "backgroundColor": style["bg"], "contents": [ {"type": "text", "text": "Báo cáo Realtime", "color": style["text"], "size": "md", "align": "center", "weight": "bold"}, {"type": "text", "text": f"🏪 {ten_sieu_thi_rut_gon.upper()}", "color": style["text"], "weight": "bold", "size": "lg", "align": "center", "margin": "md", "wrap": True}, {"type": "box", "layout": "vertical", "margin": "lg", "spacing": "sm", "contents": [ {"type": "text", "text": f"⭐ Cụm: {cum}", "size": "xs", "color": style["text"]}, {"type": "text", "text": f"🕒 Thời gian: {thoi_gian}", "size": "xs", "color": style["text"]}, {"type": "text", "text": f"🏆 NH thi đua đạt: {nh_thi_dua_dat}", "size": "xs", "color": style["text"]} ]} ] }, "body": { "type": "box", "layout": "vertical", "paddingAll": "20px", "backgroundColor": "#FFFFFF", "contents": [ {"type": "box", "layout": "horizontal", "contents": [ {"type": "box", "layout": "vertical", "flex": 1, "spacing": "sm", "contents": [ {"type": "text", "text": "💰 DOANH THU", "color": "#007BFF", "size": "sm", "align": "center", "weight":"bold"}, {"type": "text", "text": realtime_tong, "color": "#007BFF", "size": "xl", "weight": "bold", "align": "center"} ]}, {"type": "box", "layout": "vertical", "flex": 1, "spacing": "sm", "contents": [ {"type": "text", "text": "🎯 TARGET", "color": "#DC3545", "size": "sm", "align": "center", "weight":"bold"}, {"type": "text", "text": target_tong, "color": "#DC3545", "size": "xl", "weight": "bold", "align": "center"} ]} ]}, {"type": "text", "text": "% HOÀN THÀNH", "color": TEXT_COLOR, "size": "sm", "align": "center", "margin": "xl"}, {"type": "text", "text": percent_ht_tong, "color": percent_color, "size": "xxl", "weight": "bold", "align": "center"}, {"type": "box", "layout": "vertical", "backgroundColor": "#DDDDDD", "height": "8px", "cornerRadius": "md", "margin": "md", "contents": [ {"type": "box", "layout": "vertical", "backgroundColor": percent_color, "height": "8px", "cornerRadius": "md", "width": f"{min(100, round(percent_float * 100))}%"} ]}, {"type": "box", "layout": "horizontal", "margin": "xl", "contents": [{"type": "text", "text": "XH D.Thu Kênh", "size": "xs", "color": TEXT_COLOR, "align": "center", "flex": 1}]}, {"type": "box", "layout": "horizontal", "contents": [{"type": "text", "text": ranking, "weight": "bold", "size": "md", "color": TEXT_COLOR, "align": "center", "flex": 1}]}, {"type": "separator", "margin": "xl", "color": SEPARATOR_COLOR}, {"type": "box", "layout": "horizontal", "margin": "md", "contents": [{"type": "text", "text": "Ngành Hàng", "color": "#555555", "size": "xs", "flex": 4, "weight": "bold", "align": "center"}, {"type": "text", "text": "Realtime", "color": "#555555", "size": "xs", "flex": 2, "align": "center", "weight": "bold"}, {"type": "text", "text": "Target", "color": "#555555", "size": "xs", "flex": 2, "align": "center", "weight": "bold"}, {"type": "text", "text": "%HT", "color": "#555555", "size": "xs", "flex": 2, "align": "end", "weight": "bold"}]}, {"type": "separator", "margin": "md", "color": SEPARATOR_COLOR}, *sold_components, *unsold_components ] } }}
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
        header = {"type": "box", "layout": "vertical", "backgroundColor": header_bg_color, "paddingAll": "lg", "contents": [{"type": "text", "text": title, "weight": "bold", "size": "lg", "color": header_text_color, "align": "center", "wrap": True}]}
        body_bg_color, text_color_body, separator_color = "#FFFFFF", "#000000", "#EEEEEE"
        table_header = {"type": "box", "layout": "horizontal", "margin": "md", "paddingAll": "sm", "backgroundColor": header_bg_color, "cornerRadius": "md", "contents": [{"type": "text", "text": "STT", "weight": "bold", "size": "xs", "color": header_text_color, "flex": 1, "align": "center", "gravity":"center"},{"type": "separator", "color": separator_color},{"type": "text", "text": "KÊNH", "weight": "bold", "size": "xs", "color": header_text_color, "flex": 2, "align": "center", "gravity":"center"},{"type": "separator", "color": separator_color},{"type": "text", "text": "SIÊU THỊ", "weight": "bold", "size": "xs", "color": header_text_color, "flex": 6, "align": "center", "gravity":"center"},{"type": "separator", "color": separator_color},{"type": "text", "text": "RT", "weight": "bold", "size": "xs", "color": header_text_color, "flex": 2, "align": "center", "gravity":"center"}]}
        rows = [table_header, {"type": "separator", "margin": "sm", "color": separator_color}]
        for i, store in enumerate(stores):
            name_parts = store['sieu_thi'].split(' - ', 1)
            short_name = name_parts[1] if len(name_parts) > 1 else store['sieu_thi']
            row_component = {
                "type": "box", 
                "layout": "horizontal", 
                "margin": "md", 
                "paddingTop":"sm", 
                "paddingBottom":"sm", 
                "contents": [
                    {"type": "text", "text": str(i+1), "size": "xs", "color": text_color_body, "flex": 1, "gravity": "center", "align": "center"},
                    {"type": "separator", "color": separator_color},
                    {"type": "text", "text": store['kenh'], "size": "xs", "color": text_color_body, "flex": 2, "gravity": "center", "align": "center"},
                    {"type": "separator", "color": separator_color},
                    {"type": "text", "text": short_name, "size": "xxs", "color": text_color_body, "flex": 6, "wrap": True, "gravity": "center", "align": "center"},
                    {"type": "separator", "color": separator_color},
                    {"type": "text", "text": str(round(store['doanh_thu'])), "size": "xs", "color": text_color_body, "flex": 2, "align": "center", "gravity": "center"}
                ]
            }
            rows.append(row_component)
            if i < len(stores) -1: rows.append({"type": "separator", "margin": "sm", "color": separator_color})
        return {"type": "bubble", "size": "mega", "header": header, "body": { "type": "box", "layout": "vertical", "contents": rows, "paddingAll":"lg", "backgroundColor": body_bg_color }}
        
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

# --- ĐIỂM TIẾP NHẬN (ROUTES) ---

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

@app.route("/send_report", methods=['POST'])
def send_report():
    data = request.json
    if not data:
        return jsonify({"error": "No JSON data provided"}), 400
        
    cmd_id = data.get("cmdId")
    screenshot_url = data.get("screenshotUrl")
    
    if not cmd_id or not screenshot_url:
        return jsonify({"error": "Missing cmdId or screenshotUrl"}), 400
        
    # Tách source_id từ cmd_id
    parts = cmd_id.split("__")
    if len(parts) < 2:
        return jsonify({"error": "Invalid cmdId structure (missing source_id)"}), 400
        
    source_id = parts[1]
    cmd_type = cmd_id.split("_")[0]
    
    cmd_name = "Realtime"
    if cmd_type == "LK":
        cmd_name = "Lũy Kế"
    elif cmd_type == "NV0":
        cmd_name = "Nhân viên"
        
    try:
        img_msg = ImageSendMessage(
            original_content_url=screenshot_url,
            preview_image_url=screenshot_url
        )
        line_bot_api.push_message(source_id, [
            TextSendMessage(text=f"✅ Báo cáo {cmd_name} đã được đổ số mới thành công! Dưới đây là ảnh chụp báo cáo:"),
            img_msg
        ])
        print(f"Gửi thành công ảnh báo cáo {cmd_name} cho {source_id}")
        return jsonify({"status": "success"}), 200
    except Exception as e:
        error_msg = str(e)
        print(f"Lỗi khi gửi ảnh báo cáo LINE từ webhook: {error_msg}")
        
        # Ghi nhận lỗi trực tiếp lên Google Sheet để phục vụ việc debug
        try:
            sheet = CLIENT.open(SHEET_NAME).worksheet('bot_commands')
            records = sheet.get_all_values()
            for idx, row in enumerate(records):
                if row[0] == cmd_id:
                    sheet.update_cell(idx + 1, 3, "LINE_ERROR")
                    sheet.update_cell(idx + 1, 4, error_msg[:200])
                    break
        except Exception as sheet_err:
            print(f"Lỗi ghi log lên sheet: {sheet_err}")
            
        try:
            line_bot_api.push_message(source_id, TextSendMessage(text=f"❌ Lỗi gửi ảnh báo cáo {cmd_name} qua Line: {error_msg[:100]}"))
        except Exception as push_err:
            print(f"Lỗi khi gửi tin nhắn báo lỗi qua LINE: {push_err}")
            
        return jsonify({"error": error_msg}), 500

# --- XỬ LÝ SỰ KIỆN POSTBACK ---

@handler.add(PostbackEvent)
def handle_postback(event):
    data_str = event.postback.data
    data = dict(part.split('=') for part in data_str.split('&'))
    action = data.get('action')

    # 1. Gia hạn User
    if action == 'renew':
        target_id = data.get('id')
        duration_str = data.get('duration')
        
        delta, duration_text = parse_duration(duration_str)
        if not delta:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="Thời hạn gia hạn không hợp lệ."))
            return

        try:
            start_date = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh'))
            new_expiration_date = start_date + delta
            new_expiration_date_str = new_expiration_date.strftime('%Y-%m-%d')
            
            update_expiration_in_sheet(target_id, new_expiration_date_str)
            load_allowed_ids()

            reply_text = f"✅ Đã gia hạn thành công!\n- ID: {target_id}\n- Thêm: {duration_text}\n- Hạn mới: {new_expiration_date.strftime('%d-%m-%Y')}"
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))

        except Exception as e:
            print(f"Lỗi khi gia hạn: {e}")
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="Có lỗi xảy ra khi gia hạn."))
        return

    # 2. Hoàn thành Task công việc (Checklist Công việc)
    if action == 'complete_task':
        task_id = data.get('task_id')
        shift_type = data.get('shift')
        group_id = event.source.group_id
        user_id = event.source.user_id

        try:
            profile = line_bot_api.get_group_member_profile(group_id, user_id)
            user_name = profile.display_name
            
            sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_TRACKER_NAME)
            tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
            today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
            
            all_records = sheet.get_all_records()
            row_to_update = -1
            target_record = None
            
            for i, record in enumerate(all_records):
                if (str(record.get('group_id')) == group_id and
                    record.get('date') == today_str and
                    record.get('task_id') == task_id):
                    row_to_update = i + 2 
                    target_record = record
                    break
            
            if row_to_update != -1:
                range_to_update = f'F{row_to_update}:G{row_to_update}'
                sheet.update(range_name=range_to_update, values=[['complete', user_name]])
                
                if target_record:
                    target_record['status'] = 'complete'
                    target_record['user_name'] = user_name
            
            updated_flex_content = generate_checklist_flex(group_id, shift_type, all_records_prefetched=all_records)

            line_bot_api.reply_message(
                event.reply_token,
                FlexSendMessage(alt_text=f"Cập nhật checklist ca {shift_type}", contents=updated_flex_content)
            )

        except Exception as e:
            print(f"Lỗi nghiêm trọng khi xử lý postback hoàn thành công việc: {e}")
        return

    # 3. Check-in Ăn Sáng/Chiều
    if action == 'meal_checkin':
        session_type = data.get('session')
        staff_name = data.get('name')
        group_id = getattr(event.source, 'group_id', None)
        
        if not group_id: return

        # === LẤY TÊN NGƯỜI BẤM (NICK LINE) ===
        try:
            user_id = event.source.user_id
            profile = line_bot_api.get_group_member_profile(group_id, user_id)
            clicker_name = profile.display_name
        except:
            try:
                profile = line_bot_api.get_profile(user_id)
                clicker_name = profile.display_name
            except:
                clicker_name = "Unknown"

        success, time_str = update_meal_status(group_id, session_type, staff_name, clicker_name)
        
        if success:
            updated_flex = generate_meal_flex(group_id, session_type)
            if updated_flex:
                line_bot_api.reply_message(
                    event.reply_token,
                    FlexSendMessage(alt_text=f"Checklist ăn {session_type} updated", contents=updated_flex)
                )
        else:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❌ Lỗi: Không tìm thấy tên hoặc lỗi cập nhật."))
        return

# --- XỬ LÝ TIN NHẮN ---

@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    user_message = event.message.text.strip()
    user_msg_upper = user_message.upper()
    user_id = event.source.user_id
    source_id = getattr(event.source, 'group_id', user_id)
    
    # 1. Admin ADD
    if user_msg_upper.startswith('ADD '):
        if user_id != ADMIN_USER_ID:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="Bạn không có quyền thực hiện lệnh này."))
            return

        parts = user_message.split()
        if len(parts) != 3:
            reply = "Sai cú pháp. Sử dụng: add [ID] [thời hạn]\nVí dụ:\n- `add U... 3d` (3 ngày)\n- `add C... 1m` (1 tháng)\n- `add U... 0` (vĩnh viễn)"
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))
            return
            
        target_id = parts[1]
        duration_str = parts[2]
        
        delta, duration_text = parse_duration(duration_str)
        if not delta:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="Thời hạn không hợp lệ."))
            return

        try:
            if duration_str == '0':
                expiration_date_str = '9999-12-31'
                reply_duration = "vĩnh viễn"
            else:
                start_date = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh'))
                expiration_date = start_date + delta
                expiration_date_str = expiration_date.strftime('%Y-%m-%d')
                reply_duration = f"{duration_text} (hết hạn ngày {expiration_date.strftime('%d-%m-%Y')})"
        
            action_text = update_expiration_in_sheet(target_id, expiration_date_str)
            load_allowed_ids()
            
            reply_text = f"✅ {action_text} thành công!\n- ID: {target_id}\n- Thời hạn: {reply_duration}"
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))
            
        except Exception as e:
            print(f"Lỗi khi cập nhật Google Sheet: {e}")
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"Có lỗi xảy ra khi {action_text.lower()} ID."))
        return

    # 2. Check quyền
    is_controlled_environment = bool(allowed_ids_cache) and ADMIN_USER_ID
    if is_controlled_environment and source_id not in allowed_ids_cache:
        public_commands = ['ID', 'MENU BOT']
        if user_msg_upper not in public_commands and user_id != ADMIN_USER_ID:
            print(f"Bỏ qua tin nhắn từ ID không được phép: {source_id}")
            return

    # 3. ID
    if user_msg_upper == 'ID':
        reply_text = f'👤 User ID:\n{user_id}'
        if hasattr(event.source, 'group_id'):
            reply_text = f'👥 Group ID:\n{source_id}\n\n' + reply_text
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))
        return

    # 4. MENU
    if user_msg_upper == 'MENU BOT':
        menu_text = (
            "🤖 **MENU HƯỚNG DẪN BOT** 🤖\n"
            "----------------------------------\n"
            "**🍱 ĐIỂM DANH ĂN:**\n"
            "• `ăn` - Tự động hiển thị (Trưa <15h, Tối >=15h).\n"
            "• `ansang` / `anchieu` - Thủ công.\n"
            "\n"
            "**✅ CHECKLIST CÔNG VIỆC:**\n"
            "• `sang` - Checklist sáng.\n"
            "• `chieu` - Checklist chiều.\n"
            "\n"
            "**📅 LỊCH LÀM VIỆC:**\n"
            "• `nv` / `pg` - Lịch hôm nay.\n"
            "• `nv2`..`nv8` - Lịch NV theo thứ.\n"
            "\n"
            "**📊 BÁO CÁO REALTIME:**\n"
            "• `ST [Mã ST]` - Báo cáo chi tiết.\n"
            "• `bxh` - Top 20.\n"
            "\n"
            "**🔄 ĐỔ SỐ TỰ ĐỘNG (AUTO):**\n"
            "• `RT` - Đổ số Realtime & Gửi ảnh báo cáo.\n"
            "• `LK` - Đổ số Lũy kế & Gửi ảnh báo cáo.\n"
            "• `NV0` - Đổ số Nhân viên & Gửi ảnh báo cáo."
        )
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=menu_text))
        return

    # === LỆNH ĐỔ SỐ TỰ ĐỘNG (AUTO SYNC) ===
    if user_msg_upper in ['RT', 'LK', 'NV0']:
        handle_auto_sync_command(event, user_msg_upper)
        return

    # === 5. XỬ LÝ LỆNH ĂN UỐNG ===
    cmd_normalized = user_message.lower().replace(" ", "")
    meal_cmds = ['ansang', 'anchieu', 'an', 'ăn']
    
    if cmd_normalized in meal_cmds:
        if not hasattr(event.source, 'group_id'):
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="⚠️ Lệnh này chỉ hoạt động trong nhóm chat."))
            return

        session_type = None
        if cmd_normalized == 'ansang': session_type = 'ansang'
        elif cmd_normalized == 'anchieu': session_type = 'anchieu'
        elif cmd_normalized in ['an', 'ăn']:
            tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
            current_hour = datetime.now(tz_vietnam).hour
            session_type = 'ansang' if current_hour < 15 else 'anchieu'

        if session_type:
            try:
                flex_content = generate_meal_flex(source_id, session_type)
                if flex_content:
                    alt = "Check list ăn trưa" if session_type == 'ansang' else "Check list ăn tối"
                    line_bot_api.reply_message(event.reply_token, FlexSendMessage(alt_text=alt, contents=flex_content))
                else:
                    line_bot_api.reply_message(event.reply_token, TextSendMessage(text="⚠️ Không tìm thấy dữ liệu lịch hoặc toàn bộ nhân sự đều OFF."))
            except Exception as e:
                print(f"Lỗi tạo meal flex: {e}")
                # Không push lỗi ra group
        return

    # 6. Checklist công việc (Sang/Chieu)
    if user_msg_upper in ['SANG', 'CHIEU']:
        shift_type = 'sang' if user_msg_upper == 'SANG' else 'chieu'
        group_id = getattr(event.source, 'group_id', None)
        
        if not group_id:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="Lệnh này chỉ hoạt động trong nhóm chat."))
            return
        try:
            initialize_daily_tasks(group_id, shift_type)
            flex_content = generate_checklist_flex(group_id, shift_type)
            
            if flex_content:
                message = FlexSendMessage(alt_text=f"Checklist công việc ca {shift_type}", contents=flex_content)
                line_bot_api.reply_message(event.reply_token, message)
            else:
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"Không thể tạo checklist cho ca {shift_type}."))

        except Exception as e:
            print(f"Lỗi khi xử lý lệnh checklist '{shift_type}': {e}")
        return

    # 7. Lịch làm việc (NV/PG)
    schedule_match = re.match(r"^(NV|PG)([2-8])$", user_msg_upper)
    if schedule_match:
        schedule_type_cmd = schedule_match.group(1)
        day_number = int(schedule_match.group(2))
        schedule_type = 'employee' if schedule_type_cmd == 'NV' else 'pg'
        days_map = {2: "Thứ Hai", 3: "Thứ Ba", 4: "Thứ Tư", 5: "Thứ Năm", 6: "Thứ Sáu", 7: "Thứ Bảy", 8: "Chủ Nhật"}
        day_str = days_map.get(day_number)
        try:
            # Gửi lịch với reply_token
            send_daily_schedule(schedule_type, reply_token=event.reply_token, day_of_week_str=day_str)
        except Exception as e:
            print(f"Error schedule: {e}")
        return

    if user_msg_upper in ['NV', 'PG']:
        schedule_type = 'employee' if user_msg_upper == 'NV' else 'pg'
        try:
            # Gửi lịch với reply_token
            send_daily_schedule(schedule_type, reply_token=event.reply_token)
        except Exception as e:
            print(f"Error schedule: {e}")
        return
        
    # 8. Báo cáo (ST, BXH)
    try:
        sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_NAME)
        all_data = sheet.get_all_values()
        reply_messages = []
        cluster_names = {row[0].strip().upper() for row in all_data[1:] if len(row) > 0 and row[0]}
        header_row = all_data[0]
        
        if user_msg_upper.startswith('ST '):
            supermarket_code = user_message[3:].strip().upper()
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
        
        else:
            parts = user_message.split()
            if len(parts) == 2 and parts[0].upper() in cluster_names:
                cluster_name_cmd = parts[0].upper()
                channel_choice = parts[1]
                channel_filter = None
                if channel_choice == '1':
                    channel_filter = 'dmx'
                elif channel_choice == '2':
                    channel_filter = 'tgdd'
                
                if channel_filter:
                    bxh_messages = create_leaderboard_flex_message(all_data, cluster_name=cluster_name_cmd, channel_filter=channel_filter)
                    if not bxh_messages:
                         reply_messages.append(TextSendMessage(text=f"Không có dữ liệu cho kênh bạn chọn trong cụm {cluster_name_cmd}."))
                    else:
                        for flex_data in bxh_messages:
                            reply_messages.append(FlexSendMessage(alt_text=flex_data['altText'], contents=flex_data['contents']))

            elif user_msg_upper in cluster_names:
                for flex_data in create_leaderboard_flex_message(all_data, cluster_name=user_msg_upper):
                    reply_messages.append(FlexSendMessage(alt_text=flex_data['altText'], contents=flex_data['contents']))
            
            else:
                found_row = next((row for row in all_data[1:] if row and len(row) > 2 and row[2] and row[2].strip().split(' ')[0] == user_msg_upper), None)
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
        print(f"!!! GẶP LỖI NGHIÊM TRỌNG KHI XỬ LÝ BÁO CÁO: {repr(e)}")

# --- ENDPOINTS CRON JOB (ĐÃ CẬP NHẬT GOM TIN) ---

@app.route("/trigger-morning-tasks", methods=['POST'])
def trigger_morning_tasks():
    incoming_secret = request.headers.get('X-Cron-Secret')
    if not CRON_SECRET_KEY or incoming_secret != CRON_SECRET_KEY:
        abort(403)
    
    print("Cron Job: Bắt đầu tác vụ buổi sáng (GOM TIN)...")
    try:
        pg_group_id = os.environ.get('PG_GROUP_ID')
        employee_group_id = os.environ.get('EMPLOYEE_GROUP_ID')
        
        # Danh sách tin nhắn để gửi gộp
        messages_to_send = []

        # 1. Lấy Lịch PG (Nếu cần)
        # Nếu PG và Employee chung group, chúng ta sẽ gửi hết vào employee_group_id
        if pg_group_id:
            # Nếu PG Group ID khác Employee Group ID, phải gửi riêng (hoặc chấp nhận tốn thêm 1 tin)
            # Ở đây tôi ưu tiên gom nếu trùng hoặc logic của bạn cho phép
            msg_pg = send_daily_schedule('pg', return_msg_only=True)
            if msg_pg: messages_to_send.append(msg_pg)

        # 2. Lấy Lịch NV
        if employee_group_id:
            msg_nv = send_daily_schedule('employee', return_msg_only=True)
            if msg_nv: messages_to_send.append(msg_nv)

        # 3. Lấy Checklist Sáng
        if employee_group_id:
            msg_check = get_checklist_message('sang', employee_group_id)
            if msg_check: messages_to_send.append(msg_check)
        
        # --- GỬI TIN NHẮN (BATCHING) ---
        if messages_to_send and employee_group_id:
            # Gửi tối đa 5 bong bóng trong 1 lần push -> Chỉ tính 1 tin nhắn
            line_bot_api.push_message(employee_group_id, messages_to_send[:5])
            print(f"Đã gửi gộp {len(messages_to_send)} thông báo sáng.")
        else:
            print("Không có nội dung nào để gửi sáng nay.")

        return "OK", 200
    except Exception as e:
        print(f"Lỗi khi chạy tác vụ buổi sáng: {e}")
        return "Error", 500

@app.route("/trigger-afternoon-tasks", methods=['POST'])
def trigger_afternoon_tasks():
    incoming_secret = request.headers.get('X-Cron-Secret')
    if not CRON_SECRET_KEY or incoming_secret != CRON_SECRET_KEY:
        abort(403)
    
    print("Cron Job: Bắt đầu tác vụ buổi chiều (GOM TIN)...")
    try:
        employee_group_id = os.environ.get('EMPLOYEE_GROUP_ID')
        
        messages_to_send = []

        # 1. Lịch NV (Nếu muốn nhắc lại chiều)
        if employee_group_id:
            msg_nv = send_daily_schedule('employee', return_msg_only=True)
            if msg_nv: messages_to_send.append(msg_nv)

        # 2. Checklist Chiều
        if employee_group_id:
            msg_check = get_checklist_message('chieu', employee_group_id)
            if msg_check: messages_to_send.append(msg_check)
        
        # --- GỬI TIN NHẮN (BATCHING) ---
        if messages_to_send and employee_group_id:
            line_bot_api.push_message(employee_group_id, messages_to_send[:5])
            print(f"Đã gửi gộp {len(messages_to_send)} thông báo chiều.")
        else:
            print("Không có nội dung nào để gửi chiều nay.")

        return "OK", 200
    except Exception as e:
        print(f"Lỗi khi chạy tác vụ buổi chiều: {e}")
        return "Error", 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)