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

from flask import Flask, request, abort
from linebot import (
    LineBotApi, WebhookHandler
)
from linebot.exceptions import InvalidSignatureError
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage, FlexSendMessage, PostbackEvent
)
import pandas as pd

# --- IMPORT TỪ CÁC FILE KHÁC ---
from config import CLIENT, SHEET_NAME, WORKSHEET_NAME_USERS, WORKSHEET_NAME, WORKSHEET_TRACKER_NAME
from schedule_handler import send_daily_schedule
from flex_handler import initialize_daily_tasks, generate_checklist_flex
from checklist_scheduler import send_initial_checklist
# === IMPORT MỚI ===
from meal_handler import generate_meal_flex, update_meal_status

# --- PHẦN CẤU HÌNH ---
CHANNEL_ACCESS_TOKEN = os.environ.get('CHANNEL_ACCESS_TOKEN')
CHANNEL_SECRET = os.environ.get('CHANNEL_SECRET')
ADMIN_USER_ID = os.environ.get('ADMIN_USER_ID')
CRON_SECRET_KEY = os.environ.get('CRON_SECRET_KEY')

if not all([CHANNEL_ACCESS_TOKEN, CHANNEL_SECRET, ADMIN_USER_ID]):
    raise ValueError("Lỗi: Hãy kiểm tra lại các biến môi trường trên Render.")

allowed_ids_cache = set()
app = Flask(__name__)
line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)

# --- CÁC HÀM TIỆN ÍCH (Giữ nguyên) ---
def load_allowed_ids():
    global allowed_ids_cache
    try:
        print("Đang tải danh sách ID được phép...")
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
    return None, None

# --- CÁC HÀM XỬ LÝ DỮ LIỆU BÁO CÁO (Giữ nguyên như file cũ của bạn) ---
# ... (Phần logic báo cáo ST, BXH không thay đổi, tôi lược bớt để tập trung vào phần mới) ...
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

# (Giữ nguyên các hàm parse_competition_data, format_currency, calculate_ranking, 
# create_flex_message, create_summary_text_message, create_leaderboard_flex_message như cũ)
# Tôi sẽ không paste lại toàn bộ phần Báo cáo ở đây để code ngắn gọn, 
# bạn hãy giữ nguyên phần Báo cáo từ file cũ.

# --- KHỞI ĐỘNG ---
load_allowed_ids()
if 'RENDER' in os.environ:
    threading.Thread(target=keep_alive, daemon=True).start()

# --- ROUTES ---
@app.route("/callback", methods=['POST'])
def callback():
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)
    try: handler.handle(body, signature)
    except InvalidSignatureError: abort(400)
    return 'OK'

@app.route("/ping")
def ping(): return "OK", 200

# --- SỰ KIỆN POSTBACK (NÚT BẤM) ---
@handler.add(PostbackEvent)
def handle_postback(event):
    data_str = event.postback.data
    data = dict(part.split('=') for part in data_str.split('&'))
    action = data.get('action')

    # 1. Gia hạn User
    if action == 'renew':
        # ... (giữ nguyên logic renew) ...
        target_id = data.get('id')
        duration_str = data.get('duration')
        delta, duration_text = parse_duration(duration_str)
        if not delta: return
        try:
            new_date = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')) + delta
            update_expiration_in_sheet(target_id, new_date.strftime('%Y-%m-%d'))
            load_allowed_ids()
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"✅ Gia hạn thành công đến {new_date.strftime('%d-%m-%Y')}"))
        except: line_bot_api.reply_message(event.reply_token, TextSendMessage(text="Lỗi gia hạn."))
        return

    # 2. Hoàn thành Task công việc (Sang/Chieu)
    if action == 'complete_task':
        # ... (giữ nguyên logic complete_task) ...
        task_id = data.get('task_id')
        shift_type = data.get('shift')
        group_id = event.source.group_id
        user_id = event.source.user_id
        try:
            profile = line_bot_api.get_group_member_profile(group_id, user_id)
            sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_TRACKER_NAME)
            tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
            today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
            all_records = sheet.get_all_records()
            row_to_update = -1
            for i, record in enumerate(all_records):
                if (str(record.get('group_id')) == group_id and record.get('date') == today_str and record.get('task_id') == task_id):
                    row_to_update = i + 2
                    break
            if row_to_update != -1:
                sheet.update(range_name=f'F{row_to_update}:G{row_to_update}', values=[['complete', profile.display_name]])
            updated_flex = generate_checklist_flex(group_id, shift_type)
            line_bot_api.reply_message(event.reply_token, FlexSendMessage(alt_text="Updated Checklist", contents=updated_flex))
        except Exception as e: print(f"Error task: {e}")
        return

    # === 3. XỬ LÝ CHECK-IN ĂN SÁNG/CHIỀU (MỚI) ===
    if action == 'meal_checkin':
        session_type = data.get('session')
        staff_name = data.get('name')
        group_id = getattr(event.source, 'group_id', None)
        
        if not group_id: return

        # Update Google Sheet
        success, time_str = update_meal_status(group_id, session_type, staff_name)
        
        if success:
            # Vẽ lại Flex Message để hiển thị giờ
            updated_flex = generate_meal_flex(group_id, session_type)
            if updated_flex:
                line_bot_api.reply_message(
                    event.reply_token,
                    FlexSendMessage(alt_text=f"Checklist ăn {session_type} updated", contents=updated_flex)
                )
        else:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❌ Lỗi: Không tìm thấy tên hoặc lỗi cập nhật."))
        return

# --- SỰ KIỆN TIN NHẮN TEXT ---
@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    user_message = event.message.text.strip()
    user_msg_upper = user_message.upper()
    user_id = event.source.user_id
    source_id = getattr(event.source, 'group_id', user_id)
    
    # 1. Logic ADD User (Admin)
    if user_msg_upper.startswith('ADD '):
        if user_id != ADMIN_USER_ID: return
        parts = user_message.split()
        if len(parts) != 3:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="Sai cú pháp: add [ID] [hạn]"))
            return
        target_id, duration_str = parts[1], parts[2]
        delta, duration_text = parse_duration(duration_str)
        if not delta: return
        try:
            exp_str = '9999-12-31' if duration_str == '0' else (datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')) + delta).strftime('%Y-%m-%d')
            res = update_expiration_in_sheet(target_id, exp_str)
            load_allowed_ids()
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"✅ {res} thành công."))
        except: line_bot_api.reply_message(event.reply_token, TextSendMessage(text="Lỗi update sheet."))
        return

    # 2. Check quyền truy cập
    is_controlled = bool(allowed_ids_cache) and ADMIN_USER_ID
    if is_controlled and source_id not in allowed_ids_cache:
        if user_msg_upper not in ['ID', 'MENU BOT'] and user_id != ADMIN_USER_ID: return

    # 3. Các lệnh cơ bản
    if user_msg_upper == 'ID':
        reply = f'User: {user_id}\nGroup: {source_id}' if hasattr(event.source, 'group_id') else f'User: {user_id}'
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))
        return

    if user_msg_upper == 'MENU BOT':
        menu = "🤖 MENU\n- `ansang`/`anchieu`: Điểm danh ăn.\n- `sang`/`chieu`: Checklist công việc.\n- `nv`/`pg`: Xem lịch.\n- `st [mã]`: Báo cáo ST."
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=menu))
        return

    # === 4. LỆNH ĂN SÁNG / ĂN CHIỀU (MỚI) ===
    if user_msg_upper in ['ANSANG', 'ANCHIEU']:
        if not hasattr(event.source, 'group_id'):
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="⚠️ Lệnh này chỉ hoạt động trong nhóm."))
            return

        session_type = user_msg_upper.lower() # 'ansang' hoặc 'anchieu'
        
        try:
            flex_content = generate_meal_flex(source_id, session_type)
            if flex_content:
                alt = "Danh sách ăn sáng" if session_type == 'ansang' else "Danh sách ăn chiều"
                line_bot_api.reply_message(event.reply_token, FlexSendMessage(alt_text=alt, contents=flex_content))
            else:
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text="⚠️ Không tìm thấy lịch làm việc hoặc tất cả nhân sự đều nghỉ ca này."))
        except Exception as e:
            print(f"Lỗi ansang/anchieu: {e}")
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❌ Lỗi hệ thống khi tạo danh sách."))
        return

    # 5. Các lệnh cũ (Checklist công việc, Lịch làm việc, Báo cáo)
    # ... (Giữ nguyên logic SANG, CHIEU, NV, PG, ST, BXH từ code cũ của bạn) ...
    # Để code ngắn gọn tôi sẽ thay thế bằng comment này, bạn hãy paste lại đoạn logic cũ vào đây.
    # Logic checklist công việc:
    if user_msg_upper in ['SANG', 'CHIEU']:
        shift = 'sang' if user_msg_upper == 'SANG' else 'chieu'
        if not hasattr(event.source, 'group_id'): return
        initialize_daily_tasks(source_id, shift)
        flex = generate_checklist_flex(source_id, shift)
        line_bot_api.reply_message(event.reply_token, FlexSendMessage(alt_text=f"Checklist {shift}", contents=flex))
        return
        
    # Logic Lịch làm việc:
    if user_msg_upper in ['NV', 'PG'] or re.match(r"^(NV|PG)([2-8])$", user_msg_upper):
        target_type = 'pg' if 'PG' in user_msg_upper else 'employee'
        day_match = re.search(r"\d", user_msg_upper)
        day_str = None
        if day_match:
            d_map = {2:"Thứ Hai", 3:"Thứ Ba", 4:"Thứ Tư", 5:"Thứ Năm", 6:"Thứ Sáu", 7:"Thứ Bảy", 8:"Chủ Nhật"}
            day_str = d_map.get(int(day_match.group(0)))
        send_daily_schedule(target_type, source_id, event.reply_token, day_of_week_str=day_str)
        return

    # Logic Báo cáo (Cần import các hàm xử lý báo cáo đầy đủ ở trên):
    # (Bạn giữ nguyên logic xử lý ST..., BXH... ở cuối hàm này)

# --- ROUTES CRON ---
@app.route("/trigger-morning-tasks", methods=['POST'])
def trigger_morning():
    if request.headers.get('X-Cron-Secret') != CRON_SECRET_KEY: abort(403)
    try:
        if os.environ.get('PG_GROUP_ID'): send_daily_schedule('pg', os.environ.get('PG_GROUP_ID'))
        if os.environ.get('EMPLOYEE_GROUP_ID'): send_daily_schedule('employee', os.environ.get('EMPLOYEE_GROUP_ID'))
        send_initial_checklist('sang')
        return "OK", 200
    except: return "Error", 500

@app.route("/trigger-afternoon-tasks", methods=['POST'])
def trigger_afternoon():
    if request.headers.get('X-Cron-Secret') != CRON_SECRET_KEY: abort(403)
    try:
        if os.environ.get('EMPLOYEE_GROUP_ID'): send_daily_schedule('employee', os.environ.get('EMPLOYEE_GROUP_ID'))
        send_initial_checklist('chieu')
        return "OK", 200
    except: return "Error", 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(os.environ.get("PORT", 5000)))