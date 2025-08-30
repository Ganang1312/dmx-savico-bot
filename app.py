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
    MessageEvent, TextMessage, TextSendMessage, FlexSendMessage, PostbackEvent
)
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler

# --- IMPORT TỪ CÁC FILE KHÁC CỦA BẠN ---
# Thay đổi quan trọng: Import cấu hình từ file config.py
from config import CLIENT, SHEET_NAME, WORKSHEET_NAME_USERS, WORKSHEET_TRACKER_NAME
from flex_handler import generate_checklist_flex, initialize_daily_tasks
from checklist_scheduler import send_initial_checklist

# --- PHẦN CẤU HÌNH: ĐỌC TỪ BIẾN MÔI TRƯỜNG ---
CHANNEL_ACCESS_TOKEN = os.environ.get('CHANNEL_ACCESS_TOKEN')
CHANNEL_SECRET = os.environ.get('CHANNEL_SECRET')
ADMIN_USER_ID = os.environ.get('ADMIN_USER_ID')
CRON_SECRET_KEY = os.environ.get('CRON_SECRET_KEY') # Chìa khóa bí mật cho webhook

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
            requests.get(ping_url + "/ping", timeout=10)
        except requests.exceptions.RequestException as e:
            print(f"Lỗi khi ping: {e}")
        time.sleep(600) # Ping mỗi 10 phút

# --- LOGIC NHẮC NHỞ TỰ ĐỘNG (SCHEDULER) ---
def reminder_job():
    print("Scheduler: Đang chạy kiểm tra nhắc nhở...")
    try:
        sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_TRACKER_NAME)
        df = pd.DataFrame(sheet.get_all_records())
        
        if df.empty:
            return

        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        now = datetime.now(tz_vietnam)
        today_str = now.strftime('%Y-%m-%d')

        overdue_tasks = df[(df['date'] == today_str) & (df['status'] == 'incomplete')]

        for index, task in overdue_tasks.iterrows():
            deadline_time = datetime.strptime(str(task['deadline']), '%H:%M').time()
            deadline_dt = now.replace(hour=deadline_time.hour, minute=deadline_time.minute, second=0, microsecond=0)
            
            if now > deadline_dt:
                last_reminded_str = task.get('last_reminded')
                should_remind = False
                
                if not last_reminded_str or last_reminded_str == '':
                    should_remind = True
                else:
                    last_reminded_dt = datetime.fromisoformat(last_reminded_str)
                    if (now - last_reminded_dt).total_seconds() > 600: # Nhắc lại sau 10 phút
                        should_remind = True
                
                if should_remind:
                    group_id = str(task['group_id'])
                    reminder_text = f"🚨 NHẮC NHỞ: Công việc '{task['task_name']}' đã quá hạn lúc {task['deadline']}!"
                    print(f"Gửi nhắc nhở cho task: {task['task_name']} đến group: {group_id}")
                    
                    line_bot_api.push_message(group_id, TextSendMessage(text=reminder_text))
                    
                    cell_list = sheet.findall(str(task['task_id']))
                    for cell in cell_list:
                        row_values = sheet.row_values(cell.row)
                        if str(row_values[0]) == group_id and row_values[1] == today_str:
                             sheet.update_cell(cell.row, 7, now.isoformat())
                             break
    except Exception as e:
        print(f"Lỗi trong reminder_job: {e}")

# --- CÁC HÀM XỬ LÝ DỮ LIỆU BÁO CÁO (TỪ FILE GỐC CỦA BẠN) ---
def parse_float_from_string(s):
    # ... (code của bạn)
    pass

def handle_percentage_string(percent_str):
    # ... (code của bạn)
    pass
# ... (Và các hàm xử lý báo cáo khác: parse_competition_data, format_currency, etc.)
# <<< DÁN CÁC HÀM XỬ LÝ DỮ LIỆU CỦA BẠN VÀO ĐÂY >>>

# --- KHỞI ĐỘNG CÁC TÁC VỤ NỀN ---
load_allowed_ids()
if 'RENDER' in os.environ:
    keep_alive_thread = threading.Thread(target=keep_alive, daemon=True)
    keep_alive_thread.start()

scheduler = BackgroundScheduler(daemon=True, timezone='Asia/Ho_Chi_Minh')
scheduler.add_job(reminder_job, 'interval', minutes=5)
scheduler.start()

# --- ĐIỂM TIẾP NHẬN (ROUTES) ---
@app.route("/trigger-checklist", methods=['POST'])
def trigger_checklist():
    incoming_secret = request.headers.get('X-Cron-Secret')
    if not CRON_SECRET_KEY or incoming_secret != CRON_SECRET_KEY:
        print("Lỗi bảo mật: Sai hoặc thiếu CRON_SECRET_KEY.")
        abort(403)

    data = request.get_json()
    shift = data.get('shift')

    if shift not in ['sang', 'chieu']:
        return "Lỗi: 'shift' phải là 'sang' hoặc 'chieu'.", 400

    try:
        thread = threading.Thread(target=send_initial_checklist, args=(shift,))
        thread.start()
        print(f"Đã kích hoạt thành công checklist cho ca: {shift}")
        return f"OK, đã kích hoạt checklist ca {shift}.", 200
    except Exception as e:
        print(f"Lỗi khi kích hoạt checklist: {e}")
        return f"Lỗi máy chủ: {e}", 500

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

# --- BỘ XỬ LÝ SỰ KIỆN (HANDLERS) ---
@handler.add(PostbackEvent)
def handle_postback(event):
    data = event.postback.data
    params = dict(x.split('=') for x in data.split('&'))
    action = params.get('action')
    group_id = event.source.group_id if event.source.type == 'group' else event.source.user_id
    
    if action == 'complete_task':
        task_id = params.get('task_id')
        shift_type = params.get('shift')
        print(f"Nhận yêu cầu hoàn tất task: {task_id} từ group: {group_id}")
        try:
            sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_TRACKER_NAME)
            tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
            today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
            
            cell_list = sheet.findall(task_id)
            target_row = -1
            for cell in cell_list:
                 row_values = sheet.row_values(cell.row)
                 if str(row_values[0]) == group_id and row_values[1] == today_str:
                     target_row = cell.row
                     break
            
            if target_row != -1:
                sheet.update_cell(target_row, 6, 'complete')
                print(f"Đã cập nhật task {task_id} thành 'complete' trong Google Sheet.")
            else:
                 print(f"Không tìm thấy task {task_id} để cập nhật.")

            new_flex_message = generate_checklist_flex(group_id, shift_type)
            if new_flex_message:
                line_bot_api.push_message(
                    group_id,
                    FlexSendMessage(alt_text=f"Cập nhật checklist ca {shift_type}", contents=new_flex_message)
                )
        except Exception as e:
            print(f"Lỗi khi xử lý postback: {e}")

@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    user_message = event.message.text.strip()
    user_msg_upper = user_message.upper()
    user_id = event.source.user_id
    source_id = event.source.group_id if event.source.type == 'group' else user_id

    if user_msg_upper == 'ID':
        # ... (logic lấy ID của bạn)
        return

    if ADMIN_USER_ID and user_id == ADMIN_USER_ID:
        # ... (logic các lệnh admin của bạn)
        return
            
    # --- XỬ LÝ LỆNH TEST VÀ RESET CHECKLIST ---
    shift_to_process = None
    if user_msg_upper in ['TEST SANG', 'RESET SANG']:
        shift_to_process = 'sang'
    elif user_msg_upper in ['TEST CHIEU', 'RESET CHIEU']:
        shift_to_process = 'chieu'

    if shift_to_process:
        print(f"Nhận lệnh '{user_msg_upper}' từ group {source_id}")
        try:
            initialize_daily_tasks(source_id, shift_to_process)
            flex_message = generate_checklist_flex(source_id, shift_to_process)
            
            if flex_message:
                line_bot_api.reply_message(
                    event.reply_token,
                    FlexSendMessage(alt_text=f"Checklist ca {shift_to_process}", contents=flex_message)
                )
            else:
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text="Không thể tạo checklist."))
        except Exception as e:
            print(f"Lỗi khi thực thi lệnh test/reset: {e}")
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"Gặp lỗi: {e}"))
        return

    # --- KIỂM TRA QUYỀN TRUY CẬP CHO BÁO CÁO REALTIME ---
    is_controlled = bool(allowed_ids_cache) and ADMIN_USER_ID
    if is_controlled and source_id not in allowed_ids_cache:
        return

    # --- XỬ LÝ LOGIC BÁO CÁO REALTIME (CODE GỐC CỦA BẠN) ---
    try:
        # sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_NAME) # Đã có trong config.py
        # all_data = sheet.get_all_values()
        # ... (toàn bộ logic xử lý báo cáo realtime của bạn)
        reply_messages = []
        # ... (thêm tin nhắn vào reply_messages)
        if reply_messages:
            line_bot_api.reply_message(event.reply_token, reply_messages)

    except Exception as e:
        print(f"!!! GẶP LỖI NGHIÊM TRỌNG (BÁO CÁO): {repr(e)}")
        # line_bot_api.reply_message(event.reply_token, TextSendMessage(text='Đã có lỗi xảy ra khi truy vấn dữ liệu.'))

# --- CHẠY ỨNG DỤNG ---
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
