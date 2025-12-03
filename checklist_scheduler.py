# checklist_scheduler.py

import os
import sys
from linebot import LineBotApi
from linebot.models import FlexSendMessage

# Import các hàm cần thiết từ flex_handler
from flex_handler import initialize_daily_tasks, generate_checklist_flex

# --- Cấu hình ---
CHANNEL_ACCESS_TOKEN = os.environ.get('CHANNEL_ACCESS_TOKEN')
CHECKLIST_GROUP_ID = os.environ.get('CHECKLIST_GROUP_ID')

if CHANNEL_ACCESS_TOKEN:
    line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)

def get_checklist_message(shift_type, group_id):
    """
    Hàm mới: Chỉ khởi tạo công việc và TRẢ VỀ đối tượng tin nhắn (Message Object).
    KHÔNG thực hiện gửi tin nhắn. Dùng để gom tin nhắn (Batching).
    """
    try:
        if not group_id:
            print("Lỗi: Không có Group ID để tạo checklist.")
            return None

        # 1. Reset và khởi tạo công việc trong Google Sheet
        initialize_daily_tasks(group_id, shift_type)

        # 2. Tạo nội dung Flex
        flex_content = generate_checklist_flex(group_id, shift_type)
        if flex_content:
            return FlexSendMessage(
                alt_text=f"Checklist công việc ca {shift_type}", 
                contents=flex_content
            )
        return None
    except Exception as e:
        # Chỉ in lỗi ra log server, không gửi tin nhắn báo lỗi
        print(f"Lỗi tạo checklist message ca {shift_type}: {e}")
        return None

def send_initial_checklist(shift_type):
    """
    Hàm cũ (Legacy): Vẫn giữ lại để tương thích nếu cần gọi đơn lẻ, 
    nhưng Cron Job nên chuyển sang dùng get_checklist_message ở app.py
    """
    try:
        if not CHECKLIST_GROUP_ID: 
            print("Thiếu CHECKLIST_GROUP_ID")
            return

        msg = get_checklist_message(shift_type, CHECKLIST_GROUP_ID)
        if msg:
            line_bot_api.push_message(CHECKLIST_GROUP_ID, msg)
            print(f"Gửi checklist ban đầu ca {shift_type} thành công!")
            
    except Exception as e:
        print(f"Lỗi trong send_initial_checklist: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        shift = sys.argv[1].lower()
        if shift in ['sang', 'chieu']:
            send_initial_checklist(shift)
        else:
            print("Tham số không hợp lệ. Chỉ chấp nhận 'sang' hoặc 'chieu'.")
    else:
        print("Cần cung cấp tham số 'sang' hoặc 'chieu'.")