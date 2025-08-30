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

if not all([CHANNEL_ACCESS_TOKEN, CHECKLIST_GROUP_ID]):
    raise ValueError("Lỗi: Thiếu biến môi trường CHANNEL_ACCESS_TOKEN hoặc CHECKLIST_GROUP_ID.")

line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)

def send_initial_checklist(shift_type):
    """
    Hàm được kích hoạt bởi webhook để bắt đầu checklist hàng ngày.
    """
    try:
        # 1. Reset và khởi tạo công việc trong Google Sheet
        initialize_daily_tasks(CHECKLIST_GROUP_ID, shift_type)

        # 2. Tạo và gửi tin nhắn Flex
        flex_message = generate_checklist_flex(CHECKLIST_GROUP_ID, shift_type)
        if not flex_message:
            raise Exception("Không thể tạo Flex Message.")

        line_bot_api.push_message(
            CHECKLIST_GROUP_ID,
            FlexSendMessage(alt_text=f"Checklist công việc ca {shift_type}", contents=flex_message)
        )
        print("Gửi checklist ban đầu thành công!")

    except Exception as e:
        print(f"Lỗi nghiêm trọng trong send_initial_checklist: {e}")
        ADMIN_USER_ID = os.environ.get('ADMIN_USER_ID')
        if ADMIN_USER_ID:
            line_bot_api.push_message(ADMIN_USER_ID, text=f"Lỗi Cron Job khởi tạo checklist: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        shift = sys.argv[1].lower()
        if shift in ['sang', 'chieu']:
            send_initial_checklist(shift)
        else:
            print("Tham số không hợp lệ. Chỉ chấp nhận 'sang' hoặc 'chieu'.")
    else:
        print("Cần cung cấp tham số 'sang' hoặc 'chieu'.")
