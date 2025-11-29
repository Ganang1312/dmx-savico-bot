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
    # Chỉ raise lỗi ở log server, không gửi tin nhắn
    print("Lỗi: Thiếu biến môi trường CHANNEL_ACCESS_TOKEN hoặc CHECKLIST_GROUP_ID.") 
    # Nếu muốn code vẫn chạy tiếp (dù lỗi) thì dùng print, 
    # nhưng ở đây thiếu Token thì bot không chạy được nên raise cũng được,
    # miễn là không gọi push_message.
    # Tuy nhiên, để an toàn tôi sẽ giữ nguyên logic cũ nhưng không gửi tin nhắn.

line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)

def send_initial_checklist(shift_type):
    """
    Hàm được kích hoạt bởi webhook để bắt đầu checklist hàng ngày.
    """
    try:
        # 1. Reset và khởi tạo công việc trong Google Sheet
        # (Hàm này đã được sửa bên flex_handler để xóa sạch lịch sử cũ)
        initialize_daily_tasks(CHECKLIST_GROUP_ID, shift_type)

        # 2. Tạo và gửi tin nhắn Flex
        flex_message = generate_checklist_flex(CHECKLIST_GROUP_ID, shift_type)
        if not flex_message:
            raise Exception("Không thể tạo Flex Message.")

        line_bot_api.push_message(
            CHECKLIST_GROUP_ID,
            FlexSendMessage(alt_text=f"Checklist công việc ca {shift_type}", contents=flex_message)
        )
        print(f"Gửi checklist ban đầu ca {shift_type} thành công!")

    except Exception as e:
        # === CẬP NHẬT QUAN TRỌNG ===
        # Chỉ in lỗi ra màn hình (Log Server) để kiểm tra.
        # TUYỆT ĐỐI KHÔNG gửi tin nhắn báo lỗi cho Admin qua LINE ở đây.
        # Điều này giúp tránh việc Bot spam tin nhắn khi gặp lỗi mạng/Sheet, gây tốn quota.
        print(f"Lỗi nghiêm trọng trong send_initial_checklist: {e}")
        
        # Đoạn code cũ gửi tin cho ADMIN đã được loại bỏ.

if __name__ == "__main__":
    if len(sys.argv) > 1:
        shift = sys.argv[1].lower()
        if shift in ['sang', 'chieu']:
            send_initial_checklist(shift)
        else:
            print("Tham số không hợp lệ. Chỉ chấp nhận 'sang' hoặc 'chieu'.")
    else:
        print("Cần cung cấp tham số 'sang' hoặc 'chieu'.")