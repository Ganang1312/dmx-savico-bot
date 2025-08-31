import os
from datetime import datetime
import pytz
from linebot import LineBotApi
from linebot.models import FlexSendMessage, TextSendMessage

# SỬA LỖI: Import thêm WORKSHEET_SCHEDULES_NAME từ config
from config import CLIENT, SHEET_NAME, WORKSHEET_SCHEDULES_NAME

# Khởi tạo LineBotApi
CHANNEL_ACCESS_TOKEN = os.environ.get('CHANNEL_ACCESS_TOKEN')
if not CHANNEL_ACCESS_TOKEN:
    raise ValueError("Lỗi: Biến môi trường CHANNEL_ACCESS_TOKEN không được thiết lập.")
line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)

def get_vietnamese_day_of_week():
    """Lấy tên ngày trong tuần bằng tiếng Việt."""
    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    weekday = datetime.now(tz_vietnam).weekday()
    days = ["Thứ Hai", "Thứ Ba", "Thứ Tư", "Thứ Năm", "Thứ Sáu", "Thứ Bảy", "Chủ Nhật"]
    return days[weekday]

def create_schedule_flex_message(schedule_type, schedule_text):
    """Tạo tin nhắn Flex Message cho lịch làm việc."""
    if schedule_type == 'pg':
        title = "📅 LỊCH LÀM VIỆC PG HÔM NAY"
        header_color = "#FF6B6B" # Màu hồng
    else: # employee
        title = "📅 LỊCH LÀM VIỆC NHÂN VIÊN"
        header_color = "#4D96FF" # Màu xanh dương

    # Xử lý xuống dòng trong text, hỗ trợ cả <br> và \n
    schedule_lines = schedule_text.replace('<br>', '\n').split('\n')
    text_components = []
    for line in schedule_lines:
        if line.strip(): # Bỏ qua các dòng trống
            text_components.append({"type": "text", "text": line, "wrap": True, "size": "md"})

    flex_content = {
      "type": "bubble",
      "header": {
        "type": "box",
        "layout": "vertical",
        "contents": [
          {
            "type": "text",
            "text": title,
            "color": "#FFFFFF",
            "weight": "bold",
            "size": "lg",
            "align": "center"
          }
        ],
        "backgroundColor": header_color
      },
      "body": {
        "type": "box",
        "layout": "vertical",
        "spacing": "md",
        "contents": text_components
      }
    }
    return flex_content

def send_daily_schedule(schedule_type):
    """Hàm chính để tìm và gửi lịch làm việc hàng ngày."""
    print(f"Bắt đầu gửi lịch làm việc cho nhóm: {schedule_type}")
    
    # 1. Xác định Group ID và cột cần đọc từ biến môi trường
    if schedule_type == 'pg':
        target_group_id = os.environ.get('PG_GROUP_ID')
        column_to_read = 'pg_schedule'
    elif schedule_type == 'employee':
        target_group_id = os.environ.get('EMPLOYEE_GROUP_ID')
        column_to_read = 'employee_schedule'
    else:
        print(f"Lỗi: Loại lịch '{schedule_type}' không hợp lệ.")
        return

    if not target_group_id:
        print(f"CẢNH BÁO: Bỏ qua gửi lịch vì biến môi trường cho '{schedule_type}' chưa được thiết lập.")
        return

    try:
        # 2. Lấy lịch làm việc từ Google Sheet
        sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_SCHEDULES_NAME)
        all_schedules = sheet.get_all_records()
        today_str = get_vietnamese_day_of_week()
        
        schedule_text_today = None
        for row in all_schedules:
            if row.get('day_of_week') == today_str:
                schedule_text_today = row.get(column_to_read)
                break
        
        # 3. Gửi tin nhắn
        if schedule_text_today:
            print(f"Tìm thấy lịch cho {today_str}: {schedule_text_today}")
            flex_message_content = create_schedule_flex_message(schedule_type, schedule_text_today)
            message = FlexSendMessage(
                alt_text=f"Lịch làm việc hôm nay cho {schedule_type}",
                contents=flex_message_content
            )
            line_bot_api.push_message(target_group_id, message)
            print(f"Gửi lịch thành công đến group ID: {target_group_id}")
        else:
            print(f"Không tìm thấy lịch làm việc cho {today_str} trong sheet.")
            # Gửi tin nhắn thông báo nếu muốn
            # line_bot_api.push_message(target_group_id, TextSendMessage(text=f"Không tìm thấy lịch làm việc cho hôm nay ({today_str})."))

    except Exception as e:
        print(f"Lỗi nghiêm trọng khi gửi lịch làm việc: {e}")

