import os
from datetime import datetime
import pytz
from linebot import LineBotApi
from linebot.models import FlexSendMessage, TextSendMessage
import re

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

def parse_schedule_text(schedule_text):
    """Phân tích văn bản lịch làm việc thành các ca và nhân viên."""
    # Các từ khóa để phân tách các ca
    keywords = ["Ca Sáng", "Ca Chiều", "Nghỉ", "Vệ Sinh Kho"]
    
    # Tạo một pattern regex để tìm các từ khóa
    pattern = '|'.join(keywords)
    parts = re.split(f'({pattern})', schedule_text)
    
    schedule_parts = []
    # Bỏ qua phần tử đầu tiên nếu nó trống
    i = 1 if parts[0].strip() == "" else 0
    while i < len(parts) - 1:
        keyword = parts[i].strip()
        # Lấy nội dung cho đến từ khóa tiếp theo, loại bỏ dấu ';' ở đầu
        content = parts[i+1].strip().lstrip(';').strip()
        schedule_parts.append({"shift": keyword, "staff": content})
        i += 2
        
    return schedule_parts

def create_schedule_flex_message(schedule_type, schedule_text):
    """Tạo tin nhắn Flex Message cho lịch làm việc với giao diện mới."""
    if schedule_type == 'pg':
        title = "LỊCH LÀM VIỆC PG HÔM NAY"
        header_color = "#FF6B6B" # Màu hồng
    else: # employee
        title = "LỊCH LÀM VIỆC NHÂN VIÊN"
        header_color = "#4D96FF" # Màu xanh dương

    # Icon cho từng ca
    shift_icons = {
        "Ca Sáng": "☀️",
        "Ca Chiều": "🌙",
        "Nghỉ": "⚪️",
        "Vệ Sinh Kho": "🧹"
    }

    parsed_schedule = parse_schedule_text(schedule_text)
    
    body_components = []
    for part in parsed_schedule:
        shift_name = part["shift"]
        staff_list = part["staff"]
        icon = shift_icons.get(shift_name, "📌")

        section_component = {
            "type": "box",
            "layout": "vertical",
            "margin": "lg",
            "spacing": "sm",
            "contents": [
                {
                    "type": "box",
                    "layout": "horizontal",
                    "spacing": "md",
                    "contents": [
                        {
                            "type": "text",
                            "text": icon,
                            "flex": 0,
                            "gravity": "center"
                        },
                        {
                            "type": "text",
                            "text": shift_name,
                            "weight": "bold",
                            "color": "#111111",
                            "size": "md"
                        }
                    ]
                },
                {
                    "type": "text",
                    "text": staff_list,
                    "wrap": True,
                    "size": "sm",
                    "color": "#555555",
                    "margin": "md"
                },
                {
                    "type": "separator",
                    "margin": "lg"
                }
            ]
        }
        body_components.append(section_component)
    
    # Xóa separator cuối cùng để đẹp hơn
    if body_components:
        body_components[-1]['contents'].pop()

    flex_content = {
      "type": "bubble",
      "size": "giga",
      "header": {
        "type": "box",
        "layout": "horizontal",
        "alignItems": "center",
        "spacing": "md",
        "contents": [
          {
            "type": "text",
            "text": "📅",
            "flex": 0,
            "size": "xl"
          },
          {
            "type": "text",
            "text": title,
            "color": "#FFFFFF",
            "weight": "bold",
            "size": "md", # Giảm size chữ tiêu đề để không bị tràn
            "wrap": True
          }
        ],
        "backgroundColor": header_color,
        "paddingTop": "12px",
        "paddingBottom": "12px"
      },
      "body": {
        "type": "box",
        "layout": "vertical",
        "contents": body_components,
        "paddingAll": "md"
      }
    }
    return flex_content

def send_daily_schedule(schedule_type):
    """Hàm chính để tìm và gửi lịch làm việc hàng ngày."""
    print(f"Bắt đầu gửi lịch làm việc cho nhóm: {schedule_type}")
    
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
        sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_SCHEDULES_NAME)
        all_schedules = sheet.get_all_records()
        today_str = get_vietnamese_day_of_week()
        
        schedule_text_today = None
        for row in all_schedules:
            if row.get('day_of_week') == today_str:
                schedule_text_today = row.get(column_to_read)
                break
        
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
            line_bot_api.push_message(target_group_id, TextSendMessage(text=f"Không tìm thấy lịch làm việc cho hôm nay ({today_str})."))

    except Exception as e:
        print(f"Lỗi nghiêm trọng khi gửi lịch làm việc: {e}")

