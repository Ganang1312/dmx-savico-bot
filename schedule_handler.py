import os
from datetime import datetime
import pytz
from linebot import LineBotApi
from linebot.models import FlexSendMessage, TextSendMessage
import re

# Import từ file cấu hình trung tâm
from config import CLIENT, SHEET_NAME, WORKSHEET_SCHEDULES_NAME

# Khởi tạo LineBotApi
CHANNEL_ACCESS_TOKEN = os.environ.get('CHANNEL_ACCESS_TOKEN')
if not CHANNEL_ACCESS_TOKEN:
    raise ValueError("Lỗi: Biến môi trường CHANNEL_ACCESS_TOKEN không được thiết lập.")
line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)

def get_vietnamese_day_of_week():
    """Lấy tên ngày trong tuần bằng tiếng Việt cho ngày hiện tại."""
    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    weekday = datetime.now(tz_vietnam).weekday()
    days = ["Thứ Hai", "Thứ Ba", "Thứ Tư", "Thứ Năm", "Thứ Sáu", "Thứ Bảy", "Chủ Nhật"]
    return days[weekday]

def parse_schedule_text(schedule_text):
    """Phân tích văn bản lịch làm việc thành các ca và nhân viên."""
    keywords = ["Ca Sáng", "Ca Chiều", "Nghỉ", "Vệ Sinh Kho"]
    pattern = '|'.join(keywords)
    
    parts = re.split(f'({pattern})', schedule_text.replace('<br>', '\n'))
    
    schedule_parts = []
    i = 1 if parts and not parts[0].strip() else 0
    while i < len(parts):
        keyword = parts[i].strip()
        content = parts[i+1].strip().lstrip(':').lstrip(';').strip() if i + 1 < len(parts) else ""
        schedule_parts.append({"shift": keyword, "staff": content})
        i += 2
        
    return schedule_parts

def create_schedule_flex_message(schedule_type, schedule_text, schedule_day_str):
    """Tạo tin nhắn Flex Message cho lịch làm việc với giao diện cột mới."""
    if schedule_type == 'pg':
        # === SỬA ĐỔI: Thêm ngày vào tiêu đề ===
        title = f"LỊCH LÀM VIỆC PG - {schedule_day_str.upper()}"
        header_color = "#FF6B6B"
    else: # employee
        title = f"LỊCH LÀM VIỆC NHÂN VIÊN - {schedule_day_str.upper()}"
        header_color = "#4D96FF"

    shift_icons = {"Ca Sáng": "☀️", "Ca Chiều": "🌙", "Nghỉ": "⚪️", "Vệ Sinh Kho": "🧹"}
    parsed_schedule = parse_schedule_text(schedule_text)
    
    body_components = []
    for part in parsed_schedule:
        shift_name = part["shift"]
        staff_list_text = part["staff"]
        icon = shift_icons.get(shift_name, "📌")

        section_header = {
            "type": "box", "layout": "horizontal", "spacing": "md",
            "contents": [
                {"type": "text", "text": icon, "flex": 0, "gravity": "center"},
                {"type": "text", "text": shift_name, "weight": "bold", "color": "#111111", "size": "sm"}
            ]
        }
        
        content_box = None

        if schedule_type == 'employee' and shift_name in ["Ca Sáng", "Ca Chiều"]:
            special_roles = ['(ERP)', '(GH1)', '(GH2)']
            special_staff, regular_staff = [], []
            staff_list_text = re.sub(r'\(\d+\s*NV\):\s*', '', staff_list_text)
            all_staff = [s.strip() for s in staff_list_text.split(',') if s.strip()]

            for staff in all_staff:
                if any(role in staff for role in special_roles):
                    special_staff.append(staff)
                else:
                    regular_staff.append(staff)

            col1 = [{"type": "text", "text": f"• {s}", "size": "xs", "wrap": True, "margin": "xs"} for s in special_staff]
            col2 = [{"type": "text", "text": f"• {s}", "size": "xs", "wrap": True, "margin": "xs"} for s in regular_staff]
            content_box = {
                "type": "box", "layout": "horizontal", "spacing": "md", "margin": "md",
                "contents": [
                    {"type": "box", "layout": "vertical", "flex": 1, "contents": col1},
                    {"type": "box", "layout": "vertical", "flex": 1, "contents": col2}
                ]
            }
        
        elif schedule_type == 'pg' and shift_name in ["Ca Sáng", "Ca Chiều"]:
            staff_list_text = re.sub(r'\(\d+\):\s*', '', staff_list_text)
            all_staff = [s.strip() for s in staff_list_text.split('\n') if s.strip()]
            
            pgs_per_column = 3
            chunks = [all_staff[i:i + pgs_per_column] for i in range(0, len(all_staff), pgs_per_column)]
            
            columns = []
            for chunk in chunks:
                col_components = [{"type": "text", "text": f"• {s}", "size": "xs", "wrap": True, "margin": "xs"} for s in chunk]
                columns.append({"type": "box", "layout": "vertical", "flex": 1, "contents": col_components})
            
            content_box = {"type": "box", "layout": "horizontal", "spacing": "sm", "margin": "md", "contents": columns}

        else:
            content_box = {"type": "text", "text": staff_list_text, "wrap": True, "size": "xs", "color": "#555555", "margin": "md"}

        section_component = {
            "type": "box", "layout": "vertical", "margin": "lg", "spacing": "sm",
            "contents": [ section_header, content_box, {"type": "separator", "margin": "lg"} ]
        }
        body_components.append(section_component)

    if body_components:
        body_components[-1]['contents'].pop()

    flex_content = {
      "type": "bubble", "size": "mega",
      "header": {
        "type": "box", "layout": "horizontal", "alignItems": "center", "spacing": "md",
        "contents": [
          {"type": "text", "text": "📅", "flex": 0, "size": "xl"},
          {"type": "text", "text": title, "color": "#FFFFFF", "weight": "bold", "size": "sm", "wrap": True}
        ],
        "backgroundColor": header_color, "paddingTop": "12px", "paddingBottom": "12px"
      },
      "body": {"type": "box", "layout": "vertical", "contents": body_components, "paddingAll": "md"}
    }
    return flex_content

# === BẮT ĐẦU SỬA ĐỔI ===
def send_daily_schedule(schedule_type, target_id, reply_token=None, day_of_week_str=None):
    """
    Hàm chính để tìm và gửi lịch làm việc.
    - Sẽ ưu tiên dùng reply_token nếu được cung cấp.
    - Sẽ dùng day_of_week_str nếu được cung cấp, nếu không sẽ lấy ngày hiện tại.
    """
    column_to_read = 'pg_schedule' if schedule_type == 'pg' else 'employee_schedule'

    if not target_id:
        if schedule_type == 'pg':
            target_id = os.environ.get('PG_GROUP_ID')
        else:
            target_id = os.environ.get('EMPLOYEE_GROUP_ID')

    if not target_id:
        print(f"CẢNH BÁO: Bỏ qua gửi lịch vì không có ID đích.")
        return

    try:
        # Nếu không có ngày cụ thể được truyền vào, lấy ngày hiện tại
        schedule_day_str = day_of_week_str if day_of_week_str else get_vietnamese_day_of_week()
        
        print(f"Bắt đầu xử lý lịch '{schedule_type}' cho ngày '{schedule_day_str}' đến ID: {target_id}")
        sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_SCHEDULES_NAME)
        all_schedules = sheet.get_all_records()
        
        schedule_text_for_day = next((row.get(column_to_read) for row in all_schedules if row.get('day_of_week') == schedule_day_str), None)
        
        if schedule_text_for_day:
            flex_message_content = create_schedule_flex_message(schedule_type, schedule_text_for_day, schedule_day_str)
            # Alt text cũng được cập nhật để rõ ràng hơn
            alt_text = f"Lịch làm việc {schedule_day_str} cho {schedule_type}"
            message = FlexSendMessage(alt_text=alt_text, contents=flex_message_content)
            
            if reply_token:
                line_bot_api.reply_message(reply_token, message)
                print(f"Đã trả lời (reply) lịch thành công đến: {target_id}")
            else:
                line_bot_api.push_message(target_id, message)
                print(f"Đã đẩy (push) lịch thành công đến: {target_id}")
        else:
            # Tin nhắn lỗi cũng được cập nhật
            error_text = f"Không tìm thấy lịch làm việc cho {schedule_day_str}."
            print(error_text)
            error_message = TextSendMessage(text=error_text)
            
            if reply_token:
                line_bot_api.reply_message(reply_token, error_message)
            else:
                line_bot_api.push_message(target_id, error_message)

    except Exception as e:
        print(f"Lỗi nghiêm trọng khi gửi lịch làm việc: {e}")
# === KẾT THÚC SỬA ĐỔI ===