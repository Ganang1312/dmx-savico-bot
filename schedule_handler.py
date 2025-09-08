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
    """Lấy tên ngày trong tuần bằng tiếng Việt."""
    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    weekday = datetime.now(tz_vietnam).weekday()
    days = ["Thứ Hai", "Thứ Ba", "Thứ Tư", "Thứ Năm", "Thứ Sáu", "Thứ Bảy", "Chủ Nhật"]
    return days[weekday]

def parse_schedule_text(schedule_text):
    """Phân tích văn bản lịch làm việc thành các ca và nhân viên."""
    keywords = ["Ca Sáng", "Ca Chiều", "Nghỉ", "Vệ Sinh Kho"]
    pattern = '|'.join(keywords)
    
    # Sử dụng lookahead assertion để giữ lại keyword khi split
    parts = re.split(f'({pattern})', schedule_text.replace('<br>', '\n'))
    
    schedule_parts = []
    # Bỏ qua phần tử đầu tiên nếu nó trống
    i = 1 if parts and not parts[0].strip() else 0
    while i < len(parts):
        keyword = parts[i].strip()
        # Nội dung là phần tử tiếp theo, làm sạch nó
        content = parts[i+1].strip().lstrip(':').lstrip(';').strip() if i + 1 < len(parts) else ""
        schedule_parts.append({"shift": keyword, "staff": content})
        i += 2
        
    return schedule_parts


def create_schedule_flex_message(schedule_type, schedule_text):
    """Tạo tin nhắn Flex Message cho lịch làm việc với giao diện cột mới."""
    if schedule_type == 'pg':
        title = "LỊCH LÀM VIỆC PG HÔM NAY"
        header_color = "#FF6B6B" # Màu hồng
    else: # employee
        title = "LỊCH LÀM VIỆC NHÂN VIÊN"
        header_color = "#4D96FF" # Màu xanh dương

    shift_icons = {"Ca Sáng": "☀️", "Ca Chiều": "🌙", "Nghỉ": "⚪️", "Vệ Sinh Kho": "🧹"}
    parsed_schedule = parse_schedule_text(schedule_text)
    
    body_components = []
    for part in parsed_schedule:
        shift_name = part["shift"]
        staff_list_text = part["staff"]
        icon = shift_icons.get(shift_name, "📌")

        # --- Tiêu đề của mỗi ca (ví dụ: "☀️ Ca Sáng") ---
        section_header = {
            "type": "box", "layout": "horizontal", "spacing": "md",
            "contents": [
                {"type": "text", "text": icon, "flex": 0, "gravity": "center"},
                {"type": "text", "text": shift_name, "weight": "bold", "color": "#111111", "size": "md"}
            ]
        }
        
        content_box = None # Hộp chứa nội dung chia cột

        # --- Logic chia 2 cột cho Lịch Nhân Viên ---
        if schedule_type == 'employee' and shift_name in ["Ca Sáng", "Ca Chiều"]:
            special_roles = ['(ERP)', '(GH1)', '(GH2)']
            special_staff, regular_staff = [], []
            staff_list_text = re.sub(r'\(\d+\s*NV\):\s*', '', staff_list_text) # Xóa "(10 NV): "
            all_staff = [s.strip() for s in staff_list_text.split(',') if s.strip()]

            for staff in all_staff:
                if any(role in staff for role in special_roles):
                    special_staff.append(staff)
                else:
                    regular_staff.append(staff)

            col1 = [{"type": "text", "text": f"• {s}", "size": "sm", "wrap": True, "margin": "xs"} for s in special_staff]
            col2 = [{"type": "text", "text": f"• {s}", "size": "sm", "wrap": True, "margin": "xs"} for s in regular_staff]
            content_box = {
                "type": "box", "layout": "horizontal", "spacing": "md", "margin": "md",
                "contents": [
                    {"type": "box", "layout": "vertical", "flex": 1, "contents": col1},
                    {"type": "box", "layout": "vertical", "flex": 1, "contents": col2}
                ]
            }
        
        # --- Logic chia 3 cột cho Lịch PG ---
        elif schedule_type == 'pg' and shift_name in ["Ca Sáng", "Ca Chiều"]:
            staff_list_text = re.sub(r'\(\d+\):\s*', '', staff_list_text) # Xóa "(8): "
            all_staff = [s.strip() for s in staff_list_text.split('\n') if s.strip()]
            
            pgs_per_column = 3
            chunks = [all_staff[i:i + pgs_per_column] for i in range(0, len(all_staff), pgs_per_column)]
            
            columns = []
            for chunk in chunks:
                col_components = [{"type": "text", "text": f"• {s}", "size": "sm", "wrap": True, "margin": "xs"} for s in chunk]
                columns.append({"type": "box", "layout": "vertical", "flex": 1, "contents": col_components})
            
            content_box = {"type": "box", "layout": "horizontal", "spacing": "sm", "margin": "md", "contents": columns}

        # --- Giao diện mặc định cho các mục khác (Nghỉ,...) ---
        else:
            content_box = {"type": "text", "text": staff_list_text, "wrap": True, "size": "sm", "color": "#555555", "margin": "md"}

        # --- Ghép các thành phần của một ca lại ---
        section_component = {
            "type": "box", "layout": "vertical", "margin": "lg", "spacing": "sm",
            "contents": [ section_header, content_box, {"type": "separator", "margin": "lg"} ]
        }
        body_components.append(section_component)

    if body_components:
        body_components[-1]['contents'].pop() # Xóa separator cuối cùng

    flex_content = {
      "type": "bubble", "size": "mega",
      "header": {
        "type": "box", "layout": "horizontal", "alignItems": "center", "spacing": "md",
        "contents": [
          {"type": "text", "text": "📅", "flex": 0, "size": "xl"},
          {"type": "text", "text": title, "color": "#FFFFFF", "weight": "bold", "size": "md", "wrap": True}
        ],
        "backgroundColor": header_color, "paddingTop": "12px", "paddingBottom": "12px"
      },
      "body": {"type": "box", "layout": "vertical", "contents": body_components, "paddingAll": "md"}
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
        print(f"Lỗi: Loại lịch '{schedule_type}' không hợp lệ."); return

    if not target_group_id:
        print(f"CẢNH BÁO: Bỏ qua gửi lịch vì biến môi trường cho '{schedule_type}' chưa được thiết lập."); return

    try:
        sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_SCHEDULES_NAME)
        all_schedules = sheet.get_all_records()
        today_str = get_vietnamese_day_of_week()
        
        schedule_text_today = next((row.get(column_to_read) for row in all_schedules if row.get('day_of_week') == today_str), None)
        
        if schedule_text_today:
            print(f"Tìm thấy lịch cho {today_str}: {schedule_text_today}")
            flex_message_content = create_schedule_flex_message(schedule_type, schedule_text_today)
            message = FlexSendMessage(alt_text=f"Lịch làm việc hôm nay cho {schedule_type}", contents=flex_message_content)
            line_bot_api.push_message(target_group_id, message)
            print(f"Gửi lịch thành công đến group ID: {target_group_id}")
        else:
            print(f"Không tìm thấy lịch làm việc cho {today_str} trong sheet.")
            line_bot_api.push_message(target_group_id, TextSendMessage(text=f"Không tìm thấy lịch làm việc cho hôm nay ({today_str})."))

    except Exception as e:
        print(f"Lỗi nghiêm trọng khi gửi lịch làm việc: {e}")