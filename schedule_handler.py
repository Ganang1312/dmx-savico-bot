import os
from datetime import datetime
import pytz
from linebot import LineBotApi
from linebot.models import FlexSendMessage, TextSendMessage
import re

# Import từ file cấu hình trung tâm
from config import CLIENT, SHEET_NAME, WORKSHEET_SCHEDULES_NAME, get_spreadsheet

# Khởi tạo LineBotApi
CHANNEL_ACCESS_TOKEN = os.environ.get('CHANNEL_ACCESS_TOKEN')
if not CHANNEL_ACCESS_TOKEN:
    # Chỉ print warning, không raise error để tránh crash app nếu config lỗi nhẹ
    print("Cảnh báo: Biến môi trường CHANNEL_ACCESS_TOKEN chưa được thiết lập.")

line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN) if CHANNEL_ACCESS_TOKEN else None

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

_PREFIX_RE = re.compile(r"^\s*\(\s*\d+[^)]*\)\s*:?\s*")


def _count_shift(staff_text, schedule_type):
    """Đếm số người trong một khối ca (đã bỏ tiền tố '(6 NV):')."""
    if not staff_text:
        return 0
    clean = _PREFIX_RE.sub("", staff_text)
    if schedule_type == 'employee':
        items = clean.split(',')
    else:
        items = clean.split('\n')
    return len([s for s in items if s.strip() and not s.strip().isdigit()])


def create_schedule_flex_message(schedule_type, schedule_text, schedule_day_str):
    """Tạo tin nhắn Flex Message cho lịch làm việc (có STT + đếm sáng/chiều)."""
    if schedule_type == 'pg':
        title = f"LỊCH LÀM VIỆC PG - {schedule_day_str.upper()}"
        header_color = "#FF6B6B"
        unit = "PG"
    else:  # employee
        title = f"LỊCH LÀM VIỆC NHÂN VIÊN - {schedule_day_str.upper()}"
        header_color = "#4D96FF"
        unit = "NV"

    shift_icons = {"Ca Sáng": "☀️", "Ca Chiều": "🌙", "Nghỉ": "⚪️", "Vệ Sinh Kho": "🧹"}
    parsed_schedule = parse_schedule_text(schedule_text)

    # Đếm số người từng ca để ghi rõ "sáng mấy, chiều mấy" (thêm 22/09/2026)
    dem = {}
    for part in parsed_schedule:
        if part["shift"] in ("Ca Sáng", "Ca Chiều"):
            dem[part["shift"]] = _count_shift(part["staff"], schedule_type)
    so_sang = dem.get("Ca Sáng", 0)
    so_chieu = dem.get("Ca Chiều", 0)
    summary_text = ("☀️ Sáng: %d %s   🌙 Chiều: %d %s   •   Tổng: %d %s"
                    % (so_sang, unit, so_chieu, unit, so_sang + so_chieu, unit))

    body_components = []
    for part in parsed_schedule:
        shift_name = part["shift"]
        staff_list_text = part["staff"]
        icon = shift_icons.get(shift_name, "📌")
        is_shift = shift_name in ("Ca Sáng", "Ca Chiều")

        header_label = shift_name
        if is_shift:
            header_label = "%s  ·  %d %s" % (shift_name, dem.get(shift_name, 0), unit)

        section_header = {
            "type": "box", "layout": "horizontal", "spacing": "md",
            "contents": [
                {"type": "text", "text": icon, "flex": 0, "gravity": "center"},
                {"type": "text", "text": header_label, "weight": "bold",
                 "color": "#111111", "size": "sm"}
            ]
        }

        content_box = None

        if schedule_type == 'employee' and is_shift:
            special_roles = ['(ERP)', '(GH1)', '(GH2)']
            clean = _PREFIX_RE.sub("", staff_list_text)
            all_staff = [s.strip() for s in clean.split(',')
                         if s.strip() and not s.strip().isdigit()]
            numbered = [(i + 1, s) for i, s in enumerate(all_staff)]
            special_staff = [(n, s) for n, s in numbered
                             if any(r in s for r in special_roles)]
            regular_staff = [(n, s) for n, s in numbered
                             if not any(r in s for r in special_roles)]

            if not all_staff:
                content_box = {"type": "text", "text": "Không có ai.",
                               "size": "xs", "color": "#999999", "margin": "md"}
            else:
                col1 = [{"type": "text", "text": "%d. %s" % (n, s), "size": "xs",
                         "wrap": True, "margin": "xs"} for n, s in special_staff]
                col2 = [{"type": "text", "text": "%d. %s" % (n, s), "size": "xs",
                         "wrap": True, "margin": "xs"} for n, s in regular_staff]
                content_box = {
                    "type": "box", "layout": "horizontal", "spacing": "md", "margin": "md",
                    "contents": [
                        {"type": "box", "layout": "vertical", "flex": 1, "contents": col1},
                        {"type": "box", "layout": "vertical", "flex": 1, "contents": col2}
                    ]
                }

        elif schedule_type == 'pg' and is_shift:
            clean = _PREFIX_RE.sub("", staff_list_text)
            all_staff = [s.strip() for s in clean.split('\n')
                         if s.strip() and not s.strip().isdigit()]
            numbered = ["%d. %s" % (i + 1, s) for i, s in enumerate(all_staff)]

            if not numbered:
                content_box = {"type": "text", "text": "Không có ai.",
                               "size": "xs", "color": "#999999", "margin": "md"}
            else:
                pgs_per_column = 3
                chunks = [numbered[i:i + pgs_per_column]
                          for i in range(0, len(numbered), pgs_per_column)]
                columns = []
                for chunk in chunks:
                    col_components = [{"type": "text", "text": s, "size": "xs",
                                       "wrap": True, "margin": "xs"} for s in chunk]
                    columns.append({"type": "box", "layout": "vertical",
                                    "flex": 1, "contents": col_components})
                content_box = {"type": "box", "layout": "horizontal", "spacing": "sm",
                               "margin": "md", "contents": columns}

        else:
            content_box = {"type": "text", "text": staff_list_text, "wrap": True,
                           "size": "xs", "color": "#555555", "margin": "md"}

        section_component = {
            "type": "box", "layout": "vertical", "margin": "lg", "spacing": "sm",
            "contents": [section_header, content_box, {"type": "separator", "margin": "lg"}]
        }
        body_components.append(section_component)

    if body_components:
        body_components[-1]['contents'].pop()

    flex_content = {
        "type": "bubble", "size": "mega",
        "header": {
            "type": "box", "layout": "vertical", "spacing": "sm",
            "contents": [
                {
                    "type": "box", "layout": "horizontal", "alignItems": "center",
                    "spacing": "md",
                    "contents": [
                        {"type": "text", "text": "📅", "flex": 0, "size": "xl"},
                        {"type": "text", "text": title, "color": "#FFFFFF",
                         "weight": "bold", "size": "sm", "wrap": True}
                    ]
                },
                {"type": "text", "text": summary_text, "color": "#FFFFFF",
                 "size": "xs", "wrap": True}
            ],
            "backgroundColor": header_color, "paddingTop": "12px", "paddingBottom": "12px"
        },
        "body": {"type": "box", "layout": "vertical", "contents": body_components,
                 "paddingAll": "md"}
    }
    return flex_content


def send_daily_schedule(schedule_type, target_id=None, reply_token=None, day_of_week_str=None, return_msg_only=False):
    """
    Hàm chính để tìm và gửi lịch làm việc.
    CẬP NHẬT: Thêm return_msg_only để gom tin nhắn và TẮT push báo lỗi.
    """
    column_to_read = 'pg_schedule' if schedule_type == 'pg' else 'employee_schedule'

    # Nếu không có target_id và không phải chế độ lấy tin thì tự tìm ID từ env
    if not target_id and not return_msg_only and not reply_token:
        if schedule_type == 'pg':
            target_id = os.environ.get('PG_GROUP_ID')
        else:
            target_id = os.environ.get('EMPLOYEE_GROUP_ID')

    try:
        schedule_day_str = day_of_week_str if day_of_week_str else get_vietnamese_day_of_week()
        
        sheet = get_spreadsheet().worksheet(WORKSHEET_SCHEDULES_NAME)
        all_schedules = sheet.get_all_records()
        
        schedule_text_for_day = next((row.get(column_to_read) for row in all_schedules if row.get('day_of_week') == schedule_day_str), None)
        
        if schedule_text_for_day:
            flex_message_content = create_schedule_flex_message(schedule_type, schedule_text_for_day, schedule_day_str)
            alt_text = f"Lịch làm việc {schedule_day_str} cho {schedule_type}"
            message = FlexSendMessage(alt_text=alt_text, contents=flex_message_content)
            
            # --- LOGIC MỚI: Chỉ trả về message object để gom (Tiết kiệm tin nhắn) ---
            if return_msg_only:
                return message

            # Logic cũ: Gửi ngay (Dùng cho lệnh chat thủ công "NV", "PG")
            if reply_token:
                line_bot_api.reply_message(reply_token, message)
                print(f"Đã trả lời (reply) lịch thành công.")
            elif target_id:
                line_bot_api.push_message(target_id, message)
                print(f"Đã đẩy (push) lịch thành công đến: {target_id}")
            return message
        else:
            # --- CẬP NHẬT QUAN TRỌNG: KHÔNG PUSH LỖI ---
            # Chỉ gửi tin báo lỗi nếu là người dùng chat hỏi (có reply_token)
            # Nếu là Cron Job chạy tự động thì IM LẶNG để tránh tốn tiền.
            error_text = f"Không tìm thấy lịch làm việc cho {schedule_day_str}."
            print(f"[LOG] {error_text}") 
            
            if reply_token:
                line_bot_api.reply_message(reply_token, TextSendMessage(text=error_text))
            return None

    except Exception as e:
        print(f"[ERROR] Lỗi nghiêm trọng khi lấy lịch {schedule_type}: {e}")
        # Tuyệt đối không gửi tin nhắn báo lỗi qua Push
        return None