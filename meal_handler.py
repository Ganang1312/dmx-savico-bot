import re
import math
from datetime import datetime
import pytz
import unicodedata
import gspread
from linebot.models import FlexSendMessage

# Import từ file cấu hình trung tâm
from config import CLIENT, SHEET_NAME, WORKSHEET_SCHEDULES_NAME, WORKSHEET_MEAL_TRACKER_NAME

# Định nghĩa Header chuẩn (8 cột)
MEAL_HEADERS = ['group_id', 'date', 'session', 'type', 'name', 'status', 'time_clicked', 'clicked_by']

def normalize_text(text):
    """Chuẩn hóa chuỗi để so sánh chính xác."""
    if not text: return ""
    text = str(text).strip().lower()
    return unicodedata.normalize('NFC', text)

def get_vietnamese_day_of_week():
    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    weekday = datetime.now(tz_vietnam).weekday()
    days = ["Thứ Hai", "Thứ Ba", "Thứ Tư", "Thứ Năm", "Thứ Sáu", "Thứ Bảy", "Chủ Nhật"]
    return days[weekday]

def clean_staff_name(name):
    # Loại bỏ các ký tự thừa
    name = re.sub(r'^\(\d+.*?\):?\s*', '', name)
    name = re.sub(r'^[•\-\+:\.]\s*', '', name)
    return name.strip()

def get_working_staff(session_type):
    day_str = get_vietnamese_day_of_week()
    target_shift_name = "Ca Sáng" if session_type == 'ansang' else "Ca Chiều"
    exclude_pattern = r'off\s*ca\s*3' if session_type == 'ansang' else r'off\s*ca\s*4'
    
    try:
        sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_SCHEDULES_NAME)
        records = sheet.get_all_records()
        today_schedule = next((row for row in records if row.get('day_of_week') == day_str), None)
        if not today_schedule: return {}

        results = {'NV': [], 'PG': []}
        
        for staff_type, col_name in [('NV', 'employee_schedule'), ('PG', 'pg_schedule')]:
            raw_text = today_schedule.get(col_name, "")
            pattern = f"{target_shift_name}(.*?)(Ca Chiều|Nghỉ|Vệ Sinh|$)"
            match = re.search(pattern, raw_text, re.DOTALL | re.IGNORECASE)
            
            if match:
                staff_block = match.group(1).strip()
                staff_block = staff_block.lstrip(':').lstrip(';').strip()
                raw_names = re.split(r'[,\n]', staff_block)
                
                for name in raw_names:
                    clean_name = clean_staff_name(name)
                    if not clean_name or clean_name.isdigit(): continue
                    if re.search(exclude_pattern, clean_name, re.IGNORECASE): continue
                    results[staff_type].append(clean_name)
        return results
    except Exception as e:
        print(f"Lỗi lấy lịch: {e}")
        return {}

def sync_meal_sheet(group_id, session_type):
    try:
        sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_MEAL_TRACKER_NAME)
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
        
        # 1. Kiểm tra ngày để reset sheet
        first_data_date = None
        try:
            val = sheet.acell('B2').value 
            if val: first_data_date = val
        except: pass

        if first_data_date and first_data_date != today_str:
            print(f"Ngày mới! Xóa dữ liệu cũ ({first_data_date})...")
            sheet.clear()
            sheet.append_row(MEAL_HEADERS)
            all_records = []
        else:
            all_records = sheet.get_all_records()

        # 2. Đồng bộ
        existing_entries = {}
        for row in all_records:
            key_name = normalize_text(row.get('name'))
            if (str(row.get('group_id')) == group_id and 
                row.get('date') == today_str and 
                row.get('session') == session_type):
                existing_entries[key_name] = row

        staff_lists = get_working_staff(session_type)
        final_data = [] 
        new_rows = []

        for s_type in ['NV', 'PG']:
            for name in staff_lists.get(s_type, []):
                norm_name = normalize_text(name)
                
                if norm_name in existing_entries:
                    final_data.append(existing_entries[norm_name])
                else:
                    # Tạo dòng mới, cột clicked_by để trống
                    entry = {
                        'group_id': group_id, 'date': today_str, 'session': session_type,
                        'type': s_type, 'name': name, 'status': 'waiting', 'time_clicked': '', 'clicked_by': ''
                    }
                    new_rows.append([group_id, today_str, session_type, s_type, name, 'waiting', '', ''])
                    final_data.append(entry)
        
        if new_rows:
            sheet.append_rows(new_rows, value_input_option='USER_ENTERED')
            
        return final_data

    except Exception as e:
        print(f"Lỗi sync sheet: {e}")
        return []

def update_meal_status(group_id, session_type, staff_name, clicker_name, target_status='done'):
    """
    Cập nhật trạng thái và Nick LINE người bấm.
    """
    try:
        sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_MEAL_TRACKER_NAME)
        all_values = sheet.get_all_values()
        
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
        time_now = datetime.now(tz_vietnam).strftime('%H:%M') if target_status == 'done' else ''

        target_name_norm = normalize_text(staff_name)
        target_group_id = str(group_id).strip()

        row_index = -1
        current_status = None
        # Tìm dòng tương ứng
        for i, row in enumerate(all_values[1:], start=2):
            if len(row) < 5: continue
            
            row_group = str(row[0]).strip()
            row_date = str(row[1]).strip()
            row_session = str(row[2]).strip()
            row_name_norm = normalize_text(row[4])

            if (row_group == target_group_id and 
                row_date == today_str and 
                row_session == session_type and 
                row_name_norm == target_name_norm):
                row_index = i
                if len(row) >= 6:
                    current_status = row[5].strip()
                break
        
        if row_index != -1:
            # Nếu trạng thái hiện tại đã khớp với mục tiêu, bỏ qua (tránh duplicate)
            if current_status == target_status:
                print(f"Trạng thái của {staff_name} đã là {target_status} từ trước. Bỏ qua.")
                return False, None

            # Ghi dữ liệu vào 3 cột:
            # Cột 6 (F): Status -> target_status
            # Cột 7 (G): Time -> Giờ hiện tại hoặc rỗng
            # Cột 8 (H): Clicked By -> Nick Line hoặc rỗng
            
            clicked_user = clicker_name if target_status == 'done' else ''
            cells = [
                gspread.Cell(row_index, 6, target_status),
                gspread.Cell(row_index, 7, time_now),
                gspread.Cell(row_index, 8, clicked_user)
            ]
            sheet.update_cells(cells)
            return True, time_now
        
        print(f"Không tìm thấy dòng khớp cho: {staff_name}")
        return False, None
    except Exception as e:
        print(f"Lỗi update status: {e}")
        return False, None

def generate_meal_flex(group_id, session_type):
    data = sync_meal_sheet(group_id, session_type)
    if not data: return None

    is_lunch = (session_type == 'ansang')
    title_text = "🍱 CHECK LIST ĂN TRƯA" if is_lunch else "🍲 CHECK LIST ĂN TỐI"
    header_color = "#FFA000" if is_lunch else "#303F9F" 
    
    nv_list = [d for d in data if d['type'] == 'NV']
    pg_list = [d for d in data if d['type'] == 'PG']

    body_contents = []

    def create_staff_row(index, item):
        is_done = item.get('status') == 'done'
        time_val = item.get('time_clicked', '')
        name = item.get('name')
        
        # Cắt tên ngắn gọn (15 ký tự)
        display_name = (name[:15] + '..') if len(name) > 16 else name

        left_side = {
            "type": "text", "text": f"{index}. {display_name}", 
            "size": "xxs", "color": "#111111", "flex": 1, "gravity": "center", "wrap": False
        }

        if is_done:
            # Nếu đã xong thì hiện giờ (có thể click để hủy)
            right_side = {
                "type": "text", "text": f"🟢 {time_val}", 
                "flex": 0, "width": "55px", "align": "end", "size": "xxs", 
                "color": "#2E7D32", "gravity": "center", "weight": "bold",
                "action": {
                    "type": "postback",
                    "label": "Hủy",
                    "data": f"action=meal_checkin&session={session_type}&name={name}&target_status=waiting"
                }
            }
        else:
            # Nút bấm hình bát phở 🍲
            right_side = {
                "type": "button", "style": "secondary", "height": "sm", 
                "action": {"type": "postback", "label": "🍲", "data": f"action=meal_checkin&session={session_type}&name={name}&target_status=done"},
                "flex": 0, "width": "40px", "margin": "xs"
            }
            
        return {"type": "box", "layout": "horizontal", "contents": [left_side, right_side], "margin": "xs", "alignItems": "center"}

    def create_section_grid(title, items, icon):
        if not items: return None
        header = {"type": "text", "text": f"{icon} {title} ({len(items)})", "weight": "bold", "size": "sm", "color": "#555555", "margin": "lg"}
        
        # Luôn chia tối đa 2 cột nếu số lượng nhiều (> 5) để tránh bị khuất tên
        if len(items) > 5:
            chunk_size = math.ceil(len(items) / 2)
        else:
            chunk_size = len(items)
            
        chunks = [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]
        
        columns = []
        global_idx = 1
        for chunk in chunks:
            col_contents = []
            for item in chunk:
                col_contents.append(create_staff_row(global_idx, item))
                global_idx += 1
            columns.append({"type": "box", "layout": "vertical", "flex": 1, "contents": col_contents})
            
        grid_container = {"type": "box", "layout": "horizontal", "contents": columns, "margin": "sm", "alignItems": "flex-start", "spacing": "md"}
        return {"type": "box", "layout": "vertical", "contents": [header, {"type": "separator", "margin": "sm"}, grid_container]}

    nv_section = create_section_grid("NHÂN VIÊN", nv_list, "👨‍💼")
    if nv_section: body_contents.append(nv_section)

    pg_section = create_section_grid("ĐỘI NGŨ PG", pg_list, "👩‍💼")
    if pg_section: body_contents.append(pg_section)

    if not body_contents:
        body_contents.append({"type": "text", "text": "Không có lịch hoặc mọi người đều OFF.", "align": "center", "size": "xs", "color": "#999999", "margin": "md"})

    flex_msg = {
        "type": "bubble", "size": "mega", 
        "header": {
            "type": "box", "layout": "vertical", "backgroundColor": header_color, "paddingAll": "md",
            "contents": [
                {"type": "text", "text": title_text, "weight": "bold", "size": "md", "color": "#FFFFFF", "align": "center"},
                {"type": "text", "text": "(Bấm nút bên dưới khi đi ăn)", "size": "xxs", "color": "#FFFFFF", "align": "center", "margin": "xs", "alpha": 0.8}
            ]
        },
        "body": {"type": "box", "layout": "vertical", "contents": body_contents, "paddingAll": "md"}
    }
    return flex_msg