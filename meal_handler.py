import re
import math
from datetime import datetime
import pytz
from linebot.models import FlexSendMessage

# Import từ file cấu hình trung tâm
from config import CLIENT, SHEET_NAME, WORKSHEET_SCHEDULES_NAME, WORKSHEET_MEAL_TRACKER_NAME

def get_vietnamese_day_of_week():
    """Lấy tên thứ tiếng Việt để tra cứu lịch."""
    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    weekday = datetime.now(tz_vietnam).weekday()
    days = ["Thứ Hai", "Thứ Ba", "Thứ Tư", "Thứ Năm", "Thứ Sáu", "Thứ Bảy", "Chủ Nhật"]
    return days[weekday]

def clean_staff_name(name):
    """
    Làm sạch tên nhân viên:
    - Loại bỏ: (11 NV):, (2 PG):
    - Loại bỏ các ký tự đầu dòng như: -, +, •
    """
    name = re.sub(r'^\(\d+\s*(NV|PG)\):\s*', '', name, flags=re.IGNORECASE)
    name = re.sub(r'^[•\-\+]\s*', '', name)
    return name.strip()

def get_working_staff(session_type):
    """
    Lọc danh sách nhân viên từ lịch làm việc.
    session_type: 'ansang' -> Lấy Ca Sáng (Ăn trưa), loại 'off ca 3'
    session_type: 'anchieu' -> Lấy Ca Chiều (Ăn tối), loại 'off ca 4'
    """
    day_str = get_vietnamese_day_of_week()
    target_shift_name = "Ca Sáng" if session_type == 'ansang' else "Ca Chiều"
    
    exclude_pattern = r'off\s*ca\s*3' if session_type == 'ansang' else r'off\s*ca\s*4'
    
    try:
        sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_SCHEDULES_NAME)
        records = sheet.get_all_records()
        
        today_schedule = next((row for row in records if row.get('day_of_week') == day_str), None)
        if not today_schedule:
            return {}

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
                    if not clean_name: continue
                    
                    if re.search(exclude_pattern, clean_name, re.IGNORECASE):
                        continue
                        
                    results[staff_type].append(clean_name)
                    
        return results

    except Exception as e:
        print(f"Lỗi khi lấy danh sách nhân sự đi ăn: {e}")
        return {}

def sync_meal_sheet(group_id, session_type):
    """Đồng bộ danh sách từ Lịch -> Sheet Meal Tracker."""
    try:
        sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_MEAL_TRACKER_NAME)
        all_records = sheet.get_all_records()
        
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
        
        existing_entries = {}
        for row in all_records:
            if (str(row.get('group_id')) == group_id and 
                row.get('date') == today_str and 
                row.get('session') == session_type):
                existing_entries[row.get('name')] = row

        staff_lists = get_working_staff(session_type)
        final_data = [] 
        new_rows = []

        for s_type in ['NV', 'PG']:
            for name in staff_lists.get(s_type, []):
                if name in existing_entries:
                    final_data.append(existing_entries[name])
                else:
                    entry = {
                        'group_id': group_id, 'date': today_str, 'session': session_type,
                        'type': s_type, 'name': name, 'status': 'waiting', 'time_clicked': ''
                    }
                    new_rows.append([group_id, today_str, session_type, s_type, name, 'waiting', ''])
                    final_data.append(entry)
        
        if new_rows:
            sheet.append_rows(new_rows, value_input_option='USER_ENTERED')
            
        return final_data

    except Exception as e:
        print(f"Lỗi khi đồng bộ meal sheet: {e}")
        return []

def update_meal_status(group_id, session_type, staff_name):
    """Cập nhật trạng thái check-in vào Google Sheet."""
    try:
        sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_MEAL_TRACKER_NAME)
        all_values = sheet.get_all_values()
        
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
        time_now = datetime.now(tz_vietnam).strftime('%H:%M')

        row_index = -1
        for i, row in enumerate(all_values[1:], start=2):
            if (str(row[0]) == group_id and row[1] == today_str and 
                row[2] == session_type and row[4] == staff_name):
                row_index = i
                break
        
        if row_index != -1:
            sheet.update_cell(row_index, 6, 'done')
            sheet.update_cell(row_index, 7, time_now)
            return True, time_now
        return False, None
    except Exception as e:
        print(f"Lỗi update meal status: {e}")
        return False, None

def generate_meal_flex(group_id, session_type):
    """Tạo Flex Message chia cột thông minh."""
    data = sync_meal_sheet(group_id, session_type)
    if not data: return None

    # --- CẤU HÌNH GIAO DIỆN ---
    is_lunch = (session_type == 'ansang')
    title_text = "🍱 CHECK LIST ĂN TRƯA" if is_lunch else "🍲 CHECK LIST ĂN TỐI"
    header_color = "#FFA000" if is_lunch else "#303F9F" 
    
    nv_list = [d for d in data if d['type'] == 'NV']
    pg_list = [d for d in data if d['type'] == 'PG']

    body_contents = []

    def create_staff_row(index, item):
        """Tạo 1 dòng chứa: STT. Tên + Nút bấm (hoặc giờ)"""
        is_done = item.get('status') == 'done'
        time_val = item.get('time_clicked', '')
        name = item.get('name')
        
        display_name = (name[:14] + '..') if len(name) > 15 else name

        left_side = {
            "type": "text", 
            "text": f"{index}. {display_name}", 
            "size": "xs", 
            "color": "#111111", 
            "flex": 6, 
            "gravity": "center",
            "wrap": False
        }

        if is_done:
            right_side = {
                "type": "text", "text": f"✅ {time_val}", 
                "flex": 4, "align": "end", "size": "xxs", 
                "color": "#2E7D32", "gravity": "center", "weight": "bold"
            }
        else:
            right_side = {
                "type": "button",
                "style": "secondary",
                "height": "sm",
                "action": {
                    "type": "postback",
                    "label": "🍽️ Đi ăn", 
                    "data": f"action=meal_checkin&session={session_type}&name={name}"
                },
                "flex": 4,
                "margin": "xs"
            }
            
        return {"type": "box", "layout": "horizontal", "contents": [left_side, right_side], "margin": "xs", "alignItems": "center"}

    def create_section_grid(title, items, icon):
        """Chia danh sách thành các cột, tối đa 5 người/cột."""
        if not items: return None
        
        header = {
            "type": "text", 
            "text": f"{icon} {title} ({len(items)})", 
            "weight": "bold", "size": "sm", "color": "#555555", "margin": "lg"
        }
        
        chunk_size = 5
        chunks = [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]
        
        columns = []
        global_idx = 1
        
        for chunk in chunks:
            col_contents = []
            for item in chunk:
                col_contents.append(create_staff_row(global_idx, item))
                global_idx += 1
            
            columns.append({
                "type": "box", 
                "layout": "vertical", 
                "flex": 1, 
                "contents": col_contents
            })
            columns.append({"type": "spacer", "size": "md"})

        if columns: columns.pop()

        grid_container = {
            "type": "box",
            "layout": "horizontal",
            "contents": columns,
            "margin": "sm",
            # --- SỬA LỖI TẠI ĐÂY: Dùng flex-start thay vì start ---
            "alignItems": "flex-start" 
        }

        return {"type": "box", "layout": "vertical", "contents": [header, {"type": "separator", "margin": "sm"}, grid_container]}

    nv_section = create_section_grid("NHÂN VIÊN", nv_list, "👨‍💼")
    if nv_section: body_contents.append(nv_section)

    pg_section = create_section_grid("ĐỘI NGŨ PG", pg_list, "👩‍💼")
    if pg_section: body_contents.append(pg_section)

    if not body_contents:
        body_contents.append({"type": "text", "text": "Không có lịch hoặc mọi người đều OFF.", "align": "center", "size": "xs", "color": "#999999", "margin": "md"})

    flex_msg = {
        "type": "bubble",
        "size": "mega", 
        "header": {
            "type": "box", "layout": "vertical", "backgroundColor": header_color, "paddingAll": "md",
            "contents": [
                {"type": "text", "text": title_text, "weight": "bold", "size": "md", "color": "#FFFFFF", "align": "center"},
                {"type": "text", "text": "(Bấm nút bên dưới khi đi ăn)", "size": "xxs", "color": "#FFFFFF", "align": "center", "margin": "xs", "alpha": 0.8}
            ]
        },
        "body": {
            "type": "box", "layout": "vertical", "contents": body_contents, "paddingAll": "md"
        }
    }
    return flex_msg