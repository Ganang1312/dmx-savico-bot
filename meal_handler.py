import re
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

def get_working_staff(session_type):
    """
    Lọc danh sách nhân viên từ lịch làm việc.
    session_type: 'ansang' hoặc 'anchieu'
    Logic lọc:
      - ansang: Lấy 'Ca Sáng', loại bỏ người có note 'off ca 3'
      - anchieu: Lấy 'Ca Chiều', loại bỏ người có note 'off ca 4'
    """
    day_str = get_vietnamese_day_of_week()
    target_shift_name = "Ca Sáng" if session_type == 'ansang' else "Ca Chiều"
    
    # Từ khóa để loại bỏ nhân sự khỏi danh sách ăn
    exclude_keyword = "off ca 3" if session_type == 'ansang' else "off ca 4"
    
    try:
        sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_SCHEDULES_NAME)
        records = sheet.get_all_records()
        
        # Tìm dòng lịch của ngày hôm nay
        today_schedule = next((row for row in records if row.get('day_of_week') == day_str), None)
        if not today_schedule:
            return {}

        results = {'NV': [], 'PG': []}
        
        # Duyệt qua cả 2 cột lịch NV và PG
        for staff_type, col_name in [('NV', 'employee_schedule'), ('PG', 'pg_schedule')]:
            raw_text = today_schedule.get(col_name, "")
            
            # Regex để lấy nội dung nằm trong ca tương ứng
            # Tìm từ khóa Ca hiện tại cho đến khi gặp từ khóa Ca tiếp theo hoặc hết dòng
            pattern = f"{target_shift_name}(.*?)(Ca Chiều|Nghỉ|Vệ Sinh|$)"
            match = re.search(pattern, raw_text, re.DOTALL | re.IGNORECASE)
            
            if match:
                staff_block = match.group(1).strip()
                # Xóa các ký tự thừa đầu dòng
                staff_block = staff_block.lstrip(':').lstrip(';').strip()
                
                # Tách tên (dựa trên dấu phẩy hoặc xuống dòng)
                raw_names = re.split(r'[,\n]', staff_block)
                
                for name in raw_names:
                    clean_name = name.strip()
                    if not clean_name: continue
                    
                    # === LOGIC LỌC QUAN TRỌNG ===
                    # Nếu tên có chứa từ khóa loại trừ (ví dụ: "Nguyễn Văn A (off ca 3)") -> Bỏ qua
                    if exclude_keyword.lower() in clean_name.lower():
                        continue
                        
                    # Làm sạch tên để hiển thị (xóa dấu gạch đầu dòng nếu có)
                    clean_name = re.sub(r'^[•\-\+]\s*', '', clean_name)
                    
                    results[staff_type].append(clean_name)
                    
        return results

    except Exception as e:
        print(f"Lỗi khi lấy danh sách nhân sự đi ăn: {e}")
        return {}

def sync_meal_sheet(group_id, session_type):
    """
    Đồng bộ danh sách từ Lịch -> Sheet Meal Tracker.
    Nếu chưa có tên trong Sheet (ngày hôm nay), sẽ thêm mới vào.
    """
    try:
        sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_MEAL_TRACKER_NAME)
        all_records = sheet.get_all_records()
        
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
        
        # Lấy dữ liệu đã tồn tại trong sheet (để giữ trạng thái status/time)
        existing_entries = {}
        for row in all_records:
            if (str(row.get('group_id')) == group_id and 
                row.get('date') == today_str and 
                row.get('session') == session_type):
                existing_entries[row.get('name')] = row

        # Lấy danh sách mới nhất từ lịch
        staff_lists = get_working_staff(session_type)
        
        final_data = [] # Dữ liệu cuối cùng để vẽ Flex Message
        new_rows = []   # Dữ liệu mới cần ghi vào Sheet

        # Duyệt qua danh sách NV và PG
        for s_type in ['NV', 'PG']:
            for name in staff_lists.get(s_type, []):
                if name in existing_entries:
                    # Nếu đã có trong sheet, dùng dữ liệu cũ (để giữ status 'done')
                    final_data.append(existing_entries[name])
                else:
                    # Nếu chưa có, tạo mới
                    entry = {
                        'group_id': group_id, 'date': today_str, 'session': session_type,
                        'type': s_type, 'name': name, 'status': 'waiting', 'time_clicked': ''
                    }
                    new_rows.append([group_id, today_str, session_type, s_type, name, 'waiting', ''])
                    final_data.append(entry)
        
        # Ghi hàng loạt người mới vào sheet
        if new_rows:
            sheet.append_rows(new_rows, value_input_option='USER_ENTERED')
            print(f"Đã thêm {len(new_rows)} người vào danh sách ăn {session_type}.")
            
        return final_data

    except Exception as e:
        print(f"Lỗi khi đồng bộ meal sheet: {e}")
        return []

def update_meal_status(group_id, session_type, staff_name):
    """Cập nhật trạng thái 'done' và giờ khi user bấm nút."""
    try:
        sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_MEAL_TRACKER_NAME)
        all_values = sheet.get_all_values()
        
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
        time_now = datetime.now(tz_vietnam).strftime('%H:%M')

        row_index_to_update = -1
        
        # Tìm dòng cần update
        for i, row in enumerate(all_values[1:], start=2): # Bỏ header, index sheet bắt đầu từ 1
            # Cấu trúc cột: group_id(0), date(1), session(2), type(3), name(4)
            if (str(row[0]) == group_id and 
                row[1] == today_str and 
                row[2] == session_type and 
                row[4] == staff_name):
                row_index_to_update = i
                break
        
        if row_index_to_update != -1:
            # Update cột Status (6 - F) và Time (7 - G)
            # Dùng batch_update hoặc update_cell
            sheet.update_cell(row_index_to_update, 6, 'done')
            sheet.update_cell(row_index_to_update, 7, time_now)
            return True, time_now
        else:
            return False, None

    except Exception as e:
        print(f"Lỗi update meal status: {e}")
        return False, None

def generate_meal_flex(group_id, session_type):
    """Tạo giao diện Flex Message."""
    data = sync_meal_sheet(group_id, session_type)
    
    if not data:
        # Trường hợp không có dữ liệu (có thể do lỗi đọc lịch hoặc không có ai làm)
        return None

    title_text = "DANH SÁCH ĂN SÁNG 🍳" if session_type == 'ansang' else "DANH SÁCH ĂN CHIỀU 🍱"
    header_bg = "#FF9800" if session_type == 'ansang' else "#5D4037" # Cam / Nâu

    nv_list = [d for d in data if d['type'] == 'NV']
    pg_list = [d for d in data if d['type'] == 'PG']

    body_contents = []

    def create_section(section_title, items, icon):
        if not items: return None
        
        rows = [
            {"type": "text", "text": f"{icon} {section_title}", "weight": "bold", "size": "sm", "color": "#555555", "margin": "lg"},
            {"type": "separator", "margin": "sm"}
        ]
        
        for item in items:
            is_done = item.get('status') == 'done'
            time_val = item.get('time_clicked', '')
            name = item.get('name')
            
            # Dòng chứa Tên + Nút bấm (hoặc giờ)
            row_content = [
                {"type": "text", "text": name, "flex": 6, "gravity": "center", "size": "sm", "wrap": True, "color": "#111111"}
            ]

            if is_done:
                # Đã bấm: Hiện giờ
                row_content.append({
                    "type": "text", "text": f"✅ {time_val}", "flex": 3, "align": "end", "size": "xs", "color": "#2E7D32", "gravity": "center", "weight": "bold"
                })
            else:
                # Chưa bấm: Hiện nút
                row_content.append({
                    "type": "button",
                    "style": "secondary",
                    "height": "sm",
                    "action": {
                        "type": "postback",
                        "label": "Check",
                        "data": f"action=meal_checkin&session={session_type}&name={name}"
                    },
                    "flex": 3
                })

            rows.append({
                "type": "box", "layout": "horizontal", "margin": "md", "contents": row_content
            })
            
        return {"type": "box", "layout": "vertical", "contents": rows}

    nv_section = create_section("NHÂN VIÊN", nv_list, "🧑‍💼")
    if nv_section: body_contents.append(nv_section)

    pg_section = create_section("ĐỘI NGŨ PG", pg_list, "👩‍💼")
    if pg_section: body_contents.append(pg_section)

    if not body_contents:
         body_contents.append({"type": "text", "text": "Không có nhân sự nào trong ca này (hoặc đã bị lọc 'off').", "align": "center", "color": "#999999", "margin": "md"})

    flex_msg = {
        "type": "bubble",
        "header": {
            "type": "box", "layout": "vertical", "backgroundColor": header_bg, "paddingAll": "lg",
            "contents": [{"type": "text", "text": title_text, "weight": "bold", "size": "xl", "color": "#FFFFFF", "align": "center"}]
        },
        "body": {
            "type": "box", "layout": "vertical", "contents": body_contents
        }
    }
    return flex_msg