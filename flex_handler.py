from datetime import datetime
import pytz
# Import từ file cấu hình trung tâm
from config import CLIENT, SHEET_NAME, WORKSHEET_TRACKER_NAME, get_spreadsheet

# --- Danh sách công việc ---
TASKS = {
    'sang': [
        {'id': 'sang_1', 'icon': '📦', 'name': 'Check lệnh chuyển kho online', 'time': '09:15'},
        {'id': 'sang_2', 'icon': '🚚', 'name': 'Check đơn GHTK chuyển kho', 'time': '09:30'},
        {'id': 'sang_3', 'icon': '🏷️', 'name': 'Chạy tủ, thay giá (thứ 2 & 5)', 'time': '10:00'},
        {'id': 'sang_4', 'icon': '🧹', 'name': 'Rà soát tốc kệ', 'time': '10:30'},
        {'id': 'sang_5', 'icon': '📑', 'name': 'Check Phiếu CK/NK quá 7 ngày', 'time': '11:30'},
        {'id': 'sang_6', 'icon': '🔧', 'name': 'Đổ tồn hàng T.Thái (lỗi)', 'time': '14:00'},
    ],
    'chieu': [
        {'id': 'chieu_1', 'icon': '📦', 'name': 'Check lệnh online', 'time': '15:15'},
        {'id': 'chieu_2', 'icon': '🚚', 'name': 'Check đơn GHTK', 'time': '15:30'},
        {'id': 'chieu_3', 'icon': '📦🧹', 'name': 'Sắp xếp hàng hóa kho', 'time': '16:00'},
        {'id': 'chieu_4', 'icon': '🖼️', 'name': 'Rà soát tốc kệ (gia dụng/tivi)', 'time': '16:30'},
        {'id': 'chieu_5', 'icon': '📊', 'name': 'Xử lý BCNB chiều', 'time': '17:30'},
        {'id': 'chieu_6', 'icon': '🔧', 'name': 'Đổ tồn hàng T.Thái (lỗi)', 'time': '19:00'},
        {'id': 'chieu_7', 'icon': '📦🚚', 'name': 'Check GHTK / Grab', 'time': '21:00'},
        {'id': 'chieu_8', 'icon': '📸', 'name': 'Up hình máy cũ / trưng bày', 'time': '21:30'},
    ],
    'vs': [
        {'id': 'vs_1', 'icon': '📸', 'name': '1. Cụm 14285', 'time': '10:00'},
        {'id': 'vs_2', 'icon': '📸', 'name': '2. Cụm 5468', 'time': '10:00'},
        {'id': 'vs_3', 'icon': '📸', 'name': '3. Ngọc Lâm', 'time': '10:00'},
        {'id': 'vs_4', 'icon': '📸', 'name': '4. Ngọc Trì', 'time': '10:00'},
        {'id': 'vs_5', 'icon': '📸', 'name': '5. Ngô Gia Tự', 'time': '10:00'},
        {'id': 'vs_6', 'icon': '📸', 'name': '6. Savico', 'time': '10:00'},
    ]
}

def initialize_daily_tasks(group_id, shift_type, force=False):
    """
    Reset và khởi tạo lại danh sách công việc cho ca cụ thể.
    Giữ lại các ca khác của ngày hôm nay và xóa các dữ liệu cũ.
    Nếu force=False, sẽ chỉ khởi tạo nếu hôm nay chưa có dữ liệu cho ca này.
    """
    print(f"Bắt đầu khởi tạo công việc ca {shift_type} cho group {group_id} (force={force})...")
    try:
        sheet = get_spreadsheet().worksheet(WORKSHEET_TRACKER_NAME)
        all_values = sheet.get_all_values()
        
        headers = ['group_id', 'date', 'task_id', 'name', 'time', 'status', 'user_name']
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
        
        # Kiểm tra xem ca này hôm nay đã có dữ liệu chưa
        has_today_tasks = False
        rows_to_keep = []
        
        if all_values:
            headers = all_values[0]
            for row in all_values[1:]:
                if len(row) < 3: continue
                r_group = str(row[0])
                r_date = str(row[1])
                r_task_id = str(row[2])
                
                if r_date == today_str and r_group == str(group_id) and r_task_id.startswith(shift_type):
                    has_today_tasks = True
                
                # Giữ lại các dòng của ngày hôm nay thuộc các ca/group khác
                if r_date == today_str:
                    if r_group == str(group_id) and r_task_id.startswith(shift_type):
                        continue
                    rows_to_keep.append(row)
        
        # Nếu đã có dữ liệu và không yêu cầu force reset, bỏ qua bước khởi tạo lại
        if has_today_tasks and not force:
            print(f"Ca {shift_type} đã được khởi tạo hôm nay cho group {group_id}. Bỏ qua.")
            return True
            
        # Ghi đè lại dữ liệu (Clear và Append)
        sheet.clear()
        sheet.append_row(headers)
        if rows_to_keep:
            sheet.append_rows(rows_to_keep, value_input_option='USER_ENTERED')
            
        # Thêm các task mới của ca này
        tasks_to_add = []
        for task in TASKS.get(shift_type, []):
            new_row = [group_id, today_str, task['id'], task['name'], task['time'], 'incomplete', '']
            tasks_to_add.append(new_row)

        if tasks_to_add:
            sheet.append_rows(tasks_to_add, value_input_option='USER_ENTERED')
            print(f"Đã khởi tạo mới checklist ca {shift_type} thành công.")
        return True
    except Exception as e:
        print(f"Lỗi khi khởi tạo công việc: {e}")
        return False

def get_tasks_status_from_sheet(group_id, shift_type, all_records=None):
    try:
        if all_records is None:
            sheet = get_spreadsheet().worksheet(WORKSHEET_TRACKER_NAME)
            all_records = sheet.get_all_records()
        
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
        
        task_statuses = {}
        
        for record in all_records:
            if str(record.get('group_id')) == group_id and record.get('date') == today_str:
                task_id = record.get('task_id')
                if task_id and task_id.startswith(shift_type):
                    task_statuses[task_id] = {
                        'status': record.get('status', 'incomplete'),
                        'user_name': record.get('user_name', '')
                    }
        return task_statuses
    except Exception as e:
        print(f"Lỗi khi lấy trạng thái công việc: {e}")
        return {}

def generate_checklist_flex(group_id, shift_type, all_records_prefetched=None):
    task_statuses = get_tasks_status_from_sheet(group_id, shift_type, all_records=all_records_prefetched)
    
    if not task_statuses:
        task_statuses = {task['id']: {'status': 'incomplete', 'user_name': ''} for task in TASKS.get(shift_type, [])}

    if shift_type == 'sang':
        title = "CHECKLIST CÔNG VIỆC CA SÁNG"
        title_icon = "✅"
    elif shift_type == 'chieu':
        title = "CHECKLIST CÔNG VIỆC CA CHIỀU"
        title_icon = "🌙"
    else:
        title = "CHECKLIST HÌNH ẢNH TRƯỚC 10H"
        title_icon = "📸"
    
    task_components = []
    
    # Add description box for VS
    if shift_type == 'vs':
        desc_box = {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#F4F6F9",
            "paddingAll": "md",
            "margin": "xs",
            "cornerRadius": "sm",
            "contents": [
                {
                    "type": "text",
                    "text": "Các cụm tạo Album đúng deadline nhé ACE",
                    "weight": "bold",
                    "size": "xs",
                    "color": "#E65100",
                    "wrap": True
                },
                {
                    "type": "text",
                    "text": "• Ảnh chụp bên ngoài ST (thấy biển, sân xe, toàn cảnh ST)\n• Ảnh chụp bên trong ST (thấy các góc ST, Line)\n• Ảnh chụp kho ST ( thấy quầy kệ gọn sạch )",
                    "size": "xxs",
                    "color": "#4A4A4A",
                    "wrap": True,
                    "margin": "xs"
                }
            ]
        }
        task_components.append(desc_box)
        task_components.append({"type": "separator", "margin": "sm"})
        
    for task in TASKS.get(shift_type, []):
        status_info = task_statuses.get(task['id'], {})
        if isinstance(status_info, str):
            status = status_info
            completed_by = ''
        else:
            status = status_info.get('status', 'incomplete')
            completed_by = status_info.get('user_name', '')
            
        is_complete = (status == 'complete')
        
        text_decoration = "line-through" if is_complete else "none"
        main_text_color = "#AAAAAA" if is_complete else "#111111"
        deadline_color = "#AAAAAA" if is_complete else "#FF5555"
        
        # Nút hoàn tất màu xanh, nút xong màu xám
        button_color = "#CCCCCC" if is_complete else "#00B33C"
        button_label = "✓ Xong" if is_complete else "Hoàn tất"
        
        sub_text_label = "Deadline"
        sub_text_value = task['time']
        if is_complete:
            target_status_param = "incomplete"
        else:
            target_status_param = "complete"

        task_component = {
            "type": "box",
            "layout": "horizontal",
            "spacing": "lg",
            "paddingAll": "md",
            "alignItems": "center",
            "contents": [
                {
                    "type": "text",
                    "text": task.get('icon', '❓'),
                    "size": "xl",
                    "flex": 0
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "spacing": "xs",
                    "contents": [
                        {
                            "type": "text",
                            "text": task['name'],
                            "wrap": True,
                            "weight": "bold",
                            "size": "sm",
                            "color": main_text_color,
                            "decoration": text_decoration
                        },
                        {
                            "type": "box",
                            "layout": "horizontal",
                            "spacing": "xs",
                            "contents": [
                                {
                                    "type": "text",
                                    "text": sub_text_label,
                                    "color": deadline_color,
                                    "size": "xs",
                                    "flex": 0
                                },
                                {
                                    "type": "text",
                                    "text": sub_text_value,
                                    "color": deadline_color,
                                    "weight": "bold",
                                    "size": "xs",
                                    "flex": 1,
                                    "wrap": True
                                }
                            ]
                        }
                    ]
                },
                {
                    "type": "button",
                    "action": {
                        "type": "postback",
                        "label": button_label,
                        "data": f"action=complete_task&task_id={task['id']}&shift={shift_type}&target_status={target_status_param}"
                    },
                    "style": "primary",
                    "color": button_color,
                    "height": "sm",
                    "flex": 0
                }
            ]
        }
        task_components.append(task_component)
        task_components.append({"type": "separator"})

    if task_components:
        task_components.pop()

    flex_content = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "horizontal",
            "alignItems": "center",
            "spacing": "md",
            "contents": [
                {
                    "type": "text",
                    "text": title_icon,
                    "size": "xxl",
                    "flex": 0
                },
                {
                    "type": "text",
                    "text": title,
                    "weight": "bold",
                    "size": "lg",
                    "color": "#FFFFFF",
                    "wrap": True
                }
            ],
            "backgroundColor": "#0080ff" if shift_type in ['sang', 'vs'] else "#27406E",
            "paddingTop": "12px",
            "paddingBottom": "12px",
            "paddingStart": "16px",
            "paddingEnd": "16px"
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "paddingAll": "sm",
            "contents": task_components
        }
    }
    return flex_content

# ==========================================
# PHẦN XỬ LÝ CÔNG VIỆC PHÁT SINH (ADHOC TASKS)
# ==========================================
import uuid

_adhoc_sheet_cache = None
_last_clean_date = None

def get_or_create_adhoc_worksheet():
    """
    Lấy worksheet adhoc_tasks, nếu chưa tồn tại thì tạo mới.
    """
    global _adhoc_sheet_cache
    if _adhoc_sheet_cache is not None:
        return _adhoc_sheet_cache
        
    from config import get_spreadsheet, WORKSHEET_ADHOC_TASKS
    import gspread
    try:
        spreadsheet = get_spreadsheet()
        try:
            worksheet = spreadsheet.worksheet(WORKSHEET_ADHOC_TASKS)
            _adhoc_sheet_cache = worksheet
            return worksheet
        except gspread.exceptions.WorksheetNotFound:
            headers = ['group_id', 'date', 'assignee', 'task_id', 'task_name', 'status', 'completed_by', 'completed_at', 'created_at']
            worksheet = spreadsheet.add_worksheet(title=WORKSHEET_ADHOC_TASKS, rows="1000", cols="20")
            worksheet.append_row(headers)
            print(f"Đã tạo worksheet mới: {WORKSHEET_ADHOC_TASKS}")
            _adhoc_sheet_cache = worksheet
            return worksheet
    except Exception as e:
        print(f"Lỗi khi lấy/tạo worksheet adhoc_tasks: {e}")
        return None

def clean_old_adhoc_tasks(sheet):
    """
    Xóa các công việc cũ (khác ngày hôm nay) trong trang tính adhoc_tasks để giải phóng dữ liệu.
    """
    global _last_clean_date
    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
    
    if _last_clean_date == today_str:
        return
        
    try:
        all_values = sheet.get_all_values()
        if len(all_values) <= 1:
            _last_clean_date = today_str
            return
        
        headers = all_values[0]
        rows_to_keep = []
        has_old_rows = False
        for row in all_values[1:]:
            if len(row) > 1 and row[1] == today_str:
                rows_to_keep.append(row)
            else:
                has_old_rows = True
                
        if has_old_rows:
            sheet.clear()
            sheet.append_row(headers)
            if rows_to_keep:
                sheet.append_rows(rows_to_keep, value_input_option='USER_ENTERED')
            print("Đã tự động dọn dẹp các công việc phát sinh cũ của những ngày trước.")
        _last_clean_date = today_str
    except Exception as e:
        print(f"Lỗi khi dọn dẹp adhoc tasks cũ: {e}")

def add_adhoc_tasks(group_id, assignee, tasks_list):
    """
    Thêm danh sách các công việc phát sinh cho nhân viên vào sheet.
    """
    sheet = get_or_create_adhoc_worksheet()
    if not sheet:
        return False
        
    clean_old_adhoc_tasks(sheet)
    
    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
    time_now_str = datetime.now(tz_vietnam).strftime('%H:%M')
    
    rows_to_add = []
    for task_name in tasks_list:
        task_id = f"adhoc_{uuid.uuid4().hex[:8]}"
        new_row = [
            str(group_id),
            today_str,
            assignee,
            task_id,
            task_name,
            'incomplete',
            '',
            '',
            time_now_str
        ]
        rows_to_add.append(new_row)
        
    if rows_to_add:
        sheet.append_rows(rows_to_add, value_input_option='USER_ENTERED')
        print(f"Đã thêm {len(rows_to_add)} công việc phát sinh cho {assignee}")
        return True
    return False

def get_adhoc_tasks_today(group_id, assignee):
    """
    Lấy danh sách công việc phát sinh hôm nay của nhân viên đó.
    """
    sheet = get_or_create_adhoc_worksheet()
    if not sheet:
        return []
    
    try:
        all_records = sheet.get_all_records()
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
        
        filtered_tasks = []
        for record in all_records:
            if (str(record.get('group_id')) == str(group_id) and 
                record.get('date') == today_str and 
                str(record.get('assignee')).strip().lower() == str(assignee).strip().lower()):
                filtered_tasks.append(record)
        return filtered_tasks
    except Exception as e:
        print(f"Lỗi khi lấy adhoc tasks hôm nay: {e}")
        return []

def update_adhoc_task_status(group_id, task_id, target_status, completed_by):
    """
    Cập nhật trạng thái của adhoc task.
    """
    sheet = get_or_create_adhoc_worksheet()
    if not sheet:
        return False, None, None
    
    try:
        all_records = sheet.get_all_records()
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
        time_str = datetime.now(tz_vietnam).strftime('%H:%M')
        
        row_idx = -1
        assignee = None
        task_group_hash = None
        for i, record in enumerate(all_records):
            if (str(record.get('group_id')) == str(group_id) and 
                record.get('task_id') == task_id):
                row_idx = i + 2  # 1-indexed and header row
                assignee = record.get('assignee')
                if str(task_id).startswith('all_') or str(task_id).startswith('multi_'):
                    parts = str(task_id).split('_')
                    if len(parts) >= 3:
                        task_group_hash = parts[1]
                break
                
        if row_idx != -1:
            comp_by = completed_by if target_status == 'complete' else ''
            comp_at = time_str if target_status == 'complete' else ''
            
            # Cột F: status, Cột G: completed_by, Cột H: completed_at
            range_to_update = f'F{row_idx}:H{row_idx}'
            sheet.update(range_name=range_to_update, values=[[target_status, comp_by, comp_at]])
            return True, assignee, task_group_hash
        return False, None, None
    except Exception as e:
        print(f"Lỗi khi cập nhật trạng thái adhoc task: {e}")
        return False, None, None

def generate_adhoc_flex(group_id, assignee, tasks_data=None):
    """
    Tạo Flex Message cho danh sách công việc phát sinh hôm nay của nhân viên.
    """
    if tasks_data is None:
        tasks_data = get_adhoc_tasks_today(group_id, assignee)
        
    if not tasks_data:
        return None
        
    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    today_str = datetime.now(tz_vietnam).strftime('%d/%m/%Y')
    
    task_components = []
    
    for task in tasks_data:
        task_id = task.get('task_id')
        task_name = task.get('task_name')
        status = task.get('status', 'incomplete')
        completed_by = task.get('completed_by', '')
        completed_at = task.get('completed_at', '')
        created_at = task.get('created_at', '')
        
        is_complete = (status == 'complete')
        
        text_decoration = "line-through" if is_complete else "none"
        main_text_color = "#AAAAAA" if is_complete else "#111111"
        
        # Nút hoàn tất màu xanh, nút xong màu xám
        button_color = "#CCCCCC" if is_complete else "#00B33C"
        button_label = "✓ Xong" if is_complete else "Hoàn tất"
        
        target_status_param = "incomplete" if is_complete else "complete"
        
        task_info_contents = [
            {
                "type": "text",
                "text": task_name,
                "wrap": True,
                "weight": "bold",
                "size": "sm",
                "color": main_text_color,
                "decoration": text_decoration
            }
        ]
        
        if created_at:
            task_info_contents.append({
                "type": "text",
                "text": f"🕒 Giao lúc: {created_at}",
                "color": "#888888" if is_complete else "#E65100",
                "size": "xs",
                "margin": "xs"
            })
        

            
        task_component = {
            "type": "box",
            "layout": "horizontal",
            "spacing": "lg",
            "paddingAll": "md",
            "alignItems": "center",
            "contents": [
                {
                    "type": "text",
                    "text": "✅" if is_complete else "📝",
                    "size": "lg",
                    "flex": 0
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "spacing": "xs",
                    "contents": task_info_contents
                },
                {
                    "type": "button",
                    "action": {
                        "type": "postback",
                        "label": button_label,
                        "data": f"action=complete_adhoc_task&task_id={task_id}&assignee={assignee}&target_status={target_status_param}"
                    },
                    "style": "primary",
                    "color": button_color,
                    "height": "sm",
                    "flex": 0
                }
            ]
        }
        task_components.append(task_component)
        task_components.append({"type": "separator"})
        
    if task_components:
        task_components.pop() # Xóa separator cuối cùng
        
    flex_content = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#5E35B1",  # Deep Purple sang trọng
            "paddingTop": "16px",
            "paddingBottom": "16px",
            "paddingStart": "20px",
            "paddingEnd": "20px",
            "contents": [
                {
                    "type": "text",
                    "text": "📋 CÔNG VIỆC PHÁT SINH",
                    "weight": "bold",
                    "size": "md",
                    "color": "#FFFFFF"
                },
                {
                    "type": "text",
                    "text": f"👤 Nhân viên: {assignee}",
                    "weight": "bold",
                    "size": "lg",
                    "color": "#FFFFFF",
                    "margin": "sm",
                    "wrap": True
                },
                {
                    "type": "text",
                    "text": f"📅 Ngày giao: {today_str}",
                    "size": "xs",
                    "color": "#D1C4E9",
                    "margin": "xs"
                }
            ]
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "paddingAll": "sm",
            "contents": task_components
        }
    }
    return flex_content

def add_all_adhoc_tasks(group_id, members, task_name):
    """
    Giao việc chung @all cho toàn bộ thành viên trong nhóm.
    """
    sheet = get_or_create_adhoc_worksheet()
    if not sheet:
        return None
        
    clean_old_adhoc_tasks(sheet)
        
    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
    time_now_str = datetime.now(tz_vietnam).strftime('%H:%M')
    
    task_group_hash = uuid.uuid4().hex[:6]
    
    rows_to_add = []
    for index, member in enumerate(members):
        task_id = f"all_{task_group_hash}_{index}"
        new_row = [
            str(group_id),
            today_str,
            member,
            task_id,
            task_name,
            'incomplete',
            '',
            '',
            time_now_str
        ]
        rows_to_add.append(new_row)
        
    if rows_to_add:
        sheet.append_rows(rows_to_add, value_input_option='USER_ENTERED')
        print(f"Đã thêm việc @all {task_name} cho {len(rows_to_add)} thành viên")
        return task_group_hash
    return None

def generate_all_adhoc_flex(group_id, task_group_hash):
    """
    Tạo Flex Message hiển thị danh sách thành viên thực hiện việc chung @all.
    """
    sheet = get_or_create_adhoc_worksheet()
    if not sheet:
        return None
        
    try:
        all_records = sheet.get_all_records()
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
        today_display_str = datetime.now(tz_vietnam).strftime('%d/%m/%Y')
        
        # Lấy tất cả task thuộc nhóm hash này
        prefix = f"all_{task_group_hash}_"
        filtered_tasks = []
        for record in all_records:
            if (str(record.get('group_id')) == str(group_id) and 
                record.get('date') == today_str and 
                str(record.get('task_id')).startswith(prefix)):
                filtered_tasks.append(record)
                
        if not filtered_tasks:
            return None
            
        # Nội dung công việc chung (lấy từ bản ghi đầu tiên)
        task_name = filtered_tasks[0].get('task_name', 'Công việc chung')
        created_at = filtered_tasks[0].get('created_at', '')
        
        task_components = []
        for i, task in enumerate(filtered_tasks, start=1):
            task_id = task.get('task_id')
            assignee = task.get('assignee')
            status = task.get('status', 'incomplete')
            completed_by = task.get('completed_by', '')
            completed_at = task.get('completed_at', '')
            
            is_complete = (status == 'complete')
            main_text_color = "#AAAAAA" if is_complete else "#111111"
            
            button_color = "#CCCCCC" if is_complete else "#00B33C"
            button_label = "✓ Xong" if is_complete else "Hoàn tất"
            target_status_param = "incomplete" if is_complete else "complete"
            
            task_info_contents = [
                {
                    "type": "text",
                    "text": f"{i}. {assignee}",
                    "wrap": False,
                    "maxLines": 1,
                    "weight": "bold",
                    "size": "xs",
                    "color": main_text_color,
                    "decoration": "line-through" if is_complete else "none"
                }
            ]
                
            task_component = {
                "type": "box",
                "layout": "horizontal",
                "spacing": "sm",
                "paddingAll": "sm",
                "alignItems": "center",
                "contents": [
                    {
                        "type": "text",
                        "text": "✅" if is_complete else "⏳",
                        "size": "md",
                        "flex": 0
                    },
                    {
                        "type": "box",
                        "layout": "vertical",
                        "flex": 1,
                        "spacing": "xs",
                        "contents": task_info_contents
                    },
                    {
                        "type": "button",
                        "action": {
                            "type": "postback",
                            "label": button_label,
                            "data": f"action=complete_adhoc_task&task_id={task_id}&assignee={assignee}&target_status={target_status_param}"
                        },
                        "style": "primary",
                        "color": button_color,
                        "height": "sm",
                        "flex": 0,
                        "width": "75px"
                    }
                ]
            }
            task_components.append(task_component)
            task_components.append({"type": "separator"})
            
        if task_components:
            task_components.pop()
            
        flex_content = {
            "type": "bubble",
            "size": "mega",
            "header": {
                "type": "box",
                "layout": "vertical",
                "backgroundColor": "#1565C0",  # Royal Blue đậm chuyên nghiệp
                "paddingTop": "16px",
                "paddingBottom": "16px",
                "paddingStart": "20px",
                "paddingEnd": "20px",
                "contents": [
                    {
                        "type": "text",
                        "text": "📢 CÔNG VIỆC CHUNG @ALL",
                        "weight": "bold",
                        "size": "md",
                        "color": "#FFFFFF"
                    },
                    {
                        "type": "text",
                        "text": task_name,
                        "weight": "bold",
                        "size": "lg",
                        "color": "#FFFFFF",
                        "margin": "sm",
                        "wrap": True
                    },
                    {
                        "type": "text",
                        "text": f"📅 Ngày giao: {today_display_str}  |  🕒 Giao lúc: {created_at}",
                        "size": "xs",
                        "color": "#BBDEFB",
                        "margin": "xs"
                    }
                ]
            },
            "body": {
                "type": "box",
                "layout": "vertical",
                "spacing": "sm",
                "paddingAll": "sm",
                "contents": task_components
            }
        }
        return flex_content
    except Exception as e:
        print(f"Lỗi khi tạo flex công việc chung: {e}")
        return None

_group_members_sheet_cache_flex = None

def register_group_member(group_id, user_id, display_name):
    """
    Lưu thành viên của nhóm vào sheet group_members để phục vụ cho việc giao việc @all.
    """
    global _group_members_sheet_cache_flex
    if not group_id or not user_id or not display_name:
        return
    # Nếu group_id giống user_id (chat 1-1), bỏ qua
    if str(group_id) == str(user_id):
        return
        
    try:
        from config import get_spreadsheet
        import gspread
        
        if _group_members_sheet_cache_flex is None:
            spreadsheet = get_spreadsheet()
            try:
                _group_members_sheet_cache_flex = spreadsheet.worksheet('group_members')
            except gspread.exceptions.WorksheetNotFound:
                headers = ['group_id', 'user_id', 'display_name', 'last_seen']
                _group_members_sheet_cache_flex = spreadsheet.add_worksheet(title='group_members', rows="1000", cols="10")
                _group_members_sheet_cache_flex.append_row(headers)
                print("Đã tạo worksheet lưu thành viên nhóm: group_members")
                
        sheet = _group_members_sheet_cache_flex
        all_records = sheet.get_all_records()
        
        row_idx = -1
        for i, r in enumerate(all_records):
            if str(r.get('group_id')) == str(group_id) and str(r.get('user_id')) == str(user_id):
                row_idx = i + 2
                break
                
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        now_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d %H:%M:%S')
        
        if row_idx != -1:
            sheet.update(range_name=f'C{row_idx}:D{row_idx}', values=[[display_name, now_str]])
        else:
            new_row = [str(group_id), str(user_id), display_name, now_str]
            sheet.append_row(new_row)
    except Exception as e:
        print(f"Lỗi khi lưu group member: {e}")

def add_multi_adhoc_tasks(group_id, job_name, task_assignments):
    """
    Thêm danh sách các công việc phát sinh cho nhiều nhân viên dưới một tên công việc chung (multi-assignee checklist).
    task_assignments là danh sách các tuple dạng (sub_task_name, assignee).
    """
    sheet = get_or_create_adhoc_worksheet()
    if not sheet:
        return None
        
    clean_old_adhoc_tasks(sheet)
    
    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
    time_now_str = datetime.now(tz_vietnam).strftime('%H:%M')
    
    task_group_hash = uuid.uuid4().hex[:6]
    
    rows_to_add = []
    for index, (sub_task, assignee) in enumerate(task_assignments):
        task_id = f"multi_{task_group_hash}_{index}"
        combined_task_name = f"{job_name} | {sub_task}"
        new_row = [
            str(group_id),
            today_str,
            assignee,
            task_id,
            combined_task_name,
            'incomplete',
            '',
            '',
            time_now_str
        ]
        rows_to_add.append(new_row)
        
    if rows_to_add:
        sheet.append_rows(rows_to_add, value_input_option='USER_ENTERED')
        print(f"Đã thêm checklist công việc '{job_name}' cho {len(rows_to_add)} nhân sự")
        return task_group_hash
    return None

def generate_multi_adhoc_flex(group_id, task_group_hash):
    """
    Tạo Flex Message hiển thị danh sách checklist công việc (multi-assignee) với nhiều người/nhiều việc khác nhau.
    """
    sheet = get_or_create_adhoc_worksheet()
    if not sheet:
        return None
        
    try:
        all_records = sheet.get_all_records()
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
        today_display_str = datetime.now(tz_vietnam).strftime('%d/%m/%Y')
        
        prefix = f"multi_{task_group_hash}_"
        filtered_tasks = []
        for record in all_records:
            if (str(record.get('group_id')) == str(group_id) and 
                record.get('date') == today_str and 
                str(record.get('task_id')).startswith(prefix)):
                filtered_tasks.append(record)
                
        if not filtered_tasks:
            return None
            
        combined_name = filtered_tasks[0].get('task_name', 'Công việc phát sinh')
        created_at = filtered_tasks[0].get('created_at', '')
        
        if ' | ' in combined_name:
            main_job_name = combined_name.split(' | ', 1)[0]
        else:
            main_job_name = combined_name

        grouped_subtasks = {}
        for task in filtered_tasks:
            task_name_val = task.get('task_name', '')
            if ' | ' in task_name_val:
                sub_task_name = task_name_val.split(' | ', 1)[1]
            else:
                sub_task_name = task_name_val
                
            if sub_task_name not in grouped_subtasks:
                grouped_subtasks[sub_task_name] = []
            grouped_subtasks[sub_task_name].append(task)

        task_components = []
        for sub_idx, (sub_task_name, task_list) in enumerate(grouped_subtasks.items(), start=1):
            all_complete = all(t.get('status') == 'complete' for t in task_list)
            main_text_color = "#AAAAAA" if all_complete else "#111111"
            main_decoration = "line-through" if all_complete else "none"
            
            subtask_header_box = {
                "type": "box",
                "layout": "horizontal",
                "spacing": "sm",
                "alignItems": "center",
                "contents": [
                    {
                        "type": "text",
                        "text": "✅" if all_complete else "⏳",
                        "size": "md",
                        "flex": 0
                    },
                    {
                        "type": "text",
                        "text": f"{sub_idx}. {sub_task_name}",
                        "wrap": True,
                        "weight": "bold",
                        "size": "sm",
                        "color": main_text_color,
                        "decoration": main_decoration,
                        "flex": 1
                    }
                ]
            }
            
            assignee_rows = []
            for t in task_list:
                t_id = t.get('task_id')
                assignee = t.get('assignee')
                status = t.get('status', 'incomplete')
                is_comp = (status == 'complete')
                text_dec = "line-through" if is_comp else "none"
                text_col = "#888888" if is_comp else "#1565C0"
                btn_col = "#CCCCCC" if is_comp else "#00B33C"
                btn_lbl = "✓ Xong" if is_comp else "Hoàn tất"
                target_status_param = "incomplete" if is_comp else "complete"

                assignee_row = {
                    "type": "box",
                    "layout": "horizontal",
                    "spacing": "sm",
                    "alignItems": "center",
                    "paddingStart": "24px",
                    "margin": "xs",
                    "contents": [
                        {
                            "type": "text",
                            "text": f"👤 Giao cho: {assignee}",
                            "color": text_col,
                            "size": "xs",
                            "decoration": text_dec,
                            "flex": 1,
                            "wrap": True
                        },
                        {
                            "type": "button",
                            "action": {
                                "type": "postback",
                                "label": btn_lbl,
                                "data": f"action=complete_adhoc_task&task_id={t_id}&assignee={assignee}&target_status={target_status_param}"
                            },
                            "style": "primary",
                            "color": btn_col,
                            "height": "sm",
                            "flex": 0,
                            "width": "75px"
                        }
                    ]
                }
                assignee_rows.append(assignee_row)

            subtask_container = {
                "type": "box",
                "layout": "vertical",
                "paddingAll": "sm",
                "spacing": "xs",
                "contents": [subtask_header_box] + assignee_rows
            }
            task_components.append(subtask_container)
            task_components.append({"type": "separator"})

        if task_components:
            task_components.pop()
            
        flex_content = {
            "type": "bubble",
            "size": "mega",
            "header": {
                "type": "box",
                "layout": "vertical",
                "backgroundColor": "#0288D1",
                "paddingTop": "16px",
                "paddingBottom": "16px",
                "paddingStart": "20px",
                "paddingEnd": "20px",
                "contents": [
                    {
                        "type": "text",
                        "text": "📋 DANH SÁCH CHECKLIST CÔNG VIỆC",
                        "weight": "bold",
                        "size": "xs",
                        "color": "#B3E5FC"
                    },
                    {
                        "type": "text",
                        "text": main_job_name,
                        "weight": "bold",
                        "size": "lg",
                        "color": "#FFFFFF",
                        "margin": "sm",
                        "wrap": True
                    },
                    {
                        "type": "text",
                        "text": f"📅 Ngày giao: {today_display_str}  |  🕒 Giao lúc: {created_at}",
                        "size": "xs",
                        "color": "#B3E5FC",
                        "margin": "xs"
                    }
                ]
            },
            "body": {
                "type": "box",
                "layout": "vertical",
                "spacing": "sm",
                "paddingAll": "sm",
                "contents": task_components
            }
        }
        return flex_content
    except Exception as e:
        print(f"Lỗi khi tạo flex checklist công việc: {e}")
        return None