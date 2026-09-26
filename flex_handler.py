from datetime import datetime
import pytz
# Import từ file cấu hình trung tâm
from config import CLIENT, SHEET_NAME, WORKSHEET_TRACKER_NAME, get_spreadsheet, MAIN_CHECKLIST_GROUP_ID

# --- Danh sách công việc ---
TASKS = {
    'sang': [
        {'id': 'sang_1', 'icon': '📦', 'name': 'Check lệnh chuyển kho online', 'time': '09:15'},
        {'id': 'sang_2', 'icon': '🚚', 'name': 'Check đơn GHTK chuyển kho', 'time': '09:30'},
        {'id': 'sang_3', 'icon': '🏷️', 'name': 'Chạy tủ, thay giá (thứ 2 & 5)', 'time': '10:00'},
        {'id': 'sang_4', 'icon': '🧹', 'name': 'Rà soát tốc kệ', 'time': '10:30'},
        {'id': 'sang_5', 'icon': '📑', 'name': 'Check Phiếu CK/NK quá 7 ngày', 'time': '11:30'},
        {'id': 'sang_6', 'icon': '🔧', 'name': 'Đổ tồn hàng T.Thái (lỗi)', 'time': '14:00'},
        {'id': 'sang_7', 'icon': '📊', 'name': 'Xử lý BCNB sáng', 'time': '15:00'},
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
            ss = get_spreadsheet()
            if not ss:
                return {}
            sheet = ss.worksheet(WORKSHEET_TRACKER_NAME)
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

    if task_components and task_components[-1].get("type") == "separator":
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

def get_adhoc_tasks_for_group_today(group_id):
    """
    Lấy toàn bộ công việc phát sinh trong ngày của nhóm (mọi nhân viên).
    """
    sheet = get_or_create_adhoc_worksheet()
    if not sheet or not group_id:
        return []
    
    try:
        all_records = sheet.get_all_records()
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
        
        filtered_tasks = []
        for record in all_records:
            if (str(record.get('group_id')) == str(group_id) and 
                record.get('date') == today_str):
                filtered_tasks.append(record)
        return filtered_tasks
    except Exception as e:
        print(f"Lỗi khi lấy adhoc tasks của nhóm hôm nay: {e}")
        return []

def task_group_token(task_id):
    """
    'all_ab12cd_3' -> 'ab12cd' ; 'multi_ab12cd_3' -> 'ab12cd' ;
    'adhoc_9f3c1d2e' -> giữ nguyên 'adhoc_9f3c1d2e'.
    """
    tid = str(task_id)
    parts = tid.split('_')
    if len(parts) >= 3 and parts[0] in ('all', 'multi'):
        return parts[1]
    return tid


def _adhoc_layout(sheet):
    """Trả về (headers, chỉ_số_cột) hoặc (None, None)."""
    all_values = sheet.get_all_values()
    if len(all_values) <= 1:
        return (None, None)
    headers = all_values[0]
    try:
        idx = (headers.index('group_id'), headers.index('date'),
               headers.index('task_id'), headers.index('task_name'))
    except ValueError:
        return (None, None)
    return (all_values, idx)


def find_latest_task_token(group_id):
    """
    Tìm nhóm việc MỚI NHẤT trong hôm nay của nhóm chat này.
    Trả về (token, nhãn, số_dòng) hoặc (None, None, 0).
    """
    sheet = get_or_create_adhoc_worksheet()
    if not sheet:
        return (None, None, 0)
    try:
        all_values, idx = _adhoc_layout(sheet)
        if not all_values:
            return (None, None, 0)
        i_gid, i_date, i_tid, i_name = idx
        today_str = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).strftime('%Y-%m-%d')

        rows = [r for r in all_values[1:]
                if len(r) > max(i_gid, i_date, i_tid)
                and str(r[i_gid]) == str(group_id) and r[i_date] == today_str]
        if not rows:
            return (None, None, 0)

        last = rows[-1]
        token = task_group_token(str(last[i_tid]))
        label = last[i_name] if len(last) > i_name else ''
        count = sum(1 for r in rows if task_group_token(str(r[i_tid])) == token)
        return (token, label, count)
    except Exception as e:
        print(f"Lỗi tìm nhóm việc mới nhất: {e}")
        return (None, None, 0)


def cancel_task_group(group_id, token):
    """
    Xoá toàn bộ dòng việc thuộc một nhóm trong HÔM NAY.
    token: hash nhóm (all_/multi_) hoặc task_id đầy đủ (adhoc_).
    Trả về (số_dòng_đã_xoá, [tên_việc...]).
    """
    sheet = get_or_create_adhoc_worksheet()
    if not sheet:
        return (0, [])
    try:
        all_values, idx = _adhoc_layout(sheet)
        if not all_values:
            return (0, [])
        i_gid, i_date, i_tid, i_name = idx
        today_str = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).strftime('%Y-%m-%d')

        keep, names, removed = [], [], 0
        for r in all_values[1:]:
            if len(r) <= max(i_gid, i_date, i_tid):
                keep.append(r)
                continue
            tid = str(r[i_tid])
            hit = (str(r[i_gid]) == str(group_id) and r[i_date] == today_str
                   and (tid == token or tid.startswith(token + "_")
                        or task_group_token(tid) == token))
            if hit:
                removed += 1
                if len(r) > i_name and r[i_name] not in names:
                    names.append(r[i_name])
            else:
                keep.append(r)

        if removed == 0:
            return (0, [])

        sheet.clear()
        sheet.append_row(all_values[0])
        if keep:
            sheet.append_rows(keep, value_input_option='USER_ENTERED')
        print(f"Đã hủy {removed} dòng việc của nhóm {token}.")
        return (removed, names)
    except Exception as e:
        print(f"Lỗi hủy nhóm việc: {e}")
        return (0, [])


def build_cancel_footer(task_group_hash):
    """Nút hủy nhóm việc ở chân thẻ Flex."""
    return {
        "type": "box",
        "layout": "vertical",
        "spacing": "sm",
        "paddingAll": "sm",
        "contents": [
            {
                "type": "button",
                "style": "secondary",
                "height": "sm",
                "action": {
                    "type": "postback",
                    "label": "🗑 Hủy nhóm việc này",
                    "data": f"action=cancel_task_group&token={task_group_hash}"
                }
            }
        ]
    }


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

def generate_today_adhoc_flex(group_id):
    """
    Tạo Flex Bubble hiển thị TỔNG HỢP toàn bộ công việc giao thêm trong ngày hôm nay của nhóm (từ việc 1 đến việc N).
    Mỗi việc hiển thị:
      - Số thứ tự + Tên công việc (kèm icon ⏳ hoặc ✅)
      - Thời gian giao việc (chuyển xuống dưới công việc: 🕒 Giao lúc: HH:MM)
      - Danh sách người nhận việc kèm nút [ Hoàn tất ] / [ ✓ Xong ]
    """
    tasks_data = get_adhoc_tasks_for_group_today(group_id)
    if not tasks_data:
        return None

    try:
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        today_display_str = datetime.now(tz_vietnam).strftime('%d/%m/%Y')

        # Gom nhóm các dòng việc theo đầu việc và thứ tự giao
        ordered_tasks = []
        seen_groups = {}  # group_key -> index in ordered_tasks
        latest_token = None

        for record in tasks_data:
            task_id = str(record.get('task_id', ''))
            task_name_val = str(record.get('task_name', 'Công việc phát sinh')).strip()
            token = task_group_token(task_id)
            if token:
                latest_token = token

            created_at = str(record.get('created_at', '')).strip()

            # Phân tích xem có tiêu đề việc chung (dạng "In giá | Tivi")
            parent_job = None
            sub_task = task_name_val

            if ' | ' in task_name_val:
                p_parts = task_name_val.split(' | ', 1)
                p_lead = p_parts[0].strip()
                p_sub = p_parts[1].strip()
                if p_lead and p_lead not in ("Việc phát sinh", ""):
                    parent_job = p_lead
                    sub_task = p_sub
                else:
                    sub_task = p_sub

            assignee_data = {
                'task_id': task_id,
                'assignee': record.get('assignee', ''),
                'status': record.get('status', 'incomplete'),
                'completed_by': record.get('completed_by', ''),
                'completed_at': record.get('completed_at', '')
            }

            if parent_job:
                # 1 việc chia nhiều nhánh (ví dụ: Việc In giá -> Tivi, tủ lạnh)
                group_key = (token, parent_job)
                if group_key not in seen_groups:
                    seen_groups[group_key] = len(ordered_tasks)
                    ordered_tasks.append({
                        'is_branched': True,
                        'title': parent_job,
                        'created_at': created_at,
                        'token': token,
                        'branches': {}  # sub_task -> list of assignees
                    })
                idx = seen_groups[group_key]
                if not ordered_tasks[idx]['created_at'] and created_at:
                    ordered_tasks[idx]['created_at'] = created_at

                if sub_task not in ordered_tasks[idx]['branches']:
                    ordered_tasks[idx]['branches'][sub_task] = []
                ordered_tasks[idx]['branches'][sub_task].append(assignee_data)
            else:
                # Việc đơn lẻ không chia nhánh
                group_key = (token, sub_task)
                if group_key not in seen_groups:
                    seen_groups[group_key] = len(ordered_tasks)
                    ordered_tasks.append({
                        'is_branched': False,
                        'title': sub_task,
                        'created_at': created_at,
                        'token': token,
                        'assignees': []
                    })
                idx = seen_groups[group_key]
                if not ordered_tasks[idx]['created_at'] and created_at:
                    ordered_tasks[idx]['created_at'] = created_at
                ordered_tasks[idx]['assignees'].append(assignee_data)

        if not ordered_tasks:
            return None

        task_components = []
        for stt, item in enumerate(ordered_tasks, start=1):
            if item.get('is_branched'):
                # --- TRƯỜNG HỢP 1 VIỆC CHIA NHIỀU NHÁNH ---
                all_assignees = [a for b_list in item['branches'].values() for a in b_list]
                all_complete = all(a.get('status') == 'complete' for a in all_assignees)
                main_text_color = "#888888" if all_complete else "#111111"
                main_decoration = "line-through" if all_complete else "none"

                # 1. Dòng tiêu đề mục lớn (ví dụ: ⏳ 3. In giá)
                main_header_box = {
                    "type": "box",
                    "layout": "horizontal",
                    "spacing": "sm",
                    "alignItems": "flex-start",
                    "contents": [
                        {
                            "type": "text",
                            "text": "✅" if all_complete else "⏳",
                            "size": "md",
                            "flex": 0
                        },
                        {
                            "type": "text",
                            "text": f"{stt}. {item['title']}",
                            "wrap": True,
                            "weight": "bold",
                            "size": "sm",
                            "color": main_text_color,
                            "decoration": main_decoration,
                            "flex": 1
                        }
                    ]
                }

                # 2. Dòng thời gian giao của việc lớn
                time_row = None
                created_at_str = item.get('created_at')
                if created_at_str:
                    time_row = {
                        "type": "box",
                        "layout": "horizontal",
                        "spacing": "xs",
                        "paddingStart": "24px",
                        "margin": "xs",
                        "contents": [
                            {
                                "type": "text",
                                "text": f"🕒 Giao lúc: {created_at_str}",
                                "size": "xxs",
                                "color": "#888888"
                            }
                        ]
                    }

                # 3. Các nhánh công việc con bên dưới
                branch_contents = []
                for sub_name, b_assignees in item['branches'].items():
                    sub_complete = all(a.get('status') == 'complete' for a in b_assignees)
                    sub_text_col = "#888888" if sub_complete else "#222222"
                    sub_text_dec = "line-through" if sub_complete else "none"

                    # Dòng tên nhánh con (ví dụ: 🔹 Tivi hoặc 🔹 tủ lạnh)
                    branch_title_row = {
                        "type": "box",
                        "layout": "horizontal",
                        "spacing": "xs",
                        "alignItems": "center",
                        "paddingStart": "24px",
                        "marginTop": "sm",
                        "contents": [
                            {
                                "type": "text",
                                "text": "🔹",
                                "size": "xs",
                                "flex": 0
                            },
                            {
                                "type": "text",
                                "text": sub_name,
                                "weight": "bold",
                                "size": "xs",
                                "color": sub_text_col,
                                "decoration": sub_text_dec,
                                "flex": 1,
                                "wrap": True
                            }
                        ]
                    }
                    branch_contents.append(branch_title_row)

                    # Danh sách người nhận việc trong nhánh con
                    for a in b_assignees:
                        t_id = a.get('task_id')
                        assignee = a.get('assignee')
                        status = a.get('status', 'incomplete')
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
                            "paddingStart": "36px",
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
                        branch_contents.append(assignee_row)

                item_contents = [main_header_box]
                if time_row:
                    item_contents.append(time_row)
                item_contents.extend(branch_contents)

            else:
                # --- TRƯỜNG HỢP VIỆC ĐƠN LẺ KHÔNG CHIA NHÁNH ---
                all_complete = all(a.get('status') == 'complete' for a in item['assignees'])
                main_text_color = "#888888" if all_complete else "#111111"
                main_decoration = "line-through" if all_complete else "none"

                subtask_header_box = {
                    "type": "box",
                    "layout": "horizontal",
                    "spacing": "sm",
                    "alignItems": "flex-start",
                    "contents": [
                        {
                            "type": "text",
                            "text": "✅" if all_complete else "⏳",
                            "size": "md",
                            "flex": 0
                        },
                        {
                            "type": "text",
                            "text": f"{stt}. {item['title']}",
                            "wrap": True,
                            "weight": "bold",
                            "size": "sm",
                            "color": main_text_color,
                            "decoration": main_decoration,
                            "flex": 1
                        }
                    ]
                }

                time_row = None
                created_at_str = item.get('created_at')
                if created_at_str:
                    time_row = {
                        "type": "box",
                        "layout": "horizontal",
                        "spacing": "xs",
                        "paddingStart": "24px",
                        "margin": "xs",
                        "contents": [
                            {
                                "type": "text",
                                "text": f"🕒 Giao lúc: {created_at_str}",
                                "size": "xxs",
                                "color": "#888888"
                            }
                        ]
                    }

                assignee_rows = []
                for a in item['assignees']:
                    t_id = a.get('task_id')
                    assignee = a.get('assignee')
                    status = a.get('status', 'incomplete')
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

                item_contents = [subtask_header_box]
                if time_row:
                    item_contents.append(time_row)
                item_contents.extend(assignee_rows)

            subtask_container = {
                "type": "box",
                "layout": "vertical",
                "paddingAll": "sm",
                "spacing": "xs",
                "contents": item_contents
            }
            task_components.append(subtask_container)
            task_components.append({"type": "separator"})

        if task_components and task_components[-1].get("type") == "separator":
            task_components.pop()

        header_contents = [
            {
                "type": "text",
                "text": "📋 CÔNG VIỆC GIAO THÊM",
                "weight": "bold",
                "size": "md",
                "color": "#FFFFFF"
            },
            {
                "type": "text",
                "text": f"📅 Ngày giao: {today_display_str}",
                "size": "xxs",
                "color": "#B3E5FC",
                "margin": "xs"
            }
        ]

        flex_content = {
            "type": "bubble",
            "size": "mega",
            "header": {
                "type": "box",
                "layout": "vertical",
                "backgroundColor": "#0288D1",
                "paddingTop": "14px",
                "paddingBottom": "14px",
                "paddingStart": "16px",
                "paddingEnd": "16px",
                "contents": header_contents
            },
            "body": {
                "type": "box",
                "layout": "vertical",
                "spacing": "sm",
                "paddingAll": "sm",
                "contents": task_components
            }
        }
        if latest_token:
            flex_content["footer"] = build_cancel_footer(latest_token)

        return flex_content
    except Exception as e:
        print(f"Lỗi khi tạo flex tổng hợp công việc trong ngày: {e}")
        return None

def generate_all_adhoc_flex(group_id, task_group_hash=None):
    """Ủy quyền sang generate_today_adhoc_flex để luôn hiển thị đầy đủ mọi việc hôm nay."""
    return generate_today_adhoc_flex(group_id)

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
        if job_name and str(job_name).strip() and str(job_name).strip() != "Việc phát sinh":
            combined_task_name = f"{str(job_name).strip()} | {sub_task}"
        else:
            combined_task_name = sub_task
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

def generate_multi_adhoc_flex(group_id, task_group_hash=None):
    """Ủy quyền sang generate_today_adhoc_flex để luôn hiển thị đầy đủ mọi việc hôm nay."""
    return generate_today_adhoc_flex(group_id)


def generate_latest_adhoc_flex(group_id):
    """
    Hiển thị toàn bộ công việc giao thêm hôm nay của nhóm (gộp từ việc 1 đến việc N).
    """
    return generate_today_adhoc_flex(group_id)


def build_work_carousel(checklist_bubble, adhoc_bubble):
    """
    Tạo Flex Carousel hiển thị 2 thẻ song song:
      - Thẻ bên trái: Thẻ checklist ca riêng (sang/chieu/vs)
      - Thẻ bên phải: Thẻ công việc giao thêm riêng (gộp toàn bộ việc hôm nay)
    Nếu chỉ có 1 thẻ thì trả về thẻ đó (bubble).
    """
    bubbles = []
    if checklist_bubble:
        bubbles.append(checklist_bubble)
    if adhoc_bubble:
        bubbles.append(adhoc_bubble)

    if len(bubbles) >= 2:
        return {
            "type": "carousel",
            "contents": bubbles
        }
    elif len(bubbles) == 1:
        return bubbles[0]
    return None


def generate_combined_work_flex(group_id, shift_type=None, adhoc_hash=None, adhoc_mode=None):
    """
    Tạo Flex hiển thị công việc:
      - CHỈ nhóm MAIN_CHECKLIST_GROUP_ID (C37e48216804398593d8c79fe3edacdc7) mới gộp 2 thẻ:
          + Thẻ bên trái: Thẻ checklist ca riêng (sang/chieu/vs)
          + Thẻ bên phải: Thẻ công việc giao thêm riêng (gộp toàn bộ việc hôm nay từ việc 1 đến việc N)
      - Các nhóm khác:
          + KHÔNG được gộp công việc ca sáng/chiều vào.
          + Trả về thẻ công việc giao thêm riêng (nếu có), hoặc thẻ checklist ca riêng (nếu gọi checklist).
    """
    adhoc_bubble = generate_today_adhoc_flex(group_id)

    # Chỉ nhóm C37e48216804398593d8c79fe3edacdc7 mới được gộp 2 thẻ
    if group_id == MAIN_CHECKLIST_GROUP_ID:
        if not shift_type:
            tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
            current_hour = datetime.now(tz_vietnam).hour
            shift_type = 'sang' if current_hour < 15 else 'chieu'

        checklist_bubble = generate_checklist_flex(group_id, shift_type)
        return build_work_carousel(checklist_bubble, adhoc_bubble)

    # Các nhóm khác: KHÔNG gộp checklist ca vào việc giao thêm
    if adhoc_bubble:
        return adhoc_bubble
    if shift_type:
        return generate_checklist_flex(group_id, shift_type)
    return None
