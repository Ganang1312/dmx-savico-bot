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
        {'id': 'vs_1', 'icon': '📸', 'name': '1. 396 NVC', 'time': '10:00'},
        {'id': 'vs_2', 'icon': '📸', 'name': '2. Cụm 5468', 'time': '10:00'},
        {'id': 'vs_3', 'icon': '📸', 'name': '3. Ngọc Lâm', 'time': '10:00'},
        {'id': 'vs_4', 'icon': '📸', 'name': '4. Ngọc Trì', 'time': '10:00'},
        {'id': 'vs_5', 'icon': '📸', 'name': '5. Ngô Gia Tự', 'time': '10:00'},
        {'id': 'vs_6', 'icon': '📸', 'name': '6. Savico', 'time': '10:00'},
        {'id': 'vs_7', 'icon': '📸', 'name': '7. Ngọc Thụy', 'time': '10:00'},
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
        target_gid = str(group_id or '').strip()
        
        for record in all_records:
            r_gid = str(record.get('group_id', '')).strip()
            r_date = str(record.get('date', '')).strip()
            if (not target_gid or r_gid == target_gid) and r_date == today_str:
                task_id = str(record.get('task_id', '')).strip()
                if task_id and task_id.startswith(shift_type):
                    task_statuses[task_id] = {
                        'status': str(record.get('status', 'incomplete')).strip(),
                        'user_name': str(record.get('user_name', '')).strip()
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
                        "data": f"action=complete_task&task_id={task['id']}&shift={shift_type}&target_status={target_status_param}&group_id={group_id}"
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

def _get_adhoc_col_indices(headers):
    """
    Xác định vị trí các cột trong sheet adhoc_tasks linh hoạt,
    bất kể viết hoa, thường, có khoảng trắng hay dấu gạch.
    """
    norm = [str(h).strip().lower().replace(' ', '_').replace('-', '_') for h in headers]
    
    def find_idx(candidates, default_val):
        for cand in candidates:
            if cand in norm:
                return norm.index(cand)
        return default_val

    i_gid = find_idx(['group_id', 'groupid', 'gid', 'group', 'nhom', 'mã_nhóm', 'nhóm'], 0)
    i_date = find_idx(['date', 'ngay', 'ngày', 'ngay_giao'], 1)
    i_assignee = find_idx(['assignee', 'nhan_vien', 'nguoi_nhan', 'ten_nv', 'nhân_viên', 'người_nhận'], 2)
    i_tid = find_idx(['task_id', 'taskid', 'id', 'ma_viec'], 3)
    i_name = find_idx(['task_name', 'taskname', 'name', 'ten_viec', 'cong_viec', 'tên_việc', 'công_việc'], 4)
    i_status = find_idx(['status', 'trang_thai', 'trạng_thái'], 5)
    i_comp_by = find_idx(['completed_by', 'nguoi_hoan_thanh', 'xong_boi', 'người_hoàn_thành'], 6)
    i_comp_at = find_idx(['completed_at', 'thoi_gian_xong', 'xong_luc', 'thời_gian_xong'], 7)
    i_created_at = find_idx(['created_at', 'thoi_gian_giao', 'giao_luc', 'tao_luc', 'thời_gian_giao'], 8)
    
    return {
        'group_id': i_gid,
        'date': i_date,
        'assignee': i_assignee,
        'task_id': i_tid,
        'task_name': i_name,
        'status': i_status,
        'completed_by': i_comp_by,
        'completed_at': i_comp_at,
        'created_at': i_created_at
    }

def _build_adhoc_row(headers, group_id, date_str, assignee, task_id, task_name, status, comp_by, comp_at, created_at):
    """
    Tạo dòng dữ liệu mới khớp chính xác 100% với cấu trúc cột thực tế của sheet.
    """
    cols = _get_adhoc_col_indices(headers)
    max_idx = max(max(cols.values()), len(headers) - 1, 8)
    row = [''] * (max_idx + 1)
    row[cols['group_id']] = str(group_id).strip()
    row[cols['date']] = str(date_str).strip()
    row[cols['assignee']] = str(assignee).strip()
    row[cols['task_id']] = str(task_id).strip()
    row[cols['task_name']] = str(task_name).strip()
    row[cols['status']] = str(status).strip()
    row[cols['completed_by']] = str(comp_by).strip()
    row[cols['completed_at']] = str(comp_at).strip()
    row[cols['created_at']] = str(created_at).strip()
    return row

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
        cols = _get_adhoc_col_indices(headers)
        i_date = cols['date']
        
        rows_to_keep = []
        has_old_rows = False
        for row in all_values[1:]:
            if len(row) > i_date and str(row[i_date]).strip() == today_str:
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
    if not sheet or not group_id:
        return False
        
    clean_old_adhoc_tasks(sheet)
    
    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
    time_now_str = datetime.now(tz_vietnam).strftime('%H:%M')
    
    try:
        all_values = sheet.get_all_values()
        headers = all_values[0] if all_values else ['group_id', 'date', 'assignee', 'task_id', 'task_name', 'status', 'completed_by', 'completed_at', 'created_at']
        
        rows_to_add = []
        for task_name in tasks_list:
            task_id = f"adhoc_{uuid.uuid4().hex[:8]}"
            new_row = _build_adhoc_row(
                headers=headers,
                group_id=group_id,
                date_str=today_str,
                assignee=assignee,
                task_id=task_id,
                task_name=task_name,
                status='incomplete',
                comp_by='',
                comp_at='',
                created_at=time_now_str
            )
            rows_to_add.append(new_row)
            
        if rows_to_add:
            sheet.append_rows(rows_to_add, value_input_option='USER_ENTERED')
            print(f"Đã thêm {len(rows_to_add)} công việc phát sinh cho {assignee} tại group {group_id}")
            return True
        return False
    except Exception as e:
        print(f"Lỗi khi thêm adhoc tasks: {e}")
        return False

def get_adhoc_tasks_for_group_today(group_id):
    """
    Lấy toàn bộ công việc phát sinh trong ngày của nhóm (mọi nhân viên).
    CHỈ lấy công việc của ĐÚNG group_id này, cô lập 100% không lẫn sang nhóm khác.
    """
    sheet = get_or_create_adhoc_worksheet()
    if not sheet or not group_id:
        return []
    
    try:
        all_values = sheet.get_all_values()
        if len(all_values) <= 1:
            return []
            
        cols = _get_adhoc_col_indices(all_values[0])
        i_gid = cols['group_id']
        i_date = cols['date']
        i_assignee = cols['assignee']
        i_tid = cols['task_id']
        i_name = cols['task_name']
        i_status = cols['status']
        i_comp_by = cols['completed_by']
        i_comp_at = cols['completed_at']
        i_created_at = cols['created_at']

        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
        target_gid = str(group_id).strip()

        filtered_tasks = []
        for row in all_values[1:]:
            if len(row) <= max(i_gid, i_date):
                continue
            row_gid = str(row[i_gid]).strip()
            row_date = str(row[i_date]).strip()
            
            # CÔ LẬP TUYỆT ĐỐI THEO GROUP_ID VÀ NGÀY HÔM NAY
            if row_gid == target_gid and row_date == today_str:
                filtered_tasks.append({
                    'group_id': row_gid,
                    'date': row_date,
                    'assignee': str(row[i_assignee]).strip() if len(row) > i_assignee else '',
                    'task_id': str(row[i_tid]).strip() if len(row) > i_tid else '',
                    'task_name': str(row[i_name]).strip() if len(row) > i_name else '',
                    'status': str(row[i_status]).strip() if len(row) > i_status else 'incomplete',
                    'completed_by': str(row[i_comp_by]).strip() if len(row) > i_comp_by else '',
                    'completed_at': str(row[i_comp_at]).strip() if len(row) > i_comp_at else '',
                    'created_at': str(row[i_created_at]).strip() if len(row) > i_created_at else ''
                })
        return filtered_tasks
    except Exception as e:
        print(f"Lỗi khi lấy adhoc tasks của nhóm hôm nay: {e}")
        return []

def get_adhoc_tasks_today(group_id, assignee):
    """
    Lấy danh sách công việc phát sinh hôm nay của nhân viên đó trong nhóm.
    """
    all_group_tasks = get_adhoc_tasks_for_group_today(group_id)
    target_assignee = str(assignee).strip().lower()
    return [t for t in all_group_tasks if str(t.get('assignee', '')).strip().lower() == target_assignee]

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
    """Trả về (all_values, idx) hoặc (None, None)."""
    all_values = sheet.get_all_values()
    if len(all_values) <= 1:
        return (None, None)
    cols = _get_adhoc_col_indices(all_values[0])
    idx = (cols['group_id'], cols['date'], cols['task_id'], cols['task_name'])
    return (all_values, idx)


def find_latest_task_token(group_id):
    """
    Tìm nhóm việc MỚI NHẤT trong hôm nay của nhóm chat này.
    Trả về (token, nhãn, số_dòng) hoặc (None, None, 0).
    """
    sheet = get_or_create_adhoc_worksheet()
    if not sheet or not group_id:
        return (None, None, 0)
    try:
        all_values, idx = _adhoc_layout(sheet)
        if not all_values:
            return (None, None, 0)
        i_gid, i_date, i_tid, i_name = idx
        today_str = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).strftime('%Y-%m-%d')
        target_gid = str(group_id).strip()

        rows = [r for r in all_values[1:]
                if len(r) > max(i_gid, i_date, i_tid)
                and str(r[i_gid]).strip() == target_gid and str(r[i_date]).strip() == today_str]
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
    if not sheet or not group_id or not token:
        return (0, [])
    try:
        all_values, idx = _adhoc_layout(sheet)
        if not all_values:
            return (0, [])
        i_gid, i_date, i_tid, i_name = idx
        today_str = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).strftime('%Y-%m-%d')
        target_gid = str(group_id).strip()

        keep, names, removed = [], [], 0
        for r in all_values[1:]:
            if len(r) <= max(i_gid, i_date, i_tid):
                keep.append(r)
                continue
            r_gid = str(r[i_gid]).strip()
            r_date = str(r[i_date]).strip()
            tid = str(r[i_tid]).strip()
            hit = (r_gid == target_gid and r_date == today_str
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
        print(f"Đã hủy {removed} dòng việc của nhóm {token} trong group {group_id}.")
        return (removed, names)
    except Exception as e:
        print(f"Lỗi hủy nhóm việc: {e}")
        return (0, [])


def update_adhoc_task_status(group_id, task_id, target_status, completed_by):
    """
    Cập nhật trạng thái của adhoc task theo đúng group_id và task_id.
    """
    sheet = get_or_create_adhoc_worksheet()
    if not sheet or not task_id:
        return False, None, None
    
    try:
        all_values = sheet.get_all_values()
        if len(all_values) <= 1:
            return False, None, None

        cols = _get_adhoc_col_indices(all_values[0])
        i_gid = cols['group_id']
        i_tid = cols['task_id']
        i_assignee = cols['assignee']
        i_status = cols['status']
        i_comp_by = cols['completed_by']
        i_comp_at = cols['completed_at']

        time_str = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).strftime('%H:%M')
        target_gid = str(group_id).strip() if group_id else ''

        row_indices = []
        assignee = None
        task_group_hash = None
        clean_tid = str(task_id).strip()

        for i, row in enumerate(all_values[1:], start=2):
            if len(row) <= max(i_gid, i_tid):
                continue
            r_gid = str(row[i_gid]).strip()
            r_tid = str(row[i_tid]).strip()

            if (not target_gid or r_gid == target_gid) and r_tid == clean_tid:
                row_indices.append(i)
                assignee = str(row[i_assignee]).strip() if len(row) > i_assignee else ''
                if clean_tid.startswith('all_') or clean_tid.startswith('multi_'):
                    parts = clean_tid.split('_')
                    if len(parts) >= 3:
                        task_group_hash = parts[1]

        # Fallback: nếu group_id lệch nhưng task_id (chứa mã hash) khớp chính xác
        if not row_indices:
            for i, row in enumerate(all_values[1:], start=2):
                if len(row) <= i_tid:
                    continue
                r_tid = str(row[i_tid]).strip()
                if r_tid == clean_tid:
                    row_indices.append(i)
                    assignee = str(row[i_assignee]).strip() if len(row) > i_assignee else ''
                    if clean_tid.startswith('all_') or clean_tid.startswith('multi_'):
                        parts = clean_tid.split('_')
                        if len(parts) >= 3:
                            task_group_hash = parts[1]

        if row_indices:
            comp_by = completed_by if target_status == 'complete' else ''
            comp_at = time_str if target_status == 'complete' else ''

            import gspread.utils
            batch_data = []
            for r_idx in row_indices:
                col_letter_status = gspread.utils.rowcol_to_a1(r_idx, i_status + 1)
                col_letter_comp_by = gspread.utils.rowcol_to_a1(r_idx, i_comp_by + 1)
                col_letter_comp_at = gspread.utils.rowcol_to_a1(r_idx, i_comp_at + 1)
                batch_data.extend([
                    {'range': col_letter_status, 'values': [[target_status]]},
                    {'range': col_letter_comp_by, 'values': [[comp_by]]},
                    {'range': col_letter_comp_at, 'values': [[comp_at]]},
                ])
            sheet.batch_update(batch_data, value_input_option='USER_ENTERED')
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
    if not sheet or not group_id:
        return None
        
    clean_old_adhoc_tasks(sheet)
        
    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
    time_now_str = datetime.now(tz_vietnam).strftime('%H:%M')
    
    task_group_hash = uuid.uuid4().hex[:6]
    
    try:
        all_values = sheet.get_all_values()
        headers = all_values[0] if all_values else ['group_id', 'date', 'assignee', 'task_id', 'task_name', 'status', 'completed_by', 'completed_at', 'created_at']
        
        rows_to_add = []
        for index, member in enumerate(members):
            task_id = f"all_{task_group_hash}_{index}"
            new_row = _build_adhoc_row(
                headers=headers,
                group_id=group_id,
                date_str=today_str,
                assignee=member,
                task_id=task_id,
                task_name=task_name,
                status='incomplete',
                comp_by='',
                comp_at='',
                created_at=time_now_str
            )
            rows_to_add.append(new_row)
            
        if rows_to_add:
            sheet.append_rows(rows_to_add, value_input_option='USER_ENTERED')
            print(f"Đã thêm việc @all {task_name} cho {len(rows_to_add)} thành viên tại group {group_id}")
            return task_group_hash
        return None
    except Exception as e:
        print(f"Lỗi khi thêm adhoc tasks @all: {e}")
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
                        text_col = "#94a3b8" if is_comp else "#0f172a"
                        text_weight = "regular" if is_comp else "bold"
                        text_size = "xs" if is_comp else "sm"
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
                                    "text": f"👤 {assignee}",
                                    "color": text_col,
                                    "size": text_size,
                                    "weight": text_weight,
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
                    text_col = "#94a3b8" if is_comp else "#0f172a"
                    text_weight = "regular" if is_comp else "bold"
                    text_size = "xs" if is_comp else "sm"
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
                                "text": f"👤 {assignee}",
                                "color": text_col,
                                "size": text_size,
                                "weight": text_weight,
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
    if not sheet or not group_id:
        return None
        
    clean_old_adhoc_tasks(sheet)
    
    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
    time_now_str = datetime.now(tz_vietnam).strftime('%H:%M')
    
    task_group_hash = uuid.uuid4().hex[:6]
    
    try:
        all_values = sheet.get_all_values()
        headers = all_values[0] if all_values else ['group_id', 'date', 'assignee', 'task_id', 'task_name', 'status', 'completed_by', 'completed_at', 'created_at']
        
        rows_to_add = []
        for index, (sub_task, assignee) in enumerate(task_assignments):
            task_id = f"multi_{task_group_hash}_{index}"
            if job_name and str(job_name).strip() and str(job_name).strip() != "Việc phát sinh":
                combined_task_name = f"{str(job_name).strip()} | {sub_task}"
            else:
                combined_task_name = sub_task
            new_row = _build_adhoc_row(
                headers=headers,
                group_id=group_id,
                date_str=today_str,
                assignee=assignee,
                task_id=task_id,
                task_name=combined_task_name,
                status='incomplete',
                comp_by='',
                comp_at='',
                created_at=time_now_str
            )
            rows_to_add.append(new_row)
            
        if rows_to_add:
            sheet.append_rows(rows_to_add, value_input_option='USER_ENTERED')
            print(f"Đã thêm checklist công việc '{job_name}' cho {len(rows_to_add)} nhân sự tại group {group_id}")
            return task_group_hash
        return None
    except Exception as e:
        print(f"Lỗi khi thêm multi adhoc tasks: {e}")
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
    if str(group_id).strip() == str(MAIN_CHECKLIST_GROUP_ID).strip():
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


# ============================================================================
# HÀM TIỆN ÍCH GỬI CHECKLIST ĐỊNH KỲ (GỘP TỪ checklist_scheduler.py)
# ============================================================================
from linebot.models import FlexSendMessage as _FlexSendMessage

def get_checklist_message(shift_type, group_id):
    """Khởi tạo công việc và trả về đối tượng FlexSendMessage cho ca làm việc."""
    try:
        if not group_id:
            print("Lỗi: Không có Group ID để tạo checklist.")
            return None
        initialize_daily_tasks(group_id, shift_type)
        flex_content = generate_checklist_flex(group_id, shift_type)
        if flex_content:
            return _FlexSendMessage(
                alt_text=f"Checklist công việc ca {shift_type}",
                contents=flex_content
            )
        return None
    except Exception as e:
        print(f"Lỗi tạo checklist message ca {shift_type}: {e}")
        return None


def send_initial_checklist(shift_type):
    """Hàm tương thích gửi checklist đơn lẻ theo biến môi trường CHECKLIST_GROUP_ID."""
    import os
    from linebot import LineBotApi
    token = os.environ.get('CHANNEL_ACCESS_TOKEN')
    chk_gid = os.environ.get('CHECKLIST_GROUP_ID')
    if not token or not chk_gid:
        print("Thiếu CHANNEL_ACCESS_TOKEN hoặc CHECKLIST_GROUP_ID")
        return
    try:
        msg = get_checklist_message(shift_type, chk_gid)
        if msg:
            LineBotApi(token).push_message(chk_gid, msg)
    except Exception as e:
        print(f"Lỗi trong send_initial_checklist: {e}")


# ============================================================================
# BỘ PHÂN TÍCH CÚ PHÁP GIAO VIỆC (GỘP TỪ task_parser.py)
# ============================================================================
import re
from meal_handler import clean_person_name, normalize_person_key

GROUP_KEYWORDS = {
    "all": ("PG", "NV", "QL+TC"),
    "nv": ("NV",),
    "pg": ("PG",),
    "st": ("NV", "QL+TC"),
    "ql": ("QL+TC",),
    "tc": ("QL+TC",),
}

GROUP_LABEL = {
    "all": "toàn siêu thị (PG + NV + QL + TC)",
    "nv": "nhân viên (NV)",
    "pg": "PG",
    "st": "nhân viên siêu thị (NV + QL + TC)",
    "ql": "quản lý + trưởng ca",
    "tc": "quản lý + trưởng ca",
}

PLACEHOLDER = {
    "", "khong co", "khong", "trong", "n a", "na", "none", "null",
    "khong co ai", "chua co", "-", "--", "0",
}

BULLETS = "-*•+–—"

_ONE_LINE_RE = re.compile(r"^\s*\S+\s+@[^:]+:\s*(\S.*)$")


def is_work_command(lines):
    """True nếu các dòng này là một lệnh `việc`."""
    lines = [l.strip() for l in lines if l.strip()]
    if not lines:
        return False
    head = lines[0].lower()
    if not (head.startswith("việc") or head.startswith("viec")):
        return False
    if len(lines) >= 2 and lines[1][:1] in BULLETS:
        return True
    return bool(_ONE_LINE_RE.match(lines[0]))


def _expand_one_line(lines):
    """Biến 'việc @tuấn: dọn kho' thành ['việc @tuấn', '- dọn kho']."""
    if len(lines) != 1:
        return lines
    s = lines[0]
    i = s.find(":")
    if i == -1 or "@" not in s[:i]:
        return lines
    return [s[:i].strip() + ":", "- " + s[i + 1:].strip()]


def is_placeholder(name):
    """True nếu chuỗi không phải tên người (vd 'Không có')."""
    return normalize_person_key(name) in PLACEHOLDER


def split_names(cell):
    """Tách một ô sheet thành danh sách tên đã làm sạch và khử trùng."""
    if not cell:
        return []
    out, seen = [], set()
    for token in re.split(r"[,\n;•+]", str(cell)):
        name = clean_person_name(token)
        if not name or name.isdigit() or len(name) < 2:
            continue
        if is_placeholder(name):
            continue
        key = normalize_person_key(name)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(name)
    return out


def extract_code(name):
    """Lấy mã số trong tên dạng 'Thảo-156494*' -> '156494'."""
    m = re.search(r"[-–]\s*(\d{2,8})", str(name))
    return m.group(1) if m else None


def build_token_index(names):
    """Trả về list [(token, tên_đầy_đủ)] sắp theo độ dài GIẢM DẦN."""
    idx = []
    for nm in names:
        idx.append((nm, nm))
        code = extract_code(nm)
        if code:
            idx.append((code, nm))
        alias = re.split(r"\s*[-–]\s*", nm)[0].strip()
        if alias and alias != nm and len(alias) >= 2:
            idx.append((alias, nm))
    idx.sort(key=lambda t: len(t[0]), reverse=True)
    return idx


def build_known_tokens(index):
    """Danh sách token để dò sau dấu @, dài trước ngắn sau."""
    toks = [t for t, _ in index] + list(GROUP_KEYWORDS.keys())
    return sorted(set(toks), key=len, reverse=True)


def hint(err, names):
    if err == "ambiguous":
        return " Trùng %d người: %s." % (len(names), ", ".join(names))
    if err == "empty":
        return " Nhóm này đang không có ai trong danh sách."
    return " Không có trong danh sách nhân sự của siêu thị."


MENTION_SPACE = "\uE000"


def roster_code_set(roster_names):
    """Tập mã NV (chuỗi số) có trong roster — dùng để nhận diện mention."""
    idx = build_token_index([nm for grp in roster_names.values() for nm in grp])
    return {tok for tok, _ in idx if tok.isdigit()}


def _mention_code(display, code_set):
    """Mã NV (2-8 chữ số) ĐẦU TIÊN trong tên hiển thị mà CÓ trong roster."""
    for m in re.finditer(r"\d{2,8}", str(display)):
        if m.group(0) in code_set:
            return m.group(0)
    return None


def resolve_mentions(raw_text, mentionees, roster_names=None):
    """Viết lại raw_text để mỗi MENTION THẬT trở thành MỘT token duy nhất."""
    if not mentionees or not raw_text:
        return raw_text

    utf16_to_char = {}
    current_utf16 = 0
    for char_idx, ch in enumerate(raw_text):
        utf16_to_char[current_utf16] = char_idx
        current_utf16 += 2 if ord(ch) > 0xFFFF else 1
    utf16_to_char[current_utf16] = len(raw_text)

    sorted_mentions = sorted(mentionees, key=lambda x: int(x["index"]), reverse=True)
    out = raw_text

    for m in sorted_mentions:
        idx = int(m["index"])
        ln = int(m["length"])
        end_idx = idx + ln

        char_start = utf16_to_char.get(idx)
        if char_start is None:
            keys = sorted(utf16_to_char.keys())
            char_start = utf16_to_char[max(k for k in keys if k <= idx)] if any(k <= idx for k in keys) else 0

        char_end = utf16_to_char.get(end_idx)
        if char_end is None:
            keys = sorted(utf16_to_char.keys())
            char_end = utf16_to_char[min(k for k in keys if k >= end_idx)] if any(k >= end_idx for k in keys) else len(raw_text)

        seg = out[char_start:char_end]
        if not seg:
            continue

        overflow = ""
        if "\n" in seg:
            seg, overflow = seg.split("\n", 1)
            overflow = "\n" + overflow

        display = seg[1:] if seg.startswith("@") else seg

        if str(m.get("type", "user")).lower() == "all":
            token = "all"
        else:
            token = display.replace(" ", MENTION_SPACE).strip() or "?"

        out = out[:char_start] + "@" + token + overflow + out[char_end:]

    return out


def resolve_target(token, roster_names, index):
    """Trả về (danh_sách_tên, lỗi). lỗi ∈ (None, 'empty')."""
    t = str(token).strip().replace(MENTION_SPACE, " ")
    low = t.lower()

    if low in GROUP_KEYWORDS:
        out = []
        for grp in GROUP_KEYWORDS[low]:
            for nm in roster_names.get(grp, []):
                if nm not in out:
                    out.append(nm)
        return (out, None) if out else ([], "empty")

    if re.fullmatch(r"\d{2,8}", t):
        cand = []
        for tok, nm in index:
            if tok == t and nm not in cand:
                cand.append(nm)
        if len(cand) == 1:
            return (cand, None)

    return ([t], None) if t else ([], None)


def split_line(line, known_tokens, known_norm=None, allow_colon=False):
    """Tách 1 dòng thành (nội_dung_việc, [đích...]). @ nhận ở BẤT KỲ vị trí nào."""
    if known_norm is None:
        known_norm = {normalize_person_key(t) for t in known_tokens}

    spans, tokens = [], []
    i = 0
    while True:
        at = line.find("@", i)
        if at == -1:
            break
        rest = line[at + 1:]

        best = ""
        words = re.split(r"\s+", rest)
        for n in range(min(4, len(words)), 0, -1):
            cand = " ".join(words[:n]).rstrip(",;:.")
            if cand and normalize_person_key(cand) in known_norm:
                best = cand
                break

        if not best and allow_colon:
            colon = rest.find(":")
            nxt = rest.find("@")
            if colon != -1 and (nxt == -1 or colon < nxt):
                cand = rest[:colon].strip()
                if cand:
                    best = cand

        if not best:
            m = re.match(r"[^\s,;:@]+", rest)
            best = m.group(0) if m else ""

        spans.append((at, at + 1 + len(best)))
        tokens.append(best)
        i = at + 1 + len(best)

    text = line
    for a, b in reversed(spans):
        text = text[:a] + " " + text[b:]
    text = re.sub(r"[:;,\-–—•+]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text, [t for t in tokens if t]


def parse_work_command(lines, roster_names):
    """Phân tích toàn bộ lệnh `việc`."""
    index = build_token_index(
        [nm for grp in roster_names.values() for nm in grp])
    known = build_known_tokens(index)
    known_norm = {normalize_person_key(t) for t in known}

    lines = _expand_one_line([l.strip() for l in lines if l.strip()])

    header = (lines[0] if lines else "").strip()
    header_body = re.sub(r"^(việc|viec)\b", "", header, flags=re.IGNORECASE).strip()
    header_text, header_tokens = split_line(header_body, known, known_norm,
                                            allow_colon=True)

    header_names, errors = [], []
    _seen_h = set()
    for t in header_tokens:
        names, err = resolve_target(t, roster_names, index)
        if err:
            errors.append("Dòng đầu: không hiểu “@%s”.%s" % (t, hint(err, names)))
            continue
        for nm in names:
            k = normalize_person_key(nm) or nm.lower().strip()
            if not k or k in _seen_h:
                continue
            _seen_h.add(k)
            header_names.append(nm)

    job = header_text or None
    assignments = []

    for raw in lines[1:]:
        s = raw.strip()
        if not s or s[0] not in BULLETS:
            continue
        body = s[1:].strip()
        if not body:
            continue

        text, tokens = split_line(body, known, known_norm)

        names = []
        _seen_n = set()
        for t in tokens:
            rn, err = resolve_target(t, roster_names, index)
            if err:
                errors.append("Dòng “%s”: không hiểu “@%s”.%s" % (s, t, hint(err, rn)))
                continue
            for nm in rn:
                k = normalize_person_key(nm) or nm.lower().strip()
                if not k or k in _seen_n:
                    continue
                _seen_n.add(k)
                names.append(nm)

        if not names:
            if tokens:
                continue
            names = list(header_names)
        if not names:
            errors.append("Dòng “%s”: chưa chỉ định ai làm." % s)
            continue

        if job:
            task = ("%s | %s" % (job, text)) if text else job
        else:
            task = text
        if not task:
            errors.append("Dòng “%s”: thiếu nội dung công việc." % s)
            continue

        assignments.append((task, names))

    if not assignments and not errors and job and header_names:
        assignments.append((job, list(header_names)))
        job = None

    if not assignments and not errors:
        errors.append("Chưa có dòng công việc nào (mỗi việc bắt đầu bằng dấu “-”).")

    return {"job": job, "header_names": header_names,
            "assignments": assignments, "errors": errors}


def plan_assignments(parsed):
    """Quyết định bot sẽ LƯU việc như thế nào."""
    assigns = parsed["assignments"]
    job = parsed["job"]

    if job is None and len(assigns) == 1:
        task, names = assigns[0]
        return {"mode": "all", "job": task, "task": task, "names": list(names)}

    card_job = job if job else ""
    pairs = []

    if job and parsed["header_names"]:
        for n in parsed["header_names"]:
            pairs.append((job, n))

    for task_name, names in assigns:
        sub = task_name.split(" | ", 1)[-1]
        for n in names:
            pairs.append((sub, n))
    return {"mode": "multi", "job": card_job, "pairs": pairs}

