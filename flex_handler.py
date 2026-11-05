from datetime import datetime
import pytz
import uuid
import re
import gspread

# Import từ file cấu hình trung tâm
from config import (
    CLIENT, SHEET_NAME, WORKSHEET_TRACKER_NAME, WORKSHEET_ADHOC_TASKS,
    get_spreadsheet, MAIN_CHECKLIST_GROUP_ID, get_liff_url,
    get_cached_values, get_cached_records, set_cached_values,
    run_bg_sheet_task, get_worksheet_cached_obj, values_to_records
)

TRACKER_HEADERS = ['group_id', 'date', 'task_id', 'name', 'time', 'status', 'user_name']
ADHOC_HEADERS = ['group_id', 'date', 'assignee', 'task_id', 'task_name', 'status', 'completed_by', 'completed_at', 'created_at']
GROUP_MEMBERS_HEADERS = ['group_id', 'user_id', 'display_name', 'last_seen']

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


# ============================================================================
# HELPER UI: THANH TIẾN ĐỘ (PROGRESS BAR) & FORMAT HIỂN THỊ
# ============================================================================

def _build_progress_summary_box(completed_count, total_count, label_left="📊 Tiến độ hoàn thành"):
    """Tạo hộp hiển thị thanh tiến độ (Progress Bar) trực quan trên đầu thẻ Flex."""
    pct = round((completed_count / total_count) * 100) if total_count > 0 else 0
    if pct >= 100:
        bar_color = "#16A34A"
        bg_color = "#F0FDF4"
        border_color = "#BBF7D0"
        badge_text = f"🎉 Hoàn tất {completed_count}/{total_count} (100%)"
    elif pct >= 50:
        bar_color = "#0284C7"
        bg_color = "#F8FAFC"
        border_color = "#BAE6FD"
        badge_text = f"⚡ Đã xong {completed_count}/{total_count} ({pct}%)"
    else:
        bar_color = "#EA580C"
        bg_color = "#FFF7ED"
        border_color = "#FED7AA"
        badge_text = f"⏳ Đã xong {completed_count}/{total_count} ({pct}%)"

    bar_width = f"{max(4, min(100, pct))}%" if completed_count > 0 else "2%"

    return {
        "type": "box",
        "layout": "vertical",
        "backgroundColor": bg_color,
        "borderColor": border_color,
        "borderWidth": "1px",
        "cornerRadius": "md",
        "paddingAll": "sm",
        "margin": "xs",
        "contents": [
            {
                "type": "box",
                "layout": "horizontal",
                "alignItems": "center",
                "contents": [
                    {
                        "type": "text",
                        "text": label_left,
                        "size": "xxs",
                        "color": "#475569",
                        "weight": "bold",
                        "flex": 1
                    },
                    {
                        "type": "text",
                        "text": badge_text,
                        "size": "xs",
                        "color": bar_color,
                        "weight": "bold",
                        "align": "end",
                        "flex": 0
                    }
                ]
            },
            {
                "type": "box",
                "layout": "vertical",
                "backgroundColor": "#E2E8F0",
                "height": "7px",
                "cornerRadius": "md",
                "margin": "xs",
                "contents": [
                    {
                        "type": "box",
                        "layout": "vertical",
                        "backgroundColor": bar_color,
                        "height": "7px",
                        "cornerRadius": "md",
                        "width": bar_width,
                        "contents": [{"type": "filler"}]
                    }
                ]
            }
        ]
    }


def _format_completed_badge(user_name_str):
    """Chuẩn hóa chuỗi 'Nguyễn Văn A lúc 09:15' thành '✅ Nguyễn Văn A • 09:15'."""
    if not user_name_str:
        return "✅ Đã hoàn tất"
    s = str(user_name_str).strip()
    if " lúc " in s:
        parts = s.split(" lúc ", 1)
        return f"✅ {parts[0].strip()} • {parts[1].strip()}"
    return f"✅ {s}"


def _short_assignee_display(name, max_len=16):
    """Rút gọn tên hiển thị cho ô Grid 2 cột để không bị tràn chữ."""
    s = str(name or "").strip()
    if not s:
        return "NV"
    if len(s) <= max_len:
        return s
    # Nếu có dạng 'Nguyễn Văn Thảo-156494*' -> giữ 'Thảo-156494'
    if "-" in s:
        left, right = s.split("-", 1)
        words = left.strip().split()
        short_left = words[-1] if words else left.strip()
        cand = f"{short_left}-{right.strip()}"
        if len(cand) <= max_len:
            return cand
    return s[:max_len - 1] + "…"


# ============================================================================
# PHẦN 1: CHECKLIST KHO CA SÁNG / CA CHIỀU / HÌNH ẢNH (CÓ RAM CACHE & CARD UI)
# ============================================================================

def initialize_daily_tasks(group_id, shift_type, force=False):
    """
    Reset và khởi tạo lại danh sách công việc cho ca cụ thể trong RAM Cache & đồng bộ ngầm lên Sheet.
    """
    print(f"Bắt đầu khởi tạo công việc ca {shift_type} cho group {group_id} (force={force})...")
    try:
        all_values = get_cached_values(WORKSHEET_TRACKER_NAME, ttl=60, create_headers=TRACKER_HEADERS)
        headers = list(TRACKER_HEADERS)
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
        
        has_today_tasks = False
        rows_to_keep = []
        
        if all_values:
            headers = all_values[0] if all_values[0] else list(TRACKER_HEADERS)
            for row in all_values[1:]:
                if len(row) < 3:
                    continue
                r_group = str(row[0]).strip()
                r_date = str(row[1]).strip()
                r_task_id = str(row[2]).strip()
                
                if r_date == today_str and r_group == str(group_id).strip() and r_task_id.startswith(shift_type):
                    has_today_tasks = True
                
                if r_date == today_str:
                    if r_group == str(group_id).strip() and r_task_id.startswith(shift_type):
                        continue
                    rows_to_keep.append(list(row))
        
        if has_today_tasks and not force:
            return True
            
        tasks_to_add = []
        for task in TASKS.get(shift_type, []):
            new_row = [str(group_id).strip(), today_str, task['id'], task['name'], task['time'], 'incomplete', '']
            tasks_to_add.append(new_row)

        new_all_values = [list(headers)] + rows_to_keep + tasks_to_add
        set_cached_values(WORKSHEET_TRACKER_NAME, new_all_values)

        def _bg_init_tracker(hdrs, keep_rows, add_rows):
            ws = get_worksheet_cached_obj(WORKSHEET_TRACKER_NAME, create_headers=TRACKER_HEADERS)
            if not ws:
                return
            ws.clear()
            ws.append_row(hdrs)
            combined = keep_rows + add_rows
            if combined:
                ws.append_rows(combined, value_input_option='USER_ENTERED')

        run_bg_sheet_task(_bg_init_tracker, list(headers), rows_to_keep, tasks_to_add, desc=f"init_tasks_{shift_type}")
        return True
    except Exception as e:
        print(f"Lỗi khi khởi tạo công việc: {e}")
        return False


def update_shift_task_status(group_id, task_id, shift_type, target_status, user_name):
    """
    Cập nhật trạng thái công việc Checklist Kho tức thì trong RAM Cache và đẩy ghi Google Sheets chạy ngầm.
    Trả về danh sách records mới nhất trong RAM để vẽ Flex ngay lập tức.
    """
    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
    time_str = datetime.now(tz_vietnam).strftime('%H:%M')
    new_user = f"{user_name} lúc {time_str}" if target_status == 'complete' else ''

    all_values = get_cached_values(WORKSHEET_TRACKER_NAME, ttl=60, create_headers=TRACKER_HEADERS)
    if not all_values:
        all_values = [list(TRACKER_HEADERS)]

    target_gid = str(group_id or '').strip()
    clean_tid = str(task_id or '').strip()

    matched_row_nums = []
    # 1. Tìm dòng khớp group_id, date, task_id
    for idx in range(1, len(all_values)):
        row = all_values[idx]
        if len(row) < 3:
            continue
        r_gid = str(row[0]).strip()
        r_date = str(row[1]).strip()
        r_tid = str(row[2]).strip()
        if r_date == today_str and r_tid == clean_tid and (not target_gid or r_gid == target_gid):
            matched_row_nums.append(idx + 1)  # 1-based sheet row

    # 2. Fallback nếu lệch group_id
    if not matched_row_nums:
        for idx in range(1, len(all_values)):
            row = all_values[idx]
            if len(row) < 3:
                continue
            r_date = str(row[1]).strip()
            r_tid = str(row[2]).strip()
            if r_date == today_str and r_tid == clean_tid:
                matched_row_nums.append(idx + 1)
                target_gid = str(row[0]).strip()

    if matched_row_nums:
        for r_num in matched_row_nums:
            mem_row = all_values[r_num - 1]
            while len(mem_row) < 7:
                mem_row.append('')
            mem_row[5] = target_status
            mem_row[6] = new_user
        set_cached_values(WORKSHEET_TRACKER_NAME, all_values)

        def _bg_batch_update_tracker(r_nums, st, usr):
            ws = get_worksheet_cached_obj(WORKSHEET_TRACKER_NAME, create_headers=TRACKER_HEADERS)
            if not ws:
                return
            batch_updates = [
                {'range': f'F{rn}:G{rn}', 'values': [[st, usr]]}
                for rn in r_nums
            ]
            ws.batch_update(batch_updates, value_input_option='USER_ENTERED')

        run_bg_sheet_task(_bg_batch_update_tracker, list(matched_row_nums), target_status, new_user, desc=f"update_tracker_{clean_tid}")
    else:
        task_name_val = clean_tid
        task_time_val = ''
        for t in TASKS.get(shift_type, []):
            if t['id'] == clean_tid:
                task_name_val = t['name']
                task_time_val = t['time']
                break
        new_row = [target_gid, today_str, clean_tid, task_name_val, task_time_val, target_status, new_user]
        all_values.append(new_row)
        set_cached_values(WORKSHEET_TRACKER_NAME, all_values)

        def _bg_append_tracker(row_data):
            ws = get_worksheet_cached_obj(WORKSHEET_TRACKER_NAME, create_headers=TRACKER_HEADERS)
            if ws:
                ws.append_row(row_data, value_input_option='USER_ENTERED')

        run_bg_sheet_task(_bg_append_tracker, list(new_row), desc=f"append_tracker_{clean_tid}")

    return values_to_records(all_values), target_gid


def get_tasks_status_from_sheet(group_id, shift_type, all_records=None):
    try:
        if all_records is None:
            all_records = get_cached_records(WORKSHEET_TRACKER_NAME, ttl=60, create_headers=TRACKER_HEADERS)
        
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
    """
    Tạo Flex Message cho Checklist Kho Ca Sáng / Ca Chiều / Hình Ảnh (VS).
    Giao diện mới (Nhóm 2 - Mục 6, 7, 8):
      - Thanh tiến độ (Progress Bar) trực quan trên đầu Body
      - Từng công việc đóng khung Card bo góc (#F8FAFC khi chờ, #F0FDF4 khi hoàn tất)
      - Hiển thị rõ tên người hoàn thành & thời gian hoàn thành (✅ Tên NV • HH:MM)
    """
    task_statuses = get_tasks_status_from_sheet(group_id, shift_type, all_records=all_records_prefetched)
    shift_tasks = TASKS.get(shift_type, [])

    if not task_statuses:
        task_statuses = {task['id']: {'status': 'incomplete', 'user_name': ''} for task in shift_tasks}

    if shift_type == 'sang':
        title = "CHECKLIST KHO & VẬN HÀNH SÁNG"
        subtitle = "Ca Sáng • Hoàn thành đúng khung giờ quy định"
        title_icon = "☀️"
        header_bg = "#0284C7"
    elif shift_type == 'chieu':
        title = "CHECKLIST KHO & VẬN HÀNH CHIỀU"
        subtitle = "Ca Chiều • Hoàn thành đúng khung giờ quy định"
        title_icon = "🌙"
        header_bg = "#1E3A8A"
    else:
        title = "CHECKLIST HÌNH ẢNH TRƯỚC 10H"
        subtitle = "Các cụm tạo Album báo cáo đúng hạn"
        title_icon = "📸"
        header_bg = "#0D9488"

    completed_count = 0
    for task in shift_tasks:
        st_info = task_statuses.get(task['id'], {})
        st_val = st_info if isinstance(st_info, str) else st_info.get('status', 'incomplete')
        if st_val == 'complete':
            completed_count += 1

    total_count = len(shift_tasks)
    task_components = []

    # 1. Thanh tiến độ tổng quan (Mục 6)
    if total_count > 0:
        task_components.append(
            _build_progress_summary_box(completed_count, total_count, "📊 Tiến độ công việc ca")
        )

    # 2. Hộp hướng dẫn cho VS
    if shift_type == 'vs':
        desc_box = {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#FFFBEB",
            "borderColor": "#FDE68A",
            "borderWidth": "1px",
            "paddingAll": "sm",
            "margin": "sm",
            "cornerRadius": "md",
            "contents": [
                {
                    "type": "text",
                    "text": "📌 Các cụm tạo Album đúng deadline nhé ACE:",
                    "weight": "bold",
                    "size": "xs",
                    "color": "#B45309",
                    "wrap": True
                },
                {
                    "type": "text",
                    "text": "• Bên ngoài ST (thấy biển, sân xe, toàn cảnh)\n• Bên trong ST (thấy các góc ST, Line)\n• Kho ST (quầy kệ gọn sạch)",
                    "size": "xxs",
                    "color": "#475569",
                    "wrap": True,
                    "margin": "xs"
                }
            ]
        }
        task_components.append(desc_box)

    # 3. Danh sách từng công việc dạng Card bo góc (Mục 7 & Mục 8)
    for task in shift_tasks:
        status_info = task_statuses.get(task['id'], {})
        if isinstance(status_info, str):
            status = status_info
            completed_by = ''
        else:
            status = status_info.get('status', 'incomplete')
            completed_by = status_info.get('user_name', '')

        is_complete = (status == 'complete')

        card_bg = "#F0FDF4" if is_complete else "#F8FAFC"
        card_border = "#BBF7D0" if is_complete else "#E2E8F0"
        text_decoration = "line-through" if is_complete else "none"
        main_text_color = "#64748B" if is_complete else "#0F172A"
        deadline_color = "#94A3B8" if is_complete else "#DC2626"

        button_color = "#94A3B8" if is_complete else "#0284C7"
        button_label = "✓ Xong" if is_complete else "Hoàn tất"
        target_status_param = "incomplete" if is_complete else "complete"

        info_contents = [
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
                "alignItems": "center",
                "margin": "xs",
                "contents": [
                    {
                        "type": "text",
                        "text": f"⏰ Hạn: {task['time']}",
                        "color": deadline_color,
                        "weight": "bold",
                        "size": "xxs",
                        "flex": 0
                    }
                ]
            }
        ]

        # Mục 8: Hiển thị rõ ai đã làm & lúc mấy giờ
        if is_complete:
            info_contents.append({
                "type": "text",
                "text": _format_completed_badge(completed_by),
                "color": "#15803D",
                "weight": "bold",
                "size": "xxs",
                "margin": "xs",
                "wrap": True
            })

        task_card = {
            "type": "box",
            "layout": "horizontal",
            "spacing": "md",
            "paddingAll": "sm",
            "margin": "sm",
            "backgroundColor": card_bg,
            "borderColor": card_border,
            "borderWidth": "1px",
            "cornerRadius": "md",
            "alignItems": "center",
            "contents": [
                {
                    "type": "text",
                    "text": "✅" if is_complete else task.get('icon', '📦'),
                    "size": "xl",
                    "flex": 0
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "contents": info_contents
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
        task_components.append(task_card)

    flex_content = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": header_bg,
            "paddingTop": "14px",
            "paddingBottom": "14px",
            "paddingStart": "16px",
            "paddingEnd": "16px",
            "contents": [
                {
                    "type": "box",
                    "layout": "horizontal",
                    "alignItems": "center",
                    "spacing": "sm",
                    "contents": [
                        {
                            "type": "text",
                            "text": title_icon,
                            "size": "xl",
                            "flex": 0
                        },
                        {
                            "type": "text",
                            "text": title,
                            "weight": "bold",
                            "size": "md",
                            "color": "#FFFFFF",
                            "wrap": True,
                            "flex": 1
                        }
                    ]
                },
                {
                    "type": "text",
                    "text": subtitle,
                    "size": "xxs",
                    "color": "#E0F2FE",
                    "margin": "xs"
                }
            ]
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "xs",
            "paddingAll": "md",
            "backgroundColor": "#FFFFFF",
            "contents": task_components
        }
    }
    return flex_content


# ==========================================
# PHẦN 2: CÔNG VIỆC PHÁT SINH (ADHOC TASKS)
# ==========================================

_last_clean_date = None

def get_or_create_adhoc_worksheet():
    """Lấy worksheet adhoc_tasks có cache đối tượng."""
    return get_worksheet_cached_obj(WORKSHEET_ADHOC_TASKS, create_headers=ADHOC_HEADERS, rows="1000", cols="20")


def _get_adhoc_col_indices(headers):
    norm = [str(h).strip().lower().replace(' ', '_').replace('-', '_') for h in headers]
    
    def find_idx(candidates, default_val):
        for cand in candidates:
            if cand in norm:
                return norm.index(cand)
        return default_val

    return {
        'group_id': find_idx(['group_id', 'groupid', 'gid', 'group', 'nhom', 'mã_nhóm', 'nhóm'], 0),
        'date': find_idx(['date', 'ngay', 'ngày', 'ngay_giao'], 1),
        'assignee': find_idx(['assignee', 'nhan_vien', 'nguoi_nhan', 'ten_nv', 'nhân_viên', 'người_nhận'], 2),
        'task_id': find_idx(['task_id', 'taskid', 'id', 'ma_viec'], 3),
        'task_name': find_idx(['task_name', 'taskname', 'name', 'ten_viec', 'cong_viec', 'tên_việc', 'công_việc'], 4),
        'status': find_idx(['status', 'trang_thai', 'trạng_thái'], 5),
        'completed_by': find_idx(['completed_by', 'nguoi_hoan_thanh', 'xong_boi', 'người_hoàn_thành'], 6),
        'completed_at': find_idx(['completed_at', 'thoi_gian_xong', 'xong_luc', 'thời_gian_xong'], 7),
        'created_at': find_idx(['created_at', 'thoi_gian_giao', 'giao_luc', 'tao_luc', 'thời_gian_giao'], 8)
    }


def _build_adhoc_row(headers, group_id, date_str, assignee, task_id, task_name, status, comp_by, comp_at, created_at):
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


def _ensure_clean_adhoc_ram():
    """Đảm bảo dữ liệu adhoc_tasks trong RAM đã lọc bỏ ngày cũ và trả về all_values hiện tại."""
    global _last_clean_date
    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')

    all_values = get_cached_values(WORKSHEET_ADHOC_TASKS, ttl=60, create_headers=ADHOC_HEADERS)
    if not all_values:
        all_values = [list(ADHOC_HEADERS)]

    if _last_clean_date == today_str or len(all_values) <= 1:
        _last_clean_date = today_str
        return all_values

    headers = all_values[0]
    cols = _get_adhoc_col_indices(headers)
    i_date = cols['date']

    rows_to_keep = []
    has_old_rows = False
    for row in all_values[1:]:
        if len(row) > i_date and str(row[i_date]).strip() == today_str:
            rows_to_keep.append(list(row))
        else:
            has_old_rows = True

    _last_clean_date = today_str
    if has_old_rows:
        cleaned_values = [list(headers)] + rows_to_keep
        set_cached_values(WORKSHEET_ADHOC_TASKS, cleaned_values)
        def _bg_clean_adhoc(hdrs, keep):
            ws = get_or_create_adhoc_worksheet()
            if not ws:
                return
            ws.clear()
            ws.append_row(hdrs)
            if keep:
                ws.append_rows(keep, value_input_option='USER_ENTERED')
        run_bg_sheet_task(_bg_clean_adhoc, list(headers), rows_to_keep, desc="clean_old_adhoc_tasks")
        return cleaned_values

    return all_values


def clean_old_adhoc_tasks(sheet=None):
    _ensure_clean_adhoc_ram()


def add_adhoc_tasks(group_id, assignee, tasks_list):
    """Thêm danh sách công việc phát sinh cho 1 nhân viên vào RAM Cache & ghi Sheet chạy ngầm."""
    if not group_id or not tasks_list:
        return False

    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
    time_now_str = datetime.now(tz_vietnam).strftime('%H:%M')

    try:
        all_values = _ensure_clean_adhoc_ram()
        headers = all_values[0] if all_values else list(ADHOC_HEADERS)

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
            all_values.append(new_row)

        if rows_to_add:
            set_cached_values(WORKSHEET_ADHOC_TASKS, all_values)
            def _bg_add_adhoc(to_add):
                ws = get_or_create_adhoc_worksheet()
                if ws:
                    ws.append_rows(to_add, value_input_option='USER_ENTERED')
            run_bg_sheet_task(_bg_add_adhoc, rows_to_add, desc="add_adhoc_tasks")
            return True
        return False
    except Exception as e:
        print(f"Lỗi khi thêm adhoc tasks: {e}")
        return False


def get_adhoc_tasks_for_group_today(group_id):
    """Lấy toàn bộ công việc phát sinh trong ngày của nhóm từ RAM Cache."""
    if not group_id:
        return []

    try:
        all_values = get_cached_values(WORKSHEET_ADHOC_TASKS, ttl=60, create_headers=ADHOC_HEADERS)
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
    all_group_tasks = get_adhoc_tasks_for_group_today(group_id)
    target_assignee = str(assignee).strip().lower()
    return [t for t in all_group_tasks if str(t.get('assignee', '')).strip().lower() == target_assignee]


def task_group_token(task_id):
    tid = str(task_id)
    parts = tid.split('_')
    if len(parts) >= 3 and parts[0] in ('all', 'multi'):
        return parts[1]
    return tid


def _adhoc_layout(sheet=None):
    all_values = get_cached_values(WORKSHEET_ADHOC_TASKS, ttl=60, create_headers=ADHOC_HEADERS)
    if len(all_values) <= 1:
        return (None, None)
    cols = _get_adhoc_col_indices(all_values[0])
    idx = (cols['group_id'], cols['date'], cols['task_id'], cols['task_name'])
    return (all_values, idx)


def find_latest_task_token(group_id):
    if not group_id:
        return (None, None, 0)
    try:
        all_values, idx = _adhoc_layout()
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
    """Xoá toàn bộ dòng việc thuộc một nhóm trong HÔM NAY (cập nhật RAM tức thì + ghi Sheet ngầm)."""
    if not group_id or not token:
        return (0, [])
    try:
        all_values, idx = _adhoc_layout()
        if not all_values:
            return (0, [])
        i_gid, i_date, i_tid, i_name = idx
        today_str = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).strftime('%Y-%m-%d')
        target_gid = str(group_id).strip()

        keep, names, removed = [], [], 0
        for r in all_values[1:]:
            if len(r) <= max(i_gid, i_date, i_tid):
                keep.append(list(r))
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
                keep.append(list(r))

        if removed == 0:
            return (0, [])

        headers = list(all_values[0])
        set_cached_values(WORKSHEET_ADHOC_TASKS, [headers] + keep)

        def _bg_cancel_adhoc(hdrs, keep_rows):
            ws = get_or_create_adhoc_worksheet()
            if not ws:
                return
            ws.clear()
            ws.append_row(hdrs)
            if keep_rows:
                ws.append_rows(keep_rows, value_input_option='USER_ENTERED')

        run_bg_sheet_task(_bg_cancel_adhoc, headers, keep, desc=f"cancel_task_{token}")
        return (removed, names)
    except Exception as e:
        print(f"Lỗi hủy nhóm việc: {e}")
        return (0, [])


def update_adhoc_task_status(group_id, task_id, target_status, completed_by):
    """
    Cập nhật trạng thái của adhoc task tức thì trong RAM Cache và đẩy ghi Google Sheets chạy ngầm.
    """
    if not task_id:
        return False, None, None
    
    try:
        all_values = get_cached_values(WORKSHEET_ADHOC_TASKS, ttl=60, create_headers=ADHOC_HEADERS)
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

            max_col_needed = max(i_status, i_comp_by, i_comp_at)
            for r_idx in row_indices:
                mem_row = all_values[r_idx - 1]
                while len(mem_row) <= max_col_needed:
                    mem_row.append('')
                mem_row[i_status] = target_status
                mem_row[i_comp_by] = comp_by
                mem_row[i_comp_at] = comp_at

            set_cached_values(WORKSHEET_ADHOC_TASKS, all_values)

            def _bg_update_adhoc(indices, st_col, by_col, at_col, st_val, by_val, at_val):
                ws = get_or_create_adhoc_worksheet()
                if not ws:
                    return
                import gspread.utils
                batch_data = []
                for r_idx in indices:
                    c_st = gspread.utils.rowcol_to_a1(r_idx, st_col + 1)
                    c_by = gspread.utils.rowcol_to_a1(r_idx, by_col + 1)
                    c_at = gspread.utils.rowcol_to_a1(r_idx, at_col + 1)
                    batch_data.extend([
                        {'range': c_st, 'values': [[st_val]]},
                        {'range': c_by, 'values': [[by_val]]},
                        {'range': c_at, 'values': [[at_val]]},
                    ])
                ws.batch_update(batch_data, value_input_option='USER_ENTERED')

            run_bg_sheet_task(
                _bg_update_adhoc, list(row_indices),
                i_status, i_comp_by, i_comp_at,
                target_status, comp_by, comp_at,
                desc=f"update_adhoc_{clean_tid}"
            )
            return True, assignee, task_group_hash

        return False, None, None
    except Exception as e:
        print(f"Lỗi khi cập nhật trạng thái adhoc task: {e}")
        return False, None, None


def generate_adhoc_flex(group_id, assignee, tasks_data=None):
    """
    Tạo Flex Message cho danh sách công việc phát sinh cá nhân (Card UI + Progress Bar + Giờ hoàn thành).
    """
    if tasks_data is None:
        tasks_data = get_adhoc_tasks_today(group_id, assignee)
        
    if not tasks_data:
        return None
        
    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    today_str = datetime.now(tz_vietnam).strftime('%d/%m/%Y')
    
    completed_count = sum(1 for t in tasks_data if t.get('status') == 'complete')
    total_count = len(tasks_data)

    task_components = [
        _build_progress_summary_box(completed_count, total_count, "📊 Tiến độ cá nhân")
    ]
    
    for task in tasks_data:
        task_id = task.get('task_id')
        task_name = task.get('task_name')
        status = task.get('status', 'incomplete')
        completed_by = task.get('completed_by', '')
        completed_at = task.get('completed_at', '')
        created_at = task.get('created_at', '')
        
        is_complete = (status == 'complete')
        card_bg = "#F0FDF4" if is_complete else "#F8FAFC"
        card_border = "#BBF7D0" if is_complete else "#E2E8F0"
        text_decoration = "line-through" if is_complete else "none"
        main_text_color = "#64748B" if is_complete else "#0F172A"
        
        button_color = "#94A3B8" if is_complete else "#00B33C"
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
                "color": "#94A3B8" if is_complete else "#D97706",
                "size": "xxs",
                "weight": "bold",
                "margin": "xs"
            })

        if is_complete:
            by_disp = completed_by or assignee
            time_disp = f" • {completed_at}" if completed_at else ""
            task_info_contents.append({
                "type": "text",
                "text": f"✅ {by_disp}{time_disp}",
                "color": "#15803D",
                "size": "xxs",
                "weight": "bold",
                "margin": "xs",
                "wrap": True
            })

        task_component = {
            "type": "box",
            "layout": "horizontal",
            "spacing": "md",
            "paddingAll": "sm",
            "margin": "sm",
            "backgroundColor": card_bg,
            "borderColor": card_border,
            "borderWidth": "1px",
            "cornerRadius": "md",
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
        
    flex_content = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#5E35B1",
            "paddingTop": "14px",
            "paddingBottom": "14px",
            "paddingStart": "16px",
            "paddingEnd": "16px",
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
                    "text": f"👤 Nhân viên: {assignee}  |  📅 {today_str}",
                    "size": "xs",
                    "color": "#D1C4E9",
                    "margin": "xs",
                    "wrap": True
                }
            ]
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "xs",
            "paddingAll": "md",
            "backgroundColor": "#FFFFFF",
            "contents": task_components
        }
    }
    return flex_content


def add_all_adhoc_tasks(group_id, members, task_name):
    """Giao việc chung @all cho toàn bộ thành viên trong nhóm (cập nhật RAM tức thì + ghi Sheet ngầm)."""
    if not group_id or not members:
        return None

    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
    time_now_str = datetime.now(tz_vietnam).strftime('%H:%M')
    task_group_hash = uuid.uuid4().hex[:6]

    try:
        all_values = _ensure_clean_adhoc_ram()
        headers = all_values[0] if all_values else list(ADHOC_HEADERS)

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
            all_values.append(new_row)

        if rows_to_add:
            set_cached_values(WORKSHEET_ADHOC_TASKS, all_values)
            def _bg_add_all(to_add):
                ws = get_or_create_adhoc_worksheet()
                if ws:
                    ws.append_rows(to_add, value_input_option='USER_ENTERED')
            run_bg_sheet_task(_bg_add_all, rows_to_add, desc=f"add_all_{task_group_hash}")
            return task_group_hash
        return None
    except Exception as e:
        print(f"Lỗi khi thêm adhoc tasks @all: {e}")
        return None


def _render_assignees_ui(assignees_list, indent_px="0px"):
    """
    Render danh sách người nhận việc hiện đại (Mục 8 & Mục 9):
      - Những người CHƯA XONG (pending):
          + Nếu <= 3 người: hiển thị từng dòng ngang rõ ràng kèm nút [Hoàn tất].
          + Nếu >= 4 người (nhóm đông @all, @nv, @pg): tự động chia Grid 2 cột ngang gọn gàng,
            chạm vào ô tên là đánh dấu hoàn tất ngay!
      - Những người ĐÃ XONG (completed):
          + Gom gọn vào một khung xanh lá nhạt bên dưới kèm thời gian hoàn thành (✓ Tên • HH:MM).
          + Vẫn gắn postback hoàn tác (target_status=incomplete) trên từng tên để bấm nhầm có thể hủy.
    """
    pending_list = [a for a in assignees_list if a.get('status') != 'complete']
    completed_list = [a for a in assignees_list if a.get('status') == 'complete']
    ui_boxes = []

    # 1. Hiển thị danh sách người CHƯA XONG
    if pending_list:
        if len(pending_list) <= 3:
            for a in pending_list:
                t_id = a.get('task_id')
                assignee = a.get('assignee', '')
                row_box = {
                    "type": "box",
                    "layout": "horizontal",
                    "spacing": "sm",
                    "alignItems": "center",
                    "backgroundColor": "#FFFFFF",
                    "borderColor": "#E2E8F0",
                    "borderWidth": "1px",
                    "cornerRadius": "sm",
                    "paddingAll": "xs",
                    "paddingStart": "8px",
                    "margin": "xs",
                    "contents": [
                        {
                            "type": "text",
                            "text": f"👤 {assignee}",
                            "color": "#0F172A",
                            "size": "xs",
                            "weight": "bold",
                            "flex": 1,
                            "wrap": True
                        },
                        {
                            "type": "button",
                            "action": {
                                "type": "postback",
                                "label": "Hoàn tất",
                                "data": f"action=complete_adhoc_task&task_id={t_id}&assignee={assignee}&target_status=complete"
                            },
                            "style": "primary",
                            "color": "#00B33C",
                            "height": "sm",
                            "flex": 0
                        }
                    ]
                }
                ui_boxes.append(row_box)
        else:
            # Mục 9: Chia Grid 2 cột cho danh sách đông người (@all, @nv, @pg)
            ui_boxes.append({
                "type": "text",
                "text": f"⏳ Chờ thực hiện ({len(pending_list)} người) — Chạm tên để Hoàn tất:",
                "size": "xxs",
                "color": "#B45309",
                "weight": "bold",
                "margin": "xs"
            })
            for i in range(0, len(pending_list), 2):
                pair = pending_list[i:i + 2]
                row_cells = []
                for a in pair:
                    t_id = a.get('task_id')
                    assignee = a.get('assignee', '')
                    disp_name = _short_assignee_display(assignee, max_len=15)
                    cell = {
                        "type": "box",
                        "layout": "horizontal",
                        "flex": 1,
                        "backgroundColor": "#FFFFFF",
                        "borderColor": "#CBD5E1",
                        "borderWidth": "1px",
                        "cornerRadius": "sm",
                        "paddingAll": "xs",
                        "alignItems": "center",
                        "spacing": "xs",
                        "action": {
                            "type": "postback",
                            "label": "Hoàn tất",
                            "data": f"action=complete_adhoc_task&task_id={t_id}&assignee={assignee}&target_status=complete"
                        },
                        "contents": [
                            {
                                "type": "text",
                                "text": disp_name,
                                "size": "xxs",
                                "weight": "bold",
                                "color": "#0F172A",
                                "flex": 1,
                                "wrap": True
                            },
                            {
                                "type": "box",
                                "layout": "vertical",
                                "backgroundColor": "#00B33C",
                                "cornerRadius": "xs",
                                "paddingAll": "2px",
                                "paddingStart": "4px",
                                "paddingEnd": "4px",
                                "flex": 0,
                                "contents": [
                                    {
                                        "type": "text",
                                        "text": "Xong",
                                        "size": "xxs",
                                        "color": "#FFFFFF",
                                        "weight": "bold"
                                    }
                                ]
                            }
                        ]
                    }
                    row_cells.append(cell)
                if len(row_cells) == 1:
                    row_cells.append({"type": "filler", "flex": 1})
                ui_boxes.append({
                    "type": "box",
                    "layout": "horizontal",
                    "spacing": "xs",
                    "margin": "xs",
                    "contents": row_cells
                })

    # 2. Hiển thị gọn gàng những người ĐÃ HOÀN TẤT kèm giờ hoàn thành (Mục 8 & Mục 9)
    if completed_list:
        comp_rows = [
            {
                "type": "text",
                "text": f"✅ Đã xong ({len(completed_list)}/{len(assignees_list)}) — chạm tên nếu muốn hủy:",
                "size": "xxs",
                "weight": "bold",
                "color": "#166534"
            }
        ]
        for i in range(0, len(completed_list), 2):
            pair = completed_list[i:i + 2]
            row_chips = []
            for a in pair:
                t_id = a.get('task_id')
                assignee = a.get('assignee', '')
                comp_at = a.get('completed_at', '')
                disp_name = _short_assignee_display(assignee, max_len=13)
                time_suffix = f" • {comp_at}" if comp_at else ""
                chip = {
                    "type": "box",
                    "layout": "horizontal",
                    "flex": 1,
                    "backgroundColor": "#FFFFFF",
                    "borderColor": "#86EFAC",
                    "borderWidth": "1px",
                    "cornerRadius": "xs",
                    "paddingAll": "xs",
                    "alignItems": "center",
                    "action": {
                        "type": "postback",
                        "label": "Hủy",
                        "data": f"action=complete_adhoc_task&task_id={t_id}&assignee={assignee}&target_status=incomplete"
                    },
                    "contents": [
                        {
                            "type": "text",
                            "text": f"✓ {disp_name}{time_suffix}",
                            "size": "xxs",
                            "color": "#15803D",
                            "weight": "bold",
                            "wrap": True,
                            "flex": 1
                        }
                    ]
                }
                row_chips.append(chip)
            if len(row_chips) == 1:
                row_chips.append({"type": "filler", "flex": 1})
            comp_rows.append({
                "type": "box",
                "layout": "horizontal",
                "spacing": "xs",
                "margin": "xs",
                "contents": row_chips
            })

        ui_boxes.append({
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#DCFCE7",
            "cornerRadius": "sm",
            "paddingAll": "xs",
            "margin": "xs",
            "contents": comp_rows
        })

    if indent_px and indent_px != "0px":
        return [{
            "type": "box",
            "layout": "vertical",
            "paddingStart": indent_px,
            "contents": ui_boxes
        }]
    return ui_boxes


def generate_today_adhoc_flex(group_id):
    """
    Tạo Flex Bubble hiển thị TỔNG HỢP toàn bộ công việc giao thêm trong ngày hôm nay của nhóm.
    Thiết kế mới (Nhóm 2 - Mục 6, 7, 8, 9 & Nhóm 4 - Mục 15):
      - Thanh tiến độ (Progress Bar) tổng hợp toàn bộ đầu việc trong ngày
      - Đóng khung từng đầu việc thành Task Card bo góc nổi khối (#F8FAFC / #F0FDF4)
      - Chia Grid 2 cột cho danh sách người nhận việc đông & gom gọn người đã hoàn thành kèm giờ xong
      - Nút mở nhanh Web App (LINE LIFF) ở phần Footer nếu có cấu hình
    """
    tasks_data = get_adhoc_tasks_for_group_today(group_id)
    if not tasks_data:
        return None

    try:
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        today_display_str = datetime.now(tz_vietnam).strftime('%d/%m/%Y')

        ordered_tasks = []
        seen_groups = {}

        for record in tasks_data:
            task_id = str(record.get('task_id', ''))
            task_name_val = str(record.get('task_name', 'Công việc phát sinh')).strip()
            token = task_group_token(task_id)
            created_at = str(record.get('created_at', '')).strip()

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
                group_key = (token, parent_job)
                if group_key not in seen_groups:
                    seen_groups[group_key] = len(ordered_tasks)
                    ordered_tasks.append({
                        'is_branched': True,
                        'title': parent_job,
                        'created_at': created_at,
                        'token': token,
                        'branches': {}
                    })
                idx = seen_groups[group_key]
                if not ordered_tasks[idx]['created_at'] and created_at:
                    ordered_tasks[idx]['created_at'] = created_at

                if sub_task not in ordered_tasks[idx]['branches']:
                    ordered_tasks[idx]['branches'][sub_task] = []
                ordered_tasks[idx]['branches'][sub_task].append(assignee_data)
            else:
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

        # Tính tổng tiến độ toàn bộ lượt giao việc trong ngày (Mục 6)
        total_assign_rows = len(tasks_data)
        done_assign_rows = sum(1 for r in tasks_data if r.get('status') == 'complete')

        task_components = [
            _build_progress_summary_box(
                done_assign_rows,
                total_assign_rows,
                f"📊 Tiến độ ({len(ordered_tasks)} đầu việc)"
            )
        ]

        for stt, item in enumerate(ordered_tasks, start=1):
            created_at_str = item.get('created_at')

            if item.get('is_branched'):
                all_assignees = [a for b_list in item['branches'].values() for a in b_list]
                done_cnt = sum(1 for a in all_assignees if a.get('status') == 'complete')
                tot_cnt = len(all_assignees)
                all_complete = (tot_cnt > 0 and done_cnt == tot_cnt)

                card_bg = "#F0FDF4" if all_complete else "#F8FAFC"
                card_border = "#86EFAC" if all_complete else "#CBD5E1"
                main_text_color = "#64748B" if all_complete else "#0F172A"
                main_decoration = "line-through" if all_complete else "none"
                badge_bg = "#16A34A" if all_complete else "#0284C7"

                main_header_box = {
                    "type": "box",
                    "layout": "horizontal",
                    "spacing": "sm",
                    "alignItems": "center",
                    "contents": [
                        {
                            "type": "text",
                            "text": "✅" if all_complete else "📌",
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
                        },
                        {
                            "type": "box",
                            "layout": "vertical",
                            "backgroundColor": badge_bg,
                            "cornerRadius": "xs",
                            "paddingAll": "2px",
                            "paddingStart": "6px",
                            "paddingEnd": "6px",
                            "flex": 0,
                            "contents": [
                                {
                                    "type": "text",
                                    "text": f"{done_cnt}/{tot_cnt}",
                                    "size": "xxs",
                                    "color": "#FFFFFF",
                                    "weight": "bold"
                                }
                            ]
                        }
                    ]
                }

                card_contents = [main_header_box]
                if created_at_str:
                    card_contents.append({
                        "type": "text",
                        "text": f"🕒 Giao lúc: {created_at_str}  •  Mã: {item.get('token', '')}",
                        "size": "xxs",
                        "color": "#64748B",
                        "margin": "xs"
                    })

                card_contents.append({"type": "separator", "color": "#E2E8F0", "margin": "xs"})

                for sub_name, b_assignees in item['branches'].items():
                    sub_complete = all(a.get('status') == 'complete' for a in b_assignees)
                    sub_text_col = "#64748B" if sub_complete else "#1E293B"
                    sub_text_dec = "line-through" if sub_complete else "none"

                    card_contents.append({
                        "type": "box",
                        "layout": "horizontal",
                        "spacing": "xs",
                        "alignItems": "center",
                        "margin": "sm",
                        "contents": [
                            {"type": "text", "text": "🔹", "size": "xs", "flex": 0},
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
                    })
                    card_contents.extend(_render_assignees_ui(b_assignees))

            else:
                all_assignees = item['assignees']
                done_cnt = sum(1 for a in all_assignees if a.get('status') == 'complete')
                tot_cnt = len(all_assignees)
                all_complete = (tot_cnt > 0 and done_cnt == tot_cnt)

                card_bg = "#F0FDF4" if all_complete else "#F8FAFC"
                card_border = "#86EFAC" if all_complete else "#CBD5E1"
                main_text_color = "#64748B" if all_complete else "#0F172A"
                main_decoration = "line-through" if all_complete else "none"
                badge_bg = "#16A34A" if all_complete else "#0284C7"

                subtask_header_box = {
                    "type": "box",
                    "layout": "horizontal",
                    "spacing": "sm",
                    "alignItems": "center",
                    "contents": [
                        {
                            "type": "text",
                            "text": "✅" if all_complete else "📌",
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
                        },
                        {
                            "type": "box",
                            "layout": "vertical",
                            "backgroundColor": badge_bg,
                            "cornerRadius": "xs",
                            "paddingAll": "2px",
                            "paddingStart": "6px",
                            "paddingEnd": "6px",
                            "flex": 0,
                            "contents": [
                                {
                                    "type": "text",
                                    "text": f"{done_cnt}/{tot_cnt}",
                                    "size": "xxs",
                                    "color": "#FFFFFF",
                                    "weight": "bold"
                                }
                            ]
                        }
                    ]
                }

                card_contents = [subtask_header_box]
                if created_at_str:
                    card_contents.append({
                        "type": "text",
                        "text": f"🕒 Giao lúc: {created_at_str}  •  Mã: {item.get('token', '')}",
                        "size": "xxs",
                        "color": "#64748B",
                        "margin": "xs"
                    })

                card_contents.append({"type": "separator", "color": "#E2E8F0", "margin": "xs"})
                card_contents.extend(_render_assignees_ui(all_assignees))

            # Mục 7: Đóng khung mỗi đầu việc thành Card bo góc hiện đại
            subtask_card = {
                "type": "box",
                "layout": "vertical",
                "backgroundColor": card_bg,
                "borderColor": card_border,
                "borderWidth": "1px",
                "cornerRadius": "md",
                "paddingAll": "sm",
                "margin": "md",
                "spacing": "xs",
                "contents": card_contents
            }
            task_components.append(subtask_card)

        header_contents = [
            {
                "type": "text",
                "text": "📋 BẢNG CÔNG VIỆC GIAO THÊM",
                "weight": "bold",
                "size": "md",
                "color": "#FFFFFF"
            },
            {
                "type": "text",
                "text": f"📅 Ngày: {today_display_str}  •  Tổng: {len(ordered_tasks)} đầu việc",
                "size": "xxs",
                "color": "#E0F2FE",
                "margin": "xs"
            }
        ]

        flex_content = {
            "type": "bubble",
            "size": "mega",
            "header": {
                "type": "box",
                "layout": "vertical",
                "backgroundColor": "#0284C7",
                "paddingTop": "14px",
                "paddingBottom": "14px",
                "paddingStart": "16px",
                "paddingEnd": "16px",
                "contents": header_contents
            },
            "body": {
                "type": "box",
                "layout": "vertical",
                "spacing": "xs",
                "paddingAll": "md",
                "backgroundColor": "#FFFFFF",
                "contents": task_components
            }
        }

        return flex_content
    except Exception as e:
        print(f"Lỗi khi tạo flex tổng hợp công việc trong ngày: {e}")
        return None


def generate_all_adhoc_flex(group_id, task_group_hash=None):
    return generate_today_adhoc_flex(group_id)


_group_members_ram_seen = set()  # {(group_id, user_id, display_name, date_str)}

def register_group_member(group_id, user_id, display_name):
    """
    Lưu thành viên của nhóm vào sheet group_members chạy ngầm (chỉ đồng bộ 1 lần/ngày/user để không làm chậm chat).
    """
    if not group_id or not user_id or not display_name:
        return
    if str(group_id) == str(user_id):
        return

    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
    cache_key = (str(group_id), str(user_id), str(display_name), today_str)
    if cache_key in _group_members_ram_seen:
        return
    _group_members_ram_seen.add(cache_key)

    def _bg_reg_member(gid, uid, dname):
        ws = get_worksheet_cached_obj('group_members', create_headers=GROUP_MEMBERS_HEADERS, rows="1000", cols="10")
        if not ws:
            return
        all_vals = get_cached_values('group_members', ttl=300, create_headers=GROUP_MEMBERS_HEADERS)
        if not all_vals:
            all_vals = [list(GROUP_MEMBERS_HEADERS)]
        now_str = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).strftime('%Y-%m-%d %H:%M:%S')
        row_idx = -1
        for i, r in enumerate(all_vals[1:], start=2):
            if len(r) >= 2 and str(r[0]) == str(gid) and str(r[1]) == str(uid):
                row_idx = i
                break
        if row_idx != -1:
            while len(all_vals[row_idx - 1]) < 4:
                all_vals[row_idx - 1].append('')
            all_vals[row_idx - 1][2] = dname
            all_vals[row_idx - 1][3] = now_str
            set_cached_values('group_members', all_vals)
            ws.update(range_name=f'C{row_idx}:D{row_idx}', values=[[dname, now_str]])
        else:
            new_row = [str(gid), str(uid), dname, now_str]
            all_vals.append(new_row)
            set_cached_values('group_members', all_vals)
            ws.append_row(new_row)

    run_bg_sheet_task(_bg_reg_member, str(group_id), str(user_id), str(display_name), desc="register_group_member")


def add_multi_adhoc_tasks(group_id, job_name, task_assignments):
    """Thêm danh sách công việc nhiều nhánh vào RAM Cache & ghi Sheet chạy ngầm."""
    if not group_id or not task_assignments:
        return None
    
    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
    time_now_str = datetime.now(tz_vietnam).strftime('%H:%M')
    task_group_hash = uuid.uuid4().hex[:6]
    
    try:
        all_values = _ensure_clean_adhoc_ram()
        headers = all_values[0] if all_values else list(ADHOC_HEADERS)
        
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
            all_values.append(new_row)
            
        if rows_to_add:
            set_cached_values(WORKSHEET_ADHOC_TASKS, all_values)
            def _bg_add_multi(to_add):
                ws = get_or_create_adhoc_worksheet()
                if ws:
                    ws.append_rows(to_add, value_input_option='USER_ENTERED')
            run_bg_sheet_task(_bg_add_multi, rows_to_add, desc=f"add_multi_{task_group_hash}")
            return task_group_hash
        return None
    except Exception as e:
        print(f"Lỗi khi thêm multi adhoc tasks: {e}")
        return None


def generate_multi_adhoc_flex(group_id, task_group_hash=None):
    return generate_today_adhoc_flex(group_id)


def generate_latest_adhoc_flex(group_id):
    return generate_today_adhoc_flex(group_id)


def build_work_carousel(checklist_bubble, adhoc_bubble):
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
    adhoc_bubble = generate_today_adhoc_flex(group_id)

    if str(group_id).strip() == str(MAIN_CHECKLIST_GROUP_ID).strip():
        if not shift_type:
            tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
            current_hour = datetime.now(tz_vietnam).hour
            shift_type = 'sang' if current_hour < 15 else 'chieu'

        checklist_bubble = generate_checklist_flex(group_id, shift_type)
        return build_work_carousel(checklist_bubble, adhoc_bubble)

    if adhoc_bubble:
        return adhoc_bubble
    if shift_type:
        return generate_checklist_flex(group_id, shift_type)
    return None


# ============================================================================
# HÀM TIỆN ÍCH GỬI CHECKLIST ĐỊNH KỲ
# ============================================================================
from linebot.models import FlexSendMessage as _FlexSendMessage

def get_checklist_message(shift_type, group_id):
    """Khởi tạo công việc và trả về đối tượng FlexSendMessage cho ca làm việc."""
    try:
        if not group_id:
            print("Lỗi: Không có Group ID để tạo checklist.")
            return None
        initialize_daily_tasks(group_id, shift_type)
        if str(group_id).strip() == str(MAIN_CHECKLIST_GROUP_ID).strip():
            flex_content = generate_combined_work_flex(group_id, shift_type)
        else:
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
# BỘ PHÂN TÍCH CÚ PHÁP GIAO VIỆC (GIỮ NGUYÊN 100% LOGIC GỐC)
# ============================================================================
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
    if len(lines) != 1:
        return lines
    s = lines[0]
    i = s.find(":")
    if i == -1 or "@" not in s[:i]:
        return lines
    return [s[:i].strip() + ":", "- " + s[i + 1:].strip()]


def is_placeholder(name):
    return normalize_person_key(name) in PLACEHOLDER


def split_names(cell):
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
    m = re.search(r"[-–]\s*(\d{2,8})", str(name))
    return m.group(1) if m else None


def build_token_index(names):
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
    idx = build_token_index([nm for grp in roster_names.values() for nm in grp])
    return {tok for tok, _ in idx if tok.isdigit()}


def _mention_code(display, code_set):
    for m in re.finditer(r"\d{2,8}", str(display)):
        if m.group(0) in code_set:
            return m.group(0)
    return None


def merge_broken_task_lines(raw_lines):
    """
    Gộp các dòng người nhận/thông tin bổ trợ bị xuống dòng
    vào dòng gạch đầu dòng (-) trước đó.
    """
    merged = []
    for l in raw_lines:
        line_clean = l.strip()
        if not line_clean:
            continue
        if not merged:
            merged.append(line_clean)
        elif line_clean[0] in BULLETS or line_clean.lower().startswith("việc") or line_clean.lower().startswith("viec"):
            merged.append(line_clean)
        else:
            merged[-1] += " " + line_clean
    return merged


def resolve_mentions(raw_text, mentionees, roster_names=None):
    if not mentionees or not raw_text:
        return raw_text

    all_roster = []
    if roster_names and isinstance(roster_names, dict):
        all_roster = [nm for grp in roster_names.values() for nm in grp]

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
            # Thông minh: nếu nick LINE chứa mã NV thì thay bằng mã NV
            matched_code = None
            clean_disp = display.replace("\n", " ").strip()
            for code_match in re.finditer(r"(\d{4,8})", clean_disp):
                c = code_match.group(1)
                for r in all_roster:
                    if c in r:
                        matched_code = c
                        break
                if matched_code:
                    break

            if matched_code:
                token = matched_code
            else:
                token = display.replace(" ", MENTION_SPACE).strip() or "?"

        out = out[:char_start] + "@" + token + overflow + out[char_end:]

    return out


def build_roster_indexes(roster_dict):
    """
    Tạo các bảng tra cứu từ roster:
    - code_map: ánh xạ mã nhân viên (4-8 số) -> tên đầy đủ trong danh sách.
    - alias_map: ánh xạ tên gọi (viết thường, không dấu) -> danh sách người có tên đó.
    """
    all_names = [nm for grp in roster_dict.values() for nm in grp]
    code_map = {}
    alias_map = {}
    for nm in all_names:
        for c in re.findall(r"(\d{4,8})", nm):
            code_map[c] = nm
        first_alias = re.split(r"[-–_\s]", nm)[0].strip()
        a_norm = normalize_person_key(first_alias)
        if len(a_norm) >= 2:
            alias_map.setdefault(a_norm, []).append(nm)
    return all_names, code_map, alias_map


def extract_assignees_smart(line_str, roster_dict, code_map, alias_map):
    """
    Nhận diện người nhận việc thông minh và an toàn:
    1. Nhóm từ khóa: @all, @nv, @pg, @st, @ql...
    2. Mã nhân viên thực tế: dãy 4-8 số có trong code_map.
    3. Tên nhân viên: nếu duy nhất 1 người trùng tên trong ca thì gán.
    Tuyệt đối không lấy token rác (SVC, AIO, v.v.) làm người nhận.
    """
    assigned = []
    _seen = set()

    def add_nm(nm):
        if nm and nm not in _seen:
            _seen.add(nm)
            assigned.append(nm)

    low = line_str.lower()
    for kw, grps in GROUP_KEYWORDS.items():
        if re.search(r"@" + re.escape(kw) + r"\b", low):
            for g in grps:
                for nm in roster_dict.get(g, []):
                    add_nm(nm)

    for c in re.findall(r"(\d{4,8})", line_str):
        if c in code_map:
            add_nm(code_map[c])

    norm_text = normalize_person_key(line_str)
    tokens = norm_text.split()
    for tok in tokens:
        if tok in alias_map:
            cands = alias_map[tok]
            if len(cands) == 1:
                add_nm(cands[0])

    return assigned


def clean_task_desc(line_str, assigned_names):
    """
    Dọn dẹp chuỗi mô tả công việc, loại bỏ token tag, mã nhân viên và tên người nhận đã gán.
    """
    s = re.sub(r"^[-*•+–—]\s*", "", line_str).strip()
    s = re.sub(r"@[^\s@]+", " ", s)
    s = re.sub(r"\b\d{4,8}\b", " ", s)
    s = re.sub(r"\b(SVC|AIO|AIOvn|vn)\b", " ", s, flags=re.IGNORECASE)
    for nm in assigned_names:
        first = re.split(r"[-–_\s]", nm)[0].strip()
        s = re.sub(r"\b" + re.escape(first) + r"\b", " ", s, flags=re.IGNORECASE)
        s = re.sub(r"\b" + re.escape(nm) + r"\b", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"[\s,;:\-_–—]+$", "", s)
    s = re.sub(r"\s+", " ", s).strip(" ,;:-_–—")
    return s


def parse_work_command(lines, roster_names):
    all_names, code_map, alias_map = build_roster_indexes(roster_names)

    lines = merge_broken_task_lines([l.strip() for l in lines if l.strip()])
    lines = _expand_one_line(lines)
    if not lines:
        return {"job": None, "header_names": [], "assignments": [], "errors": ["Không có nội dung tin nhắn."]}

    header = (lines[0] if lines else "").strip()
    header_body = re.sub(r"^(việc|viec)\b", "", header, flags=re.IGNORECASE).strip(" :;-")
    header_names = extract_assignees_smart(header_body, roster_names, code_map, alias_map)
    header_task = clean_task_desc(header_body, header_names)
    job = header_task or (header_body if not header_names else None)

    assignments = []
    errors = []

    for raw in lines[1:]:
        s = raw.strip()
        if not s or s[0] not in BULLETS:
            continue
        body = s[1:].strip()
        if not body:
            continue

        names = extract_assignees_smart(s, roster_names, code_map, alias_map)
        if not names:
            names = list(header_names)
        if not names:
            errors.append("Dòng “%s”: chưa chỉ định ai làm." % s)
            continue

        task_text = clean_task_desc(s, names)
        if not task_text:
            task_text = job or "Công việc giao thêm"

        full_task = ("%s | %s" % (job, task_text)) if job else task_text
        assignments.append((full_task, names))

    if not assignments and not errors and job and header_names:
        assignments.append((job, list(header_names)))
        job = None

    if not assignments and not errors:
        errors.append("Chưa có dòng công việc nào (mỗi việc bắt đầu bằng dấu “-”).")

    return {
        "job": job,
        "header_names": header_names,
        "assignments": assignments,
        "errors": errors
    }


def plan_assignments(parsed):
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