import re
import math
import urllib.parse
from datetime import datetime
import pytz
import unicodedata
import gspread
from linebot.models import FlexSendMessage

# Import từ file cấu hình trung tâm
from config import (
    CLIENT, SHEET_NAME, WORKSHEET_SCHEDULES_NAME, WORKSHEET_MEAL_TRACKER_NAME,
    get_spreadsheet, get_cached_values, get_cached_records,
    set_cached_values, run_bg_sheet_task, get_worksheet_cached_obj, values_to_records
)

# Định nghĩa Header chuẩn (8 cột)
MEAL_HEADERS = ['group_id', 'date', 'session', 'type', 'name', 'status', 'time_clicked', 'clicked_by']

def normalize_text(text):
    """Chuẩn hóa chuỗi để so sánh chính xác."""
    if not text: return ""
    text = urllib.parse.unquote(str(text))
    text = text.strip().lower()
    return unicodedata.normalize('NFC', text)

def get_vietnamese_day_of_week():
    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    weekday = datetime.now(tz_vietnam).weekday()
    days = ["Thứ Hai", "Thứ Ba", "Thứ Tư", "Thứ Năm", "Thứ Sáu", "Thứ Bảy", "Chủ Nhật"]
    return days[weekday]

def clean_staff_name(name):
    name = re.sub(r'^\(\d+.*?\):?\s*', '', name)
    name = re.sub(r'^[•\-\+:\.]\s*', '', name)
    return name.strip()


SCHEDULE_KEYWORDS = ["Ca Sáng", "Ca Chiều", "Nghỉ", "Vệ Sinh Kho", "Vệ Sinh"]
SHIFT_BLOCKS = ("Ca Sáng", "Ca Chiều")

_EMOJI_RE = re.compile(
    r"[\U0001F000-\U0001FAFF\u2600-\u27BF\u2B00-\u2BFF\uFE0F\u200D]"
)


def split_schedule_blocks(raw_text):
    """Tách một ô lịch thành {tên ca: nội dung}."""
    if not raw_text:
        return {}
    text = str(raw_text).replace("<br>", "\n")
    pattern = "|".join(re.escape(k) for k in SCHEDULE_KEYWORDS)
    parts = re.split("(%s)" % pattern, text)
    blocks = {}
    i = 1 if parts and not parts[0].strip() else 0
    while i < len(parts):
        shift = parts[i].strip()
        content = ""
        if i + 1 < len(parts):
            content = parts[i + 1].strip().lstrip(":").lstrip(";").strip()
        blocks.setdefault(shift, content)
        i += 2
    return blocks


def clean_person_name(name):
    """Bỏ tiền tố '(8 NV): ', số thứ tự, ký hiệu đầu dòng và emoji."""
    if not name:
        return ""
    s = _EMOJI_RE.sub(" ", str(name))
    s = re.sub(r"^\(\s*\d+\s*(?:NV|PG)?\s*\)\s*:?\s*", "", s)
    s = re.sub(r"^[-+*.\d)]+\s*", "", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip(" .,;:-*")


def normalize_person_key(name):
    """Khoá so trùng: bỏ dấu, bỏ ký hiệu vai trò, gộp khoảng trắng, viết thường."""
    s = clean_person_name(name)
    s = re.sub(r"\((?:ERP|GH1|GH2|PG|NV)\)", " ", s, flags=re.IGNORECASE)
    s = s.replace("*", " ")
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = s.replace("đ", "d").replace("Đ", "d")
    s = re.sub(r"[^a-z0-9\s]", " ", s.lower())
    return re.sub(r"\s+", " ", s).strip()


def match_staff_name(n1, n2):
    """
    So khớp thông minh giữa tên nhân sự trên hệ thống và tên nhận từ nút bấm / tin nhắn.
    Hỗ trợ chuẩn hóa dấu tiếng Việt NFC/NFD, loại bỏ ký tự đặc biệt (*, ERP, GH1, GH2, emoji),
    so khớp theo mã số nhân viên (4-8 chữ số), và so khớp bao hàm tên an toàn.
    """
    if not n1 or not n2:
        return False
    s1, s2 = str(n1).strip(), str(n2).strip()
    if s1.lower() == s2.lower():
        return True
    if normalize_text(s1) == normalize_text(s2):
        return True

    k1 = normalize_person_key(s1)
    k2 = normalize_person_key(s2)
    if k1 and k2 and k1 == k2:
        return True

    # So khớp theo mã số nhân viên (4 đến 8 chữ số liên tiếp)
    codes1 = re.findall(r"\b\d{4,8}\b", s1) or re.findall(r"\d{4,8}", s1)
    codes2 = re.findall(r"\b\d{4,8}\b", s2) or re.findall(r"\d{4,8}", s2)
    if codes1 and codes2:
        if set(codes1) & set(codes2):
            return True
        # Nếu cả hai bên đều có mã NV nhưng mã lại khác nhau thì chắc chắn không phải cùng người
        return False

    if k1 and k2:
        alpha1 = re.sub(r"\d+", "", k1).strip()
        alpha2 = re.sub(r"\d+", "", k2).strip()
        if alpha1 and alpha2 and alpha1 == alpha2:
            return True
        if len(k1) >= 4 and k1 in k2:
            return True
        if len(k2) >= 4 and k2 in k1:
            return True

    return False


def get_daily_roster(day_str=None):
    """
    Danh sách người làm hôm nay lấy từ sheet `schedules` (có RAM Cache), đã khử trùng.
    """
    day = day_str or get_vietnamese_day_of_week()
    try:
        records = get_cached_records(WORKSHEET_SCHEDULES_NAME, ttl=300)
    except Exception as e:
        print("Lỗi đọc sheet schedules: %s" % e)
        return []

    row = next((r for r in records
                if str(r.get("day_of_week", "")).strip() == day), None)
    if not row:
        print("Sheet schedules không có dòng cho %s." % day)
        return []

    roster = []
    seen = set()
    for role, col_name in (("NV", "employee_schedule"), ("PG", "pg_schedule")):
        blocks = split_schedule_blocks(row.get(col_name, ""))
        for shift in SHIFT_BLOCKS:
            content = blocks.get(shift, "")
            if not content:
                continue
            for raw in re.split(r"[,\n;•+]", content):
                name = clean_person_name(raw)
                if not name or name.isdigit() or len(name) < 2:
                    continue
                if is_placeholder_name(name):
                    continue
                key = normalize_person_key(name)
                if not key or key in seen:
                    continue
                seen.add(key)
                roster.append({"name": name, "role": role, "shift": shift})

    return roster


def get_daily_roster_names(day_str=None):
    """Tiện dụng: chỉ lấy danh sách tên."""
    return [r["name"] for r in get_daily_roster(day_str)]


ROSTER_GROUPS = ("PG", "NV", "QL+TC")
ROSTER_COL_FALLBACK = {"PG": 4, "NV": 5, "QL+TC": 6}
_PLACEHOLDER_KEYS = ("", "khong co", "khong", "trong", "none", "null", "n a", "na")


def is_placeholder_name(name):
    """True nếu chuỗi không phải tên người (ví dụ 'Không có')."""
    return normalize_person_key(name) in _PLACEHOLDER_KEYS


def _header_and_rows(values):
    """Tách dòng tiêu đề và các dòng dữ liệu của một bảng giá trị."""
    if not values:
        return [], []
    header = [str(h).strip() for h in values[0]]
    return header, values[1:]


def _find_day_row(values, day):
    """Trả về dict {tên_cột: giá_trị} của dòng có day_of_week == day."""
    header, rows = _header_and_rows(values)
    if "day_of_week" not in header:
        return None
    i_day = header.index("day_of_week")
    for row in rows:
        if i_day < len(row) and str(row[i_day]).strip() == day:
            return {header[i]: (row[i] if i < len(row) else "")
                    for i in range(len(header))}
    return None


def _read_roster_columns(values):
    """Đọc 3 cột E/F/G thành {'PG': [...], 'NV': [...], 'QL+TC': [...]}."""
    from flex_handler import split_names

    header, rows = _header_and_rows(values)
    out = {}
    for group in ROSTER_GROUPS:
        if group in header:
            ci = header.index(group)
        else:
            ci = ROSTER_COL_FALLBACK[group]
        names, seen = [], set()
        for row in rows:
            if ci >= len(row):
                continue
            for nm in split_names(row[ci]):
                key = normalize_person_key(nm)
                if not key or key in seen:
                    continue
                seen.add(key)
                names.append(nm)
        out[group] = names
    return out


def _names_from_detail(row, col_name):
    out, seen = [], set()
    blocks = split_schedule_blocks(row.get(col_name, ""))
    for shift in SHIFT_BLOCKS:
        content = blocks.get(shift, "")
        if not content:
            continue
        for raw in re.split(r"[,\n;•+]", content):
            name = clean_person_name(raw)
            if not name or name.isdigit() or len(name) < 2:
                continue
            if is_placeholder_name(name):
                continue
            key = normalize_person_key(name)
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(name)
    return out


def get_task_roster(day_str=None):
    """
    Danh sách người nhận việc theo 3 nhóm, đọc 3 cột E/F/G của sheet `schedules` (từ RAM Cache 5 phút).
    """
    empty = {g: [] for g in ROSTER_GROUPS}
    day = day_str or get_vietnamese_day_of_week()
    try:
        values = get_cached_values(WORKSHEET_SCHEDULES_NAME, ttl=300)
    except Exception as e:
        print("Lỗi đọc sheet schedules: %s" % e)
        return empty

    try:
        roster = _read_roster_columns(values)
    except Exception as e:
        print("Lỗi đọc 3 cột PG/NV/QL+TC: %s" % e)
        return empty

    if not all(roster[g] for g in ROSTER_GROUPS):
        row = _find_day_row(values, day)
        if row:
            for group, detail_col in (("PG", "pg_schedule"),
                                      ("NV", "employee_schedule")):
                if not roster[group]:
                    roster[group] = _names_from_detail(row, detail_col)
                    if roster[group]:
                        print("[ROSTER] cột %s trống -> tạm bóc từ %s (%d người)."
                              % (group, detail_col, len(roster[group])))
        else:
            print("[ROSTER] không thấy dòng cho %s để dự phòng." % day)

    return roster


def get_working_staff(session_type):
    day_str = get_vietnamese_day_of_week()
    target_shift_name = "Ca Sáng" if session_type == 'ansang' else "Ca Chiều"
    exclude_pattern = r'off\s*ca\s*3' if session_type == 'ansang' else r'off\s*ca\s*4'
    
    try:
        records = get_cached_records(WORKSHEET_SCHEDULES_NAME, ttl=300)
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
                    if not clean_name or clean_name.isdigit(): continue
                    if re.search(exclude_pattern, clean_name, re.IGNORECASE): continue
                    results[staff_type].append(clean_name)
        return results
    except Exception as e:
        print(f"Lỗi lấy lịch: {e}")
        return {}


def sync_meal_sheet(group_id, session_type):
    """Đồng bộ danh sách ăn trưa/tối sử dụng RAM Cache + Ghi Google Sheets chạy ngầm."""
    try:
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
        
        all_values = get_cached_values(WORKSHEET_MEAL_TRACKER_NAME, ttl=60, create_headers=MEAL_HEADERS)
        if not all_values:
            all_values = [list(MEAL_HEADERS)]

        # Dọn dẹp dữ liệu ngày cũ, chỉ giữ lại header và các dòng của ngày hôm nay
        today_rows = [r for r in all_values[1:] if len(r) > 1 and str(r[1]).strip() == today_str]
        need_clear_sheet = False
        if len(all_values) > 1 and len(today_rows) < (len(all_values) - 1):
            print(f"Ngày mới! Dọn dẹp dữ liệu meal ngày cũ...")
            all_values = [list(MEAL_HEADERS)] + today_rows
            set_cached_values(WORKSHEET_MEAL_TRACKER_NAME, all_values)
            need_clear_sheet = True

        all_records = values_to_records(all_values)

        existing_entries = {}
        for row in all_records:
            r_date = str(row.get('date')).strip()
            r_session = str(row.get('session')).strip()
            r_name = str(row.get('name', '')).strip()
            if r_date == today_str and r_session == session_type:
                k_norm = normalize_text(r_name)
                k_key = normalize_person_key(r_name)
                if k_norm and (k_norm not in existing_entries or row.get('status') == 'done'):
                    existing_entries[k_norm] = row
                if k_key and (k_key not in existing_entries or row.get('status') == 'done'):
                    existing_entries[k_key] = row

        staff_lists = get_working_staff(session_type)
        final_data = [] 
        new_rows = []

        for s_type in ['NV', 'PG']:
            for name in staff_lists.get(s_type, []):
                norm_name = normalize_text(name)
                person_key = normalize_person_key(name)
                existing = existing_entries.get(norm_name) or existing_entries.get(person_key)
                if existing:
                    final_data.append(existing)
                else:
                    entry = {
                        'group_id': group_id, 'date': today_str, 'session': session_type,
                        'type': s_type, 'name': name, 'status': 'waiting', 'time_clicked': '', 'clicked_by': ''
                    }
                    new_row = [str(group_id), today_str, session_type, s_type, name, 'waiting', '', '']
                    new_rows.append(new_row)
                    all_values.append(new_row)
                    final_data.append(entry)
                    if norm_name: existing_entries[norm_name] = entry
                    if person_key: existing_entries[person_key] = entry
        
        if need_clear_sheet or new_rows:
            set_cached_values(WORKSHEET_MEAL_TRACKER_NAME, all_values)
            rows_snapshot = [list(r) for r in new_rows]
            def _bg_sync_meal(do_clear, to_append):
                ws = get_worksheet_cached_obj(WORKSHEET_MEAL_TRACKER_NAME, create_headers=MEAL_HEADERS)
                if not ws:
                    return
                if do_clear:
                    ws.clear()
                    ws.append_row(MEAL_HEADERS)
                if to_append:
                    ws.append_rows(to_append, value_input_option='USER_ENTERED')
            run_bg_sheet_task(_bg_sync_meal, need_clear_sheet, rows_snapshot, desc="sync_meal_sheet")
            
        return final_data

    except Exception as e:
        print(f"Lỗi sync meal sheet: {e}")
        return []


def update_meal_status(group_id, session_type, staff_name, clicker_name, target_status='done'):
    """
    Cập nhật trạng thái ăn uống tức thì trong RAM Cache và đẩy lệnh ghi Google Sheets chạy ngầm.
    Tự động đồng bộ nếu ca/ngày hôm nay chưa có dòng trong bảng, so khớp thông minh bằng match_staff_name
    và tự động bổ sung dòng hoàn tất cứu hộ nếu dữ liệu lịch bị thiếu sót.
    """
    try:
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
        time_now = datetime.now(tz_vietnam).strftime('%H:%M') if target_status == 'done' else ''

        all_values = get_cached_values(WORKSHEET_MEAL_TRACKER_NAME, ttl=60, create_headers=MEAL_HEADERS)
        
        # 1. Tự động kiểm tra xem ngày hôm nay và ca này đã được khởi tạo trong cache chưa
        has_session_today = False
        if all_values and len(all_values) > 1:
            for r in all_values[1:]:
                if len(r) > 2 and str(r[1]).strip() == today_str and str(r[2]).strip() == session_type:
                    has_session_today = True
                    break

        if not has_session_today:
            sync_meal_sheet(group_id, session_type)
            all_values = get_cached_values(WORKSHEET_MEAL_TRACKER_NAME, ttl=60, create_headers=MEAL_HEADERS)
            if not all_values:
                all_values = [list(MEAL_HEADERS)]

        row_index = -1
        current_status = None
        target_gid = str(group_id or '').strip()

        # Bước 1: Khớp chính xác hôm nay + đúng ca + tên khớp + cùng group_id
        if target_gid and target_gid != 'default':
            for i, row in enumerate(all_values[1:], start=2):
                if len(row) < 5: continue
                r_group = str(row[0]).strip()
                r_date = str(row[1]).strip()
                r_session = str(row[2]).strip()
                r_name = str(row[4]).strip()
                if r_date == today_str and r_session == session_type and r_group == target_gid:
                    if match_staff_name(r_name, staff_name):
                        row_index = i
                        if len(row) >= 6: current_status = row[5].strip()
                        break

        # Bước 2: Khớp hôm nay + đúng ca + tên khớp (bỏ qua group_id nếu bấm chéo nhóm hoặc từ Web App)
        if row_index == -1:
            for i, row in enumerate(all_values[1:], start=2):
                if len(row) < 5: continue
                r_date = str(row[1]).strip()
                r_session = str(row[2]).strip()
                r_name = str(row[4]).strip()
                if r_date == today_str and r_session == session_type:
                    if match_staff_name(r_name, staff_name):
                        row_index = i
                        if len(row) >= 6: current_status = row[5].strip()
                        break

        # Bước 3: Khớp hôm nay + tên khớp (bất kỳ ca nào)
        if row_index == -1:
            for i, row in enumerate(all_values[1:], start=2):
                if len(row) < 5: continue
                r_date = str(row[1]).strip()
                r_name = str(row[4]).strip()
                if r_date == today_str and match_staff_name(r_name, staff_name):
                    row_index = i
                    if len(row) >= 6: current_status = row[5].strip()
                    break

        if row_index != -1:
            if current_status == target_status:
                return "already", None

            clicked_user = clicker_name if target_status == 'done' else ''
            # Cập nhật ngay trong RAM
            mem_row = all_values[row_index - 1]
            while len(mem_row) < 8:
                mem_row.append('')
            mem_row[5] = target_status
            mem_row[6] = time_now
            mem_row[7] = clicked_user
            set_cached_values(WORKSHEET_MEAL_TRACKER_NAME, all_values)

            # Đẩy ghi Sheet chạy ngầm
            def _bg_update_meal(r_idx, st, tm, usr):
                ws = get_worksheet_cached_obj(WORKSHEET_MEAL_TRACKER_NAME, create_headers=MEAL_HEADERS)
                if not ws:
                    return
                cells = [
                    gspread.Cell(r_idx, 6, st),
                    gspread.Cell(r_idx, 7, tm),
                    gspread.Cell(r_idx, 8, usr)
                ]
                ws.update_cells(cells)
            run_bg_sheet_task(_bg_update_meal, row_index, target_status, time_now, clicked_user, desc="update_meal_status")
            return True, time_now

        # Bước 4: Fallback cứu hộ an toàn tuyệt đối (Không bao giờ báo lỗi cho người dùng)
        clicked_user = clicker_name if target_status == 'done' else ''
        new_row = [
            str(group_id or 'default'),
            today_str,
            session_type,
            'NV',
            staff_name,
            target_status,
            time_now,
            clicked_user
        ]
        all_values.append(new_row)
        set_cached_values(WORKSHEET_MEAL_TRACKER_NAME, all_values)

        def _bg_append_meal(to_append):
            ws = get_worksheet_cached_obj(WORKSHEET_MEAL_TRACKER_NAME, create_headers=MEAL_HEADERS)
            if ws:
                ws.append_rows([to_append], value_input_option='USER_ENTERED')
        run_bg_sheet_task(_bg_append_meal, new_row, desc=f"fallback_add_meal_{staff_name}")
        return True, time_now

    except Exception as e:
        print(f"Lỗi update meal status: {e}")
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
        
        display_name = (name[:15] + '..') if len(name) > 16 else name

        left_side = {
            "type": "text", "text": f"{index}. {display_name}", 
            "size": "xxs", "color": "#111111", "flex": 1, "gravity": "center", "wrap": False
        }

        if is_done:
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
            right_side = {
                "type": "button", "style": "secondary", "height": "sm", 
                "action": {"type": "postback", "label": "🍲", "data": f"action=meal_checkin&session={session_type}&name={name}&target_status=done"},
                "flex": 0, "width": "40px", "margin": "xs"
            }
            
        return {"type": "box", "layout": "horizontal", "contents": [left_side, right_side], "margin": "xs", "alignItems": "center"}

    def create_section_grid(title, items, icon):
        if not items: return None
        header = {"type": "text", "text": f"{icon} {title} ({len(items)})", "weight": "bold", "size": "sm", "color": "#555555", "margin": "lg"}
        
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