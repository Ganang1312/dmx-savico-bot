import re
import math
from datetime import datetime
import pytz
import unicodedata
import gspread
from linebot.models import FlexSendMessage

# Import từ file cấu hình trung tâm
from config import CLIENT, SHEET_NAME, WORKSHEET_SCHEDULES_NAME, WORKSHEET_MEAL_TRACKER_NAME, get_spreadsheet

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


# ============================================================================
# DANH SÁCH NGƯỜI LÀM HÔM NAY — đọc thẳng sheet `schedules` (thêm 21/09/2026)
# ----------------------------------------------------------------------------
# Sheet `schedules` đã có lịch làm việc của NV + PG cho cả tuần nên đây mới là
# nguồn chuẩn. Hàm dưới đọc ô `employee_schedule` + `pg_schedule` của ĐÚNG ngày
# hôm nay, gộp cả Ca Sáng và Ca Chiều, rồi khử trùng theo tên đã chuẩn hoá
# (bỏ dấu / emoji / ký hiệu vai trò).
#
# ĐỊNH DẠNG Ô — suy ra từ schedule_handler.py dòng 74 và 94:
#   employee_schedule: "Ca Sáng (8 NV): A (ERP), B, C\nCa Chiều (7 NV): D, E\nNghỉ: ..."
#       -> tên NV ngăn bằng DẤU PHẨY, có tiền tố "(8 NV): "
#   pg_schedule:       "Ca Sáng (6): A\nB\nC\nCa Chiều (5): ..."
#       -> tên PG ngăn bằng XUỐNG DÒNG, có tiền tố "(6): "
#
# Vì sao cần khử trùng: cùng một người có thể xuất hiện ở cả ca sáng lẫn ca chiều,
# hoặc bị ghi ở cả cột NV lẫn cột PG -> nếu không khử sẽ nhận 2-3 dòng việc trùng.
# ============================================================================

SCHEDULE_KEYWORDS = ["Ca Sáng", "Ca Chiều", "Nghỉ", "Vệ Sinh Kho", "Vệ Sinh"]
SHIFT_BLOCKS = ("Ca Sáng", "Ca Chiều")
# GHI CHÚ về "(off ca3)" / "(off ca4)" trong cột chi tiết:
#   Anh Dương xác nhận 22/09/2026 — đó là người đó NGHỈ CA ĐÓ trong ngày
#   (ví dụ làm ca 2-4-5-6 thì off ca 3), KHÔNG phải nghỉ cả ngày.
#   => Vẫn đi làm các ca khác, nên VẪN PHẢI có trong danh sách nhận việc.
#   Trước đây code loại họ ra là SAI. Đừng thêm lại.
#   (get_working_staff() cho điểm danh ăn uống vẫn giữ luật riêng của nó.)

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
    # BẪeY: 'Đ'/'đ' KHÔNG có phân rã NFD (không phải D + dấu gạch ngang tổ hợp),
    # nên nếu để nguyên sẽ bị regex [^a-z0-9\s] xoá mất -> "Đỗ Văn Hùng" thành
    # "o van hung". Phải quy về 'd' thủ công trước khi lọc.
    s = s.replace("đ", "d").replace("Đ", "d")
    s = re.sub(r"[^a-z0-9\s]", " ", s.lower())
    return re.sub(r"\s+", " ", s).strip()


def get_daily_roster(day_str=None):
    """
    Danh sách người làm hôm nay lấy từ sheet `schedules`, đã khử trùng.

    Trả về list[dict]: [{'name': 'Nguyễn Văn A', 'role': 'NV', 'shift': 'Ca Sáng'}, ...]
    Khử trùng theo normalize_person_key.
    """
    day = day_str or get_vietnamese_day_of_week()
    try:
        sheet = get_spreadsheet().worksheet(WORKSHEET_SCHEDULES_NAME)
        records = sheet.get_all_records()
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

    print("[ROSTER] %s: %d người sau khử trùng (%d NV, %d PG)" % (
        day, len(roster),
        sum(1 for r in roster if r["role"] == "NV"),
        sum(1 for r in roster if r["role"] == "PG")))
    return roster


def get_daily_roster_names(day_str=None):
    """Tiện dụng: chỉ lấy danh sách tên."""
    return [r["name"] for r in get_daily_roster(day_str)]


# ============================================================================
# BA NHÓM NGƯỜI NHẬN VIỆC — 3 cột E/F/G của sheet `schedules` (22/09/2026)
# ----------------------------------------------------------------------------
# CẤU TRÚC THẬT (đối chiếu file thật 22/09/2026):
#   A day_of_week | B pg_schedule | C employee_schedule | D (trống)
#   E PG | F NV | G QL+TC
#
#   Dòng 0     = tiêu đề
#   Dòng 1..7  = Thứ Hai .. Chủ Nhật
#   Dòng 8+    = cột A TRỐNG, nhưng E/F/G vẫn ghi tiếp tên
#
#   => E/F/G là DANH SÁCH DỌC trải nhiều dòng, KHÔNG phải mỗi ngày một ô.
#      Phải quét HẾT mọi dòng. Lọc theo `day_of_week` sẽ chỉ ra 1 người/nhóm.
#
#   Thực tế: E = 12 PG, F = 11 NV, G = 2 QL+TC -> tổng 25 người.
# ============================================================================

ROSTER_GROUPS = ("PG", "NV", "QL+TC")
# Cột E, F, G (0-based) — dùng khi dòng tiêu đề bị đổi tên khác
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
    """
    Đọc 3 cột E/F/G thành {'PG': [...], 'NV': [...], 'QL+TC': [...]}.

    QUÉT HẾT MỌI DÒNG — không lọc theo day_of_week, vì danh sách nằm dọc.
    """
    from task_parser import split_names

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
    """
    Bóc tên từ cột chi tiết (gộp Ca Sáng + Ca Chiều). CHỈ dùng làm dự phòng khi
    cột gọn E/F/G trống hẳn.

    KHÔNG loại người ghi "(off ca3)/(off ca4)": anh Dương xác nhận đó chỉ là
    ghi chú nghỉ CA ĐÓ trong ngày, người ta vẫn đi làm các ca khác.
    """
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
    Danh sách người nhận việc theo 3 nhóm, đọc 3 cột E/F/G của sheet `schedules`.

    Trả về {'PG': [...], 'NV': [...], 'QL+TC': [...]}

    day_str chỉ dùng cho phần DỰ PHÒNG (khi cột gọn trống hẳn).
    """
    empty = {g: [] for g in ROSTER_GROUPS}
    day = day_str or get_vietnamese_day_of_week()
    try:
        sheet = get_spreadsheet().worksheet(WORKSHEET_SCHEDULES_NAME)
        values = sheet.get_all_values()
    except Exception as e:
        print("Lỗi đọc sheet schedules: %s" % e)
        return empty

    try:
        roster = _read_roster_columns(values)
    except Exception as e:
        print("Lỗi đọc 3 cột PG/NV/QL+TC: %s" % e)
        return empty

    # Dự phòng: nhóm nào TRỐNG HẲN mới bóc tạm từ cột chi tiết của hôm nay.
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

    print("[ROSTER] PG %d | NV %d | QL+TC %d"
          % (len(roster["PG"]), len(roster["NV"]), len(roster["QL+TC"])))
    return roster


def get_working_staff(session_type):
    day_str = get_vietnamese_day_of_week()
    target_shift_name = "Ca Sáng" if session_type == 'ansang' else "Ca Chiều"
    exclude_pattern = r'off\s*ca\s*3' if session_type == 'ansang' else r'off\s*ca\s*4'
    
    try:
        sheet = get_spreadsheet().worksheet(WORKSHEET_SCHEDULES_NAME)
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
        sheet = get_spreadsheet().worksheet(WORKSHEET_MEAL_TRACKER_NAME)
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
        sheet = get_spreadsheet().worksheet(WORKSHEET_MEAL_TRACKER_NAME)
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
                return "already", None

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