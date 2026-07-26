import re
import math
from datetime import datetime
import pytz
import unicodedata
import gspread
from linebot.models import FlexSendMessage, TextSendMessage

# Import từ file cấu hình trung tâm
from config import CLIENT, SHEET_NAME, WORKSHEET_SCHEDULES_NAME, WORKSHEET_VESINH_TRACKER_NAME, get_spreadsheet
from meal_handler import get_working_staff, normalize_text

VESINH_HEADERS = ['group_id', 'date', 'session', 'type', 'name', 'zone', 'status', 'time_clicked', 'clicked_by']

ZONES = {
    1: "Bàn thu ngân + dãy lọc nước phía sau + Sạc ĐT và POS",
    2: "Chạy và vệ sinh tủ điện thoại",
    3: "Vệ sinh khu laptop + tủ đồng hồ",
    4: "Vệ sinh khu bàn trải nghiệm điện thoại + loa + camera",
    5: "Vách phụ kiện"
}

def get_current_vesinh_session(override_session=None):
    """
    Xác định ca vệ sinh:
    - Nếu trước 14h -> Ca Sáng (vesinh_sang)
    - Nếu từ 14h trở đi -> Ca Chiều (vesinh_chieu)
    """
    if override_session in ['vesinh_sang', 'vesinh_chieu', 'sang', 'chieu']:
        if override_session in ['vesinh_sang', 'sang']:
            return 'vesinh_sang'
        return 'vesinh_chieu'
        
    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    current_hour = datetime.now(tz_vietnam).hour
    if current_hour < 14:
        return 'vesinh_sang'
    else:
        return 'vesinh_chieu'

def allocate_cleaning_zones(nv_list):
    """
    Phân bổ 5 khu vực vệ sinh theo đúng quy tắc:
    1. ERP: Có chữ 'ERP' trong tên -> Phân công Khu 1 (hoặc Khu 1 + 5 nếu < 6 NV)
    2. Nam: Tên KHÔNG chứa dấu '*' -> Phân công Khu 2 / Khu 4
    3. GH2: Tên có chữ 'GH2' -> Không phân công vệ sinh
    4. TH1 (<6 NV): ERP -> Khu 1+5; 2 Nam -> Khu 2 & Khu 4; Còn lại -> Khu 3
    5. TH2 (>=6 NV): ERP -> Khu 1; 2 Nam -> Khu 2; Còn lại -> Chia đều Khu 3, 4, 5
    """
    if not nv_list:
        return []

    # Phân loại nhân viên
    gh2_staff = [name for name in nv_list if "GH2" in name.upper()]
    cleaning_nv = [name for name in nv_list if "GH2" not in name.upper()]
    
    total_cleaning = len(cleaning_nv)
    
    erp_staff = [name for name in cleaning_nv if "ERP" in name.upper()]
    # Nhân viên Nam là nhân viên trong tên không có dấu '*'
    male_staff = [name for name in cleaning_nv if "*" not in name and name not in erp_staff]
    other_staff = [name for name in cleaning_nv if name not in erp_staff and name not in male_staff]

    assignments = [] # List dicts: {'name': str, 'zones': [int], 'zone_desc': str, 'type': 'NV'}

    if total_cleaning < 6:
        # --- TH1: THIẾU NHÂN SỰ (< 6 NV) ---
        # 1. ERP -> Khu 1 và Khu 5
        erp_assigned = False
        if erp_staff:
            for p in erp_staff:
                assignments.append({
                    'name': p,
                    'zones': [1, 5],
                    'zone_desc': f"Khu 1: {ZONES[1]} & Khu 5: {ZONES[5]}"
                })
            erp_assigned = True
        
        # 2. Nam & GH1 -> Nếu có GH1: gộp GH1 + 1 Nam cùng làm Khu 2 & Khu 4
        assigned_males = []
        gh1_in_list = [p for p in male_staff if "GH1" in p.upper()]
        non_gh1_males = [p for p in male_staff if "GH1" not in p.upper()]

        if gh1_in_list:
            gh1_p = gh1_in_list[0]
            if non_gh1_males:
                partner_male = non_gh1_males[0]
                desc = f"Khu 2: {ZONES[2]} & Khu 4: {ZONES[4]} (Gộp GH1 + 1 Nam)"
                assignments.append({'name': gh1_p, 'zones': [2, 4], 'zone_desc': desc})
                assignments.append({'name': partner_male, 'zones': [2, 4], 'zone_desc': desc})
                assigned_males = [gh1_p, partner_male]
            else:
                desc = f"Khu 2: {ZONES[2]} & Khu 4: {ZONES[4]}"
                assignments.append({'name': gh1_p, 'zones': [2, 4], 'zone_desc': desc})
                assigned_males = [gh1_p]
        else:
            if len(male_staff) >= 2:
                assignments.append({'name': male_staff[0], 'zones': [2], 'zone_desc': f"Khu 2: {ZONES[2]}"})
                assignments.append({'name': male_staff[1], 'zones': [4], 'zone_desc': f"Khu 4: {ZONES[4]}"})
                assigned_males = male_staff[:2]
            elif len(male_staff) == 1:
                assignments.append({'name': male_staff[0], 'zones': [2, 4], 'zone_desc': f"Khu 2: {ZONES[2]} & Khu 4: {ZONES[4]}"})
                assigned_males = male_staff[:1]
        
        # 3. Còn lại -> Khu 3 (và khu 1, 5 nếu chưa có ERP)
        remaining = [p for p in cleaning_nv if p not in erp_staff and p not in assigned_males]
        for p in remaining:
            desc = f"Khu 3: {ZONES[3]}"
            if not erp_assigned:
                desc += f" & Khu 1: {ZONES[1]} & Khu 5: {ZONES[5]}"
                erp_assigned = True
            assignments.append({'name': p, 'zones': [3], 'zone_desc': desc})
            
    else:
        # --- TH2: ĐỦ NHÂN SỰ (>= 6 NV) ---
        # 1. ERP -> Khu 1
        used_staff = set()
        if erp_staff:
            for p in erp_staff:
                assignments.append({'name': p, 'zones': [1], 'zone_desc': f"Khu 1: {ZONES[1]}"})
                used_staff.add(p)
        else:
            if cleaning_nv:
                p = cleaning_nv[0]
                assignments.append({'name': p, 'zones': [1], 'zone_desc': f"Khu 1: {ZONES[1]}"})
                used_staff.add(p)

        # 2. Nam & GH1 -> Nếu có GH1: gộp GH1 + 2 Nam cùng làm Khu 2
        assigned_males = [p for p in male_staff if p not in used_staff]
        gh1_in_list = [p for p in assigned_males if "GH1" in p.upper()]
        non_gh1_males = [p for p in assigned_males if "GH1" not in p.upper()]

        if gh1_in_list:
            gh1_p = gh1_in_list[0]
            if len(non_gh1_males) >= 2:
                m1, m2 = non_gh1_males[0], non_gh1_males[1]
                desc = f"Khu 2: {ZONES[2]} (Gộp GH1 + 2 Nam)"
                assignments.append({'name': gh1_p, 'zones': [2], 'zone_desc': desc})
                assignments.append({'name': m1, 'zones': [2], 'zone_desc': desc})
                assignments.append({'name': m2, 'zones': [2], 'zone_desc': desc})
                used_staff.update([gh1_p, m1, m2])
            elif len(non_gh1_males) == 1:
                m1 = non_gh1_males[0]
                desc = f"Khu 2: {ZONES[2]} (Gộp GH1 + 1 Nam)"
                assignments.append({'name': gh1_p, 'zones': [2], 'zone_desc': desc})
                assignments.append({'name': m1, 'zones': [2], 'zone_desc': desc})
                used_staff.update([gh1_p, m1])
            else:
                assignments.append({'name': gh1_p, 'zones': [2], 'zone_desc': f"Khu 2: {ZONES[2]}"})
                used_staff.add(gh1_p)
        else:
            if len(assigned_males) >= 2:
                assignments.append({'name': assigned_males[0], 'zones': [2], 'zone_desc': f"Khu 2: {ZONES[2]}"})
                assignments.append({'name': assigned_males[1], 'zones': [2], 'zone_desc': f"Khu 2: {ZONES[2]}"})
                used_staff.add(assigned_males[0])
                used_staff.add(assigned_males[1])
            elif len(assigned_males) == 1:
                assignments.append({'name': assigned_males[0], 'zones': [2], 'zone_desc': f"Khu 2: {ZONES[2]}"})
                used_staff.add(assigned_males[0])

        # 3. Nhân viên còn lại -> Chia đều Khu 3, Khu 4, Khu 5
        pool = [p for p in cleaning_nv if p not in used_staff]
        rem_zones = [3, 4, 5]
        
        if pool:
            for idx, p in enumerate(pool):
                z_id = rem_zones[idx % len(rem_zones)]
                assignments.append({'name': p, 'zones': [z_id], 'zone_desc': f"Khu {z_id}: {ZONES[z_id]}"})
        else:
            for idx, z_id in enumerate(rem_zones):
                if assignments:
                    target_person = assignments[idx % len(assignments)]
                    target_person['zone_desc'] += f" & Khu {z_id}: {ZONES[z_id]}"

    # Ghi nhận các bạn GH2 (không phân chia)
    for p in gh2_staff:
        assignments.append({'name': p, 'zones': [], 'zone_desc': "Bảo lưu / Trực GH2 (Không phân chia vệ sinh)"})

    return assignments

def clean_staff_name(name):
    name = re.sub(r'^\(\d+.*?\):?\s*', '', name)
    name = re.sub(r'^[•\-\+:\.]\s*', '', name)
    return name.strip()

def parse_staff_from_raw(raw_text, target_shift):
    if not raw_text:
        return []
    keywords = ["Ca Sáng", "Ca Chiều", "Nghỉ", "Vệ Sinh Kho", "Vệ Sinh"]
    pattern = '|'.join(keywords)
    parts = re.split(f'({pattern})', str(raw_text).replace('<br>', '\n'))
    names = []
    i = 1 if parts and not parts[0].strip() else 0
    while i < len(parts):
        shift_name = parts[i].strip()
        content = parts[i+1].strip().lstrip(':').lstrip(';').strip() if i + 1 < len(parts) else ""
        if shift_name.lower() == target_shift.lower():
            raw_names = re.split(r'[,\n;•\+]', content)
            for n in raw_names:
                cn = clean_staff_name(n)
                if cn and not cn.isdigit() and len(cn) > 1:
                    names.append(cn)
        i += 2

    # Khử trùng lặp danh sách tên
    seen = set()
    unique_names = []
    for n in names:
        norm = normalize_text(n)
        if norm not in seen:
            seen.add(norm)
            unique_names.append(n)
    return unique_names

def get_working_staff_vesinh(session_type):
    meal_session = 'ansang' if session_type == 'vesinh_sang' else 'anchieu'
    target_shift_name = "Ca Sáng" if session_type == 'vesinh_sang' else "Ca Chiều"
    
    staff_lists = {}
    try:
        staff_lists = get_working_staff(meal_session)
    except Exception as e:
        print(f"Lỗi get_working_staff: {e}")
        
    nv_list = staff_lists.get('NV', []) if isinstance(staff_lists, dict) else []
    pg_list = staff_lists.get('PG', []) if isinstance(staff_lists, dict) else []

    if not nv_list:
        try:
            from meal_handler import get_vietnamese_day_of_week
            day_str = get_vietnamese_day_of_week()
            sheet = get_spreadsheet().worksheet(WORKSHEET_SCHEDULES_NAME)
            records = sheet.get_all_records()
            today_sched = next((row for row in records if row.get('day_of_week') == day_str), None)
            if today_sched:
                nv_raw = today_sched.get('employee_schedule', '')
                pg_raw = today_sched.get('pg_schedule', '')
                nv_list = parse_staff_from_raw(nv_raw, target_shift_name)
                pg_list = parse_staff_from_raw(pg_raw, target_shift_name)
        except Exception as err:
            print(f"Lỗi parse fallback lịch vệ sinh: {err}")

    # Xử lý vệ sinh kho (chỉ lấy cho ca sáng)
    pg_kho_list = []
    if session_type == 'vesinh_sang':
        try:
            from meal_handler import get_vietnamese_day_of_week
            day_str = get_vietnamese_day_of_week()
            sheet = get_spreadsheet().worksheet(WORKSHEET_SCHEDULES_NAME)
            records = sheet.get_all_records()
            today_sched = next((row for row in records if row.get('day_of_week') == day_str), None)
            if today_sched:
                pg_raw = today_sched.get('pg_schedule', '')
                pg_kho_list = parse_staff_from_raw(pg_raw, "Vệ Sinh Kho")
        except Exception as err:
            print(f"Lỗi đọc vệ sinh kho: {err}")

    # Áp dụng quy tắc vệ sinh ca chiều
    if session_type == 'vesinh_chieu':
        try:
            from meal_handler import get_vietnamese_day_of_week, normalize_text
            day_str = get_vietnamese_day_of_week()
            
            if day_str in ["Thứ Bảy", "Chủ Nhật"]:
                # Thứ 7, Chủ Nhật: Chiều chỉ PG vệ sinh, NV đã vệ sinh sáng
                nv_list = []
            else:
                # Thứ 2 - Thứ 6: Loại bỏ NV đã làm Ca Sáng (làm cả ngày)
                morning_staff = get_working_staff('ansang')
                morning_nvs = morning_staff.get('NV', []) if isinstance(morning_staff, dict) else []
                if not morning_nvs:
                    sheet = get_spreadsheet().worksheet(WORKSHEET_SCHEDULES_NAME)
                    records = sheet.get_all_records()
                    today_sched = next((row for row in records if row.get('day_of_week') == day_str), None)
                    if today_sched:
                        nv_raw = today_sched.get('employee_schedule', '')
                        morning_nvs = parse_staff_from_raw(nv_raw, "Ca Sáng")
                
                morning_nvs_norm = [normalize_text(name) for name in morning_nvs]
                nv_list = [nv for nv in nv_list if normalize_text(nv) not in morning_nvs_norm]
        except Exception as e:
            print(f"Lỗi lọc nhân viên vệ sinh: {e}")

    return {'NV': nv_list, 'PG': pg_list, 'PG_KHO': pg_kho_list}

def sync_vesinh_sheet(group_id, session_type):
    """
    Đồng bộ dữ liệu vệ sinh vào Google Sheets 'vesinh_tracker'.
    Khởi tạo worksheet nếu chưa tồn tại.
    """
    sheet = None
    try:
        sheet = get_spreadsheet().worksheet(WORKSHEET_VESINH_TRACKER_NAME)
    except Exception as e:
        print(f"Chưa tìm thấy worksheet '{WORKSHEET_VESINH_TRACKER_NAME}', đang tạo mới: {e}")
        try:
            spreadsheet = get_spreadsheet()
            sheet = spreadsheet.add_worksheet(title=WORKSHEET_VESINH_TRACKER_NAME, rows=100, cols=10)
            sheet.append_row(VESINH_HEADERS)
        except Exception as create_err:
            print(f"Lỗi khởi tạo worksheet '{WORKSHEET_VESINH_TRACKER_NAME}': {create_err}")
            sheet = None

    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
    
    all_records = []
    if sheet:
        try:
            first_data_date = None
            try:
                val = sheet.acell('B2').value 
                if val: first_data_date = val
            except: pass

            if first_data_date and first_data_date != today_str:
                print(f"Ngày mới! Xóa dữ liệu vệ sinh cũ ({first_data_date})...")
                sheet.clear()
                sheet.append_row(VESINH_HEADERS)
                all_records = []
            else:
                all_records = sheet.get_all_records()
        except Exception as sheet_err:
            print(f"Lỗi đọc dữ liệu từ worksheet vesinh: {sheet_err}")

    existing_entries = {}
    for row in all_records:
        key_name = normalize_text(row.get('name'))
        if (str(row.get('group_id')) == str(group_id) and 
            row.get('date') == today_str and 
            row.get('session') == session_type):
            # Nếu đã có dòng và dòng hiện tại đã 'done', ưu tiên giữ dòng 'done'
            if key_name not in existing_entries or row.get('status') == 'done':
                existing_entries[key_name] = row

    staff_lists = get_working_staff_vesinh(session_type)
    raw_nv_list = staff_lists.get('NV', [])
    raw_pg_list = staff_lists.get('PG', [])
    raw_pg_kho_list = staff_lists.get('PG_KHO', [])

    # Khử trùng lặp danh sách PG Kho
    seen_pg_kho = set()
    pg_kho_list = []
    for name in raw_pg_kho_list:
        norm = normalize_text(name)
        if norm not in seen_pg_kho:
            seen_pg_kho.add(norm)
            pg_kho_list.append(name)

    # Khử trùng lặp danh sách PG Gian hàng & loại bỏ ai đã làm Kho
    seen_pg = set()
    pg_list = []
    for name in raw_pg_list:
        norm = normalize_text(name)
        if norm not in seen_pg_kho and norm not in seen_pg:
            seen_pg.add(norm)
            pg_list.append(name)

    nv_list = raw_nv_list
    nv_assignments = allocate_cleaning_zones(nv_list)

    final_data = [] 
    new_rows = []
    processed_names = set()

    # 1. Nhân viên NV
    for assign in nv_assignments:
        name = assign['name']
        zone_desc = assign['zone_desc']
        norm_name = normalize_text(name)
        if norm_name in processed_names:
            continue
        processed_names.add(norm_name)
        
        if norm_name in existing_entries:
            item = existing_entries[norm_name]
            item['zone'] = zone_desc
            final_data.append(item)
        else:
            entry = {
                'group_id': group_id, 'date': today_str, 'session': session_type,
                'type': 'NV', 'name': name, 'zone': zone_desc, 'status': 'waiting',
                'time_clicked': '', 'clicked_by': ''
            }
            new_rows.append([group_id, today_str, session_type, 'NV', name, zone_desc, 'waiting', '', ''])
            existing_entries[norm_name] = entry
            final_data.append(entry)

    # 2. PG Kho
    for name in pg_kho_list:
        norm_name = normalize_text(name)
        if norm_name in processed_names:
            continue
        processed_names.add(norm_name)
        zone_desc = "Vệ sinh kho"
        if norm_name in existing_entries:
            final_data.append(existing_entries[norm_name])
        else:
            entry = {
                'group_id': group_id, 'date': today_str, 'session': session_type,
                'type': 'PG_KHO', 'name': name, 'zone': zone_desc, 'status': 'waiting',
                'time_clicked': '', 'clicked_by': ''
            }
            new_rows.append([group_id, today_str, session_type, 'PG_KHO', name, zone_desc, 'waiting', '', ''])
            existing_entries[norm_name] = entry
            final_data.append(entry)

    # 3. PG Gian hàng
    for name in pg_list:
        norm_name = normalize_text(name)
        if norm_name in processed_names:
            continue
        processed_names.add(norm_name)
        zone_desc = "Vệ sinh gian hàng PG"
        if norm_name in existing_entries:
            final_data.append(existing_entries[norm_name])
        else:
            entry = {
                'group_id': group_id, 'date': today_str, 'session': session_type,
                'type': 'PG', 'name': name, 'zone': zone_desc, 'status': 'waiting',
                'time_clicked': '', 'clicked_by': ''
            }
            new_rows.append([group_id, today_str, session_type, 'PG', name, zone_desc, 'waiting', '', ''])
            existing_entries[norm_name] = entry
            final_data.append(entry)

    if new_rows and sheet:
        try:
            sheet.append_rows(new_rows, value_input_option='USER_ENTERED')
        except Exception as append_err:
            print(f"Lỗi ghi dữ liệu mới vào sheet vesinh: {append_err}")

    return final_data

def update_vesinh_status(group_id, session_type, staff_name, clicker_name, target_status='done'):
    """
    Cập nhật trạng thái hoàn thành vệ sinh khi bấm nút.
    """
    try:
        sheet = get_spreadsheet().worksheet(WORKSHEET_VESINH_TRACKER_NAME)
        all_values = sheet.get_all_values()
        
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
        time_now = datetime.now(tz_vietnam).strftime('%H:%M') if target_status == 'done' else ''

        target_name_norm = normalize_text(staff_name)
        target_group_id = str(group_id).strip()

        matching_rows = []
        already_done = True
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
                matching_rows.append((i, row))
                if len(row) < 7 or row[6].strip() != target_status:
                    already_done = False
        
        if matching_rows:
            if already_done:
                return "already", None

            clicked_user = clicker_name if target_status == 'done' else ''
            cells = []
            for row_index, _ in matching_rows:
                cells.extend([
                    gspread.Cell(row_index, 7, target_status), # Column G: status
                    gspread.Cell(row_index, 8, time_now),      # Column H: time_clicked
                    gspread.Cell(row_index, 9, clicked_user)   # Column I: clicked_by
                ])
            sheet.update_cells(cells)
            return True, time_now
        
        return False, None
    except Exception as e:
        print(f"Lỗi update vesinh status: {e}")
        return False, None

def generate_vesinh_flex(group_id, session_type=None):
    """
    Tạo Flex Message giao diện Phân Công & Theo Dõi Vệ Sinh.
    Thiết kế mới: Gom nhóm theo Khu (Zone Header ở trên, danh sách nhân viên & nút hoàn tất bên phải ở dưới).
    """
    if not session_type:
        session_type = get_current_vesinh_session()

    data = sync_vesinh_sheet(group_id, session_type)
    if not data:
        return None

    is_sang = (session_type == 'vesinh_sang')
    shift_name = "CA SÁNG" if is_sang else "CA CHIỀU"
    header_title = f"🧹 BẢNG PHÂN CÔNG VỆ SINH ({shift_name})"
    header_color = "#0d9488" if is_sang else "#0f766e"

    nv_data = [d for d in data if d.get('type') == 'NV']
    pg_data = [d for d in data if d.get('type') == 'PG']
    pg_kho_data = [d for d in data if d.get('type') == 'PG_KHO']

    body_contents = []

    # --- NHÂN VIÊN SECTION ---
    if nv_data:
        body_contents.append({
            "type": "text",
            "text": f"🧹 PHÂN CÔNG VỆ SINH NV ({len(nv_data)} người)",
            "weight": "bold",
            "size": "xs",
            "color": "#0f766e",
            "margin": "xs"
        })

        # Gom nhóm NV theo Khu (zone)
        zone_groups = {}
        for item in nv_data:
            z_desc = item.get('zone', 'Khu vực khác')
            if z_desc not in zone_groups:
                zone_groups[z_desc] = []
            zone_groups[z_desc].append(item)

        global_nv_idx = 1
        for z_desc, staff_list in zone_groups.items():
            is_gh2_zone = "GH2" in z_desc.upper()
            zone_header_title = f"📍 {z_desc}" if not is_gh2_zone else f"⚪ {z_desc}"
            
            zone_box_contents = [
                {
                    "type": "text",
                    "text": zone_header_title,
                    "weight": "bold",
                    "size": "xs",
                    "color": "#0f766e" if not is_gh2_zone else "#64748b",
                    "wrap": True
                },
                {"type": "separator", "color": "#cbd5e1" if not is_gh2_zone else "#e2e8f0", "margin": "xs"}
            ]

            for idx, item in enumerate(staff_list):
                is_done = (item.get('status') == 'done')
                time_val = item.get('time_clicked', '')
                name = item.get('name', '')
                is_gh2 = "GH2" in z_desc.upper()

                status_text = f"✅ Xong ({time_val})" if is_done else ("⚪ GH2" if is_gh2 else "⏳ Chờ")
                text_color = "#16a34a" if is_done else ("#94a3b8" if is_gh2 else "#d97706")

                btn_color = "#0284c7" if not is_done else "#ef4444"
                btn_label = "Hoàn tất" if not is_done else "Hủy"
                next_status = "done" if not is_done else "waiting"

                if idx > 0:
                    zone_box_contents.append({"type": "separator", "color": "#f1f5f9", "margin": "xs"})

                staff_row = {
                    "type": "box",
                    "layout": "horizontal",
                    "margin": "xs",
                    "alignItems": "center",
                    "contents": [
                        {
                            "type": "box",
                            "layout": "vertical",
                            "flex": 6,
                            "contents": [
                                {
                                    "type": "text",
                                    "text": f"{global_nv_idx}. {name}",
                                    "weight": "bold",
                                    "size": "xs",
                                    "color": "#0f172a",
                                    "wrap": True
                                },
                                {
                                    "type": "text",
                                    "text": status_text,
                                    "weight": "bold",
                                    "size": "xxs",
                                    "color": text_color,
                                    "margin": "xs"
                                }
                            ]
                        }
                    ]
                }

                if not is_gh2:
                    staff_row["contents"].append({
                        "type": "button",
                        "action": {
                            "type": "postback",
                            "label": btn_label,
                            "data": f"action=complete_vesinh&session={session_type}&name={name}&target_status={next_status}"
                        },
                        "style": "primary",
                        "color": btn_color,
                        "size": "xs",
                        "height": "sm",
                        "flex": 4
                    })

                zone_box_contents.append(staff_row)
                global_nv_idx += 1

            zone_card = {
                "type": "box",
                "layout": "vertical",
                "margin": "md",
                "paddingAll": "sm",
                "backgroundColor": "#f8fafc",
                "cornerRadius": "md",
                "borderColor": "#cbd5e1",
                "borderWidth": "1px",
                "contents": zone_box_contents
            }
            body_contents.append(zone_card)

    # --- PG SECTION ---
    if pg_data or pg_kho_data:
        body_contents.append({
            "type": "separator",
            "margin": "md"
        })
        
        total_pg_count = len(pg_data) + len(pg_kho_data)
        body_contents.append({
            "type": "text",
            "text": f"👗 PHÂN CÔNG VỆ SINH PG ({total_pg_count} người)",
            "weight": "bold",
            "size": "xs",
            "color": "#0369a1",
            "margin": "md"
        })

        pg_box_contents = []

        # Render PG KHO trước
        if pg_kho_data:
            pg_box_contents.append({
                "type": "text",
                "text": "📦 Vệ sinh kho",
                "weight": "bold",
                "size": "xs",
                "color": "#333333",
                "margin": "sm"
            })
            pg_box_contents.append({"type": "separator", "color": "#fde68a", "margin": "xs"})
            
            for idx, item in enumerate(pg_kho_data, 1):
                is_done = (item.get('status') == 'done')
                time_val = item.get('time_clicked', '')
                name = item.get('name', '')

                status_text = f"✅ Xong ({time_val})" if is_done else "⏳ Chờ"
                text_color = "#16a34a" if is_done else "#d97706"

                btn_color = "#0284c7" if not is_done else "#ef4444"
                btn_label = "Hoàn tất" if not is_done else "Hủy"
                next_status = "done" if not is_done else "waiting"

                if idx > 1:
                    pg_box_contents.append({"type": "separator", "color": "#fef3c7", "margin": "xs"})

                pg_box_contents.append({
                    "type": "box",
                    "layout": "horizontal",
                    "margin": "xs",
                    "alignItems": "center",
                    "contents": [
                        {
                            "type": "box",
                            "layout": "vertical",
                            "flex": 6,
                            "contents": [
                                {
                                    "type": "text",
                                    "text": f"{idx}. {name}",
                                    "weight": "bold",
                                    "size": "xs",
                                    "color": "#0f172a",
                                    "wrap": True
                                },
                                {
                                    "type": "text",
                                    "text": status_text,
                                    "weight": "bold",
                                    "size": "xxs",
                                    "color": text_color,
                                    "margin": "xs"
                                }
                            ]
                        },
                        {
                            "type": "button",
                            "action": {
                                "type": "postback",
                                "label": btn_label,
                                "data": f"action=complete_vesinh&session={session_type}&name={name}&target_status={next_status}"
                            },
                            "style": "primary",
                            "color": btn_color,
                            "size": "xs",
                            "height": "sm",
                            "flex": 4
                        }
                    ]
                })

        # Render PG gian hàng
        if pg_data:
            if pg_kho_data:
                pg_box_contents.append({"type": "separator", "margin": "md"})
            
            pg_box_contents.append({
                "type": "text",
                "text": "📍 Vệ sinh gian hàng PG",
                "weight": "bold",
                "size": "xs",
                "color": "#333333",
                "margin": "sm"
            })
            pg_box_contents.append({"type": "separator", "color": "#fde68a", "margin": "xs"})
            
            for idx, item in enumerate(pg_data, 1):
                is_done = (item.get('status') == 'done')
                time_val = item.get('time_clicked', '')
                name = item.get('name', '')

                status_text = f"✅ Xong ({time_val})" if is_done else "⏳ Chờ"
                text_color = "#16a34a" if is_done else "#d97706"

                btn_color = "#0284c7" if not is_done else "#ef4444"
                btn_label = "Hoàn tất" if not is_done else "Hủy"
                next_status = "done" if not is_done else "waiting"

                if idx > 1:
                    pg_box_contents.append({"type": "separator", "color": "#fef3c7", "margin": "xs"})

                pg_box_contents.append({
                    "type": "box",
                    "layout": "horizontal",
                    "margin": "xs",
                    "alignItems": "center",
                    "contents": [
                        {
                            "type": "box",
                            "layout": "vertical",
                            "flex": 6,
                            "contents": [
                                {
                                    "type": "text",
                                    "text": f"{idx}. {name}",
                                    "weight": "bold",
                                    "size": "xs",
                                    "color": "#0f172a",
                                    "wrap": True
                                },
                                {
                                    "type": "text",
                                    "text": status_text,
                                    "weight": "bold",
                                    "size": "xxs",
                                    "color": text_color,
                                    "margin": "xs"
                                }
                            ]
                        },
                        {
                            "type": "button",
                            "action": {
                                "type": "postback",
                                "label": btn_label,
                                "data": f"action=complete_vesinh&session={session_type}&name={name}&target_status={next_status}"
                            },
                            "style": "primary",
                            "color": btn_color,
                            "size": "xs",
                            "height": "sm",
                            "flex": 4
                        }
                    ]
                })

        pg_card = {
            "type": "box",
            "layout": "vertical",
            "margin": "md",
            "paddingAll": "sm",
            "backgroundColor": "#fffbeb",
            "cornerRadius": "md",
            "borderColor": "#fde68a",
            "borderWidth": "1px",
            "contents": pg_box_contents
        }
        body_contents.append(pg_card)

    flex_bubble = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": header_color,
            "paddingAll": "md",
            "contents": [
                {
                    "type": "text",
                    "text": header_title,
                    "weight": "bold",
                    "size": "sm",
                    "color": "#ffffff",
                    "align": "center",
                    "wrap": True
                }
            ]
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#ffffff",
            "paddingAll": "md",
            "contents": body_contents
        }
    }

    return flex_bubble
