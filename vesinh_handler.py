import re
import math
from datetime import datetime
import pytz
import unicodedata
import gspread
from linebot.models import FlexSendMessage, TextSendMessage

# Import từ file cấu hình trung tâm
from config import (
    CLIENT, SHEET_NAME, WORKSHEET_SCHEDULES_NAME, WORKSHEET_VESINH_TRACKER_NAME,
    get_spreadsheet, get_cached_values, get_cached_records,
    set_cached_values, run_bg_sheet_task, get_worksheet_cached_obj, values_to_records
)
from meal_handler import (
    get_working_staff, normalize_text, match_staff_name,
    clean_person_name, normalize_person_key
)

VESINH_HEADERS = ['group_id', 'date', 'session', 'type', 'name', 'zone', 'status', 'time_clicked', 'clicked_by']

ZONES = {
    1: "Bàn thu ngân + dãy lọc nước phía sau + Sạc ĐT và POS",
    2: "Chạy và vệ sinh tủ điện thoại",
    3: "Vệ sinh khu laptop + tủ đồng hồ",
    4: "Vệ sinh khu bàn trải nghiệm điện thoại + loa + camera",
    5: "Vách phụ kiện"
}

def get_current_vesinh_session(override_session=None):
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
    if not nv_list:
        return []

    gh2_staff = [name for name in nv_list if "GH2" in name.upper()]
    cleaning_nv = [name for name in nv_list if "GH2" not in name.upper()]
    
    total_cleaning = len(cleaning_nv)
    
    erp_staff = [name for name in cleaning_nv if "ERP" in name.upper()]
    male_staff = [name for name in cleaning_nv if "*" not in name and name not in erp_staff]
    other_staff = [name for name in cleaning_nv if name not in erp_staff and name not in male_staff]

    assignments = []

    if total_cleaning < 6:
        erp_assigned = False
        if erp_staff:
            for p in erp_staff:
                assignments.append({
                    'name': p,
                    'zones': [1, 5],
                    'zone_desc': f"Khu 1: {ZONES[1]} & Khu 5: {ZONES[5]}"
                })
            erp_assigned = True
        
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
        
        remaining = [p for p in cleaning_nv if p not in erp_staff and p not in assigned_males]
        for p in remaining:
            desc = f"Khu 3: {ZONES[3]}"
            if not erp_assigned:
                desc += f" & Khu 1: {ZONES[1]} & Khu 5: {ZONES[5]}"
                erp_assigned = True
            assignments.append({'name': p, 'zones': [3], 'zone_desc': desc})
            
    else:
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

    from meal_handler import get_vietnamese_day_of_week
    day_str = get_vietnamese_day_of_week()
    records = get_cached_records(WORKSHEET_SCHEDULES_NAME, ttl=300)
    today_sched = next((row for row in records if row.get('day_of_week') == day_str), None)

    if not nv_list and today_sched:
        try:
            nv_raw = today_sched.get('employee_schedule', '')
            pg_raw = today_sched.get('pg_schedule', '')
            nv_list = parse_staff_from_raw(nv_raw, target_shift_name)
            pg_list = parse_staff_from_raw(pg_raw, target_shift_name)
        except Exception as err:
            print(f"Lỗi parse fallback lịch vệ sinh: {err}")

    pg_kho_list = []
    if session_type == 'vesinh_sang' and today_sched:
        try:
            pg_raw = today_sched.get('pg_schedule', '')
            pg_kho_list = parse_staff_from_raw(pg_raw, "Vệ Sinh Kho")
        except Exception as err:
            print(f"Lỗi đọc vệ sinh kho: {err}")

    if session_type == 'vesinh_chieu':
        try:
            if day_str in ["Thứ Bảy", "Chủ Nhật"]:
                nv_list = []
            else:
                morning_staff = get_working_staff('ansang')
                morning_nvs = morning_staff.get('NV', []) if isinstance(morning_staff, dict) else []
                if not morning_nvs and today_sched:
                    nv_raw = today_sched.get('employee_schedule', '')
                    morning_nvs = parse_staff_from_raw(nv_raw, "Ca Sáng")
                
                morning_nvs_norm = [normalize_text(name) for name in morning_nvs]
                nv_list = [nv for nv in nv_list if normalize_text(nv) not in morning_nvs_norm]
        except Exception as e:
            print(f"Lỗi lọc nhân viên vệ sinh: {e}")

    return {'NV': nv_list, 'PG': pg_list, 'PG_KHO': pg_kho_list}

def sync_vesinh_sheet(group_id, session_type):
    """
    Đồng bộ dữ liệu vệ sinh sử dụng RAM Cache + Ghi Google Sheets chạy ngầm.
    """
    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
    
    all_values = get_cached_values(WORKSHEET_VESINH_TRACKER_NAME, ttl=60, create_headers=VESINH_HEADERS)
    if not all_values:
        all_values = [list(VESINH_HEADERS)]

    # Dọn dẹp dữ liệu ngày cũ, chỉ giữ lại header và các dòng của ngày hôm nay
    today_rows = [r for r in all_values[1:] if len(r) > 1 and str(r[1]).strip() == today_str]
    need_clear_sheet = False
    if len(all_values) > 1 and len(today_rows) < (len(all_values) - 1):
        print(f"Ngày mới! Dọn dẹp dữ liệu vệ sinh cũ...")
        all_values = [list(VESINH_HEADERS)] + today_rows
        set_cached_values(WORKSHEET_VESINH_TRACKER_NAME, all_values)
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

    staff_lists = get_working_staff_vesinh(session_type)
    raw_nv_list = staff_lists.get('NV', [])
    raw_pg_list = staff_lists.get('PG', [])
    raw_pg_kho_list = staff_lists.get('PG_KHO', [])

    seen_pg_kho = set()
    pg_kho_list = []
    for name in raw_pg_kho_list:
        norm = normalize_text(name)
        if norm not in seen_pg_kho:
            seen_pg_kho.add(norm)
            pg_kho_list.append(name)

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

    for assign in nv_assignments:
        name = assign['name']
        zone_desc = assign['zone_desc']
        norm_name = normalize_text(name)
        person_key = normalize_person_key(name)
        if norm_name in processed_names or person_key in processed_names:
            continue
        processed_names.add(norm_name)
        processed_names.add(person_key)
        
        existing = existing_entries.get(norm_name) or existing_entries.get(person_key)
        if existing:
            item = dict(existing)
            item['zone'] = zone_desc
            final_data.append(item)
        else:
            entry = {
                'group_id': group_id, 'date': today_str, 'session': session_type,
                'type': 'NV', 'name': name, 'zone': zone_desc, 'status': 'waiting',
                'time_clicked': '', 'clicked_by': ''
            }
            new_row = [str(group_id), today_str, session_type, 'NV', name, zone_desc, 'waiting', '', '']
            new_rows.append(new_row)
            all_values.append(new_row)
            existing_entries[norm_name] = entry
            if person_key: existing_entries[person_key] = entry
            final_data.append(entry)

    for name in pg_kho_list:
        norm_name = normalize_text(name)
        person_key = normalize_person_key(name)
        if norm_name in processed_names or person_key in processed_names:
            continue
        processed_names.add(norm_name)
        processed_names.add(person_key)
        zone_desc = "Vệ sinh kho"
        existing = existing_entries.get(norm_name) or existing_entries.get(person_key)
        if existing:
            final_data.append(existing)
        else:
            entry = {
                'group_id': group_id, 'date': today_str, 'session': session_type,
                'type': 'PG_KHO', 'name': name, 'zone': zone_desc, 'status': 'waiting',
                'time_clicked': '', 'clicked_by': ''
            }
            new_row = [str(group_id), today_str, session_type, 'PG_KHO', name, zone_desc, 'waiting', '', '']
            new_rows.append(new_row)
            all_values.append(new_row)
            existing_entries[norm_name] = entry
            if person_key: existing_entries[person_key] = entry
            final_data.append(entry)

    for name in pg_list:
        norm_name = normalize_text(name)
        person_key = normalize_person_key(name)
        if norm_name in processed_names or person_key in processed_names:
            continue
        processed_names.add(norm_name)
        processed_names.add(person_key)
        zone_desc = "Vệ sinh gian hàng PG"
        existing = existing_entries.get(norm_name) or existing_entries.get(person_key)
        if existing:
            final_data.append(existing)
        else:
            entry = {
                'group_id': group_id, 'date': today_str, 'session': session_type,
                'type': 'PG', 'name': name, 'zone': zone_desc, 'status': 'waiting',
                'time_clicked': '', 'clicked_by': ''
            }
            new_row = [str(group_id), today_str, session_type, 'PG', name, zone_desc, 'waiting', '', '']
            new_rows.append(new_row)
            all_values.append(new_row)
            existing_entries[norm_name] = entry
            if person_key: existing_entries[person_key] = entry
            final_data.append(entry)

    if need_clear_sheet or new_rows:
        set_cached_values(WORKSHEET_VESINH_TRACKER_NAME, all_values)
        rows_snapshot = [list(r) for r in new_rows]
        def _bg_sync_vesinh(do_clear, to_append):
            ws = get_worksheet_cached_obj(WORKSHEET_VESINH_TRACKER_NAME, create_headers=VESINH_HEADERS, rows=100, cols=10)
            if not ws:
                return
            if do_clear:
                ws.clear()
                ws.append_row(VESINH_HEADERS)
            if to_append:
                ws.append_rows(to_append, value_input_option='USER_ENTERED')
        run_bg_sheet_task(_bg_sync_vesinh, need_clear_sheet, rows_snapshot, desc="sync_vesinh_sheet")

    return final_data

def update_vesinh_status(group_id, session_type, staff_name, clicker_name, target_status='done'):
    """
    Cập nhật trạng thái hoàn thành vệ sinh tức thì trong RAM Cache và đẩy ghi Sheet chạy ngầm.
    Tự động đồng bộ nếu ca/ngày hôm nay chưa có dòng trong bảng, so khớp thông minh bằng match_staff_name
    và tự động bổ sung dòng hoàn tất cứu hộ nếu dữ liệu lịch bị thiếu sót.
    """
    try:
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
        time_now = datetime.now(tz_vietnam).strftime('%H:%M') if target_status == 'done' else ''

        if target_status == 'waiting':
            # Bấm 1 lần xong là chốt luôn, khóa vĩnh viễn không hoàn lại thao tác
            return "already", None

        all_values = get_cached_values(WORKSHEET_VESINH_TRACKER_NAME, ttl=300, create_headers=VESINH_HEADERS)
        
        # 1. Tự động kiểm tra xem ngày hôm nay và ca này đã được khởi tạo trong cache chưa
        has_session_today = False
        if all_values and len(all_values) > 1:
            for r in all_values[1:]:
                if len(r) > 2 and str(r[1]).strip() == today_str and str(r[2]).strip() == session_type:
                    has_session_today = True
                    break

        if not has_session_today:
            sync_vesinh_sheet(group_id, session_type)
            all_values = get_cached_values(WORKSHEET_VESINH_TRACKER_NAME, ttl=300, create_headers=VESINH_HEADERS)
            if not all_values:
                all_values = [list(VESINH_HEADERS)]

        matching_rows = []
        already_done = True
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
                        matching_rows.append((i, row))
                        if len(row) < 7 or row[6].strip() != target_status:
                            already_done = False

        # Bước 2: Khớp hôm nay + đúng ca + tên khớp (bỏ qua group_id nếu bấm chéo nhóm hoặc từ Web App)
        if not matching_rows:
            for i, row in enumerate(all_values[1:], start=2):
                if len(row) < 5: continue
                r_date = str(row[1]).strip()
                r_session = str(row[2]).strip()
                r_name = str(row[4]).strip()
                if r_date == today_str and r_session == session_type:
                    if match_staff_name(r_name, staff_name):
                        matching_rows.append((i, row))
                        if len(row) < 7 or row[6].strip() != target_status:
                            already_done = False

        # Bước 3: Khớp hôm nay + tên khớp (bất kỳ ca nào)
        if not matching_rows:
            for i, row in enumerate(all_values[1:], start=2):
                if len(row) < 5: continue
                r_date = str(row[1]).strip()
                r_name = str(row[4]).strip()
                if r_date == today_str and match_staff_name(r_name, staff_name):
                    matching_rows.append((i, row))
                    if len(row) < 7 or row[6].strip() != target_status:
                        already_done = False

        if matching_rows and already_done:
            return "already", None

        clicked_user = clicker_name if target_status == 'done' else ''

        if matching_rows:
            row_indices_for_bg = []
            for row_index, _ in matching_rows:
                mem_row = all_values[row_index - 1]
                while len(mem_row) < 9:
                    mem_row.append('')
                mem_row[6] = target_status
                mem_row[7] = time_now
                mem_row[8] = clicked_user
                row_indices_for_bg.append(row_index)

            set_cached_values(WORKSHEET_VESINH_TRACKER_NAME, all_values)

            def _bg_update_vesinh(indices, st, tm, usr):
                ws = get_worksheet_cached_obj(WORKSHEET_VESINH_TRACKER_NAME, create_headers=VESINH_HEADERS, rows=100, cols=10)
                if not ws:
                    return
                cells = []
                for r_idx in indices:
                    cells.extend([
                        gspread.Cell(r_idx, 7, st),
                        gspread.Cell(r_idx, 8, tm),
                        gspread.Cell(r_idx, 9, usr)
                    ])
                ws.update_cells(cells)
            run_bg_sheet_task(_bg_update_vesinh, row_indices_for_bg, target_status, time_now, clicked_user, desc="update_vesinh_status")
            return True, time_now

        # Bước 4: Fallback cứu hộ an toàn tuyệt đối (Không bao giờ báo lỗi cho người dùng)
        new_row = [
            str(group_id or 'default'),
            today_str,
            session_type,
            'NV',
            staff_name,
            'Vệ sinh bổ sung',
            target_status,
            time_now,
            clicked_user
        ]
        all_values.append(new_row)
        set_cached_values(WORKSHEET_VESINH_TRACKER_NAME, all_values)

        def _bg_append_vesinh(to_append):
            ws = get_worksheet_cached_obj(WORKSHEET_VESINH_TRACKER_NAME, create_headers=VESINH_HEADERS, rows=100, cols=10)
            if ws:
                ws.append_rows([to_append], value_input_option='USER_ENTERED')
        run_bg_sheet_task(_bg_append_vesinh, new_row, desc=f"fallback_add_vesinh_{staff_name}")
        return True, time_now

    except Exception as e:
        print(f"Lỗi update vesinh status: {e}")
        return False, None

def generate_vesinh_flex(group_id, session_type=None):
    """
    Tạo Flex Message giao diện Phân Công & Theo Dõi Vệ Sinh.
    Tối ưu siêu tốc độ: Lấy trực tiếp từ RAM Cache khi đã có sẵn dữ liệu hôm nay.
    """
    if not session_type:
        session_type = get_current_vesinh_session()

    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')

    # Tối ưu siêu tốc độ phản hồi: Lấy trực tiếp từ RAM Cache (0ms)
    all_values = get_cached_values(WORKSHEET_VESINH_TRACKER_NAME, ttl=300, create_headers=VESINH_HEADERS)
    data = []
    if all_values and len(all_values) > 1:
        all_records = values_to_records(all_values)
        data = [r for r in all_records if str(r.get('date')).strip() == today_str and str(r.get('session')).strip() == session_type]

    if not data:
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

    if nv_data:
        body_contents.append({
            "type": "text",
            "text": f"🧹 PHÂN CÔNG VỆ SINH NV ({len(nv_data)} người)",
            "weight": "bold",
            "size": "xs",
            "color": "#0f766e",
            "margin": "xs"
        })

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
                    if is_done:
                        staff_row["contents"].append({
                            "type": "box",
                            "layout": "vertical",
                            "flex": 4,
                            "backgroundColor": "#DCFCE7",
                            "borderColor": "#86EFAC",
                            "borderWidth": "1px",
                            "cornerRadius": "sm",
                            "paddingAll": "xs",
                            "alignItems": "center",
                            "justifyContent": "center",
                            "contents": [
                                {
                                    "type": "text",
                                    "text": "✓ Đã xong",
                                    "weight": "bold",
                                    "size": "xs",
                                    "color": "#15803D"
                                }
                            ]
                        })
                    else:
                        staff_row["contents"].append({
                            "type": "button",
                            "action": {
                                "type": "postback",
                                "label": "Hoàn tất",
                                "data": f"action=complete_vesinh&session={session_type}&name={name}&target_status=done"
                            },
                            "style": "primary",
                            "color": "#0284c7",
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
                            "type": "box" if is_done else "button",
                            **({
                                "layout": "vertical",
                                "flex": 4,
                                "backgroundColor": "#DCFCE7",
                                "borderColor": "#86EFAC",
                                "borderWidth": "1px",
                                "cornerRadius": "sm",
                                "paddingAll": "xs",
                                "alignItems": "center",
                                "justifyContent": "center",
                                "contents": [
                                    {
                                        "type": "text",
                                        "text": "✓ Đã xong",
                                        "weight": "bold",
                                        "size": "xs",
                                        "color": "#15803D"
                                    }
                                ]
                            } if is_done else {
                                "action": {
                                    "type": "postback",
                                    "label": "Hoàn tất",
                                    "data": f"action=complete_vesinh&session={session_type}&name={name}&target_status=done"
                                },
                                "style": "primary",
                                "color": "#0284c7",
                                "size": "xs",
                                "height": "sm",
                                "flex": 4
                            })
                        }
                    ]
                })

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
                            "type": "box" if is_done else "button",
                            **({
                                "layout": "vertical",
                                "flex": 4,
                                "backgroundColor": "#DCFCE7",
                                "borderColor": "#86EFAC",
                                "borderWidth": "1px",
                                "cornerRadius": "sm",
                                "paddingAll": "xs",
                                "alignItems": "center",
                                "justifyContent": "center",
                                "contents": [
                                    {
                                        "type": "text",
                                        "text": "✓ Đã xong",
                                        "weight": "bold",
                                        "size": "xs",
                                        "color": "#15803D"
                                    }
                                ]
                            } if is_done else {
                                "action": {
                                    "type": "postback",
                                    "label": "Hoàn tất",
                                    "data": f"action=complete_vesinh&session={session_type}&name={name}&target_status=done"
                                },
                                "style": "primary",
                                "color": "#0284c7",
                                "size": "xs",
                                "height": "sm",
                                "flex": 4
                            })
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
