import os
import re
import json
import time
import copy
import tempfile
import threading
import queue
from datetime import datetime
import pytz
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --- PHẦN CẤU HÌNH GOOGLE SHEETS DÙNG CHUNG ---

GOOGLE_CREDS_JSON = os.environ.get('GOOGLE_CREDENTIALS_JSON')
if not GOOGLE_CREDS_JSON:
    raise ValueError("Lỗi: Biến môi trường GOOGLE_CREDENTIALS_JSON không được thiết lập.")

SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
google_creds_dict = json.loads(GOOGLE_CREDS_JSON)
CREDS = ServiceAccountCredentials.from_json_keyfile_dict(google_creds_dict, SCOPE)
CLIENT = gspread.authorize(CREDS)

_SPREADSHEET = None
_SPREADSHEET_LOCK = threading.RLock()

def get_spreadsheet():
    global _SPREADSHEET
    with _SPREADSHEET_LOCK:
        if _SPREADSHEET is None:
            _SPREADSHEET = CLIENT.open(SHEET_NAME)
        return _SPREADSHEET

# Tên các trang tính dùng chung
SHEET_NAME = 'DATA REATIME'
WORKSHEET_NAME_USERS = 'allowed_users'
WORKSHEET_TRACKER_NAME = 'task_tracker'
WORKSHEET_NAME = 'chi_tiet_cum'
WORKSHEET_SCHEDULES_NAME = 'schedules'
WORKSHEET_MEAL_TRACKER_NAME = 'meal_tracker'
WORKSHEET_ADHOC_TASKS = 'adhoc_tasks'
WORKSHEET_VESINH_TRACKER_NAME = 'vesinh_tracker'

# Nhóm duy nhất được phép gộp Checklist ca và Công việc giao thêm
MAIN_CHECKLIST_GROUP_ID = (
    os.environ.get('MAIN_CHECKLIST_GROUP_ID')
    or os.environ.get('CHECKLIST_GROUP_ID')
    or 'C37e48216804398593d8c79fe3edacdc7'
)

# URL của LINE LIFF hoặc Web App nội bộ trên Render
def get_liff_url():
    liff_env = (os.environ.get('LIFF_URL') or '').strip()
    if liff_env:
        return liff_env
    render_url = (os.environ.get('RENDER_EXTERNAL_URL') or '').strip()
    if render_url:
        return render_url.rstrip('/') + '/liff'
    return ''


# ============================================================================
# HÀM TIỆN ÍCH SO KHỚP NGÀY ĐA ĐỊNH DẠNG (CHỐNG LỖI AUTO-FORMAT CỦA GOOGLE SHEETS)
# ============================================================================

def get_today_str():
    """Trả về chuỗi ngày hôm nay theo múi giờ Việt Nam (YYYY-MM-DD)."""
    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    return datetime.now(tz_vietnam).strftime('%Y-%m-%d')


def is_date_today(date_str, today_str=None):
    """
    So khớp ngày an toàn, chấp nhận mọi định dạng YYYY-MM-DD, DD/MM/YYYY, D/M/YYYY
    hoặc MM/DD/YYYY khi Google Sheets tự động format cột ngày.
    """
    d = str(date_str or '').strip()
    if not d:
        return False
    t = str(today_str or get_today_str()).strip()
    if d == t:
        return True
    try:
        parts_d = [int(p) for p in re.split(r'[-/.]', d.split()[0]) if p.isdigit()]
        parts_t = [int(p) for p in re.split(r'[-/.]', t.split()[0]) if p.isdigit()]
        if len(parts_d) == 3 and len(parts_t) == 3:
            return sorted(parts_d) == sorted(parts_t)
    except Exception:
        pass
    return False


# ============================================================================
# HỆ THỐNG BỘ NHỚ ĐỆM RAM + ĐỒNG BỘ ĐA TIẾN TRÌNH (/tmp IPC) & KHÓA MỘT CHIỀU
# Giúp phản hồi nút bấm tức thì (~0.1s), đồng bộ xuyên suốt các Gunicorn workers
# và đảm bảo mục đã Hoàn tất (done/complete) KHÔNG BAO GIỜ bị nhảy ngược về Chờ.
# ============================================================================

_CACHE_LOCK = threading.RLock()
_RAM_SHEET_CACHE = {}  # {worksheet_name: {'values': [[...]], 'ts': float, 'dirty_until': float, 'mtime': float}}
_WORKSHEET_OBJ_CACHE = {}  # {worksheet_name: gspread.Worksheet}
_IPC_DIR = os.path.join(tempfile.gettempdir(), 'dmx_savico_ipc_v2')
try:
    os.makedirs(_IPC_DIR, exist_ok=True)
except Exception:
    pass

_COMPLETED_LOCK_MEM = {'date': '', 'items': {}}  # {'date': 'YYYY-MM-DD', 'items': {ws_name: {item_key: dict}}}

_TRACKER_MONOTONIC_SPECS = {
    WORKSHEET_TRACKER_NAME: {
        'date_col': 1,
        'status_col': 5,
        'done_val': 'complete',
        'min_len': 7,
        'extra_cols': [6],
        'key_fn': lambda r: str(r[2]).strip().lower() if len(r) > 2 else ''
    },
    WORKSHEET_MEAL_TRACKER_NAME: {
        'date_col': 1,
        'status_col': 5,
        'done_val': 'done',
        'min_len': 8,
        'extra_cols': [6, 7],
        'key_fn': lambda r: f"{str(r[2]).strip().lower()}|{re.sub(r'\\s+', ' ', str(r[4]).strip().lower())}" if len(r) > 4 else ''
    },
    WORKSHEET_VESINH_TRACKER_NAME: {
        'date_col': 1,
        'status_col': 6,
        'done_val': 'done',
        'min_len': 9,
        'extra_cols': [7, 8],
        'key_fn': lambda r: f"{str(r[2]).strip().lower()}|{re.sub(r'\\s+', ' ', str(r[4]).strip().lower())}" if len(r) > 4 else ''
    },
    WORKSHEET_ADHOC_TASKS: {
        'date_col': 1,
        'status_col': 5,
        'done_val': 'complete',
        'min_len': 9,
        'extra_cols': [6, 7],
        'key_fn': lambda r: str(r[3]).strip().lower() if len(r) > 3 else ''
    }
}


def _ipc_cache_path(worksheet_name):
    safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', str(worksheet_name))
    return os.path.join(_IPC_DIR, f"sheet_{safe_name}.json")


def _ipc_lock_path():
    return os.path.join(_IPC_DIR, "completed_lock.json")


def _load_shared_completed_lock(today_str):
    """Đọc và hợp nhất danh sách các mục đã hoàn tất trong ngày hôm nay giữa RAM và đĩa (/tmp)."""
    global _COMPLETED_LOCK_MEM
    if _COMPLETED_LOCK_MEM.get('date') != today_str:
        _COMPLETED_LOCK_MEM = {'date': today_str, 'items': {}}

    lock_file = _ipc_lock_path()
    try:
        if os.path.exists(lock_file):
            with open(lock_file, 'r', encoding='utf-8') as f:
                disk_data = json.load(f)
            if isinstance(disk_data, dict) and disk_data.get('date') == today_str:
                disk_items = disk_data.get('items', {})
                for ws_name, ws_dict in disk_items.items():
                    if isinstance(ws_dict, dict):
                        target = _COMPLETED_LOCK_MEM['items'].setdefault(ws_name, {})
                        target.update(ws_dict)
    except Exception:
        pass
    return _COMPLETED_LOCK_MEM['items']


def _save_shared_completed_lock(today_str):
    """Lưu danh sách khóa hoàn tất của ngày hôm nay xuống /tmp để mọi worker đều nhận biết."""
    lock_file = _ipc_lock_path()
    tmp_file = f"{lock_file}.{os.getpid()}.{threading.get_ident()}.tmp"
    try:
        payload = {'date': today_str, 'items': _COMPLETED_LOCK_MEM.get('items', {})}
        with open(tmp_file, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False)
        os.replace(tmp_file, lock_file)
    except Exception:
        try:
            if os.path.exists(tmp_file):
                os.remove(tmp_file)
        except Exception:
            pass


def _sync_and_apply_monotonic_lock(worksheet_name, values):
    """
    1. Chuẩn hóa cột ngày của các dòng hôm nay về đúng định dạng YYYY-MM-DD.
    2. Ghi nhận mọi mục đã hoàn tất (done / complete) vào bộ khóa một chiều của ngày hôm nay.
    3. Áp khóa ngược lại vào bảng dữ liệu để KHÔNG BAO GIỜ một mục đã hoàn tất bị lùi về waiting/incomplete.
    """
    if not values or len(values) <= 1:
        return values
    spec = _TRACKER_MONOTONIC_SPECS.get(worksheet_name)
    if not spec:
        return [list(r) for r in values]

    today_str = get_today_str()
    date_col = spec['date_col']
    status_col = spec['status_col']
    done_val = spec['done_val']
    min_len = spec['min_len']
    extra_cols = spec['extra_cols']
    key_fn = spec['key_fn']

    with _CACHE_LOCK:
        all_locks = _load_shared_completed_lock(today_str)
        ws_locks = all_locks.setdefault(worksheet_name, {})
        lock_changed = False

        out_rows = [list(values[0])]
        for raw_row in values[1:]:
            if not any(str(c).strip() for c in raw_row):
                continue
            row = list(raw_row)
            if len(row) > date_col and is_date_today(row[date_col], today_str):
                # Chuẩn hóa định dạng ngày về YYYY-MM-DD để không bao giờ lệch chuỗi ngày
                row[date_col] = today_str
                item_key = key_fn(row)
                if item_key:
                    curr_st = str(row[status_col]).strip() if len(row) > status_col else ''
                    if curr_st == done_val:
                        extras = {
                            str(c_idx): (str(row[c_idx]).strip() if len(row) > c_idx else '')
                            for c_idx in extra_cols
                        }
                        prev = ws_locks.get(item_key)
                        if prev != extras:
                            ws_locks[item_key] = extras
                            lock_changed = True
                    elif item_key in ws_locks:
                        # Khôi phục trạng thái hoàn tất nếu dòng này bị ghi đè bởi dữ liệu cũ
                        while len(row) < min_len:
                            row.append('')
                        row[status_col] = done_val
                        saved_extras = ws_locks[item_key]
                        for c_idx in extra_cols:
                            val = saved_extras.get(str(c_idx), '')
                            if val and not str(row[c_idx]).strip():
                                row[c_idx] = val
            out_rows.append(row)

        if lock_changed:
            _save_shared_completed_lock(today_str)

        return out_rows


def _merge_missing_today_rows(worksheet_name, prev_values, fresh_values):
    """
    Khi làm mới từ Google Sheets, nếu có dòng của ngày hôm nay vừa được tạo trong RAM/IPC
    nhưng Google Sheets chưa kịp trả về (hoặc mạng trễ), tự động giữ lại dòng đó.
    """
    spec = _TRACKER_MONOTONIC_SPECS.get(worksheet_name)
    if not spec or not prev_values or len(prev_values) <= 1:
        return fresh_values

    today_str = get_today_str()
    date_col = spec['date_col']
    key_fn = spec['key_fn']

    if not fresh_values:
        return [list(r) for r in prev_values]

    fresh_keys = set()
    for r in fresh_values[1:]:
        if len(r) > date_col and is_date_today(r[date_col], today_str):
            k = key_fn(r)
            if k:
                fresh_keys.add(k)

    merged = [list(r) for r in fresh_values]
    for r in prev_values[1:]:
        if len(r) > date_col and is_date_today(r[date_col], today_str):
            k = key_fn(r)
            if k and k not in fresh_keys:
                row_copy = list(r)
                row_copy[date_col] = today_str
                merged.append(row_copy)
                fresh_keys.add(k)

    return merged


def _read_ipc_sheet_cache(worksheet_name):
    """Đọc cache dùng chung giữa các tiến trình Gunicorn từ /tmp."""
    path = _ipc_cache_path(worksheet_name)
    try:
        if not os.path.exists(path):
            return None
        mtime = os.path.getmtime(path)
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, dict) and 'values' in data:
            data['mtime'] = mtime
            return data
    except Exception:
        pass
    return None


def _write_ipc_sheet_cache(worksheet_name, entry):
    """Ghi nguyên tử (atomic write) cache dùng chung xuống /tmp."""
    path = _ipc_cache_path(worksheet_name)
    tmp_path = f"{path}.{os.getpid()}.{threading.get_ident()}.tmp"
    try:
        payload = {
            'values': entry['values'],
            'ts': entry['ts'],
            'dirty_until': entry.get('dirty_until', 0.0)
        }
        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False)
        os.replace(tmp_path, path)
        entry['mtime'] = os.path.getmtime(path)
    except Exception:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


# Hàng đợi ghi Google Sheets tuần tự chạy ngầm
_BG_QUEUE = queue.Queue()

def _bg_worker_loop():
    while True:
        item = _BG_QUEUE.get()
        if item is None:
            break
        fn, args, kwargs, desc = item
        try:
            fn(*args, **kwargs)
        except Exception as e:
            print(f"[BG-SYNC ERROR] ({desc}): {e}")
        finally:
            _BG_QUEUE.task_done()

_BG_THREAD = threading.Thread(target=_bg_worker_loop, daemon=True)
_BG_THREAD.start()


def run_bg_sheet_task(fn, *args, desc="sheet_write", **kwargs):
    """Đẩy một tác vụ ghi Google Sheets vào hàng đợi chạy ngầm để không chặn phản hồi LINE."""
    _BG_QUEUE.put((fn, args, kwargs, desc))


def sync_table_to_sheet_bg(worksheet_name, create_headers=None, clear_tail=False):
    """
    Đồng bộ nguyên bảng dữ liệu mới nhất từ bộ nhớ đệm lên Google Sheets ở chế độ chạy ngầm.
    - Ghi nguyên khối từ ô A1 bằng value_input_option='RAW' (không bao giờ làm trắng bảng như ws.clear(),
      không bao giờ ghi lệch dòng row_index, và không bị Google Sheets tự đổi định dạng ngày).
    """
    def _do_sync():
        ws = get_worksheet_cached_obj(worksheet_name, create_headers=create_headers)
        if not ws:
            return
        with _CACHE_LOCK:
            ipc_entry = _read_ipc_sheet_cache(worksheet_name)
            mem_entry = _RAM_SHEET_CACHE.get(worksheet_name)
            if ipc_entry and (not mem_entry or ipc_entry.get('ts', 0) >= mem_entry.get('ts', 0)):
                raw_vals = ipc_entry['values']
            elif mem_entry:
                raw_vals = mem_entry['values']
            else:
                return
            clean_vals = _sync_and_apply_monotonic_lock(worksheet_name, raw_vals)

        if not clean_vals:
            return

        num_cols = max((len(r) for r in clean_vals), default=len(create_headers or []) or 9)
        normalized_rows = []
        for r in clean_vals:
            row_copy = [str(c) if c is not None else '' for c in r]
            while len(row_copy) < num_cols:
                row_copy.append('')
            normalized_rows.append(row_copy)

        # Đệm thêm các dòng trống phía dưới để xóa sạch dữ liệu thừa từ ngày cũ trong đúng 1 lệnh gọi API
        pad_count = 45 if clear_tail else 15
        padded_rows = normalized_rows + [[''] * num_cols for _ in range(pad_count)]

        try:
            ws.update(range_name='A1', values=padded_rows, value_input_option='RAW')
        except TypeError:
            ws.update('A1', padded_rows, value_input_option='RAW')

        if clear_tail:
            try:
                ws.batch_clear([f'A{len(padded_rows) + 1}:Z500'])
            except Exception:
                pass

    run_bg_sheet_task(_do_sync, desc=f"sync_table_{worksheet_name}")


def get_worksheet_cached_obj(worksheet_name, create_headers=None, rows="1000", cols="20"):
    """Lấy đối tượng Worksheet có cache tránh gọi metadata lặp lại."""
    with _CACHE_LOCK:
        ws = _WORKSHEET_OBJ_CACHE.get(worksheet_name)
        if ws is not None:
            return ws
    try:
        ss = get_spreadsheet()
        try:
            ws = ss.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            if create_headers:
                ws = ss.add_worksheet(title=worksheet_name, rows=str(rows), cols=str(cols))
                ws.append_row(create_headers, value_input_option='RAW')
                with _CACHE_LOCK:
                    entry = {
                        'values': [list(create_headers)],
                        'ts': time.time(),
                        'dirty_until': 0.0,
                        'mtime': 0.0
                    }
                    _RAM_SHEET_CACHE[worksheet_name] = entry
                    _write_ipc_sheet_cache(worksheet_name, entry)
            else:
                raise
        with _CACHE_LOCK:
            _WORKSHEET_OBJ_CACHE[worksheet_name] = ws
        return ws
    except Exception as e:
        print(f"Lỗi lấy worksheet '{worksheet_name}': {e}")
        return None


def get_cached_values(worksheet_name, ttl=300, force_refresh=False, create_headers=None):
    """
    Đọc toàn bộ dữ liệu dạng mảng 2 chiều (get_all_values) từ RAM + IPC Cache.
    Tự động đồng bộ giữa các Gunicorn workers và bảo vệ trạng thái đã hoàn tất (Monotonic Lock).
    """
    now = time.time()
    with _CACHE_LOCK:
        entry = _RAM_SHEET_CACHE.get(worksheet_name)
        ipc_path = _ipc_cache_path(worksheet_name)
        try:
            disk_mtime = os.path.getmtime(ipc_path) if os.path.exists(ipc_path) else 0.0
        except Exception:
            disk_mtime = 0.0

        # Nếu có worker khác vừa cập nhật file IPC trên đĩa, nạp ngay vào RAM
        if disk_mtime > 0 and (entry is None or disk_mtime > entry.get('mtime', 0.0)):
            ipc_entry = _read_ipc_sheet_cache(worksheet_name)
            if ipc_entry is not None:
                entry = ipc_entry
                _RAM_SHEET_CACHE[worksheet_name] = entry

        if entry is not None and not force_refresh:
            if now < entry.get('dirty_until', 0.0) or (now - entry['ts']) < ttl:
                locked_vals = _sync_and_apply_monotonic_lock(worksheet_name, entry['values'])
                entry['values'] = locked_vals
                return [list(r) for r in locked_vals]

    ws = get_worksheet_cached_obj(worksheet_name, create_headers=create_headers)
    if not ws:
        with _CACHE_LOCK:
            if entry is not None:
                locked_vals = _sync_and_apply_monotonic_lock(worksheet_name, entry['values'])
                return [list(r) for r in locked_vals]
        return []

    try:
        raw_fresh = ws.get_all_values()
        # Loại bỏ các dòng đệm trống hoàn toàn (trừ dòng header đầu tiên)
        if raw_fresh:
            fresh_vals = [list(raw_fresh[0])] + [
                list(r) for r in raw_fresh[1:] if any(str(c).strip() for c in r)
            ]
        else:
            fresh_vals = [list(create_headers)] if create_headers else []

        with _CACHE_LOCK:
            # Kiểm tra lại xem trong lúc tải mạng có worker nào vừa ghi dữ liệu mới hơn không
            ipc_entry = _read_ipc_sheet_cache(worksheet_name)
            cur = _RAM_SHEET_CACHE.get(worksheet_name)
            if ipc_entry and (not cur or ipc_entry.get('ts', 0) >= cur.get('ts', 0)):
                cur = ipc_entry

            if cur is not None and time.time() < cur.get('dirty_until', 0.0) and not force_refresh:
                locked_vals = _sync_and_apply_monotonic_lock(worksheet_name, cur['values'])
                cur['values'] = locked_vals
                _RAM_SHEET_CACHE[worksheet_name] = cur
                return [list(r) for r in locked_vals]

            prev_vals = cur['values'] if cur is not None else None
            merged_vals = _merge_missing_today_rows(worksheet_name, prev_vals, fresh_vals)
            locked_vals = _sync_and_apply_monotonic_lock(worksheet_name, merged_vals)

            new_entry = {
                'values': [list(r) for r in locked_vals],
                'ts': time.time(),
                'dirty_until': 0.0,
                'mtime': 0.0
            }
            _write_ipc_sheet_cache(worksheet_name, new_entry)
            _RAM_SHEET_CACHE[worksheet_name] = new_entry
            return [list(r) for r in locked_vals]
    except Exception as e:
        print(f"Lỗi đọc get_all_values cho '{worksheet_name}': {e}")
        with _CACHE_LOCK:
            if entry is not None:
                locked_vals = _sync_and_apply_monotonic_lock(worksheet_name, entry['values'])
                return [list(r) for r in locked_vals]
        return []


def set_cached_values(worksheet_name, new_values, dirty_hold_seconds=30.0):
    """
    Cập nhật ngay lập tức dữ liệu của worksheet trong RAM và file IPC dùng chung (/tmp).
    Tự động khóa một chiều mọi mục đã hoàn tất (done/complete) của ngày hôm nay.
    """
    now = time.time()
    with _CACHE_LOCK:
        locked_vals = _sync_and_apply_monotonic_lock(worksheet_name, new_values)
        entry = {
            'values': [list(r) for r in locked_vals],
            'ts': now,
            'dirty_until': now + dirty_hold_seconds,
            'mtime': 0.0
        }
        _write_ipc_sheet_cache(worksheet_name, entry)
        _RAM_SHEET_CACHE[worksheet_name] = entry


def invalidate_cache(worksheet_name):
    """Xóa cache của một worksheet khi cần làm mới hoàn toàn."""
    with _CACHE_LOCK:
        _RAM_SHEET_CACHE.pop(worksheet_name, None)
        path = _ipc_cache_path(worksheet_name)
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception:
            pass


def values_to_records(all_values):
    """Chuyển đổi mảng 2 chiều (get_all_values) sang danh sách dict giống hệt get_all_records()."""
    if not all_values or len(all_values) <= 1:
        return []
    headers = [str(h).strip() for h in all_values[0]]
    records = []
    for row in all_values[1:]:
        if not any(str(c).strip() for c in row):
            continue
        rec = {}
        for idx, h in enumerate(headers):
            if not h:
                continue
            rec[h] = row[idx] if idx < len(row) else ''
        records.append(rec)
    return records


def get_cached_records(worksheet_name, ttl=60, force_refresh=False, create_headers=None):
    """Đọc danh sách dict (get_all_records) trực tiếp từ RAM Cache."""
    vals = get_cached_values(worksheet_name, ttl=ttl, force_refresh=force_refresh, create_headers=create_headers)
    return values_to_records(vals)