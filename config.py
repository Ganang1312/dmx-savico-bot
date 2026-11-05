import os
import json
import time
import copy
import threading
import queue
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
# HỆ THỐNG BỘ NHỚ ĐỆM RAM (WRITE-THROUGH CACHE) & GHI GOOGLE SHEETS CHẠY NGẦM
# Giúp phản hồi nút bấm tức thì (~0.1-0.2s) và chống lỗi Quota 429 của Google
# ============================================================================

_CACHE_LOCK = threading.RLock()
_RAM_SHEET_CACHE = {}  # {worksheet_name: {'values': [[...]], 'ts': float, 'dirty_until': float}}
_WORKSHEET_OBJ_CACHE = {}  # {worksheet_name: gspread.Worksheet}

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
                ws.append_row(create_headers)
                with _CACHE_LOCK:
                    _RAM_SHEET_CACHE[worksheet_name] = {
                        'values': [list(create_headers)],
                        'ts': time.time(),
                        'dirty_until': 0.0
                    }
            else:
                raise
        with _CACHE_LOCK:
            _WORKSHEET_OBJ_CACHE[worksheet_name] = ws
        return ws
    except Exception as e:
        print(f"Lỗi lấy worksheet '{worksheet_name}': {e}")
        return None


def get_cached_values(worksheet_name, ttl=60, force_refresh=False, create_headers=None):
    """
    Đọc toàn bộ dữ liệu dạng mảng 2 chiều (get_all_values) từ RAM Cache.
    Nếu đang trong thời gian chờ ghi ngầm (dirty_until > now), luôn ưu tiên bản RAM mới nhất.
    """
    now = time.time()
    with _CACHE_LOCK:
        entry = _RAM_SHEET_CACHE.get(worksheet_name)
        if entry is not None and not force_refresh:
            if now < entry.get('dirty_until', 0.0) or (now - entry['ts']) < ttl:
                return [list(r) for r in entry['values']]

    ws = get_worksheet_cached_obj(worksheet_name, create_headers=create_headers)
    if not ws:
        with _CACHE_LOCK:
            if entry is not None:
                return [list(r) for r in entry['values']]
        return []

    try:
        fresh_vals = ws.get_all_values()
        with _CACHE_LOCK:
            # Nếu trong lúc đang tải mạng mà có luồng khác vừa cập nhật RAM (dirty_until > now), giữ bản RAM
            cur = _RAM_SHEET_CACHE.get(worksheet_name)
            if cur is not None and time.time() < cur.get('dirty_until', 0.0) and not force_refresh:
                return [list(r) for r in cur['values']]
            _RAM_SHEET_CACHE[worksheet_name] = {
                'values': [list(r) for r in fresh_vals],
                'ts': time.time(),
                'dirty_until': 0.0
            }
        return [list(r) for r in fresh_vals]
    except Exception as e:
        print(f"Lỗi đọc get_all_values cho '{worksheet_name}': {e}")
        with _CACHE_LOCK:
            if entry is not None:
                return [list(r) for r in entry['values']]
        return []


def set_cached_values(worksheet_name, new_values, dirty_hold_seconds=15.0):
    """
    Cập nhật ngay lập tức dữ liệu của worksheet trong RAM.
    dirty_hold_seconds giữ cho bản RAM không bị ghi đè bởi dữ liệu cũ trên Sheet trong lúc luồng ngầm đang ghi.
    """
    now = time.time()
    with _CACHE_LOCK:
        _RAM_SHEET_CACHE[worksheet_name] = {
            'values': [list(r) for r in new_values],
            'ts': now,
            'dirty_until': now + dirty_hold_seconds
        }


def invalidate_cache(worksheet_name):
    """Xóa cache của một worksheet khi cần làm mới hoàn toàn."""
    with _CACHE_LOCK:
        _RAM_SHEET_CACHE.pop(worksheet_name, None)


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