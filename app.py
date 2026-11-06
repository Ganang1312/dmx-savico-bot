import os
import json
import collections
import math
import threading
import time
import urllib.parse
import requests
from datetime import datetime, date
import pytz
from dateutil.relativedelta import relativedelta
import re

from flask import Flask, request, abort, jsonify, Response
from linebot import (
    LineBotApi, WebhookHandler
)
from linebot.exceptions import InvalidSignatureError
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage, FlexSendMessage, PostbackEvent
)

# --- IMPORT ---
from config import (
    CLIENT, SHEET_NAME, WORKSHEET_NAME_USERS, WORKSHEET_NAME,
    WORKSHEET_TRACKER_NAME, WORKSHEET_SCHEDULES_NAME,
    get_spreadsheet, MAIN_CHECKLIST_GROUP_ID,
    get_cached_values, get_cached_records, set_cached_values,
    run_bg_sheet_task, get_liff_url
)
from schedule_handler import send_daily_schedule
from flex_handler import (
    TASKS,
    initialize_daily_tasks, generate_checklist_flex, get_tasks_status_from_sheet,
    update_shift_task_status, get_adhoc_tasks_for_group_today,
    add_adhoc_tasks, generate_adhoc_flex, update_adhoc_task_status,
    add_all_adhoc_tasks, generate_all_adhoc_flex, register_group_member,
    add_multi_adhoc_tasks, generate_multi_adhoc_flex,
    find_latest_task_token, cancel_task_group,
    generate_combined_work_flex, generate_today_adhoc_flex,
    send_initial_checklist, get_checklist_message,
    is_work_command, parse_work_command, plan_assignments, resolve_mentions
)
from meal_handler import (
    generate_meal_flex, update_meal_status, get_task_roster,
    sync_meal_sheet, get_vietnamese_day_of_week
)
from vesinh_handler import (
    generate_vesinh_flex, update_vesinh_status,
    get_current_vesinh_session, sync_vesinh_sheet
)
from dmx_flex_messages import (
    trigger_adhoc_scrape, check_scrape_status,
    build_luyke_flex, build_nhanvien_flex,
    build_help_commands_flex, build_realtime_messages,
    build_realtime_staff_detail_messages,
    build_custom_groups_messages,
    LINE_MAX_MESSAGES
)
from coupon_handler import (
    build_category_menu_flex, build_product_list_flex,
    claim_coupon_for_user, import_coupons_from_text,
    build_claimed_coupon_flex, clear_all_coupons
)

# --- CẤU HÌNH ---
CHANNEL_ACCESS_TOKEN = os.environ.get('CHANNEL_ACCESS_TOKEN')
CHANNEL_SECRET = os.environ.get('CHANNEL_SECRET')
ADMIN_USER_ID = os.environ.get('ADMIN_USER_ID')
CRON_SECRET_KEY = os.environ.get('CRON_SECRET_KEY')

if not all([CHANNEL_ACCESS_TOKEN, CHANNEL_SECRET, ADMIN_USER_ID]):
    print("Cảnh báo: Thiếu biến môi trường quan trọng.")

allowed_ids_cache = set()
app = Flask(__name__)

line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
handler = WebhookHandler(CHANNEL_SECRET)


# ============================================================================
# BỘ ĐỆM TÊN HIỂN THỊ NGƯỜI DÙNG (TRÁNH GỌI LINE PROFILE API LẶP LẠI MỖI LẦN BẤM)
# ============================================================================
_USER_PROFILE_CACHE = {}  # {(group_id_or_empty, user_id): (display_name, ts)}

def get_cached_user_display_name(group_id, user_id, fallback="Nhân viên"):
    if not user_id:
        return fallback
    now = time.time()
    user_key = str(user_id)
    # 1. Kiểm tra cache tức thì theo user_id (0ms)
    cached_user = _USER_PROFILE_CACHE.get(user_key)
    if cached_user and (now - cached_user[1]) < 86400:
        return cached_user[0]

    gid = str(group_id or '').strip()
    group_key = (gid, user_key)
    cached = _USER_PROFILE_CACHE.get(group_key)
    if cached and (now - cached[1]) < 86400:
        return cached[0]

    try:
        if gid and (gid.startswith('C') or gid.startswith('R')):
            profile = line_bot_api.get_group_member_profile(gid, user_id)
        else:
            profile = line_bot_api.get_profile(user_id)
        if profile and profile.display_name:
            _USER_PROFILE_CACHE[group_key] = (profile.display_name, now)
            _USER_PROFILE_CACHE[user_key] = (profile.display_name, now)
            return profile.display_name
    except Exception:
        pass
    return fallback


# ============================================================================
# NHÓM 1 (MỤC 1, 2, 4): CƠ CHẾ AUTO-TRIGGER ĐẦU CA BẰNG REPLY_TOKEN (TỐN 0 PUSH)
# ============================================================================
_SHIFT_BROADCAST_LOCK = threading.RLock()
_SHIFT_BROADCAST_STATE = {
    'date': '',
    'sang': set(),   # Tập hợp các group_id đã phát thông báo Ca Sáng hôm nay
    'chieu': set(),  # Tập hợp các group_id đã phát thông báo Ca Chiều hôm nay
}


def _sync_broadcast_date():
    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
    with _SHIFT_BROADCAST_LOCK:
        if _SHIFT_BROADCAST_STATE['date'] != today_str:
            _SHIFT_BROADCAST_STATE['date'] = today_str
            _SHIFT_BROADCAST_STATE['sang'] = set()
            _SHIFT_BROADCAST_STATE['chieu'] = set()
    return today_str


def mark_shift_broadcast_sent(group_id, shift_type):
    if not group_id or shift_type not in ('sang', 'chieu'):
        return
    _sync_broadcast_date()
    with _SHIFT_BROADCAST_LOCK:
        _SHIFT_BROADCAST_STATE[shift_type].add(str(group_id).strip())


def is_shift_broadcast_sent(group_id, shift_type):
    if not group_id or shift_type not in ('sang', 'chieu'):
        return False
    _sync_broadcast_date()
    with _SHIFT_BROADCAST_LOCK:
        return str(group_id).strip() in _SHIFT_BROADCAST_STATE[shift_type]


def build_shift_broadcast_messages(group_id, shift_type):
    """
    Tạo danh sách tin nhắn mở ca cho nhóm:
      - Ca Sáng ('sang'):
          + Nếu là nhóm PG riêng: [Lịch PG]
          + Nếu là nhóm NV / nhóm chính: [Lịch PG (nếu gộp), Lịch NV, Checklist Ca Sáng]
      - Ca Chiều ('chieu') (Mục 4):
          + KHÔNG gửi lặp lại Lịch NV & Lịch PG (vì đã gửi từ sáng).
          + Chỉ gửi [Checklist Ca Chiều].
    """
    gid = str(group_id or '').strip()
    if not gid:
        return []

    pg_group_id = (os.environ.get('PG_GROUP_ID') or '').strip()
    employee_group_id = (os.environ.get('EMPLOYEE_GROUP_ID') or MAIN_CHECKLIST_GROUP_ID or '').strip()

    msgs = []
    if shift_type == 'sang':
        if pg_group_id and gid == pg_group_id and pg_group_id != employee_group_id:
            msg_pg = send_daily_schedule('pg', return_msg_only=True)
            if msg_pg:
                msgs.append(msg_pg)
        else:
            if not pg_group_id or pg_group_id == employee_group_id or gid == str(MAIN_CHECKLIST_GROUP_ID).strip():
                msg_pg = send_daily_schedule('pg', return_msg_only=True)
                if msg_pg:
                    msgs.append(msg_pg)
            msg_nv = send_daily_schedule('employee', return_msg_only=True)
            if msg_nv:
                msgs.append(msg_nv)
            msg_check = get_checklist_message('sang', gid)
            if msg_check:
                msgs.append(msg_check)
    elif shift_type == 'chieu':
        # Mục 4: Buổi chiều chỉ gửi Checklist Kho Ca Chiều, cắt bỏ gửi lặp Lịch NV & PG
        if pg_group_id and gid == pg_group_id and pg_group_id != employee_group_id:
            return []
        msg_check = get_checklist_message('chieu', gid)
        if msg_check:
            msgs.append(msg_check)

    return msgs


def get_pending_auto_shift_messages(group_id):
    """
    Kiểm tra xem nhóm `group_id` hiện tại có gói thông báo đầu ca nào chưa phát không.
    Khung giờ tự động:
      - Ca Sáng: 07:00 -> 13:59
      - Ca Chiều: 14:00 -> 21:59
    """
    gid = str(group_id or '').strip()
    if not gid:
        return [], None

    pg_group_id = (os.environ.get('PG_GROUP_ID') or '').strip()
    employee_group_id = (os.environ.get('EMPLOYEE_GROUP_ID') or '').strip()
    main_gid = str(MAIN_CHECKLIST_GROUP_ID or '').strip()

    target_groups = {g for g in (pg_group_id, employee_group_id, main_gid) if g}
    if target_groups and gid not in target_groups:
        return [], None

    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    hour = datetime.now(tz_vietnam).hour

    if 7 <= hour < 14:
        shift_type = 'sang'
    elif 14 <= hour < 22:
        shift_type = 'chieu'
    else:
        return [], None

    if is_shift_broadcast_sent(gid, shift_type):
        return [], None

    msgs = build_shift_broadcast_messages(gid, shift_type)
    return msgs, shift_type


def reply_with_auto_shift(reply_token, primary_messages, group_id=None):
    """
    Gửi reply_message an toàn cho LINE.
    Không tự ý đính kèm Lịch làm việc PG / Nhân viên khi người dùng đang giao việc hoặc thao tác.
    """
    if primary_messages is None:
        msg_list = []
    elif isinstance(primary_messages, list):
        msg_list = list(primary_messages)
    else:
        msg_list = [primary_messages]

    if not msg_list:
        return
    try:
        line_bot_api.reply_message(reply_token, msg_list[:LINE_MAX_MESSAGES])
    except Exception as e:
        print(f"Lỗi gửi reply_message: {e}")


def build_scrape_pending_flex(scrape_type_val, req_time_str=""):
    """
    Mục 3: Thẻ Flex thông báo đang đồng bộ dữ liệu BI kèm nút Postback [🔄 Xem báo cáo vừa cào]
    giúp lấy kết quả hoàn toàn bằng reply_token miễn phí, KHÔNG tốn push_message.
    """
    label_type = "LŨY KẾ THÁNG" if scrape_type_val == "luyke" else "REALTIME NGÀY"
    return {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#0284C7",
            "paddingAll": "md",
            "contents": [
                {
                    "type": "text",
                    "text": f"⏳ ĐANG ĐỒNG BỘ BÁO CÁO {label_type}",
                    "weight": "bold",
                    "size": "sm",
                    "color": "#FFFFFF"
                }
            ]
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "paddingAll": "md",
            "spacing": "sm",
            "contents": [
                {
                    "type": "text",
                    "text": "Hệ thống đang cào dữ liệu mới nhất từ Portal BI.",
                    "size": "xs",
                    "weight": "bold",
                    "color": "#0F172A",
                    "wrap": True
                },
                {
                    "type": "text",
                    "text": "👉 Vui lòng đợi khoảng 10–15 giây rồi bấm nút bên dưới để hiển thị báo cáo (cơ chế mới tiết kiệm 100% tin nhắn Push cho nhóm).",
                    "size": "xxs",
                    "color": "#475569",
                    "wrap": True
                }
            ]
        },
        "footer": {
            "type": "box",
            "layout": "vertical",
            "paddingAll": "sm",
            "contents": [
                {
                    "type": "button",
                    "action": {
                        "type": "postback",
                        "label": f"🔄 Xem báo cáo {label_type}",
                        "data": f"action=view_scraped_report&type={scrape_type_val}&req={req_time_str[:16]}"
                    },
                    "style": "primary",
                    "color": "#16A34A",
                    "height": "sm"
                }
            ]
        }
    }


def resolve_effective_liff_url(group_id=None):
    base_url = get_liff_url()
    if not base_url:
        try:
            host = request.url_root.rstrip('/')
            if host.startswith('http://') and 'localhost' not in host and '127.0.0.1' not in host:
                host = 'https://' + host[len('http://'):]
            base_url = host + '/liff'
        except Exception:
            base_url = ''
    if base_url and group_id:
        sep = '&' if '?' in base_url else '?'
        return f"{base_url}{sep}group_id={group_id}"
    return base_url


# --- UTILS ---
def load_allowed_ids():
    global allowed_ids_cache
    try:
        records = get_cached_records(WORKSHEET_NAME_USERS, ttl=120, force_refresh=True)
        new_allowed_ids = set()
        today = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')).date()
        for record in records:
            user_id = record.get('id')
            exp_date_str = record.get('expiration_date')
            if not user_id or not exp_date_str: continue
            try:
                exp_date = datetime.strptime(exp_date_str, '%Y-%m-%d').date()
                if exp_date >= today: new_allowed_ids.add(str(user_id))
            except ValueError: continue
        allowed_ids_cache = new_allowed_ids
    except Exception as e:
        print(f"Lỗi tải danh sách ID: {e}")
        allowed_ids_cache = set()

def keep_alive():
    ping_url = os.environ.get("RENDER_EXTERNAL_URL")
    if not ping_url: return
    while True:
        try: requests.get(ping_url.rstrip('/') + "/ping", timeout=10)
        except: pass
        time.sleep(300)

def update_expiration_in_sheet(target_id, expiration_date_str):
    sheet = get_spreadsheet().worksheet(WORKSHEET_NAME_USERS)
    all_ids = sheet.col_values(1)
    try:
        row_to_update = all_ids.index(target_id) + 1
        sheet.update_cell(row_to_update, 2, expiration_date_str)
        return "Cập nhật"
    except ValueError:
        sheet.append_row([target_id, expiration_date_str])
        return "Thêm mới"

def parse_duration(duration_str):
    if duration_str == '0': return relativedelta(years=999), "vĩnh viễn"
    match = re.match(r"^(\d+)([dm])$", duration_str.lower())
    if not match: return None, None
    value = int(match.group(1))
    unit = match.group(2)
    if unit == 'd': return relativedelta(days=value), f"{value} ngày"
    if unit == 'm': return relativedelta(months=value), f"{value} tháng"
    return None, None

# --- REPORT UTILS ---
def parse_float_from_string(s):
    if s is None: return 0.0
    if not isinstance(s, str): s = str(s)
    clean_s = s.strip()
    if not clean_s or clean_s == '-': return 0.0
    try: return float(clean_s.replace(',', '.'))
    except ValueError: return 0.0

def handle_percentage_string(percent_str):
    if not percent_str: return 0.0, "0%"
    clean_str = str(percent_str).strip().replace(',', '.')
    if '%' in clean_str:
        try:
            value = float(clean_str.replace('%', '')) / 100
            return value, f"{round(value * 100)}%"
        except: return 0.0, "0%"
    else:
        try:
            value = float(clean_str)
            return value, f"{round(value * 100)}%"
        except: return 0.0, "0%"

def calculate_ranking(all_data, current_row):
    try:
        current_channel = (current_row[1] or "").strip()
        channel_stores = []
        for row in all_data[1:]:
            if len(row) > 4 and (row[1] or "").strip() == current_channel:
                try:
                    revenue = parse_float_from_string(row[4])
                    channel_stores.append({'revenue': revenue, 'full_row': row})
                except (ValueError, TypeError): continue
        channel_stores.sort(key=lambda x: x['revenue'], reverse=True)
        rank = -1
        for i, store in enumerate(channel_stores):
            if store['full_row'] == current_row:
                rank = i + 1; break
        if rank != -1: return f"{rank}/{len(channel_stores)}"
        return "-/-"
    except: return "-/-"

def parse_competition_data(header_row, data_row):
    start_column_index = 6
    category_indices = collections.defaultdict(list)
    for i, header in enumerate(header_row[start_column_index:], start=start_column_index):
        if header: category_indices[header].append(i)
    results = []
    for category_name, indices in category_indices.items():
        if len(indices) == 3:
            try:
                percent_ht_val = data_row[indices[0]]
                realtime_val_str = data_row[indices[1]] if data_row[indices[1]] and data_row[indices[1]].strip() != '-' else "0"
                target_val_str = data_row[indices[2]] if data_row[indices[2]] and data_row[indices[2]].strip() != '-' else "0"
                percent_float, percent_ht_formatted = handle_percentage_string(percent_ht_val)
                results.append({"name": category_name, "realtime": parse_float_from_string(realtime_val_str), "target": target_val_str, "percent_ht": percent_ht_formatted, "percent_val": percent_float})
            except: continue
    results.sort(key=lambda x: x['percent_val'], reverse=True)
    return results

def format_currency(value_str, remove_decimal=False):
    if not value_str or str(value_str).strip() == '-': return "-"
    try:
        value = parse_float_from_string(value_str)
        if remove_decimal:
            if value >= 1000: return f"{math.floor(value / 1000)} Tỷ"
            return f"{math.floor(value)} Tr"
        else:
            if value >= 1000: return f"{round(value / 1000, 2)} Tỷ"
            return f"{round(value, 2)} Tr"
    except: return "-"

def create_flex_message(store_data, competition_results, ranking):
    cum = store_data[0] or "-"
    kenh = (store_data[1] or "").strip()
    sieu_thi_full = store_data[2] or "Không có tên"
    ten_sieu_thi_parts = sieu_thi_full.split(' - ')
    short_name_part = ten_sieu_thi_parts[-1] if len(ten_sieu_thi_parts) > 1 else sieu_thi_full
    ten_sieu_thi_rut_gon = f"{kenh} {short_name_part}"
    realtime_tong = format_currency(store_data[4], remove_decimal=True)
    target_tong = format_currency(store_data[3], remove_decimal=True)
    percent_float, percent_ht_tong = handle_percentage_string(store_data[5])
    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    now = datetime.now(tz_vietnam)
    thoi_gian = f"{now.hour}h Ngày {now.day}/{now.month}"
    sold_items = [item for item in competition_results if item['realtime'] > 0]
    unsold_items = [item for item in competition_results if item['realtime'] == 0]
    nh_thi_dua_dat = sum(1 for item in sold_items if item.get("percent_val", 0) >= 1)
    channel_styles = {"ĐML": {"bg": "#1E88E5", "text": "#FFFFFF"}, "ĐMM": {"bg": "#43A047", "text": "#FFFFFF"},"TGD": {"bg": "#FDD835", "text": "#000000"}, "AAR": {"bg": "#212121", "text": "#FFFFFF"}}
    style = channel_styles.get(kenh, {"bg": "#006c83", "text": "#FFFFFF"})
    TEXT_COLOR = "#000000"
    SEPARATOR_COLOR = "#EEEEEE"
    sold_components = []
    for item in sold_items:
        percent_val = item.get("percent_val", 0)
        color = "#00B33C" if percent_val >= 1 else ("#FFC400" if percent_val > 0.7 else "#FF3B30")
        target_display = str(parse_float_from_string(item["target"]))
        component = {"type": "box", "layout": "horizontal", "margin": "md", "paddingTop": "sm", "paddingBottom": "sm", "contents": [{"type": "text", "text": item["name"], "wrap": True, "size": "xs", "color": TEXT_COLOR, "flex": 4, "gravity": "center"},{"type": "separator", "color": SEPARATOR_COLOR},{"type": "text", "text": str(round(item["realtime"], 2)), "size": "xs", "color": TEXT_COLOR, "align": "center", "flex": 2, "gravity": "center"},{"type": "separator", "color": SEPARATOR_COLOR},{"type": "text", "text": target_display, "size": "xs", "color": TEXT_COLOR, "align": "center", "flex": 2, "gravity": "center"},{"type": "separator", "color": SEPARATOR_COLOR},{"type": "box", "layout": "vertical", "flex": 2, "contents": [{"type": "text", "text": item["percent_ht"], "size": "xs", "color": color, "align": "end", "weight": "bold", "gravity": "center"}]}]}
        sold_components.append(component)
        sold_components.append({"type": "separator", "margin": "md", "color": SEPARATOR_COLOR})
    unsold_components = []
    if unsold_items:
        unsold_components.extend([{"type": "separator", "margin": "xl", "color": SEPARATOR_COLOR}, {"type": "text", "text": "NGÀNH HÀNG CHƯA CÓ SỐ:", "color": "#888888", "size": "xs", "align": "center", "margin": "lg", "weight": "bold"}])
        num_columns = 3
        columns = [[] for _ in range(num_columns)]
        for i, item in enumerate(unsold_items):
            columns[i % num_columns].append({"type": "text", "text": f"• {item['name']}", "color": TEXT_COLOR, "size": "xxs", "margin": "sm", "wrap": True})
        column_boxes = [{"type": "box", "layout": "vertical", "flex": 1, "contents": col} for col in columns]
        unsold_components.append({"type": "box", "layout": "horizontal", "margin": "md", "spacing": "md", "contents": column_boxes})
    percent_color = "#00B33C" if percent_float >= 1 else ("#FFC400" if percent_float > 0.7 else "#FF3B30")
    flex_json = {"type": "flex", "altText": f"Báo cáo cho {ten_sieu_thi_rut_gon}", "contents": { "type": "bubble", "size": "mega", "header": { "type": "box", "layout": "vertical", "paddingAll": "20px", "backgroundColor": style["bg"], "contents": [ {"type": "text", "text": "Báo cáo Realtime", "color": style["text"], "size": "md", "align": "center", "weight": "bold"}, {"type": "text", "text": f"🏪 {ten_sieu_thi_rut_gon.upper()}", "color": style["text"], "weight": "bold", "size": "lg", "align": "center", "margin": "md", "wrap": True}, {"type": "box", "layout": "vertical", "margin": "lg", "spacing": "sm", "contents": [ {"type": "text", "text": f"⭐ Cụm: {cum}", "size": "xs", "color": style["text"]}, {"type": "text", "text": f"🕒 Thời gian: {thoi_gian}", "size": "xs", "color": style["text"]}, {"type": "text", "text": f"🏆 NH thi đua đạt: {nh_thi_dua_dat}", "size": "xs", "color": style["text"]} ]} ] }, "body": { "type": "box", "layout": "vertical", "paddingAll": "20px", "backgroundColor": "#FFFFFF", "contents": [ {"type": "box", "layout": "horizontal", "contents": [ {"type": "box", "layout": "vertical", "flex": 1, "spacing": "sm", "contents": [ {"type": "text", "text": "💰 DOANH THU", "color": "#007BFF", "size": "sm", "align": "center", "weight":"bold"}, {"type": "text", "text": realtime_tong, "color": "#007BFF", "size": "xl", "weight": "bold", "align": "center"} ]}, {"type": "box", "layout": "vertical", "flex": 1, "spacing": "sm", "contents": [ {"type": "text", "text": "🎯 TARGET", "color": "#DC3545", "size": "sm", "align": "center", "weight":"bold"}, {"type": "text", "text": target_tong, "color": "#DC3545", "size": "xl", "weight": "bold", "align": "center"} ]} ]}, {"type": "text", "text": "% HOÀN THÀNH", "color": TEXT_COLOR, "size": "sm", "align": "center", "margin": "xl"}, {"type": "text", "text": percent_ht_tong, "color": percent_color, "size": "xxl", "weight": "bold", "align": "center"}, {"type": "box", "layout": "vertical", "backgroundColor": "#DDDDDD", "height": "8px", "cornerRadius": "md", "margin": "md", "contents": [ {"type": "box", "layout": "vertical", "backgroundColor": percent_color, "height": "8px", "cornerRadius": "md", "width": f"{min(100, round(percent_float * 100))}%"} ]}, {"type": "box", "layout": "horizontal", "margin": "xl", "contents": [{"type": "text", "text": "XH D.Thu Kênh", "size": "xs", "color": TEXT_COLOR, "align": "center", "flex": 1}]}, {"type": "box", "layout": "horizontal", "contents": [{"type": "text", "text": ranking, "weight": "bold", "size": "md", "color": TEXT_COLOR, "align": "center", "flex": 1}]}, {"type": "separator", "margin": "xl", "color": SEPARATOR_COLOR}, {"type": "box", "layout": "horizontal", "margin": "md", "contents": [{"type": "text", "text": "Ngành Hàng", "color": "#555555", "size": "xs", "flex": 4, "weight": "bold", "align": "center"}, {"type": "text", "text": "Realtime", "color": "#555555", "size": "xs", "flex": 2, "align": "center", "weight": "bold"}, {"type": "text", "text": "Target", "color": "#555555", "size": "xs", "flex": 2, "align": "center", "weight": "bold"}, {"type": "text", "text": "%HT", "color": "#555555", "size": "xs", "flex": 2, "align": "end", "weight": "bold"}]}, {"type": "separator", "margin": "md", "color": SEPARATOR_COLOR}, *sold_components, *unsold_components ] } }}
    return flex_json

def create_summary_text_message(store_data, competition_results):
    try:
        target_val = parse_float_from_string(store_data[3])
        realtime_val = parse_float_from_string(store_data[4])
        percent_float, _ = handle_percentage_string(store_data[5])
        remaining_val = target_val - realtime_val
        finished_items_count = sum(1 for item in competition_results if item['percent_val'] >= 1)
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        now = datetime.now(tz_vietnam)
        time_str = now.strftime("%H:%M:%S")
        summary = f"📊 BÁO CÁO NHANH REAL-TIME - {time_str} 📊\n"
        summary += "-------------------\n"
        summary += f"- 🎯 Target Ngày: {math.floor(target_val)}\n"
        summary += f"- 📈 Realtime: {math.floor(realtime_val)} ({round(percent_float*100)}%)\n"
        summary += f"- 📉 Còn lại: {math.floor(remaining_val)}\n"
        summary += f"- 🏆 NH thi đua đạt: {finished_items_count}/{len(competition_results)}\n"
        summary += "-------------------\n"
        summary += "🏁 THI ĐUA NH:\n\n"
        sold_items = [item for item in competition_results if item.get('realtime', 0) > 0]
        if sold_items:
            for item in sold_items:
                try:
                    realtime = item.get('realtime', 0)
                    target = parse_float_from_string(item.get('target', '0'))
                    remaining = target - realtime
                    percent_ht = item.get('percent_ht', '0%')
                    realtime_disp = math.floor(realtime) if realtime == math.floor(realtime) else round(realtime, 2)
                    target_disp = math.floor(target) if target == math.floor(target) else round(target, 2)
                    remaining_disp = math.floor(remaining) if remaining == math.floor(remaining) else round(remaining, 2)
                    summary += f"• {item['name']}: {realtime_disp}/{target_disp} ({percent_ht}) còn lại: {remaining_disp}\n"
                except (ValueError, TypeError):
                    summary += f"• {item['name']}: {item.get('realtime', 0)} ({item.get('percent_ht', '0%')})\n"
        else:
            summary += "Chưa có ngành hàng thi đua nào phát sinh doanh số."
        return TextSendMessage(text=summary)
    except Exception as e:
        print(f"Lỗi khi tạo tin nhắn tóm tắt: {e}")
        return None

def create_leaderboard_flex_message(all_data, cluster_name=None, channel_filter=None):
    dmx_channels = ['ĐML', 'ĐMM', 'ĐMS']; tgdd_channels = ['TGD', 'AAR']
    dmx_stores, tgdd_stores = [], []
    data_to_process = all_data[1:]
    if cluster_name:
        data_to_process = [row for row in data_to_process if len(row) > 0 and row[0] and row[0].strip().upper() == cluster_name.strip().upper()]
    for row in data_to_process:
        try:
            kenh = (row[1] or "").strip()
            if not kenh: continue
            store_info = {'kenh': kenh, 'sieu_thi': row[2], 'doanh_thu': parse_float_from_string(row[4])}
            if kenh in dmx_channels: dmx_stores.append(store_info)
            elif kenh in tgdd_channels: tgdd_stores.append(store_info)
        except (IndexError, ValueError, TypeError): continue
    dmx_stores.sort(key=lambda x: x['doanh_thu'], reverse=True)
    tgdd_stores.sort(key=lambda x: x['doanh_thu'], reverse=True)
    if not cluster_name: dmx_stores, tgdd_stores = dmx_stores[:20], tgdd_stores[:20]
    
    def build_leaderboard_bubble(title, stores, header_bg_color, header_text_color):
        header = {"type": "box", "layout": "vertical", "backgroundColor": header_bg_color, "paddingAll": "lg", "contents": [{"type": "text", "text": title, "weight": "bold", "size": "lg", "color": header_text_color, "align": "center", "wrap": True}]}
        body_bg_color, text_color_body, separator_color = "#FFFFFF", "#000000", "#EEEEEE"
        table_header = {"type": "box", "layout": "horizontal", "margin": "md", "paddingAll": "sm", "backgroundColor": header_bg_color, "cornerRadius": "md", "contents": [{"type": "text", "text": "STT", "weight": "bold", "size": "xs", "color": header_text_color, "flex": 1, "align": "center", "gravity":"center"},{"type": "separator", "color": separator_color},{"type": "text", "text": "KÊNH", "weight": "bold", "size": "xs", "color": header_text_color, "flex": 2, "align": "center", "gravity":"center"},{"type": "separator", "color": separator_color},{"type": "text", "text": "SIÊU THỊ", "weight": "bold", "size": "xs", "color": header_text_color, "flex": 6, "align": "center", "gravity":"center"},{"type": "separator", "color": separator_color},{"type": "text", "text": "RT", "weight": "bold", "size": "xs", "color": header_text_color, "flex": 2, "align": "center", "gravity":"center"}]}
        rows = [table_header, {"type": "separator", "margin": "sm", "color": separator_color}]
        for i, store in enumerate(stores):
            name_parts = store['sieu_thi'].split(' - ', 1)
            short_name = name_parts[1] if len(name_parts) > 1 else store['sieu_thi']
            row_component = {
                "type": "box", 
                "layout": "horizontal", 
                "margin": "md", 
                "paddingTop":"sm", 
                "paddingBottom":"sm", 
                "contents": [
                    {"type": "text", "text": str(i+1), "size": "xs", "color": text_color_body, "flex": 1, "gravity": "center", "align": "center"},
                    {"type": "separator", "color": separator_color},
                    {"type": "text", "text": store['kenh'], "size": "xs", "color": text_color_body, "flex": 2, "gravity": "center", "align": "center"},
                    {"type": "separator", "color": separator_color},
                    {"type": "text", "text": short_name, "size": "xxs", "color": text_color_body, "flex": 6, "wrap": True, "gravity": "center", "align": "center"},
                    {"type": "separator", "color": separator_color},
                    {"type": "text", "text": str(round(store['doanh_thu'])), "size": "xs", "color": text_color_body, "flex": 2, "align": "center", "gravity": "center"}
                ]
            }
            rows.append(row_component)
            if i < len(stores) -1: rows.append({"type": "separator", "margin": "sm", "color": separator_color})
        return {"type": "bubble", "size": "mega", "header": header, "body": { "type": "box", "layout": "vertical", "contents": rows, "paddingAll":"lg", "backgroundColor": body_bg_color }}
        
    if cluster_name: dmx_title, tgdd_title = f"🏆 BXH CỤM {cluster_name.upper()} - ĐMX 🏆", f"🏆 BXH CỤM {cluster_name.upper()} - TGDD 🏆"
    else: dmx_title, tgdd_title = "🏆 REALTIME TOP 20 ĐMX 🏆", "🏆 REALTIME TOP 20 TGDD 🏆"
    
    messages_to_return = []
    show_dmx = False
    show_tgdd = False

    if channel_filter is None:
        show_dmx = True
        show_tgdd = True
    elif channel_filter == 'dmx':
        show_dmx = True
    elif channel_filter == 'tgdd':
        show_tgdd = True
    elif channel_filter in dmx_channels:
        show_dmx = True
    elif channel_filter in tgdd_channels:
        show_tgdd = True

    if show_dmx and dmx_stores:
        messages_to_return.append({ "type": "flex", "altText": dmx_title, "contents": build_leaderboard_bubble(dmx_title, dmx_stores, "#1E88E5", "#FFFFFF") })
    if show_tgdd and tgdd_stores:
        messages_to_return.append({ "type": "flex", "altText": tgdd_title, "contents": build_leaderboard_bubble(tgdd_title, tgdd_stores, "#FDD835", "#000000") })
        
    return messages_to_return

# --- KHỞI ĐỘNG CÁC TÁC VỤ NỀN ---
load_allowed_ids()
if 'RENDER' in os.environ:
    keep_alive_thread = threading.Thread(target=keep_alive, daemon=True)
    keep_alive_thread.start()

# --- ĐIỂM TIẾP NHẬN (ROUTES) ---

@app.route("/callback", methods=['POST'])
def callback():
    signature = request.headers['X-Line-Signature']
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return 'OK'

@app.route("/ping")
def ping():
    return "OK", 200


# ============================================================================
# NHÓM 4 (MỤC 15): ROUTES MINI WEB-APP / LINE LIFF TÍCH HỢP TRỰC TIẾP TRÊN FLASK
# ============================================================================

def _resolve_liff_group_id(req_gid=None):
    gid = str(req_gid or '').strip()
    if gid and gid not in ('null', 'undefined', 'None'):
        return gid
    return (
        os.environ.get('EMPLOYEE_GROUP_ID')
        or os.environ.get('MAIN_CHECKLIST_GROUP_ID')
        or MAIN_CHECKLIST_GROUP_ID
    )


@app.route('/api/liff/state', methods=['GET'])
def api_liff_state():
    """Trả về toàn bộ trạng thái công việc, nhân sự, vệ sinh, ăn uống từ RAM Cache."""
    try:
        tz = pytz.timezone('Asia/Ho_Chi_Minh')
        now = datetime.now(tz)
        group_id = _resolve_liff_group_id(request.args.get('group_id'))
        shift = request.args.get('shift') or ('sang' if now.hour < 15 else 'chieu')
        if shift not in ('sang', 'chieu', 'vs'):
            shift = 'sang'

        initialize_daily_tasks(group_id, shift)
        statuses = get_tasks_status_from_sheet(group_id, shift)
        shift_tasks = []
        for t in TASKS.get(shift, []):
            st_obj = statuses.get(t['id'], {})
            st_val = st_obj if isinstance(st_obj, str) else st_obj.get('status', 'incomplete')
            usr_val = '' if isinstance(st_obj, str) else st_obj.get('user_name', '')
            shift_tasks.append({
                'id': t['id'],
                'icon': t.get('icon', '📦'),
                'name': t['name'],
                'time': t['time'],
                'status': st_val,
                'user_name': usr_val
            })

        adhoc_tasks = get_adhoc_tasks_for_group_today(group_id)
        roster = get_task_roster()

        meal_session = 'ansang' if now.hour < 15 else 'anchieu'
        meal_list = sync_meal_sheet(group_id, meal_session)

        vesinh_session = get_current_vesinh_session()
        vesinh_list = sync_vesinh_sheet(group_id, vesinh_session)

        schedules = get_cached_records(WORKSHEET_SCHEDULES_NAME, ttl=300)
        today_day = get_vietnamese_day_of_week()
        today_sched = next((r for r in schedules if r.get('day_of_week') == today_day), {})

        return jsonify({
            'ok': True,
            'group_id': group_id,
            'now_str': now.strftime('%H:%M • %d/%m/%Y'),
            'day_of_week': today_day,
            'current_shift': shift,
            'shift_tasks': shift_tasks,
            'adhoc_tasks': adhoc_tasks,
            'roster': roster,
            'meal_session': meal_session,
            'meal_list': meal_list,
            'vesinh_session': vesinh_session,
            'vesinh_list': vesinh_list,
            'today_schedule': {
                'employee': today_sched.get('employee_schedule', ''),
                'pg': today_sched.get('pg_schedule', '')
            }
        })
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/api/liff/toggle-shift-task', methods=['POST'])
def api_toggle_shift_task():
    try:
        body = request.get_json(force=True) or {}
        group_id = _resolve_liff_group_id(body.get('group_id'))
        task_id = str(body.get('task_id') or '').strip()
        shift = str(body.get('shift') or 'sang').strip()
        target_status = str(body.get('target_status') or 'complete').strip()
        user_name = str(body.get('user_name') or 'Nhân viên').strip()
        if not task_id:
            return jsonify({'ok': False, 'error': 'Thiếu task_id'}), 400
        update_shift_task_status(group_id, task_id, shift, target_status, user_name)
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/api/liff/toggle-adhoc-task', methods=['POST'])
def api_toggle_adhoc_task():
    try:
        body = request.get_json(force=True) or {}
        group_id = _resolve_liff_group_id(body.get('group_id'))
        task_id = str(body.get('task_id') or '').strip()
        target_status = str(body.get('target_status') or 'complete').strip()
        user_name = str(body.get('user_name') or 'Nhân viên').strip()
        if not task_id:
            return jsonify({'ok': False, 'error': 'Thiếu task_id'}), 400
        ok, _, _ = update_adhoc_task_status(group_id, task_id, target_status, user_name)
        return jsonify({'ok': ok})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/api/liff/assign-task', methods=['POST'])
def api_assign_task():
    try:
        body = request.get_json(force=True) or {}
        group_id = _resolve_liff_group_id(body.get('group_id'))
        task_title = str(body.get('task_title') or '').strip()
        assignees = body.get('assignees') or []
        if not task_title:
            return jsonify({'ok': False, 'error': 'Vui lòng nhập nội dung công việc.'}), 400
        if not assignees or not isinstance(assignees, list):
            return jsonify({'ok': False, 'error': 'Vui lòng chọn ít nhất 1 nhân sự nhận việc.'}), 400

        clean_names = [str(n).strip() for n in assignees if str(n).strip()]
        lines = [l.lstrip('-*•+ ').strip() for l in task_title.split('\n') if l.strip()]
        if len(lines) == 1:
            token = add_all_adhoc_tasks(group_id, clean_names, lines[0])
        else:
            pairs = []
            for ln in lines:
                for nm in clean_names:
                    pairs.append((ln, nm))
            token = add_multi_adhoc_tasks(group_id, "", pairs)
        return jsonify({'ok': bool(token), 'token': token, 'count': len(clean_names) * len(lines)})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/api/liff/cancel-latest-task', methods=['POST'])
def api_cancel_latest_task():
    try:
        body = request.get_json(force=True) or {}
        group_id = _resolve_liff_group_id(body.get('group_id'))
        token, label, _ = find_latest_task_token(group_id)
        if not token:
            return jsonify({'ok': False, 'error': 'Hôm nay chưa có việc nào được giao để hủy.'})
        removed, names = cancel_task_group(group_id, token)
        return jsonify({'ok': removed > 0, 'removed': removed, 'token': token, 'names': names})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/api/liff/toggle-meal', methods=['POST'])
def api_toggle_meal():
    try:
        body = request.get_json(force=True) or {}
        group_id = _resolve_liff_group_id(body.get('group_id'))
        session_type = str(body.get('session') or 'ansang').strip()
        staff_name = str(body.get('name') or '').strip()
        clicker_name = str(body.get('user_name') or staff_name).strip()
        target_status = str(body.get('target_status') or 'done').strip()
        ok, _ = update_meal_status(group_id, session_type, staff_name, clicker_name, target_status)
        return jsonify({'ok': ok is True or ok == 'already'})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/api/liff/toggle-vesinh', methods=['POST'])
def api_toggle_vesinh():
    try:
        body = request.get_json(force=True) or {}
        group_id = _resolve_liff_group_id(body.get('group_id'))
        session_type = str(body.get('session') or 'vesinh_sang').strip()
        staff_name = str(body.get('name') or '').strip()
        clicker_name = str(body.get('user_name') or staff_name).strip()
        target_status = str(body.get('target_status') or 'done').strip()
        ok, _ = update_vesinh_status(group_id, session_type, staff_name, clicker_name, target_status)
        return jsonify({'ok': ok is True or ok == 'already'})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/liff', methods=['GET'])
def serve_liff_app():
    """Giao diện Mini Web-App (LINE LIFF) chuẩn Mobile cho ĐMX Savico."""
    liff_id = (os.environ.get('LIFF_ID') or '').strip()
    default_gid = _resolve_liff_group_id(request.args.get('group_id'))

    html = f"""<!DOCTYPE html>
<html lang="vi">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no" />
  <title>ĐMX Savico • Quản Lý & Giao Việc</title>
  <script src="https://static.line-scdn.net/liff/edge/2/sdk.js"></script>
  <style>
    :root {{
      --primary: #0284c7;
      --green: #16a34a;
      --green-bg: #f0fdf4;
      --green-border: #bbf7d0;
      --red: #dc2626;
      --bg: #f1f5f9;
      --card: #ffffff;
      --text: #0f172a;
      --muted: #64748b;
      --border: #e2e8f0;
    }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }}
    body {{ background: var(--bg); color: var(--text); padding-bottom: 80px; }}
    .topbar {{
      background: linear-gradient(135deg, #0284c7, #1e3a8a);
      color: #fff; padding: 14px 16px; position: sticky; top: 0; z-index: 50;
      box-shadow: 0 2px 10px rgba(0,0,0,0.12);
    }}
    .topbar-row {{ display: flex; justify-content: space-between; align-items: center; gap: 8px; }}
    .topbar h1 {{ font-size: 16px; font-weight: 800; }}
    .topbar-sub {{ font-size: 11px; color: #bae6fd; margin-top: 4px; display: flex; justify-content: space-between; }}
    .user-pill {{
      background: rgba(255,255,255,0.18); border: 1px solid rgba(255,255,255,0.3);
      color: #fff; border-radius: 999px; padding: 4px 10px; font-size: 12px; font-weight: 600;
    }}
    .tabs {{
      display: flex; background: #fff; border-bottom: 1px solid var(--border);
      position: sticky; top: 61px; z-index: 40;
    }}
    .tab-btn {{
      flex: 1; text-align: center; padding: 11px 4px; font-size: 12px; font-weight: 700;
      color: var(--muted); border: none; background: none; border-bottom: 3px solid transparent; cursor: pointer;
    }}
    .tab-btn.active {{ color: var(--primary); border-bottom-color: var(--primary); background: #f8fafc; }}
    .container {{ max-width: 600px; margin: 0 auto; padding: 12px; }}
    .card {{
      background: var(--card); border-radius: 12px; padding: 14px; margin-bottom: 12px;
      border: 1px solid var(--border); box-shadow: 0 1px 3px rgba(0,0,0,0.04);
    }}
    .card-title {{ font-size: 14px; font-weight: 800; margin-bottom: 10px; display: flex; justify-content: space-between; align-items: center; }}
    .badge {{
      font-size: 11px; font-weight: 700; padding: 3px 8px; border-radius: 999px;
      background: #e0f2fe; color: #0369a1;
    }}
    .badge.green {{ background: #dcfce7; color: #15803d; }}
    .progress-wrap {{ background: #e2e8f0; height: 8px; border-radius: 999px; overflow: hidden; margin: 8px 0 12px; }}
    .progress-bar {{ background: var(--green); height: 100%; border-radius: 999px; transition: width 0.25s ease; }}
    .task-item {{
      display: flex; align-items: center; justify-content: space-between; gap: 10px;
      padding: 10px 12px; border-radius: 10px; border: 1px solid var(--border);
      background: #f8fafc; margin-bottom: 8px;
    }}
    .task-item.done {{ background: var(--green-bg); border-color: var(--green-border); }}
    .task-info {{ flex: 1; }}
    .task-name {{ font-size: 13px; font-weight: 700; color: var(--text); }}
    .task-item.done .task-name {{ text-decoration: line-through; color: var(--muted); }}
    .task-meta {{ font-size: 11px; color: var(--red); font-weight: 600; margin-top: 2px; }}
    .task-done-by {{ font-size: 11px; color: var(--green); font-weight: 700; margin-top: 2px; }}
    .btn {{
      border: none; border-radius: 8px; padding: 8px 12px; font-size: 12px; font-weight: 700;
      cursor: pointer; white-space: nowrap;
    }}
    .btn-primary {{ background: var(--primary); color: #fff; }}
    .btn-muted {{ background: #94a3b8; color: #fff; }}
    .btn-danger {{ background: #fee2e2; color: #b91c1c; }}
    .shift-switch {{ display: flex; gap: 6px; margin-bottom: 10px; }}
    .shift-switch button {{
      flex: 1; padding: 7px; border-radius: 8px; border: 1px solid var(--border);
      background: #f8fafc; font-size: 12px; font-weight: 700; color: var(--muted); cursor: pointer;
    }}
    .shift-switch button.active {{ background: var(--primary); color: #fff; border-color: var(--primary); }}
    .grid-2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 6px; margin-top: 8px; }}
    .chip-btn {{
      display: flex; align-items: center; justify-content: space-between;
      padding: 7px 9px; border-radius: 8px; border: 1px solid #cbd5e1; background: #fff;
      font-size: 11px; font-weight: 700; color: var(--text); cursor: pointer; text-align: left;
    }}
    .chip-btn.done {{ background: #dcfce7; border-color: #86efac; color: #15803d; }}
    textarea, select {{
      width: 100%; border: 1px solid #cbd5e1; border-radius: 8px; padding: 9px 11px;
      font-size: 13px; margin-bottom: 8px; background: #fff;
    }}
    .group-pills {{ display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 10px; }}
    .pill {{
      padding: 5px 10px; border-radius: 999px; font-size: 11px; font-weight: 700;
      border: 1px solid #cbd5e1; background: #f8fafc; cursor: pointer;
    }}
    .roster-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 6px; max-height: 220px; overflow-y: auto; padding: 4px 0; }}
    .staff-chk {{
      display: flex; align-items: center; gap: 6px; padding: 6px 8px;
      border: 1px solid var(--border); border-radius: 8px; font-size: 11px; font-weight: 600; background: #f8fafc;
    }}
    .toast {{
      position: fixed; bottom: 18px; left: 50%; transform: translateX(-50%);
      background: #0f172a; color: #fff; padding: 9px 16px; border-radius: 999px;
      font-size: 12px; font-weight: 700; display: none; z-index: 100;
    }}
    pre.sched-box {{
      white-space: pre-wrap; font-size: 12px; line-height: 1.5; color: #334155;
      background: #f8fafc; padding: 10px; border-radius: 8px; border: 1px solid var(--border);
    }}
  </style>
</head>
<body>
  <div class="topbar">
    <div class="topbar-row">
      <h1>⚡ ĐMX SAVICO • MINI APP</h1>
      <select id="userSelect" class="user-pill" onchange="saveCurrentUser(this.value)">
        <option value="Nhân viên">👤 Chọn tên bạn...</option>
      </select>
    </div>
    <div class="topbar-sub">
      <span id="clockText">Đang tải dữ liệu...</span>
      <span onclick="loadState()" style="cursor:pointer;text-decoration:underline;">🔄 Làm mới</span>
    </div>
  </div>

  <div class="tabs">
    <button class="tab-btn active" onclick="switchTab('work', this)">📋 Công Việc</button>
    <button class="tab-btn" onclick="switchTab('assign', this)">➕ Giao Việc</button>
    <button class="tab-btn" onclick="switchTab('ops', this)">🧹 Vệ Sinh & Ăn</button>
    <button class="tab-btn" onclick="switchTab('sched', this)">📅 Lịch Làm</button>
  </div>

  <div class="container">
    <div id="tab-work">
      <div class="card">
        <div class="shift-switch">
          <button id="btn-shift-sang" onclick="changeShift('sang')">☀️ Ca Sáng</button>
          <button id="btn-shift-chieu" onclick="changeShift('chieu')">🌙 Ca Chiều</button>
          <button id="btn-shift-vs" onclick="changeShift('vs')">📸 Hình Ảnh 10h</button>
        </div>
        <div class="card-title">
          <span>📦 Checklist Kho & Vận Hành</span>
          <span id="shift-badge" class="badge">0/0</span>
        </div>
        <div class="progress-wrap"><div id="shift-progress" class="progress-bar" style="width:0%"></div></div>
        <div id="shift-tasks-list"></div>
      </div>

      <div class="card">
        <div class="card-title">
          <span>📌 Công Việc Giao Thêm Hôm Nay</span>
          <span id="adhoc-badge" class="badge">0/0</span>
        </div>
        <div class="progress-wrap"><div id="adhoc-progress" class="progress-bar" style="width:0%"></div></div>
        <div id="adhoc-tasks-list"></div>
      </div>
    </div>

    <div id="tab-assign" style="display:none;">
      <div class="card">
        <div class="card-title"><span>🚀 Giao Việc Nhanh (Không cần nhớ cú pháp)</span></div>
        <label style="font-size:12px;font-weight:700;color:var(--muted);display:block;margin-bottom:4px;">
          1. Nhập nội dung công việc (mỗi dòng 1 việc):
        </label>
        <textarea id="assignTaskInput" rows="3" placeholder="Ví dụ:&#10;Thay bảng giá khuyến mãi Tivi&#10;Dọn vệ sinh kho tầng 2"></textarea>

        <label style="font-size:12px;font-weight:700;color:var(--muted);display:block;margin:6px 0;">
          2. Chọn nhanh nhóm hoặc tick chọn từng người:
        </label>
        <div class="group-pills">
          <span class="pill" onclick="selectRosterGroup('ALL')">👥 @All (Cả ST)</span>
          <span class="pill" onclick="selectRosterGroup('NV')">👨‍💼 @NV</span>
          <span class="pill" onclick="selectRosterGroup('PG')">👩‍💼 @PG</span>
          <span class="pill" onclick="selectRosterGroup('QL+TC')">⭐ @QL+TC</span>
          <span class="pill" onclick="selectRosterGroup('NONE')">✖ Bỏ chọn</span>
        </div>
        <div id="rosterCheckboxes" class="roster-grid"></div>

        <div style="display:flex;gap:8px;margin-top:12px;">
          <button class="btn btn-primary" style="flex:2;padding:11px;" onclick="submitAssignTask()">🚀 Giao Việc Ngay</button>
          <button class="btn btn-danger" style="flex:1;padding:11px;" onclick="cancelLatestTask()">🗑 Hủy việc cuối</button>
        </div>
      </div>
    </div>

    <div id="tab-ops" style="display:none;">
      <div class="card">
        <div class="card-title">
          <span id="vesinh-title">🧹 Phân Công Vệ Sinh</span>
          <span id="vesinh-badge" class="badge">0/0</span>
        </div>
        <div id="vesinh-list"></div>
      </div>

      <div class="card">
        <div class="card-title">
          <span id="meal-title">🍱 Điểm Danh Ăn Ca</span>
          <span id="meal-badge" class="badge">0/0</span>
        </div>
        <div id="meal-list" class="grid-2"></div>
      </div>
    </div>

    <div id="tab-sched" style="display:none;">
      <div class="card">
        <div class="card-title"><span>👨‍💼 Lịch Nhân Viên Hôm Nay</span></div>
        <pre id="sched-nv" class="sched-box">Đang tải...</pre>
      </div>
      <div class="card">
        <div class="card-title"><span>👩‍💼 Lịch PG Hôm Nay</span></div>
        <pre id="sched-pg" class="sched-box">Đang tải...</pre>
      </div>
    </div>
  </div>

  <div id="toast" class="toast"></div>

  <script>
    const LIFF_ID = "{liff_id}";
    const urlParams = new URLSearchParams(window.location.search);
    const GROUP_ID = urlParams.get('group_id') || "{default_gid}";
    let currentShift = "";

    function showToast(msg) {{
      const el = document.getElementById('toast');
      el.textContent = msg;
      el.style.display = 'block';
      setTimeout(() => {{ el.style.display = 'none'; }}, 2400);
    }}

    function getCurrentUser() {{
      return localStorage.getItem('dmx_user_name') || document.getElementById('userSelect').value || 'Nhân viên';
    }}

    function saveCurrentUser(val) {{
      if (val) localStorage.setItem('dmx_user_name', val);
    }}

    function switchTab(tab, btn) {{
      ['work', 'assign', 'ops', 'sched'].forEach(t => {{
        document.getElementById('tab-' + t).style.display = (t === tab) ? 'block' : 'none';
      }});
      document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      if (btn) btn.classList.add('active');
    }}

    function changeShift(s) {{
      currentShift = s;
      loadState();
    }}

    async function initLiffIfAvailable() {{
      if (!LIFF_ID) return;
      try {{
        await liff.init({{ liffId: LIFF_ID }});
        if (liff.isLoggedIn()) {{
          const prof = await liff.getProfile();
          if (prof && prof.displayName) saveCurrentUser(prof.displayName);
        }}
      }} catch (e) {{}}
    }}

    async function loadState() {{
      try {{
        const qs = new URLSearchParams({{ group_id: GROUP_ID }});
        if (currentShift) qs.set('shift', currentShift);
        const res = await fetch('/api/liff/state?' + qs.toString());
        const data = await res.json();
        if (!data.ok) return;
        currentShift = data.current_shift;
        renderAll(data);
      }} catch (e) {{}}
    }}

    function renderAll(data) {{
      document.getElementById('clockText').textContent = data.day_of_week + ' • ' + data.now_str;
      ['sang', 'chieu', 'vs'].forEach(s => {{
        const b = document.getElementById('btn-shift-' + s);
        if (b) b.classList.toggle('active', s === data.current_shift);
      }});

      const allStaff = [];
      ['NV', 'PG', 'QL+TC'].forEach(g => {{
        (data.roster[g] || []).forEach(n => {{ if (!allStaff.includes(n)) allStaff.push(n); }});
      }});
      const sel = document.getElementById('userSelect');
      const savedUser = localStorage.getItem('dmx_user_name');
      if (sel.options.length <= 1 && allStaff.length > 0) {{
        allStaff.forEach(n => {{
          const opt = document.createElement('option');
          opt.value = n; opt.textContent = '👤 ' + n;
          sel.appendChild(opt);
        }});
        if (savedUser) sel.value = savedUser;
      }}

      const stList = document.getElementById('shift-tasks-list');
      stList.innerHTML = '';
      let stDone = 0;
      (data.shift_tasks || []).forEach(t => {{
        const isDone = t.status === 'complete';
        if (isDone) stDone++;
        const div = document.createElement('div');
        div.className = 'task-item' + (isDone ? ' done' : '');
        div.innerHTML = `
          <div class="task-info">
            <div class="task-name">${{isDone ? '✅' : t.icon}} ${{t.name}}</div>
            <div class="task-meta">⏰ Hạn: ${{t.time}}</div>
            ${{isDone && t.user_name ? `<div class="task-done-by">✓ ${{t.user_name}}</div>` : ''}}
          </div>
          <button class="btn ${{isDone ? 'btn-muted' : 'btn-primary'}}"
            onclick="toggleShiftTask('${{t.id}}', '${{isDone ? 'incomplete' : 'complete'}}')">
            ${{isDone ? '✓ Xong' : 'Hoàn tất'}}
          </button>
        `;
        stList.appendChild(div);
      }});
      const stTotal = (data.shift_tasks || []).length;
      const stPct = stTotal ? Math.round((stDone / stTotal) * 100) : 0;
      document.getElementById('shift-badge').textContent = `${{stDone}}/${{stTotal}} (${{stPct}}%)`;
      document.getElementById('shift-progress').style.width = stPct + '%';

      const adhocWrap = document.getElementById('adhoc-tasks-list');
      adhocWrap.innerHTML = '';
      const rawAdhoc = data.adhoc_tasks || [];
      let adDone = 0;
      rawAdhoc.forEach(r => {{ if (r.status === 'complete') adDone++; }});
      const adTotal = rawAdhoc.length;
      const adPct = adTotal ? Math.round((adDone / adTotal) * 100) : 0;
      document.getElementById('adhoc-badge').textContent = `${{adDone}}/${{adTotal}} (${{adPct}}%)`;
      document.getElementById('adhoc-progress').style.width = adPct + '%';

      if (rawAdhoc.length === 0) {{
        adhocWrap.innerHTML = '<div style="font-size:12px;color:var(--muted);text-align:center;padding:12px;">Hôm nay chưa có công việc giao thêm nào.</div>';
      }} else {{
        const groups = [];
        const map = {{}};
        rawAdhoc.forEach(r => {{
          const key = r.task_name;
          if (!(key in map)) {{
            map[key] = groups.length;
            groups.push({{ title: r.task_name, created_at: r.created_at, items: [] }});
          }}
          groups[map[key]].items.push(r);
        }});
        groups.forEach((g, idx) => {{
          const gDone = g.items.filter(i => i.status === 'complete').length;
          const box = document.createElement('div');
          box.className = 'task-item' + (gDone === g.items.length ? ' done' : '');
          box.style.flexDirection = 'column';
          box.style.alignItems = 'stretch';
          let chipsHtml = '<div class="grid-2">';
          g.items.forEach(item => {{
            const comp = item.status === 'complete';
            const nextSt = comp ? 'incomplete' : 'complete';
            const timeLabel = comp && item.completed_at ? ` • ${{item.completed_at}}` : '';
            chipsHtml += `
              <button class="chip-btn ${{comp ? 'done' : ''}}" onclick="toggleAdhocTask('${{item.task_id}}', '${{nextSt}}')">
                <span>${{comp ? '✓' : '⏳'}} ${{item.assignee}}${{timeLabel}}</span>
              </button>
            `;
          }});
          chipsHtml += '</div>';
          box.innerHTML = `
            <div style="display:flex;justify-content:space-between;align-items:center;">
              <div class="task-name">${{idx + 1}}. ${{g.title}}</div>
              <span class="badge ${{gDone === g.items.length ? 'green' : ''}}">${{gDone}}/${{g.items.length}}</span>
            </div>
            <div style="font-size:11px;color:var(--muted);margin-top:2px;">🕒 Giao lúc: ${{g.created_at || '--:--'}} (Chạm tên để Hoàn tất / Hủy)</div>
            ${{chipsHtml}}
          `;
          adhocWrap.appendChild(box);
        }});
      }}

      const rBox = document.getElementById('rosterCheckboxes');
      if (rBox.children.length === 0 && allStaff.length > 0) {{
        ['NV', 'PG', 'QL+TC'].forEach(grp => {{
          (data.roster[grp] || []).forEach(nm => {{
            const lbl = document.createElement('label');
            lbl.className = 'staff-chk';
            lbl.innerHTML = `<input type="checkbox" class="chk-staff" data-group="${{grp}}" value="${{nm}}" /> <span>[${{grp}}] ${{nm}}</span>`;
            rBox.appendChild(lbl);
          }});
        }});
      }}

      const vsWrap = document.getElementById('vesinh-list');
      vsWrap.innerHTML = '';
      const vsList = data.vesinh_list || [];
      let vsDone = 0;
      vsList.forEach(v => {{
        const isDone = v.status === 'done';
        if (isDone) vsDone++;
        const nextSt = isDone ? 'waiting' : 'done';
        const d = document.createElement('div');
        d.className = 'task-item' + (isDone ? ' done' : '');
        d.innerHTML = `
          <div class="task-info">
            <div class="task-name">[${{v.type}}] ${{v.name}}</div>
            <div style="font-size:11px;color:var(--muted);margin-top:2px;">📍 ${{v.zone}}</div>
            ${{isDone ? `<div class="task-done-by">✅ Xong lúc ${{v.time_clicked}}</div>` : ''}}
          </div>
          <button class="btn ${{isDone ? 'btn-muted' : 'btn-primary'}}"
            onclick="toggleVesinh('${{data.vesinh_session}}', '${{v.name}}', '${{nextSt}}')">
            ${{isDone ? '✓ Xong' : 'Hoàn tất'}}
          </button>
        `;
        vsWrap.appendChild(d);
      }});
      document.getElementById('vesinh-badge').textContent = `${{vsDone}}/${{vsList.length}}`;

      const mealWrap = document.getElementById('meal-list');
      mealWrap.innerHTML = '';
      const mList = data.meal_list || [];
      let mDone = 0;
      mList.forEach(m => {{
        const isDone = m.status === 'done';
        if (isDone) mDone++;
        const nextSt = isDone ? 'waiting' : 'done';
        const btn = document.createElement('button');
        btn.className = 'chip-btn' + (isDone ? ' done' : '');
        btn.onclick = () => toggleMeal(data.meal_session, m.name, nextSt);
        btn.innerHTML = `<span>${{isDone ? '🟢' : '🍲'}} ${{m.name}}</span><span>${{m.time_clicked || ''}}</span>`;
        mealWrap.appendChild(btn);
      }});
      document.getElementById('meal-badge').textContent = `${{mDone}}/${{mList.length}}`;

      document.getElementById('sched-nv').textContent = (data.today_schedule && data.today_schedule.employee) || 'Chưa có dữ liệu';
      document.getElementById('sched-pg').textContent = (data.today_schedule && data.today_schedule.pg) || 'Chưa có dữ liệu';
    }}

    async function toggleShiftTask(taskId, targetStatus) {{
      await fetch('/api/liff/toggle-shift-task', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ group_id: GROUP_ID, task_id: taskId, shift: currentShift, target_status: targetStatus, user_name: getCurrentUser() }})
      }});
      showToast(targetStatus === 'complete' ? '✅ Đã đánh dấu hoàn tất!' : '↩️ Đã hoàn tác!');
      loadState();
    }}

    async function toggleAdhocTask(taskId, targetStatus) {{
      await fetch('/api/liff/toggle-adhoc-task', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ group_id: GROUP_ID, task_id: taskId, target_status: targetStatus, user_name: getCurrentUser() }})
      }});
      showToast(targetStatus === 'complete' ? '✅ Đã hoàn tất công việc!' : '↩️ Đã hoàn tác!');
      loadState();
    }}

    function selectRosterGroup(grp) {{
      document.querySelectorAll('.chk-staff').forEach(chk => {{
        if (grp === 'ALL') chk.checked = true;
        else if (grp === 'NONE') chk.checked = false;
        else chk.checked = (chk.getAttribute('data-group') === grp);
      }});
    }}

    async function submitAssignTask() {{
      const title = document.getElementById('assignTaskInput').value.trim();
      const selected = Array.from(document.querySelectorAll('.chk-staff:checked')).map(c => c.value);
      if (!title) {{ showToast('⚠️ Vui lòng nhập nội dung việc!'); return; }}
      if (selected.length === 0) {{ showToast('⚠️ Vui lòng chọn người nhận việc!'); return; }}

      const res = await fetch('/api/liff/assign-task', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ group_id: GROUP_ID, task_title: title, assignees: selected }})
      }});
      const out = await res.json();
      if (out.ok) {{
        document.getElementById('assignTaskInput').value = '';
        showToast(`🚀 Đã giao việc thành công (${{out.count}} mục)!`);
        switchTab('work', document.querySelectorAll('.tab-btn')[0]);
        loadState();
      }} else {{
        showToast('❌ ' + (out.error || 'Lỗi giao việc'));
      }}
    }}

    async function cancelLatestTask() {{
      const res = await fetch('/api/liff/cancel-latest-task', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ group_id: GROUP_ID }})
      }});
      const out = await res.json();
      if (out.ok) {{
        showToast(`🗑 Đã hủy nhóm việc (${{out.removed}} mục)!`);
        loadState();
      }} else {{
        showToast('⚠️ ' + (out.error || 'Không có việc để hủy'));
      }}
    }}

    async function toggleMeal(session, name, targetStatus) {{
      await fetch('/api/liff/toggle-meal', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ group_id: GROUP_ID, session: session, name: name, user_name: getCurrentUser(), target_status: targetStatus }})
      }});
      showToast('🍱 Đã cập nhật điểm danh ăn!');
      loadState();
    }}

    async function toggleVesinh(session, name, targetStatus) {{
      await fetch('/api/liff/toggle-vesinh', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ group_id: GROUP_ID, session: session, name: name, user_name: getCurrentUser(), target_status: targetStatus }})
      }});
      showToast('🧹 Đã cập nhật vệ sinh!');
      loadState();
    }}

    initLiffIfAvailable().then(loadState);
  </script>
</body>
</html>"""
    return Response(html, mimetype='text/html')

# --- XỬ LÝ SỰ KIỆN POSTBACK ---

@handler.add(PostbackEvent)
def handle_postback(event):
    data_str = event.postback.data
    data = dict(urllib.parse.parse_qsl(data_str))
    action = data.get('action')
    user_id = event.source.user_id
    event_group_id = getattr(event.source, 'group_id', None)

    # 1. Gia hạn User
    if action == 'renew':
        target_id = data.get('id')
        duration_str = data.get('duration')
        
        delta, duration_text = parse_duration(duration_str)
        if not delta:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="Thời hạn gia hạn không hợp lệ."))
            return

        try:
            start_date = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh'))
            new_expiration_date = start_date + delta
            new_expiration_date_str = new_expiration_date.strftime('%Y-%m-%d')
            
            update_expiration_in_sheet(target_id, new_expiration_date_str)
            load_allowed_ids()

            reply_text = f"✅ Đã gia hạn thành công!\n- ID: {target_id}\n- Thêm: {duration_text}\n- Hạn mới: {new_expiration_date.strftime('%d-%m-%Y')}"
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))

        except Exception as e:
            print(f"Lỗi khi gia hạn: {e}")
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="Có lỗi xảy ra khi gia hạn."))
        return

    # 1.5. Nút phát Lịch & Checklist đầu ca miễn phí qua reply_token
    if action == 'broadcast_shift':
        shift_type = data.get('shift')
        if not shift_type:
            tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
            shift_type = 'sang' if datetime.now(tz_vietnam).hour < 14 else 'chieu'
        target_gid = event_group_id or data.get('group_id') or os.environ.get('EMPLOYEE_GROUP_ID') or MAIN_CHECKLIST_GROUP_ID
        msgs = build_shift_broadcast_messages(target_gid, shift_type)
        if msgs:
            mark_shift_broadcast_sent(target_gid, shift_type)
            line_bot_api.reply_message(event.reply_token, msgs[:LINE_MAX_MESSAGES])
        else:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="⚠️ Không có dữ liệu lịch hoặc checklist cho ca này."))
        return

    # 1.8. Mục 3: Xem kết quả cào dữ liệu BI qua Postback (Miễn phí 100% không tốn Push)
    if action == 'view_scraped_report':
        scrape_type_val = data.get('type', 'realtime')
        req_prefix = data.get('req', '')
        try:
            st_info = check_scrape_status()
            sig_status = st_info.get("status")
            if sig_status == "running":
                pending_flex = build_scrape_pending_flex(scrape_type_val, req_prefix)
                line_bot_api.reply_message(
                    event.reply_token,
                    FlexSendMessage(alt_text="⏳ Đang cào dữ liệu BI...", contents=pending_flex)
                )
                return

            if scrape_type_val == "luyke":
                flex_msg = build_luyke_flex()
                if isinstance(flex_msg, list):
                    carousel_content = {"type": "carousel", "contents": flex_msg}
                    line_bot_api.reply_message(
                        event.reply_token,
                        FlexSendMessage(alt_text="📊 BÁO CÁO LŨY KẾ (Cuộn Ngang P.1 & P.2)", contents=carousel_content)
                    )
                else:
                    line_bot_api.reply_message(
                        event.reply_token,
                        FlexSendMessage(alt_text="Báo Cáo Lũy Kế Savico", contents=flex_msg)
                    )
            else:
                rt_messages, _ = build_realtime_messages(max_messages=LINE_MAX_MESSAGES)
                if rt_messages:
                    reply_msgs = [FlexSendMessage(alt_text=m["alt_text"], contents=m["contents"]) for m in rt_messages]
                    line_bot_api.reply_message(event.reply_token, reply_msgs[:LINE_MAX_MESSAGES])
                else:
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(text="⚠️ Báo cáo realtime quá lớn để gói gọn trong 5 tin nhắn. Vui lòng xem dashboard HTML.")
                    )
        except Exception as e:
            print(f"Lỗi xem báo cáo vừa cào: {e}")
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"⚠️ Lỗi hiển thị báo cáo: {e}"))
        return

    # 2. Hoàn thành Task công việc (Checklist Công việc — Tốc độ tức thì qua RAM Cache)
    if action == 'complete_task':
        task_id = str(data.get('task_id') or '').strip()
        shift_type = data.get('shift', 'sang')
        target_status = data.get('target_status', 'complete')
        
        if target_status == 'incomplete':
            return

        group_id = str(
            event_group_id
            or data.get('group_id')
            or os.environ.get('EMPLOYEE_GROUP_ID')
            or MAIN_CHECKLIST_GROUP_ID
        ).strip()

        try:
            user_name = get_cached_user_display_name(event_group_id, user_id, fallback="Nhân viên")
            all_records, resolved_gid = update_shift_task_status(
                group_id, task_id, shift_type, target_status, user_name
            )
            if all_records == "already":
                return

            if resolved_gid:
                group_id = resolved_gid

            if shift_type == 'vs':
                updated_flex_content = generate_checklist_flex(group_id, 'vs', all_records_prefetched=all_records)
                alt_text = "Cập nhật checklist hình ảnh"
            elif str(group_id).strip() == str(MAIN_CHECKLIST_GROUP_ID).strip():
                checklist_bubble = generate_checklist_flex(group_id, shift_type, all_records_prefetched=all_records)
                adhoc_bubble = generate_today_adhoc_flex(group_id)
                if checklist_bubble and adhoc_bubble:
                    updated_flex_content = {"type": "carousel", "contents": [checklist_bubble, adhoc_bubble]}
                else:
                    updated_flex_content = checklist_bubble or adhoc_bubble
                alt_text = f"📋 Cập nhật checklist ca {shift_type}"
            else:
                updated_flex_content = generate_checklist_flex(group_id, shift_type, all_records_prefetched=all_records)
                alt_text = f"📋 Cập nhật checklist ca {shift_type}"

            mark_shift_broadcast_sent(group_id, shift_type)
            line_bot_api.reply_message(
                event.reply_token,
                FlexSendMessage(alt_text=alt_text, contents=updated_flex_content)
            )

        except Exception as e:
            print(f"Lỗi nghiêm trọng khi xử lý postback hoàn thành công việc: {e}")
            try:
                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage(text=f"⚠️ Lỗi cập nhật công việc: {e}")
                )
            except:
                pass
        return

    # 2.5. Hoàn thành công việc phát sinh (Adhoc task — Tốc độ tức thì qua RAM Cache)
    if action == 'complete_adhoc_task':
        task_id = data.get('task_id')
        assignee = data.get('assignee')
        target_status = data.get('target_status', 'complete')
        shift_type = data.get('shift')
        group_id = event_group_id or os.environ.get('EMPLOYEE_GROUP_ID') or MAIN_CHECKLIST_GROUP_ID

        if target_status == 'incomplete':
            return

        try:
            user_name = get_cached_user_display_name(event_group_id, user_id, fallback=assignee or "Nhân viên")
            success, resolved_assignee, task_group_hash = update_adhoc_task_status(group_id, task_id, target_status, user_name)
            
            if success == "already":
                return
            if success:
                if not shift_type:
                    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
                    current_hour = datetime.now(tz_vietnam).hour
                    shift_type = 'sang' if current_hour < 15 else 'chieu'

                mode = 'all' if (task_id and str(task_id).startswith('all_')) else 'multi'
                updated_flex_content = generate_combined_work_flex(
                    group_id, shift_type, adhoc_hash=task_group_hash, adhoc_mode=mode
                )
                alt_text = f"📋 Checklist ca {shift_type} & Cập nhật công việc giao thêm"

                if updated_flex_content:
                    reply_with_auto_shift(
                        event.reply_token,
                        FlexSendMessage(alt_text=alt_text, contents=updated_flex_content),
                        group_id=event_group_id
                    )
        except Exception as e:
            print(f"Lỗi nghiêm trọng khi xử lý postback hoàn thành công việc phát sinh: {e}")
        return

    # 2.7. Xem danh mục phiếu giảm giá (GVGS)
    if action == 'view_coupon_menu':
        flex = build_category_menu_flex()
        reply_with_auto_shift(event.reply_token, FlexSendMessage(
            alt_text="🎟️ Danh mục phiếu giảm giá (GVGS)", contents=flex
        ), group_id=event_group_id)
        return

    # 2.8. Xem danh sách model thuộc ngành hàng
    if action == 'view_coupon_cat':
        cat = data.get('cat')
        flex = build_product_list_flex(cat)
        reply_with_auto_shift(event.reply_token, FlexSendMessage(
            alt_text=f"🎟️ Phiếu giảm giá {cat}", contents=flex
        ), group_id=event_group_id)
        return

    # 2.9. Cấp mã coupon cho nhân viên (Claim coupon)
    if action == 'claim_coupon':
        prod_name = data.get('prod')
        cat_from_data = data.get('cat')
        user_name = get_cached_user_display_name(event_group_id, user_id, fallback="Nhân viên")

        success, res, remaining, category = claim_coupon_for_user(prod_name, user_name)
        if success:
            cat_name = cat_from_data or category or "Tất cả"
            claimed_flex = build_claimed_coupon_flex(prod_name, res, remaining, user_name)
            code_text_msg = TextSendMessage(text=str(res))
            updated_list_flex = build_product_list_flex(cat_name)

            messages_to_send = [
                FlexSendMessage(alt_text=f"🎟️ Nhận mã thành công cho {prod_name}: {res}", contents=claimed_flex),
                code_text_msg,
                FlexSendMessage(alt_text=f"🎟️ Phiếu giảm giá {cat_name} (Đã cập nhật)", contents=updated_list_flex)
            ]
            reply_with_auto_shift(event.reply_token, messages_to_send, group_id=event_group_id)
        else:
            reply_with_auto_shift(event.reply_token, TextSendMessage(text=f"⚠️ {res}"), group_id=event_group_id)
        return

    # 3. Check-in Ăn Sáng/Chiều
    if action == 'meal_checkin':
        session_type = data.get('session')
        staff_name = data.get('name')
        target_status = data.get('target_status', 'done')
        group_id = event_group_id or data.get('group_id') or os.environ.get('EMPLOYEE_GROUP_ID') or MAIN_CHECKLIST_GROUP_ID or 'default'

        clicker_name = get_cached_user_display_name(group_id, user_id, fallback="Nhân viên")
        status_code, time_str = update_meal_status(group_id, session_type, staff_name, clicker_name, target_status)
        
        if status_code is True:
            updated_flex = generate_meal_flex(group_id, session_type)
            if updated_flex:
                line_bot_api.reply_message(
                    event.reply_token,
                    FlexSendMessage(alt_text=f"Checklist ăn {session_type} updated", contents=updated_flex)
                )
        elif status_code == "already":
            return
        else:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❌ Lỗi: Không tìm thấy tên hoặc lỗi cập nhật."))
        return

    # 3.5. Check-in Vệ Sinh
    if action == 'complete_vesinh':
        session_type = data.get('session')
        staff_name = data.get('name')
        target_status = data.get('target_status', 'done')
        group_id = event_group_id or data.get('group_id') or os.environ.get('EMPLOYEE_GROUP_ID') or MAIN_CHECKLIST_GROUP_ID or 'default'

        clicker_name = get_cached_user_display_name(group_id, user_id, fallback="Nhân viên")
        status_code, time_str = update_vesinh_status(group_id, session_type, staff_name, clicker_name, target_status)
        
        if status_code is True:
            updated_flex = generate_vesinh_flex(group_id, session_type)
            if updated_flex:
                line_bot_api.reply_message(
                    event.reply_token,
                    FlexSendMessage(alt_text=f"Bảng phân công vệ sinh {session_type} updated", contents=updated_flex)
                )
        elif status_code == "already":
            return
        else:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="❌ Lỗi: Không tìm thấy tên hoặc lỗi cập nhật."))
        return


def get_group_members(group_id):
    """
    Lấy danh sách người nhận việc chung @all từ 3 cột E/F/G của sheet `schedules` (có RAM Cache).
    """
    member_names = []
    try:
        _roster = get_task_roster()
        roster = []
        for _grp in ('PG', 'NV', 'QL+TC'):
            for _nm in _roster.get(_grp, []):
                if _nm not in roster:
                    roster.append(_nm)
        if roster:
            return roster
    except Exception as e_roster:
        print(f"Lỗi đọc roster từ sheet schedules: {e_roster}")

    try:
        records = get_cached_records('group_members', ttl=300)
        seen_names = set()
        for r in records:
            if str(r.get('group_id')) == str(group_id):
                name = r.get('display_name')
                if name:
                    seen_names.add(str(name).strip())
        if seen_names:
            member_names = sorted(list(seen_names))
    except Exception as e_cache:
        print(f"Lỗi lấy danh sách thành viên từ cache sheet: {e_cache}")

    if not member_names:
        try:
            from meal_handler import get_working_staff
            staff_morning = get_working_staff('ansang')
            staff_afternoon = get_working_staff('anchieu')
            all_staff = set()
            for s_type in ['NV', 'PG']:
                for name in staff_morning.get(s_type, []):
                    all_staff.add(name)
                for name in staff_afternoon.get(s_type, []):
                    all_staff.add(name)
            member_names = sorted(list(all_staff))
        except Exception as e_fallback:
            print(f"Lỗi fallback lấy nhân viên: {e_fallback}")
            
    return member_names


# --- XỬ LÝ TIN NHẮN ---

def _extract_mentionees(message):
    mention = getattr(message, 'mention', None)
    if not mention:
        return []
    out = []
    for m in getattr(mention, 'mentionees', None) or []:
        idx = getattr(m, 'index', None)
        ln = getattr(m, 'length', None)
        if idx is None or ln is None:
            continue
        out.append({'index': int(idx), 'length': int(ln),
                    'type': getattr(m, 'type', 'user') or 'user'})
    return out


@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    user_message = event.message.text.strip()
    user_msg_upper = user_message.upper()
    user_id = event.source.user_id
    event_group_id = getattr(event.source, 'group_id', None)
    source_id = event_group_id or user_id

    # Tự động lưu thành viên nhóm chạy ngầm (không chặn luồng xử lý lệnh)
    if getattr(event.source, 'type', '') == 'group' and event_group_id:
        try:
            disp_name = get_cached_user_display_name(event_group_id, user_id, fallback="")
            if disp_name:
                register_group_member(event_group_id, user_id, disp_name)
        except Exception as e_reg:
            print(f"Không thể lấy profile để lưu thành viên: {e_reg}")

    # 0. Giao công việc phát sinh (Adhoc task)
    lines = [line.strip() for line in user_message.split('\n') if line.strip()]
    _la_lenh_viec = is_work_command(lines)

    if _la_lenh_viec:
        group_id = event_group_id
        if not group_id:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(
                text="⚠️ Chức năng giao việc chỉ sử dụng được trong nhóm chat."))
            return

        try:
            roster = get_task_roster()

            _mentionees = _extract_mentionees(event.message)
            if _mentionees:
                _fixed = resolve_mentions(event.message.text, _mentionees, roster)
                lines = [l.strip() for l in _fixed.split('\n') if l.strip()]

            parsed = parse_work_command(lines, roster)

            if parsed['errors']:
                msg = "⚠️ Chưa giao được việc, anh kiểm tra giúp em:\n"
                msg += "\n".join("• " + e for e in parsed['errors'][:6])
                msg += ("\n\nCú pháp: `việc @all:` rồi xuống dòng `- nội dung việc`.\n"
                        "Đích dùng được: @all @nv @pg @st, hoặc @156494, hoặc @Thảo.")
                reply_with_auto_shift(event.reply_token, TextSendMessage(text=msg), group_id=group_id)
                return

            tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
            current_hour = datetime.now(tz_vietnam).hour
            current_shift = 'sang' if current_hour < 15 else 'chieu'

            plan = plan_assignments(parsed)
            if plan['mode'] == 'all':
                last_hash = add_all_adhoc_tasks(group_id, plan['names'],
                                                plan['task'])
            else:
                last_hash = add_multi_adhoc_tasks(group_id, plan['job'],
                                                  plan['pairs'])

            if not last_hash:
                line_bot_api.reply_message(event.reply_token, TextSendMessage(
                    text="❌ Có lỗi xảy ra khi lưu công việc."))
                return

            flex_content = generate_combined_work_flex(
                group_id, current_shift, adhoc_hash=last_hash, adhoc_mode=plan['mode']
            )
            alt_text = f"📋 Checklist ca {current_shift} & Công việc giao thêm"

            if flex_content:
                reply_with_auto_shift(
                    event.reply_token,
                    FlexSendMessage(alt_text=alt_text, contents=flex_content),
                    group_id=group_id
                )
            else:
                line_bot_api.reply_message(event.reply_token, TextSendMessage(
                    text="❌ Có lỗi xảy ra khi tạo danh sách công việc."))
        except Exception as e:
            print(f"Lỗi khi xử lý lệnh giao việc: {e}")
            line_bot_api.reply_message(event.reply_token, TextSendMessage(
                text="❌ Gặp lỗi khi xử lý giao việc."))
        return

    # 0b. Hủy việc đã giao
    _parts = user_message.split()
    if (len(_parts) >= 2
            and _parts[0].upper() in ('HỦY', 'HUY', 'XÓA', 'XOA')
            and _parts[1].upper() in ('VIỆC', 'VIEC', 'TASK')):
        group_id = event_group_id
        if not group_id:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(
                text="⚠️ Lệnh hủy chỉ dùng được trong nhóm chat."))
            return
        try:
            token_arg = _parts[2].strip() if len(_parts) >= 3 else None
            if token_arg:
                token, label, count = token_arg, "(chỉ định theo mã)", 0
            else:
                token, label, count = find_latest_task_token(group_id)

            if not token:
                reply_with_auto_shift(event.reply_token, TextSendMessage(
                    text="Hôm nay chưa có việc nào được giao để hủy."), group_id=group_id)
                return

            removed, ten_viec = cancel_task_group(group_id, token)
            if removed:
                ds = ", ".join(ten_viec[:3])
                reply_with_auto_shift(event.reply_token, TextSendMessage(
                    text=f"🗑 Đã hủy {removed} dòng việc.\nMã nhóm: {token}\nViệc: {ds}"), group_id=group_id)
            else:
                reply_with_auto_shift(event.reply_token, TextSendMessage(
                    text=f"Không tìm thấy việc nào khớp mã {token} để hủy."), group_id=group_id)
        except Exception as e:
            print(f"Lỗi khi hủy việc: {e}")
            line_bot_api.reply_message(event.reply_token, TextSendMessage(
                text="❌ Gặp lỗi khi hủy việc."))
        return

    # 0c. Lệnh nạp mã giảm giá
    if (user_msg_upper.startswith('ADD COUPON') or 
        user_msg_upper.startswith('THEM COUPON') or 
        user_msg_upper.startswith('THÊM COUPON') or
        user_msg_upper.startswith('NẠP COUPON') or
        user_msg_upper.startswith('NAP COUPON')):
        content_after = re.sub(r'^(?:ADD|THEM|THÊM|NẠP|NAP)\s+COUPON\s*:?', '', user_message, flags=re.IGNORECASE).strip()
        if content_after:
            count, prods, dup_count = import_coupons_from_text(content_after)
            if count > 0:
                p_sample = ", ".join(prods[:4])
                if len(prods) > 4:
                    p_sample += f" và {len(prods) - 4} model khác"
                reply = f"✅ Đã nạp thành công thêm {count} mã giảm giá vào hệ thống!\n📦 Áp dụng cho: {p_sample}."
                if dup_count > 0:
                    reply += f"\nℹ️ Đã tự động bỏ qua {dup_count} mã bị trùng lặp."
                reply += "\n\n👉 Nhân viên có thể gõ `gvgs` để nhận mã."
            elif dup_count > 0:
                reply = f"ℹ️ Tất cả {dup_count} mã trong nội dung gửi đã tồn tại trên hệ thống từ trước (không nạp trùng lặp)."
            else:
                reply = "⚠️ Không tìm thấy mã hợp lệ từ nội dung đã gửi. Vui lòng kiểm tra lại định dạng (mỗi dòng gồm Ngày : Sản phẩm : Mã)."
        else:
            reply = "⚠️ Vui lòng gửi theo cú pháp:\n`ADD COUPON:` rồi xuống dòng dán danh sách mã."
        reply_with_auto_shift(event.reply_token, TextSendMessage(text=reply), group_id=event_group_id)
        return

    # Lệnh xóa toàn bộ mã giảm giá
    if user_msg_upper in ['XOA GVGS', 'XÓA GVGS', 'CLEAR GVGS', 'RESET GVGS', 'XOA COUPON', 'XÓA COUPON']:
        success, msg = clear_all_coupons()
        reply_with_auto_shift(event.reply_token, TextSendMessage(text=msg), group_id=event_group_id)
        return

    # 0d. Lệnh xem và nhận phiếu giảm giá (GVGS)
    if user_msg_upper in ['GVGS', 'COUPON', 'MA GIAM GIA', 'MÃ GIẢM GIÁ', 'GIAM GIA', 'GIẢM GIÁ']:
        flex = build_category_menu_flex()
        reply_with_auto_shift(event.reply_token, FlexSendMessage(
            alt_text="🎟️ Danh mục phiếu giảm giá (GVGS)", contents=flex
        ), group_id=event_group_id)
        return

    # 0e. Lệnh mở nhanh Giao diện Web App (LINE LIFF — Mục 15)
    if user_msg_upper in ['APP', 'WEB', 'LIFF', 'QUANLY', 'QUẢN LÝ', 'GIAOVIEC', 'GIAO VIỆC']:
        target_url = resolve_effective_liff_url(event_group_id)
        if not target_url:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="⚠️ Chưa cấu hình RENDER_EXTERNAL_URL hoặc LIFF_URL trên hệ thống.")
            )
            return
        app_bubble = {
            "type": "bubble",
            "size": "mega",
            "header": {
                "type": "box",
                "layout": "vertical",
                "backgroundColor": "#0284C7",
                "paddingAll": "md",
                "contents": [
                    {"type": "text", "text": "🌐 MINI WEB-APP ĐMX SAVICO", "weight": "bold", "size": "md", "color": "#FFFFFF"},
                    {"type": "text", "text": "Giao việc trực quan • Checklist Kho • Vệ sinh & Ăn ca", "size": "xxs", "color": "#E0F2FE", "margin": "xs"}
                ]
            },
            "body": {
                "type": "box",
                "layout": "vertical",
                "paddingAll": "md",
                "spacing": "sm",
                "contents": [
                    {
                        "type": "text",
                        "text": "• Xem & đánh dấu hoàn tất công việc cực nhanh.\n• Tick chọn nhân viên bằng Checkbox để giao việc mà không cần nhớ cú pháp gõ tay.\n• Theo dõi phân công Vệ sinh, Điểm danh ăn và Lịch làm việc.",
                        "size": "xs",
                        "color": "#334155",
                        "wrap": True
                    }
                ]
            },
            "footer": {
                "type": "box",
                "layout": "vertical",
                "paddingAll": "sm",
                "contents": [
                    {
                        "type": "button",
                        "action": {
                            "type": "uri",
                            "label": "🚀 Mở Web App Ngay",
                            "uri": target_url
                        },
                        "style": "primary",
                        "color": "#0284C7",
                        "height": "sm"
                    }
                ]
            }
        }
        reply_with_auto_shift(
            event.reply_token,
            FlexSendMessage(alt_text="🌐 Mở Mini Web-App ĐMX Savico", contents=app_bubble),
            group_id=event_group_id
        )
        return

    # 0f. Lệnh Mở Ca thủ công 1 chạm (Phát trọn bộ Lịch + Checklist bằng reply_token miễn phí)
    if user_msg_upper in ['MỞ CA', 'MO CA', 'MOCA', 'MỞ CA SÁNG', 'MO CA SANG', 'MỞ CA CHIỀU', 'MO CA CHIEU']:
        target_gid = event_group_id or os.environ.get('EMPLOYEE_GROUP_ID') or MAIN_CHECKLIST_GROUP_ID
        if 'SÁNG' in user_msg_upper or 'SANG' in user_msg_upper:
            shift_type = 'sang'
        elif 'CHIỀU' in user_msg_upper or 'CHIEU' in user_msg_upper:
            shift_type = 'chieu'
        else:
            tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
            shift_type = 'sang' if datetime.now(tz_vietnam).hour < 14 else 'chieu'

        msgs = build_shift_broadcast_messages(target_gid, shift_type)
        if msgs:
            mark_shift_broadcast_sent(target_gid, shift_type)
            line_bot_api.reply_message(event.reply_token, msgs[:LINE_MAX_MESSAGES])
        else:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="⚠️ Không tìm thấy lịch hoặc checklist cho ca này."))
        return

    # 1. Admin ADD
    if user_msg_upper.startswith('ADD '):
        if user_id != ADMIN_USER_ID:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="Bạn không có quyền thực hiện lệnh này."))
            return

        parts = user_message.split()
        if len(parts) != 3:
            reply = "Sai cú pháp. Sử dụng: add [ID] [thời hạn]\nVí dụ:\n- `add U... 3d` (3 ngày)\n- `add C... 1m` (1 tháng)\n- `add U... 0` (vĩnh viễn)"
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply))
            return
            
        target_id = parts[1]
        duration_str = parts[2]
        
        delta, duration_text = parse_duration(duration_str)
        if not delta:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="Thời hạn không hợp lệ."))
            return

        try:
            if duration_str == '0':
                expiration_date_str = '9999-12-31'
                reply_duration = "vĩnh viễn"
            else:
                start_date = datetime.now(pytz.timezone('Asia/Ho_Chi_Minh'))
                expiration_date = start_date + delta
                expiration_date_str = expiration_date.strftime('%Y-%m-%d')
                reply_duration = f"{duration_text} (hết hạn ngày {expiration_date.strftime('%d-%m-%Y')})"
        
            action_text = update_expiration_in_sheet(target_id, expiration_date_str)
            load_allowed_ids()
            
            reply_text = f"✅ {action_text} thành công!\n- ID: {target_id}\n- Thời hạn: {reply_duration}"
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))
            
        except Exception as e:
            print(f"Lỗi khi cập nhật Google Sheet: {e}")
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="Có lỗi xảy ra khi cập nhật ID."))
        return

    # 2. Check quyền
    is_controlled_environment = bool(allowed_ids_cache) and ADMIN_USER_ID
    if is_controlled_environment and source_id not in allowed_ids_cache:
        public_commands = ['ID', 'MENU BOT']
        if user_msg_upper not in public_commands and user_id != ADMIN_USER_ID:
            print(f"Bỏ qua tin nhắn từ ID không được phép: {source_id}")
            return

    # 3. ID
    if user_msg_upper == 'ID':
        reply_text = f'👤 User ID:\n{user_id}'
        if event_group_id:
            reply_text = f'👥 Group ID:\n{source_id}\n\n' + reply_text
        reply_with_auto_shift(event.reply_token, TextSendMessage(text=reply_text), group_id=event_group_id)
        return

    # 4. MENU
    if user_msg_upper == 'MENU BOT':
        menu_text = (
            "🤖 **MENU HƯỚNG DẪN BOT** 🤖\n"
            "----------------------------------\n"
            "**🌐 WEB APP QUẢN LÝ & GIAO VIỆC:**\n"
            "• `app` hoặc `liff` - Mở giao diện Web App trực quan.\n"
            "• `mở ca` - Phát Lịch & Checklist đầu ca (Miễn phí).\n"
            "\n"
            "**🍱 ĐIỂM DANH ĂN & VỆ SINH:**\n"
            "• `ăn` / `ansang` / `anchieu` - Điểm danh ăn.\n"
            "• `vesinh` - Phân công khu vực vệ sinh.\n"
            "\n"
            "**✅ CHECKLIST CÔNG VIỆC:**\n"
            "• `sang` / `chieu` - Checklist kho Sáng/Chiều.\n"
            "• `vs` - Checklist hình ảnh trước 10h.\n"
            "\n"
            "**📅 LỊCH LÀM VIỆC:**\n"
            "• `nv` / `pg` - Lịch hôm nay.\n"
            "• `nv2`..`nv8` - Lịch NV theo thứ.\n"
            "\n"
            "**📊 BÁO CÁO DOANH THU:**\n"
            "• `RT1` / `LK1` / `NV0` - Báo cáo Savico.\n"
            "• `ST [Mã ST]` / `bxh` - Báo cáo cụm.\n"
            "\n"
            "**🎟️ PHIẾU GIẢM GIÁ:**\n"
            "• `gvgs` - Danh mục phiếu giảm giá."
        )
        reply_with_auto_shift(event.reply_token, TextSendMessage(text=menu_text), group_id=event_group_id)
        return

    # 4.5. Checklist hình ảnh (VS)
    if user_msg_upper == 'VS':
        group_id = event_group_id
        if not group_id:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="Lệnh này chỉ hoạt động trong nhóm chat."))
            return
        try:
            initialize_daily_tasks(group_id, 'vs')
            flex_content = generate_checklist_flex(group_id, 'vs')
            if flex_content:
                message = FlexSendMessage(alt_text="Checklist hình ảnh trước 10h sáng", contents=flex_content)
                reply_with_auto_shift(event.reply_token, message, group_id=group_id)
            else:
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text="Không thể tạo checklist hình ảnh."))
        except Exception as e:
            print(f"Lỗi khi xử lý lệnh checklist VS: {e}")
        return

    # === 5. XỬ LÝ LỆNH ĂN UỐNG ===
    cmd_normalized = user_message.lower().replace(" ", "")
    meal_cmds = ['ansang', 'anchieu', 'an', 'ăn']
    
    if cmd_normalized in meal_cmds:
        group_id = event_group_id
        if not group_id:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="⚠️ Lệnh này chỉ hoạt động trong nhóm chat."))
            return

        session_type = None
        if cmd_normalized == 'ansang': session_type = 'ansang'
        elif cmd_normalized == 'anchieu': session_type = 'anchieu'
        elif cmd_normalized in ['an', 'ăn']:
            tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
            current_hour = datetime.now(tz_vietnam).hour
            session_type = 'ansang' if current_hour < 15 else 'anchieu'

        if session_type:
            try:
                flex_content = generate_meal_flex(group_id, session_type)
                if flex_content:
                    alt = "Check list ăn trưa" if session_type == 'ansang' else "Check list ăn tối"
                    reply_with_auto_shift(event.reply_token, FlexSendMessage(alt_text=alt, contents=flex_content), group_id=group_id)
                else:
                    reply_with_auto_shift(event.reply_token, TextSendMessage(text="⚠️ Không tìm thấy dữ liệu lịch hoặc toàn bộ nhân sự đều OFF."), group_id=group_id)
            except Exception as e:
                print(f"Lỗi tạo meal flex: {e}")
        return

    # === 5.5 XỬ LÝ LỆNH VỆ SINH (VESINH) ===
    if cmd_normalized.startswith('vesinh') or cmd_normalized.startswith('vệsinh') or cmd_normalized in ['ve sinh', 'vệ sinh']:
        group_id = event_group_id
        if not group_id:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="⚠️ Lệnh này chỉ hoạt động trong nhóm chat."))
            return

        session_type = None
        if 'sang' in cmd_normalized or 'sáng' in cmd_normalized:
            session_type = 'vesinh_sang'
        elif 'chieu' in cmd_normalized or 'chiều' in cmd_normalized:
            session_type = 'vesinh_chieu'
        else:
            session_type = get_current_vesinh_session()

        if session_type:
            try:
                flex_content = generate_vesinh_flex(group_id, session_type)
                if flex_content:
                    alt = "Bảng phân công vệ sinh Ca Sáng" if session_type == 'vesinh_sang' else "Bảng phân công vệ sinh Ca Chiều"
                    reply_with_auto_shift(event.reply_token, FlexSendMessage(alt_text=alt, contents=flex_content), group_id=group_id)
                else:
                    reply_with_auto_shift(event.reply_token, TextSendMessage(text="⚠️ Không tìm thấy dữ liệu lịch vệ sinh hoặc toàn bộ nhân sự đều OFF."), group_id=group_id)
            except Exception as e:
                print(f"Lỗi tạo vesinh flex: {e}")
        return

    # 6. Checklist công việc (Sang/Chieu)
    if user_msg_upper in ['SANG', 'CHIEU']:
        shift_type = 'sang' if user_msg_upper == 'SANG' else 'chieu'
        group_id = event_group_id
        
        if not group_id:
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text="Lệnh này chỉ hoạt động trong nhóm chat."))
            return
        try:
            initialize_daily_tasks(group_id, shift_type)
            if str(group_id).strip() == str(MAIN_CHECKLIST_GROUP_ID).strip():
                flex_content = generate_combined_work_flex(group_id, shift_type)
                alt_text = f"📋 Checklist ca {shift_type} & Việc giao thêm"
            else:
                flex_content = generate_checklist_flex(group_id, shift_type)
                alt_text = f"Checklist công việc ca {shift_type}"
            
            mark_shift_broadcast_sent(group_id, shift_type)
            if flex_content:
                message = FlexSendMessage(alt_text=alt_text, contents=flex_content)
                line_bot_api.reply_message(event.reply_token, message)
            else:
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"Không thể tạo checklist cho ca {shift_type}."))

        except Exception as e:
            print(f"Lỗi khi xử lý lệnh checklist '{shift_type}': {e}")
        return

    # === DMX SAVICO CODES: LK1, NV1, RT1 & CAO ===
    if user_msg_upper in ['LK1', 'LK', 'LK 1']:
        try:
            flex_msg = build_luyke_flex()
        except Exception as e:
            print(f"Lỗi khởi tạo báo cáo lũy kế: {e}")
            line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"Lỗi tạo báo cáo lũy kế: {str(e)}"))
            return
            
        try:
            if isinstance(flex_msg, list):
                carousel_content = {"type": "carousel", "contents": flex_msg}
                reply_with_auto_shift(event.reply_token, FlexSendMessage(alt_text="📊 BÁO CÁO LŨY KẾ (Cuộn Ngang P.1 & P.2)", contents=carousel_content), group_id=event_group_id)
            else:
                reply_with_auto_shift(event.reply_token, FlexSendMessage(alt_text="Báo Cáo Lũy Kế Savico", contents=flex_msg), group_id=event_group_id)
        except Exception as e:
            # Mục 5: Tuyệt đối KHÔNG gọi push_message báo lỗi làm tốn quota nhóm
            print(f"Lỗi gửi Flex LK1 (không push dự phòng): {e}")
        return

    if user_msg_upper in ['#LENH', '#LỆNH', 'HELP', '#HELP', 'MENU', '#CUPHAP', 'CÚ PHÁP', 'CUPHAP']:
        try:
            help_bubble = build_help_commands_flex()
            reply_with_auto_shift(
                event.reply_token,
                FlexSendMessage(alt_text="📖 Danh Sách Câu Lệnh Hỗ Trợ", contents=help_bubble),
                group_id=event_group_id
            )
        except Exception as e:
            print(f"Lỗi gửi bảng lệnh trợ giúp: {e}")
            try:
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"Lỗi gửi trợ giúp: {str(e)}"))
            except Exception as pe:
                print(f"Lỗi gửi reply dự phòng: {pe}")
        return

    cmd_clean = user_message.lower().replace(" ", "")
    has_nv_search_query = (
        user_msg_upper.startswith('NV ') and len(user_message[3:].strip()) > 0
    ) or (
        user_msg_upper.startswith(('NV:', 'NV0 ', 'NV1 ')) and len(user_message[4:].strip()) > 0
    )
    is_nv_kpi_cmd = (
        user_msg_upper in ['NV0', 'NV1', 'NV2'] or 
        cmd_clean in ['nv0', 'nv1', 'nv2'] or 
        user_msg_upper.startswith(('NV0 ', 'NV1 ', 'NV2 ', 'NV:')) or
        has_nv_search_query
    ) and not re.match(r"^(NV|PG)([2-8])$", user_msg_upper)

    if is_nv_kpi_cmd:
        try:
            flex_bubbles = build_nhanvien_flex()
        except Exception as e:
            print(f"Lỗi khởi tạo báo cáo nhân viên: {e}")
            try:
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"Lỗi tạo báo cáo nhân viên: {str(e)}"))
            except Exception as pe:
                print(f"Lỗi gửi reply dự phòng: {pe}")
            return
            
        try:
            if not isinstance(flex_bubbles, list):
                flex_bubbles = [flex_bubbles]

            overview_bubble = flex_bubbles[0]
            staff_bubbles = flex_bubbles[1:]

            if cmd_clean == 'nv0':
                overview_msg = FlexSendMessage(alt_text="🏆 Bảng Xếp Hạng Doanh Thu NV", contents=overview_bubble)
                top_staff = staff_bubbles[:6]
                reply_msgs = [overview_msg]
                if top_staff:
                    for i in range(0, len(top_staff), 2):
                        chunk = top_staff[i:i+2]
                        reply_msgs.append(FlexSendMessage(
                            alt_text=f"🎴 Thẻ KPI Nhân Viên (#{i+1}-#{i+len(chunk)})",
                            contents={"type": "carousel", "contents": chunk}
                        ))
                reply_with_auto_shift(event.reply_token, reply_msgs[:5], group_id=event_group_id)
                return

            elif cmd_clean == 'nv1':
                rem_staff = staff_bubbles[6:]
                if not rem_staff:
                    reply_with_auto_shift(
                        event.reply_token,
                        TextSendMessage(text="✅ Tất cả nhân viên đã được hiển thị trọn vẹn trong bảng xếp hạng NV0."),
                        group_id=event_group_id
                    )
                else:
                    carousel_msgs = []
                    for i in range(0, len(rem_staff), 2):
                        chunk = rem_staff[i:i+2]
                        if chunk:
                            carousel_msgs.append(FlexSendMessage(
                                alt_text=f"🎴 Thẻ KPI Nhân Viên (#{i+7}-#{i+6+len(chunk)})",
                                contents={"type": "carousel", "contents": chunk}
                            ))
                    reply_with_auto_shift(event.reply_token, carousel_msgs[:5], group_id=event_group_id)
                return

            query_param = ""
            if user_msg_upper.startswith('NV '):
                query_param = user_message[3:].strip()
            elif user_msg_upper.startswith(('NV:', 'NV1 ', 'NV0 ', 'NV2 ')):
                query_param = user_message[4:].strip()

            if query_param:
                raw_queries = [q.strip() for q in query_param.split(',') if q.strip()]
                matched_bubbles = []

                for q in raw_queries:
                    qu = q.upper()
                    m_b = None
                    for s_b in staff_bubbles:
                        if qu in json.dumps(s_b).upper():
                            m_b = s_b
                            break

                    if not m_b and qu.isdigit():
                        rank_idx = int(qu)
                        if 1 <= rank_idx <= len(staff_bubbles):
                            m_b = staff_bubbles[rank_idx - 1]

                    if m_b and m_b not in matched_bubbles:
                        matched_bubbles.append(m_b)

                if matched_bubbles:
                    if len(matched_bubbles) == 1:
                        reply_with_auto_shift(
                            event.reply_token,
                            FlexSendMessage(alt_text=f"🎴 Thẻ KPI Nhân Viên: {query_param}", contents=matched_bubbles[0]),
                            group_id=event_group_id
                        )
                    else:
                        matched_carousels = []
                        for i in range(0, len(matched_bubbles), 2):
                            chunk = matched_bubbles[i:i+2]
                            matched_carousels.append(FlexSendMessage(
                                alt_text=f"🎴 Thẻ KPI Nhân Viên ({i+1}-{i+len(chunk)})",
                                contents={"type": "carousel", "contents": chunk}
                            ))
                        reply_with_auto_shift(event.reply_token, matched_carousels[:5], group_id=event_group_id)
                else:
                    reply_with_auto_shift(
                        event.reply_token,
                        TextSendMessage(text=f"🔍 Không tìm thấy nhân viên với mã/tên: '{query_param}'. Vui lòng thử lại với Mã User (VD: nv 61169)."),
                        group_id=event_group_id
                    )

        except Exception as e:
            # Mục 5: Tuyệt đối KHÔNG gọi push_message báo lỗi làm tốn quota nhóm
            print(f"Lỗi gửi Flex NV (không push dự phòng): {e}")
        return

    if user_msg_upper in ['RT1', 'RT2', 'NHOM', 'NHOM2'] or user_msg_upper.startswith(('RT2 ', 'NHOM ')):
        try:
            rt_messages, rt_info = build_realtime_messages(max_messages=LINE_MAX_MESSAGES)
        except Exception as e:
            print(f"Lỗi khởi tạo báo cáo realtime: {e}")
            try:
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"Lỗi tạo báo cáo realtime: {str(e)}"))
            except Exception as pe:
                print(f"Lỗi gửi reply dự phòng: {pe}")
            return

        try:
            if not rt_messages:
                line_bot_api.reply_message(event.reply_token, TextSendMessage(
                    text="⚠️ Báo cáo realtime hôm nay quá lớn để gói gọn trong 5 tin nhắn.\n"
                         "Vui lòng xem chi tiết trên dashboard HTML."
                ))
                return

            reply_msgs = [FlexSendMessage(alt_text=m["alt_text"], contents=m["contents"])
                          for m in rt_messages]
            reply_with_auto_shift(event.reply_token, reply_msgs[:LINE_MAX_MESSAGES], group_id=event_group_id)
        except Exception as e:
            print(f"Lỗi gửi Flex RT1 (không push dự phòng): {e}")
        return

    if user_msg_upper in ['RTNV', 'RT NV', 'CTNV', 'CHITIETNV', 'RT1NV', 'RT1_DETAIL']:
        try:
            nv_messages, nv_info = build_realtime_staff_detail_messages(max_messages=LINE_MAX_MESSAGES)
        except Exception as e:
            print(f"Lỗi khởi tạo chi tiết nhân viên realtime: {e}")
            try:
                line_bot_api.reply_message(event.reply_token, TextSendMessage(text=f"Lỗi tạo chi tiết nhân viên: {str(e)}"))
            except Exception as pe:
                print(f"Lỗi gửi reply dự phòng: {pe}")
            return

        try:
            if not nv_messages:
                line_bot_api.reply_message(event.reply_token, TextSendMessage(
                    text="⚠️ Hôm nay chưa có nhân viên nào phát sinh doanh thu ngành hàng."
                ))
                return

            reply_msgs = [FlexSendMessage(alt_text=m["alt_text"], contents=m["contents"])
                          for m in nv_messages]
            reply_with_auto_shift(event.reply_token, reply_msgs[:LINE_MAX_MESSAGES], group_id=event_group_id)
        except Exception as e:
            print(f"Lỗi gửi Flex RTNV: {e}")
        return

    # Mục 3: Lệnh cào dữ liệu RT / CAO — KHÔNG DÙNG push_message (Tiết kiệm 100% Push Quota)
    if user_msg_upper in ['RT', 'CAO'] or user_msg_upper.startswith(('RT ', 'CAO ')):
        scrape_type = "realtime"
        if "LK" in user_msg_upper or "LUY" in user_msg_upper:
            scrape_type = "luyke"
            
        try:
            trigger_success, req_time = trigger_adhoc_scrape(scrape_type)
            if not trigger_success:
                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage(text="⚠️ Không thể gửi tín hiệu cào dữ liệu lên Supabase. Vui lòng thử lại.")
                )
                return

            completed = False
            saw_running = False
            req_prefix = req_time[:16] if req_time else ""

            for _ in range(7):
                time.sleep(3)
                st_info = check_scrape_status()
                sig_status = st_info.get("status")
                sig_req = st_info.get("requested_at", "")

                if sig_status == "running":
                    saw_running = True
                elif sig_status == "completed":
                    if saw_running or not sig_req or sig_req >= req_prefix:
                        completed = True
                        break

            if completed:
                if scrape_type == "luyke":
                    flex_msg = build_luyke_flex()
                    if isinstance(flex_msg, list):
                        carousel_content = {"type": "carousel", "contents": flex_msg}
                        reply_with_auto_shift(
                            event.reply_token,
                            FlexSendMessage(alt_text="📊 BÁO CÁO LŨY KẾ (Cuộn Ngang P.1 & P.2)", contents=carousel_content),
                            group_id=event_group_id
                        )
                    else:
                        reply_with_auto_shift(
                            event.reply_token,
                            FlexSendMessage(alt_text="Báo Cáo Lũy Kế Savico", contents=flex_msg),
                            group_id=event_group_id
                        )
                else:
                    rt_messages, _ = build_realtime_messages(max_messages=LINE_MAX_MESSAGES)
                    if rt_messages:
                        reply_msgs = [FlexSendMessage(alt_text=m["alt_text"], contents=m["contents"]) for m in rt_messages]
                        reply_with_auto_shift(event.reply_token, reply_msgs[:LINE_MAX_MESSAGES], group_id=event_group_id)
                    else:
                        line_bot_api.reply_message(
                            event.reply_token,
                            TextSendMessage(text="⚠️ Báo cáo realtime quá lớn để gói gọn trong 5 tin nhắn. Vui lòng xem dashboard HTML.")
                        )
            else:
                pending_flex = build_scrape_pending_flex(scrape_type, req_time)
                reply_with_auto_shift(
                    event.reply_token,
                    FlexSendMessage(alt_text="⏳ Đang đồng bộ báo cáo BI...", contents=pending_flex),
                    group_id=event_group_id
                )
        except Exception as e:
            print(f"Lỗi xử lý tín hiệu {user_msg_upper}: {e}")
        return

    # 7. Lịch làm việc (NV/PG)
    schedule_match = re.match(r"^(NV|PG)([2-8])$", user_msg_upper)
    if schedule_match:
        schedule_type_cmd = schedule_match.group(1)
        day_number = int(schedule_match.group(2))
        schedule_type = 'employee' if schedule_type_cmd == 'NV' else 'pg'
        days_map = {2: "Thứ Hai", 3: "Thứ Ba", 4: "Thứ Tư", 5: "Thứ Năm", 6: "Thứ Sáu", 7: "Thứ Bảy", 8: "Chủ Nhật"}
        day_str = days_map.get(day_number)
        try:
            msg = send_daily_schedule(schedule_type, day_of_week_str=day_str, return_msg_only=True)
            if msg:
                reply_with_auto_shift(event.reply_token, msg, group_id=event_group_id)
        except Exception as e:
            print(f"Error schedule: {e}")
        return

    if user_msg_upper in ['NV', 'PG']:
        schedule_type = 'employee' if user_msg_upper == 'NV' else 'pg'
        try:
            msg = send_daily_schedule(schedule_type, return_msg_only=True)
            if msg:
                reply_with_auto_shift(event.reply_token, msg, group_id=event_group_id)
        except Exception as e:
            print(f"Error schedule: {e}")
        return
        
    # 8. Báo cáo (ST, BXH)
    try:
        all_data = get_cached_values(WORKSHEET_NAME, ttl=60)
        reply_messages = []
        if all_data and len(all_data) > 1:
            cluster_names = {row[0].strip().upper() for row in all_data[1:] if len(row) > 0 and row[0]}
            header_row = all_data[0]
            
            if user_msg_upper.startswith('ST '):
                supermarket_code = user_message[3:].strip().upper()
                found_row = next((row for row in all_data[1:] if row and len(row) > 2 and row[2] and row[2].strip().split(' ')[0] == supermarket_code), None)
                if found_row:
                    ranking = calculate_ranking(all_data, found_row)
                    competition_results = parse_competition_data(header_row, found_row)
                    reply_messages.append(FlexSendMessage(alt_text=f'Báo cáo ST {supermarket_code}', contents=create_flex_message(found_row, competition_results, ranking)['contents']))
                    cluster_name = (found_row[0] or "").strip().upper()
                    store_channel = (found_row[1] or "").strip()
                    if cluster_name in cluster_names:
                        for flex_data in create_leaderboard_flex_message(all_data, cluster_name=cluster_name, channel_filter=store_channel):
                            reply_messages.append(FlexSendMessage(alt_text=flex_data['altText'], contents=flex_data['contents']))
                else:
                    reply_messages.append(TextSendMessage(text=f'Không tìm thấy dữ liệu cho mã siêu thị: {supermarket_code}'))

            elif user_msg_upper == 'BXH':
                for flex_data in create_leaderboard_flex_message(all_data):
                    reply_messages.append(FlexSendMessage(alt_text=flex_data['altText'], contents=flex_data['contents']))
            
            elif user_msg_upper == 'BXH1':
                for flex_data in create_leaderboard_flex_message(all_data, channel_filter='dmx'):
                    reply_messages.append(FlexSendMessage(alt_text=flex_data['altText'], contents=flex_data['contents']))

            elif user_msg_upper == 'BXH2':
                for flex_data in create_leaderboard_flex_message(all_data, channel_filter='tgdd'):
                    reply_messages.append(FlexSendMessage(alt_text=flex_data['altText'], contents=flex_data['contents']))
            
            else:
                parts = user_message.split()
                if len(parts) == 2 and parts[0].upper() in cluster_names:
                    cluster_name_cmd = parts[0].upper()
                    channel_choice = parts[1]
                    channel_filter = None
                    if channel_choice == '1':
                        channel_filter = 'dmx'
                    elif channel_choice == '2':
                        channel_filter = 'tgdd'
                    
                    if channel_filter:
                        bxh_messages = create_leaderboard_flex_message(all_data, cluster_name=cluster_name_cmd, channel_filter=channel_filter)
                        if not bxh_messages:
                            reply_messages.append(TextSendMessage(text=f"Không có dữ liệu cho kênh bạn chọn trong cụm {cluster_name_cmd}."))
                        else:
                            for flex_data in bxh_messages:
                                reply_messages.append(FlexSendMessage(alt_text=flex_data['altText'], contents=flex_data['contents']))

                elif user_msg_upper in cluster_names:
                    for flex_data in create_leaderboard_flex_message(all_data, cluster_name=user_msg_upper):
                        reply_messages.append(FlexSendMessage(alt_text=flex_data['altText'], contents=flex_data['contents']))
                
                else:
                    found_row = next((row for row in all_data[1:] if row and len(row) > 2 and row[2] and row[2].strip().split(' ')[0] == user_msg_upper), None)
                    if found_row:
                        ranking = calculate_ranking(all_data, found_row)
                        competition_results = parse_competition_data(header_row, found_row)
                        reply_messages.append(FlexSendMessage(alt_text='Báo cáo Realtime', contents=create_flex_message(found_row, competition_results, ranking)['contents']))
                        summary_message = create_summary_text_message(found_row, competition_results)
                        if summary_message:
                            reply_messages.append(summary_message)
        
        if reply_messages:
            reply_with_auto_shift(event.reply_token, reply_messages, group_id=event_group_id)
            return

    except Exception as e:
        print(f"!!! GẶP LỖI NGHIÊM TRỌNG KHI XỬ LÝ BÁO CÁO: {repr(e)}")

    # 9. Mục 1: Nếu đây là tin nhắn chat thông thường trong nhóm và ca hiện tại chưa được phát
    #    Lịch/Checklist -> Dùng ngay reply_token miễn phí của tin nhắn này để phát mở ca!
    if event_group_id:
        try:
            pending_msgs, pending_shift = get_pending_auto_shift_messages(event_group_id)
            if pending_msgs:
                mark_shift_broadcast_sent(event_group_id, pending_shift)
                line_bot_api.reply_message(event.reply_token, pending_msgs[:LINE_MAX_MESSAGES])
                print(f"[AUTO-TRIGGER] Phát mở ca '{pending_shift}' miễn phí qua tin nhắn đầu ca tại nhóm {event_group_id}.")
        except Exception as e_auto:
            print(f"Lỗi auto-trigger cuối handle_message: {e_auto}")


# ============================================================================
# ENDPOINTS CRON JOB (ĐÃ NÂNG CẤP TIẾT KIỆM QUOTA PUSH TỐI ĐA — MỤC 1, 2, 4)
# ============================================================================

@app.route("/trigger-morning-tasks", methods=['POST'])
def trigger_morning_tasks():
    incoming_secret = request.headers.get('X-Cron-Secret')
    if not CRON_SECRET_KEY or incoming_secret != CRON_SECRET_KEY:
        abort(403)
    
    print("Cron Job: Bắt đầu tác vụ buổi sáng (Chế độ Tiết Kiệm Quota Push)...")
    try:
        pg_group_id = (os.environ.get('PG_GROUP_ID') or '').strip()
        employee_group_id = (os.environ.get('EMPLOYEE_GROUP_ID') or MAIN_CHECKLIST_GROUP_ID or '').strip()
        force_group_push = (os.environ.get('FORCE_GROUP_PUSH') or '0').strip() == '1'

        # 1. Khởi tạo sẵn dữ liệu công việc ca sáng trong RAM & Sheet
        if employee_group_id:
            initialize_daily_tasks(employee_group_id, 'sang')

        # 2. Kiểm tra xem nhóm đã được Auto-Trigger phát miễn phí qua reply_token chưa
        if employee_group_id and is_shift_broadcast_sent(employee_group_id, 'sang'):
            print("Ca sáng đã được phát miễn phí vào nhóm qua Auto-Trigger reply_token. Bỏ qua Push.")
            return "OK (Already sent via free reply_token)", 200

        # 3. Chuẩn bị danh sách tin nhắn buổi sáng
        messages_to_send = build_shift_broadcast_messages(employee_group_id, 'sang')

        if force_group_push:
            if pg_group_id and pg_group_id != employee_group_id and not is_shift_broadcast_sent(pg_group_id, 'sang'):
                msg_pg = send_daily_schedule('pg', return_msg_only=True)
                if msg_pg:
                    line_bot_api.push_message(pg_group_id, msg_pg)
                    mark_shift_broadcast_sent(pg_group_id, 'sang')
            if messages_to_send and employee_group_id:
                line_bot_api.push_message(employee_group_id, messages_to_send[:5])
                mark_shift_broadcast_sent(employee_group_id, 'sang')
            return "OK (Group Push)", 200

        # 4. Mục 2: Chế độ Tiết Kiệm (Mặc định) — Chỉ Push 1-1 cho ADMIN_USER_ID (tốn đúng 1 tin thay vì 25-40 tin!)
        if ADMIN_USER_ID and messages_to_send:
            notice = TextSendMessage(
                text="☀️ [BÁO CÁO MỞ CA SÁNG]\n"
                     "Dữ liệu Lịch & Checklist Ca Sáng đã sẵn sàng.\n"
                     "💡 Nhóm chat sẽ TỰ ĐỘNG hiện bảng này (miễn phí 0 tốn Push) ngay khi có tin nhắn đầu tiên trong nhóm, hoặc anh gõ `mở ca` trong nhóm."
            )
            admin_payload = ([notice] + messages_to_send)[:LINE_MAX_MESSAGES]
            line_bot_api.push_message(ADMIN_USER_ID, admin_payload)
            print("Đã gửi 1 tin Push 1-1 cho ADMIN_USER_ID (tiết kiệm 96% quota Push nhóm).")

        return "OK", 200
    except Exception as e:
        print(f"Lỗi khi chạy tác vụ buổi sáng: {e}")
        return "Error", 500


@app.route("/trigger-afternoon-tasks", methods=['POST'])
def trigger_afternoon_tasks():
    incoming_secret = request.headers.get('X-Cron-Secret')
    if not CRON_SECRET_KEY or incoming_secret != CRON_SECRET_KEY:
        abort(403)
    
    print("Cron Job: Bắt đầu tác vụ buổi chiều (Chế độ Tiết Kiệm Quota Push - Không gửi lặp Lịch)...")
    try:
        employee_group_id = (os.environ.get('EMPLOYEE_GROUP_ID') or MAIN_CHECKLIST_GROUP_ID or '').strip()
        force_group_push = (os.environ.get('FORCE_GROUP_PUSH') or '0').strip() == '1'

        # 1. Khởi tạo sẵn dữ liệu công việc ca chiều trong RAM & Sheet
        if employee_group_id:
            initialize_daily_tasks(employee_group_id, 'chieu')

        # 2. Kiểm tra xem nhóm đã được Auto-Trigger phát miễn phí qua reply_token chưa
        if employee_group_id and is_shift_broadcast_sent(employee_group_id, 'chieu'):
            print("Ca chiều đã được phát miễn phí vào nhóm qua Auto-Trigger reply_token. Bỏ qua Push.")
            return "OK (Already sent via free reply_token)", 200

        # 3. Mục 4: Chỉ lấy Checklist Ca Chiều (cắt bỏ việc gửi lặp Lịch NV & PG buổi chiều)
        messages_to_send = build_shift_broadcast_messages(employee_group_id, 'chieu')

        if force_group_push:
            if messages_to_send and employee_group_id:
                line_bot_api.push_message(employee_group_id, messages_to_send[:5])
                mark_shift_broadcast_sent(employee_group_id, 'chieu')
            return "OK (Group Push)", 200

        # 4. Mục 2: Chỉ Push 1-1 cho ADMIN_USER_ID (tốn đúng 1 tin thay vì 25-40 tin)
        if ADMIN_USER_ID and messages_to_send:
            notice = TextSendMessage(
                text="🌙 [BÁO CÁO MỞ CA CHIỀU]\n"
                     "Checklist Kho Ca Chiều đã sẵn sàng.\n"
                     "💡 Nhóm chat sẽ TỰ ĐỘNG hiện Checklist Chiều (miễn phí 0 tốn Push) ngay khi có tin nhắn tiếp theo trong nhóm, hoặc anh gõ `mở ca` trong nhóm."
            )
            admin_payload = ([notice] + messages_to_send)[:LINE_MAX_MESSAGES]
            line_bot_api.push_message(ADMIN_USER_ID, admin_payload)
            print("Đã gửi 1 tin Push 1-1 ca chiều cho ADMIN_USER_ID (tiết kiệm 96% quota Push nhóm).")

        return "OK", 200
    except Exception as e:
        print(f"Lỗi khi chạy tác vụ buổi chiều: {e}")
        return "Error", 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)