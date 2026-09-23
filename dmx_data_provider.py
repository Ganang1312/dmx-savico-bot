import os
import requests
import json
from datetime import datetime
import pytz

# Đọc từ biến môi trường — KHÔNG hardcode khoá trong mã nguồn.
# Đặt trên Render: SUPABASE_URL và SUPABASE_KEY.
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://uybcglehwheygxmzlwbq.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_KEY:
    raise ValueError(
        "Lỗi: Biến môi trường SUPABASE_KEY chưa được thiết lập. "
        "Đặt khoá đọc Supabase trong biến môi trường, không ghi vào mã nguồn."
    )


def _auth_headers(extra=None):
    """Header xác thực Supabase dùng chung, tránh lặp lại ở từng hàm."""
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Cache-Control": "no-cache",
    }
    if extra:
        headers.update(extra)
    return headers

def get_dashboard_data(sheets_str):
    """
    Truy vấn trực tiếp Supabase REST API để lấy dữ liệu mới nhất (bỏ qua Proxy GAS cũ)
    """
    sheet_names = [s.strip() for s in sheets_str.split(',') if s.strip()]
    sheet_names_str = ",".join(f'"{s}"' for s in sheet_names)
    url = f"{SUPABASE_URL}/rest/v1/sheet_data?sheet_name=in.({sheet_names_str})"
    
    headers = _auth_headers()
    
    result = {name: [] for name in sheet_names}
    
    try:
        res = requests.get(url, headers=headers, timeout=12)
        if res.status_code == 200:
            rows = res.json()
            for row in rows:
                s_name = row.get("sheet_name")
                if s_name in result:
                    result[s_name] = row.get("data", [])
    except Exception as e:
        print(f"Lỗi truy vấn dữ liệu trực tiếp từ Supabase: {e}")
        
    return result

def _get_active_area_id():
    """
    Trả về areaId CHỈ khi anh Dương chủ động đặt biến môi trường AREA_ID, ngược lại None.

    LÝ DO KHÔNG CÒN TỰ ĐOÁN AREA (chốt 23/09/2026, kiểm chứng bằng dữ liệu thật):
      - Bảng `sheet_data` KHÔNG có cột area_id — area chỉ nằm trong TÊN bản khoá
        `Target_Lock_<area>_<tháng>_<timestamp>`.
      - `cloud_config.area_id` từng là 88369 trong khi area đang dùng là 94066
        (cũ từ 29/07/2026) ⇒ tin vào nó là SAI, đã bỏ không đọc nữa.
      - area_id THAY ĐỔI THEO THỜI GIAN: 2026-07 là 88369, 2026-08 và 2026-09 là 94066.
        Gán cứng 1 giá trị (kể cả đặt env trên Render) sẽ sai khi area đổi tháng.
      - Quan trọng nhất: kiểm tra 13 bản khoá thật thì **MỖI THÁNG CHỈ CÓ ĐÚNG 1 AREA**
        sinh bản khoá target ⇒ "lấy bản khoá MỚI NHẤT của tháng" cho kết quả
        **Y HỆT** "lọc đúng area", mà không cần biết area là bao nhiêu.

    Vì vậy AREA_ID chỉ còn là nút ghi đè thủ công, dùng khi tương lai có lúc
    nhiều area cùng khoá trong một tháng. Bình thường KHÔNG CẦN đặt.
    """
    env_area = os.environ.get("AREA_ID")
    if env_area and str(env_area).strip():
        return str(env_area).strip()
    return None


def get_locked_target_config():
    """
    Lấy cấu hình Target khoá cho THÁNG hiện tại.

    ĐƯỜNG CHÍNH (cách 2): lấy bản khoá MỚI NHẤT của tháng, không cần biết area.
    Đã kiểm chứng trên 13 bản khoá thật: mỗi tháng chỉ có ĐÚNG 1 area sinh bản khoá
    target (2026-07 -> 88369 · 2026-08, 2026-09 -> 94066), nên cách này cho kết quả
    Y HỆT cách lọc theo area. Ưu điểm lớn: area_id có đổi tháng nào thì bot vẫn tự
    đúng, không phải sửa biến môi trường.

    ĐƯỜNG GHI ĐÈ: nếu có đặt biến môi trường AREA_ID thì ưu tiên đúng area đó
    (chỉ cần khi tương lai có nhiều area cùng khoá trong một tháng).
    """
    tz = pytz.timezone('Asia/Ho_Chi_Minh')
    now = datetime.now(tz)
    month_str = now.strftime("%Y-%m")
    headers = _auth_headers()

    def doc_ban_khoa(url, mo_ta):
        try:
            res = requests.get(url, headers=headers, timeout=8)
            if res.status_code == 200:
                rows = res.json()
                if rows:
                    data = rows[0].get("data", {})
                    if isinstance(data, dict) and data.get("is_locked"):
                        print(f"Target_Lock: dùng {rows[0].get('sheet_name')} ({mo_ta}).")
                        return data
        except Exception as e:
            print(f"Lỗi truy vấn Target_Lock ({mo_ta}): {e}")
        return None

    # 1. Chỉ khi anh Dương chủ động đặt AREA_ID thì mới ưu tiên đúng area đó
    area_id = _get_active_area_id()
    if area_id:
        url_area = (f"{SUPABASE_URL}/rest/v1/sheet_data"
                    f"?sheet_name=ilike.Target_Lock_{area_id}_{month_str}%25"
                    f"&order=updated_at.desc&limit=1")
        cfg = doc_ban_khoa(url_area, f"đúng area {area_id}")
        if cfg:
            return cfg
        print(f"Lưu ý: area {area_id} chưa có bản khoá target tháng {month_str} "
              f"— lùi về bản khoá mới nhất của tháng.")

    # 2. Mặc định: bản khoá MỚI NHẤT của tháng (mọi area)
    url = (f"{SUPABASE_URL}/rest/v1/sheet_data"
           f"?sheet_name=like.Target_Lock_%25_{month_str}%25&order=updated_at.desc&limit=1")
    return doc_ban_khoa(url, "bản khoá mới nhất của tháng")

def get_staff_history_base():
    """
    Lấy dữ liệu lịch sử mốc so sánh (base) để tính target hybrid và thi đua.

    Điều kiện chọn ngày giống hệt baocao_nhanvien.html: mốc chỉ dùng được khi có ĐỦ
    cả Data_NV_BI (mốc doanh thu) LẪN Data_NV_ThiDua (mốc ngành hàng). Thiếu 1 trong 2
    thì target hybrid tính sai. (Trước đây bot chỉ kiểm tra Data_NV_ThiDua.)
    """
    headers = _auth_headers()
    tz = pytz.timezone('Asia/Ho_Chi_Minh')
    today_ymd = datetime.now(tz).strftime("%Y-%m-%d")
    
    try:
        url_dates = f"{SUPABASE_URL}/rest/v1/history_db?select=date&order=date.desc"
        res = requests.get(url_dates, headers=headers, timeout=6)
        if res.status_code != 200:
            return {}, None
            
        all_dates = []
        for r in res.json():
            d = r.get("date")
            if d and d not in all_dates:
                all_dates.append(d)
                
        if not all_dates:
            return {}, None
            
        start_idx = 1 if (len(all_dates) > 1 and all_dates[0] == today_ymd) else 0
        
        for candidate_date in all_dates[start_idx:]:
            url_check = f"{SUPABASE_URL}/rest/v1/history_db?date=eq.{candidate_date}&sheet_name=in.(Data_NV_BI,Data_NV_ThiDua)&select=sheet_name,data"
            res_c = requests.get(url_check, headers=headers, timeout=6)
            if res_c.status_code == 200:
                h_sheets = {item["sheet_name"]: item["data"] for item in res_c.json()}
                has_bi = bool(h_sheets.get("Data_NV_BI") and len(h_sheets["Data_NV_BI"]) > 0)
                has_td = bool(h_sheets.get("Data_NV_ThiDua") and len(h_sheets["Data_NV_ThiDua"]) > 0)
                if has_bi and has_td:
                    return h_sheets, candidate_date
    except Exception as e:
        print(f"Lỗi lấy dữ liệu lịch sử HCR: {e}")
    return {}, None


def trigger_adhoc_scrape(scrape_type):
    """
    Gửi tín hiệu cào dữ liệu mới lên Supabase để Chrome Extension phát hiện
    """
    now_utc = datetime.now(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    try:
        url = f"{SUPABASE_URL}/rest/v1/sheet_data"
        headers = _auth_headers({
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates",
        })
        payload = {
            "sheet_name": "scrape_signals",
            "data": {
                "status": "pending",
                "type": scrape_type,
                "requested_at": now_utc
            },
            "updated_at": now_utc
        }
        res = requests.post(url, headers=headers, json=payload, timeout=10)
        if res.status_code in [200, 201]:
            return True, now_utc
    except Exception as e:
        print(f"Error posting scrape signal: {e}")
    return False, now_utc

def check_scrape_status():
    """
    Kiểm tra chi tiết trạng thái cào (gồm status, requested_at, type)
    """
    try:
        import time
        timestamp = int(time.time() * 1000)
        url = f"{SUPABASE_URL}/rest/v1/sheet_data?sheet_name=in.(scrape_signals,cb_{timestamp})&select=data"
        headers = _auth_headers({"Pragma": "no-cache"})
        res = requests.get(url, headers=headers, timeout=10)
        if res.status_code == 200:
            rows = res.json()
            if rows and len(rows) > 0:
                return rows[0].get("data", {})
    except Exception as e:
        print(f"Error checking scrape status: {e}")
    return {}
