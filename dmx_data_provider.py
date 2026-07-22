import requests
import json
from datetime import datetime
import pytz

SUPABASE_URL = "https://uybcglehwheygxmzlwbq.supabase.co"
SUPABASE_KEY = "sb_publishable_tb1cO9NPuNC1cdA-pt_NNQ_1n5I9IkU"

def get_dashboard_data(sheets_str):
    """
    Truy vấn trực tiếp Supabase REST API để lấy dữ liệu mới nhất (bỏ qua Proxy GAS cũ)
    """
    sheet_names = [s.strip() for s in sheets_str.split(',') if s.strip()]
    sheet_names_str = ",".join(f'"{s}"' for s in sheet_names)
    url = f"{SUPABASE_URL}/rest/v1/sheet_data?sheet_name=in.({sheet_names_str})"
    
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Cache-Control": "no-cache"
    }
    
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

def get_locked_target_config():
    """
    Lấy cấu hình Target khóa (được lưu từ web app baocao_nhanvien theo tỷ lệ 60-40 hoặc mode chọn)
    """
    tz = pytz.timezone('Asia/Ho_Chi_Minh')
    now = datetime.now(tz)
    month_str = now.strftime("%Y-%m")
    
    # Tìm Target_Lock mới nhất theo tháng (hỗ trợ mọi areaId)
    url = f"{SUPABASE_URL}/rest/v1/sheet_data?sheet_name=like.Target_Lock_%25_{month_str}%25&order=updated_at.desc&limit=1"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Cache-Control": "no-cache"
    }
    try:
        res = requests.get(url, headers=headers, timeout=6)
        if res.status_code == 200:
            rows = res.json()
            if rows and len(rows) > 0:
                data = rows[0].get("data", {})
                if isinstance(data, dict) and data.get("is_locked"):
                    return data
    except Exception as e:
        print(f"Lỗi truy vấn Target_Lock từ Supabase: {e}")
    return None

def trigger_adhoc_scrape(scrape_type):
    """
    Gửi tín hiệu cào dữ liệu mới lên Supabase để Chrome Extension phát hiện
    """
    now_utc = datetime.now(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    try:
        url = f"{SUPABASE_URL}/rest/v1/sheet_data"
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates"
        }
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
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache"
        }
        res = requests.get(url, headers=headers, timeout=10)
        if res.status_code == 200:
            rows = res.json()
            if rows and len(rows) > 0:
                return rows[0].get("data", {})
    except Exception as e:
        print(f"Error checking scrape status: {e}")
    return {}
