import requests
import json
from datetime import datetime

GAS_URL = "https://script.google.com/macros/s/AKfycbxjTGuxxRiX1zY78pRkZ55qhJQF5om1Vht_kC3exy5JsYVBwjH1u7G2crr6dwbFz0lj/exec"
SUPABASE_URL = "https://uybcglehwheygxmzlwbq.supabase.co"
SUPABASE_KEY = "sb_publishable_tb1cO9NPuNC1cdA-pt_NNQ_1n5I9IkU"

def get_dashboard_data(sheets_str):
    """
    Truy vấn Google Apps Script Web App để nhận cấu trúc dữ liệu JSON sạch của nhiều bảng
    """
    try:
        url = f"{GAS_URL}?sheets={sheets_str}&t={int(datetime.now().timestamp() * 1000)}"
        res = requests.get(url, timeout=15)
        if res.status_code == 200:
            return res.json()
    except Exception as e:
        print(f"Error fetching dashboard data: {e}")
    return {}

def trigger_adhoc_scrape(scrape_type):
    """
    Gửi tín hiệu cào dữ liệu mới lên Supabase để Chrome Extension phát hiện
    """
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
                "requested_at": datetime.now().isoformat()
            },
            "updated_at": datetime.now().isoformat()
        }
        res = requests.post(url, headers=headers, json=payload, timeout=10)
        return res.status_code in [200, 201]
    except Exception as e:
        print(f"Error posting scrape signal: {e}")
    return False

def check_scrape_status():
    """
    Kiểm tra trạng thái cào (pending -> running -> completed)
    """
    try:
        url = f"{SUPABASE_URL}/rest/v1/sheet_data?sheet_name=eq.scrape_signals&select=data"
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}"
        }
        res = requests.get(url, headers=headers, timeout=10)
        if res.status_code == 200:
            rows = res.json()
            if rows and len(rows) > 0:
                return rows[0].get("data", {}).get("status", "completed")
    except Exception as e:
        print(f"Error checking scrape status: {e}")
    return "completed"
