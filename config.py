import os
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --- PHẦN CẤU HÌNH GOOGLE SHEETS DÙNG CHUNG ---

# Đọc credentials từ biến môi trường
GOOGLE_CREDS_JSON = os.environ.get('GOOGLE_CREDENTIALS_JSON')
if not GOOGLE_CREDS_JSON:
    raise ValueError("Lỗi: Biến môi trường GOOGLE_CREDENTIALS_JSON không được thiết lập.")

# Xác thực với Google
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
google_creds_dict = json.loads(GOOGLE_CREDS_JSON)
CREDS = ServiceAccountCredentials.from_json_keyfile_dict(google_creds_dict, SCOPE)
CLIENT = gspread.authorize(CREDS)

# Tên các trang tính dùng chung
SHEET_NAME = 'DATA REATIME'
WORKSHEET_NAME_USERS = 'allowed_users'
WORKSHEET_TRACKER_NAME = 'task_tracker'
# SỬA LỖI: Bổ sung dòng còn thiếu dưới đây
WORKSHEET_NAME = 'chi_tiet_cum'
