import os
from datetime import datetime
import pytz
from linebot import LineBotApi
from linebot.models import TextSendMessage
from linebot.exceptions import LineBotApiError

# Import từ file cấu hình trung tâm
from config import CLIENT, SHEET_NAME

# Khởi tạo LineBotApi
CHANNEL_ACCESS_TOKEN = os.environ.get('CHANNEL_ACCESS_TOKEN')
if not CHANNEL_ACCESS_TOKEN:
    raise ValueError("Lỗi: Biến môi trường CHANNEL_ACCESS_TOKEN không được thiết lập.")
line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)

WORKSHEET_THONGBAO_NAME = 'ThongBao'

def send_thongbao_messages():
    """
    Quét sheet 'ThongBao' và gửi tin nhắn nếu khớp thời gian và trạng thái active.
    """
    print("Đang quét sheet ThongBao để gửi tin nhắn...")
    try:
        sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_THONGBAO_NAME)
        records = sheet.get_all_records()
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        current_time = datetime.now(tz_vietnam).strftime('%H:%M')

        sent_count = 0
        for record in records:
            group_id = record.get('GroupID')
            content = record.get('Content')
            schedule_time = record.get('Time')
            status = record.get('Status')

            if not all([group_id, content, schedule_time, status]):
                continue

            if status == 'active' and schedule_time == current_time:
                try:
                    # Thay thế <br> bằng ký tự xuống dòng thật
                    formatted_content = content.replace('<br>', '\n')
                    line_bot_api.push_message(
                        str(group_id),
                        TextSendMessage(text=formatted_content)
                    )
                    print(f"Đã gửi thông báo đến Group ID: {group_id}")
                    sent_count += 1
                except LineBotApiError as e:
                    print(f"Lỗi API khi gửi đến Group ID {group_id}: {e.error.message}")
                except Exception as e:
                    print(f"Lỗi không xác định khi gửi đến Group ID {group_id}: {e}")
        
        if sent_count > 0:
            print(f"Hoàn tất. Đã gửi {sent_count} thông báo.")
        else:
            print("Không có thông báo nào cần gửi tại thời điểm này.")

    except gspread.exceptions.WorksheetNotFound:
        print(f"Lỗi: Không tìm thấy trang tính có tên '{WORKSHEET_THONGBAO_NAME}'")
    except Exception as e:
        print(f"Lỗi nghiêm trọng khi xử lý sheet ThongBao: {e}")