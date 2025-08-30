# flex_handler.py

from datetime import datetime
import pytz
# Thay đổi quan trọng: Import từ config.py
from config import CLIENT, SHEET_NAME, WORKSHEET_TRACKER_NAME

# --- Định nghĩa các công việc ---
TASKS = {
    'sang': [
        {'id': 'sang_1', 'name': 'Check lệnh chuyển kho online', 'time': '09:15'},
        {'id': 'sang_2', 'name': 'Check đơn GHTK chuyển kho', 'time': '09:30'},
        {'id': 'sang_3', 'name': 'Chạy tủ, thay giá (thứ 2 & 5)', 'time': '10:00'},
        {'id': 'sang_4', 'name': 'Rà soát tốc kệ', 'time': '10:30'},
        {'id': 'sang_5', 'name': 'Check Phiếu CK/NK quá 7 ngày', 'time': '11:30'},
        {'id': 'sang_6', 'name': 'Đổ tồn hàng T.Thái (lỗi)', 'time': '14:00'},
    ],
    'chieu': [
        {'id': 'chieu_1', 'name': 'Check lệnh online', 'time': '15:15'},
        {'id': 'chieu_2', 'name': 'Check đơn GHTK', 'time': '15:30'},
        {'id': 'chieu_3', 'name': 'Sắp xếp hàng hóa kho', 'time': '16:00'},
        {'id': 'chieu_4', 'name': 'Rà soát tốc kệ (gia dụng/tivi)', 'time': '16:30'},
        {'id': 'chieu_5', 'name': 'Xử lý BCNB chiều', 'time': '17:30'},
        {'id': 'chieu_6', 'name': 'Đổ tồn hàng T.Thái (lỗi)', 'time': '19:00'},
        {'id': 'chieu_7', 'name': 'Check GHTK / Grab', 'time': '21:00'},
        {'id': 'chieu_8', 'name': 'Up hình máy cũ / trưng bày', 'time': '21:30'},
    ]
}

def initialize_daily_tasks(group_id, shift_type):
    """
    Reset và khởi tạo lại danh sách công việc của một ca trong Google Sheet.
    """
    print(f"Bắt đầu reset và khởi tạo công việc ca {shift_type} cho group {group_id}...")
    try:
        sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_TRACKER_NAME)
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
        
        all_records = sheet.get_all_records()
        rows_to_delete = []
        for i, record in enumerate(all_records):
            task_id = record.get('task_id', '')
            if (str(record.get('group_id')) == group_id and
                record.get('date') == today_str and
                task_id.startswith(shift_type)):
                rows_to_delete.append(i + 2)

        if rows_to_delete:
            print(f"Tìm thấy {len(rows_to_delete)} công việc cũ, đang xóa...")
            for row_num in sorted(rows_to_delete, reverse=True):
                sheet.delete_rows(row_num)
        
        tasks_to_add = []
        for task in TASKS.get(shift_type, []):
            new_row = [group_id, today_str, task['id'], task['name'], task['time'], 'incomplete', '']
            tasks_to_add.append(new_row)

        if tasks_to_add:
            sheet.append_rows(tasks_to_add, value_input_option='USER_ENTERED')
            print(f"Đã khởi tạo mới {len(tasks_to_add)} công việc thành công.")
        return True
    except Exception as e:
        print(f"Lỗi khi khởi tạo công việc: {e}")
        return False

def get_tasks_status_from_sheet(group_id, shift_type):
    """
    Lấy trạng thái công việc hiện tại của nhóm từ Google Sheet.
    """
    try:
        sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_TRACKER_NAME)
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
        
        all_records = sheet.get_all_records()
        task_statuses = {}
        
        for record in all_records:
            if str(record['group_id']) == group_id and record['date'] == today_str:
                task_id = record['task_id']
                if task_id.startswith(shift_type):
                    task_statuses[task_id] = record['status']
        return task_statuses
    except Exception as e:
        print(f"Lỗi khi lấy trạng thái công việc: {e}")
        return {}

def generate_checklist_flex(group_id, shift_type):
    """
    Tạo nội dung Flex Message dựa trên trạng thái công việc.
    """
    task_statuses = get_tasks_status_from_sheet(group_id, shift_type)
    if not task_statuses:
        task_statuses = {task['id']: 'incomplete' for task in TASKS.get(shift_type, [])}

    title = "✅ CHECKLIST CA SÁNG" if shift_type == 'sang' else "🌙 CHECKLIST CA CHIỀU"
    
    task_components = []
    for task in TASKS.get(shift_type, []):
        status = task_statuses.get(task['id'], 'incomplete')
        is_complete = (status == 'complete')
        icon_url = "https://scdn.line-apps.com/n/channel_devcenter/img/fx/review_gold_star_28.png" if is_complete else "https://scdn.line-apps.com/n/channel_devcenter/img/fx/review_gray_star_28.png"
        text_decoration = "line-through" if is_complete else "none"
        text_color = "#999999" if is_complete else "#555555"
        button_style = "secondary" if is_complete else "primary"
        button_label = "Đã xong" if is_complete else "Hoàn tất"

        task_component = {
            "type": "box", "layout": "horizontal", "spacing": "md", "alignItems": "center",
            "contents": [
                { "type": "image", "url": icon_url, "size": "sm" },
                {
                    "type": "box", "layout": "vertical", "flex": 5,
                    "contents": [
                        { "type": "text", "text": task['name'], "wrap": True, "weight": "bold", "size": "md", "color": text_color, "decoration": text_decoration },
                        { "type": "text", "text": f"Deadline: {task['time']}", "size": "sm", "color": "#B2B2B2" }
                    ]
                },
                {
                    "type": "button",
                    "action": { "type": "postback", "label": button_label, "data": f"action=complete_task&task_id={task['id']}&shift={shift_type}" },
                    "style": button_style, "height": "sm", "flex": 2
                }
            ]
        }
        task_components.append(task_component)

    flex_content = {
        "type": "bubble",
        "header": {
            "type": "box", "layout": "vertical",
            "contents": [{"type": "text", "text": title, "weight": "bold", "size": "xl", "color": "#FFFFFF"}],
            "backgroundColor": "#0080ff" if shift_type == 'sang' else "#27406E"
        },
        "body": { "type": "box", "layout": "vertical", "spacing": "lg", "contents": task_components }
    }
    
    return flex_content
