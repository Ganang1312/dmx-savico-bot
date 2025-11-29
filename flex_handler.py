from datetime import datetime
import pytz
# Import từ file cấu hình trung tâm
from config import CLIENT, SHEET_NAME, WORKSHEET_TRACKER_NAME

# --- Danh sách công việc ---
TASKS = {
    'sang': [
        {'id': 'sang_1', 'icon': '📦', 'name': 'Check lệnh chuyển kho online', 'time': '09:15'},
        {'id': 'sang_2', 'icon': '🚚', 'name': 'Check đơn GHTK chuyển kho', 'time': '09:30'},
        {'id': 'sang_3', 'icon': '🏷️', 'name': 'Chạy tủ, thay giá (thứ 2 & 5)', 'time': '10:00'},
        {'id': 'sang_4', 'icon': '🧹', 'name': 'Rà soát tốc kệ', 'time': '10:30'},
        {'id': 'sang_5', 'icon': '📑', 'name': 'Check Phiếu CK/NK quá 7 ngày', 'time': '11:30'},
        {'id': 'sang_6', 'icon': '🔧', 'name': 'Đổ tồn hàng T.Thái (lỗi)', 'time': '14:00'},
    ],
    'chieu': [
        {'id': 'chieu_1', 'icon': '📦', 'name': 'Check lệnh online', 'time': '15:15'},
        {'id': 'chieu_2', 'icon': '🚚', 'name': 'Check đơn GHTK', 'time': '15:30'},
        {'id': 'chieu_3', 'icon': '📦🧹', 'name': 'Sắp xếp hàng hóa kho', 'time': '16:00'},
        {'id': 'chieu_4', 'icon': '🖼️', 'name': 'Rà soát tốc kệ (gia dụng/tivi)', 'time': '16:30'},
        {'id': 'chieu_5', 'icon': '📊', 'name': 'Xử lý BCNB chiều', 'time': '17:30'},
        {'id': 'chieu_6', 'icon': '🔧', 'name': 'Đổ tồn hàng T.Thái (lỗi)', 'time': '19:00'},
        {'id': 'chieu_7', 'icon': '📦🚚', 'name': 'Check GHTK / Grab', 'time': '21:00'},
        {'id': 'chieu_8', 'icon': '📸', 'name': 'Up hình máy cũ / trưng bày', 'time': '21:30'},
    ]
}

def initialize_daily_tasks(group_id, shift_type):
    """
    Reset và khởi tạo lại danh sách công việc.
    CẬP NHẬT: Xóa trắng 100% sheet và TẠO LẠI HEADER để tránh lỗi đọc dữ liệu.
    """
    print(f"Bắt đầu reset và khởi tạo công việc ca {shift_type} cho group {group_id}...")
    try:
        sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_TRACKER_NAME)
        
        # 1. Xóa sạch toàn bộ dữ liệu trong Sheet (Clear All)
        sheet.clear()
        
        # 2. Tạo lại dòng Tiêu Đề (Header) chuẩn xác
        # App.py dựa vào các tên cột này để tìm dòng cần update, sai tên là lỗi ngay.
        headers = ['group_id', 'date', 'task_id', 'name', 'time', 'status', 'user_name']
        sheet.append_row(headers)
        
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
        
        tasks_to_add = []
        for task in TASKS.get(shift_type, []):
            # Cấu trúc dữ liệu khớp với Header ở trên
            new_row = [group_id, today_str, task['id'], task['name'], task['time'], 'incomplete', '']
            tasks_to_add.append(new_row)

        if tasks_to_add:
            sheet.append_rows(tasks_to_add, value_input_option='USER_ENTERED')
            print(f"Đã tạo Header mới và {len(tasks_to_add)} công việc thành công.")
        return True
    except Exception as e:
        print(f"Lỗi khi khởi tạo công việc: {e}")
        return False

def get_tasks_status_from_sheet(group_id, shift_type, all_records=None):
    try:
        if all_records is None:
            sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_TRACKER_NAME)
            all_records = sheet.get_all_records()
        
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
        
        task_statuses = {}
        
        for record in all_records:
            # Code dùng .get() nên nếu Header sai tên, nó sẽ ra None và không tìm thấy task
            if str(record.get('group_id')) == group_id and record.get('date') == today_str:
                task_id = record.get('task_id')
                if task_id and task_id.startswith(shift_type):
                    task_statuses[task_id] = record.get('status')
        return task_statuses
    except Exception as e:
        print(f"Lỗi khi lấy trạng thái công việc: {e}")
        return {}

def generate_checklist_flex(group_id, shift_type, all_records_prefetched=None):
    task_statuses = get_tasks_status_from_sheet(group_id, shift_type, all_records=all_records_prefetched)
    
    if not task_statuses:
        task_statuses = {task['id']: 'incomplete' for task in TASKS.get(shift_type, [])}

    title = "CHECKLIST CÔNG VIỆC CA SÁNG" if shift_type == 'sang' else "CHECKLIST CÔNG VIỆC CA CHIỀU"
    title_icon = "✅" if shift_type == 'sang' else "🌙"
    
    task_components = []
    for task in TASKS.get(shift_type, []):
        status = task_statuses.get(task['id'], 'incomplete')
        is_complete = (status == 'complete')
        
        text_decoration = "line-through" if is_complete else "none"
        main_text_color = "#AAAAAA" if is_complete else "#111111"
        deadline_color = "#FF5555" if not is_complete else "#AAAAAA"
        button_style = "secondary" if is_complete else "primary"
        button_label = "✓ Xong" if is_complete else "Hoàn tất"

        task_component = {
            "type": "box",
            "layout": "horizontal",
            "spacing": "lg",
            "paddingAll": "md",
            "alignItems": "center",
            "contents": [
                {
                    "type": "text",
                    "text": task.get('icon', '❓'),
                    "size": "xl",
                    "flex": 0
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "spacing": "xs",
                    "contents": [
                        {
                            "type": "text",
                            "text": task['name'],
                            "wrap": True,
                            "weight": "bold",
                            "size": "sm",
                            "color": main_text_color,
                            "decoration": text_decoration
                        },
                        {
                            "type": "box",
                            "layout": "baseline",
                            "spacing": "sm",
                            "contents": [
                                {
                                    "type": "text",
                                    "text": "Deadline",
                                    "color": deadline_color,
                                    "size": "xs",
                                    "flex": 0
                                },
                                {
                                    "type": "text",
                                    "text": task['time'],
                                    "color": deadline_color,
                                    "weight": "bold",
                                    "size": "xs",
                                    "flex": 0
                                }
                            ]
                        }
                    ]
                },
                {
                    "type": "button",
                    "action": {
                        "type": "postback",
                        "label": button_label,
                        "data": f"action=complete_task&task_id={task['id']}&shift={shift_type}"
                    },
                    "style": button_style,
                    "height": "sm",
                    "flex": 0
                }
            ]
        }
        task_components.append(task_component)
        task_components.append({"type": "separator"})

    if task_components:
        task_components.pop()

    flex_content = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "horizontal",
            "alignItems": "center",
            "spacing": "md",
            "contents": [
                {
                    "type": "text",
                    "text": title_icon,
                    "size": "xxl",
                    "flex": 0
                },
                {
                    "type": "text",
                    "text": title,
                    "weight": "bold",
                    "size": "lg",
                    "color": "#FFFFFF",
                    "wrap": True
                }
            ],
            "backgroundColor": "#0080ff" if shift_type == 'sang' else "#27406E",
            "paddingTop": "12px",
            "paddingBottom": "12px",
            "paddingStart": "16px",
            "paddingEnd": "16px"
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "paddingAll": "sm",
            "contents": task_components
        }
    }
    return flex_content