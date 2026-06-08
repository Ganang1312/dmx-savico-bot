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
    ],
    'vs': [
        {'id': 'vs_1', 'icon': '📸', 'name': '1. Cụm 14285', 'time': '10:00'},
        {'id': 'vs_2', 'icon': '📸', 'name': '2. Cụm 5468', 'time': '10:00'},
        {'id': 'vs_3', 'icon': '📸', 'name': '3. Ngọc Lâm', 'time': '10:00'},
        {'id': 'vs_4', 'icon': '📸', 'name': '4. Ngọc Trì', 'time': '10:00'},
        {'id': 'vs_5', 'icon': '📸', 'name': '5. Ngô Gia Tự', 'time': '10:00'},
        {'id': 'vs_6', 'icon': '📸', 'name': '6. Savico', 'time': '10:00'},
        {'id': 'vs_7', 'icon': '📸', 'name': '7. Sài Đồng', 'time': '10:00'},
    ]
}

def initialize_daily_tasks(group_id, shift_type, force=False):
    """
    Reset và khởi tạo lại danh sách công việc cho ca cụ thể.
    Giữ lại các ca khác của ngày hôm nay và xóa các dữ liệu cũ.
    Nếu force=False, sẽ chỉ khởi tạo nếu hôm nay chưa có dữ liệu cho ca này.
    """
    print(f"Bắt đầu khởi tạo công việc ca {shift_type} cho group {group_id} (force={force})...")
    try:
        sheet = CLIENT.open(SHEET_NAME).worksheet(WORKSHEET_TRACKER_NAME)
        all_values = sheet.get_all_values()
        
        headers = ['group_id', 'date', 'task_id', 'name', 'time', 'status', 'user_name']
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        today_str = datetime.now(tz_vietnam).strftime('%Y-%m-%d')
        
        # Kiểm tra xem ca này hôm nay đã có dữ liệu chưa
        has_today_tasks = False
        rows_to_keep = []
        
        if all_values:
            headers = all_values[0]
            for row in all_values[1:]:
                if len(row) < 3: continue
                r_group = str(row[0])
                r_date = str(row[1])
                r_task_id = str(row[2])
                
                if r_date == today_str and r_group == str(group_id) and r_task_id.startswith(shift_type):
                    has_today_tasks = True
                
                # Giữ lại các dòng của ngày hôm nay thuộc các ca/group khác
                if r_date == today_str:
                    if r_group == str(group_id) and r_task_id.startswith(shift_type):
                        continue
                    rows_to_keep.append(row)
        
        # Nếu đã có dữ liệu và không yêu cầu force reset, bỏ qua bước khởi tạo lại
        if has_today_tasks and not force:
            print(f"Ca {shift_type} đã được khởi tạo hôm nay cho group {group_id}. Bỏ qua.")
            return True
            
        # Ghi đè lại dữ liệu (Clear và Append)
        sheet.clear()
        sheet.append_row(headers)
        if rows_to_keep:
            sheet.append_rows(rows_to_keep, value_input_option='USER_ENTERED')
            
        # Thêm các task mới của ca này
        tasks_to_add = []
        for task in TASKS.get(shift_type, []):
            new_row = [group_id, today_str, task['id'], task['name'], task['time'], 'incomplete', '']
            tasks_to_add.append(new_row)

        if tasks_to_add:
            sheet.append_rows(tasks_to_add, value_input_option='USER_ENTERED')
            print(f"Đã khởi tạo mới checklist ca {shift_type} thành công.")
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
            if str(record.get('group_id')) == group_id and record.get('date') == today_str:
                task_id = record.get('task_id')
                if task_id and task_id.startswith(shift_type):
                    task_statuses[task_id] = {
                        'status': record.get('status', 'incomplete'),
                        'user_name': record.get('user_name', '')
                    }
        return task_statuses
    except Exception as e:
        print(f"Lỗi khi lấy trạng thái công việc: {e}")
        return {}

def generate_checklist_flex(group_id, shift_type, all_records_prefetched=None):
    task_statuses = get_tasks_status_from_sheet(group_id, shift_type, all_records=all_records_prefetched)
    
    if not task_statuses:
        task_statuses = {task['id']: {'status': 'incomplete', 'user_name': ''} for task in TASKS.get(shift_type, [])}

    if shift_type == 'sang':
        title = "CHECKLIST CÔNG VIỆC CA SÁNG"
        title_icon = "✅"
    elif shift_type == 'chieu':
        title = "CHECKLIST CÔNG VIỆC CA CHIỀU"
        title_icon = "🌙"
    else:
        title = "CHECKLIST HÌNH ẢNH TRƯỚC 10H"
        title_icon = "📸"
    
    task_components = []
    
    # Add description box for VS
    if shift_type == 'vs':
        desc_box = {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#F4F6F9",
            "paddingAll": "md",
            "margin": "xs",
            "cornerRadius": "sm",
            "contents": [
                {
                    "type": "text",
                    "text": "Các cụm tạo Album đúng deadline nhé ACE",
                    "weight": "bold",
                    "size": "xs",
                    "color": "#E65100",
                    "wrap": True
                },
                {
                    "type": "text",
                    "text": "• Ảnh chụp bên ngoài ST (thấy biển, sân xe, toàn cảnh ST)\n• Ảnh chụp bên trong ST (thấy các góc ST, Line)\n• Ảnh chụp kho ST ( thấy quầy kệ gọn sạch )",
                    "size": "xxs",
                    "color": "#4A4A4A",
                    "wrap": True,
                    "margin": "xs"
                }
            ]
        }
        task_components.append(desc_box)
        task_components.append({"type": "separator", "margin": "sm"})
        
    for task in TASKS.get(shift_type, []):
        status_info = task_statuses.get(task['id'], {})
        if isinstance(status_info, str):
            status = status_info
            completed_by = ''
        else:
            status = status_info.get('status', 'incomplete')
            completed_by = status_info.get('user_name', '')
            
        is_complete = (status == 'complete')
        
        text_decoration = "line-through" if is_complete else "none"
        main_text_color = "#AAAAAA" if is_complete else "#111111"
        deadline_color = "#AAAAAA" if is_complete else "#FF5555"
        
        # Nút hoàn tất màu xanh, nút xong màu xám
        button_color = "#CCCCCC" if is_complete else "#00B33C"
        button_label = "✓ Xong" if is_complete else "Hoàn tất"
        
        if is_complete:
            sub_text_label = "Xong bởi:"
            sub_text_value = completed_by
        else:
            sub_text_label = "Deadline"
            sub_text_value = task['time']

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
                                    "text": sub_text_label,
                                    "color": deadline_color,
                                    "size": "xs",
                                    "flex": 0
                                },
                                {
                                    "type": "text",
                                    "text": sub_text_value,
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
                    "style": "primary",
                    "color": button_color,
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
            "backgroundColor": "#0080ff" if shift_type in ['sang', 'vs'] else "#27406E",
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