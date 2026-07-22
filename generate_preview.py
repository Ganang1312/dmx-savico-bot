import json
import os
import sys

# Ensure UTF-8 stdout encoding on Windows
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.dirname(__file__))

from dmx_flex_messages import build_realtime_flex, build_luyke_flex, build_nhanvien_flex

def generate():
    sample_data = {
        "Data_BI": [
            {"nhóm ngành hàng": "Điện gia dụng", "doanh thu quy đổi": 120.9, "số lượng": 25, "target": 45.1},
            {"nhóm ngành hàng": "Điện lạnh", "doanh thu quy đổi": 63.7, "số lượng": 4, "target": 85.8},
            {"nhóm ngành hàng": "Laptop", "doanh thu quy đổi": 35.9, "số lượng": 2, "target": 11.9},
            {"nhóm ngành hàng": "Phụ kiện", "doanh thu quy đổi": 17.9, "số lượng": 4, "target": 7.0},
            {"nhóm ngành hàng": "Điện tử", "doanh thu quy đổi": 9.2, "số lượng": 1, "target": 41.4}
        ],
        "Data_ThiDua": [
            {"maingroupname": f"Nhóm Thi Đua {i}", "target": 28.8, "doanh thu": 35.9 if i<=5 else 9.2, "số lượng": 10}
            for i in range(1, 24)
        ],
        "Data_Realtime_BI": [
            {"Nhóm Ngành Hàng": "Điện gia dụng", "revenue_RT": 120.9, "quantity_RT": 25, "target_Day": 45.1, "revenue_Installment": 8.8},
            {"Nhóm Ngành Hàng": "Điện lạnh", "revenue_RT": 63.7, "quantity_RT": 4, "target_Day": 85.8, "revenue_Installment": 3.0},
            {"Nhóm Ngành Hàng": "Laptop", "revenue_RT": 35.9, "quantity_RT": 2, "target_Day": 11.9, "revenue_Installment": 2.0},
            {"Nhóm Ngành Hàng": "Phụ kiện", "revenue_RT": 17.9, "quantity_RT": 4, "target_Day": 7.0, "revenue_Installment": 1.0},
            {"Nhóm Ngành Hàng": "Điện tử", "revenue_RT": 9.2, "quantity_RT": 1, "target_Day": 41.4, "revenue_Installment": 0.5}
        ],
        "Data_Realtime_ThiDua": [
            {"maingroupname": f"Nhóm Thi Đua {i}", "revenue_RT": 35.9 if i<=5 else 9.2, "quantity_RT": 10, "target_Day": 28.8}
            for i in range(1, 24)
        ],
        "Config_ThiDua": [
            {"ngành hàng": f"Nhóm Thi Đua {i}", "phân loại": 1.0}
            for i in range(1, 24)
        ] + [
            {"Họ và tên": f"Nhân viên {i}", "% chia": 20.0}
            for i in range(1, 6)
        ],
        "Data_Realtime_NV": [
            {"staffUserName": f"Nhân viên {i}", "Doanh thu Quy đổi": 120.0 + i * 50}
            for i in range(1, 6)
        ]
    }

    try:
        from unittest.mock import patch
        with patch("dmx_flex_messages.get_dashboard_data", return_value=sample_data):
            rt1 = build_realtime_flex()
            lk1 = build_luyke_flex()
            nv1 = build_nhanvien_flex()
    except Exception as e:
        print(f"Lỗi khởi tạo Flex: {e}")
        return

    out_dir = os.path.dirname(__file__)
    
    with open(os.path.join(out_dir, "rt1_flex.json"), "w", encoding="utf-8") as f:
        json.dump(rt1, f, ensure_ascii=False, indent=2)
        
    with open(os.path.join(out_dir, "lk1_flex.json"), "w", encoding="utf-8") as f:
        json.dump(lk1, f, ensure_ascii=False, indent=2)
        
    with open(os.path.join(out_dir, "nv1_flex.json"), "w", encoding="utf-8") as f:
        json.dump(nv1, f, ensure_ascii=False, indent=2)

    print("SUCCESS: Da tao thanh cong 3 file JSON:")
    print(" - rt1_flex.json")
    print(" - lk1_flex.json")
    print(" - nv1_flex.json")
    print("\nBa con co the dan noi dung JSON vao trang https://developers.line.biz/flex-simulator/ de xem preview truc quan 100%!")

if __name__ == "__main__":
    generate()
