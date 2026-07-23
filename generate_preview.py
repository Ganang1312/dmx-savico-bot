import json
from unittest.mock import patch
from dmx_flex_messages import build_realtime_flex, build_luyke_flex, build_nhanvien_flex

# Sample Mock Data matching Google Sheets structure
mock_bi = [
    {"nhóm ngành hàng": "Điện gia dụng", "doanh thu quy đổi": 1642.0, "số lượng": 1, "target": 1612.0, "rev_kft_riserate_lastmonth": 5.9},
    {"nhóm ngành hàng": "Điện lạnh", "doanh thu quy đổi": 1362.0, "số lượng": 1, "target": 3068.0, "rev_kft_riserate_lastmonth": -16.0},
    {"nhóm ngành hàng": "Điện tử", "doanh thu quy đổi": 785.0, "số lượng": 1, "target": 1481.0, "rev_kft_riserate_lastmonth": -14.0},
    {"nhóm ngành hàng": "Điện thoại mới", "doanh thu quy đổi": 680.0, "số lượng": 1, "target": 1021.0, "rev_kft_riserate_lastmonth": -12.2},
    {"nhóm ngành hàng": "Laptop", "doanh thu quy đổi": 564.0, "số lượng": 1, "target": 425.0, "rev_kft_riserate_lastmonth": 47.9},
    {"nhóm ngành hàng": "Phụ kiện", "doanh thu quy đổi": 467.0, "số lượng": 1, "target": 251.0, "rev_kft_riserate_lastmonth": 4.1},
    {"nhóm ngành hàng": "Khác", "doanh thu quy đổi": 460.0, "số lượng": 0, "target": 321.0, "rev_kft_riserate_lastmonth": 14.5},
]

mock_td = [
    {"maingroupname": "ĐIỆN TỬ & ĐIỆN TỬ TCL", "target": 36.0, "doanh thu": 9.0, "số lượng": 1},
    {"maingroupname": "Camera", "target": 4.0, "doanh thu": 0.0, "số lượng": 0},
    {"maingroupname": "NẠP RÚT TIỀN TẬN NƠI", "target": 3.0, "doanh thu": 0.0, "số lượng": 0},
    {"maingroupname": "Sim Tổng", "target": 2.0, "doanh thu": 0.0, "số lượng": 0},
    {"maingroupname": "Máy Giặt", "target": 3.0, "doanh thu": 1.0, "số lượng": 1},
]

mock_rt_bi = [
    {"Nhóm Ngành Hàng": "Điện lạnh", "revenue_RT": 9.0, "quantity_RT": 1, "target_Day": 87.0, "revenue_Installment": 0.0},
    {"Nhóm Ngành Hàng": "Điện gia dụng", "revenue_RT": 6.0, "quantity_RT": 1, "target_Day": 46.0, "revenue_Installment": 0.0},
]

mock_rt_td = [
    {"maingroupname": "ĐIỆN TỬ & ĐIỆN TỬ TCL", "revenue_RT": 9.0, "quantity_RT": 1, "target_Day": 36.0},
    {"maingroupname": "Camera", "revenue_RT": 0.0, "quantity_RT": 0, "target_Day": 4.0},
    {"maingroupname": "NẠP RÚT TIỀN TẬN NƠI", "revenue_RT": 0.0, "quantity_RT": 0, "target_Day": 3.0},
    {"maingroupname": "Sim Tổng", "revenue_RT": 0.0, "quantity_RT": 0, "target_Day": 2.0},
    {"maingroupname": "Máy Giặt", "revenue_RT": 3.0, "quantity_RT": 1, "target_Day": 3.0},
]

mock_config = [
    {"user": "U1", "user-họ và tên": "Đoàn Xuân Long", "% chia": 12.335},
    {"user": "U2", "user-họ và tên": "Bùi Hải Dương", "% chia": 10.85},
    {"user": "U3", "user-họ và tên": "Nguyễn Hữu Thắng", "% chia": 10.0},
    {"user": "U4", "user-họ và tên": "Nguyễn Tiến Hoàng", "% chia": 11.0},
    {"user": "U5", "user-họ và tên": "Dương Văn Huy", "% chia": 8.0},
    {"user": "U6", "user-họ và tên": "Đào Thị Phương Thảo", "% chia": 10.0},
    {"user": "U7", "user-họ và tên": "Nguyễn Thị Ngọc Khánh", "% chia": 10.0},
    {"user": "U8", "user-họ và tên": "Nguyễn Khánh Linh", "% chia": 8.0},
    {"user": "U9", "user-họ và tên": "Nguyễn Xuân Trường", "% chia": 6.0},
    {"user": "U10", "user-họ và tên": "Nguyễn Văn Dương", "% chia": 6.0},
    {"user": "U11", "user-họ và tên": "Đoàn Xuân Công", "% chia": 9.0},
    {"ngành hàng": "ĐIỆN TỬ & ĐIỆN TỬ TCL", "phân loại": 1.0},
    {"ngành hàng": "Camera", "phân loại": 1.0},
    {"ngành hàng": "NẠP RÚT TIỀN TẬN NƠI", "phân loại": 1.0},
    {"ngành hàng": "Sim Tổng", "phân loại": 1.0},
    {"ngành hàng": "Máy Giặt", "phân loại": 1.0},
]

mock_nv_rt = [
    {"staffUserName": "Đoàn Xuân Long", "Doanh thu Quy đổi": 930.0},
    {"staffUserName": "Bùi Hải Dương", "Doanh thu Quy đổi": 788.0},
    {"staffUserName": "Nguyễn Hữu Thắng", "Doanh thu Quy đổi": 732.0},
    {"staffUserName": "Nguyễn Tiến Hoàng", "Doanh thu Quy đổi": 617.0},
    {"staffUserName": "Dương Văn Huy", "Doanh thu Quy đổi": 568.0},
    {"staffUserName": "Đào Thị Phương Thảo", "Doanh thu Quy đổi": 495.0},
    {"staffUserName": "Nguyễn Thị Ngọc Khánh", "Doanh thu Quy đổi": 396.0},
    {"staffUserName": "Nguyễn Khánh Linh", "Doanh thu Quy đổi": 367.0},
    {"staffUserName": "Nguyễn Xuân Trường", "Doanh thu Quy đổi": 281.0},
    {"staffUserName": "Nguyễn Văn Dương", "Doanh thu Quy đổi": 226.0},
    {"staffUserName": "Đoàn Xuân Công", "Doanh thu Quy đổi": 180.0},
]

mock_data = {
    "Data_BI": mock_bi,
    "Data_ThiDua": mock_td,
    "Data_Realtime_BI": mock_rt_bi,
    "Data_Realtime_ThiDua": mock_rt_td,
    "Config_ThiDua": mock_config,
    "Data_Realtime_NV": mock_nv_rt
}

@patch("dmx_flex_messages.get_dashboard_data")
def main(mock_get_data):
    mock_get_data.return_value = mock_data
    
    rt = build_realtime_flex()
    with open("rt1_flex.json", "w", encoding="utf-8") as f:
        json.dump(rt, f, ensure_ascii=False, indent=2)
        
    lk = build_luyke_flex()
    if isinstance(lk, list):
        with open("lk1_flex_p1.json", "w", encoding="utf-8") as f:
            json.dump(lk[0], f, ensure_ascii=False, indent=2)
        with open("lk1_flex_p2.json", "w", encoding="utf-8") as f:
            json.dump(lk[1], f, ensure_ascii=False, indent=2)
        with open("lk1_flex.json", "w", encoding="utf-8") as f:
            json.dump(lk, f, ensure_ascii=False, indent=2)
    else:
        with open("lk1_flex.json", "w", encoding="utf-8") as f:
            json.dump(lk, f, ensure_ascii=False, indent=2)
        
    nv = build_nhanvien_flex()
    if isinstance(nv, list):
        with open("nv_overview_flex.json", "w", encoding="utf-8") as f:
            json.dump(nv[0], f, ensure_ascii=False, indent=2)
        if len(nv) > 1:
            with open("nv_staff_1.json", "w", encoding="utf-8") as f:
                json.dump(nv[1], f, ensure_ascii=False, indent=2)
        with open("nv1_flex.json", "w", encoding="utf-8") as f:
            json.dump(nv, f, ensure_ascii=False, indent=2)
    else:
        with open("nv1_flex.json", "w", encoding="utf-8") as f:
            json.dump(nv, f, ensure_ascii=False, indent=2)
        
    print("SUCCESS: Da tao thanh cong cac file JSON:\n - rt1_flex.json\n - lk1_flex_p1.json & lk1_flex_p2.json\n - nv_overview_flex.json, nv_staff_1.json & nv1_flex.json\n")

if __name__ == '__main__':
    main()
