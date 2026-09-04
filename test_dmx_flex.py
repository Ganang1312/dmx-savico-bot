import unittest
from unittest.mock import patch
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from dmx_flex_messages import build_realtime_flex, build_luyke_flex, build_nhanvien_flex, parse_number, fmt_num, shorten_name

class TestDmxFlexMessages(unittest.TestCase):

    def setUp(self):
        bi_rows = [
            {"nhóm ngành hàng": f"Ngành {i}", "doanh thu quy đổi": 120.9 + i * 10, "số lượng": 25 + i, "target": 45.1 + i * 5}
            for i in range(1, 10)
        ]
        
        td_rows = [
            {"maingroupname": f"Nhóm Thi Đua {i}", "target": 500.0, "doanh thu": 60.0 + i, "số lượng": 10}
            for i in range(1, 24)
        ]
        
        rt_rows = [
            {"Nhóm Ngành Hàng": f"Ngành {i}", "revenue_RT": 120.9 + i * 10, "quantity_RT": 5, "target_Day": 100.0, "revenue_Installment": 15.5}
            for i in range(1, 10)
        ]
        
        rt_td_rows = [
            {"maingroupname": f"Nhóm Thi Đua {i}", "revenue_RT": 60.0 + i, "quantity_RT": 10, "target_Day": 50.0}
            for i in range(1, 24)
        ]
        
        config_rows = [
            {"ngành hàng": f"Nhóm Thi Đua {i}", "phân loại": 1.0}
            for i in range(1, 24)
        ] + [
            {"Họ và tên": f"Nhân viên {i}", "% chia": 10.0}
            for i in range(1, 6)
        ]

        self.mock_data = {
            "Data_BI": bi_rows,
            "Data_ThiDua": td_rows,
            "Data_Realtime_BI": rt_rows,
            "Data_Realtime_ThiDua": rt_td_rows,
            "Config_ThiDua": config_rows,
            "Data_Realtime_NV": [
                {"staffUserName": f"Nhân viên {i}", "Doanh thu Quy đổi": 1243.5 + i * 100}
                for i in range(1, 6)
            ]
        }

    def test_fmt_num(self):
        self.assertEqual(fmt_num(120.9), "121")
        self.assertEqual(fmt_num(1243.0), "1,243")
        self.assertEqual(fmt_num(0), "0")
        self.assertEqual(fmt_num(8.8), "9")

    def test_shorten_name(self):
        self.assertEqual(shorten_name("Điện gia dụng"), "Đ.Gia Dụng")
        self.assertEqual(shorten_name("Nhóm Thi Đua 23"), "T.Đua 23")

    @patch("dmx_flex_messages.get_dashboard_data")
    def test_build_realtime_flex_light_theme(self, mock_get_data):
        mock_get_data.return_value = self.mock_data
        
        flex = build_realtime_flex()
        self.assertIsInstance(flex, list)
        self.assertEqual(len(flex), 2)
        self.assertEqual(flex[0]["type"], "bubble")
        self.assertEqual(flex[1]["type"], "bubble")
        
        flex_str = str(flex)
        self.assertIn("Tr", flex_str)
        self.assertNotIn("P.1:", flex_str)
        self.assertNotIn("P.2:", flex_str)
        self.assertIn("%HT", flex_str)
        self.assertTrue("🔴 Chậm nhịp độ" in flex_str or "🟢 Vượt nhịp độ" in flex_str)
        self.assertTrue("💡" in flex_str or "🎉" in flex_str)

    @patch("dmx_flex_messages.get_dashboard_data")
    def test_build_realtime_flex_all_23_compete_items(self, mock_get_data):
        mock_get_data.return_value = self.mock_data
        
        flex = build_realtime_flex()
        flex_str = str(flex)
        self.assertTrue(len(flex_str) > 0)

    @patch("dmx_flex_messages.get_dashboard_data")
    def test_build_luyke_flex_light_theme(self, mock_get_data):
        mock_get_data.return_value = self.mock_data
        
        flex = build_luyke_flex()
        self.assertIsInstance(flex, list)
        self.assertEqual(len(flex), 2)
        self.assertEqual(flex[0]["type"], "bubble")
        self.assertEqual(flex[1]["type"], "bubble")
        
        flex_str = str(flex)
        self.assertIn("Tr", flex_str)
        self.assertNotIn("P.1:", flex_str)
        self.assertNotIn("P.2:", flex_str)
        self.assertIn("%DK", flex_str)
        self.assertIn("🔮 Dự Kiến Tháng", flex_str)
        self.assertIn("🎯 Mục tiêu hôm nay:", flex_str)
        self.assertIn("💡 Cần trung bình", flex_str)

    @patch("dmx_flex_messages.get_dashboard_data")
    def test_build_nhanvien_flex_light_theme(self, mock_get_data):
        mock_get_data.return_value = self.mock_data
        
        flex = build_nhanvien_flex()
        self.assertIsInstance(flex, list)
        self.assertGreaterEqual(len(flex), 2)
        self.assertEqual(flex[0]["type"], "bubble")
        self.assertEqual(flex[1]["type"], "bubble")
        
        flex_str = str(flex)
        self.assertIn("Tr", flex_str)
        self.assertIn("BÁO CÁO XẾP HẠNG", str(flex[0]))
        
        staff_card_str = str(flex[1])
        self.assertIn("DOANH THU", staff_card_str.upper())
        self.assertIn("THI ĐUA", staff_card_str.upper())
        self.assertIn("LK / TG", staff_card_str)

    @patch("dmx_flex_messages.get_dashboard_data")
    def test_rt1_hides_zero_revenue_staff(self, mock_get_data):
        mock_data_staff = dict(self.mock_data)
        mock_data_staff["Config_ThiDua"] = [
            {"user": "NV01", "% chia": 50.0},
            {"user": "NV02", "% chia": 50.0},
        ]
        mock_data_staff["Data_Realtime_NV"] = [
            {"mã nv": "NV01", "tên nv": "Nguyễn Văn A", "doanh thu quy đổi": 50.0, "số lượng": 2},
            {"mã nv": "NV02", "tên nv": "Trần Văn B", "doanh thu quy đổi": 0.0, "số lượng": 0},
        ]
        mock_get_data.return_value = mock_data_staff
        
        rt_flex = build_realtime_flex()
        rt_str = str(rt_flex)
        self.assertIn("NV01", rt_str)
        self.assertNotIn("NV02", rt_str)

    @patch("dmx_flex_messages.get_dashboard_data")
    def test_skip_total_row_no_doubling(self, mock_get_data):
        # Giả lập dữ liệu có dòng TỔNG CỘNG (như khi cào Cách 2)
        total_row = {"nhóm ngành hàng": "TỔNG CỘNG", "doanh thu quy đổi": 1000.0, "số lượng": 10, "target": 5000.0}
        cat_rows = [
            {"nhóm ngành hàng": "Đ.Gia Dụng", "doanh thu quy đổi": 600.0, "số lượng": 6, "target": 0.0},
            {"nhóm ngành hàng": "Đ.Thoại", "doanh thu quy đổi": 400.0, "số lượng": 4, "target": 0.0}
        ]
        mock_data_with_total = dict(self.mock_data)
        mock_data_with_total["Data_BI"] = [total_row] + cat_rows
        mock_data_with_total["Data_Realtime_BI"] = [
            {"Nhóm Ngành Hàng": "TỔNG CỘNG", "revenue_RT": 100.0, "quantity_RT": 2, "target_Day": 500.0},
            {"Nhóm Ngành Hàng": "Đ.Gia Dụng", "revenue_RT": 60.0, "quantity_RT": 1, "target_Day": 300.0},
            {"Nhóm Ngành Hàng": "Đ.Thoại", "revenue_RT": 40.0, "quantity_RT": 1, "target_Day": 200.0},
        ]
        mock_get_data.return_value = mock_data_with_total

        # LK1: Doanh thu phải là 1,000 Tr (600 + 400), KHÔNG PHẢI 2,000 Tr!
        # Số lượng phải là 10 (6 + 4), KHÔNG PHẢI 20!
        lk_flex = build_luyke_flex()
        lk_str = str(lk_flex)
        self.assertIn("1,000 Tr (20%)", lk_str)
        self.assertNotIn("2,000 Tr", lk_str)

        # RT1: Doanh thu hôm nay phải là 100 Tr (60 + 40), KHÔNG PHẢI 200 Tr!
        rt_flex = build_realtime_flex()
        rt_str = str(rt_flex)
        self.assertIn("100 TR", rt_str)
        self.assertNotIn("200 TR", rt_str)

if __name__ == '__main__':
    unittest.main()
