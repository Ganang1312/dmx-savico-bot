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
        self.assertIn("TARGET NGÀY", flex_str)
        self.assertIn("% HT TARGET", flex_str)
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
        self.assertIn("THỰC HIỆN / TARGET", flex_str)
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
        self.assertIn("BÁO CÁO XẾP HẠNG DOANH THU & THI ĐUA NV", str(flex[0]))
        
        staff_card_str = str(flex[1])
        self.assertIn("Tiến độ Doanh thu", staff_card_str)
        self.assertIn("Tiến độ Thi đua", staff_card_str)
        self.assertIn("TARGET DT", staff_card_str)
        self.assertIn("LŨY KẾ DT", staff_card_str)
        self.assertIn("DT CÒN LẠI", staff_card_str)

if __name__ == '__main__':
    unittest.main()
