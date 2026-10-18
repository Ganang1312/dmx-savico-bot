# -*- coding: utf-8 -*-
"""
Test kiểm thử module coupon_handler.py:
  1. Phân tích văn bản nạp coupon hàng tuần (parse_coupons_text)
  2. Tự động nhận diện ngành hàng (detect_category)
  3. Cấp mã coupon cho nhân viên (claim_coupon_for_user)
  4. Trừ kho và chống cấp trùng lặp
  5. Xây dựng Flex Message (Menu, Danh sách sản phẩm, Thẻ nhận mã có nút clipboard)
"""

import unittest
import os
import sys

# Giả lập môi trường nếu chưa có biến môi trường
os.environ.setdefault('GOOGLE_CREDENTIALS_JSON', '{"type": "service_account"}')

from coupon_handler import (
    parse_coupons_text, detect_category,
    build_category_menu_flex, build_product_list_flex,
    build_claimed_coupon_flex
)

SAMPLE_TEXT = """
Ngày 25/09/2026 : Mã Coupon 1 - dùng cho Tủ lạnh Panasonic NR-DZ601VGKV: DMX22BV33RMK962

Ngày 25/09/2026 : Mã Coupon 2 - dùng cho Tủ lạnh Panasonic NR-DZ601VGKV: DMX23QK4GBKXX3C

Ngày 25/09/2026 : Mã Coupon 3 - dùng cho Tủ lạnh Panasonic NR-DZ601VGKV: DMX23J1NZQ1KH9Z

Ngày 26/09/2026 : Mã Coupon 1 - dùng cho Tủ lạnh Panasonic NR-DZ601VGKV: DMX26Z5GS6FVR1M

Ngày 25/09/2026 : Mã Coupon 1 - dùng cho Tủ lạnh Toshiba GR-RS910WI-PMV(06)-MG: DMX8ZN5B5KBV5PZ

Ngày 25/09/2026 : Mã Coupon 1 - dùng cho Smart Tivi Samsung 4K 55 inch: DMXTV55SAM001

Ngày 25/09/2026 : Mã Coupon 1 - dùng cho Máy giặt LG Inverter 9kg: DMXMGLG001
"""

class TestCouponHandler(unittest.TestCase):

    def test_detect_category(self):
        # 1. Tủ Lạnh
        cat1, icon1 = detect_category("Tủ lạnh Panasonic NR-DZ601VGKV")
        self.assertEqual(cat1, "Tủ Lạnh")
        self.assertEqual(icon1, "🧊")

        # 2. Tivi
        cat2, icon2 = detect_category("Smart Tivi Sony 4K 65 inch")
        self.assertEqual(cat2, "Tivi")
        self.assertEqual(icon2, "📺")

        # 3. Máy Giặt
        cat3, icon3 = detect_category("Máy giặt sấy Electrolux 10kg")
        self.assertEqual(cat3, "Máy Giặt")
        self.assertEqual(icon3, "🧺")

        # 4. Tủ Đông
        cat4, icon4 = detect_category("Tủ đông Kangaroo KG498KX2")
        self.assertEqual(cat4, "Tủ Đông")
        self.assertEqual(icon4, "❄️")

        # 5. Máy lọc nước
        cat5, icon5 = detect_category("Máy lọc nước RO nóng lạnh tủ đứng Hòa Phát HPN639")
        self.assertEqual(cat5, "Máy lọc nước")
        self.assertEqual(icon5, "💧")

        # 6. Loa Karaoke
        cat6, icon6 = detect_category("Loa Karaoke Dalton K220C")
        self.assertEqual(cat6, "Loa Karaoke")
        self.assertEqual(icon6, "🎤")

        # 7. Loa Thanh
        cat7, icon7 = detect_category("Loa Thanh Soundbar Samsung HW-Q600F/XV")
        self.assertEqual(cat7, "Loa Thanh")
        self.assertEqual(icon7, "🔊")

    def test_parse_coupons_text(self):
        parsed = parse_coupons_text(SAMPLE_TEXT)
        self.assertEqual(len(parsed), 7)

        # Kiểm tra item đầu tiên
        item0 = parsed[0]
        self.assertEqual(item0['date'], '25/09/2026')
        self.assertEqual(item0['category'], 'Tủ Lạnh')
        self.assertEqual(item0['product_name'], 'Tủ lạnh Panasonic NR-DZ601VGKV')
        self.assertEqual(item0['coupon_code'], 'DMX22BV33RMK962')

        # Kiểm tra item Tivi
        item_tv = parsed[5]
        self.assertEqual(item_tv['category'], 'Tivi')
        self.assertEqual(item_tv['coupon_code'], 'DMXTV55SAM001')

        # Kiểm tra nội dung văn bản ở Ảnh 1 người dùng gửi (Máy giặt Electrolux)
        img1_text = """add coupon:
Ngày 25/09/2026 : Mã Coupon 1 - dùng cho Máy giặt Electrolux 11kg EWF1124D3EC: DMX5SVPGF8VQ96G

Ngày 25/09/2026 : Mã Coupon 2 - dùng cho Máy giặt Electrolux 11kg EWF1124D3EC: DMX5JCH343RQPKQ
"""
        parsed_img1 = parse_coupons_text(img1_text)
        self.assertEqual(len(parsed_img1), 2)
        self.assertEqual(parsed_img1[0]['category'], 'Máy Giặt')
        self.assertEqual(parsed_img1[0]['coupon_code'], 'DMX5SVPGF8VQ96G')

    def test_flex_claimed_coupon_structure(self):
        flex = build_claimed_coupon_flex(
            product_name="Tủ lạnh Panasonic NR-DZ601VGKV",
            coupon_code="DMX22BV33RMK962",
            remaining_count=2,
            user_display_name="SVC😳LONG😳AIO😳90509🙂"
        )
        self.assertEqual(flex['type'], 'bubble')
        self.assertIn("🎉 LẤY MÃ GIẢM GIÁ THÀNH CÔNG", flex['header']['contents'][0]['text'])

        # Kiểm tra thông tin trừ số lượng
        body_str = str(flex['body']['contents'])
        self.assertIn('kho chung', body_str)
        self.assertIn('còn lại: 2 phiếu', body_str)
        self.assertIn('DMX22BV33RMK962', body_str)

        # Kiểm tra nút gửi mã ra chat an toàn cho SDK (không dùng clipboard action)
        button_item = [c for c in flex['body']['contents'] if c.get('type') == 'button'][0]
        self.assertEqual(button_item['action']['type'], 'message')
        self.assertEqual(button_item['action']['text'], 'DMX22BV33RMK962')

    def test_code_text_message_chi_co_ma(self):
        """Anh Dương chốt 25/09/2026: tin nhắn text gửi kèm thẻ Flex phải CHỈ có mã,
        không kèm chữ nào khác — để chạm đè là copy được nguyên mã, không lẫn hướng dẫn."""
        import re
        here = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(here, 'app.py'), encoding='utf-8') as f:
            src = f.read()

        # Tìm dòng gán code_text_msg trong nhánh claim_coupon
        m = re.search(r'code_text_msg\s*=\s*TextSendMessage\((.*?)\)\s*\n', src, re.S)
        self.assertIsNotNone(m, "Không tìm thấy code_text_msg trong app.py")
        arg = m.group(1)

        # Phải là text=str(res) — tức chỉ mỗi mã
        self.assertRegex(arg, r'text\s*=\s*str\(res\)|text\s*=\s*res\b',
                         "Tin nhắn mã phải là text=str(res) (chỉ chứa mã)")

        # Không được còn các nhãn trang trí cũ trong khối gửi mã
        khoi = src.split('code_text_msg')[1].split('messages_to_send')[0]
        for cam in ('MÃ COUPON', 'Chạm đè', 'Sao chép'):
            self.assertNotIn(cam, khoi, f"Khối tạo tin nhắn mã còn lẫn chữ '{cam}'")

    def test_menu_and_product_list_flex(self):
        import coupon_handler
        mock_records = [
            {'date': '25/09/2026', 'category': 'Tủ Lạnh', 'product_name': 'Tủ lạnh Panasonic NR-DZ601VGKV', 'coupon_code': 'C1', 'status': 'available'},
            {'date': '25/09/2026', 'category': 'Tủ Lạnh', 'product_name': 'Tủ lạnh Panasonic NR-DZ601VGKV', 'coupon_code': 'C2', 'status': 'available'},
            {'date': '25/09/2026', 'category': 'Tivi', 'product_name': 'Smart Tivi Sony 55 inch', 'coupon_code': 'TV1', 'status': 'available'}
        ]
        coupon_handler.get_available_coupons_today = lambda date_str: mock_records

        # Kiểm tra Menu ngành hàng
        menu_flex = build_category_menu_flex('25/09/2026')
        self.assertEqual(menu_flex['type'], 'bubble')
        self.assertIn('PHIẾU GIẢM GIÁ (GVGS)', menu_flex['header']['contents'][0]['text'])
        body_str = str(menu_flex['body'])
        self.assertIn('Tủ Lạnh', body_str)
        self.assertIn('Tivi', body_str)

        # Kiểm tra Danh sách sản phẩm ngành Tủ Lạnh
        prod_flex = build_product_list_flex('Tủ Lạnh', '25/09/2026')
        self.assertEqual(prod_flex['type'], 'bubble')
        self.assertIn('Tủ lạnh Panasonic NR-DZ601VGKV', str(prod_flex['body']))
        self.assertIn('action=claim_coupon', str(prod_flex['body']))
        self.assertIn('cat=Tủ Lạnh', str(prod_flex['body']))

    def test_import_multiple_and_clear(self):
        import coupon_handler
        # Giả lập FakeSheet
        class FakeSheet:
            def __init__(self):
                self.rows = [
                    ['date', 'category', 'product_name', 'coupon_code', 'status', 'used_by', 'used_at', 'created_at']
                ]
            def get_all_values(self):
                return [list(r) for r in self.rows]
            def update(self, *args, **kwargs):
                # Hỗ trợ cả positional và keyword arguments
                values = kwargs.get('values')
                range_name = kwargs.get('range_name')
                if not values and len(args) >= 2:
                    range_name = args[0]
                    values = args[1]
                elif not values and len(args) == 1:
                    values = args[0]
                
                # Parse range đơn giản dạng A2:H4 hoặc E2:G2
                import re
                m = re.search(r'([A-Z]+)(\d+)(?::([A-Z]+)(\d+))?', str(range_name))
                if m:
                    start_row = int(m.group(2))
                    start_col_letter = m.group(1)
                    col_offset = ord(start_col_letter) - ord('A')
                    for r_i, r_data in enumerate(values):
                        target_row_idx = start_row - 1 + r_i
                        while len(self.rows) <= target_row_idx:
                            self.rows.append([''] * 8)
                        for c_i, c_val in enumerate(r_data):
                            self.rows[target_row_idx][col_offset + c_i] = c_val
                return True
            def clear(self):
                self.rows = []
            def append_row(self, row):
                self.rows.append(row)

        fake_sheet = FakeSheet()
        coupon_handler.get_or_create_coupons_worksheet = lambda: fake_sheet

        # Lần 1: Nạp 2 mã Tủ lạnh
        text1 = """
Ngày 25/09/2026 : Mã Coupon 1 - dùng cho Tủ lạnh Panasonic NR-DZ601VGKV: DMX_PANA_01
Ngày 25/09/2026 : Mã Coupon 2 - dùng cho Tủ lạnh Panasonic NR-DZ601VGKV: DMX_PANA_02
"""
        count1, prods1, dup1 = coupon_handler.import_coupons_from_text(text1)
        self.assertEqual(count1, 2)
        self.assertEqual(dup1, 0)
        self.assertEqual(len(fake_sheet.rows), 3) # 1 header + 2 dòng

        # Lần 2 (Ảnh 1): Nạp thêm 3 mã Máy giặt
        text2 = """add coupon:
Ngày 25/09/2026 : Mã Coupon 1 - dùng cho Máy giặt Electrolux 11kg EWF1124D3EC: DMX5SVPGF8VQ96G
Ngày 25/09/2026 : Mã Coupon 2 - dùng cho Máy giặt Electrolux 11kg EWF1124D3EC: DMX5JCH343RQPKQ
Ngày 25/09/2026 : Mã Coupon 3 - dùng cho Máy giặt Electrolux 11kg EWF1124D3EC: DMX6QJ9ZXH981VD
"""
        count2, prods2, dup2 = coupon_handler.import_coupons_from_text(text2)
        self.assertEqual(count2, 3)
        self.assertEqual(dup2, 0)
        self.assertEqual(len(fake_sheet.rows), 6) # 1 header + 2 dòng đợt 1 + 3 dòng đợt 2 nối tiếp!

        # Lần 3: Nạp lại mã đã có -> Tự động phát hiện trùng lặp
        count3, prods3, dup3 = coupon_handler.import_coupons_from_text(text2)
        self.assertEqual(count3, 0)
        self.assertEqual(dup3, 3)
        self.assertEqual(len(fake_sheet.rows), 6) # Không tăng hàng

        # Lấy mã: Nhân viên lấy 1 mã Máy giặt
        ok, code, rem, cat = coupon_handler.claim_coupon_for_user(
            "Máy giặt Electrolux 11kg EWF1124D3EC", "Nhân viên A", "25/09/2026"
        )
        self.assertTrue(ok)
        self.assertEqual(code, "DMX5SVPGF8VQ96G")
        self.assertEqual(rem, 2) # Ban đầu có 3 mã, trừ 1 còn 2!
        self.assertEqual(cat, "Máy Giặt")

        # Lệnh xóa gvgs: Xóa toàn bộ
        ok_clear, msg_clear = coupon_handler.clear_all_coupons()
        self.assertTrue(ok_clear)
        self.assertEqual(len(fake_sheet.rows), 1) # Chỉ còn lại 1 header duy nhất!

    def test_user_exact_23_models(self):
        """
        Kiểm tra toàn bộ 23 model thực tế của người dùng:
        Phải chia chính xác vào 7 nhóm:
          1. Máy Giặt
          2. Tủ Lạnh
          3. Tủ Đông
          4. Máy lọc nước
          5. Loa Karaoke
          6. Loa Thanh
          7. Tivi
        """
        user_models = {
            "Máy giặt Electrolux 11kg EWF1124D3EC": "Máy Giặt",
            "Máy giặt Toshiba TW-T33B130UWV(MK)": "Máy Giặt",
            "Máy giặt Aqua AWM11-BSR1K BU": "Máy Giặt",
            "Máy giặt Samsung WW10DG6U34LBSV": "Máy Giặt",
            "Máy giặt Haier HW120-BD14387GNU1": "Máy Giặt",
            "Tủ lạnh Panasonic NR-DZ601VGKV": "Tủ Lạnh",
            "Tủ lạnh Toshiba GR-RS910WI-PMV(06)-MG": "Tủ Lạnh",
            "Tủ lạnh Toshiba GR-RF606WI-PMV(60)-AG": "Tủ Lạnh",
            "Tủ lạnh Hitachi HRSN9563DWGBVN": "Tủ Lạnh",
            "Tủ lạnh Haier HM610AGBVNU1": "Tủ Lạnh",
            "Tủ lạnh LG LSI63BLMA": "Tủ Lạnh",
            "Tủ lạnh Aqua AQR-T305FA(WLB)": "Tủ Lạnh",
            "Tủ lạnh AQUA AQR-M600XA(WGB)": "Tủ Lạnh",
            "Tủ đông Kangaroo KG498KX2": "Tủ Đông",
            "Máy lọc nước RO nóng lạnh tủ đứng Hòa Phát HPN639": "Máy lọc nước",
            "Máy lọc nước RO nóng lạnh tủ đứng Sunhouse UltraX SHA76601S": "Máy lọc nước",
            "Máy lọc nước RO nóng lạnh tủ đứng Karofi KAD-X68": "Máy lọc nước",
            "Loa Karaoke Dalton K220C": "Loa Karaoke",
            "Loa Thanh Soundbar Samsung HW-Q600F/XV": "Loa Thanh",
            "Tivi LED Sony K-55S25 (BRAVIA 2 II)": "Tivi",
            "Tivi LED Sony K-65S25 (BRAVIA 2 II)": "Tivi",
            "Tivi Mini LED Samsung UA65M8XHA": "Tivi",
            "TIVI QNED LG 65QNED70BSA": "Tivi",
        }

        for model, expected_cat in user_models.items():
            cat, icon = detect_category(model)
            self.assertEqual(cat, expected_cat, f"Model '{model}' bị phân loại sai thành '{cat}' (kỳ vọng '{expected_cat}')")


if __name__ == '__main__':
    unittest.main()
