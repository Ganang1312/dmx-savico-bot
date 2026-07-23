import os
import sys
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

os.environ['GOOGLE_CREDENTIALS_JSON'] = '{"type": "service_account", "project_id": "test"}'

with patch("gspread.authorize"):
    with patch("oauth2client.service_account.ServiceAccountCredentials.from_json_keyfile_dict"):
        import vesinh_handler
        from vesinh_handler import (
            allocate_cleaning_zones,
            get_current_vesinh_session,
            ZONES
        )

class TestVesinhHandler(unittest.TestCase):

    def test_allocate_th1_less_than_6_staff(self):
        # 5 NV (1 ERP, 2 Male, 1 Female, 1 GH2) -> 4 cleaning NV (< 6)
        staff_list = [
            "Thu Ngân ERP",
            "Nguyễn Văn Nam 1",
            "Trần Văn Nam 2",
            "Nguyễn Thị Hoa *",
            "Vũ Văn Bình GH2"
        ]
        
        assignments = allocate_cleaning_zones(staff_list)
        
        # Verify GH2 excluded
        gh2_item = next(a for a in assignments if "GH2" in a['name'])
        self.assertIn("Không phân chia", gh2_item['zone_desc'])
        
        # Verify ERP gets Khu 1 & Khu 5
        erp_item = next(a for a in assignments if "ERP" in a['name'])
        self.assertIn("Khu 1", erp_item['zone_desc'])
        self.assertIn("Khu 5", erp_item['zone_desc'])
        
        # Verify 2 Males get Khu 2 & Khu 4
        m1 = next(a for a in assignments if "Nam 1" in a['name'])
        m2 = next(a for a in assignments if "Nam 2" in a['name'])
        self.assertIn("Khu 2", m1['zone_desc'])
        self.assertIn("Khu 4", m2['zone_desc'])
        
        # Verify Female gets Khu 3
        fem = next(a for a in assignments if "Hoa *" in a['name'])
        self.assertIn("Khu 3", fem['zone_desc'])

    def test_allocate_th2_greater_or_equal_6_staff(self):
        # 7 NV (1 ERP, 2 Male, 4 Female) -> 7 cleaning NV (>= 6)
        staff_list = [
            "Thu Ngân ERP",
            "Nguyễn Văn Nam 1",
            "Trần Văn Nam 2",
            "Nguyễn Thị Nữ 1 *",
            "Nguyễn Thị Nữ 2 *",
            "Nguyễn Thị Nữ 3 *",
            "Nguyễn Thị Nữ 4 *"
        ]
        
        assignments = allocate_cleaning_zones(staff_list)
        
        # ERP gets Khu 1
        erp_item = next(a for a in assignments if "ERP" in a['name'])
        self.assertEqual(erp_item['zones'], [1])
        
        # 2 Males get Khu 2
        m1 = next(a for a in assignments if "Nam 1" in a['name'])
        m2 = next(a for a in assignments if "Nam 2" in a['name'])
        self.assertEqual(m1['zones'], [2])
        self.assertEqual(m2['zones'], [2])
        
        # Females get remaining zones 3, 4, 5
        f1 = next(a for a in assignments if "Nữ 1" in a['name'])
        f2 = next(a for a in assignments if "Nữ 2" in a['name'])
        f3 = next(a for a in assignments if "Nữ 3" in a['name'])
        f4 = next(a for a in assignments if "Nữ 4" in a['name'])
        
        assigned_rem = [f1['zones'][0], f2['zones'][0], f3['zones'][0], f4['zones'][0]]
        self.assertIn(3, assigned_rem)
        self.assertIn(4, assigned_rem)
        self.assertIn(5, assigned_rem)

    def test_get_current_vesinh_session_override(self):
        self.assertEqual(get_current_vesinh_session('sang'), 'vesinh_sang')
        self.assertEqual(get_current_vesinh_session('chieu'), 'vesinh_chieu')

if __name__ == '__main__':
    unittest.main()
