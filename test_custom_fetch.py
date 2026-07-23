import unittest
from datetime import datetime

def format_custom_date_payload(custom_from_date, custom_to_date, run_type, report_type, area_pms_id=78109):
    """
    Giả lập logic format payload trong background.js khi cào bù dữ liệu theo ngày.
    """
    if custom_from_date and custom_to_date:
        from_str = f"{custom_from_date}T00:00:00"
        to_str = f"{custom_to_date}T00:00:00"
    else:
        now = datetime.now()
        from_str = f"{now.strftime('%Y-%m-01')}T00:00:00"
        to_str = f"{now.strftime('%Y-%m-%d')}T00:00:00"

    payload = {
        "areaPMSID": area_pms_id,
        "columnName": "STOREID",
        "storeId": 3934,
        "userTypeID": 5,
        "viewMode": 1,
        "isMatrix": 0,
        "fromDate": from_str,
        "toDate": to_str
    }

    if run_type == 'realtime':
        payload["timeType"] = 1
        payload["typeView"] = "bctd" if report_type == 'bctd' else "bcdtnh"
        payload["viewTypeId"] = 1
    elif run_type == 'luyke':
        payload["timeType"] = 2
        payload["typeView"] = "bctd" if report_type == 'bctd' else "bcdtnh"
        payload["viewTypeId"] = 1
    elif run_type == 'nhanvien':
        payload["timeType"] = 3
        payload["typeView"] = "bcdtnv"
        payload["viewTypeId"] = 2 if report_type == 'bcdtnv_td' else 1

    return payload


class TestCustomFetch(unittest.TestCase):
    def test_custom_date_payload_july_6(self):
        # Kiểm tra cào bù ngày 06/07/2026
        payload = format_custom_date_payload("2026-07-06", "2026-07-06", "luyke", "bcdtnh")
        self.assertEqual(payload["fromDate"], "2026-07-06T00:00:00")
        self.assertEqual(payload["toDate"], "2026-07-06T00:00:00")
        self.assertEqual(payload["timeType"], 2)
        self.assertEqual(payload["typeView"], "bcdtnh")
        self.assertEqual(payload["areaPMSID"], 78109)

    def test_custom_date_range_payload(self):
        # Kiểm tra cào bù khoảng ngày từ 01/07 đến 06/07
        payload = format_custom_date_payload("2026-07-01", "2026-07-06", "realtime", "bctd")
        self.assertEqual(payload["fromDate"], "2026-07-01T00:00:00")
        self.assertEqual(payload["toDate"], "2026-07-06T00:00:00")
        self.assertEqual(payload["timeType"], 1)
        self.assertEqual(payload["typeView"], "bctd")

    def test_custom_date_nhanvien_payload(self):
        payload = format_custom_date_payload("2026-07-06", "2026-07-06", "nhanvien", "bcdtnv_dt")
        self.assertEqual(payload["fromDate"], "2026-07-06T00:00:00")
        self.assertEqual(payload["timeType"], 3)
        self.assertEqual(payload["typeView"], "bcdtnv")
        self.assertEqual(payload["viewTypeId"], 1)


if __name__ == '__main__':
    unittest.main()
