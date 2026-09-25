# -*- coding: utf-8 -*-
"""
test_task_parser.py — test cho task_parser.py, CHẠY OFFLINE (không cần Google/LINE).

Chạy:
    cd dmx-savico-bot
    python -m unittest test_task_parser -v

Bối cảnh: 24/09/2026 anh Dương báo giao việc bị "nhảy tên" — tên hiển thị mention
của LINE có DẤU CÁCH ("@SVC Thắng.61271 AIO") nên parser cũ chỉ lấy 1 từ, phần còn
lại lọt vào TÊN CÔNG VIỆC. Bộ test này khoá lại hành vi ĐÚNG.
"""
import sys
import types
import unittest

# ---------------------------------------------------------------------------
# Stub config / gspread / linebot để import được task_parser mà không cần creds.
# Phải chạy TRƯỚC khi import task_parser.
# ---------------------------------------------------------------------------
if "config" not in sys.modules:
    _cfg = types.ModuleType("config")
    _cfg.CLIENT = None
    _cfg.SHEET_NAME = ""
    _cfg.WORKSHEET_SCHEDULES_NAME = "schedules"
    _cfg.WORKSHEET_MEAL_TRACKER_NAME = "meal_tracker"
    _cfg.WORKSHEET_VESINH_TRACKER_NAME = "vesinh_tracker"
    _cfg.WORKSHEET_TRACKER_NAME = "task_tracker"
    _cfg.get_spreadsheet = lambda: None
    sys.modules["config"] = _cfg

if "gspread" not in sys.modules:
    _gs = types.ModuleType("gspread")
    _gs.exceptions = types.SimpleNamespace(WorksheetNotFound=Exception)
    sys.modules["gspread"] = _gs

if "linebot" not in sys.modules:
    _lb = types.ModuleType("linebot")
    _lbm = types.ModuleType("linebot.models")
    _lbm.FlexSendMessage = object
    _lbm.TextSendMessage = object
    sys.modules["linebot"] = _lb
    sys.modules["linebot.models"] = _lbm

from task_parser import (parse_work_command, plan_assignments,   # noqa: E402
                         resolve_mentions, roster_code_set, MENTION_SPACE)

# Roster mô phỏng ĐÚNG dạng sheet lịch thật: "Tên-mã"
ROSTER = {
    "NV": ["Khánh-97536", "Hoàng-132697", "Thảo-156494", "Thắng-61271",
           "Long-90509", "Dương-61169", "Dương-62210", "Trường-275563",
           "Linh-250630", "Huy-162055", "Công-267316"],
    "PG": ["Yến-111111"],
    "QL+TC": ["Sếp-999999"],
}


def _run(lines, mentionees=None, roster=ROSTER):
    """Viết lại mention (nếu có) rồi parse + plan. Trả về (parsed, plan)."""
    if mentionees:
        fixed = resolve_mentions("\n".join(lines), mentionees, roster)
        lines = [l.strip() for l in fixed.split("\n") if l.strip()]
    parsed = parse_work_command(lines, roster)
    return parsed, plan_assignments(parsed)


class TestMentionLine(unittest.TestCase):
    """Lỗi 24/09/2026: mention có dấu cách bị cắt cụt."""

    def test_mention_co_dau_cach_khong_lam_nhay_ten_cong_viec(self):
        lines = ["Việc @SVC.HUY.AIO🤔 162055 @SVC Thắng.61271 AIO",
                 "- trưng bày nhóm hàng mùa vụ sưởi theo CVST"]
        raw = "\n".join(lines)
        mentionees = [
            {"index": raw.index("@SVC.HUY.AIO"),
             "length": len("@SVC.HUY.AIO🤔 162055"), "type": "user"},
            {"index": raw.index("@SVC Thắng"),
             "length": len("@SVC Thắng.61271 AIO"), "type": "user"},
        ]
        parsed, plan = _run(lines, mentionees)

        # Tên công việc KHÔNG được lẫn tên người
        self.assertIsNone(parsed["job"])
        self.assertEqual(plan["mode"], "all")
        self.assertEqual(plan["task"], "trưng bày nhóm hàng mùa vụ sưởi theo CVST")
        # Đúng 2 người, giữ nguyên tên hiển thị theo tag
        self.assertEqual(plan["names"], ["SVC.HUY.AIO🤔 162055", "SVC Thắng.61271 AIO"])

    def test_resolve_mentions_giu_nguyen_ten_hien_thi(self):
        raw = "Việc @SVC Thắng.61271 AIO\n- dọn kho"
        fixed = resolve_mentions(raw, [{"index": 5, "length": len("@SVC Thắng.61271 AIO"),
                                        "type": "user"}], ROSTER)
        self.assertEqual(fixed, "Việc @SVC\uE000Thắng.61271\uE000AIO\n- dọn kho")

    def test_mention_co_emoji_utf16_khong_nuot_dong_cong_viec(self):
        raw = "việc @SVC\U0001F633LONG\U0001F633AIO\U0001F63390509\U0001F642\n- XỬ LÝ HÀNG ĐIỆN THOẠI TRƯNG BÀY LỖI CHUYỂN KHO 8170 BẢO HÀNH (24/09/2026)"
        m_text = "@SVC\U0001F633LONG\U0001F633AIO\U0001F63390509\U0001F642"
        # LINE API gui index va length theo UTF-16 code units
        utf16_len = len(m_text.encode('utf-16-le')) // 2
        utf16_idx = len("việc ".encode('utf-16-le')) // 2
        mentionees = [{"index": utf16_idx, "length": utf16_len, "type": "user"}]
        parsed, plan = _run(raw.split("\n"), mentionees)

        self.assertIsNone(parsed["job"])
        self.assertEqual(plan["task"], "XỬ LÝ HÀNG ĐIỆN THOẠI TRƯNG BÀY LỖI CHUYỂN KHO 8170 BẢO HÀNH (24/09/2026)")
        self.assertEqual(plan["names"], ["SVC\U0001F633LONG\U0001F633AIO\U0001F63390509\U0001F642"])

    def test_resolve_mentions_type_all(self):
        raw = "Việc @All\n- Tổng vệ sinh"
        fixed = resolve_mentions(raw, [{"index": 5, "length": 4, "type": "all"}], ROSTER)
        self.assertEqual(fixed, "Việc @all\n- Tổng vệ sinh")

    def test_mention_ten_co_dau_cach_khong_co_ma_giu_nguyen(self):
        raw = "Việc @Yến Sony\n- dán tem"
        fixed = resolve_mentions(raw, [{"index": 5, "length": len("@Yến Sony"),
                                        "type": "user"}], ROSTER)
        self.assertIn(MENTION_SPACE, fixed)          # đã tạm hoá dấu cách
        parsed, plan = _run(["Việc @Yến Sony", "- dán tem"],
                            [{"index": 5, "length": len("@Yến Sony"), "type": "user"}])
        self.assertEqual(plan["task"], "dán tem")
        self.assertEqual(plan["names"], ["Yến Sony"])  # trả lại dấu cách thật

    def test_roster_code_set(self):
        cs = roster_code_set(ROSTER)
        self.assertIn("162055", cs)
        self.assertIn("61271", cs)
        self.assertNotIn("Yến", cs)


class TestCuPhapCu(unittest.TestCase):
    """Các cú pháp cũ phải GIỮ NGUYÊN hành vi (không có mention metadata)."""

    def test_go_tay_at_ten_mot_dong(self):
        parsed, plan = _run(["việc @tuấn giá tv"])
        self.assertEqual(plan["mode"], "all")
        self.assertEqual(plan["task"], "giá tv")
        self.assertEqual(plan["names"], ["tuấn"])

    def test_nhom_all_hai_dong(self):
        parsed, plan = _run(["việc @all:", "- Vệ sinh quầy"])
        self.assertEqual(plan["mode"], "all")
        self.assertEqual(plan["task"], "Vệ sinh quầy")
        self.assertEqual(len(plan["names"]), len(ROSTER["NV"]) + len(ROSTER["PG"]) + len(ROSTER["QL+TC"]))

    def test_nhom_nv_nhieu_dong(self):
        parsed, plan = _run(["việc @nv", "- dọn kho", "- lau kệ"])
        self.assertEqual(plan["mode"], "multi")
        subs = {s for s, _ in plan["pairs"]}
        self.assertEqual(subs, {"dọn kho", "lau kệ"})
        for _, n in plan["pairs"]:
            self.assertIn(n, ROSTER["NV"])

    def test_ma_nv_thuan_tra_ten_that(self):
        parsed, plan = _run(["việc @156494", "- dán tem"])
        self.assertEqual(plan["names"], ["Thảo-156494"])

    def test_thieu_viec_thi_bao_loi(self):
        parsed, plan = _run(["việc @nv"])
        self.assertTrue(parsed["errors"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
