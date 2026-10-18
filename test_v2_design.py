import sys, json, os

sys.path.insert(0, os.path.dirname(__file__))
from dmx_flex_messages import get_color_class, fmt_num, shorten_name

def build_individual_staff_card_v2(e, rank, total_emp=11, now_str="", thi_dua_list=None):
    name = e.get("name", "Nhân Viên")
    user_id = str(e.get("user_id", "")).strip()
    full_name = str(name).strip()
    if " - " in full_name:
        parts = full_name.split(" - ")
        if not user_id or user_id == "90509" or user_id == "NV":
            user_id = parts[0].strip()
        full_name = parts[-1].strip()
        
    first_name = full_name.split()[-1] if full_name else "NV"
    
    actual_val = e.get("actual", 0.0)
    target_val = e.get("target", 0.0)
    con_lai_val = max(0.0, target_val - actual_val)
    pct_val = (actual_val / target_val * 100.0) if target_val > 0 else 0.0
    
    score_val = e.get("diem", max(60.0, 100.0 - (rank - 1) * 2.5))
    du_kien_pct = e.get("du_kien_pct", pct_val)
    days_passed = e.get("days_passed", 2)
    days_in_month = e.get("days_in_month", 30)
    days_left = max(1, days_in_month - days_passed)
    m_tieu_ngay = max(1, int(round(con_lai_val / days_left))) if con_lai_val > 0 else 0

    # Phân định màu sắc header sang trọng theo thứ hạng
    bottom_cutoff = int(total_emp * 0.7) if total_emp > 0 else 8
    if rank == 1:
        header_bg = "#b45309"
        rank_badge_bg = "#fef3c7"
        rank_badge_text = "#92400e"
        rank_label = "🥇 HẠNG 1"
    elif rank == 2:
        header_bg = "#334155"
        rank_badge_bg = "#f1f5f9"
        rank_badge_text = "#1e293b"
        rank_label = "🥈 HẠNG 2"
    elif rank == 3:
        header_bg = "#9a3412"
        rank_badge_bg = "#ffedd5"
        rank_badge_text = "#7c2d12"
        rank_label = "🥉 HẠNG 3"
    elif rank > bottom_cutoff:
        header_bg = "#991b1b"
        rank_badge_bg = "#fee2e2"
        rank_badge_text = "#991b1b"
        rank_label = f"⚠️ TOP {rank}"
    else:
        header_bg = "#0f2b48"
        rank_badge_bg = "#e0f2fe"
        rank_badge_text = "#0369a1"
        rank_label = f"🎖️ HẠNG #{rank}"

    if not thi_dua_list:
        thi_dua_list = []
    else:
        thi_dua_list = sorted(thi_dua_list, key=lambda x: x.get("du_kien", 0.0), reverse=True)

    td_passed = e.get("count_nh_du_kien")
    if td_passed is None:
        td_passed = sum(1 for td in thi_dua_list if td.get("du_kien", 0.0) >= 100.0 or td.get("ht", 0.0) >= 100 or str(td.get("con_lai")) == "🏆")
    td_total = len(thi_dua_list)
    td_pct = e.get("contest_score")
    if td_pct is None:
        td_pct = (td_passed / td_total * 100.0) if td_total > 0 else 0.0

    # 1. Header Luxury
    header_component = {
        "type": "box",
        "layout": "vertical",
        "backgroundColor": header_bg,
        "paddingAll": "md",
        "contents": [
            {
                "type": "box",
                "layout": "horizontal",
                "alignItems": "center",
                "contents": [
                    {
                        "type": "box",
                        "layout": "horizontal",
                        "alignItems": "center",
                        "spacing": "xs",
                        "flex": 6,
                        "contents": [
                            {
                                "type": "box",
                                "layout": "vertical",
                                "backgroundColor": rank_badge_bg,
                                "cornerRadius": "md",
                                "paddingStart": "sm",
                                "paddingEnd": "sm",
                                "paddingTop": "xs",
                                "paddingBottom": "xs",
                                "contents": [
                                    {"type": "text", "text": rank_label, "size": "xxs", "weight": "bold", "color": rank_badge_text}
                                ]
                            },
                            {
                                "type": "text",
                                "text": f"{first_name} ({user_id})" if user_id else first_name,
                                "weight": "bold",
                                "size": "sm",
                                "color": "#ffffff"
                            }
                        ]
                    },
                    {
                        "type": "box",
                        "layout": "vertical",
                        "backgroundColor": "#ffffff25",
                        "cornerRadius": "md",
                        "paddingStart": "sm",
                        "paddingEnd": "sm",
                        "paddingTop": "xs",
                        "paddingBottom": "xs",
                        "alignItems": "center",
                        "flex": 3,
                        "contents": [
                            {"type": "text", "text": f"⭐ {score_val:.1f} Đ", "weight": "bold", "size": "xs", "color": "#fef08a", "align": "center"}
                        ]
                    }
                ]
            }
        ]
    }

    # 2. Khối 2 Thanh Tiến Độ thiết kế Card sang trọng
    progress_bars_box = {
        "type": "box",
        "layout": "vertical",
        "backgroundColor": "#f8fafc",
        "cornerRadius": "md",
        "paddingAll": "sm",
        "margin": "sm",
        "contents": [
            # Tiến độ DT
            {
                "type": "box",
                "layout": "horizontal",
                "alignItems": "center",
                "contents": [
                    {"type": "text", "text": "💰 Doanh thu:", "size": "xxs", "color": "#64748b", "weight": "bold", "flex": 4},
                    {"type": "text", "text": f"{fmt_num(actual_val)} / {fmt_num(target_val)} TR", "size": "xxs", "color": "#0f172a", "weight": "bold", "align": "center", "flex": 5},
                    {
                        "type": "box",
                        "layout": "vertical",
                        "backgroundColor": "#fee2e2" if pct_val < 50 else ("#fef3c7" if pct_val < 100 else "#dcfce7"),
                        "cornerRadius": "sm",
                        "paddingStart": "xs",
                        "paddingEnd": "xs",
                        "paddingTop": "xxs",
                        "paddingBottom": "xxs",
                        "alignItems": "center",
                        "flex": 2,
                        "contents": [
                            {"type": "text", "text": f"{pct_val:.1f}%", "size": "xxs", "color": get_color_class(pct_val / 100.0), "weight": "bold", "align": "center"}
                        ]
                    }
                ]
            },
            {
                "type": "box",
                "layout": "vertical",
                "backgroundColor": "#e2e8f0",
                "height": "8px",
                "cornerRadius": "xxl",
                "margin": "xs",
                "contents": [
                    {
                        "type": "box",
                        "layout": "vertical",
                        "backgroundColor": get_color_class(pct_val / 100.0),
                        "height": "8px",
                        "cornerRadius": "xxl",
                        "width": f"{max(4, min(100, round(pct_val)))}%",
                        "contents": [{"type": "filler"}]
                    }
                ]
            },
            # Tiến độ Thi đua
            {
                "type": "box",
                "layout": "horizontal",
                "alignItems": "center",
                "margin": "md",
                "contents": [
                    {"type": "text", "text": "🎯 Thi đua đạt:", "size": "xxs", "color": "#64748b", "weight": "bold", "flex": 4},
                    {"type": "text", "text": f"{td_passed} / {td_total} Nhóm", "size": "xxs", "color": "#0f172a", "weight": "bold", "align": "center", "flex": 5},
                    {
                        "type": "box",
                        "layout": "vertical",
                        "backgroundColor": "#fee2e2" if td_pct < 50 else ("#fef3c7" if td_pct < 100 else "#dcfce7"),
                        "cornerRadius": "sm",
                        "paddingStart": "xs",
                        "paddingEnd": "xs",
                        "paddingTop": "xxs",
                        "paddingBottom": "xxs",
                        "alignItems": "center",
                        "flex": 2,
                        "contents": [
                            {"type": "text", "text": f"{td_pct:.1f}%", "size": "xxs", "color": get_color_class(td_pct / 100.0), "weight": "bold", "align": "center"}
                        ]
                    }
                ]
            },
            {
                "type": "box",
                "layout": "vertical",
                "backgroundColor": "#e2e8f0",
                "height": "8px",
                "cornerRadius": "xxl",
                "margin": "xs",
                "contents": [
                    {
                        "type": "box",
                        "layout": "vertical",
                        "backgroundColor": get_color_class(td_pct / 100.0),
                        "height": "8px",
                        "cornerRadius": "xxl",
                        "width": f"{max(4, min(100, round(td_pct)))}%",
                        "contents": [{"type": "filler"}]
                    }
                ]
            }
        ]
    }

    # 3. Khối 5 Chỉ Số Cốt Lõi: Ma trận 2 Hàng Soft Card (Thoáng, Sang, Không Bị Chật)
    pill_cards = {
        "type": "box",
        "layout": "vertical",
        "margin": "sm",
        "spacing": "xs",
        "contents": [
            # Hàng 1: Chỉ tiêu tài chính
            {
                "type": "box",
                "layout": "horizontal",
                "spacing": "xs",
                "contents": [
                    {
                        "type": "box", "layout": "vertical", "flex": 1, "backgroundColor": "#eff6ff", "paddingAll": "xs", "cornerRadius": "md", "alignItems": "center",
                        "contents": [
                            {"type": "text", "text": "🎯 TARGET", "size": "xxs", "color": "#1e40af", "weight": "bold", "align": "center"},
                            {"type": "text", "text": fmt_num(target_val), "size": "xs", "color": "#1d4ed8", "weight": "bold", "align": "center"}
                        ]
                    },
                    {
                        "type": "box", "layout": "vertical", "flex": 1, "backgroundColor": "#ecfdf5", "paddingAll": "xs", "cornerRadius": "md", "alignItems": "center",
                        "contents": [
                            {"type": "text", "text": "💰 LŨY KẾ", "size": "xxs", "color": "#065f46", "weight": "bold", "align": "center"},
                            {"type": "text", "text": fmt_num(actual_val), "size": "xs", "color": "#047857", "weight": "bold", "align": "center"}
                        ]
                    },
                    {
                        "type": "box", "layout": "vertical", "flex": 1, "backgroundColor": "#fff1f2" if con_lai_val > 0 else "#ecfdf5", "paddingAll": "xs", "cornerRadius": "md", "alignItems": "center",
                        "contents": [
                            {"type": "text", "text": "⏳ CÒN THIẾU", "size": "xxs", "color": "#9f1239" if con_lai_val > 0 else "#065f46", "weight": "bold", "align": "center"},
                            {"type": "text", "text": fmt_num(con_lai_val) if con_lai_val > 0 else "🏆 ĐẠT", "size": "xs", "color": "#be123c" if con_lai_val > 0 else "#047857", "weight": "bold", "align": "center"}
                        ]
                    }
                ]
            },
            # Hàng 2: Dự kiến & Kế hoạch ngày
            {
                "type": "box",
                "layout": "horizontal",
                "spacing": "xs",
                "contents": [
                    {
                        "type": "box", "layout": "horizontal", "flex": 1, "backgroundColor": "#f5f3ff", "paddingAll": "xs", "cornerRadius": "md", "alignItems": "center", "justifyContent": "space-between",
                        "contents": [
                            {"type": "text", "text": "🔮 DỰ KIẾN", "size": "xxs", "color": "#6b21a8", "weight": "bold", "flex": 3},
                            {"type": "text", "text": f"{du_kien_pct:.1f}%", "size": "xs", "color": "#7e22ce", "weight": "bold", "align": "end", "flex": 2}
                        ]
                    },
                    {
                        "type": "box", "layout": "horizontal", "flex": 1, "backgroundColor": "#fffbeb", "paddingAll": "xs", "cornerRadius": "md", "alignItems": "center", "justifyContent": "space-between",
                        "contents": [
                            {"type": "text", "text": "⚡ CẦN / NGÀY", "size": "xxs", "color": "#92400e", "weight": "bold", "flex": 3},
                            {"type": "text", "text": f"{fmt_num(m_tieu_ngay)} TR", "size": "xs", "color": "#b45309", "weight": "bold", "align": "end", "flex": 2}
                        ]
                    }
                ]
            }
        ]
    }

    # 4. Bảng Nhóm Hàng Thi Đua 7 Cột (# | NH | MT | LK / TG | CÒN | %HT | %DK)
    headers = ["#", "NHÓM HÀNG", "MT", "LK / TG", "CÒN", "%HT", "%DK"]
    weights = [1, 4, 2, 3, 2, 2, 2]
    aligns = ["center", "start", "center", "center", "center", "end", "end"]
    
    td_rows = [{
        "type": "box", "layout": "horizontal", "backgroundColor": "#1e293b", "paddingAll": "xs", "cornerRadius": "sm",
        "contents": [{"type": "text", "text": h, "size": "xxs", "weight": "bold", "color": "#f8fafc", "align": a, "flex": w} for h, w, a in zip(headers, weights, aligns)]
    }]

    for i, td in enumerate(thi_dua_list, 1):
        ht_val = td.get("ht", 0.0)
        ht_str = f"{round(ht_val)}%"
        dk_val = td.get("du_kien", 0.0)
        dk_str = f"{round(dk_val)}%"
        
        unit_tag = "(SL)" if td.get("is_sl", True) else "(DT)"
        name_s = f"{shorten_name(td.get('name', ''))} {unit_tag}"
        name_color = "#e11d48" if td.get("phan_loai") == 2.0 else "#0f172a"

        mt = str(td.get("m_tieu", "✓"))
        actual_val_item = td.get("actual", 0)
        target_val_item = td.get("target", 0)
        lk_tg = f"{actual_val_item}/{target_val_item}"
        cl = str(td.get("con_lai", 0))
        if cl == "🏆":
            cl = "Đạt"
        if mt == "🏆":
            mt = "✓"

        vals = [str(i), name_s, mt, lk_tg, cl, ht_str, dk_str]
        colors = [
            "#64748b", name_color, "#059669" if mt == "✓" else "#0284c7",
            "#0f172a", "#059669" if cl == "Đạt" else "#e11d48",
            get_color_class(ht_val / 100.0), get_color_class(dk_val / 100.0)
        ]

        def make_staff_cell_v2(v, w, a, c):
            cell = {"type": "text", "text": str(v), "size": "xxs"}
            if w > 1:
                cell["flex"] = w
            if a and a != "start":
                cell["align"] = a
            if c and c != "#0f172a":
                cell["color"] = c
            return cell

        row_bg = "#ffffff" if i % 2 != 0 else "#f8fafc"
        if ht_val >= 100 or cl == "Đạt":
            row_bg = "#f0fdf4"

        row_contents = [make_staff_cell_v2(v, w, a, c) for v, c, a, w in zip(vals, colors, aligns, weights)]

        row_box = {
            "type": "box", "layout": "horizontal", "contents": row_contents, "paddingTop": "xxs", "paddingBottom": "xxs"
        }
        if row_bg != "#ffffff":
            row_box["backgroundColor"] = row_bg

        td_rows.append(row_box)

    bubble = {
        "type": "bubble",
        "size": "mega",
        "header": header_component,
        "body": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#ffffff",
            "paddingAll": "sm",
            "contents": [
                progress_bars_box,
                pill_cards,
                {"type": "separator", "margin": "sm", "color": "#e2e8f0"},
                {"type": "box", "layout": "vertical", "margin": "xs", "contents": td_rows}
            ]
        }
    }
    return bubble

if __name__ == '__main__':
    from dmx_flex_messages import build_nhanvien_flex, build_individual_staff_card
    bubbles = build_nhanvien_flex()
    old_card = bubbles[6]
    
    # Lấy thông tin nhân viên Dương từ build_nhanvien_flex để tạo V2 tương ứng
    print("Old card generated successfully! JSON size:", len(json.dumps(old_card)))
