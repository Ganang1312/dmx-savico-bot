import pytz
from datetime import datetime
from dmx_data_provider import get_dashboard_data, get_locked_target_config

def parse_number(val):
    if val is None or val == '':
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    
    str_val = str(val).strip().replace(" ", "")
    if ',' in str_val and '.' in str_val:
        if str_val.rfind(',') > str_val.rfind('.'):
            str_val = str_val.replace(".", "").replace(",", ".")
        else:
            str_val = str_val.replace(",", "")
    elif ',' in str_val:
        str_val = str_val.replace(",", ".")
        
    try:
        num = float(str_val.replace("%", ""))
        return num
    except:
        return 0.0

def fmt_num(val):
    if val is None or val == '':
        return "0"
    num = parse_number(val)
    rounded = int(round(num))
    return f"{rounded:,}"

def shorten_name(name):
    if not name:
        return ""
    s = str(name).replace("NNH ", "").strip()
    replacements = [
        ("Điện Gia Dụng", "Đ.Gia Dụng"),
        ("Điện gia dụng", "Đ.Gia Dụng"),
        ("Điện Lạnh", "Đ.Lạnh"),
        ("Điện lạnh", "Đ.Lạnh"),
        ("Điện Tử", "Đ.Tử"),
        ("Điện tử", "Đ.Tử"),
        ("Điện Thoại", "Đ.Thoại"),
        ("Điện thoại", "Đ.Thoại"),
        ("Thiết Bị Gia Đình", "TB Gia Đình"),
        ("Thiết bị gia đình", "TB Gia Đình"),
        ("Thiết Bị", "TB"),
        ("Thiết bị", "TB"),
        ("Phụ Kiện", "P.Kiện"),
        ("Phụ kiện", "P.Kiện"),
        ("Chăm Sóc Cá Nhân", "CS Cá Nhân"),
        ("Chăm sóc cá nhân", "CS Cá Nhân"),
        ("Viễn Thông", "V.Thông"),
        ("Viễn thông", "V.Thông"),
        ("Nhóm Thi Đua", "T.Đua"),
        ("Nhóm thi đua", "T.Đua"),
        ("NẠP RÚT TIỀN TẬN NƠI", "Nạp Rút Tiền"),
        ("TRẢ CHẬM HOMECREDIT", "TC HomeCredit"),
        ("TRẢ CHẬM ĐIỆN MÁY", "TC Điện Máy"),
        ("TRẢ CHẬM", "Trả Chậm"),
        ("MÁY LỌC KHÔNG KHÍ", "Lọc K.Khí"),
        ("MÁY LỌC NƯỚC", "Lọc Nước"),
        ("ĐIỆN THOẠI & PHỤ KIỆN", "Đ.Thoại & PK"),
        ("TỦ LẠNH, TỦ ĐỒ", "Tủ Lạnh/Đông"),
        ("DOANH THU ĐỒNG HỒ", "Đồng Hồ"),
    ]
    for old, new in replacements:
        s = s.replace(old, new)
        
    if len(s) > 14:
        s = s[:13] + "…"
    return s

def shorten_staff_name(name):
    if not name:
        return ""
    parts = str(name).strip().split()
    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0]
    initials = [p[0].upper() + "." for p in parts[:-1]]
    return "".join(initials) + parts[-1]

def get_key_val(row, *possible_keys, default=None):
    if not row or not isinstance(row, dict):
        return default
    
    for pk in possible_keys:
        if pk in row:
            return row[pk]
            
    row_keys_lower = {k.strip().lower(): k for k in row.keys()}
    for pk in possible_keys:
        pk_lower = pk.strip().lower()
        if pk_lower in row_keys_lower:
            orig_key = row_keys_lower[pk_lower]
            return row[orig_key]
            
    return default

def make_table_header(cols, weights, aligns=None, bg_color="#0284c7"):
    if not aligns:
        aligns = ["start"] * len(cols)
    contents = []
    for c, w, a in zip(cols, weights, aligns):
        contents.append({
            "type": "text",
            "text": c,
            "weight": "bold",
            "size": "xxs",
            "color": "#ffffff",
            "flex": w,
            "align": a
        })
    return {
        "type": "box",
        "layout": "horizontal",
        "margin": "md",
        "backgroundColor": bg_color,
        "paddingAll": "xs",
        "cornerRadius": "sm",
        "contents": contents
    }

def make_table_row(vals, weights, aligns=None, colors=None, bold=False, bg_color=None):
    if not aligns:
        aligns = ["start"] * len(vals)
    if not colors:
        colors = ["#0f172a"] * len(vals)
    contents = []
    for v, w, a, c in zip(vals, weights, aligns, colors):
        contents.append({
            "type": "text",
            "text": str(v),
            "size": "xxs",
            "color": c,
            "flex": w,
            "align": a,
            "weight": "bold" if bold else "regular"
        })
    box_dict = {
        "type": "box",
        "layout": "horizontal",
        "margin": "xs",
        "contents": contents
    }
    if bg_color:
        box_dict["backgroundColor"] = bg_color
        box_dict["paddingAll"] = "xs"
        box_dict["cornerRadius"] = "sm"
    return box_dict

def get_color_class(pct):
    if pct >= 1.0:
        return "#16a34a" # Dark Green
    elif pct >= 0.8:
        return "#2563eb" # Blue
    elif pct >= 0.5:
        return "#ea580c" # Orange
    else:
        return "#dc2626" # Red

def make_thidua_progress_row(idx, name, con_lai_str, ht, unit):
    ht_pct = round(ht * 100)
    color = get_color_class(ht)
    
    if con_lai_str:
        display_name = f"{name} (-{con_lai_str} {unit})"
    else:
        display_name = name
        
    return {
        "type": "box",
        "layout": "horizontal",
        "margin": "sm",
        "alignItems": "center",
        "contents": [
            {
                "type": "text",
                "text": str(idx),
                "size": "xxs",
                "color": "#64748b",
                "weight": "bold",
                "flex": 1,
                "align": "start"
            },
            {
                "type": "text",
                "text": display_name,
                "size": "xxs",
                "color": "#0f172a",
                "weight": "bold",
                "flex": 6,
                "wrap": True
            },
            {
                "type": "text",
                "text": f"{ht_pct}%",
                "size": "xxs",
                "color": color,
                "weight": "bold",
                "flex": 2,
                "align": "end"
            },
            {
                "type": "box",
                "layout": "vertical",
                "flex": 3,
                "margin": "xs",
                "backgroundColor": "#e2e8f0",
                "height": "6px",
                "cornerRadius": "md",
                "contents": [
                    {
                        "type": "box",
                        "layout": "vertical",
                        "backgroundColor": color,
                        "height": "6px",
                        "cornerRadius": "md",
                        "width": f"{min(100, max(5, ht_pct))}%",
                        "contents": [{"type": "filler"}]
                    }
                ]
            }
        ]
    }

def build_luyke_flex():
    data = get_dashboard_data("Config_ThiDua,Data_BI,Data_ThiDua")
    bi_rows = data.get("Data_BI", [])
    config_rows = data.get("Config_ThiDua", [])
    td_rows = data.get("Data_ThiDua", [])
    
    tz = pytz.timezone('Asia/Ho_Chi_Minh')
    now = datetime.now(tz)
    now_str = now.strftime("%H:%M - %d/%m/%Y")
    
    tDT = sum(parse_number(get_key_val(b, "Doanh thu Quy đổi", "Doanh thu", default=0.0)) for b in bi_rows)
    tTG = sum(parse_number(get_key_val(b, "Target", "target", default=0.0)) for b in bi_rows)
    if tTG <= 0:
        tTG = 1500.0
        
    current_day = now.day
    days_in_month = (datetime(now.year, now.month + 1, 1) - datetime(now.year, now.month, 1)).days if now.month < 12 else 31
    days_passed = days_in_month if current_day == 1 else current_day - 1
    days_remaining = max(1, days_in_month - days_passed)
    
    totalHT = tDT / tTG if tTG > 0 else 0.0
    totalDKThang = (tDT / days_passed) * days_in_month if days_passed > 0 else 0.0
    totalHTDK = (totalDKThang / tTG) if tTG > 0 else 0.0
    revRem = max(0.0, tTG - tDT)
    
    target_co_dinh = tTG / days_in_month
    target_bu_tru = (tTG - tDT) / days_remaining
    if target_bu_tru < 0:
        target_bu_tru = 0.0

    holiday_target = 0.0
    if config_rows:
        for r in config_rows:
            day_val = parse_number(get_key_val(r, "ngày", "Ngày", default=0.0))
            if int(day_val) == current_day:
                holiday_target = parse_number(get_key_val(r, "Mục tiêu", "mục tiêu ngày", "mục tiêu", default=0.0))
                if holiday_target > 0:
                    break

    is_weekend = now.weekday() in [5, 6]
    target_today = 0.0
    if holiday_target > 0:
        target_today = holiday_target
    elif is_weekend:
        target_today = target_co_dinh * 2
    else:
        target_today = target_co_dinh if target_bu_tru < target_co_dinh else target_bu_tru

    expected_pacing_pct = (days_passed / days_in_month) if days_in_month > 0 else 0.0
    is_on_track = totalHT >= expected_pacing_pct
    status_badge_text = "🟢 Đang đúng tiến độ" if is_on_track else "🔴 Cần tăng tốc"

    tTC = sum(parse_number(get_key_val(b, "revenue_installment", "doanh thu trả chậm", default=0.0)) for b in bi_rows)
    tDTGoc = sum(parse_number(get_key_val(b, "doanh thu", default=0.0)) for b in bi_rows)
    totalTyLeTC = tTC / tDTGoc if tDTGoc > 0 else (tTC / tDT if tDT > 0 else 0.0)
    
    parsed_bi = []
    for b in bi_rows:
        nganh = get_key_val(b, "nhóm ngành hàng", "ngành hàng", "salegroupmastername", default=None)
        if not nganh or str(nganh).strip().upper() == "N/A":
            continue
        dt = parse_number(get_key_val(b, "doanh thu quy đổi", default=0.0))
        sl = parse_number(get_key_val(b, "số lượng", "quantity", default=0.0))
        tg = parse_number(get_key_val(b, "target", default=0.0))
        
        tang_giam_ck_raw = parse_number(get_key_val(b, "rev_kft_riserate_lastmonth", "+/- dtck", "+/- so với ck", default=0.0))
        dt_ck = 0.0
        tang_giam_ck = 0.0
        if tang_giam_ck_raw != 0:
            tang_giam_ck = tang_giam_ck_raw
            if (1 + tang_giam_ck / 100.0) != 0:
                dt_ck = dt / (1 + tang_giam_ck / 100.0)
        else:
            dt_ck = parse_number(get_key_val(b, "DT Năm ngoái", "doanh thu năm ngoái", "dt năm ngoái", "năm ngoái", "nam ngoai", "doanh thu nam ngoai", "dt nam ngoai", "doanh thu năm ngoái (cùng kỳ)", "doanh thu nam ngoai (cung ky)", "cùng kỳ", "cung ky", "tháng trước", "dt tháng trước", default=0.0))
            if dt_ck > 0:
                tang_giam_ck = ((dt - dt_ck) / dt_ck) * 100.0

        if dt > 0 or sl > 0:
            parsed_bi.append({
                "name": shorten_name(nganh),
                "sl": int(sl),
                "dt": dt,
                "tg": tg,
                "dt_ck": dt_ck,
                "tang_giam_ck": tang_giam_ck,
                "ht": dt / tg if tg > 0 else 0.0
            })
    parsed_bi.sort(key=lambda x: x["dt"], reverse=True)
    
    config_map = {}
    for c in config_rows:
        ten = get_key_val(c, "ngành hàng", "nhóm ngành hàng", default=None)
        phan_loai = parse_number(get_key_val(c, "phân loại", "loại", default=0.0))
        if ten:
            config_map[str(ten).lower().strip()] = phan_loai

    parsed_td = []
    cnt_dk = 0
    for r in td_rows:
        nganh = get_key_val(r, "maingroupname", "main group name", "nhóm ngành hàng", "nhóm ngành hàng chính", default=None)
        if not nganh or str(nganh).strip().upper() == "N/A":
            continue
        nganh_clean = str(nganh).lower().strip()
        if config_map and config_map.get(nganh_clean, 0.0) == 0.0:
            continue
            
        tg = parse_number(get_key_val(r, "target", "mục tiêu", default=0.0))
        if tg <= 0:
            continue
            
        sl = parse_number(get_key_val(r, "số lượng", "quantity", default=0.0))
        dt = parse_number(get_key_val(r, "doanh thu", default=0.0))
        
        is_dt = False
        if dt > 0 and (sl == 0 or abs((dt / tg) - 1) < abs((sl / tg) - 1)):
            is_dt = True
            
        actual = dt if is_dt else sl
        ht_target = actual / tg
        ht_du_kien = ((actual / days_passed) * days_in_month) / tg if days_passed > 0 else 0.0
        
        if ht_du_kien >= 1.0:
            cnt_dk += 1
            
        con_lai = max(0.0, tg - actual)
        parsed_td.append({
            "name": shorten_name(nganh),
            "actual": actual,
            "con_lai": con_lai,
            "target": tg,
            "ht": ht_target,
            "ht_dk": ht_du_kien,
            "unit": "TR" if is_dt else "SP"
        })
        
    td_done = [x for x in parsed_td if x["ht_dk"] >= 1.0 and x["actual"] > 0]
    td_pending = [x for x in parsed_td if x["ht_dk"] < 1.0 and x["actual"] > 0]
    td_done.sort(key=lambda x: x["ht_dk"], reverse=True)
    td_pending.sort(key=lambda x: x["ht_dk"], reverse=True)
    
    # 6 Larger Hero KPI Cards Grid (2x3)
    body_contents = [
        {
            "type": "box",
            "layout": "horizontal",
            "margin": "md",
            "spacing": "sm",
            "contents": [
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "backgroundColor": "#1e40af",
                    "paddingAll": "md",
                    "cornerRadius": "md",
                    "contents": [
                        {"type": "text", "text": "💰 DT Lũy Kế", "size": "xs", "color": "#ffffff", "weight": "bold", "align": "center"},
                        {"type": "text", "text": f"{fmt_num(tDT)} Tr ({totalHT*100:.0f}%)", "size": "sm", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "backgroundColor": "#475569",
                    "paddingAll": "md",
                    "cornerRadius": "md",
                    "contents": [
                        {"type": "text", "text": "🎯 Mục Tiêu Tháng", "size": "xs", "color": "#ffffff", "weight": "bold", "align": "center"},
                        {"type": "text", "text": f"{fmt_num(tTG)} Tr", "size": "sm", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                }
            ]
        },
        {
            "type": "box",
            "layout": "horizontal",
            "margin": "xs",
            "spacing": "sm",
            "contents": [
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "backgroundColor": "#ef4444",
                    "paddingAll": "md",
                    "cornerRadius": "md",
                    "contents": [
                        {"type": "text", "text": "📉 Còn Thiếu", "size": "xs", "color": "#ffffff", "weight": "bold", "align": "center"},
                        {"type": "text", "text": f"{fmt_num(revRem)} Tr", "size": "sm", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "backgroundColor": "#f59e0b",
                    "paddingAll": "md",
                    "cornerRadius": "md",
                    "contents": [
                        {"type": "text", "text": "🔮 Dự Kiến Tháng", "size": "xs", "color": "#ffffff", "weight": "bold", "align": "center"},
                        {"type": "text", "text": f"{fmt_num(totalDKThang)} Tr ({totalHTDK*100:.0f}%)", "size": "sm", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                }
            ]
        },
        {
            "type": "box",
            "layout": "horizontal",
            "margin": "xs",
            "spacing": "sm",
            "contents": [
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "backgroundColor": "#9333ea",
                    "paddingAll": "md",
                    "cornerRadius": "md",
                    "contents": [
                        {"type": "text", "text": "💳 Trả Góp", "size": "xs", "color": "#ffffff", "weight": "bold", "align": "center"},
                        {"type": "text", "text": f"{fmt_num(tTC)} Tr ({totalTyLeTC*100:.0f}%)", "size": "sm", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "backgroundColor": "#10b981",
                    "paddingAll": "md",
                    "cornerRadius": "md",
                    "contents": [
                        {"type": "text", "text": "🏆 Thi Đua Đạt", "size": "xs", "color": "#ffffff", "weight": "bold", "align": "center"},
                        {"type": "text", "text": f"{cnt_dk}/{len(parsed_td)} Nhóm", "size": "sm", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                }
            ]
        },
        
        # Progress Card Container
        {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#f0f9ff",
            "borderColor": "#bae6fd",
            "borderWidth": "1px",
            "paddingAll": "sm",
            "cornerRadius": "md",
            "margin": "md",
            "contents": [
                {"type": "text", "text": "📈 TIẾN ĐỘ HOÀN THÀNH THÁNG", "size": "xxs", "color": "#0f172a", "align": "center", "weight": "bold"},
                {"type": "text", "text": f"{totalHT*100:.0f}%", "size": "lg", "color": get_color_class(totalHT), "weight": "bold", "align": "center"},
                {
                    "type": "box",
                    "layout": "vertical",
                    "backgroundColor": "#e2e8f0",
                    "height": "6px",
                    "cornerRadius": "md",
                    "margin": "xs",
                    "contents": [
                        {
                            "type": "box",
                            "layout": "vertical",
                            "backgroundColor": get_color_class(totalHT),
                            "height": "6px",
                            "cornerRadius": "md",
                            "width": f"{min(100, round(totalHT * 100))}%",
                            "contents": [{"type": "filler"}]
                        }
                    ]
                },
                {"type": "text", "text": f"🎯 Mục tiêu hôm nay: {fmt_num(target_today)} Tr/ngày", "size": "xs", "color": "#d97706", "weight": "bold", "align": "center", "margin": "xs"}
            ]
        }
    ]
    
    # BẢNG 1: CHI TIẾT DOANH THU LŨY KẾ (Bảng chính phẳng, sạch mắt)
    table_card_contents = [
        {"type": "text", "text": "📊 CHI TIẾT DOANH THU LŨY KẾ", "size": "xxs", "color": "#0284c7", "weight": "bold", "margin": "xs"}
    ]
    headers1 = ["STT", "Ngành hàng", "SL", "DTQĐ", "Target", "%HT"]
    weights1 = [1, 3, 1, 2, 2, 2]
    aligns1 = ["start", "start", "center", "center", "center", "end"]
    table_card_contents.append(make_table_header(headers1, weights1, aligns1, bg_color="#0284c7"))
    table_card_contents.append({"type": "separator", "color": "#cbd5e1", "margin": "xs"})
    
    for idx, b in enumerate(parsed_bi[:6]):
        vals = [idx+1, b['name'], fmt_num(b["sl"]), fmt_num(b["dt"]), fmt_num(b["tg"]), f"{b['ht']*100:.0f}%"]
        colors = ["#64748b", "#0f172a", "#0f172a", "#0284c7", "#475569", get_color_class(b["ht"])]
        table_card_contents.append(make_table_row(vals, weights1, aligns1, colors))
        
        # Mini 3px progress bar
        table_card_contents.append({
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#e2e8f0",
            "height": "3px",
            "cornerRadius": "sm",
            "margin": "xs",
            "contents": [
                {
                    "type": "box",
                    "layout": "vertical",
                    "backgroundColor": get_color_class(b["ht"]),
                    "height": "3px",
                    "cornerRadius": "sm",
                    "width": f"{min(100, round(b['ht'] * 100))}%",
                    "contents": [{"type": "filler"}]
                }
            ]
        })
    
    tot_sl = sum(x["sl"] for x in parsed_bi)
    tot_vals = ["⭐", "TỔNG CỘNG", fmt_num(tot_sl), fmt_num(tDT), fmt_num(tTG), f"{totalHT*100:.0f}%"]
    tot_colors = ["#ffffff", "#ffffff", "#ffffff", "#ffffff", "#ffffff", "#ffffff"]
    table_card_contents.append(make_table_row(tot_vals, weights1, aligns1, tot_colors, bold=True, bg_color="#f59e0b"))

    body_contents.append({
        "type": "box",
        "layout": "vertical",
        "backgroundColor": "#ffffff",
        "borderColor": "#e2e8f0",
        "borderWidth": "1px",
        "cornerRadius": "md",
        "paddingAll": "sm",
        "margin": "md",
        "contents": table_card_contents
    })

    # BẢNG 2: BẢNG TỶ TRỌNG & TĂNG TRƯỜNG SO VỚI CÙNG KỲ (Card riêng biệt)
    growth_card_contents = [
        {"type": "text", "text": "📈 TỶ TRỌNG & TĂNG TRƯỜNG CÙNG KỲ", "size": "xxs", "color": "#0f766e", "weight": "bold", "margin": "xs"}
    ]
    headers2 = ["STT", "Ngành hàng", "Tỷ trọng", "vs Cùng kỳ"]
    weights2 = [1, 3, 2, 5]
    aligns2 = ["start", "start", "center", "end"]
    growth_card_contents.append(make_table_header(headers2, weights2, aligns2, bg_color="#0f766e"))

    for idx, b in enumerate(parsed_bi[:6]):
        ty_trong = (b["dt"] / tDT * 100) if tDT > 0 else 0.0
        dt_ck_val = b.get("dt_ck", 0.0)
        pct_ck = b.get("tang_giam_ck", 0.0)
        
        if dt_ck_val > 0 or pct_ck != 0:
            diff_ck = b["dt"] - dt_ck_val if dt_ck_val > 0 else (b["dt"] * (pct_ck / 100.0) if pct_ck != 0 else 0.0)
            sign_str = "+" if diff_ck >= 0 else ""
            pct_sign_str = "+" if pct_ck >= 0 else ""
            if diff_ck >= 0:
                growth_text = f"▲ {pct_sign_str}{pct_ck:.1f}% ({sign_str}{fmt_num(diff_ck)} Tr)"
                growth_color = "#16a34a"
            else:
                growth_text = f"▼ {pct_ck:.1f}% (-{fmt_num(abs(diff_ck))} Tr)"
                growth_color = "#dc2626"
        else:
            diff_tg = b["dt"] - b["tg"]
            pct_tg = (diff_tg / b["tg"] * 100) if b["tg"] > 0 else 0.0
            sign_str = "+" if diff_tg >= 0 else ""
            pct_sign_str = "+" if pct_tg >= 0 else ""
            if diff_tg >= 0:
                growth_text = f"▲ {pct_sign_str}{pct_tg:.1f}% ({sign_str}{fmt_num(diff_tg)} Tr)"
                growth_color = "#16a34a"
            else:
                growth_text = f"▼ {pct_tg:.1f}% (-{fmt_num(abs(diff_tg))} Tr)"
                growth_color = "#dc2626"

        row2_vals = [idx+1, b['name'], f"{ty_trong:.0f}%", growth_text]
        row2_colors = ["#64748b", "#0f172a", "#0284c7", growth_color]
        growth_card_contents.append(make_table_row(row2_vals, weights2, aligns2, row2_colors))

    tDT_CK = sum(b.get("dt_ck", 0.0) for b in parsed_bi)
    if tDT_CK > 0:
        diff_total_ck = tDT - tDT_CK
        pct_total_ck = (diff_total_ck / tDT_CK * 100)
        sign_str = "+" if diff_total_ck >= 0 else ""
        pct_sign_str = "+" if pct_total_ck >= 0 else ""
        if diff_total_ck >= 0:
            total_growth_str = f"▲ {pct_sign_str}{pct_total_ck:.1f}% ({sign_str}{fmt_num(diff_total_ck)} Tr)"
        else:
            total_growth_str = f"▼ {pct_total_ck:.1f}% (-{fmt_num(abs(diff_total_ck))} Tr)"
    else:
        diff_total_tg = tDT - tTG
        pct_total_tg = (diff_total_tg / tTG * 100) if tTG > 0 else 0.0
        sign_str = "+" if diff_total_tg >= 0 else ""
        pct_sign_str = "+" if pct_total_tg >= 0 else ""
        if diff_total_tg >= 0:
            total_growth_str = f"▲ {pct_sign_str}{pct_total_tg:.1f}% ({sign_str}{fmt_num(diff_total_tg)} Tr)"
        else:
            total_growth_str = f"▼ {pct_total_tg:.1f}% (-{fmt_num(abs(diff_total_tg))} Tr)"

    tot2_vals = ["⭐", "TỔNG CỘNG", "100%", total_growth_str]
    tot2_colors = ["#ffffff", "#ffffff", "#ffffff", "#ffffff"]
    growth_card_contents.append(make_table_row(tot2_vals, weights2, aligns2, tot2_colors, bold=True, bg_color="#0d9488"))

    body_contents.append({
        "type": "box",
        "layout": "vertical",
        "backgroundColor": "#f0fdfa",
        "borderColor": "#99f6e4",
        "borderWidth": "1px",
        "cornerRadius": "md",
        "paddingAll": "sm",
        "margin": "md",
        "contents": growth_card_contents
    })

    body_contents.append({
        "type": "box",
        "layout": "vertical",
        "backgroundColor": "#f0fdfa",
        "borderColor": "#99f6e4",
        "borderWidth": "1px",
        "cornerRadius": "md",
        "paddingAll": "sm",
        "margin": "md",
        "contents": growth_card_contents
    })

    # Smart Insight Card
    body_contents.append({
        "type": "box",
        "layout": "vertical",
        "backgroundColor": "#fffbeb",
        "borderColor": "#fde68a",
        "borderWidth": "1px",
        "cornerRadius": "md",
        "paddingAll": "sm",
        "margin": "md",
        "contents": [
            {
                "type": "text",
                "text": f"💡 Cần trung bình {fmt_num(target_today)} Tr/ngày trong {days_remaining} ngày còn lại để cán đích {fmt_num(tTG)} Tr.",
                "size": "xxs",
                "color": "#92400e",
                "wrap": True,
                "weight": "bold"
            }
        ]
    })

    # Thi Đua ĐÃ ĐẠT Card
    if td_done:
        done_contents = [
            {"type": "text", "text": f"🏆 THI ĐUA DỰ KIẾN ĐẠT ({len(td_done)} Nhóm)", "size": "xxs", "color": "#15803d", "weight": "bold", "margin": "xs"}
        ]
        for idx, t in enumerate(td_done[:6]):
            done_contents.append(make_thidua_progress_row(idx+1, t["name"], None, t["ht_dk"], t["unit"]))
            
        body_contents.append({
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#f0fdf4",
            "borderColor": "#bbf7d0",
            "borderWidth": "1px",
            "cornerRadius": "md",
            "paddingAll": "sm",
            "margin": "md",
            "contents": done_contents
        })
            
    # Thi Đua CHƯA ĐẠT Card
    if td_pending:
        pending_contents = [
            {"type": "text", "text": f"🎯 THI ĐUA CHƯA ĐẠT (Top {min(8, len(td_pending))}/{len(td_pending)} Nhóm Cần Tập Trung)", "size": "xxs", "color": "#b91c1c", "weight": "bold", "margin": "xs"}
        ]
        for idx, t in enumerate(td_pending[:8]):
            con_lai_str = fmt_num(t['con_lai'])
            pending_contents.append(make_thidua_progress_row(idx+1, t["name"], con_lai_str, t["ht_dk"], t["unit"]))
            
        body_contents.append({
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#fef2f2",
            "borderColor": "#fecaca",
            "borderWidth": "1px",
            "cornerRadius": "md",
            "paddingAll": "sm",
            "margin": "md",
            "contents": pending_contents
        })

    flex_bubble = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#1e40af",
            "paddingAll": "md",
            "contents": [
                {"type": "text", "text": "📊 BÁO CÁO LŨY KẾ CỤM SAVICO", "weight": "bold", "size": "sm", "color": "#ffffff", "align": "center"},
                {"type": "text", "text": f"🕒 Cập nhật: {now_str} • {status_badge_text}", "size": "xxs", "color": "#dbeafe", "align": "center", "margin": "xs"}
            ]
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#ffffff",
            "paddingAll": "md",
            "contents": body_contents
        }
    }

    return flex_bubble

def build_nhanvien_flex():
    data = get_dashboard_data("Config_ThiDua,Data_NV_BI,Data_BI,Data_Realtime_NV")
    config_rows = data.get("Config_ThiDua", [])
    bi_rows = data.get("Data_BI", [])
    nv_rows = data.get("Data_Realtime_NV", [])
    if not nv_rows:
        nv_rows = data.get("Data_NV_BI", [])
        
    tz = pytz.timezone('Asia/Ho_Chi_Minh')
    now_str = datetime.now(tz).strftime("%H:%M - %d/%m/%Y")
    
    tDT = sum(parse_number(get_key_val(b, "Doanh thu Quy đổi", "Doanh thu", default=0.0)) for b in bi_rows)
    total_target = sum(parse_number(get_key_val(b, "Target", "target", default=0.0)) for b in bi_rows)
    if total_target <= 0:
        total_target = 1500.0
        
    emp_targets = {}
    active_staff_names = {}
    
    # 1. Đọc cấu hình Target đã Khóa từ Supabase (Chế độ 60-40 chia theo năng lực)
    lock_config = get_locked_target_config()
    if lock_config and lock_config.get("is_locked") and lock_config.get("staff"):
        locked_staff = lock_config.get("staff", [])
        initial_ratios = {}
        sum_ratios = 0.0
        
        for s in locked_staff:
            raw_name = str(s.get("name", "")).strip()
            user_id = str(s.get("userId", "")).strip()
            if " - " in raw_name:
                raw_name = raw_name.split(" - ")[-1].strip()
            if not raw_name:
                continue
                
            locked_ratio = parse_number(s.get("lockedRatio", s.get("targetRatio", 0.0)))
            initial_ratios[raw_name] = locked_ratio
            sum_ratios += locked_ratio
            active_staff_names[raw_name.upper()] = raw_name
            if user_id:
                active_staff_names[user_id.upper()] = raw_name
            
        if sum_ratios <= 0:
            sum_ratios = 1.0
            
        for raw_name, ratio in initial_ratios.items():
            norm_ratio = ratio / sum_ratios
            emp_targets[raw_name] = norm_ratio * total_target
            
    # 2. Nếu chưa khóa target, đọc từ Config_ThiDua làm mặc định
    if not emp_targets and config_rows:
        for r in config_rows:
            user_id = get_key_val(r, "user", "User", default=None)
            if not user_id or str(user_id).strip() == "":
                continue
                
            emp_name = get_key_val(r, "user-họ và tên", "Họ và tên", "tên nhân viên", default=None)
            if not emp_name:
                continue
            emp_name_str = str(emp_name).strip()
            
            pct = parse_number(get_key_val(r, "tỷ lệ %", "% chia", default=0.0))
            ratio = pct if pct <= 1.0 else pct / 100.0
            
            emp_targets[emp_name_str] = ratio * total_target
            active_staff_names[emp_name_str.upper()] = emp_name_str

    emp_actuals = {}
    for r in nv_rows:
        name = get_key_val(r, "staffUserName", "tên nv", "Họ và tên", "user", "mã nv", default=None)
        if not name: 
            continue
        name_str = str(name).strip()
        actual = parse_number(get_key_val(r, "Doanh thu Quy đổi", "Doanh thu", "Value_Compe", default=0.0))
        emp_actuals[name_str] = emp_actuals.get(name_str, 0.0) + actual

    emp_list = []
    processed_upper = set()
    for staff_upper, clean_name in active_staff_names.items():
        if clean_name.upper() in processed_upper:
            continue
        processed_upper.add(clean_name.upper())
        
        actual = 0.0
        for act_name, val in emp_actuals.items():
            if act_name.upper() == staff_upper or act_name.upper() == clean_name.upper():
                actual += val
                
        target = emp_targets.get(clean_name, 0.0)
        pct_ht = (actual / target) if target > 0 else 0.0
        emp_list.append({
            "name": clean_name,
            "actual": actual,
            "target": target,
            "pct": pct_ht
        })
        
    emp_list.sort(key=lambda x: x["actual"], reverse=True)
    totalHT = tDT / total_target if total_target > 0 else 0.0
    
    # Larger KPI Cards
    body_contents = [
        {
            "type": "box",
            "layout": "horizontal",
            "margin": "md",
            "spacing": "sm",
            "contents": [
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "backgroundColor": "#0284c7",
                    "paddingAll": "md",
                    "cornerRadius": "md",
                    "contents": [
                        {"type": "text", "text": "🏪 DT Cửa Hàng", "size": "xs", "color": "#ffffff", "weight": "bold", "align": "center"},
                        {"type": "text", "text": f"{fmt_num(tDT)} Tr", "size": "sm", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "backgroundColor": "#475569",
                    "paddingAll": "md",
                    "cornerRadius": "md",
                    "contents": [
                        {"type": "text", "text": "🎯 Mục Tiêu Chung", "size": "xs", "color": "#ffffff", "weight": "bold", "align": "center"},
                        {"type": "text", "text": f"{fmt_num(total_target)} Tr", "size": "sm", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                }
            ]
        },
        {
            "type": "box",
            "layout": "horizontal",
            "margin": "xs",
            "spacing": "sm",
            "contents": [
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "backgroundColor": "#9333ea",
                    "paddingAll": "md",
                    "cornerRadius": "md",
                    "contents": [
                        {"type": "text", "text": "👥 Số Nhân Viên", "size": "xs", "color": "#ffffff", "weight": "bold", "align": "center"},
                        {"type": "text", "text": f"{len(emp_list)} Nhân Sự", "size": "sm", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "backgroundColor": "#10b981",
                    "paddingAll": "md",
                    "cornerRadius": "md",
                    "contents": [
                        {"type": "text", "text": "📈 % Đạt Cửa Hàng", "size": "xs", "color": "#ffffff", "weight": "bold", "align": "center"},
                        {"type": "text", "text": f"{totalHT*100:.0f}%", "size": "sm", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                }
            ]
        },
        {"type": "separator", "color": "#cbd5e1", "margin": "md"},
        {"type": "text", "text": "👥 CHI TIẾT XẾP HẠNG DOANH THU NHÂN VIÊN", "size": "xxs", "color": "#0284c7", "weight": "bold", "margin": "md"},
    ]
    
    headers = ["#", "👤 NHÂN VIÊN", "🎯 ĐẠT / TARGET", "%HT", "⌛ CÒN LẠI"]
    weights = [1, 3, 3, 2, 2]
    aligns = ["start", "start", "center", "center", "end"]
    body_contents.append(make_table_header(headers, weights, aligns, bg_color="#0f766e"))
    body_contents.append({"type": "separator", "color": "#cbd5e1", "margin": "xs"})
    
    for idx, e in enumerate(emp_list):
        crown = "🥇" if idx == 0 else ("🥈" if idx == 1 else ("🥉" if idx == 2 else idx+1))
        short_name = shorten_staff_name(e["name"])
        actual_target_str = f"{fmt_num(e['actual'])} / {fmt_num(e['target'])}"
        con_lai_val = max(0.0, e['target'] - e['actual'])
        con_lai_str = fmt_num(con_lai_val)
        
        vals = [crown, short_name, actual_target_str, f"{e['pct']*100:.0f}%", con_lai_str]
        colors = ["#d97706" if idx < 3 else "#64748b", "#0f172a", "#0284c7", get_color_class(e["pct"]), "#dc2626"]
        bg_row = "#fef3c7" if idx < 3 else None
        body_contents.append(make_table_row(vals, weights, aligns, colors, bold=(idx < 3), bg_color=bg_row))
        body_contents.append({"type": "separator", "color": "#f1f5f9", "margin": "xs"})
        
    flex_bubble = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#0f766e",
            "paddingAll": "md",
            "contents": [
                {"type": "text", "text": "🏆 BÁO CÁO NHÂN VIÊN CỤM SAVICO", "weight": "bold", "size": "sm", "color": "#ffffff", "align": "center"},
                {"type": "text", "text": f"🕒 Cập nhật: {now_str}", "size": "xxs", "color": "#ccfbf1", "align": "center", "margin": "xs"}
            ]
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#ffffff",
            "paddingAll": "md",
            "contents": body_contents
        }
    }
    return flex_bubble

def build_realtime_flex():
    data = get_dashboard_data("Data_BI,Data_ThiDua,Config_ThiDua,Data_Realtime_BI,Data_Realtime_ThiDua")
    config_rows = data.get("Config_ThiDua", [])
    bi_rows = data.get("Data_BI", [])
    rt_rows = data.get("Data_Realtime_BI", [])
    rt_td_rows = data.get("Data_Realtime_ThiDua", [])
    td_rows = data.get("Data_ThiDua", [])
    
    tz = pytz.timezone('Asia/Ho_Chi_Minh')
    now = datetime.now(tz)
    now_str = now.strftime("%H:%M - %d/%m/%Y")
    
    lk_tDT = sum(parse_number(get_key_val(b, "Doanh thu Quy đổi", "Doanh thu", default=0.0)) for b in bi_rows)
    lk_tTG = sum(parse_number(get_key_val(b, "Target", "target", default=0.0)) for b in bi_rows)
    if lk_tTG <= 0:
        lk_tTG = 1500.0
        
    current_day = now.day
    days_in_month = (datetime(now.year, now.month + 1, 1) - datetime(now.year, now.month, 1)).days if now.month < 12 else 31
    days_passed = days_in_month if current_day == 1 else current_day - 1
    days_remaining = max(1, days_in_month - days_passed)
    
    is_weekend = now.weekday() in [5, 6]
    target_co_dinh = lk_tTG / days_in_month
    target_bu_tru = (lk_tTG - lk_tDT) / days_remaining
    if target_bu_tru < 0:
        target_bu_tru = 0.0
        
    holiday_target = 0.0
    if config_rows:
        for r in config_rows:
            day_val = parse_number(get_key_val(r, "ngày", "Ngày", default=0.0))
            if int(day_val) == current_day:
                holiday_target = parse_number(get_key_val(r, "Mục tiêu", "mục tiêu ngày", "mục tiêu", default=0.0))
                break
        
    target_today = 0.0
    if holiday_target > 0:
        target_today = holiday_target
    else:
        target_today = target_co_dinh * 2 if is_weekend else max(target_co_dinh, target_bu_tru)

    rt_total = 0.0
    rt_tTC = 0.0
    rt_tSL = 0.0
    rt_tTarget = 0.0
    parsed_rt_bi = []
    
    for r in rt_rows:
        nganh = get_key_val(r, "Nhóm Ngành Hàng", "nhóm ngành hàng", "Ngành hàng", "salegroupmastername", default=None)
        if not nganh or str(nganh).strip().upper() == "N/A":
            continue
        dtqd = max(
            parse_number(get_key_val(r, "revenue_KFactor_RT")),
            parse_number(get_key_val(r, "revenue_RT")),
            parse_number(get_key_val(r, "Doanh thu", "doanh thu quy đổi"))
        )
        sl = max(
            parse_number(get_key_val(r, "quantity_RT")),
            parse_number(get_key_val(r, "quantity_KFactor")),
            parse_number(get_key_val(r, "số lượng"))
        )
        targetDay = max(
            parse_number(get_key_val(r, "revenue_KFactor_AVEDay")),
            parse_number(get_key_val(r, "target_Day"))
        )
        dtTC = parse_number(get_key_val(r, "revenue_Installment"))
        
        rt_total += dtqd
        rt_tTC += dtTC
        rt_tSL += sl
        rt_tTarget += targetDay
        
        if dtqd > 0 or sl > 0:
            parsed_rt_bi.append({
                "name": shorten_name(nganh),
                "sl": int(sl),
                "dt": dtqd,
                "tg": targetDay,
                "ht": dtqd / targetDay if targetDay > 0 else 0.0
            })
            
    parsed_rt_bi.sort(key=lambda x: x["dt"], reverse=True)
    htChung = rt_total / target_today if target_today > 0 else 0.0
    thieuDTRT = max(0.0, target_today - rt_total)
    rt_tc_pct = (rt_tTC / rt_total * 100) if rt_total > 0 else 0.0
    
    tongDTHienTai = lk_tDT + rt_total
    duKienThang = (tongDTHienTai / current_day) * days_in_month if current_day > 0 else 0.0
    ptDuKienThang = (duKienThang / lk_tTG * 100) if lk_tTG > 0 else 0.0
    
    elapsed_hours = (now.hour + now.minute / 60.0) - 9.0
    if elapsed_hours < 0: elapsed_hours = 0.0
    if elapsed_hours > 13: elapsed_hours = 13.0
    time_ratio = elapsed_hours / 13.0
    
    is_rt_on_track = htChung >= time_ratio
    status_badge_text = "🟢 Vượt nhịp độ" if is_rt_on_track else "🔴 Chậm nhịp độ"

    bi_map = {x["name"].lower().strip(): x["dt"] for x in parsed_rt_bi}
    
    thi_dua_luy_ke = {}
    for r in td_rows:
        nganh = get_key_val(r, "maingroupname", "main group name", "nhóm ngành hàng", default=None)
        if not nganh or str(nganh).strip().upper() == "N/A":
            continue
        nganh_clean = str(nganh).lower().strip()
        tg = parse_number(get_key_val(r, "target", "mục tiêu", default=0.0))
        sl = parse_number(get_key_val(r, "số lượng", default=0.0))
        dt = parse_number(get_key_val(r, "doanh thu", default=0.0))
        
        is_dt = False
        if nganh_clean == "điện tử tcl" or tg > 150.0:
            is_dt = True
        elif dt > 0 and (sl == 0 or abs((dt / tg) - 1) < abs((sl / tg) - 1)):
            is_dt = True
            
        actual = dt if is_dt else sl
        mt_ngay = max(0.0, (tg - actual) / days_remaining) if days_remaining > 0 else 0.0
        thi_dua_luy_ke[nganh_clean] = {
            "mt_ngay": mt_ngay,
            "is_dt": is_dt,
            "target_thang": tg,
            "lk_thuc_hien": actual
        }

    config_map = {}
    for c in config_rows:
        ten = get_key_val(c, "ngành hàng", "nhóm ngành hàng", default=None)
        phan_loai = parse_number(get_key_val(c, "phân loại", "loại", default=0.0))
        if ten:
            config_map[str(ten).lower().strip()] = phan_loai

    parsed_td = []
    rt_cntVD = 0
    for r in rt_td_rows:
        nganh = get_key_val(r, "maingroupname", "main group name", "nhóm ngành hàng", default=None)
        if not nganh or str(nganh).strip().upper() == "N/A":
            continue
        nganh_clean = str(nganh).lower().strip()
        if config_map and config_map.get(nganh_clean, 0.0) == 0.0:
            continue
            
        lk_info = thi_dua_luy_ke.get(nganh_clean, {"mt_ngay": 0.0, "is_dt": False, "target_thang": 0.0, "lk_thuc_hien": 0.0})
        
        rt_dt = max(bi_map.get(nganh_clean, 0.0), parse_number(get_key_val(r, "revenue_RT")), parse_number(get_key_val(r, "revenue_KFactor_RT")), parse_number(get_key_val(r, "doanh thu")))
        rt_sl = max(parse_number(get_key_val(r, "quantity_RT")), parse_number(get_key_val(r, "quantity_KFactor")), parse_number(get_key_val(r, "số lượng")))
        target_day = parse_number(get_key_val(r, "target_Day"))
        
        if not lk_info["is_dt"]:
            if target_day > 0:
                devDT = abs((rt_dt / target_day) - 1) if rt_dt > 0 else 1.0
                devSL = abs((rt_sl / target_day) - 1) if rt_sl > 0 else 1.0
                if rt_dt > 0 and (rt_sl == 0 or devDT < devSL):
                    lk_info["is_dt"] = True
            elif rt_dt > 0 and rt_sl == 0:
                lk_info["is_dt"] = True
                
        actual = rt_dt if lk_info["is_dt"] else rt_sl
        ht_target = actual / lk_info["mt_ngay"] if lk_info["mt_ngay"] > 0 else (1.0 if actual > 0 else 0.0)
        
        if ht_target >= 1.0:
            rt_cntVD += 1
            
        con_lai = max(0.0, lk_info["mt_ngay"] - actual)
        parsed_td.append({
            "name": shorten_name(nganh),
            "actual": actual,
            "con_lai": con_lai,
            "target": lk_info["mt_ngay"],
            "ht": ht_target,
            "unit": "TR" if lk_info["is_dt"] else "SP"
        })
        
    td_done = [x for x in parsed_td if x["ht"] >= 1.0 and x["actual"] > 0]
    td_pending = [x for x in parsed_td if x["ht"] < 1.0 and x["actual"] > 0]
    td_done.sort(key=lambda x: x["ht"], reverse=True)
    td_pending.sort(key=lambda x: x["ht"], reverse=True)
    
    # Overview & Progress Container Card
    overview_contents = [
        {
            "type": "text",
            "text": f"🔥 Với DT hôm nay đạt 🚀 {fmt_num(rt_total)} TR, LK siêu thị đạt 💰 {fmt_num(tongDTHienTai)} TR. DK hết tháng về đích 🎯 {fmt_num(duKienThang)} TR ({ptDuKienThang:.1f}%).",
            "size": "xxs",
            "color": "#1e40af",
            "weight": "bold",
            "wrap": True,
            "align": "center"
        },
        {"type": "separator", "color": "#cbd5e1", "margin": "sm"},
        {
            "type": "box",
            "layout": "horizontal",
            "margin": "sm",
            "contents": [
                {"type": "text", "text": "🕒 TIẾN ĐỘ THỜI GIAN", "size": "xxs", "color": "#475569", "weight": "bold", "flex": 6},
                {"type": "text", "text": f"(Còn {(13.0 - elapsed_hours):.1f}h)", "size": "xxs", "color": "#64748b", "align": "end", "flex": 4}
            ]
        },
        {
            "type": "box",
            "layout": "horizontal",
            "margin": "xs",
            "alignItems": "center",
            "contents": [
                {"type": "text", "text": f"{time_ratio*100:.0f}%", "size": "xs", "color": "#0284c7", "weight": "bold", "flex": 2},
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 8,
                    "backgroundColor": "#e2e8f0",
                    "height": "8px",
                    "cornerRadius": "md",
                    "contents": [
                        {
                            "type": "box",
                            "layout": "vertical",
                            "backgroundColor": "#0284c7",
                            "height": "8px",
                            "cornerRadius": "md",
                            "width": f"{min(100, round(time_ratio * 100))}%",
                            "contents": [{"type": "filler"}]
                        }
                    ]
                }
            ]
        },
        {
            "type": "box",
            "layout": "horizontal",
            "margin": "sm",
            "contents": [
                {"type": "text", "text": "🎯 TIẾN ĐỘ DT NGÀY", "size": "xxs", "color": "#475569", "weight": "bold", "flex": 6},
                {"type": "text", "text": f"(-{fmt_num(thieuDTRT)} Tr)", "size": "xxs", "color": "#dc2626", "align": "end", "flex": 4}
            ]
        },
        {
            "type": "box",
            "layout": "horizontal",
            "margin": "xs",
            "alignItems": "center",
            "contents": [
                {"type": "text", "text": f"{htChung*100:.0f}%", "size": "xs", "color": get_color_class(htChung), "weight": "bold", "flex": 2},
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 8,
                    "backgroundColor": "#e2e8f0",
                    "height": "8px",
                    "cornerRadius": "md",
                    "contents": [
                        {
                            "type": "box",
                            "layout": "vertical",
                            "backgroundColor": get_color_class(htChung),
                            "height": "8px",
                            "cornerRadius": "md",
                            "width": f"{min(100, round(htChung * 100))}%",
                            "contents": [{"type": "filler"}]
                        }
                    ]
                }
            ]
        }
    ]

    if time_ratio > htChung and (time_ratio - htChung) >= 0.15:
        gap_pct = round((time_ratio - htChung) * 100)
        overview_contents.append({
            "type": "text",
            "text": f"⚠️ Đang chậm {gap_pct}% so với nhịp độ thời gian khung giờ!",
            "size": "xxs",
            "color": "#dc2626",
            "weight": "bold",
            "align": "center",
            "margin": "xs"
        })

    body_contents = [
        {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#f0f9ff",
            "borderColor": "#bae6fd",
            "borderWidth": "1px",
            "paddingAll": "sm",
            "cornerRadius": "md",
            "margin": "xs",
            "contents": overview_contents
        },
        # 4 Hero KPI Cards Grid
        {
            "type": "box",
            "layout": "horizontal",
            "margin": "md",
            "spacing": "sm",
            "contents": [
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "backgroundColor": "#0284c7",
                    "paddingAll": "md",
                    "cornerRadius": "md",
                    "contents": [
                        {"type": "text", "text": "🚀 DT Thực Hiện", "size": "xs", "color": "#ffffff", "weight": "bold", "align": "center"},
                        {"type": "text", "text": f"{fmt_num(rt_total)} Tr ({htChung*100:.0f}%)", "size": "sm", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "backgroundColor": "#475569",
                    "paddingAll": "md",
                    "cornerRadius": "md",
                    "contents": [
                        {"type": "text", "text": "🎯 Mục Tiêu Ngày", "size": "xs", "color": "#ffffff", "weight": "bold", "align": "center"},
                        {"type": "text", "text": f"{fmt_num(target_today)} Tr", "size": "sm", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                }
            ]
        },
        {
            "type": "box",
            "layout": "horizontal",
            "margin": "xs",
            "spacing": "sm",
            "contents": [
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "backgroundColor": "#9333ea",
                    "paddingAll": "md",
                    "cornerRadius": "md",
                    "contents": [
                        {"type": "text", "text": "💳 Trả Góp", "size": "xs", "color": "#ffffff", "weight": "bold", "align": "center"},
                        {"type": "text", "text": f"{fmt_num(rt_tTC)} Tr ({rt_tc_pct:.0f}%)", "size": "sm", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "backgroundColor": "#10b981",
                    "paddingAll": "md",
                    "cornerRadius": "md",
                    "contents": [
                        {"type": "text", "text": "🏆 Thi Đua Đạt", "size": "xs", "color": "#ffffff", "weight": "bold", "align": "center"},
                        {"type": "text", "text": f"{rt_cntVD}/{len(parsed_td)} Nhóm", "size": "sm", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                }
            ]
        }
    ]
    
    # Revenue Table Container Card
    table_card_contents = [
        {"type": "text", "text": "⚡ CHI TIẾT DOANH THU HÔM NAY", "size": "xxs", "color": "#0284c7", "weight": "bold", "margin": "xs"}
    ]
    headers = ["STT", "Ngành hàng", "SL", "DTQĐ", "Target", "%HT"]
    weights = [1, 3, 1, 2, 2, 2]
    aligns = ["start", "start", "center", "center", "center", "end"]
    table_card_contents.append(make_table_header(headers, weights, aligns, bg_color="#0284c7"))
    table_card_contents.append({"type": "separator", "color": "#cbd5e1", "margin": "xs"})
    
    for idx, b in enumerate(parsed_rt_bi[:6]):
        ty_trong_rt = (b["dt"] / rt_total * 100) if rt_total > 0 else 0.0
        vals = [idx+1, f"{b['name']} ({ty_trong_rt:.0f}%)", fmt_num(b["sl"]), fmt_num(b["dt"]), fmt_num(b["tg"]), f"{b['ht']*100:.0f}%"]
        colors = ["#64748b", "#0f172a", "#0f172a", "#0284c7", "#475569", get_color_class(b["ht"])]
        table_card_contents.append(make_table_row(vals, weights, aligns, colors))
        table_card_contents.append({
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#e2e8f0",
            "height": "3px",
            "cornerRadius": "sm",
            "margin": "xs",
            "contents": [
                {
                    "type": "box",
                    "layout": "vertical",
                    "backgroundColor": get_color_class(b["ht"]),
                    "height": "3px",
                    "cornerRadius": "sm",
                    "width": f"{min(100, round(b['ht'] * 100))}%",
                    "contents": [{"type": "filler"}]
                }
            ]
        })
        table_card_contents.append({"type": "separator", "color": "#f1f5f9", "margin": "xs"})
        
    tot_sl = sum(x["sl"] for x in parsed_rt_bi)
    totalHTCol2 = rt_total / rt_tTarget if rt_tTarget > 0 else 0.0
    tot_vals = ["⭐", "TỔNG CỘNG", fmt_num(tot_sl), fmt_num(rt_total), fmt_num(rt_tTarget), f"{totalHTCol2*100:.0f}%"]
    tot_colors = ["#ffffff", "#ffffff", "#ffffff", "#ffffff", "#ffffff", "#ffffff"]
    table_card_contents.append(make_table_row(tot_vals, weights, aligns, tot_colors, bold=True, bg_color="#f59e0b"))

    body_contents.append({
        "type": "box",
        "layout": "vertical",
        "backgroundColor": "#ffffff",
        "borderColor": "#e2e8f0",
        "borderWidth": "1px",
        "cornerRadius": "md",
        "paddingAll": "sm",
        "margin": "md",
        "contents": table_card_contents
    })

    # Insight Box
    rem_hours = max(0.5, 13.0 - elapsed_hours)
    if thieuDTRT > 0:
        req_speed = thieuDTRT / rem_hours
        insight_msg = f"💡 Còn thiếu {fmt_num(thieuDTRT)} Tr để hoàn thành Target ngày ({fmt_num(target_today)} Tr). Cần trung bình ~{req_speed:.1f} Tr/giờ trong {rem_hours:.1f}h còn lại."
    else:
        insight_msg = f"🎉 Xuất sắc! Siêu thị đã hoàn thành Target doanh thu ngày hôm nay ({fmt_num(rt_total)}/{fmt_num(target_today)} Tr)."

    body_contents.append({
        "type": "box",
        "layout": "vertical",
        "backgroundColor": "#fffbeb",
        "borderColor": "#fde68a",
        "borderWidth": "1px",
        "cornerRadius": "md",
        "paddingAll": "sm",
        "margin": "md",
        "contents": [
            {
                "type": "text",
                "text": insight_msg,
                "size": "xxs",
                "color": "#92400e",
                "wrap": True,
                "weight": "bold"
            }
        ]
    })

    # Compete List Cards (Đã đạt & Chưa đạt)
    if td_done:
        done_contents = [
            {"type": "text", "text": "🏆 THI ĐUA ĐÃ ĐẠT CHỈ TIÊU NGÀY", "size": "xxs", "color": "#15803d", "weight": "bold", "margin": "xs"},
            {"type": "separator", "color": "#bbf7d0", "margin": "xs"}
        ]
        for idx, t in enumerate(td_done):
            done_contents.append(make_thidua_progress_row(idx+1, t["name"], None, t["ht"], t["unit"]))
            done_contents.append({"type": "separator", "color": "#f1f5f9", "margin": "xs"})
            
        body_contents.append({
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#f0fdf4",
            "borderColor": "#bbf7d0",
            "borderWidth": "1px",
            "cornerRadius": "md",
            "paddingAll": "sm",
            "margin": "md",
            "contents": done_contents
        })

    if td_pending:
        pending_contents = [
            {"type": "text", "text": "🎯 THI ĐUA CHƯA ĐẠT CHỈ TIÊU NGÀY", "size": "xxs", "color": "#b91c1c", "weight": "bold", "margin": "xs"},
            {"type": "separator", "color": "#fecaca", "margin": "xs"}
        ]
        for idx, t in enumerate(td_pending):
            con_lai_str = fmt_num(t['con_lai'])
            pending_contents.append(make_thidua_progress_row(idx+1, t["name"], con_lai_str, t["ht"], t["unit"]))
            pending_contents.append({"type": "separator", "color": "#f1f5f9", "margin": "xs"})
            
        body_contents.append({
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#fef2f2",
            "borderColor": "#fecaca",
            "borderWidth": "1px",
            "cornerRadius": "md",
            "paddingAll": "sm",
            "margin": "md",
            "contents": pending_contents
        })

    flex_bubble = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#0284c7",
            "paddingAll": "md",
            "contents": [
                {"type": "text", "text": "⚡ BÁO CÁO REALTIME HÔM NAY", "weight": "bold", "size": "sm", "color": "#ffffff", "align": "center"},
                {"type": "text", "text": f"🕒 Cập nhật: {now_str} • {status_badge_text}", "size": "xxs", "color": "#e0f2fe", "align": "center", "margin": "xs"}
            ]
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#ffffff",
            "paddingAll": "md",
            "contents": body_contents
        }
    }
    return flex_bubble
