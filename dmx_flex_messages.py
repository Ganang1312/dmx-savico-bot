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

def parse_growth_rate(val):
    if not val:
        return 0.0
    val_str = str(val).strip().replace(',', '.')
    if val_str.endswith('%'):
        val_str = val_str[:-1].strip()
        return parse_number(val_str)
    
    num = parse_number(val_str)
    if -2.0 <= num <= 2.0 and num != 0:
        return num * 100.0
    return num

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

def make_thidua_progress_row(idx, name, con_lai_str, ht, unit, mt_ngay_str=None):
    ht_pct = round(ht * 100)
    color = get_color_class(ht)
    
    parts = []
    if con_lai_str:
        parts.append(f"-{con_lai_str} {unit}")
    if mt_ngay_str:
        parts.append(f"🎯 {mt_ngay_str}")
        
    if parts:
        display_name = f"{name} ({' • '.join(parts)})"
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
    
    tDT = 0.0
    tTG = 0.0
    tTC = 0.0
    tDTGoc = 0.0
    tDT_CK_total = 0.0
    parsed_bi = []
    
    for b in bi_rows:
        nganh = get_key_val(b, "nhóm ngành hàng", "ngành hàng", "salegroupmastername", default=None)
        if not nganh or str(nganh).strip().upper() == "N/A":
            continue
            
        dt = parse_number(get_key_val(b, "doanh thu quy đổi", "doanh thu", default=0.0))
        sl = parse_number(get_key_val(b, "số lượng", "quantity", default=0.0))
        tg = parse_number(get_key_val(b, "target", default=0.0))
        dtGoc = parse_number(get_key_val(b, "doanh thu", default=0.0))
        dtTC = parse_number(get_key_val(b, "revenue_installment", "doanh thu trả chậm", default=0.0))
        
        tDT += dt
        tTG += tg
        tTC += dtTC
        tDTGoc += dtGoc

        raw_ck_val = get_key_val(b, "rev_kft_riserate_lastmonth", "+/- dtck", "+/- so với ck", default=None)
        dt_ck = 0.0
        tang_giam_ck = 0.0
        if raw_ck_val is not None and str(raw_ck_val).strip() != "":
            tang_giam_ck = parse_growth_rate(raw_ck_val)
            if (1 + tang_giam_ck / 100.0) != 0:
                dt_ck = dt / (1 + tang_giam_ck / 100.0)
        else:
            dt_ck = parse_number(get_key_val(b, "DT Năm ngoái", "doanh thu năm ngoái", "dt năm ngoái", "năm ngoái", "nam ngoai", "doanh thu nam ngoai", "dt nam ngoai", "doanh thu năm ngoái (cùng kỳ)", "doanh thu nam ngoai (cung ky)", "cùng kỳ", "cung ky", "tháng trước", "dt tháng trước", default=0.0))
            if dt_ck > 0:
                tang_giam_ck = ((dt - dt_ck) / dt_ck) * 100.0

        tDT_CK_total += dt_ck

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
    if tTG <= 0:
        tTG = 1500.0
    totalTyLeTC = tTC / tDTGoc if tDTGoc > 0 else (tTC / tDT if tDT > 0 else 0.0)
    
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
        mt_ngay_val = (con_lai / days_remaining) if days_remaining > 0 else 0.0
        if is_dt:
            mt_ngay_str = f"{mt_ngay_val:.1f} Tr/N" if mt_ngay_val > 0 else "0 Tr/N"
        else:
            mt_ngay_str = f"{int(round(mt_ngay_val))} SP/N" if mt_ngay_val >= 1 else (f"{mt_ngay_val:.1f} SP/N" if mt_ngay_val > 0 else "0 SP/N")

        parsed_td.append({
            "name": shorten_name(nganh),
            "actual": actual,
            "con_lai": con_lai,
            "target": tg,
            "ht": ht_target,
            "ht_dk": ht_du_kien,
            "unit": "TR" if is_dt else "SP",
            "mt_ngay_str": mt_ngay_str
        })
        
    td_done = [x for x in parsed_td if x["ht_dk"] >= 1.0]
    td_pending = [x for x in parsed_td if x["ht_dk"] < 1.0]
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

    tDT_CK = tDT_CK_total if tDT_CK_total > 0 else sum(b.get("dt_ck", 0.0) for b in parsed_bi)
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

    flex_bubble_p1 = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#1e40af",
            "paddingAll": "md",
            "contents": [
                {"type": "text", "text": "📊 BÁO CÁO LŨY KẾ (P.1: DOANH THU & TỶ TRỌNG)", "weight": "bold", "size": "sm", "color": "#ffffff", "align": "center"},
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

    body_contents_p2 = []
    # Thi Đua ĐÃ ĐẠT Card
    if td_done:
        done_contents = [
            {"type": "text", "text": f"🏆 THI ĐUA DỰ KIẾN ĐẠT ({len(td_done)} Nhóm)", "size": "xxs", "color": "#15803d", "weight": "bold", "margin": "xs"}
        ]
        for idx, t in enumerate(td_done):
            done_contents.append(make_thidua_progress_row(idx+1, t["name"], None, t["ht_dk"], t["unit"]))
            
        body_contents_p2.append({
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
            {"type": "text", "text": f"🎯 THI ĐUA CHƯA ĐẠT ({len(td_pending)} Nhóm Cần Tập Trung)", "size": "xxs", "color": "#b91c1c", "weight": "bold", "margin": "xs"}
        ]
        for idx, t in enumerate(td_pending):
            con_lai_str = fmt_num(t['con_lai'])
            pending_contents.append(make_thidua_progress_row(idx+1, t["name"], con_lai_str, t["ht_dk"], t["unit"], t.get("mt_ngay_str")))
            
        body_contents_p2.append({
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

    flex_bubble_p2 = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#0f766e",
            "paddingAll": "md",
            "contents": [
                {"type": "text", "text": "🏆 BÁO CÁO LŨY KẾ (P.2: NGÀNH HÀNG THI ĐUA)", "weight": "bold", "size": "sm", "color": "#ffffff", "align": "center"},
                {"type": "text", "text": f"🕒 Cập nhật: {now_str} • {cnt_dk}/{len(parsed_td)} Nhóm Đạt DK", "size": "xxs", "color": "#ccfbf1", "align": "center", "margin": "xs"}
            ]
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#ffffff",
            "paddingAll": "md",
            "contents": body_contents_p2
        }
    }

    return [flex_bubble_p1, flex_bubble_p2]

def shorten_staff_name_user(full_name, user_id=None):
    if not full_name:
        return f"NV-{user_id}" if user_id else "NV"
    full_name = str(full_name).strip()
    extracted_user = user_id
    if " - " in full_name:
        parts = full_name.split(" - ")
        if not extracted_user and parts[0].strip().isdigit():
            extracted_user = parts[0].strip()
        full_name = parts[-1].strip()
    
    name_parts = full_name.split()
    first_name = name_parts[-1] if name_parts else full_name
    
    if extracted_user:
        return f"{first_name}-{extracted_user}"
    return first_name

def build_leaderboard_overview_bubble(emp_list, now_str):
    """
    Tin nhắn 1: Bảng Xếp Hạng Doanh Thu & Thi Đua Tất Cả Nhân Viên (Chuẩn Ảnh 1 mới)
    """
    rows_contents = []
    
    # Header Bảng
    rows_contents.append({
        "type": "box",
        "layout": "horizontal",
        "backgroundColor": "#1e293b",
        "paddingAll": "xs",
        "cornerRadius": "xs",
        "contents": [
            {"type": "text", "text": "#", "size": "xxs", "color": "#ffffff", "weight": "bold", "flex": 1, "align": "center"},
            {"type": "text", "text": "👤 NV", "size": "xxs", "color": "#ffffff", "weight": "bold", "flex": 3, "align": "start"},
            {"type": "text", "text": "⭐ ĐIỂM", "size": "xxs", "color": "#ffffff", "weight": "bold", "flex": 2, "align": "center"},
            {"type": "text", "text": "🏅 TĐ", "size": "xxs", "color": "#ffffff", "weight": "bold", "flex": 2, "align": "center"},
            {"type": "text", "text": "💼 DT", "size": "xxs", "color": "#ffffff", "weight": "bold", "flex": 5, "align": "center"}
        ]
    })
    rows_contents.append({"type": "separator", "color": "#cbd5e1", "margin": "xs"})

    for idx, e in enumerate(emp_list):
        rank = idx + 1
        user_id = str(e.get("user_id", "")).strip()
        full_name = str(e.get("name", "")).strip()
        
        # Tách Tên NV (Dòng 1) và User ID (Dòng 2) chuẩn yêu cầu người dùng
        first_name = full_name.split(" - ")[-1].split()[-1] if full_name else "NV"
        row_bg = "#fef3c7" if rank <= 3 else ("#f8fafc" if rank % 2 == 0 else "#ffffff")
        
        actual_val = e.get("actual", 0.0)
        target_val = e.get("target", 0.0)
        con_lai_val = max(0.0, target_val - actual_val)
        pct_val = e.get("pct", 0.0) * 100.0
        
        score_val = e.get("diem", 90.0 - rank * 3.5)
        
        td_passed = e.get("td_passed", max(1, 10 - rank))
        td_total = e.get("td_total", 23)

        row_box = {
            "type": "box",
            "layout": "horizontal",
            "paddingAll": "xs",
            "backgroundColor": row_bg,
            "margin": "xs",
            "contents": [
                # STT
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "align": "center",
                    "contents": [
                        {
                            "type": "text",
                            "text": str(rank),
                            "weight": "bold",
                            "size": "xxs",
                            "color": "#d97706" if rank <= 3 else "#0f172a",
                            "align": "center"
                        }
                    ]
                },
                # Tên nhân viên dòng 1 (Tên), dòng 2 (User_ID nhỏ hơn)
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 3,
                    "contents": [
                        {
                            "type": "text",
                            "text": first_name,
                            "weight": "bold",
                            "size": "xxs",
                            "color": "#0f172a"
                        },
                        {
                            "type": "text",
                            "text": f"ID:{user_id}" if user_id and user_id != "NV" else "",
                            "size": "xxs",
                            "color": "#64748b",
                            "margin": "xs"
                        }
                    ]
                },
                # Điểm số
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 2,
                    "align": "center",
                    "contents": [
                        {
                            "type": "text",
                            "text": f"{score_val:.1f}",
                            "weight": "bold",
                            "size": "xxs",
                            "color": "#b45309"
                        }
                    ]
                },
                # Thi đua
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 2,
                    "align": "center",
                    "contents": [
                        {
                            "type": "text",
                            "text": f"{td_passed}/{td_total}",
                            "weight": "bold",
                            "size": "xxs",
                            "color": "#0284c7"
                        }
                    ]
                },
                # Doanh thu (Mở rộng flex: 5 để %HT không bao giờ bị cắt 9...)
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 5,
                    "contents": [
                        {
                            "type": "box",
                            "layout": "horizontal",
                            "contents": [
                                {
                                    "type": "text",
                                    "text": f"{fmt_num(actual_val)} / {fmt_num(target_val)}",
                                    "size": "xxs",
                                    "weight": "bold",
                                    "color": "#0284c7",
                                    "flex": 3
                                },
                                {
                                    "type": "text",
                                    "text": f"{pct_val:.0f}%",
                                    "size": "xxs",
                                    "weight": "bold",
                                    "color": "#d97706",
                                    "align": "end",
                                    "flex": 2
                                }
                            ]
                        },
                        {
                            "type": "text",
                            "text": f"⌛ CÒN {fmt_num(con_lai_val)}",
                            "size": "xxs",
                            "color": "#dc2626",
                            "weight": "bold",
                            "align": "end",
                            "margin": "xs"
                        }
                    ]
                }
            ]
        }
        rows_contents.append(row_box)
        rows_contents.append({"type": "separator", "color": "#e2e8f0", "margin": "xs"})

    bubble = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#0f766e",
            "paddingAll": "md",
            "contents": [
                {"type": "text", "text": "🏆 BÁO CÁO XẾP HẠNG DOANH THU & THI ĐUA NV", "weight": "bold", "size": "sm", "color": "#ffffff", "align": "center"},
                {"type": "text", "text": f"🕒 Cập nhật: {now_str} • {len(emp_list)} Nhân Viên", "size": "xxs", "color": "#ccfbf1", "align": "center", "margin": "xs"}
            ]
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#ffffff",
            "paddingAll": "sm",
            "contents": rows_contents
        }
    }
    return bubble

def build_individual_staff_card(e, rank, total_emp=11, now_str="", thi_dua_list=None):
    """
    Tin nhắn 2..N: Thẻ KPI Chi Tiết Từng Nhân Viên (Chuẩn Ảnh 2 mới - Single Card)
    """
    name = e.get("name", "Nhân Viên")
    user_id = e.get("user_id", "90509")
    full_name = str(name).strip()
    first_name = full_name.split(" - ")[-1].split()[-1] if full_name else "NV"
    name_code = f"{first_name}-{user_id}" if user_id else first_name
    
    actual_val = e.get("actual", 0.0)
    target_val = e.get("target", 0.0)
    con_lai_val = max(0.0, target_val - actual_val)
    pct_val = (actual_val / target_val * 100.0) if target_val > 0 else 0.0
    
    score_val = e.get("diem", 96.4 - (rank - 1) * 3.5)
    rank_delta = e.get("rank_delta", 22)
    
    du_kien_pct = pct_val * 1.4 if pct_val > 0 else 100.0
    m_tieu_ngay = max(1, int(round(con_lai_val / 10.0))) if con_lai_val > 0 else 0

    # Phân định màu header & tagline dựa trên Rank thực tế
    bottom_cutoff = int(total_emp * 0.7) if total_emp > 0 else 8
    if rank == 1:
        header_bg = "#eab308" # Vàng Gold
        tagline = "🥇 Dẫn đầu cuộc đua, tuyệt vời!"
    elif rank == 2:
        header_bg = "#64748b" # Xám Bạc
        tagline = "🥈 Á quân xuất sắc!"
    elif rank == 3:
        header_bg = "#d97706" # Đồng Amber
        tagline = "🥉 Top 3 xuất sắc!"
    elif rank > bottom_cutoff:
        header_bg = "#dc2626" # Đỏ Cảnh Báo cho Bottom 30%
        tagline = "⚠️ Cảnh báo: Thuộc nhóm cuối, cần bứt phá!"
    else:
        header_bg = "#0f766e" # Xanh Teal nhóm trung bình
        tagline = "💪 Đang nỗ lực gia tăng tốc độ!"

    if not thi_dua_list:
        thi_dua_list = [
            {"name": "🧺 MÁY GIẶT", "m_tieu": "🏆", "target": 63, "actual": 95, "con_lai": "🏆", "ht": 150.7, "du_kien": 212.4},
            {"name": "🌀 MÁY LỌC KHÔNG KHÍ", "m_tieu": "🏆", "target": 52, "actual": 56, "con_lai": "🏆", "ht": 107.6, "du_kien": 151.7},
            {"name": "📺 Điện tử", "m_tieu": "🏆", "target": 120, "actual": 129, "con_lai": "🏆", "ht": 107.2, "du_kien": 151.0},
            {"name": "🛡️ BẢO HIỂM", "m_tieu": "🏆", "target": 2, "actual": 3, "con_lai": "🏆", "ht": 106.8, "du_kien": 150.5},
            {"name": "💻 Laptop", "m_tieu": "174k", "target": 87, "actual": 85, "con_lai": 2, "ht": 98.2, "du_kien": 138.4},
            {"name": "🏢 ĐIỆN TỬ & ĐIỆN LẠNH LG", "m_tieu": "990k", "target": 119, "actual": 110, "con_lai": 9, "ht": 92.5, "du_kien": 130.4},
            {"name": "💳 TRẢ CHẬM HOMECREDIT", "m_tieu": "780k", "target": 39, "actual": 32, "con_lai": 7, "ht": 82.1, "du_kien": 115.7},
            {"name": "📱 Sim Tổng", "m_tieu": "1", "target": 4, "actual": 3, "con_lai": 1, "ht": 79.2, "du_kien": 111.6},
            {"name": "⌚ DOANH THU ĐỒNG HỒ", "m_tieu": "399k", "target": 16, "actual": 12, "con_lai": 4, "ht": 77.4, "du_kien": 109.1},
            {"name": "🎧 Đồng hồ - Phụ kiện", "m_tieu": "5", "target": 140, "actual": 94, "con_lai": 46, "ht": 67.0, "du_kien": 94.5},
            {"name": "❄️ Tủ Lạnh", "m_tieu": "1.2M", "target": 45, "actual": 30, "con_lai": 15, "ht": 66.7, "du_kien": 93.3},
            {"name": "🧊 Máy Lạnh", "m_tieu": "800k", "target": 30, "actual": 18, "con_lai": 12, "ht": 60.0, "du_kien": 84.0},
            {"name": "♨️ Gia Dụng Nóng", "m_tieu": "500k", "target": 80, "actual": 45, "con_lai": 35, "ht": 56.3, "du_kien": 78.8},
            {"name": "💧 Lọc Nước", "m_tieu": "1M", "target": 20, "actual": 10, "con_lai": 10, "ht": 50.0, "du_kien": 70.0},
            {"name": "🔋 Phụ Kiện Sạc", "m_tieu": "200k", "target": 100, "actual": 48, "con_lai": 52, "ht": 48.0, "du_kien": 67.2},
            {"name": "🛞 Xe Đạp", "m_tieu": "500k", "target": 10, "actual": 4, "con_lai": 6, "ht": 40.0, "du_kien": 56.0},
            {"name": "📺 Tivi OLED", "m_tieu": "2M", "target": 15, "actual": 5, "con_lai": 10, "ht": 33.3, "du_kien": 46.6},
            {"name": "🔊 Loa Karaoke", "m_tieu": "600k", "target": 25, "actual": 8, "con_lai": 17, "ht": 32.0, "du_kien": 44.8},
            {"name": "🍲 Nồi Cơm Điện", "m_tieu": "150k", "target": 60, "actual": 18, "con_lai": 42, "ht": 30.0, "du_kien": 42.0},
            {"name": "💨 Quạt Ép", "m_tieu": "100k", "target": 40, "actual": 11, "con_lai": 29, "ht": 27.5, "du_kien": 38.5},
            {"name": "🔌 Bàn Ủi", "m_tieu": "80k", "target": 35, "actual": 9, "con_lai": 26, "ht": 25.7, "du_kien": 36.0},
            {"name": "⚡ Micro", "m_tieu": "50k", "target": 20, "actual": 4, "con_lai": 16, "ht": 20.0, "du_kien": 28.0},
            {"name": "📶 Cáp Chuyển", "m_tieu": "30k", "target": 50, "actual": 8, "con_lai": 42, "ht": 16.0, "du_kien": 22.4},
        ]

    td_passed = sum(1 for td in thi_dua_list if td.get("ht", 0.0) >= 100 or td.get("con_lai") == "🏆")
    td_total = len(thi_dua_list)
    td_pct = (td_passed / td_total * 100.0) if td_total > 0 else 0.0

    # 4 Pill Tiles vô cùng gọn gàng
    tile_box = {
        "type": "box", "layout": "horizontal", "margin": "xs", "spacing": "xs",
        "contents": [
            {"type": "box", "layout": "vertical", "flex": 1, "backgroundColor": "#0284c7", "paddingAll": "xs", "cornerRadius": "xs", "contents": [{"type": "text", "text": f"🎯 TG:{fmt_num(target_val)}", "size": "xxs", "color": "#ffffff", "weight": "bold", "align": "center"}]},
            {"type": "box", "layout": "vertical", "flex": 1, "backgroundColor": "#15803d", "paddingAll": "xs", "cornerRadius": "xs", "contents": [{"type": "text", "text": f"💰 LK:{fmt_num(actual_val)}", "size": "xxs", "color": "#ffffff", "weight": "bold", "align": "center"}]},
            {"type": "box", "layout": "vertical", "flex": 1, "backgroundColor": "#b91c1c", "paddingAll": "xs", "cornerRadius": "xs", "contents": [{"type": "text", "text": f"⏳ Còn:{fmt_num(con_lai_val)}", "size": "xxs", "color": "#ffffff", "weight": "bold", "align": "center"}]},
            {"type": "box", "layout": "vertical", "flex": 1, "backgroundColor": "#7c3aed", "paddingAll": "xs", "cornerRadius": "xs", "contents": [{"type": "text", "text": f"🔮 DK:{du_kien_pct:.0f}%", "size": "xxs", "color": "#ffffff", "weight": "bold", "align": "center"}]}
        ]
    }

    # Bảng 23 Ngành Hàng Thi Đua Đầy Đủ Tất Cả Cột Chỉ Số
    headers = ["#", "NGÀNH", "MT", "TG", "LK", "CÒN", "%HT", "DK"]
    weights = [1, 4, 2, 2, 2, 2, 2, 2]
    aligns = ["center", "start", "center", "center", "center", "center", "center", "center"]
    
    td_rows = [{
        "type": "box", "layout": "horizontal", "backgroundColor": "#0284c7", "paddingAll": "xs",
        "contents": [{"type": "text", "text": h, "size": "xxs", "weight": "bold", "color": "#ffffff", "align": a, "flex": w} for h, w, a in zip(headers, weights, aligns)]
    }]

    for i, td in enumerate(thi_dua_list, 1):
        ht_val = td.get("ht", 0.0)
        ht_color = "#16a34a" if ht_val >= 100 else ("#0284c7" if ht_val >= 80 else "#d97706")
        ht_str = f"{ht_val:.0f}%" if ht_val >= 100 else f"{ht_val:.1f}%"
        dk_val = td.get("du_kien", 0.0)
        dk_str = f"{dk_val:.0f}%" if dk_val >= 100 else f"{dk_val:.1f}%"
        
        name_s = shorten_name(td["name"])
        mt = str(td.get("m_tieu", "🏆"))
        tg = str(td["target"])
        lk = str(td["actual"])
        cl = str(td["con_lai"])
        
        vals = [str(i), name_s, mt, tg, lk, cl, ht_str, dk_str]
        colors = ["#475569", "#0f172a", "#16a34a" if mt=="🏆" else "#475569", "#475569", "#0284c7", "#dc2626" if cl!="🏆" else "#16a34a", ht_color, "#16a34a"]
        
        td_rows.append({
            "type": "box", "layout": "horizontal", "margin": "xs",
            "contents": [{"type": "text", "text": v, "size": "xxs", "color": c, "align": a, "flex": w} for v, c, a, w in zip(vals, colors, aligns, weights)]
        })

    bubble = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": header_bg,
            "paddingAll": "xs",
            "contents": [
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": f"#{rank}  {name_code}", "weight": "bold", "size": "sm", "color": "#ffffff", "flex": 4},
                        {"type": "text", "text": f"🏆 HẠNG {rank}", "weight": "bold", "size": "xs", "color": "#ffffff", "align": "end", "flex": 2}
                    ]
                },
                {
                    "type": "box",
                    "layout": "horizontal",
                    "margin": "xs",
                    "contents": [
                        {"type": "text", "text": tagline, "size": "xxs", "color": "#fef3c7", "flex": 3},
                        {"type": "text", "text": f"Điểm: {score_val:.1f}", "weight": "bold", "size": "xxs", "color": "#ffffff", "align": "end", "flex": 2}
                    ]
                }
            ]
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#ffffff",
            "paddingAll": "xs",
            "contents": [
                tile_box,
                {"type": "separator", "margin": "xs"},
                {"type": "box", "layout": "vertical", "margin": "xs", "contents": td_rows}
            ]
        }
    }
    return bubble

def build_nhanvien_flex():
    data = get_dashboard_data("Config_ThiDua,Data_NV_BI,Data_BI,Data_Realtime_NV,Data_NV_ThiDua,Data_ThiDua")
    config_rows = data.get("Config_ThiDua", [])
    bi_rows = data.get("Data_BI", [])
    nv_rows = data.get("Data_Realtime_NV", [])
    if not nv_rows:
        nv_rows = data.get("Data_NV_BI", [])
    
    td_store_rows = data.get("Data_ThiDua", [])
    nv_td_rows = data.get("Data_NV_ThiDua", [])
        
    tz = pytz.timezone('Asia/Ho_Chi_Minh')
    now = datetime.now(tz)
    now_str = now.strftime("%H:%M - %d/%m/%Y")
    
    current_day = now.day
    days_in_month = (datetime(now.year, now.month + 1, 1) - datetime(now.year, now.month, 1)).days if now.month < 12 else 31
    days_passed = days_in_month if current_day == 1 else current_day - 1
    
    tDT = sum(parse_number(get_key_val(b, "Doanh thu Quy đổi", "Doanh thu", default=0.0)) for b in bi_rows if get_key_val(b, "nhóm ngành hàng", default="") != "N/A")
    total_target = sum(parse_number(get_key_val(b, "Target", "target", default=0.0)) for b in bi_rows if get_key_val(b, "nhóm ngành hàng", default="") != "N/A")
    if total_target <= 0:
        total_target = 1500.0
        
    emp_targets = {}
    active_staff_names = {}
    
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
            active_staff_names[raw_name.upper()] = {"name": raw_name, "user_id": user_id, "ratio": locked_ratio}
            
        if sum_ratios <= 0:
            sum_ratios = 1.0
            
        for raw_name, ratio in initial_ratios.items():
            norm_ratio = ratio / sum_ratios
            emp_targets[raw_name] = norm_ratio * total_target
            
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
            active_staff_names[emp_name_str.upper()] = {"name": emp_name_str, "user_id": str(user_id).strip(), "ratio": ratio}

    emp_actuals = {}
    for r in nv_rows:
        name = get_key_val(r, "staffUserName", "tên nv", "Họ và tên", "user", "mã nv", default=None)
        if not name: 
            continue
        name_str = str(name).strip()
        actual = parse_number(get_key_val(r, "Doanh thu Quy đổi", "Doanh thu", "Value_Compe", default=0.0))
        emp_actuals[name_str] = emp_actuals.get(name_str, 0.0) + actual

    # 23 Ngành Hàng Thi Đua Map từ Data_ThiDua và Data_NV_ThiDua theo cơ chế baocao_nhanvien
    store_cat_targets = {}
    for r in td_store_rows:
        nganh = get_key_val(r, "maingroupname", "main group name", "nhóm ngành hàng", "nhóm ngành hàng chính", "programname", default=None)
        if not nganh or str(nganh).strip() == "" or str(nganh).strip() == "N/A":
            continue
        tg = parse_number(get_key_val(r, "target", "mục tiêu", default=0.0))
        if tg <= 0:
            continue
        nganh_str = str(nganh).strip()
        nganh_clean = nganh_str.lower()
        if nganh_clean not in store_cat_targets:
            store_cat_targets[nganh_clean] = {"name": nganh_str, "store_target": tg}

    nv_td_actuals = {}
    for r in nv_td_rows:
        user = get_key_val(r, "staffuser", "user", "mã nv", "employeeid", default=None)
        nganh = get_key_val(r, "programname", "nhóm ngành hàng", "nhóm ngành hàng chính", default=None)
        if not user or not nganh:
            continue
        user_clean = str(user).strip().upper()
        nganh_clean = str(nganh).strip().lower()
        actual = parse_number(get_key_val(r, "value_compe", "thực hiện", "đã bán", default=0.0))
        key = (user_clean, nganh_clean)
        nv_td_actuals[key] = nv_td_actuals.get(key, 0.0) + actual

    emp_list = []
    processed_upper = set()
    for staff_upper, staff_info in active_staff_names.items():
        clean_name = staff_info["name"] if isinstance(staff_info, dict) else str(staff_info)
        user_id = staff_info.get("user_id", "") if isinstance(staff_info, dict) else ""
        ratio = staff_info.get("ratio", 0.1) if isinstance(staff_info, dict) else 0.1
        
        if clean_name.upper() in processed_upper:
            continue
        processed_upper.add(clean_name.upper())
        
        actual = 0.0
        for act_name, val in emp_actuals.items():
            if act_name.upper() == staff_upper or act_name.upper() == clean_name.upper():
                actual += val
                
        target = emp_targets.get(clean_name, 0.0)
        pct_ht = (actual / target) if target > 0 else 0.0
        
        # Build 23 thi dua items for this employee
        staff_td_items = []
        if store_cat_targets:
            for cat_clean, cat_info in store_cat_targets.items():
                cat_name = cat_info["name"]
                cat_store_tg = cat_info["store_target"]
                staff_cat_tg = max(1.0, round(cat_store_tg * ratio))
                
                staff_cat_act = nv_td_actuals.get((user_id.upper(), cat_clean), 0.0)
                if staff_cat_act == 0.0:
                    staff_cat_act = nv_td_actuals.get((clean_name.upper(), cat_clean), 0.0)
                    
                con_lai = max(0.0, staff_cat_tg - staff_cat_act)
                ht_val = (staff_cat_act / staff_cat_tg * 100.0) if staff_cat_tg > 0 else 0.0
                dk_val = ((staff_cat_act / days_passed) * days_in_month / staff_cat_tg * 100.0) if staff_cat_tg > 0 else 0.0
                
                staff_td_items.append({
                    "name": cat_name,
                    "m_tieu": "🏆" if staff_cat_act >= staff_cat_tg else f"{int(staff_cat_tg)}",
                    "target": int(staff_cat_tg),
                    "actual": int(staff_cat_act),
                    "con_lai": "🏆" if staff_cat_act >= staff_cat_tg else int(con_lai),
                    "ht": ht_val,
                    "du_kien": dk_val
                })
        
        emp_list.append({
            "name": clean_name,
            "user_id": user_id or "NV",
            "actual": actual,
            "target": target,
            "pct": pct_ht,
            "thi_dua_list": staff_td_items if staff_td_items else None
        })
        
    emp_list.sort(key=lambda x: x["actual"], reverse=True)
    
    # 1. Bubble 1: Bảng Xếp Hạng NV Overview
    overview_bubble = build_leaderboard_overview_bubble(emp_list, now_str)
    
    # 2. Bubbles 2..N: Thẻ KPI Chi Tiết Từng NV (Truyền 23 ngành hàng thi đua)
    all_bubbles = [overview_bubble]
    total_emp = len(emp_list)
    for idx, e in enumerate(emp_list, start=1):
        staff_bubble = build_individual_staff_card(e, idx, total_emp, now_str, thi_dua_list=e.get("thi_dua_list"))
        all_bubbles.append(staff_bubble)
        
    return all_bubbles

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

    body_contents_rt1 = [
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
        
    tot_sl = sum(x["sl"] for x in parsed_rt_bi)
    totalHTCol2 = rt_total / rt_tTarget if rt_tTarget > 0 else 0.0
    tot_vals = ["⭐", "TỔNG CỘNG", fmt_num(tot_sl), fmt_num(rt_total), fmt_num(rt_tTarget), f"{totalHTCol2*100:.0f}%"]
    tot_colors = ["#ffffff", "#ffffff", "#ffffff", "#ffffff", "#ffffff", "#ffffff"]
    table_card_contents.append(make_table_row(tot_vals, weights, aligns, tot_colors, bold=True, bg_color="#f59e0b"))

    body_contents_rt1.append({
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

    # Insight Box (Nhận xét dưới cùng Phần 1)
    rem_hours = max(0.5, 13.0 - elapsed_hours)
    if thieuDTRT > 0:
        req_speed = thieuDTRT / rem_hours
        insight_msg = f"💡 Còn thiếu {fmt_num(thieuDTRT)} Tr để hoàn thành Target ngày ({fmt_num(target_today)} Tr). Cần trung bình ~{req_speed:.1f} Tr/giờ trong {rem_hours:.1f}h còn lại."
    else:
        insight_msg = f"🎉 Xuất sắc! Siêu thị đã hoàn thành Target doanh thu ngày hôm nay ({fmt_num(rt_total)}/{fmt_num(target_today)} Tr)."

    body_contents_rt1.append({
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

    flex_bubble_rt1 = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#0284c7",
            "paddingAll": "md",
            "contents": [
                {"type": "text", "text": "⚡ BÁO CÁO REALTIME (P.1: DOANH THU & TIẾN ĐỘ)", "weight": "bold", "size": "sm", "color": "#ffffff", "align": "center"},
                {"type": "text", "text": f"🕒 Cập nhật: {now_str} • {status_badge_text}", "size": "xxs", "color": "#e0f2fe", "align": "center", "margin": "xs"}
            ]
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#ffffff",
            "paddingAll": "md",
            "contents": body_contents_rt1
        }
    }

    # Card 2: Các nhóm hàng thi đua hôm nay (Đã đạt & Chưa đạt)
    body_contents_rt2 = []
    if td_done:
        done_contents = [
            {"type": "text", "text": "🏆 THI ĐUA ĐÃ ĐẠT CHỈ TIÊU NGÀY", "size": "xxs", "color": "#15803d", "weight": "bold", "margin": "xs"},
            {"type": "separator", "color": "#bbf7d0", "margin": "xs"}
        ]
        for idx, t in enumerate(td_done):
            done_contents.append(make_thidua_progress_row(idx+1, t["name"], None, t["ht"], t["unit"]))
            
        body_contents_rt2.append({
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
            
        body_contents_rt2.append({
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

    flex_bubble_rt2 = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#0f766e",
            "paddingAll": "md",
            "contents": [
                {"type": "text", "text": "⚡ BÁO CÁO REALTIME (P.2: NHÓM HÀNG THI ĐUA)", "weight": "bold", "size": "sm", "color": "#ffffff", "align": "center"},
                {"type": "text", "text": f"🕒 Cập nhật: {now_str} • {rt_cntVD}/{len(parsed_td)} Nhóm Đạt Target", "size": "xxs", "color": "#ccfbf1", "align": "center", "margin": "xs"}
            ]
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#ffffff",
            "paddingAll": "md",
            "contents": body_contents_rt2
        }
    }

    return [flex_bubble_rt1, flex_bubble_rt2]
