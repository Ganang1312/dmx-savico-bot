import pytz
from datetime import datetime
from dmx_data_provider import get_dashboard_data, get_locked_target_config, get_staff_history_base

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

def is_total_row(row_or_name):
    if not row_or_name:
        return False
    if isinstance(row_or_name, dict):
        ma = str(get_key_val(row_or_name, "mã nnh", "ma nnh", "rowcode", default="")).strip().upper()
        if ma in ["TOTAL", "TỔNG", "TONG"]:
            return True
        nh = str(get_key_val(row_or_name, "nhóm ngành hàng", "ngành hàng", "salegroupmastername", "rowname", default="")).strip().upper()
    else:
        nh = str(row_or_name).strip().upper()
    return nh in ["TỔNG", "TỔNG CỘNG", "TONG CONG", "TOTAL", "GRAND TOTAL"] or nh.startswith("TỔNG ") or nh.startswith("TOTAL ")

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
            "weight": "bold" if bold else "regular",
            "wrap": True
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
    
    # Tìm dòng tổng nếu có sẵn từ nguồn Cách 2 để lấy Target chuẩn toàn siêu thị
    total_row_bi = next((b for b in bi_rows if is_total_row(b)), None)
    base_target = parse_number(get_key_val(total_row_bi, "target", "Target", default=0.0)) if total_row_bi else 0.0

    tDT = 0.0
    tTG = 0.0
    tTC = 0.0
    tDTGoc = 0.0
    tDT_CK_total = 0.0
    parsed_bi = []
    
    for b in bi_rows:
        if is_total_row(b):
            continue
        nganh = get_key_val(b, "nhóm ngành hàng", "ngành hàng", "salegroupmastername", default=None)
        if not nganh or str(nganh).strip().upper() == "N/A" or is_total_row(nganh):
            continue
            
        dt = parse_number(get_key_val(b, "doanh thu quy đổi", "doanh thu", default=0.0))
        sl = parse_number(get_key_val(b, "số lượng", "quantity", default=0.0))
        tg = parse_number(get_key_val(b, "target", default=0.0))
        dtGoc = parse_number(get_key_val(b, "doanh thu", default=0.0))
        dtTC = parse_number(get_key_val(b, "revenue_installment", "doanh thu trả chậm", "dt trả chậm", "doanh thu trả góp", "dt trả góp", "dt tra cham", default=0.0))
        
        tDT += dt
        tTG += tg
        tTC += dtTC
        tDTGoc += dtGoc

        raw_ck_val = get_key_val(b, "rev_kft_riserate_lastmonth", "+/- dtck", "+/- so với ck", "+/- dtck tháng (qđ)", "% tt", default=None)
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
    if tTG <= 0 and base_target > 0:
        tTG = base_target
    if tTG <= 0:
        tTG = 1500.0

    # Lấy doanh thu trả chậm & tỷ trọng trả chậm từ dòng TỔNG CỘNG nếu có (chuẩn theo baocao_luyke.html)
    if total_row_bi:
        tot_tc = parse_number(get_key_val(total_row_bi, "dt trả chậm", "doanh thu trả chậm", "doanh thu trả góp", "dt trả góp", "revenue_installment", default=0.0))
        if tot_tc > 0:
            tTC = tot_tc
        tot_ty_trong_tc = parse_number(get_key_val(total_row_bi, "tỷ trọng tc", "tỷ trọng trả chậm", "tỷ trọng", "ty trong tc", default=0.0))
        if tot_ty_trong_tc > 0:
            totalTyLeTC = tot_ty_trong_tc / 100.0 if tot_ty_trong_tc > 1.0 else tot_ty_trong_tc
        else:
            totalTyLeTC = tTC / tDTGoc if tDTGoc > 0 else (tTC / tDT if tDT > 0 else 0.0)
    else:
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
            mt_ngay_str = f"{mt_ngay_val:.1f} Tr" if mt_ngay_val > 0 else "0 Tr"
        else:
            mt_ngay_str = f"{int(round(mt_ngay_val))} SP" if mt_ngay_val >= 1 else (f"{mt_ngay_val:.1f} SP" if mt_ngay_val > 0 else "0 SP")

        parsed_td.append({
            "name": shorten_name(nganh),
            "actual": actual,
            "con_lai": con_lai,
            "target": tg,
            "ht": ht_target,
            "ht_dk": ht_du_kien,
            "unit": "TR" if is_dt else "SP",
            "mt_ngay_str": mt_ngay_str,
            "phan_loai": config_map.get(nganh_clean, 1.0)
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
                        {"type": "text", "text": f"{fmt_num(tTC)} Tr ({totalTyLeTC*100:.1f}%)", "size": "sm", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
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
                {"type": "text", "text": "📊 BÁO CÁO LŨY KẾ\n(DOANH THU & TỶ TRỌNG)", "weight": "bold", "size": "sm", "color": "#ffffff", "align": "center", "wrap": True},
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

    # Bảng Thi Đua Lũy Kế P.2 (Chia 2 khu: Dự kiến về đích & Chưa về đích - Chuẩn Ảnh 1 mới)
    headers_p2 = ["#", "NH", "MT", "Đạt", "%HT", "%DK"]
    weights_p2 = [1, 4, 2, 3, 2, 2]
    aligns_p2 = ["center", "start", "center", "center", "center", "center"]

    body_contents_p2 = []

    if td_done:
        done_table = [
            {"type": "text", "text": f"🏆 NHÓM DỰ KIẾN VỀ ĐÍCH ({len(td_done)})", "size": "xxs", "color": "#15803d", "weight": "bold", "margin": "xs"},
            make_table_header(headers_p2, weights_p2, aligns_p2, bg_color="#15803d"),
            {"type": "separator", "color": "#bbf7d0", "margin": "xs"}
        ]
        for idx, t in enumerate(td_done):
            unit_tag = "(DT)" if t["unit"] == "TR" else "(SL)"
            display_name = f"{t['name']} {unit_tag}"
            mt_str = "🏆" if (t["actual"] >= t["target"] or t.get("ht", 0) >= 1.0) else t.get("mt_ngay_str", "0")
            act_tg_str = f"{fmt_num(t['actual'])} / {fmt_num(t['target'])}"
            ht_str = f"{t['ht']*100:.0f}%"
            dk_str = f"{t['ht_dk']*100:.0f}%"

            name_color = "#dc2626" if t.get("phan_loai") == 2.0 else "#0f172a"
            vals = [str(idx + 1), display_name, mt_str, act_tg_str, ht_str, dk_str]
            colors = ["#64748b", name_color, "#16a34a" if mt_str == "🏆" else "#0284c7", "#0f172a", get_color_class(t["ht"]), get_color_class(t["ht_dk"])]
            done_table.append(make_table_row(vals, weights_p2, aligns_p2, colors))

        body_contents_p2.append({
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#f0fdf4",
            "borderColor": "#bbf7d0",
            "borderWidth": "1px",
            "cornerRadius": "md",
            "paddingAll": "sm",
            "margin": "xs",
            "contents": done_table
        })

    if td_pending:
        pending_table = [
            {"type": "text", "text": f"🎯 NHÓM CHƯA VỀ ĐÍCH ({len(td_pending)})", "size": "xxs", "color": "#b91c1c", "weight": "bold", "margin": "xs"},
            make_table_header(headers_p2, weights_p2, aligns_p2, bg_color="#0f766e"),
            {"type": "separator", "color": "#cbd5e1", "margin": "xs"}
        ]
        for idx, t in enumerate(td_pending):
            unit_tag = "(DT)" if t["unit"] == "TR" else "(SL)"
            display_name = f"{t['name']} {unit_tag}"
            mt_str = "🏆" if (t["actual"] >= t["target"] or t.get("ht", 0) >= 1.0) else t.get("mt_ngay_str", "0")
            act_tg_str = f"{fmt_num(t['actual'])} / {fmt_num(t['target'])}"
            ht_str = f"{t['ht']*100:.0f}%"
            dk_str = f"{t['ht_dk']*100:.0f}%"

            name_color = "#dc2626" if t.get("phan_loai") == 2.0 else "#0f172a"
            vals = [str(idx + 1), display_name, mt_str, act_tg_str, ht_str, dk_str]
            colors = ["#64748b", name_color, "#16a34a" if mt_str == "🏆" else "#0284c7", "#0f172a", get_color_class(t["ht"]), get_color_class(t["ht_dk"])]
            pending_table.append(make_table_row(vals, weights_p2, aligns_p2, colors))

        body_contents_p2.append({
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#ffffff",
            "borderColor": "#ccfbf1",
            "borderWidth": "1px",
            "cornerRadius": "md",
            "paddingAll": "sm",
            "margin": "xs",
            "contents": pending_table
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
                {"type": "text", "text": "🏆 BÁO CÁO LŨY KẾ\n(NGÀNH HÀNG THI ĐUA)", "weight": "bold", "size": "sm", "color": "#ffffff", "align": "center", "wrap": True},
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
            {"type": "text", "text": "NV", "size": "xxs", "color": "#ffffff", "weight": "bold", "flex": 3, "align": "start"},
            {"type": "text", "text": "ĐIỂM", "size": "xxs", "color": "#ffffff", "weight": "bold", "flex": 2, "align": "center"},
            {"type": "text", "text": "TĐ", "size": "xxs", "color": "#ffffff", "weight": "bold", "flex": 2, "align": "center"},
            {"type": "text", "text": "DOANH THU", "size": "xxs", "color": "#ffffff", "weight": "bold", "flex": 5, "align": "center"}
        ]
    })
    rows_contents.append({"type": "separator", "color": "#cbd5e1", "margin": "xs"})

    for idx, e in enumerate(emp_list):
        rank = idx + 1
        user_id = str(e.get("user_id", "")).strip()
        full_name = str(e.get("name", "")).strip()
        first_name = full_name.split(" - ")[-1].split()[-1] if full_name else "NV"

        if rank == 1:
            rank_str = "🥇"
            row_bg = "#fef9c3"
            score_color = "#b45309"
            name_color = "#b45309"
        elif rank == 2:
            rank_str = "🥈"
            row_bg = "#f1f5f9"
            score_color = "#475569"
            name_color = "#334155"
        elif rank == 3:
            rank_str = "🥉"
            row_bg = "#fff7ed"
            score_color = "#c2410c"
            name_color = "#c2410c"
        else:
            rank_str = str(rank)
            row_bg = "#f8fafc" if rank % 2 == 0 else "#ffffff"
            score_color = "#64748b"
            name_color = "#0f172a"

        actual_val = e.get("actual", 0.0)
        target_val = e.get("target", 0.0)
        con_lai_val = max(0.0, target_val - actual_val)
        pct_val = (actual_val / target_val * 100.0) if target_val > 0 else 0.0
        score_val = e.get("diem", 90.0 - rank * 3.5)
        td_passed = e.get("count_nh_du_kien", e.get("td_passed", max(1, 10 - rank)))
        td_total = e.get("td_total", 23)

        cl_text = f"⌛ CÒN {fmt_num(con_lai_val)}" if con_lai_val > 0 else "🏆 ĐẠT TARGET"
        cl_color = "#dc2626" if con_lai_val > 0 else "#16a34a"

        row_box = {
            "type": "box",
            "layout": "horizontal",
            "paddingAll": "xs",
            "backgroundColor": row_bg,
            "margin": "xs",
            "contents": [
                # STT Icon
                {
                    "type": "box", "layout": "vertical", "flex": 1, "align": "center",
                    "contents": [{"type": "text", "text": rank_str, "weight": "bold", "size": "xxs", "color": score_color, "align": "center"}]
                },
                # Tên nhân viên
                {
                    "type": "box", "layout": "vertical", "flex": 3,
                    "contents": [
                        {"type": "text", "text": first_name, "weight": "bold", "size": "xxs", "color": name_color},
                        {"type": "text", "text": f"ID:{user_id}" if user_id and user_id != "NV" else "", "size": "xxs", "color": "#64748b", "margin": "xs"}
                    ]
                },
                # Điểm số
                {
                    "type": "box", "layout": "vertical", "flex": 2, "align": "center",
                    "contents": [{"type": "text", "text": f"{score_val:.1f}", "weight": "bold", "size": "xxs", "color": score_color}]
                },
                # Thi đua
                {
                    "type": "box", "layout": "vertical", "flex": 2, "align": "center",
                    "contents": [{"type": "text", "text": f"{td_passed}/{td_total}", "weight": "bold", "size": "xxs", "color": "#0284c7"}]
                },
                # Doanh thu
                {
                    "type": "box", "layout": "vertical", "flex": 5,
                    "contents": [
                        {
                            "type": "box", "layout": "horizontal",
                            "contents": [
                                {"type": "text", "text": f"{fmt_num(actual_val)} / {fmt_num(target_val)}", "size": "xxs", "weight": "bold", "color": "#0284c7", "flex": 3},
                                {"type": "text", "text": f"{pct_val:.0f}%", "size": "xxs", "weight": "bold", "color": "#d97706", "align": "end", "flex": 2}
                            ]
                        },
                        {"type": "text", "text": cl_text, "size": "xxs", "color": cl_color, "weight": "bold", "align": "end", "margin": "xs"}
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
                {"type": "text", "text": "🏆 BÁO CÁO XẾP HẠNG\n(DOANH THU & THI ĐUA NV)", "weight": "bold", "size": "sm", "color": "#ffffff", "align": "center", "wrap": True},
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
    Tin nhắn 2..N: Thẻ KPI Chi Tiết Từng Nhân Viên (Chuẩn Tính Toán 100% Theo baocao_nhanvien.html)
    """
    name = e.get("name", "Nhân Viên")
    user_id = str(e.get("user_id", "")).strip()
    full_name = str(name).strip()
    if " - " in full_name:
        parts = full_name.split(" - ")
        if not user_id or user_id == "90509" or user_id == "NV":
            user_id = parts[0].strip()
        full_name = parts[-1].strip()
        
    first_name = full_name.split()[-1] if full_name else "NV"
    short_name_code = f"{first_name}-{user_id}" if user_id else first_name
    
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
        # Sắp xếp nhóm thi đua theo du_kien giảm dần và GIỮ NGUYÊN 100% 23 nhóm!
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
            # Tiến độ Doanh thu
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

    # 4. Bảng Nhóm Hàng Thi Đua 7 Cột (# | NH | MT | LK / TG | CÒN | %HT | %DK) Thiết kế Thanh Thoát
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

        def make_staff_cell(v, w, a, c):
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

        row_contents = [make_staff_cell(v, w, a, c) for v, c, a, w in zip(vals, colors, aligns, weights)]

        row_box = {
            "type": "box", "layout": "horizontal", "contents": row_contents
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

def build_nhanvien_flex():
    data = get_dashboard_data("Config_ThiDua,Data_NV_BI,Data_BI,Data_Realtime_NV,Data_NV_ThiDua,Data_ThiDua")
    config_rows = data.get("Config_ThiDua", [])
    bi_rows = data.get("Data_BI", [])
    nv_rows = data.get("Data_NV_BI", [])
    if not nv_rows:
        nv_rows = data.get("Data_Realtime_NV", [])
    
    td_store_rows = data.get("Data_ThiDua", [])
    nv_td_rows = data.get("Data_NV_ThiDua", [])
        
    tz = pytz.timezone('Asia/Ho_Chi_Minh')
    now = datetime.now(tz)
    now_str = now.strftime("%H:%M - %d/%m/%Y")
    
    current_day = now.day
    days_in_month = (datetime(now.year, now.month + 1, 1) - datetime(now.year, now.month, 1)).days if now.month < 12 else 31
    days_passed = days_in_month if current_day == 1 else current_day - 1
    
    total_row_bi = next((b for b in bi_rows if is_total_row(b)), None)
    base_target_bi = parse_number(get_key_val(total_row_bi, "Target", "target", default=0.0)) if total_row_bi else 0.0
    tTG_Store = sum(parse_number(get_key_val(b, "Target", "target", default=0.0)) for b in bi_rows if not is_total_row(b) and get_key_val(b, "nhóm ngành hàng", default="") != "N/A")
    if base_target_bi > 0:
        tTG_Store = base_target_bi
    elif tTG_Store <= 0:
        tTG_Store = 1500.0

    # 1. Đọc danh sách nhân viên và cấu hình ngành hàng từ Config_ThiDua
    active_config_staff = {}
    config_map = {}
    if config_rows:
        for r in config_rows:
            emp_name = get_key_val(r, "user-họ và tên", "Họ và tên", "tên nhân viên", default=None)
            user_id = get_key_val(r, "user", "User", "mã nv", "mã nhân viên", "staffUser", default=None)
            pct = get_key_val(r, "tỷ lệ %", "% chia", default=None)
            if (user_id and str(user_id).strip()) or (emp_name and str(emp_name).strip() and pct is not None and str(pct).strip() != ""):
                u_id_str = str(user_id or emp_name).strip().upper()
                emp_name_str = str(emp_name or user_id).strip()
                p_num = parse_number(pct)
                ratio = p_num if p_num <= 1.0 else p_num / 100.0
                active_config_staff[u_id_str] = {"name": emp_name_str, "ratio": ratio}

            cat_name = get_key_val(r, "ngành hàng", "Ngành hàng")
            pl_val = get_key_val(r, "phân loại", "Phân loại")
            if cat_name and str(cat_name).strip():
                pl_num = parse_number(pl_val)
                if pl_num in [1.0, 2.0]:
                    config_map[str(cat_name).lower().strip()] = {
                        "phanLoai": pl_num,
                        "thuTu": parse_number(get_key_val(r, "thứ tự", "Ngày") or 999.0)
                    }

    num_active_staff = len(active_config_staff) or 1

    # 2. Lấy dữ liệu lịch sử (HCR) để tính target hybrid chuẩn như baocao_nhanvien.html
    base_sheets, base_date = get_staff_history_base()
    b_data_nv_bi = base_sheets.get("Data_NV_BI", [])
    b_data_thi_dua = base_sheets.get("Data_NV_ThiDua", [])
    
    base_user_map = {}
    for r in b_data_nv_bi:
        u_id = str(get_key_val(r, "staffuser", "mã nv", "employeeid", "user", "mã nhân viên") or "").strip().upper()
        if not u_id:
            continue
        dt_qd = parse_number(get_key_val(r, "doanh thu quy đổi", "revenue_kfactor", "dt quy đổi", default=0.0))
        rev_k_cum = parse_number(get_key_val(r, "revenue_kfactor_cum", "doanh thu quy đổi lũy kế", default=0.0))
        b_dt = (rev_k_cum + dt_qd) if (rev_k_cum > 0 and dt_qd < rev_k_cum) else (dt_qd or rev_k_cum)
        if u_id not in base_user_map:
            base_user_map[u_id] = {"dt": 0.0, "td": {}}
        base_user_map[u_id]["dt"] += b_dt
        
    for r in b_data_thi_dua:
        u_id = str(get_key_val(r, "staffuser", "mã nv", "employeeid", "user", "mã nhân viên") or "").strip().upper()
        nganh = get_key_val(r, "programname", "nhóm ngành hàng", "nhóm ngành hàng chính", default=None)
        if not u_id or not nganh:
            continue
        nganh_clean = str(nganh).lower().strip()
        act = parse_number(get_key_val(r, "value_compe", "thực hiện", "đã bán", default=0.0))
        if u_id not in base_user_map:
            base_user_map[u_id] = {"dt": 0.0, "td": {}}
        base_user_map[u_id]["td"][nganh_clean] = base_user_map[u_id]["td"].get(nganh_clean, 0.0) + act
        
    total_base_dt = sum(u["dt"] for u in base_user_map.values())
    base_staff_dt_contrib = {}
    for u_id, u in base_user_map.items():
        base_staff_dt_contrib[u_id] = (u["dt"] / total_base_dt) if total_base_dt > 0 else 0.0
        
    total_base_cat = {}
    for u_id, u in base_user_map.items():
        for cat, val in u["td"].items():
            total_base_cat[cat] = total_base_cat.get(cat, 0.0) + val
            
    base_staff_cat_contrib = {}
    for u_id, u in base_user_map.items():
        base_staff_cat_contrib[u_id] = {}
        for cat, val in u["td"].items():
            base_staff_cat_contrib[u_id][cat] = (val / total_base_cat[cat]) if total_base_cat.get(cat, 0.0) > 0 else 0.0

    # 3. Xử lý target khóa (Target_Lock) nếu có, hoặc tính theo targetMode Hybrid (60% Sàn + 40% Đóng góp)
    lock_config = get_locked_target_config()
    user_map = {}
    locked_staff_list = lock_config.get("staff", []) if (lock_config and lock_config.get("is_locked")) else []

    if lock_config and lock_config.get("is_locked") and locked_staff_list:
        initial_ratios = {}
        sum_ratios = 0.0
        for u_id, u_conf in active_config_staff.items():
            locked_s = next((s for s in locked_staff_list if str(s.get("userId", "")).strip().upper() == u_id), None)
            if locked_s and locked_s.get("lockedRatio") is not None:
                r = parse_number(locked_s.get("lockedRatio"))
            else:
                hcr = base_staff_dt_contrib.get(u_id, u_conf["ratio"])
                r = 0.6 / num_active_staff + 0.4 * hcr
            initial_ratios[u_id] = r
            sum_ratios += r
        if sum_ratios <= 0:
            sum_ratios = 1.0

        for u_id, u_conf in active_config_staff.items():
            norm_r = initial_ratios[u_id] / sum_ratios
            target = norm_r * tTG_Store
            user_map[u_id] = {
                "name": u_conf["name"],
                "user_id": u_id,
                "target": target,
                "targetRatio": norm_r,
                "dt": 0.0,
                "td": {},
                "tdTargets": {}
            }
    else:
        # Default targetMode: HYBRID (60% Sàn + 40% Đóng góp lịch sử) chuẩn 100% theo baocao_nhanvien.html
        for u_id, u_conf in active_config_staff.items():
            hcr = base_staff_dt_contrib.get(u_id, u_conf["ratio"])
            target = tTG_Store * (0.6 / num_active_staff + 0.4 * hcr)
            user_map[u_id] = {
                "name": u_conf["name"],
                "user_id": u_id,
                "target": target,
                "targetRatio": u_conf["ratio"],
                "dt": 0.0,
                "td": {},
                "tdTargets": {}
            }

    # 4. Tổng hợp doanh thu thực tế của từng NV
    for r in nv_rows:
        u_id = str(get_key_val(r, "mã nv", "mã nhân viên", "employeeid", "user", "staffuser", default="") or "").strip().upper()
        if not u_id:
            name_val = str(get_key_val(r, "staffUserName", "tên nv", "Họ và tên", default="") or "").strip().upper()
            matched_uid = next((uid for uid, uinfo in active_config_staff.items() if uinfo["name"].upper() == name_val or uid == name_val), None)
            if matched_uid:
                u_id = matched_uid
        
        if u_id in user_map:
            dt_qd = parse_number(get_key_val(r, "Doanh thu Quy đổi", "doanh thu quy đổi", "revenue_kfactor", "dt quy đổi", default=0.0))
            rev_k_cum = parse_number(get_key_val(r, "revenue_kfactor_cum", "doanh thu quy đổi lũy kế", "dt quy đổi lũy kế", default=0.0))
            dt_total = (rev_k_cum + dt_qd) if (rev_k_cum > 0 and dt_qd < rev_k_cum) else (dt_qd or rev_k_cum)
            user_map[u_id]["dt"] += dt_total

    # 5. Store Thi Đua Categories (chuẩn 23 nhóm theo baocao_nhanvien.html)
    active_categories = []
    for r in td_store_rows:
        nganh = get_key_val(r, "maingroupname", "main group name", "nhóm ngành hàng", "nhóm ngành hàng chính", "programname", default=None)
        if not nganh or str(nganh).strip() == "" or str(nganh).strip() == "N/A":
            continue
        tg = parse_number(get_key_val(r, "target", "mục tiêu", default=0.0))
        if tg <= 0:
            continue
        
        nganh_str = str(nganh).strip()
        nganh_clean = nganh_str.lower()
        if config_map and nganh_clean not in config_map:
            continue
            
        c_obj = config_map.get(nganh_clean, {"phanLoai": 1.0, "thuTu": 999.0})
        sl = parse_number(get_key_val(r, "số lượng", "quantity", default=0.0))
        dt = parse_number(get_key_val(r, "doanh thu", default=0.0))
        is_sl = True
        thuc_hien_store = sl
        if dt > 0:
            if sl == 0:
                is_sl = False
                thuc_hien_store = dt
            else:
                ty_le_dt = abs((dt / tg) - 1.0)
                ty_le_sl = abs((sl / tg) - 1.0)
                if ty_le_dt < ty_le_sl:
                    is_sl = False
                    thuc_hien_store = dt

        active_categories.append({
            "nganhHang": nganh_str,
            "storeTarget": tg,
            "phanLoai": c_obj["phanLoai"],
            "thuTu": c_obj["thuTu"],
            "isSL": is_sl
        })

    active_categories.sort(key=lambda x: x["thuTu"])

    # 6. Tính target từng ngành hàng thi đua cho từng nhân viên (userCatTargets)
    user_cat_targets = {u_id: {} for u_id in active_config_staff}
    for cat in active_categories:
        nganh_clean = cat["nganhHang"].lower().strip()
        if lock_config and lock_config.get("is_locked") and locked_staff_list:
            initial_cat_ratios = {}
            sum_cat_ratios = 0.0
            for u_id in active_config_staff:
                u_conf = active_config_staff.get(u_id, {"ratio": 0.0})
                locked_s = next((s for s in locked_staff_list if str(s.get("userId", "")).strip().upper() == u_id), None)
                cat_ratio = u_conf["ratio"]
                if locked_s and locked_s.get("lockedCatRatios") and nganh_clean in locked_s["lockedCatRatios"]:
                    cat_ratio = parse_number(locked_s["lockedCatRatios"][nganh_clean])
                initial_cat_ratios[u_id] = cat_ratio
                sum_cat_ratios += cat_ratio
            if sum_cat_ratios <= 0:
                sum_cat_ratios = 1.0
            for u_id in active_config_staff:
                user_cat_targets[u_id][nganh_clean] = cat["storeTarget"] * (initial_cat_ratios[u_id] / sum_cat_ratios)
        else:
            for u_id in active_config_staff:
                hcr = base_staff_cat_contrib.get(u_id, {}).get(nganh_clean, active_config_staff[u_id]["ratio"])
                user_cat_targets[u_id][nganh_clean] = cat["storeTarget"] * (0.6 / num_active_staff + 0.4 * hcr)

    # 7. Tổng hợp thi đua thực tế của từng NV từ Data_NV_ThiDua
    active_cat_set = set(c["nganhHang"].lower().strip() for c in active_categories)
    for r in nv_td_rows:
        u_id = str(get_key_val(r, "staffuser", "user", "mã nv", "employeeid", "staffUser", default="")).strip().upper()
        if not u_id:
            name_val = str(get_key_val(r, "staffUserName", "tên nv", "Họ và tên", default="")).strip().upper()
            matched_uid = next((uid for uid, uinfo in active_config_staff.items() if uinfo["name"].upper() == name_val or uid == name_val), None)
            if matched_uid:
                u_id = matched_uid
                
        if not u_id or u_id not in user_map:
            continue
            
        nganh = get_key_val(r, "programname", "ProgramName", "nhóm ngành hàng", "nhóm ngành hàng chính", default=None)
        if not nganh:
            continue
        nganh_clean = str(nganh).strip().lower()
        actual = parse_number(get_key_val(r, "value_compe", "Value_Compe", "thực hiện", "đã bán", default=0.0))
        if nganh_clean in active_cat_set:
            user_map[u_id]["td"][nganh_clean] = user_map[u_id]["td"].get(nganh_clean, 0.0) + actual

    # 8. Tính điểm & build danh sách hiển thị
    emp_list = []
    for u_id, u in user_map.items():
        if u["dt"] == 0 and u["target"] == 0:
            continue
            
        actual = u["dt"]
        target = u["target"]
        pct_ht = (actual / target * 100.0) if target > 0 else 0.0
        du_kien_pct = (((actual / days_passed) * days_in_month) / target * 100.0) if target > 0 else 0.0

        rev_score = du_kien_pct
        rev_score = min(rev_score, 150.0) * 0.6

        staff_td_items = []
        total_weighted_earned = 0.0
        total_weights = 0.0
        count_nh_du_kien = 0

        for cat in active_categories:
            cat_name = cat["nganhHang"]
            nganh_clean = cat_name.lower().strip()
            staff_cat_tg = user_cat_targets[u_id].get(nganh_clean, 0.0)
            u["tdTargets"][nganh_clean] = staff_cat_tg

            staff_cat_act = u["td"].get(nganh_clean, 0.0)
            con_lai = max(0.0, staff_cat_tg - staff_cat_act)
            ht_val = (staff_cat_act / staff_cat_tg * 100.0) if staff_cat_tg > 0 else 0.0
            dk_val = (((staff_cat_act / days_passed) * days_in_month) / staff_cat_tg * 100.0) if staff_cat_tg > 0 else 0.0
            
            phan_loai = cat["phanLoai"]
            if dk_val >= 100.0:
                count_nh_du_kien += 1
                total_weighted_earned += phan_loai
            total_weights += phan_loai

            display_tg = max(1, round(staff_cat_tg))
            actual_display = round(staff_cat_act)
            days_left = max(1, days_in_month - days_passed)
            con_lai_raw = max(0.0, staff_cat_tg - staff_cat_act)
            display_con_lai = max(0, round(con_lai_raw))
            m_target_day = (con_lai_raw / days_left) if days_left > 0 else con_lai_raw

            is_sl = cat.get("isSL", True)
            if con_lai_raw <= 0 or actual_display >= display_tg:
                m_tieu_display = "🏆"
            else:
                if not is_sl and m_target_day < 1.0 and m_target_day > 0:
                    m_tieu_display = f"{round(m_target_day * 1000)}k"
                elif is_sl and m_target_day < 1.0 and m_target_day > 0:
                    m_tieu_display = "1"
                else:
                    m_tieu_display = str(max(1, round(m_target_day)))

            staff_td_items.append({
                "name": cat_name,
                "m_tieu": m_tieu_display,
                "target": display_tg,
                "actual": actual_display,
                "con_lai": "🏆" if actual_display >= display_tg else display_con_lai,
                "ht": ht_val,
                "du_kien": dk_val,
                "phan_loai": phan_loai,
                "is_sl": is_sl
            })

        contest_score = (total_weighted_earned / total_weights * 100.0) if total_weights > 0 else 0.0
        contest_contribution = contest_score * 0.4
        total_score = rev_score + contest_contribution

        staff_td_items.sort(key=lambda x: x.get("du_kien", 0.0), reverse=True)

        emp_list.append({
            "name": u["name"],
            "user_id": u_id,
            "actual": actual,
            "target": target,
            "pct": pct_ht,
            "du_kien_pct": du_kien_pct,
            "diem": total_score,
            "contest_score": contest_score,
            "count_nh_du_kien": count_nh_du_kien,
            "td_total": len(active_categories),
            "days_passed": days_passed,
            "days_in_month": days_in_month,
            "thi_dua_list": staff_td_items if staff_td_items else None
        })

    emp_list.sort(key=lambda x: x.get("diem", x["actual"]), reverse=True)
    
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
    data = get_dashboard_data("Data_BI,Data_ThiDua,Config_ThiDua,Data_Realtime_BI,Data_Realtime_ThiDua,Data_Realtime_NV")
    config_rows = data.get("Config_ThiDua", [])
    bi_rows = data.get("Data_BI", [])
    rt_rows = data.get("Data_Realtime_BI", [])
    rt_td_rows = data.get("Data_Realtime_ThiDua", [])
    td_rows = data.get("Data_ThiDua", [])
    
    tz = pytz.timezone('Asia/Ho_Chi_Minh')
    now = datetime.now(tz)
    now_str = now.strftime("%H:%M - %d/%m/%Y")
    
    # Lũy kế target & dt từ Data_BI (bỏ qua dòng tổng cộng)
    total_row_bi = next((b for b in bi_rows if is_total_row(b)), None)
    base_target_bi = parse_number(get_key_val(total_row_bi, "Target", "target", default=0.0)) if total_row_bi else 0.0

    lk_tDT = sum(parse_number(get_key_val(b, "Doanh thu Quy đổi", "Doanh thu", default=0.0)) for b in bi_rows if not is_total_row(b))
    lk_tTG = sum(parse_number(get_key_val(b, "Target", "target", default=0.0)) for b in bi_rows if not is_total_row(b))
    if lk_tTG <= 0 and base_target_bi > 0:
        lk_tTG = base_target_bi
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
    user_map = {}
    if config_rows:
        for r in config_rows:
            day_val = parse_number(get_key_val(r, "ngày", "Ngày", default=0.0))
            if int(day_val) == current_day:
                holiday_target = parse_number(get_key_val(r, "Mục tiêu", "mục tiêu ngày", "mục tiêu", default=0.0))
            
            user_id = get_key_val(r, "user", "User", "mã nv", "mã nhân viên", default=None)
            if user_id and str(user_id).strip():
                percent = parse_number(get_key_val(r, "tỷ lệ %", "% chia", default=0.0))
                ratio = percent if percent <= 1 else percent / 100.0
                user_map[str(user_id).strip().upper()] = ratio
        
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
        if is_total_row(r):
            continue
        nganh = get_key_val(r, "Nhóm Ngành Hàng", "nhóm ngành hàng", "Ngành hàng", "salegroupmastername", default=None)
        if not nganh or str(nganh).strip().upper() == "N/A" or is_total_row(nganh):
            continue
        dtqd = max(
            parse_number(get_key_val(r, "revenue_KFactor_RT")),
            parse_number(get_key_val(r, "revenue_RT")),
            parse_number(get_key_val(r, "Doanh thu Quy đổi", "doanh thu quy đổi", "Doanh thu", "doanh thu"))
        )
        sl = max(
            parse_number(get_key_val(r, "quantity_RT")),
            parse_number(get_key_val(r, "quantity_KFactor")),
            parse_number(get_key_val(r, "Số lượng", "số lượng"))
        )
        targetDay = max(
            parse_number(get_key_val(r, "revenue_KFactor_AVEDay")),
            parse_number(get_key_val(r, "target_Day", "Target", "target"))
        )
        dtTC = max(
            parse_number(get_key_val(r, "revenue_Installment")),
            parse_number(get_key_val(r, "Doanh thu trả góp", "DT Trả chậm", "doanh thu trả chậm"))
        )
        
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
            "unit": "TR" if lk_info["is_dt"] else "SP",
            "phan_loai": config_map.get(nganh_clean, 1.0)
        })
        
    td_done = [x for x in parsed_td if x["ht"] >= 1.0 and x["actual"] > 0]
    td_pending = [x for x in parsed_td if x["ht"] < 1.0 and x["actual"] > 0]
    td_zero = [x for x in parsed_td if x["actual"] == 0]
    td_done.sort(key=lambda x: x["ht"], reverse=True)
    td_pending.sort(key=lambda x: x["ht"], reverse=True)
    td_zero.sort(key=lambda x: x["target"], reverse=True)
    
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
    
    # Employee Revenue Table Container Card (Bảng Thứ Hạng Doanh Thu Nhân Viên - chèn TRƯỚC bảng chi tiết doanh thu hôm nay)
    data_rt_nv = data.get("Data_Realtime_NV", [])
    if not data_rt_nv:
        data_rt_nv = data.get("Data_NV_BI", [])
    if data_rt_nv and isinstance(data_rt_nv, list) and len(data_rt_nv) > 0:
        parsed_nv_rt = []
        for row in data_rt_nv:
            m_nv = str(get_key_val(row, "mã nv", "ma_nv", "user", "rowcode", default="")).strip()
            if not m_nv or m_nv.lower() == "online" or m_nv == "18001060":
                continue
            t_nv = get_key_val(row, "tên nhân viên", "tên nv", "ten_nv", "rowname", "employeeName", default="") or ""
            
            raw_dt = parse_number(get_key_val(row, "doanh thu quy đổi", "revenue_kfactor", "dt quy đổi", "Doanh thu Quy đổi", default=0))
            if abs(raw_dt) >= 1000000:
                dt_nv = raw_dt / 1000000.0
            else:
                dt_nv = raw_dt
                
            sl_nv = parse_number(get_key_val(row, "số lượng", "soluong", "quantity", "quantity_RT", default=0))
            
            # Lệnh RT1: Những nhân viên có doanh thu bằng 0 thì ẩn đi không hiển thị
            if dt_nv <= 0:
                continue
            # Chỉ hiển thị nhân viên được khai báo ở tab config, ẩn nhân viên khác
            ratio = 0.0
            if 'user_map' in locals() and user_map:
                if m_nv in user_map:
                    ratio = user_map[m_nv]
                elif m_nv.upper() in user_map:
                    ratio = user_map[m_nv.upper()]
                else:
                    continue
            
            target_nv = target_today * ratio
            
            clean_name = t_nv.strip()
            if " - " in clean_name:
                clean_name = clean_name.split(" - ", 1)[1].strip()
            clean_name = clean_name.lstrip("0123456789 -").strip()
            last_name = clean_name.split()[-1] if clean_name else ""
            disp_name = f"{last_name} - {m_nv}" if last_name else (f"{clean_name} - {m_nv}" if clean_name else m_nv)
            
            ht_nv = (dt_nv / target_nv) if target_nv > 0 else (1.0 if dt_nv > 0 else 0.0)
            
            parsed_nv_rt.append({"name": disp_name, "sl": sl_nv, "dt": dt_nv, "target": target_nv, "ht": ht_nv})
        
        parsed_nv_rt.sort(key=lambda x: x["dt"], reverse=True)

        if parsed_nv_rt:
            nv_card_contents = [
                {"type": "text", "text": "🏆 THỨ HẠNG DOANH THU NHÂN VIÊN", "size": "xxs", "color": "#0284c7", "weight": "bold", "margin": "xs"}
            ]
            nv_headers = ["#", "NV", "SL", "DT", "TG", "Còn", "%HT"]
            nv_weights = [1, 4, 1, 2, 2, 2, 2]
            nv_aligns = ["center", "start", "center", "end", "end", "end", "end"]
            nv_card_contents.append(make_table_header(nv_headers, nv_weights, nv_aligns, bg_color="#0284c7"))
            nv_card_contents.append({"type": "separator", "color": "#cbd5e1", "margin": "xs"})

            rank_icons = ["🥇", "🥈", "🥉"]
            for idx, item in enumerate(parsed_nv_rt):
                rank_str = rank_icons[idx] if idx < 3 else f"{idx+1}."
                
                sl_str = fmt_num(item["sl"])
                dt_tr_str = f"{item['dt']:.0f}"
                tg_tr_str = f"{item['target']:.0f}"

                # Còn lại
                con_lai = item['target'] - item['dt']
                if con_lai <= 0 and item['target'] > 0:
                    con_lai_str = "Đạt"
                    con_color = "#059669"
                else:
                    con_lai_str = f"{max(0, con_lai):.0f}"
                    con_color = "#ef4444"

                ht_str = f"{item['ht']*100:.0f}%"

                row_vals = [rank_str, item["name"], sl_str, dt_tr_str, tg_tr_str, con_lai_str, ht_str]
                row_colors = [
                    "#d97706" if idx < 3 else "#64748b", 
                    "#0f172a", 
                    "#475569", 
                    "#059669", 
                    "#475569", 
                    con_color, 
                    get_color_class(item["ht"])
                ]
                nv_card_contents.append(make_table_row(row_vals, nv_weights, nv_aligns, row_colors, bold=(idx < 3)))
                if idx < len(parsed_nv_rt) - 1:
                    nv_card_contents.append({"type": "separator", "color": "#f1f5f9", "margin": "xs"})

            body_contents_rt1.append({
                "type": "box",
                "layout": "vertical",
                "backgroundColor": "#ffffff",
                "borderColor": "#e2e8f0",
                "borderWidth": "1px",
                "cornerRadius": "md",
                "paddingAll": "sm",
                "margin": "md",
                "contents": nv_card_contents
            })

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
                {"type": "text", "text": "⚡ BÁO CÁO REALTIME\n(DOANH THU & TIẾN ĐỘ)", "weight": "bold", "size": "sm", "color": "#ffffff", "align": "center", "wrap": True},
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

    # Bảng Thi Đua Realtime P.2 (Chia 2 khu: Về đích ngày & Chưa về đích ngày - Chuẩn Ảnh 2 mới)
    headers_rt2 = ["#", "NH", "Đạt", "Còn", "MT", "%HT"]
    weights_rt2 = [1, 4, 2, 2, 2, 2]
    aligns_rt2 = ["center", "start", "center", "center", "center", "center"]

    body_contents_rt2 = []

    if td_done:
        done_table_rt = [
            {"type": "text", "text": f"🏆 NHÓM VỀ ĐÍCH NGÀY ({len(td_done)})", "size": "xxs", "color": "#15803d", "weight": "bold", "margin": "xs"},
            make_table_header(headers_rt2, weights_rt2, aligns_rt2, bg_color="#15803d"),
            {"type": "separator", "color": "#bbf7d0", "margin": "xs"}
        ]
        for idx, t in enumerate(td_done):
            unit_tag = "(DT)" if t["unit"] == "TR" else "(SL)"
            display_name = f"{t['name']} {unit_tag}"
            act_str = fmt_num(t['actual'])
            cl_str = "🏆" if t['con_lai'] <= 0 else fmt_num(t['con_lai'])
            tg_str = fmt_num(t['target'])
            ht_str = f"{t['ht']*100:.0f}%"

            name_color = "#dc2626" if t.get("phan_loai") == 2.0 else "#0f172a"
            vals = [str(idx + 1), display_name, act_str, cl_str, tg_str, ht_str]
            colors = ["#64748b", name_color, "#0284c7", "#16a34a" if cl_str == "🏆" else "#dc2626", "#475569", get_color_class(t["ht"])]
            if idx > 0:
                done_table_rt.append({"type": "separator", "color": "#e2e8f0", "margin": "xs"})
            done_table_rt.append(make_table_row(vals, weights_rt2, aligns_rt2, colors))

        body_contents_rt2.append({
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#f0fdf4",
            "borderColor": "#bbf7d0",
            "borderWidth": "1px",
            "cornerRadius": "md",
            "paddingAll": "sm",
            "margin": "xs",
            "contents": done_table_rt
        })

    if td_pending:
        pending_table_rt = [
            {"type": "text", "text": f"🎯 NHÓM CHƯA VỀ ĐÍCH NGÀY ({len(td_pending)})", "size": "xxs", "color": "#b91c1c", "weight": "bold", "margin": "xs"},
            make_table_header(headers_rt2, weights_rt2, aligns_rt2, bg_color="#0f766e"),
            {"type": "separator", "color": "#cbd5e1", "margin": "xs"}
        ]
        for idx, t in enumerate(td_pending):
            unit_tag = "(DT)" if t["unit"] == "TR" else "(SL)"
            display_name = f"{t['name']} {unit_tag}"
            act_str = fmt_num(t['actual'])
            cl_str = "🏆" if t['con_lai'] <= 0 else fmt_num(t['con_lai'])
            tg_str = fmt_num(t['target'])
            ht_str = f"{t['ht']*100:.0f}%"

            name_color = "#dc2626" if t.get("phan_loai") == 2.0 else "#0f172a"
            vals = [str(idx + 1), display_name, act_str, cl_str, tg_str, ht_str]
            colors = ["#64748b", name_color, "#0284c7", "#16a34a" if cl_str == "🏆" else "#dc2626", "#475569", get_color_class(t["ht"])]
            if idx > 0:
                pending_table_rt.append({"type": "separator", "color": "#f1f5f9", "margin": "xs"})
            pending_table_rt.append(make_table_row(vals, weights_rt2, aligns_rt2, colors))

        body_contents_rt2.append({
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#ffffff",
            "borderColor": "#ccfbf1",
            "borderWidth": "1px",
            "cornerRadius": "md",
            "paddingAll": "sm",
            "margin": "xs",
            "contents": pending_table_rt
        })

    if td_zero:
        col1_items = []
        col2_items = []
        for idx, t in enumerate(td_zero):
            name_color = "#dc2626" if t.get("phan_loai") == 2.0 else "#475569"
            mt_val = fmt_num(t["target"])
            item_text = f"• {t['name']} ({mt_val})"
            item_dict = {"type": "text", "text": item_text, "size": "xxs", "color": name_color, "wrap": True}
            if idx % 2 == 0:
                col1_items.append(item_dict)
            else:
                col2_items.append(item_dict)

        zero_box = {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#f8fafc",
            "borderColor": "#e2e8f0",
            "borderWidth": "1px",
            "cornerRadius": "md",
            "paddingAll": "sm",
            "margin": "xs",
            "contents": [
                {"type": "text", "text": f"💤 NHÓM CHƯA PHÁT SINH SỐ ({len(td_zero)})", "size": "xxs", "color": "#64748b", "weight": "bold", "margin": "xs"},
                {"type": "separator", "color": "#cbd5e1", "margin": "xs"},
                {
                    "type": "box",
                    "layout": "horizontal",
                    "margin": "xs",
                    "spacing": "sm",
                    "contents": [
                        {"type": "box", "layout": "vertical", "flex": 1, "contents": col1_items if col1_items else [{"type": "filler"}]},
                        {"type": "box", "layout": "vertical", "flex": 1, "contents": col2_items if col2_items else [{"type": "filler"}]}
                    ]
                }
            ]
        }
        body_contents_rt2.append(zero_box)

    flex_bubble_rt2 = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#0f766e",
            "paddingAll": "md",
            "contents": [
                {"type": "text", "text": "⚡ BÁO CÁO REALTIME\n(NHÓM HÀNG THI ĐUA)", "weight": "bold", "size": "sm", "color": "#ffffff", "align": "center", "wrap": True},
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

def build_help_commands_flex():
    """
    Tạo Flex Message Hướng Dẫn TOÀN BỘ Danh Sách Câu Lệnh Hỗ Trợ (#lenh)
    """
    commands_data = [
        # --- BÁO CÁO DOANH THU & THI ĐUA ---
        {"cmd": "LK1", "desc": "Báo cáo Lũy kế Doanh thu & Ngành hàng thi đua tháng (Flex P.1 & P.2)", "color": "#1e40af"},
        {"cmd": "RT1", "desc": "Báo cáo Realtime Doanh thu & Ngành hàng thi đua ngày (Flex P.1 & P.2)", "color": "#0284c7"},
        {"cmd": "NV0", "desc": "Bảng Xếp Hạng Doanh Thu NV Pro + Carousel Top 8 NV (#1 - #8)", "color": "#0f766e"},
        {"cmd": "NV1", "desc": "Hiển thị tiếp danh sách Thẻ KPI các Nhân viên còn lại (#9 trở đi)", "color": "#0f766e"},
        {"cmd": "NV <mã>", "desc": "Xem riêng 1 Thẻ KPI Nhân viên (VD: nv 61169 hoặc nv Dương)", "color": "#d97706"},
        {"cmd": "NV <mã1>,<mã2>...", "desc": "Xem nhiều Thẻ KPI NV cùng lúc (VD: nv 61169,98372,132697)", "color": "#b45309"},
        # --- CHECKLIST & VẬN HÀNH ---
        {"cmd": "sang / chieu", "desc": "Checklist danh sách công việc đầu ca Sáng hoặc ca Chiều", "color": "#475569"},
        {"cmd": "vs", "desc": "Checklist kiểm tra hình ảnh trước 10h sáng", "color": "#475569"},
        {"cmd": "vesinh", "desc": "Bảng phân công khu vực vệ sinh (vesinh sang / vesinh chieu)", "color": "#16a34a"},
        {"cmd": "ăn / ansang", "desc": "Điểm danh ăn uống nhân sự theo ca (ansang / anchieu)", "color": "#ea580c"},
        # --- GIAO VIỆC & HỆ THỐNG ---
        {"cmd": "việc @tên: <mô tả>", "desc": "Giao việc phát sinh cá nhân hoặc nhóm (VD: việc @Long: dọn kho)", "color": "#8b5cf6"},
        {"cmd": "id", "desc": "Tra cứu Mã User ID cá nhân & Group ID nhóm chat", "color": "#64748b"},
        {"cmd": "#lenh", "desc": "Hiển thị Bảng Hướng Dẫn Danh Sách Tất Cả Cú Pháp Lệnh Hỗ Trợ", "color": "#0f172a"}
    ]

    cmd_contents = []
    for idx, item in enumerate(commands_data):
        if idx > 0:
            cmd_contents.append({"type": "separator", "color": "#f1f5f9", "margin": "xs"})

        box_item = {
            "type": "box",
            "layout": "vertical",
            "margin": "xs",
            "contents": [
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {
                            "type": "box",
                            "layout": "vertical",
                            "backgroundColor": item["color"],
                            "paddingAll": "xs",
                            "cornerRadius": "xs",
                            "flex": 5,
                            "contents": [
                                {"type": "text", "text": item["cmd"], "size": "xxs", "weight": "bold", "color": "#ffffff", "align": "center"}
                            ]
                        },
                        {"type": "filler", "flex": 1}
                    ]
                },
                {"type": "text", "text": item["desc"], "size": "xxs", "color": "#334155", "wrap": True, "margin": "xs"}
            ]
        }
        cmd_contents.append(box_item)

    bubble = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#1e293b",
            "paddingAll": "md",
            "contents": [
                {"type": "text", "text": "📖 TOÀN BỘ DANH SÁCH CÂU LỆNH HỖ TRỢ", "weight": "bold", "size": "sm", "color": "#ffffff", "align": "center"},
                {"type": "text", "text": "Gõ đúng cú pháp để tra cứu báo cáo & vận hành tương ứng", "size": "xxs", "color": "#94a3b8", "align": "center", "margin": "xs"}
            ]
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#ffffff",
            "paddingAll": "sm",
            "contents": cmd_contents
        }
    }
    return bubble
