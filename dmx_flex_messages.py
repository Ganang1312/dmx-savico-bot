import pytz
from datetime import datetime
from dmx_data_provider import get_dashboard_data

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
    ]
    for old, new in replacements:
        s = s.replace(old, new)
        
    if len(s) > 15:
        s = s[:14] + "…"
    return s

def get_key_val(row, *possible_keys, default=None):
    if not row or not isinstance(row, dict):
        return default
    
    # 1. Exact match in order of possible_keys
    for pk in possible_keys:
        if pk in row:
            return row[pk]
            
    # 2. Case insensitive match in order of possible_keys
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

def build_luyke_flex():
    data = get_dashboard_data("Config_ThiDua,Data_BI,Data_ThiDua")
    bi_rows = data.get("Data_BI", [])
    config_rows = data.get("Config_ThiDua", [])
    td_rows = data.get("Data_ThiDua", [])
    
    tz = pytz.timezone('Asia/Ho_Chi_Minh')
    now = datetime.now(tz)
    now_str = now.strftime("%H:%M - %d/%m/%Y")
    
    # 1. Total KPI calculations
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
    revRem = max(0.0, tTG - tDT)
    
    tTC = sum(parse_number(get_key_val(b, "revenue_installment", "doanh thu trả chậm", default=0.0)) for b in bi_rows)
    tDTGoc = sum(parse_number(get_key_val(b, "doanh thu", default=0.0)) for b in bi_rows)
    totalTyLeTC = tTC / tDTGoc if tDTGoc > 0 else (tTC / tDT if tDT > 0 else 0.0)
    
    # 2. Filter & Process Category sales
    parsed_bi = []
    for b in bi_rows:
        nganh = get_key_val(b, "nhóm ngành hàng", "ngành hàng", "salegroupmastername", default=None)
        if not nganh or str(nganh).strip().upper() == "N/A":
            continue
        dt = parse_number(get_key_val(b, "doanh thu quy đổi", default=0.0))
        sl = parse_number(get_key_val(b, "số lượng", "quantity", default=0.0))
        tg = parse_number(get_key_val(b, "target", default=0.0))
        if dt > 0 or sl > 0:
            parsed_bi.append({
                "name": shorten_name(nganh),
                "sl": int(sl),
                "dt": dt,
                "tg": tg,
                "ht": dt / tg if tg > 0 else 0.0
            })
    parsed_bi.sort(key=lambda x: x["dt"], reverse=True)
    
    # 3. Process Compete list
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
    td_zero = [x for x in parsed_td if x["actual"] == 0]
    td_done.sort(key=lambda x: x["ht_dk"], reverse=True)
    td_pending.sort(key=lambda x: x["ht_dk"], reverse=True)
    
    # 4. Generate Flex UI components (Bold Vibrant Cards & High Contrast Text)
    body_contents = [
        {
            "type": "box",
            "layout": "horizontal",
            "margin": "md",
            "spacing": "xs",
            "contents": [
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "backgroundColor": "#1e40af",
                    "paddingAll": "sm",
                    "cornerRadius": "md",
                    "contents": [
                        {"type": "text", "text": "Doanh Thu Lũy Kế", "size": "xxs", "color": "#ffffff", "weight": "bold", "align": "center"},
                        {"type": "text", "text": f"{fmt_num(tDT)} Tr ({totalHT*100:.0f}%)", "size": "xs", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "backgroundColor": "#475569",
                    "paddingAll": "sm",
                    "cornerRadius": "md",
                    "contents": [
                        {"type": "text", "text": "Mục Tiêu Tháng", "size": "xxs", "color": "#ffffff", "weight": "bold", "align": "center"},
                        {"type": "text", "text": f"{fmt_num(tTG)} Tr", "size": "xs", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                }
            ]
        },
        {
            "type": "box",
            "layout": "horizontal",
            "margin": "xs",
            "spacing": "xs",
            "contents": [
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "backgroundColor": "#ef4444",
                    "paddingAll": "sm",
                    "cornerRadius": "md",
                    "contents": [
                        {"type": "text", "text": "Còn Thiếu", "size": "xxs", "color": "#ffffff", "weight": "bold", "align": "center"},
                        {"type": "text", "text": f"{fmt_num(revRem)} Tr", "size": "xs", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "backgroundColor": "#f59e0b",
                    "paddingAll": "sm",
                    "cornerRadius": "md",
                    "contents": [
                        {"type": "text", "text": "Dự Kiến Tháng", "size": "xxs", "color": "#ffffff", "weight": "bold", "align": "center"},
                        {"type": "text", "text": f"{fmt_num(totalDKThang)} Tr", "size": "xs", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                }
            ]
        },
        {
            "type": "box",
            "layout": "horizontal",
            "margin": "xs",
            "spacing": "xs",
            "contents": [
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "backgroundColor": "#9333ea",
                    "paddingAll": "sm",
                    "cornerRadius": "md",
                    "contents": [
                        {"type": "text", "text": "Trả Góp", "size": "xxs", "color": "#ffffff", "weight": "bold", "align": "center"},
                        {"type": "text", "text": f"{fmt_num(tTC)} Tr ({totalTyLeTC*100:.0f}%)", "size": "xs", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "backgroundColor": "#10b981",
                    "paddingAll": "sm",
                    "cornerRadius": "md",
                    "contents": [
                        {"type": "text", "text": "Thi Đua Đạt", "size": "xxs", "color": "#ffffff", "weight": "bold", "align": "center"},
                        {"type": "text", "text": f"{cnt_dk}/{len(parsed_td)} Nhóm", "size": "xs", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                }
            ]
        },
        {"type": "separator", "color": "#cbd5e1", "margin": "md"},
        {"type": "text", "text": "📈 TIẾN ĐỘ HOÀN THÀNH THÁNG", "size": "xxs", "color": "#0f172a", "align": "center", "margin": "md", "weight": "bold"},
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
        {"type": "separator", "color": "#cbd5e1", "margin": "md"}
    ]
    
    # Table 1: Revenue details
    body_contents.append({"type": "text", "text": "📊 CHI TIẾT DOANH THU LŨY KẾ", "size": "xxs", "color": "#0284c7", "weight": "bold", "margin": "md"})
    headers = ["STT", "Ngành hàng", "SL", "DTQĐ", "Target", "%HT"]
    weights = [1, 3, 1, 2, 2, 2]
    aligns = ["start", "start", "center", "center", "center", "end"]
    body_contents.append(make_table_header(headers, weights, aligns, bg_color="#0284c7"))
    body_contents.append({"type": "separator", "color": "#cbd5e1", "margin": "xs"})
    
    for idx, b in enumerate(parsed_bi[:6]):
        vals = [idx+1, b["name"], fmt_num(b["sl"]), fmt_num(b["dt"]), fmt_num(b["tg"]), f"{b['ht']*100:.0f}%"]
        colors = ["#64748b", "#0f172a", "#0f172a", "#0284c7", "#475569", get_color_class(b["ht"])]
        body_contents.append(make_table_row(vals, weights, aligns, colors))
        body_contents.append({"type": "separator", "color": "#f1f5f9", "margin": "xs"})
    
    # Row Total (Solid Amber Background, White Bold Text)
    tot_sl = sum(x["sl"] for x in parsed_bi)
    tot_vals = ["⭐", "TỔNG CỘNG", fmt_num(tot_sl), fmt_num(tDT), fmt_num(tTG), f"{totalHT*100:.0f}%"]
    tot_colors = ["#ffffff", "#ffffff", "#ffffff", "#ffffff", "#ffffff", "#ffffff"]
    body_contents.append(make_table_row(tot_vals, weights, aligns, tot_colors, bold=True, bg_color="#f59e0b"))
    
    headers_td = ["STT", "Ngành hàng thi đua", "Đạt", "Target", "%DK"]
    headers_td2 = ["STT", "Ngành hàng thi đua", "Đạt", "Thiếu", "%DK"]
    weights_td = [1, 4, 2, 2, 2]
    aligns_td = ["start", "start", "center", "center", "end"]

    # Table 2: Compete Items (Completed with actual > 0)
    if td_done:
        body_contents.append({"type": "separator", "color": "#cbd5e1", "margin": "md"})
        body_contents.append({"type": "text", "text": "🏆 THI ĐUA DỰ KIẾN ĐẠT (DKHT ≥ 100%)", "size": "xxs", "color": "#16a34a", "weight": "bold", "margin": "md"})
        body_contents.append(make_table_header(headers_td, weights_td, aligns_td, bg_color="#10b981"))
        body_contents.append({"type": "separator", "color": "#cbd5e1", "margin": "xs"})
        for idx, t in enumerate(td_done):
            vals = [idx+1, t["name"], f"{fmt_num(t['actual'])} {t['unit']}", fmt_num(t['target']), f"{t['ht_dk']*100:.0f}%"]
            colors = ["#64748b", "#0f172a", "#0284c7", "#475569", "#16a34a"]
            body_contents.append(make_table_row(vals, weights_td, aligns_td, colors))
            body_contents.append({"type": "separator", "color": "#f1f5f9", "margin": "xs"})
            
    # Table 3: Compete Items (Pending with actual > 0)
    if td_pending:
        body_contents.append({"type": "separator", "color": "#cbd5e1", "margin": "md"})
        body_contents.append({"type": "text", "text": "🎯 THI ĐUA CHƯA ĐẠT (DKHT < 100%)", "size": "xxs", "color": "#dc2626", "weight": "bold", "margin": "md"})
        body_contents.append(make_table_header(headers_td2, weights_td, aligns_td, bg_color="#ef4444"))
        body_contents.append({"type": "separator", "color": "#cbd5e1", "margin": "xs"})
        for idx, t in enumerate(td_pending):
            vals = [idx+1, t["name"], fmt_num(t['actual']), fmt_num(t['con_lai']), f"{t['ht_dk']*100:.0f}%"]
            colors = ["#64748b", "#0f172a", "#d97706", "#dc2626", get_color_class(t["ht_dk"])]
            body_contents.append(make_table_row(vals, weights_td, aligns_td, colors))
            body_contents.append({"type": "separator", "color": "#f1f5f9", "margin": "xs"})

    # 3-column Grid for Zero Actual Compete Items
    if td_zero:
        body_contents.append({"type": "separator", "color": "#cbd5e1", "margin": "md"})
        body_contents.append({
            "type": "text",
            "text": f"🚫 CÁC NHÓM THI ĐUA CHƯA CÓ DT ({len(td_zero)} nhóm)",
            "size": "xxs",
            "color": "#ef4444",
            "weight": "bold",
            "margin": "md"
        })
        chunks = [td_zero[i:i + 3] for i in range(0, len(td_zero), 3)]
        for chunk in chunks:
            row_contents = []
            for t in chunk:
                target_str = fmt_num(t["target"])
                row_contents.append({
                    "type": "text",
                    "text": f"• {t['name']} ({target_str})",
                    "size": "xxs",
                    "color": "#64748b",
                    "flex": 1,
                    "wrap": True
                })
            while len(row_contents) < 3:
                row_contents.append({
                    "type": "text",
                    "text": "",
                    "size": "xxs",
                    "flex": 1
                })
            body_contents.append({
                "type": "box",
                "layout": "horizontal",
                "margin": "xs",
                "contents": row_contents
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
                {"type": "text", "text": f"🕒 Cập nhật: {now_str}", "size": "xxs", "color": "#dbeafe", "align": "center", "margin": "xs"}
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
        
    # Get active config staff
    emp_targets = {}
    active_staff_names = {}
    if config_rows and len(config_rows) > 0:
        for r in config_rows:
            emp_name = get_key_val(r, "Họ và tên", "tên nhân viên", default=None)
            pct = parse_number(get_key_val(r, "% chia", "tỷ lệ %", default=0.0))
            if emp_name and pct > 0:
                ratio = pct if pct <= 1.0 else pct / 100.0
                emp_targets[str(emp_name).strip()] = ratio * total_target
                active_staff_names[str(emp_name).strip().upper()] = str(emp_name).strip()

    # Sum employee actuals
    emp_actuals = {}
    for r in nv_rows:
        name = get_key_val(r, "staffUserName", "tên nv", "Họ và tên", default=None)
        if not name: 
            continue
        name_str = str(name).strip()
        actual = parse_number(get_key_val(r, "Doanh thu Quy đổi", "Doanh thu", "Value_Compe", default=0.0))
        emp_actuals[name_str] = emp_actuals.get(name_str, 0.0) + actual

    # Map name case insensitive
    emp_list = []
    for staff_upper, clean_name in active_staff_names.items():
        actual = 0.0
        for act_name, val in emp_actuals.items():
            if act_name.upper() == staff_upper:
                actual = val
                break
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
    
    body_contents = [
        {
            "type": "box",
            "layout": "horizontal",
            "margin": "md",
            "spacing": "xs",
            "contents": [
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "backgroundColor": "#0284c7",
                    "paddingAll": "sm",
                    "cornerRadius": "md",
                    "contents": [
                        {"type": "text", "text": "Doanh Thu Cửa Hàng", "size": "xxs", "color": "#ffffff", "weight": "bold", "align": "center"},
                        {"type": "text", "text": f"{fmt_num(tDT)} Tr", "size": "xs", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "backgroundColor": "#475569",
                    "paddingAll": "sm",
                    "cornerRadius": "md",
                    "contents": [
                        {"type": "text", "text": "Mục Tiêu Chung", "size": "xxs", "color": "#ffffff", "weight": "bold", "align": "center"},
                        {"type": "text", "text": f"{fmt_num(total_target)} Tr", "size": "xs", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                }
            ]
        },
        {
            "type": "box",
            "layout": "horizontal",
            "margin": "xs",
            "spacing": "xs",
            "contents": [
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "backgroundColor": "#9333ea",
                    "paddingAll": "sm",
                    "cornerRadius": "md",
                    "contents": [
                        {"type": "text", "text": "Số Nhân Viên", "size": "xxs", "color": "#ffffff", "weight": "bold", "align": "center"},
                        {"type": "text", "text": f"{len(emp_list)} Nhân Sự", "size": "xs", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "backgroundColor": "#10b981",
                    "paddingAll": "sm",
                    "cornerRadius": "md",
                    "contents": [
                        {"type": "text", "text": "% Đạt Toàn Cửa Hàng", "size": "xxs", "color": "#ffffff", "weight": "bold", "align": "center"},
                        {"type": "text", "text": f"{totalHT*100:.0f}%", "size": "xs", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                }
            ]
        },
        {"type": "separator", "color": "#cbd5e1", "margin": "md"},
        {"type": "text", "text": "👥 CHI TIẾT XẾP HẠNG DOANH THU NHÂN VIÊN", "size": "xxs", "color": "#0284c7", "weight": "bold", "margin": "md"},
    ]
    
    headers = ["STT", "Nhân viên", "Đạt được", "Chỉ tiêu", "%HT"]
    weights = [1, 4, 3, 3, 2]
    aligns = ["start", "start", "center", "center", "end"]
    body_contents.append(make_table_header(headers, weights, aligns, bg_color="#0f766e"))
    body_contents.append({"type": "separator", "color": "#cbd5e1", "margin": "xs"})
    
    for idx, e in enumerate(emp_list):
        crown = "🥇" if idx == 0 else ("🥈" if idx == 1 else ("🥉" if idx == 2 else idx+1))
        vals = [crown, e["name"], f"{fmt_num(e['actual'])} Tr", f"{fmt_num(e['target'])} Tr", f"{e['pct']*100:.0f}%"]
        colors = ["#d97706" if idx < 3 else "#64748b", "#0f172a", "#0284c7", "#475569", get_color_class(e["pct"])]
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
    
    # 1. Target Today logic from baocao_realtime.html
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

    # 2. Total Realtime Today actual
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
    
    # Formula from baocao_realtime.html
    tongDTHienTai = lk_tDT + rt_total
    duKienThang = (tongDTHienTai / days_passed) * days_in_month if days_passed > 0 else 0.0
    ptDuKienThang = (duKienThang / lk_tTG * 100) if lk_tTG > 0 else 0.0
    
    # Time ratio (Assume 9:00 to 22:00 = 13 hours shift)
    elapsed_hours = (now.hour + now.minute / 60.0) - 9.0
    if elapsed_hours < 0: elapsed_hours = 0.0
    if elapsed_hours > 13: elapsed_hours = 13.0
    time_ratio = elapsed_hours / 13.0
    
    # 3. Compete list logic from baocao_realtime.html
    bi_map = {x["name"].lower().strip(): x["dt"] for x in parsed_rt_bi}
    
    # Make luyke map
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
    td_zero = [x for x in parsed_td if x["actual"] == 0]
    td_done.sort(key=lambda x: x["ht"], reverse=True)
    td_pending.sort(key=lambda x: x["ht"], reverse=True)
    
    # 4. Build Flex structure (Light Theme - Rich Bold Backgrounds & High Contrast)
    body_contents = [
        # Realtime summary box (like baocao_realtime.html)
        {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#eff6ff",
            "borderColor": "#0284c7",
            "borderWidth": "light",
            "paddingAll": "sm",
            "cornerRadius": "md",
            "margin": "sm",
            "contents": [
                {
                    "type": "text",
                    "text": f"🔥 Với DT hôm nay đạt 🚀 {fmt_num(rt_total)} TR, LK siêu thị đạt 💰 {fmt_num(tongDTHienTai)} TR. DK hết tháng về đích 🎯 {fmt_num(duKienThang)} TR ({ptDuKienThang:.1f}%).",
                    "size": "xxs",
                    "color": "#1e40af",
                    "weight": "bold",
                    "wrap": True,
                    "align": "center"
                }
            ]
        },
        
        # Progress bars
        {"type": "text", "text": "🕒 TIẾN ĐỘ THỜI GIAN", "size": "xxs", "color": "#475569", "weight": "bold", "margin": "md"},
        {
            "type": "box",
            "layout": "horizontal",
            "margin": "xs",
            "contents": [
                {"type": "text", "text": f"{time_ratio*100:.0f}%", "size": "xs", "color": "#0284c7", "weight": "bold", "flex": 2},
                {"type": "text", "text": f"(Còn {(13.0 - elapsed_hours):.1f}h)", "size": "xxs", "color": "#64748b", "align": "end", "flex": 8}
            ]
        },
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
                    "backgroundColor": "#0284c7",
                    "height": "6px",
                    "cornerRadius": "md",
                    "width": f"{min(100, round(time_ratio * 100))}%",
                    "contents": [{"type": "filler"}]
                }
            ]
        },
        {"type": "text", "text": "🎯 TIẾN ĐỘ DOANH THU NGÀY", "size": "xxs", "color": "#475569", "weight": "bold", "margin": "sm"},
        {
            "type": "box",
            "layout": "horizontal",
            "margin": "xs",
            "contents": [
                {"type": "text", "text": f"{htChung*100:.0f}%", "size": "xs", "color": get_color_class(htChung), "weight": "bold", "flex": 2},
                {"type": "text", "text": f"(Thiếu {fmt_num(thieuDTRT)} Tr)", "size": "xxs", "color": "#dc2626", "align": "end", "flex": 8}
            ]
        },
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
                    "backgroundColor": get_color_class(htChung),
                    "height": "6px",
                    "cornerRadius": "md",
                    "width": f"{min(100, round(htChung * 100))}%",
                    "contents": [{"type": "filler"}]
                }
            ]
        },
        {"type": "separator", "color": "#cbd5e1", "margin": "md"},
        
        # KPI boxes (Solid Bold Colored Backgrounds with Crisp White Text)
        {
            "type": "box",
            "layout": "horizontal",
            "margin": "md",
            "spacing": "xs",
            "contents": [
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "backgroundColor": "#0284c7",
                    "paddingAll": "sm",
                    "cornerRadius": "md",
                    "contents": [
                        {"type": "text", "text": "DT Thực Hiện", "size": "xxs", "color": "#ffffff", "weight": "bold", "align": "center"},
                        {"type": "text", "text": f"{fmt_num(rt_total)} Tr ({htChung*100:.0f}%)", "size": "xs", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "backgroundColor": "#475569",
                    "paddingAll": "sm",
                    "cornerRadius": "md",
                    "contents": [
                        {"type": "text", "text": "Mục Tiêu Ngày", "size": "xxs", "color": "#ffffff", "weight": "bold", "align": "center"},
                        {"type": "text", "text": f"{fmt_num(target_today)} Tr", "size": "xs", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                }
            ]
        },
        {
            "type": "box",
            "layout": "horizontal",
            "margin": "xs",
            "spacing": "xs",
            "contents": [
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "backgroundColor": "#9333ea",
                    "paddingAll": "sm",
                    "cornerRadius": "md",
                    "contents": [
                        {"type": "text", "text": "Trả Góp", "size": "xxs", "color": "#ffffff", "weight": "bold", "align": "center"},
                        {"type": "text", "text": f"{fmt_num(rt_tTC)} Tr ({rt_tc_pct:.0f}%)", "size": "xs", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "backgroundColor": "#10b981",
                    "paddingAll": "sm",
                    "cornerRadius": "md",
                    "contents": [
                        {"type": "text", "text": "Thi Đua Đạt", "size": "xxs", "color": "#ffffff", "weight": "bold", "align": "center"},
                        {"type": "text", "text": f"{rt_cntVD}/{len(parsed_td)} Nhóm", "size": "xs", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                }
            ]
        },
        {"type": "separator", "color": "#cbd5e1", "margin": "md"}
    ]
    
    # Table 1: Revenue details
    body_contents.append({"type": "text", "text": "⚡ CHI TIẾT DOANH THU HÔM NAY", "size": "xxs", "color": "#0284c7", "weight": "bold", "margin": "md"})
    headers = ["STT", "Ngành hàng", "SL", "DTQĐ", "Target", "%HT"]
    weights = [1, 3, 1, 2, 2, 2]
    aligns = ["start", "start", "center", "center", "center", "end"]
    body_contents.append(make_table_header(headers, weights, aligns, bg_color="#0284c7"))
    body_contents.append({"type": "separator", "color": "#cbd5e1", "margin": "xs"})
    
    for idx, b in enumerate(parsed_rt_bi[:6]):
        vals = [idx+1, b["name"], fmt_num(b["sl"]), fmt_num(b["dt"]), fmt_num(b["tg"]), f"{b['ht']*100:.0f}%"]
        colors = ["#64748b", "#0f172a", "#0f172a", "#0284c7", "#475569", get_color_class(b["ht"])]
        body_contents.append(make_table_row(vals, weights, aligns, colors))
        body_contents.append({"type": "separator", "color": "#f1f5f9", "margin": "xs"})
        
    # Total row (Solid Amber Background, White Bold Text)
    tot_sl = sum(x["sl"] for x in parsed_rt_bi)
    totalHTCol2 = rt_total / rt_tTarget if rt_tTarget > 0 else 0.0
    tot_vals = ["⭐", "TỔNG CỘNG", fmt_num(tot_sl), fmt_num(rt_total), fmt_num(rt_tTarget), f"{totalHTCol2*100:.0f}%"]
    tot_colors = ["#ffffff", "#ffffff", "#ffffff", "#ffffff", "#ffffff", "#ffffff"]
    body_contents.append(make_table_row(tot_vals, weights, aligns, tot_colors, bold=True, bg_color="#f59e0b"))
    
    headers_td = ["STT", "Ngành hàng thi đua", "Đạt", "Mục tiêu", "%HT"]
    headers_td2 = ["STT", "Ngành hàng thi đua", "Đạt", "Còn thiếu", "%HT"]
    weights_td = [1, 4, 2, 2, 2]
    aligns_td = ["start", "start", "center", "center", "end"]

    # Table 2: Compete Items (Completed with actual > 0)
    if td_done:
        body_contents.append({"type": "separator", "color": "#cbd5e1", "margin": "md"})
        body_contents.append({"type": "text", "text": "🏆 THI ĐUA ĐÃ ĐẠT CHỈ TIÊU NGÀY", "size": "xxs", "color": "#16a34a", "weight": "bold", "margin": "md"})
        body_contents.append(make_table_header(headers_td, weights_td, aligns_td, bg_color="#10b981"))
        body_contents.append({"type": "separator", "color": "#cbd5e1", "margin": "xs"})
        for idx, t in enumerate(td_done):
            vals = [idx+1, t["name"], f"{fmt_num(t['actual'])} {t['unit']}", fmt_num(t['target']), f"{t['ht']*100:.0f}%"]
            colors = ["#64748b", "#0f172a", "#0284c7", "#475569", "#16a34a"]
            body_contents.append(make_table_row(vals, weights_td, aligns_td, colors))
            body_contents.append({"type": "separator", "color": "#f1f5f9", "margin": "xs"})
            
    # Table 3: Compete Items (Pending with actual > 0)
    if td_pending:
        body_contents.append({"type": "separator", "color": "#cbd5e1", "margin": "md"})
        body_contents.append({"type": "text", "text": "🎯 THI ĐUA CHƯA ĐẠT CHỈ TIÊU NGÀY", "size": "xxs", "color": "#dc2626", "weight": "bold", "margin": "md"})
        body_contents.append(make_table_header(headers_td2, weights_td, aligns_td, bg_color="#ef4444"))
        body_contents.append({"type": "separator", "color": "#cbd5e1", "margin": "xs"})
        for idx, t in enumerate(td_pending):
            vals = [idx+1, t["name"], f"{fmt_num(t['actual'])} {t['unit']}", fmt_num(t['con_lai']), f"{t['ht']*100:.0f}%"]
            colors = ["#64748b", "#0f172a", "#d97706", "#dc2626", get_color_class(t["ht"])]
            body_contents.append(make_table_row(vals, weights_td, aligns_td, colors))
            body_contents.append({"type": "separator", "color": "#f1f5f9", "margin": "xs"})

    # 3-column Grid for Zero Actual Compete Items
    if td_zero:
        body_contents.append({"type": "separator", "color": "#cbd5e1", "margin": "md"})
        body_contents.append({
            "type": "text",
            "text": f"🚫 CÁC NHÓM THI ĐUA CHƯA CÓ DT ({len(td_zero)} nhóm)",
            "size": "xxs",
            "color": "#ef4444",
            "weight": "bold",
            "margin": "md"
        })
        chunks = [td_zero[i:i + 3] for i in range(0, len(td_zero), 3)]
        for chunk in chunks:
            row_contents = []
            for t in chunk:
                target_str = fmt_num(t["target"])
                row_contents.append({
                    "type": "text",
                    "text": f"• {t['name']} ({target_str})",
                    "size": "xxs",
                    "color": "#64748b",
                    "flex": 1,
                    "wrap": True
                })
            while len(row_contents) < 3:
                row_contents.append({
                    "type": "text",
                    "text": "",
                    "size": "xxs",
                    "flex": 1
                })
            body_contents.append({
                "type": "box",
                "layout": "horizontal",
                "margin": "xs",
                "contents": row_contents
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
                {"type": "text", "text": f"🕒 Cập nhật: {now_str}", "size": "xxs", "color": "#e0f2fe", "align": "center", "margin": "xs"}
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
