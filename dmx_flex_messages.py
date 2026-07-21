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

def make_table_header(cols, weights, aligns=None):
    if not aligns:
        aligns = ["start"] * len(cols)
    contents = []
    for c, w, a in zip(cols, weights, aligns):
        contents.append({
            "type": "text",
            "text": c,
            "weight": "bold",
            "size": "xxs",
            "color": "#94a3b8",
            "flex": w,
            "align": a
        })
    return {
        "type": "box",
        "layout": "horizontal",
        "margin": "md",
        "contents": contents
    }

def make_table_row(vals, weights, aligns=None, colors=None, bold=False):
    if not aligns:
        aligns = ["start"] * len(vals)
    if not colors:
        colors = ["#ffffff"] * len(vals)
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
    return {
        "type": "box",
        "layout": "horizontal",
        "margin": "xs",
        "contents": contents
    }

def get_color_class(pct):
    if pct >= 1.0:
        return "#10b981" # Green
    elif pct >= 0.8:
        return "#3b82f6" # Blue
    elif pct >= 0.5:
        return "#f97316" # Orange
    else:
        return "#ef4444" # Red

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
    totalTyLeTC = tTC / tDTGoc if tDTGoc > 0 else 0.0
    
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
                "name": str(nganh).replace("NNH ", "").strip(),
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
            "name": str(nganh).replace("NNH ", "").strip(),
            "actual": actual,
            "con_lai": con_lai,
            "target": tg,
            "ht": ht_target,
            "ht_dk": ht_du_kien,
            "unit": "TR" if is_dt else "SP"
        })
        
    td_done = [x for x in parsed_td if x["ht_dk"] >= 1.0]
    td_pending = [x for x in parsed_td if x["ht_dk"] < 1.0]
    td_done.sort(key=lambda x: x["ht_dk"], reverse=True)
    td_pending.sort(key=lambda x: x["ht_dk"], reverse=True)
    
    # 4. Generate Flex UI components
    # Header & KPI Info
    body_contents = [
        {
            "type": "box",
            "layout": "horizontal",
            "margin": "md",
            "contents": [
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "contents": [
                        {"type": "text", "text": "Doanh Thu Lũy Kế", "size": "xxs", "color": "#94a3b8", "align": "center"},
                        {"type": "text", "text": f"{tDT:.1f} M", "size": "sm", "color": "#38bdf8", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "contents": [
                        {"type": "text", "text": "Mục Tiêu Tháng", "size": "xxs", "color": "#94a3b8", "align": "center"},
                        {"type": "text", "text": f"{tTG:.1f} M", "size": "sm", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                }
            ]
        },
        {
            "type": "box",
            "layout": "horizontal",
            "margin": "sm",
            "contents": [
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "contents": [
                        {"type": "text", "text": "Còn Thiếu", "size": "xxs", "color": "#94a3b8", "align": "center"},
                        {"type": "text", "text": f"{revRem:.1f} M", "size": "sm", "color": "#ef4444", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "contents": [
                        {"type": "text", "text": "Dự Kiến Tháng", "size": "xxs", "color": "#94a3b8", "align": "center"},
                        {"type": "text", "text": f"{totalDKThang:.1f} M", "size": "sm", "color": "#fbbf24", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                }
            ]
        },
        {
            "type": "box",
            "layout": "horizontal",
            "margin": "sm",
            "contents": [
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "contents": [
                        {"type": "text", "text": "Trả Góp", "size": "xxs", "color": "#94a3b8", "align": "center"},
                        {"type": "text", "text": f"{tTC:.1f} M ({totalTyLeTC*100:.1f}%)", "size": "xs", "color": "#a855f7", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "contents": [
                        {"type": "text", "text": "Thi Đua Đạt", "size": "xxs", "color": "#94a3b8", "align": "center"},
                        {"type": "text", "text": f"{cnt_dk}/{len(parsed_td)} Nhóm", "size": "xs", "color": "#10b981", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                }
            ]
        },
        {"type": "separator", "color": "#ffffff26", "margin": "md"},
        {"type": "text", "text": "📈 TIẾN ĐỘ HOÀN THÀNH THÁNG", "size": "xxs", "color": "#ffffff", "align": "center", "margin": "md", "weight": "bold"},
        {"type": "text", "text": f"{totalHT*100:.1f}%", "size": "lg", "color": get_color_class(totalHT), "weight": "bold", "align": "center"},
        {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#ffffff1a",
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
                    "width": f"{min(100, round(totalHT * 100))}%"
                }
            ]
        },
        {"type": "separator", "color": "#ffffff26", "margin": "md"}
    ]
    
    # Table 1: Revenue details (Top 6 to keep JSON small)
    body_contents.append({"type": "text", "text": "📊 CHI TIẾT DOANH THU LŨY KẾ", "size": "xxs", "color": "#38bdf8", "weight": "bold", "margin": "md"})
    headers = ["STT", "Ngành hàng", "SL", "DTQĐ", "Target", "%HT"]
    weights = [1, 3, 1, 2, 2, 2]
    aligns = ["start", "start", "center", "center", "center", "end"]
    body_contents.append(make_table_header(headers, weights, aligns))
    body_contents.append({"type": "separator", "color": "#ffffff1a", "margin": "xs"})
    
    for idx, b in enumerate(parsed_bi[:6]):
        vals = [idx+1, b["name"][:12], b["sl"], f"{b['dt']:.1f}", f"{b['tg']:.1f}", f"{b['ht']*100:.0f}%"]
        colors = ["#94a3b8", "#ffffff", "#ffffff", "#38bdf8", "#ffffff", get_color_class(b["ht"])]
        body_contents.append(make_table_row(vals, weights, aligns, colors))
    
    # Row Total
    tot_sl = sum(x["sl"] for x in parsed_bi)
    tot_vals = ["⭐", "TỔNG CỘNG", tot_sl, f"{tDT:.1f}", f"{tTG:.1f}", f"{totalHT*100:.1f}%"]
    tot_colors = ["#fbbf24", "#fbbf24", "#fbbf24", "#38bdf8", "#fbbf24", get_color_class(totalHT)]
    body_contents.append({"type": "separator", "color": "#ffffff26", "margin": "xs"})
    body_contents.append(make_table_row(tot_vals, weights, aligns, tot_colors, bold=True))
    
    # Table 2: Compete Items (Completed)
    if td_done:
        body_contents.append({"type": "separator", "color": "#ffffff26", "margin": "md"})
        body_contents.append({"type": "text", "text": "🏆 THI ĐUA DỰ KIẾN ĐẠT (DKHT ≥ 100%)", "size": "xxs", "color": "#10b981", "weight": "bold", "margin": "md"})
        headers_td = ["STT", "Ngành hàng thi đua", "Đạt", "Target", "%DK"]
        weights_td = [1, 4, 2, 2, 2]
        aligns_td = ["start", "start", "center", "center", "end"]
        body_contents.append(make_table_header(headers_td, weights_td, aligns_td))
        body_contents.append({"type": "separator", "color": "#ffffff1a", "margin": "xs"})
        for idx, t in enumerate(td_done[:5]):
            vals = [idx+1, t["name"][:14], f"{t['actual']:.1f} {t['unit']}", f"{t['target']:.1f}", f"{t['ht_dk']*100:.0f}%"]
            colors = ["#94a3b8", "#ffffff", "#38bdf8", "#ffffff", "#10b981"]
            body_contents.append(make_table_row(vals, weights_td, aligns_td, colors))
            
    # Table 3: Compete Items (Pending)
    if td_pending:
        body_contents.append({"type": "separator", "color": "#ffffff26", "margin": "md"})
        body_contents.append({"type": "text", "text": "🎯 THI ĐUA CHƯA ĐẠT (DKHT < 100%)", "size": "xxs", "color": "#ef4444", "weight": "bold", "margin": "md"})
        headers_td2 = ["STT", "Ngành hàng thi đua", "Đạt", "Thiếu", "%DK"]
        body_contents.append(make_table_header(headers_td2, weights_td, aligns_td))
        body_contents.append({"type": "separator", "color": "#ffffff1a", "margin": "xs"})
        for idx, t in enumerate(td_pending[:6]):
            vals = [idx+1, t["name"][:14], f"{t['actual']:.1f}", f"{t['con_lai']:.1f}", f"{t['ht_dk']*100:.0f}%"]
            colors = ["#94a3b8", "#ffffff", "#fbbf24", "#ef4444", get_color_class(t["ht_dk"])]
            body_contents.append(make_table_row(vals, weights_td, aligns_td, colors))
            
    flex_bubble = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#1e293b",
            "paddingAll": "md",
            "contents": [
                {"type": "text", "text": "📊 BÁO CÁO LŨY KẾ CỤM SAVICO", "weight": "bold", "size": "sm", "color": "#ffffff", "align": "center"},
                {"type": "text", "text": f"🕒 Cập nhật: {now_str}", "size": "xxs", "color": "#94a3b8", "align": "center", "margin": "xs"}
            ]
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#0f172a",
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
    # Loop config staff to list everyone
    for staff_upper, clean_name in active_staff_names.items():
        actual = 0.0
        # Find in actuals
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
            "contents": [
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "contents": [
                        {"type": "text", "text": "Doanh Thu Cửa Hàng", "size": "xxs", "color": "#94a3b8", "align": "center"},
                        {"type": "text", "text": f"{tDT:.1f} M", "size": "sm", "color": "#38bdf8", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "contents": [
                        {"type": "text", "text": "Mục Tiêu Chung", "size": "xxs", "color": "#94a3b8", "align": "center"},
                        {"type": "text", "text": f"{total_target:.1f} M", "size": "sm", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                }
            ]
        },
        {
            "type": "box",
            "layout": "horizontal",
            "margin": "sm",
            "contents": [
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "contents": [
                        {"type": "text", "text": "Số Nhân Viên", "size": "xxs", "color": "#94a3b8", "align": "center"},
                        {"type": "text", "text": f"{len(emp_list)} Nhân Sự", "size": "sm", "color": "#a855f7", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "contents": [
                        {"type": "text", "text": "% Đạt Toàn Cửa Hàng", "size": "xxs", "color": "#94a3b8", "align": "center"},
                        {"type": "text", "text": f"{totalHT*100:.1f}%", "size": "sm", "color": get_color_class(totalHT), "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                }
            ]
        },
        {"type": "separator", "color": "#ffffff26", "margin": "md"},
        {"type": "text", "text": "👥 CHI TIẾT XẾP HẠNG DOANH THU NHÂN VIÊN", "size": "xxs", "color": "#38bdf8", "weight": "bold", "margin": "md"},
    ]
    
    headers = ["STT", "Nhân viên", "Đạt được", "Chỉ tiêu", "%HT"]
    weights = [1, 4, 3, 3, 2]
    aligns = ["start", "start", "center", "center", "end"]
    body_contents.append(make_table_header(headers, weights, aligns))
    body_contents.append({"type": "separator", "color": "#ffffff1a", "margin": "xs"})
    
    for idx, e in enumerate(emp_list):
        crown = "🥇" if idx == 0 else ("🥈" if idx == 1 else ("🥉" if idx == 2 else idx+1))
        vals = [crown, e["name"], f"{e['actual']:.1f} M", f"{e['target']:.1f} M", f"{e['pct']*100:.0f}%"]
        colors = ["#fbbf24" if idx < 3 else "#94a3b8", "#ffffff", "#38bdf8", "#ffffff", get_color_class(e["pct"])]
        body_contents.append(make_table_row(vals, weights, aligns, colors, bold=(idx < 3)))
        
    flex_bubble = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#1e293b",
            "paddingAll": "md",
            "contents": [
                {"type": "text", "text": "🏆 BÁO CÁO NHÂN VIÊN CỤM SAVICO", "weight": "bold", "size": "sm", "color": "#ffffff", "align": "center"},
                {"type": "text", "text": f"🕒 Cập nhật: {now_str}", "size": "xxs", "color": "#94a3b8", "align": "center", "margin": "xs"}
            ]
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#0f172a",
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
                "name": str(nganh).replace("NNH ", "").strip(),
                "sl": int(sl),
                "dt": dtqd,
                "tg": targetDay,
                "ht": dtqd / targetDay if targetDay > 0 else 0.0
            })
            
    parsed_rt_bi.sort(key=lambda x: x["dt"], reverse=True)
    htChung = rt_total / target_today if target_today > 0 else 0.0
    thieuDTRT = max(0.0, target_today - rt_total)
    
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
            "name": str(nganh).replace("NNH ", "").strip(),
            "actual": actual,
            "con_lai": con_lai,
            "target": lk_info["mt_ngay"],
            "ht": ht_target,
            "unit": "TR" if lk_info["is_dt"] else "SP"
        })
        
    td_done = [x for x in parsed_td if x["ht"] >= 1.0]
    td_pending = [x for x in parsed_td if x["ht"] < 1.0]
    td_done.sort(key=lambda x: x["ht"], reverse=True)
    td_pending.sort(key=lambda x: x["ht"], reverse=True)
    
    # 4. Build Flex structure
    body_contents = [
        # Progress bars
        {"type": "text", "text": "🕒 TIẾN ĐỘ THỜI GIAN", "size": "xxs", "color": "#94a3b8", "weight": "bold"},
        {
            "type": "box",
            "layout": "horizontal",
            "margin": "xs",
            "contents": [
                {"type": "text", "text": f"{time_ratio*100:.1f}%", "size": "xs", "color": "#38bdf8", "weight": "bold", "flex": 2},
                {"type": "text", "text": f"(Còn {(13.0 - elapsed_hours):.1f}h)", "size": "xxs", "color": "#94a3b8", "align": "end", "flex": 8}
            ]
        },
        {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#ffffff1a",
            "height": "6px",
            "cornerRadius": "md",
            "margin": "xs",
            "contents": [
                {
                    "type": "box",
                    "layout": "vertical",
                    "backgroundColor": "#38bdf8",
                    "height": "6px",
                    "cornerRadius": "md",
                    "width": f"{min(100, round(time_ratio * 100))}%"
                }
            ]
        },
        {"type": "text", "text": "🎯 TIẾN ĐỘ DOANH THU NGÀY", "size": "xxs", "color": "#94a3b8", "weight": "bold", "margin": "sm"},
        {
            "type": "box",
            "layout": "horizontal",
            "margin": "xs",
            "contents": [
                {"type": "text", "text": f"{htChung*100:.1f}%", "size": "xs", "color": get_color_class(htChung), "weight": "bold", "flex": 2},
                {"type": "text", "text": f"(Thiếu {thieuDTRT:.1f} M)", "size": "xxs", "color": "#ef4444", "align": "end", "flex": 8}
            ]
        },
        {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#ffffff1a",
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
                    "width": f"{min(100, round(htChung * 100))}%"
                }
            ]
        },
        {"type": "separator", "color": "#ffffff26", "margin": "md"},
        
        # KPI boxes
        {
            "type": "box",
            "layout": "horizontal",
            "margin": "md",
            "contents": [
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "contents": [
                        {"type": "text", "text": "DT Thực Hiện", "size": "xxs", "color": "#94a3b8", "align": "center"},
                        {"type": "text", "text": f"{rt_total:.1f} M", "size": "sm", "color": "#38bdf8", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "contents": [
                        {"type": "text", "text": "Mục Tiêu Ngày", "size": "xxs", "color": "#94a3b8", "align": "center"},
                        {"type": "text", "text": f"{target_today:.1f} M", "size": "sm", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                }
            ]
        },
        {
            "type": "box",
            "layout": "horizontal",
            "margin": "sm",
            "contents": [
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "contents": [
                        {"type": "text", "text": "Trả Góp", "size": "xxs", "color": "#94a3b8", "align": "center"},
                        {"type": "text", "text": f"{rt_tTC:.1f} M", "size": "xs", "color": "#a855f7", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "contents": [
                        {"type": "text", "text": "Thi Đua Đạt", "size": "xxs", "color": "#94a3b8", "align": "center"},
                        {"type": "text", "text": f"{rt_cntVD}/{len(parsed_td)} Nhóm", "size": "xs", "color": "#10b981", "weight": "bold", "align": "center", "margin": "xs"}
                    ]
                }
            ]
        },
        {"type": "separator", "color": "#ffffff26", "margin": "md"}
    ]
    
    # Table 1: Revenue details
    body_contents.append({"type": "text", "text": "⚡ CHI TIẾT DOANH THU HÔM NAY", "size": "xxs", "color": "#38bdf8", "weight": "bold", "margin": "md"})
    headers = ["STT", "Ngành hàng", "SL", "DTQĐ", "Target", "%HT"]
    weights = [1, 3, 1, 2, 2, 2]
    aligns = ["start", "start", "center", "center", "center", "end"]
    body_contents.append(make_table_header(headers, weights, aligns))
    body_contents.append({"type": "separator", "color": "#ffffff1a", "margin": "xs"})
    
    for idx, b in enumerate(parsed_rt_bi[:6]):
        vals = [idx+1, b["name"][:12], b["sl"], f"{b['dt']:.1f}", f"{b['tg']:.1f}", f"{b['ht']*100:.0f}%"]
        colors = ["#94a3b8", "#ffffff", "#ffffff", "#38bdf8", "#ffffff", get_color_class(b["ht"])]
        body_contents.append(make_table_row(vals, weights, aligns, colors))
        
    # Total row
    tot_sl = sum(x["sl"] for x in parsed_rt_bi)
    tot_vals = ["⭐", "TỔNG CỘNG", tot_sl, f"{rt_total:.1f}", f"{rt_tTarget:.1f}", f"{totalHTCol2*100:.1f}%" if (totalHTCol2:= (rt_total/rt_tTarget if rt_tTarget>0 else 0.0)) else "0%"]
    tot_colors = ["#fbbf24", "#fbbf24", "#fbbf24", "#38bdf8", "#fbbf24", get_color_class(totalHTCol2)]
    body_contents.append({"type": "separator", "color": "#ffffff26", "margin": "xs"})
    body_contents.append(make_table_row(tot_vals, weights, aligns, tot_colors, bold=True))
    
    # Table 2: Compete Items (Completed)
    if td_done:
        body_contents.append({"type": "separator", "color": "#ffffff26", "margin": "md"})
        body_contents.append({"type": "text", "text": "🏆 THI ĐUA ĐÃ ĐẠT CHỈ TIÊU NGÀY", "size": "xxs", "color": "#10b981", "weight": "bold", "margin": "md"})
        headers_td = ["STT", "Ngành hàng thi đua", "Đạt", "Mục tiêu", "%HT"]
        weights_td = [1, 4, 2, 2, 2]
        aligns_td = ["start", "start", "center", "center", "end"]
        body_contents.append(make_table_header(headers_td, weights_td, aligns_td))
        body_contents.append({"type": "separator", "color": "#ffffff1a", "margin": "xs"})
        for idx, t in enumerate(td_done[:5]):
            vals = [idx+1, t["name"][:14], f"{t['actual']:.1f} {t['unit']}", f"{t['target']:.1f}", f"{t['ht']*100:.0f}%"]
            colors = ["#94a3b8", "#ffffff", "#38bdf8", "#ffffff", "#10b981"]
            body_contents.append(make_table_row(vals, weights_td, aligns_td, colors))
            
    # Table 3: Compete Items (Pending)
    if td_pending:
        body_contents.append({"type": "separator", "color": "#ffffff26", "margin": "md"})
        body_contents.append({"type": "text", "text": "🎯 THI ĐUA CHƯA ĐẠT CHỈ TIÊU NGÀY", "size": "xxs", "color": "#ef4444", "weight": "bold", "margin": "md"})
        headers_td2 = ["STT", "Ngành hàng thi đua", "Đạt", "Còn thiếu", "%HT"]
        body_contents.append(make_table_header(headers_td2, weights_td, aligns_td))
        body_contents.append({"type": "separator", "color": "#ffffff1a", "margin": "xs"})
        for idx, t in enumerate(td_pending[:6]):
            vals = [idx+1, t["name"][:14], f"{t['actual']:.1f}", f"{t['con_lai']:.1f}", f"{t['ht']*100:.0f}%"]
            colors = ["#94a3b8", "#ffffff", "#fbbf24", "#ef4444", get_color_class(t["ht"])]
            body_contents.append(make_table_row(vals, weights_td, aligns_td, colors))
            
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
            "backgroundColor": "#0f172a",
            "paddingAll": "md",
            "contents": body_contents
        }
    }
    return flex_bubble
