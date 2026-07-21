import pytz
from datetime import datetime
from dmx_data_provider import get_dashboard_data

def parse_number(val):
    if not val:
        return 0.0
    try:
        if isinstance(val, (int, float)):
            return float(val)
        val_str = str(val).replace(",", "").replace("%", "").strip()
        return float(val_str)
    except:
        return 0.0

def get_key_val(row, *possible_keys, default=None):
    if not row or not isinstance(row, dict):
        return default
    # Clean possible keys
    norm_keys = [k.strip().lower() for k in possible_keys]
    
    # Try exact matches
    for pk in possible_keys:
        if pk in row:
            return row[pk]
            
    # Try case/space insensitive matches
    for rk, rv in row.items():
        if rk.strip().lower() in norm_keys:
            return rv
            
    return default

def build_luyke_flex():
    data = get_dashboard_data("Config_ThiDua,Data_BI,Data_ThiDua,Data_NV_ThiDua")
    config_rows = data.get("Config_ThiDua", [])
    bi_rows = data.get("Data_BI", [])
    
    target_thang = 1500.0
    if config_rows and len(config_rows) > 0:
        target_thang = parse_number(get_key_val(config_rows[0], "Target Tháng", "Target", default=1500.0))

    # Calculate cumulative DT (Latest day from config or fallback to sum of BI actuals)
    dt_thang_nay = 0.0
    latest_day_dt = 0.0
    for r in config_rows:
        day_val = parse_number(get_key_val(r, "Ngày", "ngày", default=0))
        if day_val > 0:
            val = parse_number(get_key_val(r, "DT tháng này", "DT lũy kế", default=0.0))
            if val > 0:
                latest_day_dt = val
                
    if latest_day_dt > 0:
        dt_thang_nay = latest_day_dt
    else:
        for b in bi_rows:
            dt_thang_nay += parse_number(get_key_val(b, "Doanh thu", "doanh thu quy đổi", "revenue_All", default=0.0))

    percent_ht = (dt_thang_nay / target_thang) if target_thang > 0 else 0
    percent_str = f"{percent_ht * 100:.1f}%"
    percent_color = "#34d399" if percent_ht >= 1.0 else ("#fbbf24" if percent_ht >= 0.8 else "#ef4444")
    
    tz = pytz.timezone('Asia/Ho_Chi_Minh')
    now_str = datetime.now(tz).strftime("%H:%M - %d/%m/%Y")
    
    # Process categories list
    cat_items = []
    # Try to load categories from config list
    for r in config_rows:
        cat_name = get_key_val(r, "Ngành hàng", "nhóm ngành hàng", default=None)
        if cat_name and isinstance(cat_name, str) and cat_name.strip():
            actual = 0.0
            for b in bi_rows:
                bi_cat = get_key_val(b, "maingroupname", "main group name", "nhóm ngành hàng", "nhóm ngành hàng chính", default="")
                if str(bi_cat).strip().lower() == str(cat_name).strip().lower():
                    actual = parse_number(get_key_val(b, "Doanh thu", "doanh thu quy đổi", default=0.0))
                    break
            cat_items.append({
                "name": str(cat_name).strip(),
                "actual": actual
            })
            
    # Fallback to direct BI rows if no categories mapped from config
    if not cat_items:
        for b in bi_rows:
            bi_cat = get_key_val(b, "maingroupname", "main group name", "nhóm ngành hàng", "nhóm ngành hàng chính", default=None)
            actual = parse_number(get_key_val(b, "Doanh thu", "doanh thu quy đổi", default=0.0))
            if bi_cat and actual > 0:
                cat_items.append({
                    "name": str(bi_cat).strip(),
                    "actual": actual
                })

    seen = set()
    unique_cats = []
    for c in cat_items:
        if c["name"] not in seen:
            seen.add(c["name"])
            unique_cats.append(c)
    unique_cats.sort(key=lambda x: x["actual"], reverse=True)
    
    cat_boxes = []
    for c in unique_cats[:8]:
        cat_boxes.append({
            "type": "box",
            "layout": "horizontal",
            "margin": "sm",
            "contents": [
                {"type": "text", "text": f"📁 {c['name']}", "size": "xs", "color": "#ffffff", "flex": 6},
                {"type": "text", "text": f"{c['actual']:.1f} M", "size": "xs", "color": "#38bdf8", "align": "end", "flex": 4, "weight": "bold"}
            ]
        })
        cat_boxes.append({"type": "separator", "color": "#ffffff1a", "margin": "sm"})
        
    flex_bubble = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#1e293b",
            "paddingAll": "lg",
            "contents": [
                {"type": "text", "text": "📊 BÁO CÁO LŨY KẾ CỤM SAVICO", "weight": "bold", "size": "md", "color": "#ffffff", "align": "center"},
                {"type": "text", "text": f"🕒 Cập nhật: {now_str}", "size": "xxs", "color": "#94a3b8", "align": "center", "margin": "sm"}
            ]
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#0f172a",
            "paddingAll": "lg",
            "contents": [
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
                                {"type": "text", "text": "Lũy Kế Đạt", "size": "xs", "color": "#94a3b8", "align": "center"},
                                {"type": "text", "text": f"{dt_thang_nay:.1f} M", "size": "lg", "color": "#38bdf8", "weight": "bold", "align": "center", "margin": "xs"}
                            ]
                        },
                        {
                            "type": "box",
                            "layout": "vertical",
                            "flex": 1,
                            "contents": [
                                {"type": "text", "text": "Mục Tiêu Tháng", "size": "xs", "color": "#94a3b8", "align": "center"},
                                {"type": "text", "text": f"{target_thang:.1f} M", "size": "lg", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                            ]
                        }
                    ]
                },
                {"type": "separator", "color": "#ffffff26", "margin": "md"},
                {"type": "text", "text": "% HOÀN THÀNH CHỈ TIÊU", "size": "xs", "color": "#ffffff", "align": "center", "margin": "md", "weight": "bold"},
                {"type": "text", "text": percent_str, "size": "xxl", "color": percent_color, "weight": "bold", "align": "center", "margin": "xs"},
                {
                    "type": "box",
                    "layout": "vertical",
                    "backgroundColor": "#ffffff1a",
                    "height": "8px",
                    "cornerRadius": "md",
                    "margin": "md",
                    "contents": [
                        {
                            "type": "box",
                            "layout": "vertical",
                            "backgroundColor": percent_color,
                            "height": "8px",
                            "cornerRadius": "md",
                            "width": f"{min(100, round(percent_ht * 100))}%"
                        }
                    ]
                },
                {"type": "separator", "color": "#ffffff26", "margin": "lg"},
                {"type": "text", "text": "🏆 CÁC PHÂN NHÓM NGÀNH HÀNG", "size": "xs", "color": "#fbbf24", "weight": "bold", "margin": "md"},
                *cat_boxes
            ]
        }
    }
    return flex_bubble

def build_nhanvien_flex():
    data = get_dashboard_data("Config_ThiDua,Data_NV_BI,Data_NV_ThiDua,Data_BI,Data_ThiDua,Data_Realtime_NV")
    config_rows = data.get("Config_ThiDua", [])
    nv_rows = data.get("Data_Realtime_NV", [])
    if not nv_rows:
        nv_rows = data.get("Data_NV_BI", [])
        
    tz = pytz.timezone('Asia/Ho_Chi_Minh')
    now_str = datetime.now(tz).strftime("%H:%M - %d/%m/%Y")
    
    emp_targets = {}
    total_target = 1500.0
    if config_rows and len(config_rows) > 0:
        total_target = parse_number(get_key_val(config_rows[0], "Target Tháng", "Target", default=1500.0))
        for r in config_rows:
            emp_name = get_key_val(r, "Họ và tên", "tên nhân viên", default=None)
            pct = parse_number(get_key_val(r, "% chia", "tỷ lệ %", default=0.0))
            if emp_name and pct > 0:
                emp_targets[str(emp_name).strip()] = (pct / 100.0) * total_target

    emp_list = []
    seen_names = set()
    for r in nv_rows:
        name = get_key_val(r, "staffUserName", "tên nv", "Họ và tên", default=None)
        if not name or str(name).strip() in seen_names: 
            continue
        seen_names.add(str(name).strip())
        actual = parse_number(get_key_val(r, "Doanh thu", "doanh thu quy đổi", "Value_Compe", default=0.0))
        target = emp_targets.get(str(name).strip(), 0.0)
        pct_ht = (actual / target) if target > 0 else 0.0
        emp_list.append({
            "name": str(name).strip(),
            "actual": actual,
            "target": target,
            "pct": pct_ht
        })
        
    emp_list.sort(key=lambda x: x["actual"], reverse=True)
    
    leaderboard_boxes = []
    for idx, e in enumerate(emp_list[:8]):
        crown = "🥇" if idx == 0 else ("🥈" if idx == 1 else ("🥉" if idx == 2 else "👤"))
        pct_str = f"{e['pct'] * 100:.1f}%"
        leaderboard_boxes.append({
            "type": "box",
            "layout": "horizontal",
            "margin": "sm",
            "contents": [
                {"type": "text", "text": f"{crown} {e['name']}", "size": "xs", "color": "#ffffff", "flex": 5, "weight": "bold" if idx < 3 else "regular"},
                {"type": "text", "text": f"{e['actual']:.1f} M", "size": "xs", "color": "#38bdf8", "align": "center", "flex": 3},
                {"type": "text", "text": pct_str, "size": "xs", "color": "#34d399" if e['pct'] >= 1.0 else "#fbbf24", "align": "end", "flex": 2, "weight": "bold"}
            ]
        })
        leaderboard_boxes.append({"type": "separator", "color": "#ffffff1a", "margin": "sm"})
        
    flex_bubble = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#1e293b",
            "paddingAll": "lg",
            "contents": [
                {"type": "text", "text": "🏆 XẾP HẠNG DOANH THU NHÂN VIÊN", "weight": "bold", "size": "md", "color": "#ffffff", "align": "center"},
                {"type": "text", "text": f"🕒 Cập nhật: {now_str}", "size": "xxs", "color": "#94a3b8", "align": "center", "margin": "sm"}
            ]
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#0f172a",
            "paddingAll": "lg",
            "contents": [
                {
                    "type": "box",
                    "layout": "horizontal",
                    "margin": "md",
                    "contents": [
                        {"type": "text", "text": "Nhân Viên", "size": "xs", "color": "#94a3b8", "flex": 5, "weight": "bold"},
                        {"type": "text", "text": "Đạt Được", "size": "xs", "color": "#94a3b8", "flex": 3, "align": "center", "weight": "bold"},
                        {"type": "text", "text": "% Đạt", "size": "xs", "color": "#94a3b8", "flex": 2, "align": "end", "weight": "bold"}
                    ]
                },
                {"type": "separator", "color": "#ffffff33", "margin": "sm"},
                *leaderboard_boxes
            ]
        }
    }
    return flex_bubble

def build_realtime_flex():
    data = get_dashboard_data("Data_BI,Data_ThiDua,Config_ThiDua,Data_Realtime_BI,Data_Realtime_ThiDua,Data_Realtime_NV")
    config_rows = data.get("Config_ThiDua", [])
    rt_rows = data.get("Data_Realtime_BI", [])
    
    tz = pytz.timezone('Asia/Ho_Chi_Minh')
    now = datetime.now(tz)
    now_str = now.strftime("%H:%M - %d/%m/%Y")
    
    day_num = now.day
    target_today = 0.0
    if config_rows and len(config_rows) >= day_num:
        target_today = parse_number(get_key_val(config_rows[day_num - 1], "Mục tiêu", "mục tiêu ngày", default=0.0))
    if target_today <= 0 and config_rows:
        target_thang = parse_number(get_key_val(config_rows[0], "Target Tháng", "Target", default=1500.0))
        target_today = target_thang / 31.0
        
    rt_total = 0.0
    for r in rt_rows:
        rt_total += parse_number(get_key_val(r, "Doanh thu", "doanh thu quy đổi", default=0.0))
        
    percent_ht = (rt_total / target_today) if target_today > 0 else 0
    percent_str = f"{percent_ht * 100:.1f}%"
    percent_color = "#34d399" if percent_ht >= 1.0 else ("#fbbf24" if percent_ht >= 0.8 else "#ef4444")
    
    cat_items = []
    seen_cats = set()
    for r in rt_rows:
        cat_name = get_key_val(r, "Nhóm Ngành Hàng", "nhóm ngành hàng", "Ngành hàng", "NhomNganhHang", default=None)
        if cat_name and str(cat_name).strip() and str(cat_name).strip() not in seen_cats:
            seen_cats.add(str(cat_name).strip())
            cat_items.append({
                "name": str(cat_name).strip(),
                "actual": parse_number(get_key_val(r, "Doanh thu", "doanh thu quy đổi", default=0.0))
            })
    cat_items.sort(key=lambda x: x["actual"], reverse=True)
    
    cat_boxes = []
    for c in cat_items[:6]:
        cat_boxes.append({
            "type": "box",
            "layout": "horizontal",
            "margin": "sm",
            "contents": [
                {"type": "text", "text": f"⚡ {c['name']}", "size": "xs", "color": "#ffffff", "flex": 6},
                {"type": "text", "text": f"{c['actual']:.1f} M", "size": "xs", "color": "#38bdf8", "align": "end", "flex": 4, "weight": "bold"}
            ]
        })
        cat_boxes.append({"type": "separator", "color": "#ffffff1a", "margin": "sm"})
        
    flex_bubble = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#0284c7",
            "paddingAll": "lg",
            "contents": [
                {"type": "text", "text": "⚡ BÁO CÁO REALTIME HÔM NAY", "weight": "bold", "size": "md", "color": "#ffffff", "align": "center"},
                {"type": "text", "text": f"🕒 Cập nhật: {now_str}", "size": "xxs", "color": "#e0f2fe", "align": "center", "margin": "sm"}
            ]
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#0f172a",
            "paddingAll": "lg",
            "contents": [
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
                                {"type": "text", "text": "Doanh Thu Hôm Nay", "size": "xs", "color": "#94a3b8", "align": "center"},
                                {"type": "text", "text": f"{rt_total:.1f} M", "size": "lg", "color": "#38bdf8", "weight": "bold", "align": "center", "margin": "xs"}
                            ]
                        },
                        {
                            "type": "box",
                            "layout": "vertical",
                            "flex": 1,
                            "contents": [
                                {"type": "text", "text": "Chỉ Tiêu Hôm Nay", "size": "xs", "color": "#94a3b8", "align": "center"},
                                {"type": "text", "text": f"{target_today:.1f} M", "size": "lg", "color": "#ffffff", "weight": "bold", "align": "center", "margin": "xs"}
                            ]
                        }
                    ]
                },
                {"type": "separator", "color": "#ffffff26", "margin": "md"},
                {"type": "text", "text": "% HOÀN THÀNH NGÀY", "size": "xs", "color": "#ffffff", "align": "center", "margin": "md", "weight": "bold"},
                {"type": "text", "text": percent_str, "size": "xxl", "color": percent_color, "weight": "bold", "align": "center", "margin": "xs"},
                {
                    "type": "box",
                    "layout": "vertical",
                    "backgroundColor": "#ffffff1a",
                    "height": "8px",
                    "cornerRadius": "md",
                    "margin": "md",
                    "contents": [
                        {
                            "type": "box",
                            "layout": "vertical",
                            "backgroundColor": percent_color,
                            "height": "8px",
                            "cornerRadius": "md",
                            "width": f"{min(100, round(percent_ht * 100))}%"
                        }
                    ]
                },
                {"type": "separator", "color": "#ffffff26", "margin": "lg"},
                {"type": "text", "text": "🎯 NGÀNH HÀNG PHÁT SINH SỐ HÔM NAY", "size": "xs", "color": "#fbbf24", "weight": "bold", "margin": "md"},
                *cat_boxes
            ]
        }
    }
    return flex_bubble
