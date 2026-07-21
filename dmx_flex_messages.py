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

def build_luyke_flex():
    # Load cumulative and targets
    data = get_dashboard_data("Config_ThiDua,Data_BI,Data_ThiDua,Data_NV_ThiDua")
    config_rows = data.get("Config_ThiDua", [])
    bi_rows = data.get("Data_BI", [])
    
    target_thang = 1500.0
    if config_rows and len(config_rows) > 0:
        target_thang = parse_number(config_rows[0].get("Target Tháng", 1500))

    dt_thang_nay = 0.0
    for r in bi_rows:
        dt_thang_nay += parse_number(r.get("DT lũy kế", 0))

    percent_ht = (dt_thang_nay / target_thang) if target_thang > 0 else 0
    percent_str = f"{percent_ht * 100:.1f}%"
    percent_color = "#34d399" if percent_ht >= 1.0 else ("#fbbf24" if percent_ht >= 0.8 else "#ef4444")
    
    tz = pytz.timezone('Asia/Ho_Chi_Minh')
    now_str = datetime.now(tz).strftime("%H:%M - %d/%m/%Y")
    
    # Process categories list
    cat_items = []
    for r in config_rows:
        cat_name = r.get("Ngành hàng")
        if cat_name and cat_name.strip():
            actual = 0.0
            for b in bi_rows:
                if b.get("Ngành hàng", "").strip() == cat_name.strip():
                    actual = parse_number(b.get("DT lũy kế", 0))
                    break
            cat_items.append({
                "name": cat_name.strip(),
                "actual": actual
            })
    
    # Filter duplicate category names and sort
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
        cat_boxes.append({"type": "separator", "color": "rgba(255, 255, 255, 0.1)", "margin": "sm"})
        
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
                {"type": "separator", "color": "rgba(255, 255, 255, 0.15)", "margin": "md"},
                {"type": "text", "text": "% HOÀN THÀNH CHỈ TIÊU", "size": "xs", "color": "#ffffff", "align": "center", "margin": "md", "weight": "bold"},
                {"type": "text", "text": percent_str, "size": "xxl", "color": percent_color, "weight": "bold", "align": "center", "margin": "xs"},
                {
                    "type": "box",
                    "layout": "vertical",
                    "backgroundColor": "rgba(255, 255, 255, 0.1)",
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
                {"type": "separator", "color": "rgba(255, 255, 255, 0.15)", "margin": "lg"},
                {"type": "text", "text": "🏆 CÁC PHÂN NHÓM NGÀNH HÀNG", "size": "xs", "color": "#fbbf24", "weight": "bold", "margin": "md"},
                *cat_boxes
            ]
        }
    }
    return flex_bubble

def build_nhanvien_flex():
    # Load employee stats
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
        total_target = parse_number(config_rows[0].get("Target Tháng", 1500))
        for r in config_rows:
            emp_name = r.get("Họ và tên")
            pct = parse_number(r.get("% chia"))
            if emp_name and pct > 0:
                emp_targets[emp_name.strip()] = (pct / 100.0) * total_target

    emp_list = []
    seen_names = set()
    for r in nv_rows:
        name = r.get("tên nv") or r.get("Họ và tên")
        if not name or name.strip() in seen_names: 
            continue
        seen_names.add(name.strip())
        actual = parse_number(r.get("doanh thu") or r.get("DT thực hiện"))
        target = emp_targets.get(name.strip(), 0.0)
        pct_ht = (actual / target) if target > 0 else 0.0
        emp_list.append({
            "name": name.strip(),
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
        leaderboard_boxes.append({"type": "separator", "color": "rgba(255, 255, 255, 0.1)", "margin": "sm"})
        
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
                {"type": "separator", "color": "rgba(255, 255, 255, 0.2)", "margin": "sm"},
                *leaderboard_boxes
            ]
        }
    }
    return flex_bubble

def build_realtime_flex():
    # Load realtime stats
    data = get_dashboard_data("Data_BI,Data_ThiDua,Config_ThiDua,Data_Realtime_BI,Data_Realtime_ThiDua,Data_Realtime_NV")
    config_rows = data.get("Config_ThiDua", [])
    rt_rows = data.get("Data_Realtime_BI", [])
    
    tz = pytz.timezone('Asia/Ho_Chi_Minh')
    now = datetime.now(tz)
    now_str = now.strftime("%H:%M - %d/%m/%Y")
    
    day_num = now.day
    target_today = 0.0
    if config_rows and len(config_rows) >= day_num:
        target_today = parse_number(config_rows[day_num - 1].get("Mục tiêu", 0))
    if target_today <= 0 and config_rows:
        target_thang = parse_number(config_rows[0].get("Target Tháng", 1500))
        target_today = target_thang / 31.0
        
    rt_total = 0.0
    for r in rt_rows:
        rt_total += parse_number(r.get("Doanh thu", 0))
        
    percent_ht = (rt_total / target_today) if target_today > 0 else 0
    percent_str = f"{percent_ht * 100:.1f}%"
    percent_color = "#34d399" if percent_ht >= 1.0 else ("#fbbf24" if percent_ht >= 0.8 else "#ef4444")
    
    cat_items = []
    seen_cats = set()
    for r in rt_rows:
        cat_name = r.get("Ngành hàng") or r.get("NhomNganhHang")
        if cat_name and cat_name.strip() and cat_name.strip() not in seen_cats:
            seen_cats.add(cat_name.strip())
            cat_items.append({
                "name": cat_name.strip(),
                "actual": parse_number(r.get("Doanh thu", 0))
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
        cat_boxes.append({"type": "separator", "color": "rgba(255, 255, 255, 0.1)", "margin": "sm"})
        
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
                {"type": "separator", "color": "rgba(255, 255, 255, 0.15)", "margin": "md"},
                {"type": "text", "text": "% HOÀN THÀNH NGÀY", "size": "xs", "color": "#ffffff", "align": "center", "margin": "md", "weight": "bold"},
                {"type": "text", "text": percent_str, "size": "xxl", "color": percent_color, "weight": "bold", "align": "center", "margin": "xs"},
                {
                    "type": "box",
                    "layout": "vertical",
                    "backgroundColor": "rgba(255, 255, 255, 0.1)",
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
                {"type": "separator", "color": "rgba(255, 255, 255, 0.15)", "margin": "lg"},
                {"type": "text", "text": "🎯 NGÀNH HÀNG PHÁT SINH SỐ HÔM NAY", "size": "xs", "color": "#fbbf24", "weight": "bold", "margin": "md"},
                *cat_boxes
            ]
        }
    }
    return flex_bubble
