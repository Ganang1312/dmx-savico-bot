import json
import pytz
import re
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

def parse_is_sl(val):
    if val is None or val == '':
        return None
    if isinstance(val, bool):
        return val
    s = str(val).strip().lower()
    if s in ['true', '1', 'sl', 'số lượng']:
        return True
    if s in ['false', '0', 'dt', 'doanh thu']:
        return False
    return None

def find_td_config(name, config_map):
    """
    So khớp ngành hàng thi đua đồng bộ 100% với hàm findTDConfig trong baocao_luyke.html & baocao_realtime.html:
    1. So khớp chính xác tên
    2. So khớp bỏ tiền tố 'nnh '
    3. So khớp thêm tiền tố 'nnh '
    4. So khớp bỏ hậu tố tháng (ví dụ: 'Đồng hồ tháng 9' vs 'Đồng hồ')
    """
    if not name or not config_map:
        return {}
    clean = str(name).lower().strip()
    clean_no_prefix = re.sub(r'^nnh\s+', '', clean)
    if clean in config_map:
        return config_map[clean]
    if clean_no_prefix in config_map:
        return config_map[clean_no_prefix]
    if ('nnh ' + clean_no_prefix) in config_map:
        return config_map['nnh ' + clean_no_prefix]
    for k in config_map:
        if re.sub(r'^nnh\s+', '', k) == clean_no_prefix:
            return config_map[k]
    clean_no_month = re.sub(r'\s+tháng\s+\d+', '', clean_no_prefix).strip()
    for k in config_map:
        k_clean_no_month = re.sub(r'\s+tháng\s+\d+', '', re.sub(r'^nnh\s+', '', k)).strip()
        if k_clean_no_month == clean_no_month and len(k_clean_no_month) > 3:
            return config_map[k]
    return {}

def determine_is_sl(comp_type, nganh_clean, sl=0.0, dt=0.0, tg=0.0, config_obj=None):
    """
    Chuẩn hóa xác định chỉ tiêu là Sản Lượng (isSL=True) hay Doanh Thu (isSL=False)
    đồng bộ 100% với baocao_luyke.html & baocao_realtime.html:
    1. compType in (2, 6) hoặc nhóm từ khóa ('sim', 'camera', 'nạp rút', 'thẻ tín dụng'): SL
    2. compType in (1, 3, 4, 5): DT
    3. Fallback: so sánh tỷ lệ hoàn thành khi sl > 0 và dt > 0
    4. Ưu tiên cấu hình isSL từ Config_ThiDua nếu có
    """
    comp_type_num = int(parse_number(comp_type)) if comp_type is not None else 0
    clean = str(nganh_clean).lower().strip()
    
    is_sl = False
    if comp_type_num in (2, 6) or any(k in clean for k in ['sim', 'camera', 'nạp rút', 'thẻ tín dụng']):
        is_sl = True
    elif comp_type_num in (1, 3, 4, 5):
        is_sl = False
    else:
        is_sl = sl > 0 and (dt == 0 or abs((sl / tg) - 1.0) < abs((dt / tg) - 1.0)) if tg > 0 else (sl > 0 and dt == 0)

    if config_obj and isinstance(config_obj, dict):
        cfg_is_sl = config_obj.get("isSL")
        if cfg_is_sl is not None:
            parsed = parse_is_sl(cfg_is_sl)
            if parsed is not None:
                is_sl = parsed

    return is_sl

def fmt_val_td(val, is_sl=False):
    """
    Format giá trị Thực hiện / Target cho bảng thi đua:
    - Nếu là SL: hiển thị số nguyên
    - Nếu là DT: nếu có số lẻ và < 100 thì giữ tối đa 2 chữ số thập phân (ví dụ 2.45, 19.5),
      nếu là số nguyên hoặc >= 100 thì làm tròn định dạng hàng nghìn
    """
    if val is None or val == '':
        return "0"
    num = parse_number(val)
    if is_sl:
        return f"{int(round(num)):,}"
    if abs(num) < 100 and (num != int(num)):
        return f"{num:.2f}".rstrip('0').rstrip('.')
    return f"{int(round(num)):,}"

def shorten_name(name):
    if not name:
        return ""
    import re
    s = str(name).replace("NNH ", "").strip()
    
    # Danh sách chuẩn hóa & viết tắt tên ngành hàng thi đua
    # Nguyên tắc: Cái nào quá dài thì viết tắt nhưng vẫn đảm bảo đọc hiểu 100%,
    # phân biệt rõ ràng giữa các nhóm cùng chủng loại (Realme / Vivo / Tablet, ICT / CE, v.v.)
    replacements = [
        # Trả chậm
        ("TRẢ CHẬM FECREDIT, SHINHAN, SAMSUNG FINANCE+", "TC FE, Shinhan, SS+"),
        ("TRẢ CHẬM FECREDIT, SHINHAN, SAMSUNG FINANCE", "TC FE, Shinhan, SS+"),
        ("TRẢ CHẬM HOMECREDIT", "TC HomeCredit"),
        ("TRẢ CHẬM ĐIỆN MÁY VÀ GIA DỤNG", "TC Điện Máy & GD"),
        ("TRẢ CHẬM ĐIỆN MÁY", "TC Điện Máy"),
        ("TRẢ CHẬM", "TC"),
        ("Trả Chậm", "TC"),
        
        # Bảo hiểm
        ("BẢO HIỂM THỢ ĐMX_ICT", "BH Thợ ICT"),
        ("BẢO HIỂM THỢ ĐMX_CE", "BH Thợ CE"),
        ("BẢO HIỂM THỢ ĐMX", "BH Thợ ĐMX"),
        ("Bảo Hiểm Tổng", "BH Tổng"),
        ("Bảo hiểm tổng", "BH Tổng"),
        ("BẢO HIỂM", "BH"),
        ("Bảo hiểm", "BH"),
        
        # Điện thoại & Apple
        ("IPHONE 18 series, iPhone Duo", "iPhone 18, Duo"),
        ("IPHONE 18 series", "iPhone 18"),
        ("ĐIỆN THOẠI REALME", "ĐT realme"),
        ("ĐIỆN THOẠI VIVO", "ĐT Vivo"),
        ("ĐIỆN THOẠI & TABLET ANDROID", "ĐT & Tablet"),
        ("ĐIỆN THOẠI & PHỤ KIỆN", "ĐT & PK"),
        ("ĐIỆN THOẠI", "ĐT"),
        ("Điện thoại", "ĐT"),
        ("Điện Thoại", "ĐT"),
        
        # Điện máy & Điện lạnh
        ("TỦ LẠNH, TỦ ĐÔNG, TỦ MÁT", "Tủ Lạnh, Đông, Mát"),
        ("TỦ LẠNH, TỦ MÁT", "Tủ Lạnh, Mát"),
        ("TỦ LẠNH, TỦ ĐỒ", "Tủ Lạnh/Đông"),
        ("MÁY SẤY & MÁY RỬA CHÉN", "Sấy & Rửa Chén"),
        ("MÁY NƯỚC NÓNG FERROLI", "Nước Nóng Ferroli"),
        ("MÁY LỌC KHÔNG KHÍ", "Lọc K.Khí"),
        ("MÁY LỌC NƯỚC", "Lọc Nước"),
        ("MÁY GIẶT", "Máy Giặt"),
        ("Máy giặt", "Máy Giặt"),
        ("ĐIỆN TỬ TCL", "Điện Tử TCL"),
        ("Điện Gia Dụng", "Gia Dụng"),
        ("Điện gia dụng", "Gia Dụng"),
        
        # Gia dụng & Khác
        ("NỒI CƠM &  NỒI CHIÊN", "Nồi Cơm & Chiên"),
        ("NỒI CƠM & NỒI CHIÊN", "Nồi Cơm & Chiên"),
        ("GIA DỤNG SUNHOUSE", "GD Sunhouse"),
        ("Hút bụi - 20 Tỉnh", "Hút Bụi 20 Tỉnh"),
        ("Hút bụi", "Hút Bụi"),
        ("HÚT BỤI", "Hút Bụi"),
        ("SẠC DỰ PHÒNG", "Sạc Dự Phòng"),
        ("DOANH THU ĐỒNG HỒ", "Đồng Hồ"),
        ("ĐỒNG HỒ", "Đồng Hồ"),
        
        # Phụ kiện & IT & Viễn thông
        ("PHỤ KIỆN IT - NHÓM KHÁC", "PK IT & Khác"),
        ("PHỤ KIỆN CÔNG NGHỆ", "PK Công Nghệ"),
        ("PHỤ KIỆN", "PK"),
        ("Phụ kiện", "PK"),
        ("Phụ Kiện", "PK"),
        ("TABLET ANDROID VÀ MÁY ĐỌC SÁCH", "Tablet & Đọc Sách"),
        ("MỞ THẺ TÍN DỤNG TPBANK EVO VÀ VPBANK MWG", "Mở Thẻ TPB, VPB"),
        ("NẠP - RÚT TIỀN TÀI KHOẢN NGÂN HÀNG", "Nạp Rút Tiền NH"),
        ("NẠP RÚT TIỀN TẬN NƠI", "Nạp Rút Tiền"),
        ("SIM MOBIFONE/SIM DMX", "SIM Mobi/DMX"),
        ("SIM TỔNG", "SIM Tổng"),
        ("Thiết Bị Gia Đình", "TB Gia Đình"),
        ("Thiết bị gia đình", "TB Gia Đình"),
        ("Thiết Bị", "TB"),
        ("Thiết bị", "TB"),
        ("Chăm Sóc Cá Nhân", "CS Cá Nhân"),
        ("Chăm sóc cá nhân", "CS Cá Nhân"),
        ("Nhóm Thi Đua", "Thi Đua"),
        ("Nhóm thi đua", "Thi Đua"),
        ("VÍ TRẢ SAU", "Ví Trả Sau"),
        ("Vay tiền mặt", "Vay Tiền Mặt"),
    ]
    for old, new in replacements:
        if old.lower() in s.lower():
            pattern = re.compile(re.escape(old), re.IGNORECASE)
            s = pattern.sub(new, s)
            
    # Rút gọn khoảng trắng thừa
    s = re.sub(r'\s+', ' ', s).strip()
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

def get_cat_emoji(name):
    n = str(name).strip().upper()
    if "QUẠT" in n: return "🎐"
    if "ĐIỆN TỬ" in n or "TIVI" in n or "SONY" in n: return "📺"
    if "GIẶT" in n or "SẤY" in n: return "🧺"
    if "PIN" in n or "SẠC" in n: return "🔋"
    if "LAPTOP" in n or "IT" in n or "MÁY TÍNH" in n: return "💻"
    if "TỦ LẠNH" in n or "ĐÔNG" in n or "MÁT" in n: return "🧊"
    if "LỌC" in n or "NƯỚC" in n or "MLN" in n: return "🚰"
    if "GIA DỤNG" in n: return "🍳"
    if "ĐIỆN THOẠI" in n or "SMARTPHONE" in n or "PHONE" in n or "FLAGSHIP" in n: return "📱"
    if "CAMERA" in n: return "📷"
    if "MÁY LẠNH" in n or "ĐIỀU HÒA" in n or "LẠNH" in n: return "📦"
    if "ĐỒNG HỒ" in n or "THỜI TRANG" in n or "WEARABLE" in n: return "📦"
    if "BẢO HIỂM" in n or "FINANCE" in n or "CREDIT" in n: return "💳"
    if "SIM" in n: return "📶"
    if "PHỤ KIỆN" in n: return "🎧"
    return "📦"

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

def make_table_row(vals, weights, aligns=None, colors=None, bold=False, bg_color=None, wraps=None):
    if not aligns:
        aligns = ["start"] * len(vals)
    if not colors:
        colors = ["#0f172a"] * len(vals)
    if not wraps:
        wraps = [False] * len(vals)
    elif isinstance(wraps, bool):
        wraps = [wraps] * len(vals)
    contents = []
    for v, w, a, c, wr in zip(vals, weights, aligns, colors, wraps):
        item = {
            "type": "text",
            "text": str(v),
            "size": "xxs",
            "color": c,
            "flex": w
        }
        if a and a != "start":
            item["align"] = a
        if bold:
            item["weight"] = "bold"
        if wr:
            item["wrap"] = True
        contents.append(item)
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

        diff_ck = 0.0
        if dt_ck > 0:
            diff_ck = dt - dt_ck
        elif tang_giam_ck != 0:
            diff_ck = dt * (tang_giam_ck / 100.0)
        else:
            diff_ck = dt - tg

        if dt > 0 or sl > 0:
            parsed_bi.append({
                "name": shorten_name(nganh),
                "sl": int(sl),
                "dt": dt,
                "tg": tg,
                "dt_ck": dt_ck,
                "diff_ck": diff_ck,
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
        is_sl_cfg = parse_is_sl(get_key_val(c, "isSL", "issl", "đơn vị", "donvi", "loại chỉ tiêu", default=None))
        if ten:
            clean_ten = str(ten).lower().strip()
            config_map[clean_ten] = {
                "phanLoai": phan_loai,
                "isSL": is_sl_cfg,
                "thuTu": parse_number(get_key_val(c, "thứ tự", "Ngày", default=999.0))
            }

    parsed_td = []
    cnt_dk = 0
    for r in td_rows:
        nganh = get_key_val(r, "maingroupname", "main group name", "nhóm ngành hàng", "nhóm ngành hàng chính", "programname", default=None)
        if not nganh or str(nganh).strip().upper() == "N/A":
            continue
        nganh_clean = str(nganh).lower().strip()
        c_obj = find_td_config(nganh_clean, config_map)
        # CHỈ TÍNH NHỮNG MÔN KHAI BÁO TRONG CONFIG VỚI ƯU TIÊN 1 VÀ 2 (ĐỒNG BỘ 100% VỚI BAOCAO_LUYKE.HTML)
        if config_map and (c_obj.get("phanLoai") not in [1.0, 2.0]):
            continue
            
        tg = parse_number(get_key_val(r, "target", "target (qđ)", "mục tiêu", "kế hoạch", default=0.0))
        if tg <= 0:
            continue
            
        sl = parse_number(get_key_val(r, "số lượng", "quantity", "sl", default=0.0))
        dt = max(
            parse_number(get_key_val(r, "doanh thu quy đổi", "revenue_kfactor", default=0.0)),
            parse_number(get_key_val(r, "doanh thu", "revenue", default=0.0))
        )
        
        comp_type = get_key_val(r, "competitiontype", "competitionType", "phân loại", "loại", default=None)
        is_sl = determine_is_sl(comp_type, nganh_clean, sl=sl, dt=dt, tg=tg, config_obj=c_obj)
        
        actual = sl if is_sl else dt
        ht_target = actual / tg
        ht_du_kien = ((actual / days_passed) * days_in_month) / tg if days_passed > 0 else 0.0
        
        if ht_du_kien >= 1.0:
            cnt_dk += 1
            
        con_lai = max(0.0, tg - actual)
        mt_ngay_val = (con_lai / days_remaining) if days_remaining > 0 else 0.0
        if not is_sl:
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
            "unit": "SP" if is_sl else "TR",
            "is_sl": is_sl,
            "mt_ngay_str": mt_ngay_str,
            "phan_loai": c_obj.get("phanLoai", 1.0)
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
                            "width": f"{min(100, max(2, round(totalHT * 100)))}%",
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
                    "width": f"{min(100, max(2, round(b['ht'] * 100)))}%",
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

    # BẢNG 2: BẢNG TỶ TRỌNG & TĂNG TRƯỞNG SO VỚI CÙNG KỲ (Card riêng biệt)
    # Cân bằng hợp lý: Top tăng trưởng mạnh & Top giảm sâu so với cùng kỳ
    growth_card_contents = [
        {"type": "text", "text": "📈 TỶ TRỌNG & TĂNG TRƯỞNG CÙNG KỲ", "size": "xxs", "color": "#0f766e", "weight": "bold", "margin": "xs"}
    ]
    headers2 = ["STT", "Ngành hàng", "Tỷ trọng", "vs Cùng kỳ"]
    weights2 = [1, 3, 2, 5]
    aligns2 = ["start", "start", "center", "end"]

    top_growth = sorted([x for x in parsed_bi if x.get("diff_ck", 0.0) > 0], key=lambda x: x.get("diff_ck", 0.0), reverse=True)[:6]
    top_decline = sorted([x for x in parsed_bi if x.get("diff_ck", 0.0) < 0], key=lambda x: x.get("diff_ck", 0.0))[:6]

    # Section 1: Top Tăng Trưởng Mạnh (Tối đa 6 ngành)
    growth_card_contents.append({
        "type": "box",
        "layout": "horizontal",
        "backgroundColor": "#f0fdf4",
        "paddingAll": "xs",
        "cornerRadius": "xs",
        "margin": "xs",
        "contents": [
            {"type": "text", "text": f"🚀 TOP TĂNG TRƯỞNG DẪN ĐẦU ({len(top_growth)})", "size": "xxs", "color": "#15803d", "weight": "bold"}
        ]
    })
    growth_card_contents.append(make_table_header(headers2, weights2, aligns2, bg_color="#15803d"))

    for idx, b in enumerate(top_growth):
        ty_trong = (b["dt"] / tDT * 100) if tDT > 0 else 0.0
        diff_ck = b.get("diff_ck", 0.0)
        pct_ck = b.get("tang_giam_ck", 0.0)
        sign_str = "+" if diff_ck >= 0 else ""
        pct_sign_str = "+" if pct_ck >= 0 else ""
        diff_fmt = fmt_val_td(diff_ck, is_sl=False)
        growth_text = f"▲ {pct_sign_str}{pct_ck:.1f}% ({sign_str}{diff_fmt} Tr)"
        row2_vals = [idx + 1, b['name'], f"{ty_trong:.0f}%", growth_text]
        row2_colors = ["#64748b", "#0f172a", "#0284c7", "#16a34a"]
        growth_card_contents.append(make_table_row(row2_vals, weights2, aligns2, row2_colors))

    # Section 2: Top Giảm So Với Cùng Kỳ (Tối đa 6 ngành)
    if top_decline:
        growth_card_contents.append({"type": "separator", "color": "#fecaca", "margin": "sm"})
        growth_card_contents.append({
            "type": "box",
            "layout": "horizontal",
            "backgroundColor": "#fef2f2",
            "paddingAll": "xs",
            "cornerRadius": "xs",
            "margin": "xs",
            "contents": [
                {"type": "text", "text": f"🔻 TOP GIẢM SO VỚI CÙNG KỲ ({len(top_decline)})", "size": "xxs", "color": "#b91c1c", "weight": "bold"}
            ]
        })
        growth_card_contents.append(make_table_header(headers2, weights2, aligns2, bg_color="#b91c1c"))

        for idx, b in enumerate(top_decline):
            ty_trong = (b["dt"] / tDT * 100) if tDT > 0 else 0.0
            diff_ck = b.get("diff_ck", 0.0)
            pct_ck = b.get("tang_giam_ck", 0.0)
            diff_fmt = fmt_val_td(abs(diff_ck), is_sl=False)
            growth_text = f"▼ {pct_ck:.1f}% (-{diff_fmt} Tr)"
            row2_vals = [idx + 1, b['name'], f"{ty_trong:.0f}%", growth_text]
            row2_colors = ["#64748b", "#0f172a", "#0284c7", "#dc2626"]
            growth_card_contents.append(make_table_row(row2_vals, weights2, aligns2, row2_colors))

    tDT_CK = tDT_CK_total if tDT_CK_total > 0 else sum(b.get("dt_ck", 0.0) for b in parsed_bi)
    if tDT_CK > 0:
        diff_total_ck = tDT - tDT_CK
        pct_total_ck = (diff_total_ck / tDT_CK * 100)
        sign_str = "+" if diff_total_ck >= 0 else ""
        pct_sign_str = "+" if pct_total_ck >= 0 else ""
        total_diff_fmt = fmt_val_td(abs(diff_total_ck), is_sl=False)
        if diff_total_ck >= 0:
            total_growth_str = f"▲ {pct_sign_str}{pct_total_ck:.1f}% ({sign_str}{total_diff_fmt} Tr)"
        else:
            total_growth_str = f"▼ {pct_total_ck:.1f}% (-{total_diff_fmt} Tr)"
    else:
        diff_total_tg = tDT - tTG
        pct_total_tg = (diff_total_tg / tTG * 100) if tTG > 0 else 0.0
        sign_str = "+" if diff_total_tg >= 0 else ""
        pct_sign_str = "+" if pct_total_tg >= 0 else ""
        total_diff_fmt = fmt_val_td(abs(diff_total_tg), is_sl=False)
        if diff_total_tg >= 0:
            total_growth_str = f"▲ {pct_sign_str}{pct_total_tg:.1f}% ({sign_str}{total_diff_fmt} Tr)"
        else:
            total_growth_str = f"▼ {pct_total_tg:.1f}% (-{total_diff_fmt} Tr)"

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
    weights_p2 = [1, 5, 2, 3, 2, 2]
    aligns_p2 = ["center", "start", "center", "center", "center", "center"]
    wraps_p2 = [False, True, False, False, False, False]

    body_contents_p2 = []

    # ===== DMX STT LIEN TUC 25/09/2026 =====
    # Hai bang (DU KIEN VE DICH / CHUA VE DICH) truoc day moi bang danh lai STT tu 1
    # => bang duoi ket thuc o 10, nguoi doc tuong chi co 13+9=22 nhom (thuc te 23).
    # Nay bang 2 danh NOI TIEP tu len(td_done)+1, khop 1-1 voi bang lien tuc cua baocao_luyke.html.
    shift_done = 0
    shift_pending = len(td_done)
    # ===== HET DMX STT LIEN TUC =====

    if td_done:
        done_table = [
            {"type": "text", "text": f"🏆 NHÓM DỰ KIẾN VỀ ĐÍCH (1–{len(td_done)})", "size": "xxs", "color": "#15803d", "weight": "bold", "margin": "xs"},
            make_table_header(headers_p2, weights_p2, aligns_p2, bg_color="#15803d"),
            {"type": "separator", "color": "#bbf7d0", "margin": "xs"}
        ]
        for idx, t in enumerate(td_done, start=shift_done + 1):
            unit_tag = "(SL)" if t.get("is_sl") else "(DT)"
            display_name = f"{t['name']} {unit_tag}"
            mt_str = "🏆" if (t["actual"] >= t["target"] or t.get("ht", 0) >= 1.0) else t.get("mt_ngay_str", "0")
            act_tg_str = f"{fmt_val_td(t['actual'], t.get('is_sl', False))} / {fmt_val_td(t['target'], t.get('is_sl', False))}"
            ht_str = f"{t['ht']*100:.0f}%"
            dk_str = f"{t['ht_dk']*100:.0f}%"

            name_color = "#dc2626" if t.get("phan_loai") == 2.0 else "#0f172a"
            vals = [str(idx), display_name, mt_str, act_tg_str, ht_str, dk_str]
            colors = ["#64748b", name_color, "#16a34a" if mt_str == "🏆" else "#0284c7", "#0f172a", get_color_class(t["ht"]), get_color_class(t["ht_dk"])]
            done_table.append(make_table_row(vals, weights_p2, aligns_p2, colors, wraps=wraps_p2))

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
            {"type": "text", "text": f"🎯 NHÓM CHƯA VỀ ĐÍCH ({len(td_done) + 1}–{len(td_done) + len(td_pending)})", "size": "xxs", "color": "#b91c1c", "weight": "bold", "margin": "xs"},
            make_table_header(headers_p2, weights_p2, aligns_p2, bg_color="#0f766e"),
            {"type": "separator", "color": "#cbd5e1", "margin": "xs"}
        ]
        for idx, t in enumerate(td_pending, start=shift_pending + 1):
            unit_tag = "(SL)" if t.get("is_sl") else "(DT)"
            display_name = f"{t['name']} {unit_tag}"
            mt_str = "🏆" if (t["actual"] >= t["target"] or t.get("ht", 0) >= 1.0) else t.get("mt_ngay_str", "0")
            act_tg_str = f"{fmt_val_td(t['actual'], t.get('is_sl', False))} / {fmt_val_td(t['target'], t.get('is_sl', False))}"
            ht_str = f"{t['ht']*100:.0f}%"
            dk_str = f"{t['ht_dk']*100:.0f}%"

            name_color = "#dc2626" if t.get("phan_loai") == 2.0 else "#0f172a"
            vals = [str(idx), display_name, mt_str, act_tg_str, ht_str, dk_str]
            colors = ["#64748b", name_color, "#16a34a" if mt_str == "🏆" else "#0284c7", "#0f172a", get_color_class(t["ht"]), get_color_class(t["ht_dk"])]
            pending_table.append(make_table_row(vals, weights_p2, aligns_p2, colors, wraps=wraps_p2))

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
        # Hạng 4+ bỏ chữ "HẠNG #" cho gọn — badge nằm riêng 1 dòng, không còn đè tên NV
        header_bg = "#0f2b48"
        rank_badge_bg = "#e0f2fe"
        rank_badge_text = "#0369a1"
        rank_label = f"#{rank}"

    if not thi_dua_list:
        thi_dua_list = []
    else:
        # Sắp xếp nhóm thi đua theo du_kien giảm dần và GIỮ NGUYÊN toàn bộ nhóm
        thi_dua_list = sorted(thi_dua_list, key=lambda x: x.get("du_kien", 0.0), reverse=True)

    td_passed = e.get("count_nh_du_kien")
    if td_passed is None:
        td_passed = sum(1 for td in thi_dua_list if td.get("du_kien", 0.0) >= 100.0 or td.get("ht", 0.0) >= 100 or str(td.get("con_lai")) == "🏆")
    td_total = len(thi_dua_list)
    td_pct = e.get("contest_score")
    if td_pct is None:
        td_pct = (td_passed / td_total * 100.0) if td_total > 0 else 0.0

    # 1. Header Luxury — 3 tầng để KHÔNG BAO GIỜ đè nhau:
    #    tầng 1: badge hạng (trái) + điểm (phải)  |  tầng 2: họ tên đầy đủ  |  tầng 3: mã NV
    header_rows = [
        {
            "type": "box",
            "layout": "horizontal",
            "alignItems": "center",
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
                    "flex": 0,
                    "contents": [
                        {"type": "text", "text": rank_label, "size": "xxs", "weight": "bold", "color": rank_badge_text}
                    ]
                },
                {"type": "filler"},
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
                    "flex": 0,
                    "contents": [
                        {"type": "text", "text": f"⭐ {score_val:.1f} Đ", "weight": "bold", "size": "xs", "color": "#fef08a", "align": "center"}
                    ]
                }
            ]
        },
        {
            "type": "text",
            "text": full_name,
            "weight": "bold",
            "size": "lg",
            "color": "#ffffff",
            "wrap": True
        }
    ]
    if user_id:
        header_rows.append({
            "type": "text",
            "text": f"Mã NV: {user_id}",
            "size": "xxs",
            "color": "#e2e8f0",
            "wrap": True
        })

    header_component = {
        "type": "box",
        "layout": "vertical",
        "backgroundColor": header_bg,
        "paddingAll": "md",
        "spacing": "xs",
        "contents": header_rows
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

    # 4. Bảng Nhóm Hàng Thi Đua 7 Cột (# | NH | MT | LK/TG | CÒN | %HT | %DK)
    #    Có lại cột "#" (anh Dương muốn nhìn STT cho dễ trao đổi); cột tên vẫn đủ rộng (4/15)
    #    và có wrap nên tên dài xuống dòng thay vì bị LINE cắt thành "…".
    headers = ["#", "NHÓM HÀNG", "MT", "LK / TG", "CÒN", "%HT", "%DK"]
    weights = [1, 4, 1, 3, 2, 2, 2]
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
            # wrap=True là CHÌA KHOÁ: tên ngành dài xuống dòng thay vì bị LINE cắt thành "…"
            cell = {"type": "text", "text": str(v), "size": "xxs", "wrap": True}
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
            "type": "box", "layout": "horizontal", "paddingAll": "xs", "contents": row_contents
        }
        if row_bg != "#ffffff":
            row_box["backgroundColor"] = row_bg
            row_box["cornerRadius"] = "xs"

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
            is_sl_cfg = parse_is_sl(get_key_val(r, "isSL", "issl", "đơn vị", "donvi", "loại chỉ tiêu", default=None))
            if cat_name and str(cat_name).strip():
                pl_num = parse_number(pl_val)
                if pl_num in [1.0, 2.0]:
                    config_map[str(cat_name).lower().strip()] = {
                        "phanLoai": pl_num,
                        "isSL": is_sl_cfg,
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
        dt_qd = parse_number(get_key_val(r, "doanh thu quy đổi", "revenue_kfactor", "dt quy đổi", "revenue_kfactor_ave3m", default=0.0))
        rev_k_cum = parse_number(get_key_val(r, "revenue_kfactor_cum", "revenue_cum", "doanh thu quy đổi lũy kế", "dt quy đổi lũy kế", "doanh thu lũy kế", "dt lũy kế", default=0.0))
        # baocao_nhanvien.html (baseUserMap): b_dt = revKCum > 0 ? revKCum : dtQD
        b_dt = rev_k_cum if rev_k_cum > 0 else dt_qd
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
        # baocao_nhanvien.html lấy chế độ từ locked_mode của bản khoá (không hardcode hybrid)
        locked_mode = lock_config.get("locked_mode") or "hybrid"
        for u_id, u_conf in active_config_staff.items():
            locked_s = next((s for s in locked_staff_list if str(s.get("userId", "")).strip().upper() == u_id), None)
            if locked_s and locked_s.get("lockedRatio") is not None:
                r = parse_number(locked_s.get("lockedRatio"))
            elif locked_mode == "equal":
                r = 1.0 / num_active_staff
            elif locked_mode == "hybrid":
                hcr = base_staff_dt_contrib.get(u_id, u_conf["ratio"])
                r = 0.6 / num_active_staff + 0.4 * hcr
            else:
                r = u_conf["ratio"]
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
            dt_qd = parse_number(get_key_val(r, "Doanh thu Quy đổi", "doanh thu quy đổi", "revenue_kfactor", "dt quy đổi", "revenue_kfactor_ave3m", default=0.0))
            rev_k_cum = parse_number(get_key_val(r, "revenue_kfactor_cum", "revenue_cum", "doanh thu quy đổi lũy kế", "dt quy đổi lũy kế", "doanh thu lũy kế", "dt lũy kế", default=0.0))
            # baocao_nhanvien.html: totalDT = revKCum > 0 ? revKCum : dtQD
            # (KHÔNG cộng dồn rev_k_cum + dt_qd — làm vậy là tính trùng doanh thu,
            #  vì "Doanh thu Quy đổi" và "revenue_kfactor_cum" đang cùng là số lũy kế.)
            dt_total = rev_k_cum if rev_k_cum > 0 else dt_qd
            user_map[u_id]["dt"] += dt_total

    # 5. Bộ ngành thi đua — DỰNG ĐÚNG THEO baocao_nhanvien.html
    #    (khớp CHÍNH XÁC tên ngành · chỉ đọc cột "doanh thu" · isSL chỉ so tỷ lệ hoàn thành)
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

        # ===== DMX DONG BO KHOP MO 25/09/2026 =====
        # DÙNG find_td_config() — ĐỒNG BỘ với `findTDConfig()` của baocao_nhanvien.html.
        #
        # LỊCH SỬ (đừng lặp lại sai lầm cũ): trước đây chỗ này khớp CHÍNH XÁC
        # (`config_map.get(nganh_clean)`) kèm ghi chú rằng find_td_config "sẽ nhận thêm
        # Đồng hồ -> tổng hệ số 36 thay vì 34". Ghi chú đó nay ĐÃ LỖI THỜI: chính
        # baocao_nhanvien.html sau đó được đồng bộ sang findTDConfig (xem khối
        # "DMX KHOP TEN THIDUA 25/09/2026" trong file HTML), nên HTML tính theo 23 nhóm
        # / hệ số 36, còn bot tính 22 nhóm / 34 -> ĐIỂM THI ĐUA CỦA MỌI NHÂN VIÊN LỆCH.
        # Ca thật: Config ghi "Đồng hồ tháng 9", dữ liệu ghi "Đồng hồ" (phanLoai=2).
        # Kiểm chứng 25/09/2026: khớp thô 22 nhóm/34 hệ số · khớp mờ 23 nhóm/36 hệ số.
        c_obj = find_td_config(nganh_clean, config_map) or {}
        if config_map and (c_obj.get("phanLoai") not in [1.0, 2.0]):
            continue
        # ===== HET DMX DONG BO KHOP MO =====

        c_obj = c_obj or {"phanLoai": 1.0, "thuTu": 999.0}
        sl = parse_number(get_key_val(r, "số lượng", "quantity", "sl", default=0.0))
        dt = parse_number(get_key_val(r, "doanh thu", default=0.0))

        # isSL đúng baocao_nhanvien.html: CHỈ so tỷ lệ hoàn thành giữa SL và DT.
        # KHÔNG dùng determine_is_sl(): hàm đó có thêm bộ luật compType/từ khóa
        # (dành cho baocao_luyke.html và baocao_realtime.html) -> lệch 4 ngành.
        is_sl = True
        thuc_hien_store = sl
        if dt > 0:
            if sl == 0:
                is_sl = False
                thuc_hien_store = dt
            elif abs((dt / tg) - 1) < abs((sl / tg) - 1):
                is_sl = False
                thuc_hien_store = dt

        active_categories.append({
            "nganhHang": nganh_str,
            "storeTarget": tg,
            "phanLoai": c_obj["phanLoai"],
            "thuTu": c_obj["thuTu"],
            "isSL": is_sl,
            "storeHtDuKien": (((thuc_hien_store / days_passed) * days_in_month) / tg) if tg > 0 else 0.0
        })

    # baocao_nhanvien.html sắp theo mức hoàn thành dự kiến GIẢM DẦN (không phải theo thứ tự cấu hình)
    active_categories.sort(key=lambda x: x["storeHtDuKien"], reverse=True)

    # 6. Tính target từng ngành hàng thi đua cho từng nhân viên (userCatTargets)
    user_cat_targets = {u_id: {} for u_id in active_config_staff}
    for cat in active_categories:
        nganh_clean = cat["nganhHang"].lower().strip()
        if lock_config and lock_config.get("is_locked") and locked_staff_list:
            initial_cat_ratios = {}
            sum_cat_ratios = 0.0
            # baocao_nhanvien.html chọn giá trị dự phòng theo locked_mode của bản khoá
            locked_mode = lock_config.get("locked_mode") or "hybrid"
            for u_id in active_config_staff:
                u_conf = active_config_staff.get(u_id, {"ratio": 0.0})

                # Dự phòng ĐÚNG HTML: equal -> chia đều · hybrid -> 0.6/N + 0.4*hcr · còn lại -> % chia.
                # (Trước đây bot luôn lấy thẳng % chia, nên target của ngành KHÔNG có trong bản khoá
                #  bị lệch — ví dụ "Gia dụng Kangaroo" và "Đồng hồ".)
                if locked_mode == "equal":
                    cat_ratio = 1.0 / num_active_staff
                elif locked_mode == "hybrid":
                    hcr = base_staff_cat_contrib.get(u_id, {}).get(nganh_clean, u_conf["ratio"])
                    cat_ratio = 0.6 / num_active_staff + 0.4 * hcr
                else:
                    cat_ratio = u_conf["ratio"]

                locked_s = next((s for s in locked_staff_list if str(s.get("userId", "")).strip().upper() == u_id), None)
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
    active_cat_is_sl = {c["nganhHang"].lower().strip(): c.get("isSL", True) for c in active_categories}
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

        # baocao_nhanvien.html: khi value_compe = 0 thì thay bằng SL hoặc DT thực hiện của dòng đó
        # (chọn theo isSL của ngành). Trước đây bot bỏ qua bước này nên cộng 0.
        if actual == 0:
            if active_cat_is_sl.get(nganh_clean, False):
                actual = parse_number(get_key_val(r, "số lượng", "quantity", default=0.0))
            else:
                actual = max(
                    parse_number(get_key_val(r, "Doanh thu Quy đổi", "revenue_kfactor", default=0.0)),
                    parse_number(get_key_val(r, "Doanh thu", "revenue", default=0.0))
                )

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
    
    # 2. Bubbles 2..N: Thẻ KPI Chi Tiết Từng NV (truyền đủ số ngành hàng thi đua của báo cáo)
    all_bubbles = [overview_bubble]
    total_emp = len(emp_list)
    for idx, e in enumerate(emp_list, start=1):
        staff_bubble = build_individual_staff_card(e, idx, total_emp, now_str, thi_dua_list=e.get("thi_dua_list"))
        all_bubbles.append(staff_bubble)
        
    return all_bubbles

# ============================================================================
#  ĐÓNG GÓI MESSAGE CHO LINE — CHỈ dùng tin nhắn MIỄN PHÍ (reply_message)
#  Tuyệt đối KHÔNG dùng push_message cho báo cáo vì push tốn quota tháng của LINE OA.
#  Giới hạn thật của LINE (tra tài liệu chính thức 23/09/2026):
#    - tối đa 5 message object / 1 request
#    - Flex bubble   ≤ 30 KB JSON
#    - Flex carousel ≤ 50 KB JSON và ≤ 12 thẻ
#  => Phải nhét vừa 5 message NGAY từ lần reply đầu, không có van xả bằng push.
# ============================================================================
LINE_MAX_MESSAGES = 5
FLEX_BUBBLE_MAX = 30000
FLEX_CAROUSEL_MAX = 50000
FLEX_CAROUSEL_MAX_ITEMS = 12


def json_size(obj):
    """Kích thước thật (byte UTF-8) của object khi serialize sang JSON."""
    try:
        return len(json.dumps(obj, ensure_ascii=False).encode("utf-8"))
    except Exception:
        return 0


def greedy_carousels(bubbles, max_items=FLEX_CAROUSEL_MAX_ITEMS, max_bytes=FLEX_CAROUSEL_MAX):
    """Chia danh sách bubble thành các carousel, mỗi cái ≤ max_bytes và ≤ max_items thẻ.

    Đi theo đúng thứ tự nên số carousel sinh ra là ÍT NHẤT có thể: đây là bài toán
    chia đoạn liên tiếp với trần dung lượng, greedy tuần tự là cách chia tối ưu.
    """
    parts, cur = [], []
    for b in bubbles:
        if cur:
            cand = {"type": "carousel", "contents": cur + [b]}
            if len(cur) >= max_items or json_size(cand) > max_bytes:
                parts.append(cur)
                cur = [b]
                continue
        cur.append(b)
    if cur:
        parts.append(cur)
    return parts


def pack_rt_messages(rt_bubbles, nv_bubbles, max_messages=LINE_MAX_MESSAGES):
    """Đóng gói bubble báo cáo realtime thành TỐI ĐA max_messages tin nhắn.

    Trả về (messages, info) khi xếp vừa, hoặc (None, info) khi KHÔNG vừa
    — lúc đó caller phải dựng lại với detail_limit nhỏ hơn.
    messages: list dict {"alt_text", "contents"} — sẵn sàng bọc FlexSendMessage.
    """
    rt_parts = greedy_carousels(rt_bubbles)
    nv_parts = greedy_carousels(nv_bubbles)
    info = {
        "rt_parts": len(rt_parts),
        "nv_parts": len(nv_parts),
        "rt_bytes": [json_size({"type": "carousel", "contents": p}) for p in rt_parts],
        "nv_bytes": [json_size({"type": "carousel", "contents": p}) for p in nv_parts],
        "nv_bubbles": len(nv_bubbles),
    }

    if len(rt_parts) + len(nv_parts) > max_messages:
        return None, info

    messages = []
    for part in rt_parts:
        if len(part) == 1:
            messages.append({"alt_text": "⚡ BÁO CÁO REALTIME", "contents": part[0]})
        else:
            messages.append({"alt_text": "⚡ BÁO CÁO REALTIME (Doanh Thu & Thi Đua)",
                             "contents": {"type": "carousel", "contents": part}})
    total_nv = len(nv_parts)
    for i, part in enumerate(nv_parts):
        tag = "" if total_nv == 1 else f" ({i+1}/{total_nv})"
        messages.append({
            "alt_text": f"👑 CHI TIẾT DTNV{tag}",
            "contents": {"type": "carousel", "contents": part} if len(part) > 1 else part[0],
        })
    return messages, info


# Các mức tiết lưu dòng chi tiết ngành hàng, thử lần lượt từ đầy đủ đến gọn nhất.
# None = KHÔNG cắt gì (giữ nguyên hành vi cũ).
RT_DETAIL_FALLBACKS = (None, 8, 6, 4, 3, 2, 1, 0)


def build_realtime_messages(max_messages=LINE_MAX_MESSAGES):
    """Dựng báo cáo realtime RT1 tinh gọn (Carousel 2 Thẻ: Tổng Quan Siêu Thị & Thi Đua).
    Đã tích hợp nút bấm '👑 Xem Chi Tiết Doanh Thu NV' để ẩn chi tiết vào nút, 
    chỉ khi người dùng bấm vào mới kích hoạt gửi chi tiết.
    """
    bubbles = build_realtime_flex(detail_limit=0)
    if not isinstance(bubbles, list):
        bubbles = [bubbles]
    if not bubbles:
        return None, {"error": "Không có dữ liệu realtime"}

    summary_bubbles = bubbles[:2]
    if len(summary_bubbles) == 1:
        msg_contents = summary_bubbles[0]
    else:
        msg_contents = {"type": "carousel", "contents": summary_bubbles}

    return [{
        "alt_text": "⚡ Báo Cáo Realtime Siêu Thị (RT1)",
        "contents": msg_contents
    }], {"rt_parts": 1, "nv_parts": 0, "mode": "summary_button"}


def build_realtime_staff_detail_messages(max_messages=LINE_MAX_MESSAGES, detail_fallbacks=RT_DETAIL_FALLBACKS):
    """Dựng bộ thẻ CHI TIẾT DOANH THU NHÂN VIÊN theo từng ngành hàng (👑 CHI TIẾT DTNV).
    Được kích hoạt khi người dùng bấm nút '👑 Xem Chi Tiết Doanh Thu NV' trên thẻ RT1 
    hoặc gõ lệnh RTNV / CTNV.
    """
    last_info = None
    for lim in detail_fallbacks:
        bubbles = build_realtime_flex(detail_limit=lim)
        if not isinstance(bubbles, list):
            bubbles = [bubbles]
        nv_bubbles = bubbles[2:]
        if not nv_bubbles:
            return None, {"error": "Hôm nay chưa có nhân viên nào phát sinh doanh thu"}

        nv_parts = greedy_carousels(nv_bubbles)
        info = {
            "nv_parts": len(nv_parts),
            "nv_bytes": [json_size({"type": "carousel", "contents": p}) for p in nv_parts],
            "nv_bubbles": len(nv_bubbles),
            "detail_limit": lim
        }
        last_info = info
        if len(nv_parts) <= max_messages:
            messages = []
            total_nv = len(nv_parts)
            for i, part in enumerate(nv_parts):
                tag = "" if total_nv == 1 else f" (P.{i+1}/{total_nv})"
                messages.append({
                    "alt_text": f"👑 CHI TIẾT DTNV{tag}",
                    "contents": {"type": "carousel", "contents": part} if len(part) > 1 else part[0],
                })
            return messages, info

    # Chốt hạ fallback
    bubbles = build_realtime_flex(detail_limit=detail_fallbacks[-1])
    if not isinstance(bubbles, list):
        bubbles = [bubbles]
    nv_bubbles = bubbles[2:]
    if not nv_bubbles:
        return None, last_info

    nv_parts = greedy_carousels(nv_bubbles)
    messages = []
    for i, part in enumerate(nv_parts[:max_messages]):
        messages.append({
            "alt_text": f"👑 CHI TIẾT DTNV ({i+1}/{min(len(nv_parts), max_messages)})",
            "contents": {"type": "carousel", "contents": part} if len(part) > 1 else part[0],
        })
    return messages, {"nv_parts": len(messages), "detail_limit": detail_fallbacks[-1]}


def build_realtime_flex(detail_limit=None):
    data = get_dashboard_data("Data_BI,Data_ThiDua,Config_ThiDua,Data_Realtime_BI,Data_Realtime_ThiDua,Data_Realtime_NV,Data_Realtime_NV_NganhHang")
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
        
        if dtqd != 0 or sl != 0:
            parsed_rt_bi.append({
                "name": str(nganh).strip(),
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
    
    config_map = {}
    for c in config_rows:
        ten = get_key_val(c, "ngành hàng", "nhóm ngành hàng", default=None)
        phan_loai = parse_number(get_key_val(c, "phân loại", "loại", default=0.0))
        is_sl_cfg = parse_is_sl(get_key_val(c, "isSL", "issl", "đơn vị", "donvi", "loại chỉ tiêu", default=None))
        if ten:
            clean_ten = str(ten).lower().strip()
            config_map[clean_ten] = {
                "phanLoai": phan_loai,
                "isSL": is_sl_cfg,
                "thuTu": parse_number(get_key_val(c, "thứ tự", "Ngày", default=999.0))
            }

    thi_dua_luy_ke = {}
    for r in td_rows:
        nganh = get_key_val(r, "maingroupname", "main group name", "nhóm ngành hàng", "nhóm ngành hàng chính", "programname", default=None)
        if not nganh or str(nganh).strip().upper() == "N/A":
            continue
        nganh_clean = str(nganh).lower().strip()
        c_obj = find_td_config(nganh_clean, config_map)
        if config_map and (c_obj.get("phanLoai") not in [1.0, 2.0]):
            continue
        
        tg = parse_number(get_key_val(r, "target", "target (qđ)", "mục tiêu", "kế hoạch", default=0.0))
        sl = parse_number(get_key_val(r, "số lượng", "quantity", "sl", default=0.0))
        dt = max(
            parse_number(get_key_val(r, "doanh thu quy đổi", "revenue_kfactor", default=0.0)),
            parse_number(get_key_val(r, "doanh thu", "revenue", default=0.0))
        )
        
        comp_type = get_key_val(r, "competitiontype", "competitionType", "phân loại", "loại", default=None)
        is_sl = determine_is_sl(comp_type, nganh_clean, sl=sl, dt=dt, tg=tg, config_obj=c_obj)
        
        actual = sl if is_sl else dt
        mt_ngay = max(0.0, (tg - actual) / days_remaining) if days_remaining > 0 else 0.0
        thi_dua_luy_ke[nganh_clean] = {
            "mt_ngay": mt_ngay,
            "is_sl": is_sl,
            "is_dt": not is_sl,
            "target_thang": tg,
            "lk_thuc_hien": actual
        }

    parsed_td = []
    rt_cntVD = 0
    for r in rt_td_rows:
        nganh = get_key_val(r, "maingroupname", "main group name", "nhóm ngành hàng", "nhóm ngành hàng chính", "programname", default=None)
        if not nganh or str(nganh).strip().upper() == "N/A":
            continue
        nganh_clean = str(nganh).lower().strip()
        c_obj = find_td_config(nganh_clean, config_map)
        if config_map and (c_obj.get("phanLoai") not in [1.0, 2.0]):
            continue
            
        lk_info = thi_dua_luy_ke.get(nganh_clean, {"mt_ngay": 0.0, "is_sl": False, "is_dt": True, "target_thang": 0.0, "lk_thuc_hien": 0.0})
        
        rt_dt = max(
            bi_map.get(nganh_clean, 0.0),
            parse_number(get_key_val(r, "revenue_RT", "revenue_rt", default=0.0)),
            parse_number(get_key_val(r, "revenue_KFactor_RT", "revenue_kfactor_rt", default=0.0)),
            parse_number(get_key_val(r, "doanh thu", "revenue", default=0.0))
        )
        rt_sl = max(
            parse_number(get_key_val(r, "quantity_RT", "quantity_rt", default=0.0)),
            parse_number(get_key_val(r, "quantity_KFactor", "quantity_kfactor", default=0.0)),
            parse_number(get_key_val(r, "số lượng", "quantity", default=0.0))
        )
        target_day = parse_number(get_key_val(r, "target_Day", "target_day", "mục tiêu ngày", default=0.0))
        target_effective = target_day if target_day > 0 else lk_info["mt_ngay"]
        
        comp_type_rt = get_key_val(r, "competitiontype", "competitionType", "phân loại", "loại", default=None)
        is_sl_rt = lk_info.get("is_sl")
        if is_sl_rt is None:
            is_sl_rt = determine_is_sl(comp_type_rt, nganh_clean, sl=rt_sl, dt=rt_dt, tg=target_effective, config_obj=c_obj)
                
        actual = rt_sl if is_sl_rt else rt_dt
        ht_target = actual / target_effective if target_effective > 0 else (1.0 if actual > 0 else 0.0)
        
        if ht_target >= 1.0:
            rt_cntVD += 1
            
        con_lai = max(0.0, target_effective - actual)
        parsed_td.append({
            "name": shorten_name(nganh),
            "actual": actual,
            "con_lai": con_lai,
            "target": target_effective,
            "ht": ht_target,
            "unit": "SP" if is_sl_rt else "TR",
            "is_sl": is_sl_rt,
            "phan_loai": c_obj.get("phanLoai", 1.0)
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
                            "width": f"{min(100, max(2, round(time_ratio * 100)))}%",
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
                            "width": f"{min(100, max(2, round(htChung * 100)))}%",
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
    data_rt_nv_detail = data.get("Data_Realtime_NV_NganhHang", [])

    # Chuẩn bị bản đồ hệ số quy đổi K-Factor từ Data_Realtime_BI (Đồng bộ chuẩn xác 100% với giao diện web baocao_realtime.js)
    bi_kfactor_map = {}
    total_bi_thuc_calc = 0.0
    total_bi_qd_calc = 0.0
    for r in rt_rows:
        nh_raw = get_key_val(r, "nhóm ngành hàng", "ngành hàng", "salegroupmastername", "groupname", default="")
        nh_lower = str(nh_raw).lower().strip()
        dt_thuc_row = parse_number(get_key_val(r, "doanh thu", "revenue", "revenue_off", "doanh thu thực", default=0))
        dt_qd_row = parse_number(get_key_val(r, "revenue_KFactor_RT", "revenue_RT", "doanh thu quy đổi", "doanh thu quy doi", "doanh thu qđ", "dtqd", default=0))
        if nh_lower in ["tổng cộng", "total"] or nh_lower.startswith("tổng"):
            total_bi_thuc_calc = dt_thuc_row
            total_bi_qd_calc = dt_qd_row
            continue
        clean_key = re.sub(r"^nnh\s+", "", nh_lower).strip()
        factor = (dt_qd_row / dt_thuc_row) if dt_thuc_row > 0 else 1.0
        bi_kfactor_map[nh_lower] = factor
        bi_kfactor_map[clean_key] = factor

    default_kfactor = (total_bi_qd_calc / total_bi_thuc_calc) if total_bi_thuc_calc > 0 else 1.35

    def resolve_staff_kfactor(cat_name):
        if not cat_name:
            return default_kfactor
        c = str(cat_name).lower().strip()
        c = re.sub(r"^nnh\s+", "", c).strip()
        if c in bi_kfactor_map:
            return bi_kfactor_map[c]

        if re.search(r"tivi|loa|dàn máy|soundbar|amply", c):
            if 'điện tử' in bi_kfactor_map: return bi_kfactor_map['điện tử']
        if re.search(r"máy giặt|máy sấy|rửa chén", c):
            if 'máy giặt, sấy' in bi_kfactor_map: return bi_kfactor_map['máy giặt, sấy']
        if re.search(r"tủ lạnh|tủ đông|tủ mát", c):
            if 'tủ lạnh, đông, mát' in bi_kfactor_map: return bi_kfactor_map['tủ lạnh, đông, mát']
        if re.search(r"máy lạnh|nước nóng|điều hòa", c):
            if 'máy lạnh, nước nóng' in bi_kfactor_map: return bi_kfactor_map['máy lạnh, nước nóng']
        if re.search(r"apple|phụ kiện tiện ích", c):
            if 'phụ kiện tiện ích' in bi_kfactor_map: return bi_kfactor_map['phụ kiện tiện ích']
        if re.search(r"trang trí", c):
            if 'phụ kiện trang trí' in bi_kfactor_map: return bi_kfactor_map['phụ kiện trang trí']
        if re.search(r"khung treo|giá đỡ", c):
            if 'phụ kiện lắp đặt' in bi_kfactor_map: return bi_kfactor_map['phụ kiện lắp đặt']
        if re.search(r"gia dụng lắp đặt", c):
            if 'gia dụng lắp đặt' in bi_kfactor_map: return bi_kfactor_map['gia dụng lắp đặt']
        if re.search(r"điện gia dụng", c):
            if 'điện gia dụng' in bi_kfactor_map: return bi_kfactor_map['điện gia dụng']
        if re.search(r"laptop", c):
            if 'laptop' in bi_kfactor_map: return bi_kfactor_map['laptop']
        if re.search(r"điện thoại", c):
            if 'điện thoại' in bi_kfactor_map: return bi_kfactor_map['điện thoại']
        if re.search(r"tablet", c):
            if 'tablet' in bi_kfactor_map: return bi_kfactor_map['tablet']
        if re.search(r"wearable|đồng hồ", c):
            if 'wearable' in bi_kfactor_map: return bi_kfactor_map['wearable']
        if re.search(r"it\b|chuột|bàn phím", c):
            if 'it' in bi_kfactor_map: return bi_kfactor_map['it']
        if re.search(r"software", c):
            if 'software' in bi_kfactor_map: return bi_kfactor_map['software']
        if re.search(r"bảo hành|bảo dưỡng", c) or c == 'n/a':
            if 'dịch vụ bảo hành, bảo dưỡng điện máy xanh' in bi_kfactor_map: return bi_kfactor_map['dịch vụ bảo hành, bảo dưỡng điện máy xanh']
        if re.search(r"dịch vụ", c):
            if 'dịch vụ' in bi_kfactor_map: return bi_kfactor_map['dịch vụ']

        for k, v in bi_kfactor_map.items():
            if k in c or c in k:
                return v
        return default_kfactor

    # Gom nhóm chi tiết thực bán theo nhân viên từ Data_Realtime_NV_NganhHang
    staff_realtime_agg = {}
    if data_rt_nv_detail and isinstance(data_rt_nv_detail, list):
        for d_row in data_rt_nv_detail:
            m_id = str(get_key_val(d_row, "Mã NV", "ma_nv", "user", default="")).strip()
            store_group = str(get_key_val(d_row, "STOREGROUPNAME", default="")).strip()
            if not m_id and store_group:
                m_match = re.match(r"^(\d+)", store_group)
                if m_match:
                    m_id = m_match.group(1)
            if not m_id or m_id.lower() == "online" or m_id == "18001060":
                continue
            s_name = str(get_key_val(d_row, "Tên Nhân Viên", "Tên NV", "ten_nv", default="")).strip()
            if not s_name and store_group and " - " in store_group:
                s_name = store_group.split(" - ", 1)[1].strip()
            sl_val = parse_number(get_key_val(d_row, "Số Lượng", "so_luong", default=0))
            raw_rev = parse_number(get_key_val(d_row, "Doanh Thu Thực", "Doanh Thu", "Doanh thu", "doanh_thu_thuc", default=0))
            rev_val = raw_rev / 1000000.0 if abs(raw_rev) >= 1000000 else raw_rev
            nganh_hang = str(get_key_val(d_row, "Ngành Hàng", "SALEGROUPNAME", "nganh_hang", default="")).strip()
            k_fact = resolve_staff_kfactor(nganh_hang)
            rev_qd = rev_val * k_fact

            if m_id not in staff_realtime_agg:
                staff_realtime_agg[m_id] = {
                    "mã nv": m_id,
                    "tên nv": s_name,
                    "số lượng": 0,
                    "doanh thu thực": 0.0,
                    "doanh thu quy đổi": 0.0
                }
            if not staff_realtime_agg[m_id]["tên nv"] and s_name:
                staff_realtime_agg[m_id]["tên nv"] = s_name
            staff_realtime_agg[m_id]["số lượng"] += sl_val
            staff_realtime_agg[m_id]["doanh thu thực"] += rev_val
            staff_realtime_agg[m_id]["doanh thu quy đổi"] += rev_qd

    # Ưu tiên số 1: Lấy chuẩn xác từ chi tiết từng đơn hàng realtime trong ngày (Fail-safe chốt chặn an toàn)
    has_rt_detail = len(staff_realtime_agg) > 0

    # Kiểm tra xem Data_Realtime_NV có bị lẫn số lũy kế cả tháng (Data_NV_BI) không
    is_cum_data = False
    if data_rt_nv and isinstance(data_rt_nv, list) and len(data_rt_nv) > 0:
        sample_first = data_rt_nv[0]
        if sample_first.get("numday_cum") is not None or sample_first.get("cum_as_of_date_key") is not None:
            is_cum_data = True
        else:
            total_check_dt = sum(parse_number(get_key_val(r, "doanh thu quy đổi", "revenue_kfactor", "dt quy đổi", "Doanh thu Quy đổi", default=0)) for r in data_rt_nv[:5])
            if abs(total_check_dt) >= 1000000:
                total_check_dt /= 1000000.0
            if rt_total > 0 and total_check_dt > (rt_total * 2.5):
                is_cum_data = True

    source_nv_rows = []
    if has_rt_detail:
        source_nv_rows = list(staff_realtime_agg.values())
    elif not is_cum_data and data_rt_nv:
        source_nv_rows = data_rt_nv

    parsed_nv_rt = []
    if source_nv_rows and isinstance(source_nv_rows, list) and len(source_nv_rows) > 0:
        for row in source_nv_rows:
            m_nv = str(get_key_val(row, "mã nv", "ma_nv", "user", "rowcode", default="")).strip()
            if not m_nv or m_nv.lower() == "online" or m_nv == "18001060":
                continue
            t_nv = get_key_val(row, "tên nhân viên", "tên nv", "ten_nv", "rowname", "employeeName", default="") or ""

            raw_dt = parse_number(get_key_val(row, "doanh thu quy đổi", "revenue_kfactor", "dt quy đổi", "Doanh thu Quy đổi", default=0))
            if abs(raw_dt) >= 1000000:
                dt_nv = raw_dt / 1000000.0
            else:
                dt_nv = raw_dt

            raw_thuc = parse_number(get_key_val(row, "doanh thu thực", "doanh thu", "revenue", default=0))
            if abs(raw_thuc) >= 1000000:
                dt_thuc_nv = raw_thuc / 1000000.0
            else:
                dt_thuc_nv = raw_thuc
            if dt_thuc_nv <= 0:
                dt_thuc_nv = dt_nv

            sl_nv = parse_number(get_key_val(row, "số lượng", "soluong", "quantity", "quantity_RT", default=0))

            # Lệnh RT1: Ẩn những nhân viên hoàn toàn không phát sinh số (bằng 0), doanh thu âm (do trả hàng) vẫn hiển thị
            if dt_nv == 0 and sl_nv == 0:
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

            parsed_nv_rt.append({
                "ma_nv": m_nv,
                "clean_name_lower": clean_name.lower().strip() if clean_name else "",
                "name": disp_name,
                "sl": sl_nv,
                "dt": dt_nv,
                "dt_thuc": dt_thuc_nv,
                "target": target_nv,
                "ht": ht_nv
            })

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
                dt_tr_str = f"{item['dt']:.1f}".rstrip('0').rstrip('.') if item['dt'] == int(item['dt']) else f"{item['dt']:.1f}"
                tg_tr_str = f"{item['target']:.1f}".rstrip('0').rstrip('.') if item['target'] == int(item['target']) else f"{item['target']:.1f}"

                # Còn lại
                con_lai = item['target'] - item['dt']
                if con_lai <= 0 and item['target'] > 0:
                    con_lai_str = "Đạt"
                    con_color = "#059669"
                else:
                    val_cl = max(0, con_lai)
                    con_lai_str = f"{val_cl:.1f}".rstrip('0').rstrip('.') if val_cl == int(val_cl) else f"{val_cl:.1f}"
                    con_color = "#ef4444"

                ht_str = f"{item['ht']*100:.1f}%".rstrip('0').rstrip('.') if (item['ht']*100) == int(item['ht']*100) else f"{item['ht']*100:.1f}%"

                row_vals = [rank_str, item["name"], sl_str, dt_tr_str, tg_tr_str, con_lai_str, ht_str]
                row_colors = [
                    "#d97706" if idx < 3 else "#64748b",
                    "#0f172a",
                    "#475569",
                    "#ef4444" if item["dt"] < 0 else "#059669",
                    "#475569",
                    con_color,
                    get_color_class(item["ht"])
                ]
                nv_card_contents.append(make_table_row(row_vals, nv_weights, nv_aligns, row_colors, bold=(idx < 3)))
                if idx < len(parsed_nv_rt) - 1:
                    nv_card_contents.append({"type": "separator", "color": "#f1f5f9", "margin": "xs"})

            # Thêm dòng TỔNG ở cuối bảng Thứ Hạng Doanh Thu Nhân Viên (Đồng bộ chuẩn Ảnh 2)
            tot_nv_sl = sum(it["sl"] for it in parsed_nv_rt)
            tot_nv_dt = sum(it["dt"] for it in parsed_nv_rt)
            tot_nv_tg = sum(it["target"] for it in parsed_nv_rt)
            tot_nv_cl = max(0, tot_nv_tg - tot_nv_dt)
            tot_nv_ht = (tot_nv_dt / tot_nv_tg) if tot_nv_tg > 0 else 0.0

            tot_dt_str = f"{tot_nv_dt:.1f}".rstrip('0').rstrip('.') if tot_nv_dt == int(tot_nv_dt) else f"{tot_nv_dt:.1f}"
            tot_tg_str = f"{tot_nv_tg:.1f}".rstrip('0').rstrip('.') if tot_nv_tg == int(tot_nv_tg) else f"{tot_nv_tg:.1f}"
            tot_cl_str = "Đạt" if tot_nv_dt >= tot_nv_tg else (f"{tot_nv_cl:.1f}".rstrip('0').rstrip('.') if tot_nv_cl == int(tot_nv_cl) else f"{tot_nv_cl:.1f}")
            tot_ht_str = f"{tot_nv_ht*100:.1f}%".rstrip('0').rstrip('.') if (tot_nv_ht*100) == int(tot_nv_ht*100) else f"{tot_nv_ht*100:.1f}%"

            nv_card_contents.append({"type": "separator", "color": "#cbd5e1", "margin": "xs"})
            tot_nv_row_vals = ["⭐", "TỔNG", fmt_num(tot_nv_sl), tot_dt_str, tot_tg_str, tot_cl_str, tot_ht_str]
            tot_nv_row_colors = [
                "#d97706",
                "#0f172a",
                "#0f172a",
                "#0284c7",
                "#475569",
                "#059669" if tot_nv_dt >= tot_nv_tg else "#ef4444",
                get_color_class(tot_nv_ht)
            ]
            nv_card_contents.append(make_table_row(tot_nv_row_vals, nv_weights, nv_aligns, tot_nv_row_colors, bold=True, bg_color="#f8fafc"))

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

    # Revenue Table Container Card (Bảng Chi Tiết Doanh Thu Hôm Nay - Tối ưu weights và bật wrap tránh che tên ngành hàng)
    table_card_contents = [
        {"type": "text", "text": "⚡ CHI TIẾT DOANH THU HÔM NAY", "size": "xxs", "color": "#0284c7", "weight": "bold", "margin": "xs"}
    ]
    headers = ["STT", "Ngành hàng", "SL", "DTQĐ", "Target", "%HT"]
    weights = [1, 5, 1, 2, 2, 2]
    aligns = ["start", "start", "center", "center", "center", "end"]
    wraps_bi = [False, True, False, False, False, False]
    table_card_contents.append(make_table_header(headers, weights, aligns, bg_color="#0284c7"))
    table_card_contents.append({"type": "separator", "color": "#cbd5e1", "margin": "xs"})

    for idx, b in enumerate(parsed_rt_bi):
        ty_trong_rt = (b["dt"] / rt_total * 100) if rt_total > 0 else 0.0
        vals = [idx+1, f"{b['name']} ({ty_trong_rt:.0f}%)", fmt_num(b["sl"]), fmt_num(b["dt"]), fmt_num(b["tg"]), f"{b['ht']*100:.0f}%"]
        colors = ["#64748b", "#0f172a", "#0f172a", "#ef4444" if b["dt"] < 0 else "#0284c7", "#475569", get_color_class(b["ht"])]
        table_card_contents.append(make_table_row(vals, weights, aligns, colors, wraps=wraps_bi))
        if idx < len(parsed_rt_bi) - 1:
            table_card_contents.append({"type": "separator", "color": "#f1f5f9", "margin": "xs"})

    tot_sl = sum(x["sl"] for x in parsed_rt_bi)
    totalHTCol2 = rt_total / rt_tTarget if rt_tTarget > 0 else 0.0
    tot_vals = ["⭐", "TỔNG CỘNG", fmt_num(tot_sl), fmt_num(rt_total), fmt_num(rt_tTarget), f"{totalHTCol2*100:.0f}%"]
    tot_colors = ["#ffffff", "#ffffff", "#ffffff", "#ffffff", "#ffffff", "#ffffff"]
    table_card_contents.append(make_table_row(tot_vals, weights, aligns, tot_colors, bold=True, bg_color="#f59e0b", wraps=wraps_bi))

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
        },
        "footer": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#ffffff",
            "paddingAll": "sm",
            "contents": [
                {
                    "type": "button",
                    "action": {
                        "type": "message",
                        "label": "👑 Xem Chi Tiết Doanh Thu NV",
                        "text": "RTNV"
                    },
                    "style": "primary",
                    "color": "#0284c7",
                    "height": "sm"
                }
            ]
        }
    }

    # Bảng Thi Đua Realtime P.2 (Chia 2 khu: Về đích ngày & Chưa về đích ngày - Chuẩn Ảnh 2 mới)
    headers_rt2 = ["#", "NH", "Đạt", "Còn", "MT", "%HT"]
    weights_rt2 = [1, 5, 2, 2, 2, 2]
    aligns_rt2 = ["center", "start", "center", "center", "center", "center"]
    wraps_rt2 = [False, True, False, False, False, False]

    body_contents_rt2 = []

    if td_done:
        done_table_rt = [
            {"type": "text", "text": f"🏆 NHÓM VỀ ĐÍCH NGÀY (1–{len(td_done)})", "size": "xxs", "color": "#15803d", "weight": "bold", "margin": "xs"},
            make_table_header(headers_rt2, weights_rt2, aligns_rt2, bg_color="#15803d"),
            {"type": "separator", "color": "#bbf7d0", "margin": "xs"}
        ]
        for idx, t in enumerate(td_done, start=1):
            unit_tag = "(SL)" if t.get("is_sl") else "(DT)"
            display_name = f"{t['name']} {unit_tag}"
            act_str = fmt_val_td(t['actual'], t.get('is_sl', False))
            cl_str = "🏆" if t['con_lai'] <= 0 else fmt_val_td(t['con_lai'], t.get('is_sl', False))
            tg_str = fmt_val_td(t['target'], t.get('is_sl', False))
            ht_str = f"{t['ht']*100:.0f}%"

            name_color = "#dc2626" if t.get("phan_loai") == 2.0 else "#0f172a"
            vals = [str(idx), display_name, act_str, cl_str, tg_str, ht_str]
            colors = ["#64748b", name_color, "#0284c7", "#16a34a" if cl_str == "🏆" else "#dc2626", "#475569", get_color_class(t["ht"])]
            if idx > 1:
                done_table_rt.append({"type": "separator", "color": "#e2e8f0", "margin": "xs"})
            done_table_rt.append(make_table_row(vals, weights_rt2, aligns_rt2, colors, wraps=wraps_rt2))

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
            {"type": "text", "text": f"🎯 NHÓM CHƯA VỀ ĐÍCH NGÀY ({len(td_done) + 1}–{len(td_done) + len(td_pending)})", "size": "xxs", "color": "#b91c1c", "weight": "bold", "margin": "xs"},
            make_table_header(headers_rt2, weights_rt2, aligns_rt2, bg_color="#0f766e"),
            {"type": "separator", "color": "#cbd5e1", "margin": "xs"}
        ]
        for idx, t in enumerate(td_pending, start=len(td_done) + 1):
            unit_tag = "(SL)" if t.get("is_sl") else "(DT)"
            display_name = f"{t['name']} {unit_tag}"
            act_str = fmt_val_td(t['actual'], t.get('is_sl', False))
            cl_str = "🏆" if t['con_lai'] <= 0 else fmt_val_td(t['con_lai'], t.get('is_sl', False))
            tg_str = fmt_val_td(t['target'], t.get('is_sl', False))
            ht_str = f"{t['ht']*100:.0f}%"

            name_color = "#dc2626" if t.get("phan_loai") == 2.0 else "#0f172a"
            vals = [str(idx), display_name, act_str, cl_str, tg_str, ht_str]
            colors = ["#64748b", name_color, "#0284c7", "#16a34a" if cl_str == "🏆" else "#dc2626", "#475569", get_color_class(t["ht"])]
            if idx > len(td_done) + 1:
                pending_table_rt.append({"type": "separator", "color": "#f1f5f9", "margin": "xs"})
            pending_table_rt.append(make_table_row(vals, weights_rt2, aligns_rt2, colors, wraps=wraps_rt2))

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
        # ===== DMX STT LIEN TUC 25/09/2026 =====
        # Bang 3 nay truoc day chi liet ke gach dau dong, KHONG co STT => nguoi doc khong biet
        # no nam o dau trong tong so nhom. Nay danh noi tiep sau bang 2 (vd 7..23).
        total_rt = len(td_done) + len(td_pending) + len(td_zero)
        zero_start = len(td_done) + len(td_pending) + 1
        # ===== HET DMX STT LIEN TUC =====
        for idx, t in enumerate(td_zero, start=zero_start):
            name_color = "#dc2626" if t.get("phan_loai") == 2.0 else "#475569"
            mt_val = fmt_val_td(t["target"], t.get("is_sl", False))
            item_text = f"{idx}. {t['name']} ({mt_val})"
            item_dict = {"type": "text", "text": item_text, "size": "xxs", "color": name_color, "wrap": True}
            if (idx - zero_start) % 2 == 0:
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
                {"type": "text", "text": f"💤 NHÓM CHƯA PHÁT SINH SỐ ({zero_start}–{total_rt})", "size": "xxs", "color": "#64748b", "weight": "bold", "margin": "xs"},
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

    # =========================================================================
    # THẺ 2: CHI TIẾT DOANH THU NHÂN VIÊN (THỰC BÁN THEO NGÀNH HÀNG - CHUẨN ẢNH)
    # =========================================================================
    data_rt_nv_detail = data.get("Data_Realtime_NV_NganhHang", [])
    staff_detail_map = {}
    if data_rt_nv_detail and isinstance(data_rt_nv_detail, list):
        for row in data_rt_nv_detail:
            m_id = str(get_key_val(row, "Mã NV", "ma_nv", "user", default="")).strip()
            store_group = str(get_key_val(row, "STOREGROUPNAME", default="")).strip()
            if not m_id and store_group:
                m_match = re.match(r"^(\d+)", store_group)
                if m_match:
                    m_id = m_match.group(1)
            s_name = str(get_key_val(row, "Tên Nhân Viên", "Tên NV", "ten_nv", default="")).strip()
            nganh_hang = str(get_key_val(row, "Ngành Hàng", "SALEGROUPNAME", "nganh_hang", default="")).strip()
            sl_detail = parse_number(get_key_val(row, "Số Lượng", "so_luong", default=0))
            raw_dt_thuc = parse_number(get_key_val(row, "Doanh Thu Thực", "Doanh Thu", "Doanh thu", "doanh_thu_thuc", default=0))
            dt_thuc = raw_dt_thuc / 1000000.0 if abs(raw_dt_thuc) >= 1000000 else raw_dt_thuc
            
            d_item = {"nganh_hang": nganh_hang, "sl": sl_detail, "dt_thuc": dt_thuc}
            if m_id:
                staff_detail_map.setdefault(m_id, []).append(d_item)
            if s_name:
                clean_sname = re.sub(r"^[0-9\s\-]+", "", s_name).lower().strip()
                if clean_sname:
                    staff_detail_map.setdefault(clean_sname, []).append(d_item)

    staff_cards_list = []
    if parsed_nv_rt:
        rank_styles = [
            {"badge": "🥇 TOP 1", "badge_bg": "#fef3c7", "badge_color": "#b45309", "card_border": "#f97316", "header_bg": "#fff7ed", "bar_color": "#0d9488"},
            {"badge": "🥈 TOP 2", "badge_bg": "#e0f2fe", "badge_color": "#0369a1", "card_border": "#0284c7", "header_bg": "#f0f9ff", "bar_color": "#10b981"},
            {"badge": "🥉 TOP 3", "badge_bg": "#dcfce7", "badge_color": "#15803d", "card_border": "#16a34a", "header_bg": "#f0fdf4", "bar_color": "#84cc16"},
        ]
        for idx, item in enumerate(parsed_nv_rt):
            st = rank_styles[idx] if idx < 3 else {
                "badge": f"#{idx+1}",
                "badge_bg": "#f1f5f9",
                "badge_color": "#475569",
                "card_border": "#cbd5e1",
                "header_bg": "#f8fafc",
                "bar_color": "#f59e0b" if item["ht"] >= 0.5 else "#ef4444"
            }

            # Còn lại & tiến độ
            con_lai = item["target"] - item["dt"]
            if con_lai <= 0 and item["target"] > 0:
                con_lai_text = "Đã đạt mục tiêu"
                con_color = "#16a34a"
            elif item["target"] <= 0:
                con_lai_text = "Chưa có MT"
                con_color = "#94a3b8"
            else:
                con_lai_text = f"Còn: {max(0, con_lai):.1f} Tr".replace(".", ",")
                con_color = "#ef4444"

            dt_str = f"{item['dt']:.1f}".replace(".", ",")
            tg_str = f"{item['target']:.1f}".replace(".", ",")
            ht_pct_str = f"{item['ht']*100:.1f}%".replace(".", ",")
            ht_color = "#16a34a" if item["ht"] >= 1.0 else ("#ef4444" if item["ht"] < 0 else "#ea580c")

            # Chi tiết ngành hàng của nhân viên này
            staff_details = staff_detail_map.get(item["ma_nv"]) or staff_detail_map.get(item.get("clean_name_lower", "")) or []
            cat_agg = {}
            total_thuc_qty = 0
            total_thuc_rev = 0.0
            for d in staff_details:
                nh = d["nganh_hang"] or "Khác"
                if nh not in cat_agg:
                    cat_agg[nh] = {"nganh_hang": nh, "sl": 0, "dt_thuc": 0.0}
                cat_agg[nh]["sl"] += int(d["sl"])
                cat_agg[nh]["dt_thuc"] += d["dt_thuc"]
                total_thuc_qty += int(d["sl"])
                total_thuc_rev += d["dt_thuc"]
            
            detail_list = sorted(cat_agg.values(), key=lambda x: x["dt_thuc"], reverse=True)
            if not detail_list and item["sl"] > 0:
                total_thuc_qty = int(item["sl"])
                total_thuc_rev = item["dt"]

            # Tiết lưu khi cần: chỉ giữ top N ngành hàng có doanh thu thực cao nhất.
            # detail_limit = None -> giữ nguyên như cũ (KHÔNG cắt gì).
            so_nganh_bi_luoc = 0
            if detail_limit is not None and len(detail_list) > detail_limit:
                so_nganh_bi_luoc = len(detail_list) - detail_limit
                detail_list = detail_list[:detail_limit]

            dong_xem_them = {
                "type": "box",
                "layout": "horizontal",
                "paddingTop": "xs",
                "paddingBottom": "xs",
                "contents": [
                    {"type": "text",
                     "text": f"… và {so_nganh_bi_luoc} ngành hàng khác",
                     "size": "xxs", "color": "#94a3b8", "flex": 1}
                ]
            }

            # Danh sách từng ngành hàng (Giao diện dòng trực quan, không cắt chữ, không lặp tiêu đề)
            item_rows = []
            if detail_list:
                for cat_idx, cat in enumerate(detail_list):
                    item_rows.append({
                        "type": "box",
                        "layout": "horizontal",
                        "alignItems": "center",
                        "paddingTop": "xs",
                        "paddingBottom": "xs",
                        "contents": [
                            {"type": "text", "text": cat["nganh_hang"], "size": "xs", "color": "#1e293b", "flex": 5, "wrap": True},
                            {
                                "type": "box",
                                "layout": "vertical",
                                "backgroundColor": "#ffedd5",
                                "paddingStart": "xs",
                                "paddingEnd": "xs",
                                "cornerRadius": "xs",
                                "flex": 0,
                                "contents": [
                                    {"type": "text", "text": f"{cat['sl']} SP", "size": "xxs", "color": "#c2410c", "weight": "bold"}
                                ]
                            },
                            {"type": "text", "text": f"{cat['dt_thuc']:.1f} Tr".replace(".", ","), "size": "xs", "color": "#0f172a", "weight": "bold", "align": "end", "flex": 3}
                        ]
                    })
                    if cat_idx < len(detail_list) - 1:
                        item_rows.append({"type": "separator", "color": "#f8fafc", "margin": "xs"})
                if so_nganh_bi_luoc > 0:
                    item_rows.append(dong_xem_them)
            elif so_nganh_bi_luoc > 0:
                # Bị tiết lưu xuống 0 dòng — báo đúng sự thật, không hiện câu "chưa có phát sinh".
                item_rows.append(dong_xem_them)
            else:
                item_rows.append({
                    "type": "box",
                    "layout": "horizontal",
                    "paddingAll": "xs",
                    "contents": [
                        {"type": "text", "text": "Chưa có phát sinh chi tiết ngành hàng thực bán", "size": "xxs", "color": "#94a3b8", "align": "center", "flex": 1}
                    ]
                })

            # Từng Thẻ Nhân Viên Độc Lập, Nổi Bật, Dễ Phân Biệt
            staff_card = {
                "type": "box",
                "layout": "vertical",
                "backgroundColor": "#ffffff",
                "borderColor": st["card_border"],
                "borderWidth": "1.5px",
                "cornerRadius": "md",
                "margin": "md",
                "contents": [
                    # Phần 1: Header Thẻ Nhân Viên (Có nền màu nhận diện riêng)
                    {
                        "type": "box",
                        "layout": "vertical",
                        "backgroundColor": st["header_bg"],
                        "paddingAll": "sm",
                        "contents": [
                            # Hàng 1: Badge Top + Tên + DT Quy đổi
                            {
                                "type": "box",
                                "layout": "horizontal",
                                "alignItems": "center",
                                "contents": [
                                    {
                                        "type": "box",
                                        "layout": "vertical",
                                        "backgroundColor": st["badge_bg"],
                                        "paddingStart": "sm",
                                        "paddingEnd": "sm",
                                        "paddingTop": "xs",
                                        "paddingBottom": "xs",
                                        "cornerRadius": "sm",
                                        "flex": 0,
                                        "contents": [
                                            {"type": "text", "text": st["badge"], "size": "xxs", "weight": "bold", "color": st["badge_color"]}
                                        ]
                                    },
                                    {"type": "text", "text": item["name"], "size": "sm", "weight": "bold", "color": "#0f172a", "margin": "sm", "flex": 1},
                                    {"type": "text", "text": f"{dt_str} Tr", "size": "sm", "weight": "bold", "color": "#ef4444" if item["dt"] < 0 else "#0284c7", "align": "end", "flex": 0}
                                ]
                            },
                            # Hàng 2: Mục tiêu, Còn lại, % Đạt (tránh trùng lặp, không lặp lại số DT và %)
                            {
                                "type": "box",
                                "layout": "horizontal",
                                "margin": "xs",
                                "alignItems": "center",
                                "contents": [
                                    {"type": "text", "text": f"Mục tiêu: {tg_str} Tr", "size": "xxs", "color": "#64748b", "flex": 0},
                                    {"type": "text", "text": "•", "size": "xxs", "color": "#cbd5e1", "margin": "xs", "flex": 0},
                                    {"type": "text", "text": con_lai_text, "size": "xxs", "color": con_color, "weight": "bold" if con_lai <= 0 else "regular", "margin": "xs", "flex": 1},
                                    {"type": "text", "text": ht_pct_str, "size": "xs", "weight": "bold", "color": ht_color, "align": "end", "flex": 0}
                                ]
                            },
                            # Hàng 3: Thanh tiến độ trực quan full-width (bỏ % trùng lặp ở đuôi thanh)
                            {
                                "type": "box",
                                "layout": "vertical",
                                "margin": "xs",
                                "height": "5px",
                                "backgroundColor": "#e2e8f0",
                                "cornerRadius": "sm",
                                "contents": [
                                    {
                                        "type": "box",
                                        "layout": "vertical",
                                        "height": "5px",
                                        "backgroundColor": st["bar_color"],
                                        "cornerRadius": "sm",
                                        "width": f"{min(100, max(3, round(item['ht'] * 100)))}%",
                                        "contents": [{"type": "filler"}]
                                    }
                                ]
                            }
                        ]
                    },
                    {"type": "separator", "color": "#e2e8f0"},
                    # Phần 2: Danh Sách Ngành Hàng Thực Bán
                    {
                        "type": "box",
                        "layout": "vertical",
                        "backgroundColor": "#ffffff",
                        "paddingAll": "sm",
                        "contents": item_rows + [
                            {"type": "separator", "color": "#e2e8f0", "margin": "xs"},
                            # Hàng tóm tắt cuối thẻ nhân viên (bỏ icon)
                            {
                                "type": "box",
                                "layout": "horizontal",
                                "margin": "xs",
                                "contents": [
                                    {"type": "text", "text": f"Tổng thực bán: {total_thuc_qty} SP", "size": "xxs", "color": "#64748b", "flex": 1},
                                    {"type": "text", "text": f"{total_thuc_rev:.1f} Tr thực".replace(".", ","), "size": "xxs", "color": "#0284c7", "weight": "bold", "align": "end", "flex": 1}
                                ]
                            }
                        ]
                    }
                ]
            }
            staff_cards_list.append(staff_card)

    flex_bubbles_rt_nv = []
    if not staff_cards_list:
        empty_bubble = {
            "type": "bubble",
            "size": "mega",
            "header": {
                "type": "box",
                "layout": "vertical",
                "backgroundColor": "#ea580c",
                "paddingAll": "md",
                "contents": [
                    {"type": "text", "text": "👑 CHI TIẾT DTNV", "weight": "bold", "size": "sm", "color": "#ffffff", "align": "center", "wrap": True},
                    {"type": "text", "text": f"🕒 Cập nhật: {now_str} • 0 Nhân sự có số", "size": "xxs", "color": "#ffedd5", "align": "center", "margin": "xs"}
                ]
            },
            "body": {
                "type": "box",
                "layout": "vertical",
                "backgroundColor": "#f1f5f9",
                "paddingAll": "md",
                "contents": [
                    {
                        "type": "box",
                        "layout": "vertical",
                        "backgroundColor": "#ffffff",
                        "borderColor": "#e2e8f0",
                        "borderWidth": "1px",
                        "cornerRadius": "md",
                        "paddingAll": "md",
                        "contents": [
                            {"type": "text", "text": "Chưa có nhân viên phát sinh doanh thu trong ngày.", "size": "xs", "color": "#64748b", "align": "center"}
                        ]
                    }
                ]
            }
        }
        flex_bubbles_rt_nv.append(empty_bubble)
    else:
        # Tách thành các thẻ nhỏ thông minh (tối đa 5 nhân sự/thẻ hoặc khi dung lượng chạm 23KB)
        # Giúp chia chuẩn: Thẻ 3 (Top 1 - 5: ~22KB) và Thẻ 4 (Top 6 - 9: ~15KB)
        # Hoàn toàn miễn nhiễm với lỗi 30KB của LINE!
        chunks = []
        cur_chunk = []
        cur_estimated_bytes = 1200
        for sc in staff_cards_list:
            sc_bytes = len(json.dumps(sc, ensure_ascii=False).encode('utf-8'))
            if cur_chunk and (len(cur_chunk) >= 5 or cur_estimated_bytes + sc_bytes > 23000):
                chunks.append(cur_chunk)
                cur_chunk = [sc]
                cur_estimated_bytes = 1200 + sc_bytes
            else:
                cur_chunk.append(sc)
                cur_estimated_bytes += sc_bytes
        if cur_chunk:
            chunks.append(cur_chunk)

        total_chunks = len(chunks)
        accum_count = 0
        for c_idx, chk in enumerate(chunks):
            start_rank = accum_count + 1
            end_rank = accum_count + len(chk)
            accum_count += len(chk)
            
            if total_chunks > 1:
                part_title = f" (P.{c_idx+1}/{total_chunks})"
                hdr_sub = f"🕒 Cập nhật: {now_str} • Top {start_rank} - {end_rank} ({len(chk)}/{len(staff_cards_list)} NV)"
                banner_txt = f"👑 Chi tiết ngành hàng thực bán (Top {start_rank} - {end_rank} trên {len(staff_cards_list)} NV có số hôm nay):"
            else:
                part_title = ""
                hdr_sub = f"🕒 Cập nhật: {now_str} • {len(staff_cards_list)} Nhân sự có số"
                banner_txt = f"👑 Chi tiết ngành hàng thực bán của từng nhân viên ({len(staff_cards_list)} nhân sự có số hôm nay):"

            b_contents = [
                {
                    "type": "box",
                    "layout": "vertical",
                    "backgroundColor": "#fff7ed",
                    "borderColor": "#fed7aa",
                    "borderWidth": "1px",
                    "cornerRadius": "md",
                    "paddingAll": "sm",
                    "margin": "xs",
                    "contents": [
                        {
                            "type": "text",
                            "text": banner_txt,
                            "size": "xxs",
                            "color": "#c2410c",
                            "weight": "bold",
                            "wrap": True
                        }
                    ]
                }
            ] + chk

            bubble_nv = {
                "type": "bubble",
                "size": "mega",
                "header": {
                    "type": "box",
                    "layout": "vertical",
                    "backgroundColor": "#ea580c",
                    "paddingAll": "md",
                    "contents": [
                        {"type": "text", "text": f"👑 CHI TIẾT DTNV{part_title}", "weight": "bold", "size": "sm", "color": "#ffffff", "align": "center", "wrap": True},
                        {"type": "text", "text": hdr_sub, "size": "xxs", "color": "#ffedd5", "align": "center", "margin": "xs"}
                    ]
                },
                "body": {
                    "type": "box",
                    "layout": "vertical",
                    "backgroundColor": "#f1f5f9",
                    "paddingAll": "sm",
                    "contents": b_contents
                }
            }
            flex_bubbles_rt_nv.append(bubble_nv)

    return [flex_bubble_rt1, flex_bubble_rt2] + flex_bubbles_rt_nv

def build_help_commands_flex():
    """
    Tạo Flex Message Hướng Dẫn TOÀN BỘ Danh Sách Câu Lệnh Hỗ Trợ (#lenh)
    """
    commands_data = [
        # --- BÁO CÁO DOANH THU & THI ĐUA ---
        {"cmd": "LK1", "desc": "Báo cáo Lũy kế Doanh thu & Ngành hàng thi đua tháng (Flex P.1 & P.2)", "color": "#1e40af"},
        {"cmd": "RT1", "desc": "Báo cáo Realtime Doanh thu & Thứ hạng NV (tinh gọn, kèm nút bấm xem chi tiết)", "color": "#0284c7"},
        {"cmd": "RTNV", "desc": "Xem chi tiết ngành hàng thực bán của từng nhân viên (kích hoạt từ nút RT1)", "color": "#0284c7"},
        {"cmd": "RT2", "desc": "Báo cáo Realtime Theo dõi Nhóm Ngành Hàng Gộp (Phụ kiện, Bảo hiểm thợ...)", "color": "#0284c7"},
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
        {"cmd": "việc", "desc": "Giao việc — mỗi việc 1 dòng bắt đầu bằng dấu gạch, dấu @ đặt đâu cũng được. Dòng đầu CHỈ CẦN chữ việc, không bắt buộc tag. VD:\nviệc\n- Giá Tivi @tuấn @linh\n- giá tủ lạnh @linh\nMuốn giao cho CẢ NHÓM thì ghi tag ở dòng đầu: việc @all\nGõ nhanh 1 dòng: việc @Khánh: dọn kho", "color": "#8b5cf6"},
        {"cmd": "@all @nv @pg @st", "desc": "Đích giao việc: @all = ĐỦ cả siêu thị (PG+NV+QL+TC, hiện 25 người, không lọc theo lịch hôm nay) · @nv = nhân viên · @pg = PG · @st = NV+QL+TC. Nhóm lấy từ sheet schedules cột E/F/G. Gọi đích danh (@tuan, @156494) thì ghi nguyên văn, không cần có trong sheet.", "color": "#7c3aed"},
        {"cmd": "hủy việc", "desc": "Hủy nhóm việc vừa giao trong hôm nay (hoặc bấm nút 🗑 Hủy nhóm việc trên thẻ việc)", "color": "#dc2626"},
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


# =====================================================================
# BÁO CÁO REALTIME THEO DÕI NHÓM NGÀNH HÀNG GỘP (LỆNH RT2)
# =====================================================================

CG_DETAIL_CAT_MAP = {
    "pk_lap_dat": ["khung treo", "giá đỡ", "gia do", "giá treo", "chân đế", "chan de", "vật tư lắp đặt", "vat tu lap dat"],
    "pk_tien_ich": ["sạc", "sac", "cáp", "cap", "dự phòng", "du phong", "pin", "thẻ nhớ", "the nho", "usb", "chuột", "chuot", "bàn phím", "ban phim", "tai nghe", "loa bluetooth", "loa vi tính", "balo", "túi chống sốc"],
    "pk_trang_tri": ["ốp lưng", "op lung", "miếng dán", "mieng dan", "kính cường lực", "cuong luc", "dán màn hình", "dan man hinh"],
    "pk_it": ["router", "wifi", "hub", "thiết bị mạng"],
    "gia_dung": ["hút bụi", "nồi cơm", "nồi chiên", "quạt", "lọc nước", "bếp", "máy xay", "bàn ủi", "bàn là", "ấm siêu tốc", "lò vi sóng", "lò nướng", "bình thủy"],
    "dien_lanh": ["máy lạnh", "điều hòa", "panasonic", "daikin"],
    "may_giat": ["máy giặt", "máy sấy", "máy rửa chén", "rửa bát"]
}

def clean_custom_cat(name):
    if not name:
        return ""
    s = str(name).lower().strip()
    s = re.sub(r"^t\d{1,2}\s*-\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"^nnh\s+", "", s, flags=re.IGNORECASE)
    s = re.sub(r"^thi\s*đua\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def is_custom_cat_specific_match(row_name, target_cat):
    if not row_name or not target_cat:
        return False
    r_clean = clean_custom_cat(row_name)
    t_clean = clean_custom_cat(target_cat)
    if r_clean == t_clean:
        return True

    is_pk_target = "phụ kiện" in t_clean or "pk" in t_clean
    is_gd_target = "gia dụng" in t_clean or "gd" in t_clean
    is_pk_row = "phụ kiện" in r_clean or "pk" in r_clean
    is_gd_row = "gia dụng" in r_clean or "gd" in r_clean

    if is_pk_target and is_gd_row and not is_pk_row:
        return False
    if is_gd_target and is_pk_row and not is_gd_target:
        return False

    if "trả chậm" not in t_clean and "credit" not in t_clean:
        if re.search(r"trả chậm|credit|fecredit|homecredit|shinhan|samsung finance|ngân hàng|nạp\s*-\s*rút", r_clean, re.IGNORECASE):
            return False

    sub_words = [
        "lắp đặt", "lap dat", "tiện ích", "tien ich", "trang trí", "trang tri",
        "it - nhóm khác", "công nghệ", "cong nghe", "_ict", "ict", "_ce", "ce", "tổng", "tong"
    ]
    target_subs = [w for w in sub_words if w in t_clean]
    row_subs = [w for w in sub_words if w in r_clean]

    if target_subs:
        has_same = any(w in r_clean for w in target_subs)
        has_conflict = any(w not in target_subs for w in row_subs)
        if has_same and not has_conflict:
            return True
        return False

    if row_subs:
        if t_clean in r_clean:
            return True
        return False

    return (t_clean in r_clean) or (r_clean in t_clean)

def is_custom_cat_matched(prog_name, selected_cats):
    if not prog_name or not selected_cats or not isinstance(selected_cats, list):
        return False
    clean_prog = clean_custom_cat(prog_name)
    is_tc = any("trả chậm" in clean_custom_cat(c) or "credit" in clean_custom_cat(c) for c in selected_cats)
    if not is_tc:
        if re.search(r"trả chậm|credit|fecredit|homecredit|shinhan|samsung finance|ngân hàng|nạp\s*-\s*rút", clean_prog, re.IGNORECASE):
            return False

    if any(is_custom_cat_specific_match(prog_name, cat) for cat in selected_cats):
        return True

    has_pk = any("phụ kiện" in clean_custom_cat(c) or "pk" in clean_custom_cat(c) or "lắp đặt" in clean_custom_cat(c) or "tiện ích" in clean_custom_cat(c) or "trang trí" in clean_custom_cat(c) for c in selected_cats)
    if has_pk:
        if "phụ kiện" in clean_prog or "pk" in clean_prog:
            return True
        has_lap_dat = any("lắp đặt" in clean_custom_cat(c) or "lap dat" in clean_custom_cat(c) for c in selected_cats)
        if has_lap_dat and any(k in clean_prog for k in CG_DETAIL_CAT_MAP["pk_lap_dat"]):
            return True
        has_tien_ich = any("tiện ích" in clean_custom_cat(c) or "tien ich" in clean_custom_cat(c) for c in selected_cats)
        if has_tien_ich and any(k in clean_prog for k in CG_DETAIL_CAT_MAP["pk_tien_ich"]):
            return True
        has_trang_tri = any("trang trí" in clean_custom_cat(c) or "trang tri" in clean_custom_cat(c) for c in selected_cats)
        if has_trang_tri and any(k in clean_prog for k in CG_DETAIL_CAT_MAP["pk_trang_tri"]):
            return True
        all_pk = CG_DETAIL_CAT_MAP["pk_lap_dat"] + CG_DETAIL_CAT_MAP["pk_tien_ich"] + CG_DETAIL_CAT_MAP["pk_trang_tri"] + CG_DETAIL_CAT_MAP["pk_it"]
        if any(k in clean_prog for k in all_pk):
            return True

    all_bh = all("bảo hiểm" in clean_custom_cat(c) or "thợ đmx" in clean_custom_cat(c) for c in selected_cats)
    if all_bh and ("bảo hiểm" in clean_prog or "thợ đmx" in clean_prog):
        return True

    return False


def build_custom_groups_flex(detail_limit=None):
    """
    Xây dựng danh sách Bubble Cards cho từng Nhóm Ngành Hàng Gộp (Lệnh RT2).
    Mỗi nhóm là 1 Bubble chuẩn phong cách RT1, bảng 6 cột, nền trắng, không có đánh giá.
    """
    data = get_dashboard_data("Config_Custom_Groups,Data_Realtime_BI,Data_Realtime_ThiDua,Data_Realtime_NV_NganhHang,Data_BI,Data_ThiDua,Config_ThiDua")
    custom_groups = data.get("Config_Custom_Groups", [])

    # Nếu chưa có trong DB, nạp 2 nhóm chuẩn mặc định của siêu thị Savico
    if not custom_groups or not isinstance(custom_groups, list):
        custom_groups = [
            {
                "id": "group_pk",
                "name": "Phụ kiện",
                "unit": "DT",
                "target": 1717,
                "categories": ["Phụ kiện lắp đặt", "Phụ kiện tiện ích", "Phụ kiện trang trí"]
            },
            {
                "id": "group_bh",
                "name": "Bảo hiểm thợ",
                "unit": "DT",
                "target": 288,
                "categories": ["T10 - BẢO HIỂM THỢ ĐMX_CE", "T10 - BẢO HIỂM THỢ ĐMX_ICT"]
            }
        ]

    tz = pytz.timezone('Asia/Ho_Chi_Minh')
    now = datetime.now(tz)
    now_str = now.strftime("%H:%M")

    current_day = now.day
    days_in_month = (datetime(now.year, now.month + 1, 1) - datetime(now.year, now.month, 1)).days if now.month < 12 else 31
    days_passed = days_in_month if current_day == 1 else current_day - 1
    days_remaining = max(1, days_in_month - days_passed)
    is_weekend = now.weekday() in [5, 6]

    bi_rows = data.get("Data_BI", [])
    rt_bi_rows = data.get("Data_Realtime_BI", [])
    td_rows = data.get("Data_ThiDua", [])
    rt_td_rows = data.get("Data_Realtime_ThiDua", [])
    rt_nv_detail = data.get("Data_Realtime_NV_NganhHang", [])
    config_rows = data.get("Config_ThiDua", [])

    # Bảng màu chuẩn phong cách RT1:
    # Nhóm 1: #0284c7 (Xanh dương RT1), Nhóm 2: #0f766e (Xanh ngọc / Teal RT1)
    THEME_COLORS = ["#0284c7", "#0f766e", "#1d4ed8", "#0891b2"]
    SUB_COLORS = ["#e0f2fe", "#ccfbf1", "#dbeafe", "#cffafe"]

    bubbles = []

    for idx, grp in enumerate(custom_groups):
        theme_color = THEME_COLORS[idx % len(THEME_COLORS)]
        sub_color = SUB_COLORS[idx % len(SUB_COLORS)]

        grp_name = grp.get("name", f"Nhóm {idx+1}")
        categories = grp.get("categories", [])
        target_month = parse_number(grp.get("target", 0))

        sub_cats = []
        total_hn = 0.0
        total_lk = 0.0

        for cat in categories:
            hn_val = 0.0
            lk_val = 0.0

            # a. Hôm nay từ Data_Realtime_BI
            for r in rt_bi_rows:
                nh = get_key_val(r, "Nhóm Ngành Hàng", "nhóm ngành hàng", "Ngành hàng", "salegroupmastername", default=None)
                if nh and is_custom_cat_specific_match(nh, cat):
                    hn_val += max(
                        parse_number(get_key_val(r, "revenue_KFactor_RT")),
                        parse_number(get_key_val(r, "revenue_RT")),
                        parse_number(get_key_val(r, "Doanh thu Quy đổi", "Doanh thu"))
                    )

            # b. Hôm nay từ Data_Realtime_ThiDua
            if hn_val == 0:
                for r in rt_td_rows:
                    nh = get_key_val(r, "maingroupname", "main group name", "nhóm ngành hàng", "programname", default=None)
                    if nh and is_custom_cat_specific_match(nh, cat):
                        hn_val += max(
                            parse_number(get_key_val(r, "revenue_KFactor_RT")),
                            parse_number(get_key_val(r, "revenue_RT")),
                            parse_number(get_key_val(r, "doanh thu", "revenue", "value_compe"))
                        )

            # c. Hôm nay từ NV Detail
            if hn_val == 0:
                for r in rt_nv_detail:
                    nh = str(get_key_val(r, "Ngành Hàng", "SALEGROUPNAME", "nganh_hang", default="")).strip()
                    if nh and is_custom_cat_specific_match(nh, cat):
                        raw_rev = parse_number(get_key_val(r, "Doanh Thu Thực", "Doanh Thu", "Doanh thu", "doanh_thu_thuc", default=0))
                        if abs(raw_rev) >= 1000000:
                            raw_rev /= 1000000.0
                        hn_val += raw_rev

            # d. Lũy kế từ Data_BI
            for r in bi_rows:
                nh = get_key_val(r, "Nhóm Ngành Hàng", "nhóm ngành hàng", "Ngành hàng", "salegroupmastername", default=None)
                if nh and is_custom_cat_specific_match(nh, cat):
                    lk_val += max(
                        parse_number(get_key_val(r, "Doanh thu Quy đổi", "doanh thu quy đổi")),
                        parse_number(get_key_val(r, "Doanh thu", "doanh thu"))
                    )

            # e. Lũy kế từ Data_ThiDua
            if lk_val == 0:
                for r in td_rows:
                    nh = get_key_val(r, "maingroupname", "nhóm ngành hàng", "programname", default=None)
                    if nh and is_custom_cat_specific_match(nh, cat):
                        lk_val += max(
                            parse_number(get_key_val(r, "doanh thu", "revenue")),
                            parse_number(get_key_val(r, "thực hiện", "value_compe"))
                        )

            sub_cats.append({"name": cat, "hn": hn_val, "lk": lk_val})
            total_hn += hn_val
            total_lk += lk_val

        # Tính MT ngày của nhóm theo logic chuẩn RT1
        target_co_dinh = target_month / days_in_month if days_in_month > 0 else 0.0
        target_bu_tru = max(0.0, (target_month - total_lk) / days_remaining) if days_remaining > 0 else 0.0
        group_mt_day = target_co_dinh * 2 if is_weekend else max(target_co_dinh, target_bu_tru)
        if group_mt_day <= 0 and target_month > 0:
            group_mt_day = target_month / 30.0

        group_con_lai = max(0.0, group_mt_day - total_hn)
        group_tien_do = (total_hn / group_mt_day * 100.0) if group_mt_day > 0 else (100.0 if total_hn > 0 else 0.0)

        # Phân bổ nhân viên từ Config_ThiDua
        staff_map = {}
        for r in config_rows:
            u = str(get_key_val(r, "user", "User", "mã nv", default="")).strip().upper()
            name = str(get_key_val(r, "họ và tên", "tên", "User-Họ và tên", default="")).strip()
            pct = parse_number(get_key_val(r, "tỷ lệ %", "% chia", default=0))
            ratio = pct if pct <= 1 else pct / 100.0
            if u and u not in staff_map and u not in ["UNDEFINED", "NULL"]:
                clean_name = name.split(" - ")[-1] if " - " in name else name
                clean_name = clean_name.lstrip("0123456789 -").strip()
                last_name = clean_name.split()[-1] if clean_name else ""
                disp_name = f"{last_name} - {u}" if last_name else u
                staff_map[u] = {"id": u, "name": disp_name, "ratio": ratio, "hn": 0.0}

        # Xây dựng bảng tra cứu Hệ số K (KFactor) từ Data_Realtime_BI
        k_factor_map = {}
        for r in rt_bi_rows:
            nh_bi = str(get_key_val(r, "Nhóm Ngành Hàng", "nhóm ngành hàng", "Ngành hàng", "salegroupmastername", default="")).strip().lower()
            r_thuc = parse_number(get_key_val(r, "revenue_RT", default=0))
            r_qd = parse_number(get_key_val(r, "revenue_KFactor_RT", "Doanh thu Quy đổi", default=0))
            if r_thuc > 0 and r_qd > 0:
                k_factor_map[nh_bi] = r_qd / r_thuc

        def resolve_item_k_factor(nh_name):
            c = str(nh_name).lower().strip()
            for k_name, factor in k_factor_map.items():
                if k_name in c or c in k_name:
                    return factor
            if any(w in c for w in ['lắp đặt', 'khung treo', 'giá đỡ']):
                return k_factor_map.get('phụ kiện lắp đặt', 3.3699)
            if any(w in c for w in ['trang trí', 'ốp lưng', 'dán', 'kính']):
                return k_factor_map.get('phụ kiện trang trí', 3.3699)
            if any(w in c for w in ['phụ kiện', 'tiện ích', 'sạc', 'cáp', 'tai nghe', 'loa', 'pin']):
                return k_factor_map.get('phụ kiện tiện ích', 3.3700)
            return 1.0

        # Doanh thu hôm nay của từng nhân viên (đã quy đổi theo KFactor để khớp chuẩn với Target và Tổng nhóm)
        is_unit_sl = str(grp.get("unit", "")).upper() == "SL"
        for r in rt_nv_detail:
            u = str(get_key_val(r, "Mã NV", "ma_nv", "user", default="")).strip().upper()
            sg = str(get_key_val(r, "STOREGROUPNAME", default=""))
            if not u and sg:
                m = re.match(r"^(\d+)", sg)
                if m:
                    u = m.group(1)
            if not u or u in ["ONLINE", "18001060"] or u not in staff_map:
                continue
            nh = str(get_key_val(r, "Ngành Hàng", "SALEGROUPNAME", "nganh_hang", default="")).strip()
            if is_custom_cat_matched(nh, categories):
                rev = parse_number(get_key_val(r, "Doanh Thu Thực", "Doanh Thu", "Doanh thu", "doanh_thu_thuc", default=0))
                if abs(rev) >= 1000000:
                    rev /= 1000000.0
                k_factor = 1.0 if is_unit_sl else resolve_item_k_factor(nh)
                staff_map[u]["hn"] += (rev * k_factor)

        # Danh sách nhân viên và tính chỉ số
        staff_list = []
        tot_staff_mt = 0.0
        tot_staff_hn = 0.0

        for u, s in staff_map.items():
            s_mt = group_mt_day * s["ratio"]
            s_hn = s["hn"]
            s_cl = s_mt - s_hn
            s_pct = (s_hn / s_mt * 100.0) if s_mt > 0 else (100.0 if s_hn > 0 else 0.0)
            tot_staff_mt += s_mt
            tot_staff_hn += s_hn
            staff_list.append({
                "name": s["name"],
                "mt": s_mt,
                "hn": s_hn,
                "cl": s_cl,
                "pct": s_pct
            })

        # Sắp xếp nhân viên theo doanh thu thực hiện hôm nay giảm dần
        staff_list.sort(key=lambda x: x["hn"], reverse=True)

        tot_staff_cl = tot_staff_mt - tot_staff_hn
        tot_staff_pct = (tot_staff_hn / tot_staff_mt * 100.0) if tot_staff_mt > 0 else (100.0 if tot_staff_hn > 0 else 0.0)

        # XÂY DỰNG BUBBLE CARD (CHUẨN LINE FLEX V2)
        # 1. Header Box
        header_box = {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": theme_color,
            "paddingAll": "md",
            "contents": [
                {"type": "text", "text": f"🎯 {grp_name.upper()}", "weight": "bold", "size": "md", "color": "#ffffff", "align": "center"},
                {"type": "text", "text": f"Mục tiêu tháng: {fmt_num(target_month)} TR • Cập nhật: {now_str}", "size": "xxs", "color": sub_color, "align": "center", "margin": "xs"}
            ]
        }

        # 2. Hộp Phân Ngành Gộp
        sub_cat_boxes = []
        for sc in sub_cats:
            sub_cat_boxes.append({
                "type": "box",
                "layout": "horizontal",
                "margin": "xs",
                "contents": [
                    {"type": "text", "text": f"• {sc['name']}:", "size": "xxs", "color": "#334155", "weight": "bold", "flex": 6, "wrap": True},
                    {"type": "text", "text": f"HN: {sc['hn']:.2f}", "size": "xxs", "color": "#059669", "weight": "bold", "flex": 3, "align": "end"},
                    {"type": "text", "text": f"| LK: {fmt_num(sc['lk'])}", "size": "xxs", "color": "#64748b", "flex": 3, "align": "end"}
                ]
            })

        box_categories = {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#f8fafc",
            "borderColor": "#e2e8f0",
            "borderWidth": "1px",
            "cornerRadius": "md",
            "paddingAll": "sm",
            "contents": [
                {"type": "text", "text": "📦 GỘP CÁC PHÂN NGÀNH:", "size": "xxs", "color": "#64748b", "weight": "bold"},
                {"type": "separator", "color": "#e2e8f0", "margin": "xs"},
                *sub_cat_boxes
            ]
        }

        # 3. Hộp KPI Tiến Độ
        pct_width = min(100, max(2, round(group_tien_do)))
        cl_text = "ĐÃ ĐẠT 🏆" if group_con_lai <= 0 and total_hn > 0 else f"{group_con_lai:.2f} TR"
        cl_color = "#059669" if group_con_lai <= 0 and total_hn > 0 else "#dc2626"

        box_kpi = {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#f8fafc",
            "borderColor": "#bae6fd" if theme_color == "#0284c7" else "#99f6e4",
            "borderWidth": "1px",
            "cornerRadius": "md",
            "paddingAll": "sm",
            "contents": [
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": "⏱ MT NGÀY:", "size": "xxs", "color": "#475569", "weight": "bold", "flex": 4},
                        {"type": "text", "text": f"{group_mt_day:.2f} TR", "size": "xxs", "color": theme_color, "weight": "bold", "flex": 3},
                        {"type": "text", "text": "⚡ THỰC HIỆN:", "size": "xxs", "color": "#475569", "weight": "bold", "flex": 5, "align": "end"},
                        {"type": "text", "text": f"{total_hn:.2f} TR", "size": "xxs", "color": "#059669", "weight": "bold", "flex": 4, "align": "end"}
                    ]
                },
                {
                    "type": "box",
                    "layout": "horizontal",
                    "margin": "xs",
                    "contents": [
                        {"type": "text", "text": "⏳ CÒN:", "size": "xxs", "color": "#475569", "weight": "bold", "flex": 3},
                        {"type": "text", "text": cl_text, "size": "xxs", "color": cl_color, "weight": "bold", "flex": 4},
                        {"type": "text", "text": "TIẾN ĐỘ:", "size": "xxs", "color": "#475569", "weight": "bold", "flex": 5, "align": "end"},
                        {"type": "text", "text": f"{group_tien_do:.1f}%", "size": "xxs", "color": get_color_class(group_tien_do / 100.0), "weight": "bold", "flex": 4, "align": "end"}
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
                            "backgroundColor": "#10b981" if group_tien_do >= 100 else theme_color,
                            "height": "6px",
                            "cornerRadius": "md",
                            "width": f"{pct_width}%",
                            "contents": [{"type": "filler"}]
                        }
                    ]
                }
            ]
        }

        # 4. Bảng Nhân Viên (6 Cột, BỎ ĐÁNH GIÁ, NỀN TRẮNG HOÀN TOÀN)
        table_weights = [1, 4, 2, 2, 2, 2]
        table_aligns = ["center", "start", "end", "end", "end", "end"]
        
        table_rows_contents = [
            {"type": "text", "text": "🏆 THỨ HẠNG DOANH THU NHÂN VIÊN", "size": "xxs", "color": theme_color, "weight": "bold", "margin": "xs"},
            make_table_header(["#", "NV", "MT", "HN", "CÒN", "%"], table_weights, table_aligns, bg_color=theme_color),
            {"type": "separator", "color": "#cbd5e1", "margin": "xs"}
        ]

        rank_icons = ["🥇", "🥈", "🥉"]
        for idx_s, s in enumerate(staff_list):
            rank_str = rank_icons[idx_s] if idx_s < 3 else f"{idx_s+1}."
            cl_str = "Đạt" if s["cl"] <= 0 and s["mt"] > 0 else f"{max(0, s['cl']):.1f}"
            cl_col = "#059669" if s["cl"] <= 0 and s["mt"] > 0 else "#ef4444"

            row_vals = [
                rank_str,
                s["name"],
                f"{s['mt']:.1f}",
                f"{s['hn']:.2f}",
                cl_str,
                f"{s['pct']:.0f}%"
            ]
            row_colors = [
                "#d97706" if idx_s < 3 else "#64748b",
                "#0f172a",
                "#64748b",
                "#059669" if s["hn"] > 0 else "#64748b",
                cl_col,
                get_color_class(s["pct"] / 100.0)
            ]
            table_rows_contents.append(make_table_row(row_vals, table_weights, table_aligns, row_colors, bold=(idx_s < 3)))
            if idx_s < len(staff_list) - 1:
                table_rows_contents.append({"type": "separator", "color": "#f1f5f9", "margin": "xs"})

        # Dòng TỔNG footer của bảng
        table_rows_contents.append({"type": "separator", "color": "#cbd5e1", "margin": "xs"})
        tot_cl_str = "Đạt" if tot_staff_cl <= 0 and tot_staff_mt > 0 else f"{max(0, tot_staff_cl):.1f}"
        tot_vals = ["⭐", "TỔNG", f"{tot_staff_mt:.1f}", f"{tot_staff_hn:.2f}", tot_cl_str, f"{tot_staff_pct:.0f}%"]
        tot_colors = ["#d97706", "#0f172a", "#475569", "#059669", "#059669" if tot_staff_cl <= 0 else "#ef4444", get_color_class(tot_staff_pct / 100.0)]
        table_rows_contents.append(make_table_row(tot_vals, table_weights, table_aligns, tot_colors, bold=True, bg_color="#f8fafc"))

        box_table = {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#ffffff",  # NỀN TRẮNG CHUẨN ĐẸP THEO YÊU CẦU!
            "borderColor": "#e2e8f0",
            "borderWidth": "1px",
            "cornerRadius": "md",
            "paddingAll": "sm",
            "contents": table_rows_contents
        }

        bubble = {
            "type": "bubble",
            "size": "mega",
            "header": header_box,
            "body": {
                "type": "box",
                "layout": "vertical",
                "backgroundColor": "#ffffff",
                "paddingAll": "sm",
                "spacing": "sm",
                "contents": [
                    box_categories,
                    box_kpi,
                    box_table
                ]
            }
        }
        bubbles.append(bubble)

    return bubbles


def build_custom_groups_messages(max_messages=LINE_MAX_MESSAGES):
    """
    Đóng gói danh sách Bubble Cards thành tin nhắn Flex Message Carousel gửi cho LINE Bot.
    """
    try:
        bubbles = build_custom_groups_flex()
        if not bubbles:
            return None, {"error": "Không có nhóm cấu hình"}

        carousel_contents = {
            "type": "carousel",
            "contents": bubbles
        }

        msgs = [{
            "alt_text": "🎯 Theo Dõi Nhóm Ngành Hàng Gộp (RT2)",
            "contents": carousel_contents
        }]
        return msgs, {"group_count": len(bubbles)}
    except Exception as e:
        print(f"Lỗi build_custom_groups_messages: {e}")
        return None, {"error": str(e)}

