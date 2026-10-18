import sys, os, json

sys.path.insert(0, os.path.dirname(__file__))
from dmx_flex_messages import build_nhanvien_flex, build_individual_staff_card, get_dashboard_data, parse_number, get_key_val, is_total_row, shorten_name, fmt_num, get_color_class
from test_v2_design import build_individual_staff_card_v2

# Tạo dữ liệu thực tế cho thẻ nhân viên
data = get_dashboard_data("Config_ThiDua,Data_NV_BI,Data_BI,Data_Realtime_NV,Data_NV_ThiDua,Data_ThiDua")
config_rows = data.get("Config_ThiDua", [])
bi_rows = data.get("Data_BI", [])
nv_rows = data.get("Data_NV_BI", []) or data.get("Data_Realtime_NV", [])
td_store_rows = data.get("Data_ThiDua", [])
nv_td_rows = data.get("Data_NV_ThiDua", [])

bubbles = build_nhanvien_flex()
old_card = bubbles[6] # Duong-62210

# Lấy các tham số của Dương để tạo V2
active_config_staff = {}
config_map = {}
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
total_row_bi = next((b for b in bi_rows if is_total_row(b)), None)
base_target_bi = parse_number(get_key_val(total_row_bi, "Target", "target", default=0.0)) if total_row_bi else 0.0
tTG_Store = sum(parse_number(get_key_val(b, "Target", "target", default=0.0)) for b in bi_rows if not is_total_row(b) and get_key_val(b, "nhóm ngành hàng", default="") != "N/A")
if base_target_bi > 0:
    tTG_Store = base_target_bi
elif tTG_Store <= 0:
    tTG_Store = 1500.0

# Tìm staff Dương
staff_duong = None
duong_td_list = []
for nr in nv_rows:
    uid = str(get_key_val(nr, "mã nv", "user", "staffuser", default="")).strip()
    if uid == "62210":
        staff_duong = {
            "name": get_key_val(nr, "tên nhân viên", "staffname", default="Dương"),
            "user_id": "62210",
            "actual": parse_number(get_key_val(nr, "doanh thu quy đổi", "revenue_kfactor", default=0.0)),
            "target": round(tTG_Store * active_config_staff.get("62210", {}).get("ratio", 1.0 / num_active_staff)),
            "diem": 44.4,
            "du_kien_pct": 51.8,
            "days_passed": 4,
            "days_in_month": 30
        }
        break

if not staff_duong:
    staff_duong = {
        "name": "Dương", "user_id": "62210", "actual": 32.0, "target": 920.0,
        "diem": 44.4, "du_kien_pct": 51.8, "days_passed": 4, "days_in_month": 30
    }

# Mock 23 thi dua items matching image
sample_td_items = [
    {"name": "Nồi cơm - nồi chiên", "m_tieu": "🏆", "actual": 23, "target": 14, "con_lai": "🏆", "ht": 163.0, "du_kien": 2452.0, "is_sl": True},
    {"name": "SẠC DỰ PHÒNG", "m_tieu": "🏆", "actual": 1, "target": 1, "con_lai": "🏆", "ht": 77.0, "du_kien": 1159.0, "is_sl": True},
    {"name": "Bảo hiểm Thợ Giỏi", "m_tieu": "820k", "actual": 12, "target": 35, "con_lai": "23", "ht": 34.0, "du_kien": 506.0, "is_sl": False, "phan_loai": 2.0},
    {"name": "ĐIỆN TỬ (DT)", "m_tieu": "4", "actual": 57, "target": 181, "con_lai": "124", "ht": 31.0, "du_kien": 471.0, "is_sl": False},
    {"name": "Máy giặt (DT)", "m_tieu": "2", "actual": 13, "target": 74, "con_lai": "61", "ht": 17.0, "du_kien": 259.0, "is_sl": False, "phan_loai": 2.0},
    {"name": "ĐIỆN TỬ & ĐIỆN TỬ TCL", "m_tieu": "4", "actual": 19, "target": 118, "con_lai": "99", "ht": 16.0, "du_kien": 239.0, "is_sl": False},
    {"name": "P.Kiện IT và nhóm khác", "m_tieu": "1", "actual": 6, "target": 42, "con_lai": "36", "ht": 15.0, "du_kien": 226.0, "is_sl": True},
    {"name": "Laptop (trừ Apple)", "m_tieu": "6", "actual": 27, "target": 192, "con_lai": "165", "ht": 14.0, "du_kien": 210.0, "is_sl": False},
    {"name": "Lọc K.Khí - Hút Bụi", "m_tieu": "1", "actual": 1, "target": 32, "con_lai": "32", "ht": 2.0, "du_kien": 33.0, "is_sl": True, "phan_loai": 2.0},
    {"name": "Bảo hiểm tổng", "m_tieu": "79k", "actual": 0, "target": 2, "con_lai": "2", "ht": 0.0, "du_kien": 0.0, "is_sl": False},
    {"name": "SIM tổng (SL)", "m_tieu": "1", "actual": 0, "target": 5, "con_lai": "5", "ht": 0.0, "du_kien": 0.0, "is_sl": True, "phan_loai": 2.0},
    {"name": "VAS (SL)", "m_tieu": "1", "actual": 0, "target": 2, "con_lai": "2", "ht": 0.0, "du_kien": 0.0, "is_sl": True},
    {"name": "TABLET ANDROID", "m_tieu": "1", "actual": 0, "target": 9, "con_lai": "9", "ht": 0.0, "du_kien": 0.0, "is_sl": True, "phan_loai": 2.0},
    {"name": "Đ.Thoại realme", "m_tieu": "1", "actual": 0, "target": 2, "con_lai": "2", "ht": 0.0, "du_kien": 0.0, "is_sl": True},
    {"name": "Đ.Thoại Vivo", "m_tieu": "1", "actual": 0, "target": 3, "con_lai": "3", "ht": 0.0, "du_kien": 0.0, "is_sl": True},
    {"name": "Trả Chậm FECC", "m_tieu": "1", "actual": 0, "target": 29, "con_lai": "29", "ht": 0.0, "du_kien": 0.0, "is_sl": True, "phan_loai": 2.0},
    {"name": "TC HomeCredit", "m_tieu": "3", "actual": 0, "target": 86, "con_lai": "86", "ht": 0.0, "du_kien": 0.0, "is_sl": True},
    {"name": "TC Điện Máy Xanh", "m_tieu": "3", "actual": 0, "target": 94, "con_lai": "94", "ht": 0.0, "du_kien": 0.0, "is_sl": True},
    {"name": "TỦ LẠNH, TỦ MÁT", "m_tieu": "4", "actual": 0, "target": 118, "con_lai": "118", "ht": 0.0, "du_kien": 0.0, "is_sl": False, "phan_loai": 2.0},
    {"name": "Camera (SL)", "m_tieu": "1", "actual": 0, "target": 2, "con_lai": "2", "ht": 0.0, "du_kien": 0.0, "is_sl": True}
]

# Tạo V1 và V2
v1_card = build_individual_staff_card(staff_duong, 5, 11, "11:00", sample_td_items)
v2_card = build_individual_staff_card_v2(staff_duong, 5, 11, "11:00", sample_td_items)

def flex_to_html(node):
    if not isinstance(node, dict):
        return ""
    ntype = node.get("type", "")
    
    if ntype == "bubble":
        header_html = flex_to_html(node.get("header")) if "header" in node else ""
        body_html = flex_to_html(node.get("body")) if "body" in node else ""
        return f'<div class="flex-bubble">{header_html}{body_html}</div>'
    
    if ntype == "box":
        layout = node.get("layout", "vertical")
        styles = []
        if layout == "horizontal":
            styles.append("display: flex; flex-direction: row;")
        elif layout == "vertical":
            styles.append("display: flex; flex-direction: column;")
            
        if "flex" in node:
            styles.append(f"flex: {node['flex']};")
        if "backgroundColor" in node:
            styles.append(f"background-color: {node['backgroundColor']};")
        if "cornerRadius" in node:
            cr = node["cornerRadius"]
            rad = {"xs": "4px", "sm": "6px", "md": "8px", "lg": "12px", "xxl": "9999px"}.get(cr, cr)
            styles.append(f"border-radius: {rad};")
        if "paddingAll" in node:
            p = {"xxs": "2px", "xs": "4px", "sm": "8px", "md": "12px", "lg": "16px"}.get(node["paddingAll"], node["paddingAll"])
            styles.append(f"padding: {p};")
        if "paddingTop" in node:
            styles.append(f"padding-top: {node['paddingTop']};")
        if "paddingBottom" in node:
            styles.append(f"padding-bottom: {node['paddingBottom']};")
        if "paddingStart" in node:
            styles.append(f"padding-left: {node['paddingStart']};")
        if "paddingEnd" in node:
            styles.append(f"padding-right: {node['paddingEnd']};")
        if "margin" in node:
            m = {"xs": "4px", "sm": "8px", "md": "12px", "lg": "16px"}.get(node["margin"], node["margin"])
            styles.append(f"margin-top: {m};")
        if "spacing" in node:
            sp = {"xs": "4px", "sm": "8px", "md": "12px"}.get(node["spacing"], node["spacing"])
            styles.append(f"gap: {sp};")
        if "alignItems" in node:
            styles.append(f"align-items: {node['alignItems']};")
        if "justifyContent" in node:
            styles.append(f"justify-content: {node['justifyContent']};")
        if "height" in node:
            styles.append(f"height: {node['height']};")
        if "width" in node:
            styles.append(f"width: {node['width']};")
            
        inner = "".join(flex_to_html(c) for c in node.get("contents", []))
        return f'<div style="{" ".join(styles)}">{inner}</div>'
    
    if ntype == "text":
        styles = []
        size_map = {"xxs": "11px", "xs": "12.5px", "sm": "14px", "md": "16px", "lg": "18px"}
        if "size" in node:
            styles.append(f"font-size: {size_map.get(node['size'], '13px')};")
        if node.get("weight") == "bold":
            styles.append("font-weight: 700;")
        if "color" in node:
            styles.append(f"color: {node['color']};")
        if "align" in node:
            styles.append(f"text-align: {node['align']};")
        if "flex" in node:
            styles.append(f"flex: {node['flex']};")
        if node.get("wrap"):
            styles.append("white-space: normal;")
        else:
            styles.append("white-space: nowrap; overflow: hidden; text-overflow: ellipsis;")
        if "margin" in node:
            m = {"xs": "2px", "sm": "4px"}.get(node["margin"], "2px")
            styles.append(f"margin-top: {m};")
            
        txt = node.get("text", "")
        return f'<div style="{" ".join(styles)}">{txt}</div>'
    
    if ntype == "separator":
        col = node.get("color", "#e2e8f0")
        m = node.get("margin", "sm")
        mg = {"xs": "4px 0", "sm": "8px 0", "md": "12px 0"}.get(m, "8px 0")
        return f'<hr style="border: none; border-top: 1px solid {col}; margin: {mg};" />'
        
    if ntype == "filler":
        return '<div style="flex: 1;"></div>'
        
    return ""

html_v1 = flex_to_html(v1_card)
html_v2 = flex_to_html(v2_card)

full_html = f'''<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>So Sánh Giao Diện Thẻ Nhân Viên NV0/NV1 (Cũ vs Mới)</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", "Segoe UI", Roboto, sans-serif;
            background: #0f172a;
            color: #f8fafc;
            margin: 0;
            padding: 30px 15px;
            display: flex;
            flex-direction: column;
            align-items: center;
        }}
        .header-title {{
            text-align: center;
            margin-bottom: 25px;
        }}
        .header-title h1 {{
            font-size: 24px;
            margin: 0 0 8px 0;
            color: #38bdf8;
        }}
        .header-title p {{
            font-size: 14px;
            color: #94a3b8;
            margin: 0;
        }}
        .comparison-grid {{
            display: flex;
            flex-direction: row;
            gap: 30px;
            justify-content: center;
            flex-wrap: wrap;
            max-width: 1000px;
            width: 100%;
        }}
        .col-box {{
            display: flex;
            flex-direction: column;
            align-items: center;
        }}
        .badge-ver {{
            padding: 6px 14px;
            border-radius: 20px;
            font-size: 13px;
            font-weight: 700;
            margin-bottom: 12px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        .badge-old {{ background: #ef4444; color: #ffffff; }}
        .badge-new {{ background: #10b981; color: #ffffff; box-shadow: 0 0 15px rgba(16,185,129,0.4); }}
        .flex-bubble {{
            width: 320px;
            background: #ffffff;
            border-radius: 16px;
            overflow: hidden;
            box-shadow: 0 10px 25px rgba(0,0,0,0.3);
            border: 1px solid rgba(255,255,255,0.1);
        }}
    </style>
</head>
<body>
    <div class="header-title">
        <h1>SO SÁNH GIAO DIỆN TIN NHẮN FLEX (LỆNH NV0 & NV1)</h1>
        <p>Giữ nguyên 100% tất cả các chỉ số (Target, LK, Còn, %HT, %DK, MT, 23 nhóm hàng) - Tối ưu thẩm mỹ chuẩn Executive</p>
    </div>
    <div class="comparison-grid">
        <div class="col-box">
            <div class="badge-ver badge-old">❌ GIAO DIỆN HIỆN TẠI (BỊ CHÊ XẤU)</div>
            {html_v1}
        </div>
        <div class="col-box">
            <div class="badge-ver badge-new">✨ GIAO DIỆN MỚI (EXECUTIVE PRO)</div>
            {html_v2}
        </div>
    </div>
</body>
</html>
'''

output_path = os.path.join(os.path.dirname(__file__), "preview_flex_comparison.html")
with open(output_path, "w", encoding="utf-8") as f:
    f.write(full_html)
print(f"Da tao thanh cong file: {output_path}")
