# -*- coding: utf-8 -*-
"""
Module quản lý mã giảm giá (GVGS - Giá vạn người say / Coupon) cho Điện Máy XANH.
Hỗ trợ:
  - Nạp mã tự động từ văn bản hàng tuần
  - Xem danh sách mã theo ngành hàng (Tivi, Tủ lạnh, Máy giặt...)
  - Cấp mã 1 chạm (Claim coupon) kèm nút sao chép (Clipboard action)
  - Khóa mã đã dùng, chống cấp trùng lặp
"""

import re
import os
import pytz
from datetime import datetime

_coupons_sheet_cache = None

def get_or_create_coupons_worksheet():
    """Lấy hoặc tạo worksheet 'coupons' trên Google Spreadsheet."""
    global _coupons_sheet_cache
    if _coupons_sheet_cache is not None:
        return _coupons_sheet_cache

    try:
        import gspread
        from config import get_spreadsheet
        spreadsheet = get_spreadsheet()
        try:
            _coupons_sheet_cache = spreadsheet.worksheet('coupons')
        except gspread.exceptions.WorksheetNotFound:
            headers = [
                'date', 'category', 'product_name', 'coupon_code',
                'status', 'used_by', 'used_at', 'created_at'
            ]
            _coupons_sheet_cache = spreadsheet.add_worksheet(title='coupons', rows="3000", cols="10")
            _coupons_sheet_cache.append_row(headers)
            print("Đã tạo mới worksheet: coupons")
        return _coupons_sheet_cache
    except Exception as e:
        print(f"Lỗi khi mở worksheet coupons: {e}")
        return None


def detect_category(product_name):
    """Tự động phân loại ngành hàng từ tên sản phẩm."""
    p_lower = str(product_name).lower()
    if any(k in p_lower for k in ['tủ lạnh', 'tu lanh', 'fridge']):
        return 'Tủ lạnh', '🧊'
    if any(k in p_lower for k in ['tivi', 'tv', 'tivi ']):
        return 'Tivi', '📺'
    if any(k in p_lower for k in ['máy giặt', 'may giat', 'máy sấy', 'may say']):
        return 'Máy giặt', '🧺'
    if any(k in p_lower for k in ['máy lạnh', 'may lanh', 'điều hòa', 'dieu hoa']):
        return 'Máy lạnh', '❄️'
    if any(k in p_lower for k in ['nồi cơm', 'noi com', 'nồi chiên', 'noi chien', 'quạt', 'quat', 'lọc nước']):
        return 'Gia dụng', '🍳'
    if any(k in p_lower for k in ['điện thoại', 'dien thoai', 'iphone', 'samsung', 'oppo', 'xiaomi']):
        return 'Điện thoại', '📱'
    return 'Khác', '🎁'


def parse_coupons_text(raw_text):
    """
    Phân tích văn bản nạp coupon hàng tuần dạng:
    'Ngày 25/09/2026 : Mã Coupon 1 - dùng cho Tủ lạnh Panasonic NR-DZ601VGKV: DMX22BV33RMK962'
    Trả về danh sách dict: [{date, category, product_name, coupon_code}]
    """
    results = []
    lines = [line.strip() for line in raw_text.split('\n') if line.strip()]

    # Pattern chuẩn: Ngày dd/mm/yyyy : ... dùng cho <tên sp>: <mã coupon>
    # Hoặc: dd/mm/yyyy : <tên sp> : <mã coupon>
    pattern = re.compile(
        r"(?:Ngày\s*)?(\d{1,2}/\d{1,2}/\d{4})\s*:\s*(?:Mã\s*Coupon\s*\d*\s*-\s*)?(?:dùng\s*cho\s*)?([^:]+?)\s*:\s*([A-Za-z0-9_-]+)",
        re.IGNORECASE
    )

    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    default_date = datetime.now(tz_vietnam).strftime('%d/%m/%Y')

    for line in lines:
        match = pattern.search(line)
        if match:
            date_str = match.group(1).strip()
            prod_name = match.group(2).strip()
            code = match.group(3).strip()
            cat, _ = detect_category(prod_name)
            results.append({
                'date': date_str,
                'category': cat,
                'product_name': prod_name,
                'coupon_code': code
            })
        else:
            # Dự phòng: Tách theo dấu ':' nếu dòng có đúng 2 dấu hai chấm
            parts = [p.strip() for p in line.split(':') if p.strip()]
            if len(parts) >= 3:
                date_match = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", parts[0])
                date_str = date_match.group(1) if date_match else default_date
                # Phần tên sản phẩm thường ở giữa
                prod_part = parts[1]
                prod_clean = re.sub(r"Mã\s*Coupon\s*\d*\s*-\s*", "", prod_part, flags=re.IGNORECASE)
                prod_clean = re.sub(r"dùng\s*cho\s*", "", prod_clean, flags=re.IGNORECASE).strip()
                code = parts[2].split()[0].strip()
                cat, _ = detect_category(prod_clean)
                results.append({
                    'date': date_str,
                    'category': cat,
                    'product_name': prod_clean,
                    'coupon_code': code
                })

    return results


def import_coupons_from_text(raw_text):
    """
    Nạp danh sách mã giảm giá từ văn bản vào Google Sheet.
    Trả về (số_mã_nạp_được, danh_sách_sản_phẩm)
    """
    parsed = parse_coupons_text(raw_text)
    if not parsed:
        return 0, []

    sheet = get_or_create_coupons_worksheet()
    if not sheet:
        return 0, []

    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    created_at = datetime.now(tz_vietnam).strftime('%Y-%m-%d %H:%M:%S')

    rows_to_add = []
    for item in parsed:
        row = [
            item['date'],
            item['category'],
            item['product_name'],
            item['coupon_code'],
            'available',
            '',
            '',
            created_at
        ]
        rows_to_add.append(row)

    if rows_to_add:
        sheet.append_rows(rows_to_add, value_input_option='USER_ENTERED')
        products = list(dict.fromkeys(item['product_name'] for item in parsed))
        return len(rows_to_add), products
    return 0, []


def get_available_coupons_today(target_date_str=None):
    """
    Lấy toàn bộ phiếu còn hiệu lực hôm nay (status == 'available').
    """
    sheet = get_or_create_coupons_worksheet()
    if not sheet:
        return []

    if not target_date_str:
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        target_date_str = datetime.now(tz_vietnam).strftime('%d/%m/%Y')

    try:
        records = sheet.get_all_records()
        valid = []
        for r in records:
            r_date = str(r.get('date', '')).strip()
            r_status = str(r.get('status', '')).strip().lower()
            if r_status == 'available' and (r_date == target_date_str or not r_date):
                valid.append(r)
        return valid
    except Exception as e:
        print(f"Lỗi khi đọc coupons: {e}")
        return []


def get_coupons_summary_by_category(target_date_str=None):
    """
    Tổng hợp số lượng mã còn lại theo từng ngành hàng và sản phẩm trong ngày.
    Trả về dict: { category: { 'icon': ..., 'count': ..., 'products': { prod_name: count } } }
    """
    valid_coupons = get_available_coupons_today(target_date_str)
    summary = {}

    for c in valid_coupons:
        cat = c.get('category') or 'Khác'
        prod = c.get('product_name') or 'Sản phẩm'
        _, icon = detect_category(cat + ' ' + prod)

        if cat not in summary:
            summary[cat] = {
                'icon': icon,
                'count': 0,
                'products': {}
            }
        summary[cat]['count'] += 1
        summary[cat]['products'][prod] = summary[cat]['products'].get(prod, 0) + 1

    return summary


def claim_coupon_for_user(product_name, user_display_name, target_date_str=None):
    """
    Cấp 1 mã giảm giá của sản phẩm cho nhân viên:
      - Tìm mã 'available' đầu tiên của sản phẩm trong ngày
      - Đánh dấu 'used', ghi nhận user_display_name và thời gian
      - Trả về (True, coupon_code, remaining_count) hoặc (False, error_msg, 0)
    """
    sheet = get_or_create_coupons_worksheet()
    if not sheet:
        return False, "Không thể kết nối cơ sở dữ liệu phiếu.", 0

    if not target_date_str:
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        target_date_str = datetime.now(tz_vietnam).strftime('%d/%m/%Y')
        time_str = datetime.now(tz_vietnam).strftime('%H:%M %d/%m/%Y')
    else:
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        time_str = datetime.now(tz_vietnam).strftime('%H:%M %d/%m/%Y')

    try:
        records = sheet.get_all_records()
        target_p = str(product_name).strip().lower()

        target_row_idx = -1
        chosen_code = None
        remaining_count = 0

        # Tìm dòng đầu tiên còn trống và đếm số lượng còn lại
        for i, r in enumerate(records):
            r_prod = str(r.get('product_name', '')).strip().lower()
            r_date = str(r.get('date', '')).strip()
            r_status = str(r.get('status', '')).strip().lower()

            if r_prod == target_p and (r_date == target_date_str or not r_date):
                if r_status == 'available':
                    if target_row_idx == -1:
                        target_row_idx = i + 2  # hàng thực tế trên sheet (1-based, bỏ dòng header)
                        chosen_code = str(r.get('coupon_code', '')).strip()
                    else:
                        remaining_count += 1

        if target_row_idx == -1 or not chosen_code:
            return False, f"Đã hết phiếu giảm giá cho {product_name} trong ngày hôm nay!", 0

        # Cột E: status, Cột F: used_by, Cột G: used_at
        range_update = f'E{target_row_idx}:G{target_row_idx}'
        sheet.update(range_name=range_update, values=[['used', user_display_name, time_str]])

        return True, chosen_code, remaining_count
    except Exception as e:
        print(f"Lỗi khi cấp coupon: {e}")
        return False, f"Lỗi hệ thống khi cấp phiếu: {e}", 0


# ==================== CÁC HÀM SINH FLEX MESSAGE ====================

def build_category_menu_flex(target_date_str=None):
    """
    Thẻ Flex Menu danh mục ngành hàng khi gõ lệnh 'gvgs'.
    """
    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    if not target_date_str:
        target_date_str = datetime.now(tz_vietnam).strftime('%d/%m/%Y')

    summary = get_coupons_summary_by_category(target_date_str)

    if not summary:
        return {
            "type": "bubble",
            "size": "mega",
            "header": {
                "type": "box",
                "layout": "vertical",
                "backgroundColor": "#D32F2F",
                "paddingAll": "lg",
                "contents": [
                    {
                        "type": "text",
                        "text": "🎟️ PHIẾU GIẢM GIÁ (GVGS)",
                        "weight": "bold",
                        "size": "md",
                        "color": "#FFFFFF"
                    },
                    {
                        "type": "text",
                        "text": f"📅 Ngày: {target_date_str}",
                        "size": "xs",
                        "color": "#FFCDD2",
                        "margin": "xs"
                    }
                ]
            },
            "body": {
                "type": "box",
                "layout": "vertical",
                "paddingAll": "lg",
                "contents": [
                    {
                        "type": "text",
                        "text": "Hiện tại chưa có phiếu giảm giá nào cho ngày hôm nay.\nQuản lý có thể nạp mã bằng lệnh:\n`ADD COUPON:` kèm danh sách mã.",
                        "wrap": True,
                        "size": "sm",
                        "color": "#666666"
                    }
                ]
            }
        }

    cat_boxes = []
    for cat, info in summary.items():
        icon = info['icon']
        count = info['count']
        num_prod = len(info['products'])

        cat_row = {
            "type": "box",
            "layout": "horizontal",
            "alignItems": "center",
            "spacing": "md",
            "paddingAll": "md",
            "backgroundColor": "#F8FAFC",
            "cornerRadius": "md",
            "margin": "sm",
            "contents": [
                {
                    "type": "text",
                    "text": icon,
                    "size": "xxl",
                    "flex": 0
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "flex": 1,
                    "contents": [
                        {
                            "type": "text",
                            "text": f"{cat}",
                            "weight": "bold",
                            "size": "sm",
                            "color": "#0F172A"
                        },
                        {
                            "type": "text",
                            "text": f"Còn {count} phiếu ({num_prod} model)",
                            "size": "xs",
                            "color": "#059669",
                            "weight": "bold",
                            "margin": "xs"
                        }
                    ]
                },
                {
                    "type": "button",
                    "action": {
                        "type": "postback",
                        "label": "Xem mã",
                        "data": f"action=view_coupon_cat&cat={cat}"
                    },
                    "style": "primary",
                    "color": "#0288D1",
                    "height": "sm",
                    "flex": 0,
                    "width": "80px"
                }
            ]
        }
        cat_boxes.append(cat_row)

    flex_content = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#0288D1",
            "paddingTop": "14px",
            "paddingBottom": "14px",
            "paddingStart": "16px",
            "paddingEnd": "16px",
            "contents": [
                {
                    "type": "text",
                    "text": "🎟️ PHIẾU GIẢM GIÁ (GVGS)",
                    "weight": "bold",
                    "size": "md",
                    "color": "#FFFFFF"
                },
                {
                    "type": "text",
                    "text": f"📅 Ngày: {target_date_str}  |  Bấm 'Xem mã' để chọn model",
                    "size": "xxs",
                    "color": "#B3E5FC",
                    "margin": "xs"
                }
            ]
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "xs",
            "paddingAll": "sm",
            "contents": cat_boxes
        }
    }
    return flex_content


def build_product_list_flex(category, target_date_str=None):
    """
    Thẻ Flex danh sách sản phẩm thuộc một ngành hàng kèm nút [ 🎟️ Lấy mã ].
    """
    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    if not target_date_str:
        target_date_str = datetime.now(tz_vietnam).strftime('%d/%m/%Y')

    summary = get_coupons_summary_by_category(target_date_str)
    cat_info = summary.get(category)

    _, icon = detect_category(category)

    if not cat_info or not cat_info['products']:
        return {
            "type": "bubble",
            "size": "mega",
            "header": {
                "type": "box",
                "layout": "vertical",
                "backgroundColor": "#0288D1",
                "paddingAll": "md",
                "contents": [
                    {
                        "type": "text",
                        "text": f"{icon} PHIẾU GIẢM GIÁ {category.upper()}",
                        "weight": "bold",
                        "size": "sm",
                        "color": "#FFFFFF"
                    }
                ]
            },
            "body": {
                "type": "box",
                "layout": "vertical",
                "paddingAll": "md",
                "contents": [
                    {
                        "type": "text",
                        "text": f"Đã hết phiếu giảm giá cho ngành hàng {category} hôm nay.",
                        "size": "sm",
                        "color": "#666666"
                    }
                ]
            },
            "footer": {
                "type": "box",
                "layout": "vertical",
                "contents": [
                    {
                        "type": "button",
                        "action": {
                            "type": "postback",
                            "label": "🔙 Quay lại danh mục",
                            "data": "action=view_coupon_menu"
                        },
                        "style": "secondary",
                        "height": "sm"
                    }
                ]
            }
        }

    prod_components = []
    for prod_name, count in cat_info['products'].items():
        # Rút ngắn tên hiển thị nếu quá dài
        prod_box = {
            "type": "box",
            "layout": "vertical",
            "paddingAll": "sm",
            "margin": "xs",
            "contents": [
                {
                    "type": "text",
                    "text": prod_name,
                    "weight": "bold",
                    "size": "xs",
                    "wrap": True,
                    "color": "#111111"
                },
                {
                    "type": "box",
                    "layout": "horizontal",
                    "alignItems": "center",
                    "spacing": "sm",
                    "marginTop": "xs",
                    "contents": [
                        {
                            "type": "text",
                            "text": f"🟢 Còn {count} phiếu",
                            "size": "xxs",
                            "color": "#059669",
                            "weight": "bold",
                            "flex": 1
                        },
                        {
                            "type": "button",
                            "action": {
                                "type": "postback",
                                "label": "🎟️ Lấy mã",
                                "data": f"action=claim_coupon&prod={prod_name}"
                            },
                            "style": "primary",
                            "color": "#00B33C",
                            "height": "sm",
                            "flex": 0,
                            "width": "85px"
                        }
                    ]
                }
            ]
        }
        prod_components.append(prod_box)
        prod_components.append({"type": "separator"})

    if prod_components and prod_components[-1].get("type") == "separator":
        prod_components.pop()

    flex_content = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#0288D1",
            "paddingTop": "14px",
            "paddingBottom": "14px",
            "paddingStart": "16px",
            "paddingEnd": "16px",
            "contents": [
                {
                    "type": "text",
                    "text": f"{icon} PHIẾU GIẢM GIÁ {category.upper()}",
                    "weight": "bold",
                    "size": "md",
                    "color": "#FFFFFF"
                },
                {
                    "type": "text",
                    "text": f"📅 Ngày: {target_date_str}  |  Bấm 'Lấy mã' để nhận phiếu",
                    "size": "xxs",
                    "color": "#B3E5FC",
                    "margin": "xs"
                }
            ]
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "xs",
            "paddingAll": "sm",
            "contents": prod_components
        },
        "footer": {
            "type": "box",
            "layout": "vertical",
            "paddingAll": "sm",
            "contents": [
                {
                    "type": "button",
                    "action": {
                        "type": "postback",
                        "label": "🔙 Quay lại danh mục",
                        "data": "action=view_coupon_menu"
                    },
                    "style": "secondary",
                    "height": "sm"
                }
            ]
        }
    }
    return flex_content


def build_claimed_coupon_flex(product_name, coupon_code, remaining_count, user_display_name):
    """
    Thẻ Flex trao mã coupon cho nhân viên, kèm nút 'Sao chép mã' (Clipboard Action 1 chạm).
    """
    _, icon = detect_category(product_name)

    flex_content = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#00884A",  # Màu xanh lá nhận diện Điện Máy XANH
            "paddingTop": "16px",
            "paddingBottom": "16px",
            "paddingStart": "20px",
            "paddingEnd": "20px",
            "contents": [
                {
                    "type": "text",
                    "text": "🎉 NHẬN MÃ GIẢM GIÁ THÀNH CÔNG",
                    "weight": "bold",
                    "size": "sm",
                    "color": "#FFFFFF"
                },
                {
                    "type": "text",
                    "text": f"👤 Nhân viên: {user_display_name}",
                    "size": "xs",
                    "color": "#A7F3D0",
                    "margin": "xs"
                }
            ]
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "paddingAll": "lg",
            "contents": [
                {
                    "type": "box",
                    "layout": "vertical",
                    "spacing": "xs",
                    "contents": [
                        {
                            "type": "text",
                            "text": "📦 SẢN PHẨM ÁP DỤNG:",
                            "size": "xxs",
                            "color": "#64748B",
                            "weight": "bold"
                        },
                        {
                            "type": "text",
                            "text": f"{icon} {product_name}",
                            "weight": "bold",
                            "size": "sm",
                            "color": "#0F172A",
                            "wrap": True
                        }
                    ]
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "backgroundColor": "#F0FDF4",
                    "borderColor": "#86EFAC",
                    "borderWidth": "medium",
                    "cornerRadius": "md",
                    "paddingAll": "md",
                    "alignItems": "center",
                    "contents": [
                        {
                            "type": "text",
                            "text": "🎟️ MÃ COUPON CỦA BẠN",
                            "size": "xxs",
                            "color": "#166534",
                            "weight": "bold"
                        },
                        {
                            "type": "text",
                            "text": coupon_code,
                            "weight": "bold",
                            "size": "lg",
                            "color": "#15803D",
                            "margin": "sm"
                        }
                    ]
                },
                {
                    "type": "button",
                    "action": {
                        "type": "clipboard",
                        "label": "📋 SAO CHÉP MÃ",
                        "clipboardText": coupon_code
                    },
                    "style": "primary",
                    "color": "#0288D1",
                    "height": "sm"
                },
                {
                    "type": "text",
                    "text": f"ℹ️ Mã đã được lưu vào hệ thống và trừ khỏi kho phiếu chung (Còn lại: {remaining_count} mã). Bấm nút trên để copy mã vào bộ nhớ máy.",
                    "size": "xxs",
                    "color": "#64748B",
                    "wrap": True,
                    "align": "center"
                }
            ]
        },
        "footer": {
            "type": "box",
            "layout": "vertical",
            "paddingAll": "sm",
            "contents": [
                {
                    "type": "button",
                    "action": {
                        "type": "postback",
                        "label": "🎟️ Xem các phiếu khác",
                        "data": "action=view_coupon_menu"
                    },
                    "style": "link",
                    "height": "sm"
                }
            ]
        }
    }
    return flex_content
