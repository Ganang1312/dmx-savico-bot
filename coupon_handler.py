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
            _coupons_sheet_cache = spreadsheet.add_worksheet(title='coupons', rows="1000", cols="10")
            _coupons_sheet_cache.append_row(headers)
            print("Đã tạo mới worksheet: coupons")
        return _coupons_sheet_cache
    except Exception as e:
        print(f"Lỗi khi mở worksheet coupons: {e}")
        _coupons_sheet_cache = None
        return None


def clear_all_coupons():
    """
    Xóa toàn bộ mã giảm giá trong sheet 'coupons', chỉ giữ lại hàng tiêu đề (header).
    Trả về (True, message) hoặc (False, error_msg).
    """
    sheet = get_or_create_coupons_worksheet()
    if not sheet:
        return False, "Không thể kết nối với cơ sở dữ liệu phiếu giảm giá."

    try:
        headers = [
            'date', 'category', 'product_name', 'coupon_code',
            'status', 'used_by', 'used_at', 'created_at'
        ]
        sheet.clear()
        sheet.append_row(headers)
        return True, "🗑️ Đã xóa sạch toàn bộ mã giảm giá (GVGS) trong hệ thống!\nKho phiếu đã được làm mới, bạn có thể nạp mã mới bằng lệnh `ADD COUPON:`."
    except Exception as e:
        print(f"Lỗi khi xóa sạch coupons: {e}")
        return False, f"Lỗi khi xóa mã giảm giá: {e}"


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
    # Chuẩn hóa khoảng trắng unicode (non-breaking space), dấu hai chấm fullwidth, dấu gạch ngang
    clean_text = raw_text.replace('\u00a0', ' ').replace('：', ':').replace('–', '-').replace('—', '-')
    lines = [line.strip() for line in clean_text.split('\n') if line.strip()]

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
            # Dự phòng: Tách theo dấu ':' nếu dòng có từ 2 dấu hai chấm trở lên
            parts = [p.strip() for p in line.split(':') if p.strip()]
            if len(parts) >= 3:
                date_match = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", parts[0])
                date_str = date_match.group(1) if date_match else default_date
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
    Hỗ trợ nạp nối tiếp nhiều lần (không ghi đè, không nhảy hàng trống).
    Tự động lọc bỏ các mã đã tồn tại trên hệ thống (chống trùng lặp).
    Trả về (số_mã_nạp_mới, danh_sách_sản_phẩm, số_mã_trùng)
    """
    parsed = parse_coupons_text(raw_text)
    if not parsed:
        return 0, [], 0

    sheet = get_or_create_coupons_worksheet()
    if not sheet:
        return 0, [], 0

    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    created_at = datetime.now(tz_vietnam).strftime('%Y-%m-%d %H:%M:%S')

    try:
        all_values = sheet.get_all_values()
        existing_codes = set()
        if len(all_values) > 1:
            for row in all_values[1:]:
                if len(row) > 3 and row[3]:
                    existing_codes.add(str(row[3]).strip().upper())

        rows_to_add = []
        dup_count = 0
        added_products = []

        for item in parsed:
            code = item['coupon_code'].strip().upper()
            if code in existing_codes:
                dup_count += 1
                continue

            existing_codes.add(code)
            rows_to_add.append([
                item['date'],
                item['category'],
                item['product_name'],
                item['coupon_code'],
                'available',
                '',
                '',
                created_at
            ])
            added_products.append(item['product_name'])

        if rows_to_add:
            next_row = len(all_values) + 1
            end_row = next_row + len(rows_to_add) - 1
            range_to_update = f"A{next_row}:H{end_row}"

            try:
                sheet.update(range_name=range_to_update, values=rows_to_add, value_input_option='USER_ENTERED')
            except TypeError:
                sheet.update(range_to_update, rows_to_add, value_input_option='USER_ENTERED')

            unique_products = list(dict.fromkeys(added_products))
            return len(rows_to_add), unique_products, dup_count

        return 0, [], dup_count
    except Exception as e:
        print(f"Lỗi khi nạp coupons vào sheet: {e}")
        return 0, [], 0


def get_available_coupons_today(target_date_str=None):
    """
    Lấy toàn bộ phiếu còn hiệu lực hôm nay (status == 'available').
    Nếu hôm nay chưa có mã riêng, fallback lấy các mã 'available' chưa dùng trong kho.
    """
    sheet = get_or_create_coupons_worksheet()
    if not sheet:
        return []

    if not target_date_str:
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        target_date_str = datetime.now(tz_vietnam).strftime('%d/%m/%Y')

    try:
        all_values = sheet.get_all_values()
        if len(all_values) <= 1:
            return []

        valid = []
        for row in all_values[1:]:
            if len(row) < 5:
                continue
            r_date = str(row[0]).strip()
            r_cat = str(row[1]).strip()
            r_prod = str(row[2]).strip()
            r_code = str(row[3]).strip()
            r_status = str(row[4]).strip().lower()

            if r_status == 'available' and (r_date == target_date_str or not r_date):
                valid.append({
                    'date': r_date,
                    'category': r_cat,
                    'product_name': r_prod,
                    'coupon_code': r_code,
                    'status': r_status
                })

        # Fallback nếu hôm nay chưa có mã riêng nhưng trong kho có mã còn trống
        if not valid:
            for row in all_values[1:]:
                if len(row) < 5:
                    continue
                r_date = str(row[0]).strip()
                r_cat = str(row[1]).strip()
                r_prod = str(row[2]).strip()
                r_code = str(row[3]).strip()
                r_status = str(row[4]).strip().lower()
                if r_status == 'available':
                    valid.append({
                        'date': r_date,
                        'category': r_cat,
                        'product_name': r_prod,
                        'coupon_code': r_code,
                        'status': r_status
                    })

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
      - Tìm mã 'available' đầu tiên của sản phẩm trong kho
      - Đánh dấu 'used', ghi nhận user_display_name và thời gian
      - Trả về (True, coupon_code, remaining_count, category) hoặc (False, error_msg, 0, category)
    """
    sheet = get_or_create_coupons_worksheet()
    if not sheet:
        return False, "Không thể kết nối cơ sở dữ liệu phiếu.", 0, ""

    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    if not target_date_str:
        target_date_str = datetime.now(tz_vietnam).strftime('%d/%m/%Y')
    time_str = datetime.now(tz_vietnam).strftime('%H:%M %d/%m/%Y')

    try:
        all_values = sheet.get_all_values()
        if len(all_values) <= 1:
            cat_guess, _ = detect_category(product_name)
            return False, "Hiện chưa có phiếu giảm giá nào trong hệ thống.", 0, cat_guess

        target_p = str(product_name).strip().lower()
        available_indices = []
        category_found = ""

        # Cột: 0: date, 1: category, 2: product_name, 3: coupon_code, 4: status, 5: used_by, 6: used_at
        for row_idx, row in enumerate(all_values[1:], start=2):
            if len(row) < 5:
                continue
            r_date = str(row[0]).strip()
            r_cat = str(row[1]).strip()
            r_prod = str(row[2]).strip().lower()
            r_code = str(row[3]).strip()
            r_status = str(row[4]).strip().lower()

            if r_prod == target_p:
                if not category_found:
                    category_found = r_cat
                if r_status == 'available' and (r_date == target_date_str or not r_date):
                    available_indices.append((row_idx, r_code))

        # Nếu không tìm thấy mã theo ngày hôm nay, tìm mã available của model này trong toàn bộ sheet
        if not available_indices:
            for row_idx, row in enumerate(all_values[1:], start=2):
                if len(row) < 5:
                    continue
                r_cat = str(row[1]).strip()
                r_prod = str(row[2]).strip().lower()
                r_code = str(row[3]).strip()
                r_status = str(row[4]).strip().lower()

                if r_prod == target_p and r_status == 'available':
                    if not category_found:
                        category_found = r_cat
                    available_indices.append((row_idx, r_code))

        if not category_found:
            category_found, _ = detect_category(product_name)

        if not available_indices:
            return False, f"Đã hết phiếu giảm giá cho {product_name}!", 0, category_found

        target_row_idx, chosen_code = available_indices[0]
        remaining_count = len(available_indices) - 1

        # Cập nhật: Cột E: status, F: used_by, G: used_at
        range_update = f'E{target_row_idx}:G{target_row_idx}'
        vals = [['used', user_display_name, time_str]]
        try:
            sheet.update(range_name=range_update, values=vals)
        except TypeError:
            sheet.update(range_update, vals)

        return True, chosen_code, remaining_count, category_found
    except Exception as e:
        print(f"Lỗi khi cấp coupon: {e}")
        cat_guess, _ = detect_category(product_name)
        return False, f"Lỗi hệ thống khi cấp phiếu: {e}", 0, cat_guess


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
                                "data": f"action=claim_coupon&cat={category}&prod={prod_name}"
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
    Thẻ Flex trao mã coupon cho nhân viên:
      - Hiển thị to rõ ràng mã Coupon
      - Thông báo kho phiếu đã trừ 1 phiếu và còn lại bao nhiêu
      - Có nút dán mã ra khung chat an toàn 100% với LINE SDK
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
                    "text": "🎉 LẤY MÃ GIẢM GIÁ THÀNH CÔNG",
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
                            "size": "xl",
                            "color": "#15803D",
                            "margin": "sm"
                        }
                    ]
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "backgroundColor": "#F8FAFC",
                    "cornerRadius": "md",
                    "paddingAll": "sm",
                    "contents": [
                        {
                            "type": "text",
                            "text": f"📉 Đã trừ 1 phiếu khỏi kho chung.",
                            "size": "xs",
                            "color": "#DC2626",
                            "weight": "bold"
                        },
                        {
                            "type": "text",
                            "text": f"🟢 Hiện tại còn lại: {remaining_count} phiếu cho model này.",
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
                        "type": "message",
                        "label": "📋 Dán mã ra khung chat",
                        "text": coupon_code
                    },
                    "style": "primary",
                    "color": "#0288D1",
                    "height": "sm"
                },
                {
                    "type": "text",
                    "text": "ℹ️ Bạn cũng có thể chạm đè vào tin nhắn văn bản bên dưới để Sao chép (Copy) mã ngay lập tức.",
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
