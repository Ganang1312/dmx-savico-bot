# -*- coding: utf-8 -*-
"""
Module quản lý mã giảm giá (GVGS - Giá vạn người say / Coupon) cho Điện Máy XANH.
Hỗ trợ:
  - Nạp mã tự động từ văn bản hàng tuần
  - Xem danh sách mã theo ngành hàng (Tivi, Tủ lạnh, Máy giặt...)
  - Cấp mã 1 chạm (Claim coupon) tốc độ cao qua RAM Cache + Ghi Sheet chạy ngầm
  - Khóa mã đã dùng, chống cấp trùng lặp
"""

import re
import os
import pytz
from datetime import datetime
from config import (
    get_worksheet_cached_obj, get_cached_values, set_cached_values, run_bg_sheet_task
)

COUPON_HEADERS = [
    'date', 'category', 'product_name', 'coupon_code',
    'status', 'used_by', 'used_at', 'created_at'
]


def get_or_create_coupons_worksheet():
    """Lấy hoặc tạo worksheet 'coupons' trên Google Spreadsheet."""
    return get_worksheet_cached_obj('coupons', create_headers=COUPON_HEADERS, rows="1000", cols="10")


def clear_all_coupons():
    """
    Xóa toàn bộ mã giảm giá trong sheet 'coupons', chỉ giữ lại hàng tiêu đề (header).
    """
    sheet = get_or_create_coupons_worksheet()
    if not sheet:
        return False, "Không thể kết nối với cơ sở dữ liệu phiếu giảm giá."

    try:
        set_cached_values('coupons', [list(COUPON_HEADERS)])
        def _bg_clear_coupons():
            ws = get_or_create_coupons_worksheet()
            if ws:
                ws.clear()
                ws.append_row(COUPON_HEADERS)
        run_bg_sheet_task(_bg_clear_coupons, desc="clear_all_coupons")
        return True, "🗑️ Đã xóa sạch toàn bộ mã giảm giá (GVGS) trong hệ thống!\nKho phiếu đã được làm mới, bạn có thể nạp mã mới bằng lệnh `ADD COUPON:`."
    except Exception as e:
        print(f"Lỗi khi xóa sạch coupons: {e}")
        return False, f"Lỗi khi xóa mã giảm giá: {e}"


def detect_category(product_name):
    p_lower = str(product_name).lower()

    if any(k in p_lower for k in ['tủ đông', 'tu dong', 'tudong', 'freezer']):
        return 'Tủ Đông', '❄️'

    if any(k in p_lower for k in ['tủ lạnh', 'tu lanh', 'tulanh', 'fridge']):
        return 'Tủ Lạnh', '🧊'

    if any(k in p_lower for k in ['máy lọc nước', 'may loc nuoc', 'lọc nước', 'loc nuoc', 'ro nóng lạnh', 'water purifier']):
        return 'Máy lọc nước', '💧'

    if any(k in p_lower for k in ['loa thanh', 'soundbar', 'sound bar']):
        return 'Loa Thanh', '🔊'
    if any(k in p_lower for k in ['loa karaoke', 'karaoke', 'loa kéo', 'loa keo', 'dalton']):
        return 'Loa Karaoke', '🎤'
    if 'loa ' in p_lower or p_lower.startswith('loa'):
        return 'Loa Karaoke', '🎤'

    if any(k in p_lower for k in ['máy giặt', 'may giat', 'máy sấy', 'may say', 'giặt sấy', 'giat say', 'washer', 'dryer']):
        return 'Máy Giặt', '🧺'

    if any(k in p_lower for k in ['tivi', 'tv', 'tivi ', 'smart tv', 'qned', 'oled', 'qled', 'bravia', 'mini led']):
        return 'Tivi', '📺'

    if any(k in p_lower for k in ['máy lạnh', 'may lanh', 'điều hòa', 'dieu hoa', 'air conditioner']):
        return 'Máy lạnh', '❄️'

    if any(k in p_lower for k in ['nồi cơm', 'noi com', 'nồi chiên', 'noi chien', 'quạt', 'quat', 'bếp', 'bep', 'lò vi sóng', 'lo vi song', 'máy ép', 'may ep', 'máy xay', 'may xay', 'ấm siêu tốc', 'am sieu toc']):
        return 'Gia dụng', '🍳'

    if any(k in p_lower for k in ['điện thoại', 'dien thoai', 'smartphone', 'iphone', 'ipad', 'galaxy z', 'galaxy s']):
        return 'Điện thoại', '📱'

    return 'Khác', '🎁'


def parse_coupons_text(raw_text):
    results = []
    clean_text = raw_text.replace('\u00a0', ' ').replace('：', ':').replace('–', '-').replace('—', '-')
    lines = [line.strip() for line in clean_text.split('\n') if line.strip()]

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
    parsed = parse_coupons_text(raw_text)
    if not parsed:
        return 0, [], 0

    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    created_at = datetime.now(tz_vietnam).strftime('%Y-%m-%d %H:%M:%S')

    try:
        all_values = get_cached_values('coupons', ttl=60, create_headers=COUPON_HEADERS)
        if not all_values:
            all_values = [list(COUPON_HEADERS)]

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
            new_row = [
                item['date'],
                item['category'],
                item['product_name'],
                item['coupon_code'],
                'available',
                '',
                '',
                created_at
            ]
            rows_to_add.append(new_row)
            all_values.append(new_row)
            added_products.append(item['product_name'])

        if rows_to_add:
            set_cached_values('coupons', all_values)
            def _bg_import_coupons(to_add):
                ws = get_or_create_coupons_worksheet()
                if ws:
                    ws.append_rows(to_add, value_input_option='USER_ENTERED')
            run_bg_sheet_task(_bg_import_coupons, rows_to_add, desc="import_coupons")
            unique_products = list(dict.fromkeys(added_products))
            return len(rows_to_add), unique_products, dup_count

        return 0, [], dup_count
    except Exception as e:
        print(f"Lỗi khi nạp coupons vào sheet: {e}")
        return 0, [], 0


def get_available_coupons_today(target_date_str=None):
    if not target_date_str:
        tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
        target_date_str = datetime.now(tz_vietnam).strftime('%d/%m/%Y')

    try:
        all_values = get_cached_values('coupons', ttl=60, create_headers=COUPON_HEADERS)
        if len(all_values) <= 1:
            return []

        valid = []
        for row in all_values[1:]:
            if len(row) < 5:
                continue
            r_date = str(row[0]).strip()
            r_prod = str(row[2]).strip()
            r_cat_auto, _ = detect_category(r_prod)
            r_cat = r_cat_auto if r_cat_auto != 'Khác' else (str(row[1]).strip() or 'Khác')
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

        if not valid:
            for row in all_values[1:]:
                if len(row) < 5:
                    continue
                r_date = str(row[0]).strip()
                r_prod = str(row[2]).strip()
                r_cat_auto, _ = detect_category(r_prod)
                r_cat = r_cat_auto if r_cat_auto != 'Khác' else (str(row[1]).strip() or 'Khác')
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
    valid_coupons = get_available_coupons_today(target_date_str)
    summary = {}

    for c in valid_coupons:
        prod = c.get('product_name') or 'Sản phẩm'
        cat, icon = detect_category(prod)
        if cat == 'Khác' and c.get('category'):
            cat = c.get('category')
            _, icon = detect_category(cat)

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
    Cấp 1 mã giảm giá của sản phẩm cho nhân viên (cập nhật RAM tức thì chống trùng + ghi Sheet chạy ngầm).
    """
    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    if not target_date_str:
        target_date_str = datetime.now(tz_vietnam).strftime('%d/%m/%Y')
    time_str = datetime.now(tz_vietnam).strftime('%H:%M %d/%m/%Y')

    try:
        all_values = get_cached_values('coupons', ttl=60, create_headers=COUPON_HEADERS)
        if len(all_values) <= 1:
            cat_guess, _ = detect_category(product_name)
            return False, "Hiện chưa có phiếu giảm giá nào trong hệ thống.", 0, cat_guess

        target_p = str(product_name).strip().lower()
        available_indices = []
        category_found = ""

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

        # Khóa mã ngay trong RAM
        mem_row = all_values[target_row_idx - 1]
        while len(mem_row) < 7:
            mem_row.append('')
        mem_row[4] = 'used'
        mem_row[5] = user_display_name
        mem_row[6] = time_str
        set_cached_values('coupons', all_values)

        def _bg_claim_coupon(r_idx, usr, tm):
            ws = get_or_create_coupons_worksheet()
            if not ws:
                return
            range_update = f'E{r_idx}:G{r_idx}'
            vals = [['used', usr, tm]]
            try:
                ws.update(range_name=range_update, values=vals)
            except TypeError:
                ws.update(range_update, vals)

        run_bg_sheet_task(_bg_claim_coupon, target_row_idx, user_display_name, time_str, desc=f"claim_coupon_{chosen_code}")
        return True, chosen_code, remaining_count, category_found
    except Exception as e:
        print(f"Lỗi khi cấp coupon: {e}")
        cat_guess, _ = detect_category(product_name)
        return False, f"Lỗi hệ thống khi cấp phiếu: {e}", 0, cat_guess


# ==================== CÁC HÀM SINH FLEX MESSAGE ====================

def build_category_menu_flex(target_date_str=None):
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

    CATEGORY_ORDER = [
        'Máy Giặt',
        'Tủ Lạnh',
        'Tủ Đông',
        'Máy lọc nước',
        'Loa Karaoke',
        'Loa Thanh',
        'Tivi',
        'Máy lạnh',
        'Gia dụng',
        'Điện thoại',
        'Khác'
    ]
    sorted_cats = sorted(
        summary.keys(),
        key=lambda c: CATEGORY_ORDER.index(c) if c in CATEGORY_ORDER else 99
    )

    cat_boxes = []
    for cat in sorted_cats:
        info = summary[cat]
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
    tz_vietnam = pytz.timezone('Asia/Ho_Chi_Minh')
    if not target_date_str:
        target_date_str = datetime.now(tz_vietnam).strftime('%d/%m/%Y')

    summary = get_coupons_summary_by_category(target_date_str)
    cat_info = None
    target_cat_title = category

    for c_name, c_val in summary.items():
        if c_name.strip().lower() == str(category).strip().lower():
            cat_info = c_val
            target_cat_title = c_name
            break

    _, icon = detect_category(target_cat_title)

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
                        "text": f"{icon} PHIẾU GIẢM GIÁ {target_cat_title.upper()}",
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
                        "text": f"Đã hết phiếu giảm giá cho ngành hàng {target_cat_title} hôm nay.",
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
                                "data": f"action=claim_coupon&cat={target_cat_title}&prod={prod_name}"
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
                    "text": f"{icon} PHIẾU GIẢM GIÁ {target_cat_title.upper()}",
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
    _, icon = detect_category(product_name)

    flex_content = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#00884A",
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
                            "text": "📉 Đã trừ 1 phiếu khỏi kho chung.",
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
                    "text": "👉 Tin nhắn ngay dưới đây CHỈ có mã. Chạm đè vào mã đó để Sao chép (Copy).",
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
