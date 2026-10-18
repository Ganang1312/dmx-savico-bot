import sys, os, json, re

if 'SUPABASE_KEY' not in os.environ:
    os.environ['SUPABASE_KEY'] = 'sb_publishable_tb1cO9NPuNC1cdA-pt_NNQ_1n5I9IkU'
if 'SUPABASE_URL' not in os.environ:
    os.environ['SUPABASE_URL'] = 'https://uybcglehwheygxmzlwbq.supabase.co'

sys.path.insert(0, os.path.dirname(__file__))
from dmx_flex_messages import build_realtime_flex

flexes = build_realtime_flex()
bubble_p1 = flexes[0] # Realtime Doanh thu & Tien do
bubble_p2 = flexes[1] # Nhom hang thi dua
nv_bubbles = flexes[2:] # Cac the chi tiet Doanh thu Nhan vien (The 3, The 4...)

def flex_to_html(node):
    if not node or not isinstance(node, dict):
        return ""
    ntype = node.get("type", "")
    
    if ntype == "bubble":
        size = node.get("size", "mega")
        w = "340px" if size == "mega" else "300px"
        h_html = flex_to_html(node.get("header"))
        b_html = flex_to_html(node.get("body"))
        f_html = flex_to_html(node.get("footer"))
        return f'''<div class="flex-bubble-card" style="width: {w}; background: #ffffff; border-radius: 18px; overflow: hidden; box-shadow: 0 12px 30px rgba(0,0,0,0.25); border: 1px solid rgba(255,255,255,0.12); margin-bottom: 16px; flex-shrink: 0;">
            {h_html}
            {b_html}
            {f_html}
        </div>'''
        
    if ntype == "box":
        layout = node.get("layout", "vertical")
        styles = ["display: flex;"]
        styles.append("flex-direction: row;" if layout == "horizontal" else "flex-direction: column;")
        
        if "flex" in node:
            styles.append(f"flex: {node['flex']};")
        if "backgroundColor" in node:
            styles.append(f"background-color: {node['backgroundColor']};")
        if "borderColor" in node:
            styles.append(f"border-color: {node['borderColor']};")
        if "borderWidth" in node:
            styles.append(f"border-width: {node['borderWidth']}; border-style: solid;")
        if "cornerRadius" in node:
            cr_map = {"xs": "4px", "sm": "6px", "md": "10px", "lg": "14px", "xl": "18px", "xxl": "24px"}
            styles.append(f"border-radius: {cr_map.get(node['cornerRadius'], node['cornerRadius'])};")
            
        p = node.get("paddingAll")
        if p:
            pm = {"xs": "4px", "sm": "8px", "md": "12px", "lg": "16px"}.get(p, "8px")
            styles.append(f"padding: {pm};")
        else:
            pt = node.get("paddingTop")
            pb = node.get("paddingBottom")
            ps = node.get("paddingStart")
            pe = node.get("paddingEnd")
            pad_map = {'xxs': '2px', 'xs': '3px', 'sm': '6px', 'md': '10px'}
            if pt: styles.append(f"padding-top: {pad_map.get(pt, pt)};")
            if pb: styles.append(f"padding-bottom: {pad_map.get(pb, pb)};")
            if ps: styles.append(f"padding-left: {pad_map.get(ps, ps)};")
            if pe: styles.append(f"padding-right: {pad_map.get(pe, pe)};")
            
        m = node.get("margin")
        if m:
            mm = {"xs": "4px", "sm": "8px", "md": "12px", "lg": "16px"}.get(m, "6px")
            styles.append(f"margin-top: {mm};")
            
        if "spacing" in node:
            sp = {"xs": "4px", "sm": "8px", "md": "12px"}.get(node["spacing"], "6px")
            styles.append(f"gap: {sp};")
            
        if node.get("alignItems") == "center":
            styles.append("align-items: center;")
            
        if "height" in node:
            styles.append(f"height: {node['height']};")
        if "width" in node:
            styles.append(f"width: {node['width']};")
            
        inner = "".join(flex_to_html(c) for c in node.get("contents", []))
        return f'<div style="{" ".join(styles)}">{inner}</div>'
        
    if ntype == "text":
        styles = []
        size_map = {"xxs": "10.5px", "xs": "12px", "sm": "13.5px", "md": "15px", "lg": "17px", "xl": "19px"}
        if "size" in node:
            styles.append(f"font-size: {size_map.get(node['size'], '12.5px')};")
        if node.get("weight") == "bold":
            styles.append("font-weight: 700;")
        if "color" in node:
            styles.append(f"color: {node['color']};")
        if "align" in node:
            align_val = "right" if node['align'] == "end" else ("center" if node['align'] == "center" else "left")
            styles.append(f"text-align: {align_val};")
        if "flex" in node:
            styles.append(f"flex: {node['flex']};")
        if node.get("wrap"):
            styles.append("white-space: pre-wrap; word-break: break-word;")
        else:
            styles.append("white-space: nowrap; overflow: hidden; text-overflow: ellipsis;")
        if "margin" in node:
            m = {"xs": "2px", "sm": "4px", "md": "8px"}.get(node["margin"], "2px")
            styles.append(f"margin-top: {m};")
            
        txt = node.get("text", "").replace("\n", "<br/>")
        return f'<div style="{" ".join(styles)}">{txt}</div>'
        
    if ntype == "separator":
        col = node.get("color", "#e2e8f0")
        m = node.get("margin", "sm")
        mg = {"xs": "3px 0", "sm": "6px 0", "md": "10px 0"}.get(m, "6px 0")
        return f'<hr style="border: none; border-top: 1px solid {col}; margin: {mg}; width: 100%;" />'
        
    if ntype == "filler":
        return '<div style="flex: 1;"></div>'
        
    if ntype == "button":
        action = node.get("action", {})
        label = action.get("label", "Bấm xem")
        btn_color = node.get("color", "#0284c7")
        return f'<div style="padding: 4px 8px; margin-top: 6px;"><button style="width: 100%; background: {btn_color}; color: #ffffff; border: none; border-radius: 8px; padding: 10px 14px; font-weight: 700; font-size: 13px; cursor: pointer; box-shadow: 0 4px 10px rgba(2,132,199,0.3); display: flex; align-items: center; justify-content: center; gap: 6px;">{label}</button></div>'
        
    return ""

html_p1 = flex_to_html(bubble_p1)
html_p2 = flex_to_html(bubble_p2)
html_nv_cards = "".join(f'<div class="msg-section-title" style="margin-top: 12px; margin-bottom: 6px;">Thẻ {i+3}: Chi Tiết Nhân Viên (Phần {i+1})</div>' + flex_to_html(b) for i, b in enumerate(nv_bubbles))

preview_html = f'''<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Preview RT1 - Redesign Thẻ Chi Tiết Doanh Thu Nhân Viên (LINE Flex Message)</title>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');

        * {{
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }}

        body {{
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            background: #0b1320;
            color: #f8fafc;
            min-height: 100vh;
            padding: 24px 16px 50px;
            display: flex;
            flex-direction: column;
            align-items: center;
        }}

        .top-banner {{
            max-width: 960px;
            width: 100%;
            background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
            border: 1px solid rgba(255, 255, 255, 0.12);
            border-radius: 16px;
            padding: 20px 24px;
            margin-bottom: 24px;
            box-shadow: 0 20px 40px -10px rgba(0,0,0,0.6);
        }}

        .top-banner h1 {{
            font-size: 20px;
            font-weight: 900;
            color: #38bdf8;
            margin-bottom: 8px;
            display: flex;
            align-items: center;
            gap: 10px;
        }}

        .top-banner p {{
            font-size: 13.5px;
            color: #cbd5e1;
            line-height: 1.6;
        }}

        .diff-grid {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 14px;
            margin-top: 14px;
        }}

        .diff-card {{
            border-radius: 10px;
            padding: 12px 14px;
            font-size: 12.5px;
            line-height: 1.5;
        }}

        .diff-bad {{
            background: rgba(239, 68, 68, 0.12);
            border: 1px solid rgba(239, 68, 68, 0.3);
            color: #fca5a5;
        }}

        .diff-good {{
            background: rgba(16, 185, 129, 0.12);
            border: 1px solid rgba(16, 185, 129, 0.35);
            color: #6ee7b7;
        }}

        .diff-card strong {{
            display: block;
            margin-bottom: 6px;
            font-size: 13px;
        }}

        /* PHÒNG CHAT LINE GIẢ LẬP */
        .chat-container {{
            display: flex;
            gap: 28px;
            justify-content: center;
            align-items: flex-start;
            flex-wrap: wrap;
            max-width: 1200px;
            width: 100%;
        }}

        .phone-frame {{
            width: 380px;
            background: #849ebf;
            border-radius: 40px;
            border: 10px solid #1e293b;
            box-shadow: 0 30px 60px -15px rgba(0, 0, 0, 0.8), 0 0 0 1px rgba(255, 255, 255, 0.1);
            overflow: hidden;
            display: flex;
            flex-direction: column;
            position: relative;
        }}

        .phone-header {{
            background: #202b38;
            color: #fff;
            padding: 12px 16px;
            display: flex;
            align-items: center;
            gap: 12px;
            font-size: 13.5px;
            font-weight: 700;
        }}

        .phone-avatar {{
            width: 34px;
            height: 34px;
            border-radius: 50%;
            background: #0284c7;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 900;
        }}

        .phone-screen {{
            padding: 16px 12px;
            max-height: 80vh;
            overflow-y: auto;
            display: flex;
            flex-direction: column;
            gap: 12px;
        }}

        .msg-bubble-user {{
            align-self: flex-end;
            background: #61d856;
            color: #0f172a;
            font-weight: 700;
            padding: 8px 14px;
            border-radius: 16px 16px 2px 16px;
            font-size: 13.5px;
            box-shadow: 0 2px 6px rgba(0,0,0,0.15);
        }}

        .msg-section-title {{
            font-size: 11px;
            font-weight: 800;
            color: #ffffff;
            background: rgba(0, 0, 0, 0.4);
            padding: 4px 10px;
            border-radius: 12px;
            align-self: center;
            letter-spacing: 0.3px;
        }}

        .highlight-border {{
            outline: 3px solid #f97316;
            outline-offset: 3px;
        }}

        .carousel-track {{
            display: flex;
            flex-direction: row;
            gap: 12px;
            overflow-x: auto;
            scroll-snap-type: x mandatory;
            padding-bottom: 8px;
            scrollbar-width: thin;
        }}

        .carousel-track::-webkit-scrollbar {{
            height: 6px;
        }}

        .carousel-track::-webkit-scrollbar-thumb {{
            background: rgba(255, 255, 255, 0.35);
            border-radius: 6px;
        }}

        .carousel-hint {{
            font-size: 11px;
            color: #ffedd5;
            background: rgba(234, 88, 12, 0.35);
            border: 1px dashed #f97316;
            border-radius: 8px;
            padding: 5px 10px;
            text-align: center;
            margin-bottom: 8px;
            font-weight: 600;
        }}
    </style>
</head>
<body>

    <div class="top-banner">
        <h1>✨ GOM THÀNH CAROUSEL NẰM CÙNG 1 HÀNG NGANG (LỆNH RT1)</h1>
        <p>
            Đã chuyển Thẻ 3 (P.1/2) và Thẻ 4 (P.2/2) vào cùng <strong>1 hàng Carousel lướt ngang</strong> như Thẻ 1 và Thẻ 2. Người xem chỉ cần vuốt nhẹ sang phải là xem tiếp Top 6 - 10 mà không bị thành 2 tin nhắn dọc rời rạc.
        </p>

        <div class="diff-grid">
            <div class="diff-card diff-bad">
                <strong>❌ Trạng Thái Trước Đó:</strong>
                • Thẻ 3 và Thẻ 4 gửi thành 2 tin nhắn dọc (xếp chồng lên nhau).<br/>
                • Người xem phải cuộn tin nhắn xuống dưới.<br/>
                • Không đồng bộ trải nghiệm với Thẻ 1 & Thẻ 2.
            </div>

            <div class="diff-card diff-good">
                <strong>✅ Trạng Thái Mới (Chuẩn Carousel Ngang):</strong>
                • <strong>Cùng 1 hàng Carousel</strong>: Vuốt ngang mượt mà giữa Thẻ 3 và Thẻ 4.<br/>
                • <strong>Dung lượng an toàn tuyệt đối</strong>: Carousel 2 thẻ NV chỉ ~39KB (&lt; 50KB giới hạn LINE).<br/>
                • <strong>Đồng bộ trải nghiệm</strong>: Cả 2 tin nhắn của RT1 đều là Carousel trượt ngang hiện đại.
            </div>
        </div>
    </div>

    <div class="chat-container">
        <!-- CỘT 1: TIN NHẮN 2 - CAROUSEL CHI TIẾT NHÂN VIÊN (NẰM CÙNG 1 HÀNG) -->
        <div style="display: flex; flex-direction: column; align-items: center;">
            <div style="margin-bottom: 8px; font-weight: 800; font-size: 14px; color: #f97316; text-transform: uppercase;">
                ⭐ TIN NHẮN 2: CAROUSEL CHI TIẾT NHÂN VIÊN (TRƯỢT NGANG)
            </div>
            <div class="phone-frame highlight-border">
                <div class="phone-header">
                    <div class="phone-avatar">D</div>
                    <div>
                        <div>DMX SAVICO BOT</div>
                        <div style="font-size: 10px; color: #94a3b8; font-weight: 500;">Tin nhắn 2/2 • Carousel 2 Thẻ Lướt Ngang</div>
                    </div>
                </div>
                <div class="phone-screen">
                    <div class="msg-bubble-user">rt1</div>
                    <div class="carousel-hint">👉 Vuốt hoặc cuộn chuột sang phải để xem Thẻ (P.2/2)</div>
                    <div class="carousel-track">
                        {html_nv_cards}
                    </div>
                </div>
            </div>
        </div>

        <!-- CỘT 2: TIN NHẮN 1 - CAROUSEL DOANH THU & THI ĐUA (NẰM CÙNG 1 HÀNG) -->
        <div style="display: flex; flex-direction: column; align-items: center;">
            <div style="margin-bottom: 8px; font-weight: 800; font-size: 14px; color: #38bdf8; text-transform: uppercase;">
                📱 TIN NHẮN 1: CAROUSEL TỔNG QUAN (TRƯỢT NGANG)
            </div>
            <div class="phone-frame">
                <div class="phone-header">
                    <div class="phone-avatar">D</div>
                    <div>
                        <div>DMX SAVICO BOT</div>
                        <div style="font-size: 10px; color: #94a3b8; font-weight: 500;">Tin nhắn 1/2 • Carousel 2 Thẻ Lướt Ngang</div>
                    </div>
                </div>
                <div class="phone-screen">
                    <div class="msg-bubble-user">rt1</div>
                    <div class="carousel-hint" style="background: rgba(2, 132, 199, 0.35); border-color: #38bdf8; color: #e0f2fe;">👉 Vuốt hoặc cuộn chuột sang phải để xem Thẻ Thi Đua</div>
                    <div class="carousel-track">
                        {html_p1}
                        {html_p2}
                    </div>
                </div>
            </div>
        </div>
    </div>

</body>
</html>
'''

output_file = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "preview_rt1_detail.html"))
with open(output_file, "w", encoding="utf-8") as f:
    f.write(preview_html)

print("Xuat file thanh cong toi preview_rt1_detail.html")
