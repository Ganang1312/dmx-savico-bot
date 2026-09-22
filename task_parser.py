# -*- coding: utf-8 -*-
"""
Tách lệnh giao việc cho bot LINE — logic thuần, test được offline.

CÚ PHÁP LỆNH `việc` (mở rộng 22/09/2026 — VẪN NHẬN cú pháp cũ)

    việc <ĐÍCH hoặc TÊN VIỆC>
    - <nội dung, được phép chèn @... ở BẤT KỲ đâu trong dòng>

Bốn cách viết dưới đây đều chạy:

    việc @all:            việc Giá tivi          việc
    - Vệ sinh quầy        - @tuấn @linh          - @tuấn giá tv
                                                 - giá tủ @linh

ĐÍCH (@...) nhận được — HAI LOẠI, KHÁC NHAU VỀ CÁCH XỬ LÝ:

  1) NHÓM — phải đọc đúng từ sheet `schedules` (3 cột E/F/G):
       @all    -> PG + NV + QL + TC
       @nv     -> chỉ NV
       @pg     -> chỉ PG
       @st     -> NV + QL + TC   (nhân viên siêu thị, KHÔNG gồm PG)
       @ql     -> chỉ QL + TC
       @tc     -> chỉ QL + TC

  2) ĐÍCH DANH MỘT NGƯỜI — DÙNG NGUYÊN VĂN, KHÔNG đối chiếu sheet:
       @tuấn          -> ghi đúng chữ "tuấn"
       @Yến Sony      -> ghi đúng chữ "Yến Sony"
       @Khánh-97536   -> ghi đúng chữ "Khánh-97536"
       @156494        -> ngoại lệ: số thuần thì tra mã trong sheet cho ra tên thật
     Anh Dương chốt 22/09/2026: anh biết mình gọi ai, bot chỉ ghi lại đúng tên đó;
     gõ sai tên cũng KHÔNG báo lỗi, KHÔNG tự gán cho người khác.

Sheet chỉ được dùng để: (a) mở rộng 6 nhóm trên, (b) biết tên dài tới đâu khi
anh gõ tên có khoảng trắng, (c) tra mã số ở mục 2.
"""
import re

from meal_handler import clean_person_name, normalize_person_key

# Nhóm đích -> các cột trong sheet schedules
GROUP_KEYWORDS = {
    "all": ("PG", "NV", "QL+TC"),
    "nv": ("NV",),
    "pg": ("PG",),
    "st": ("NV", "QL+TC"),
    "ql": ("QL+TC",),
    "tc": ("QL+TC",),
}

GROUP_LABEL = {
    "all": "toàn siêu thị (PG + NV + QL + TC)",
    "nv": "nhân viên (NV)",
    "pg": "PG",
    "st": "nhân viên siêu thị (NV + QL + TC)",
    "ql": "quản lý + trưởng ca",
    "tc": "quản lý + trưởng ca",
}

# Các chữ "rỗng" hay gặp trong ô lịch — không phải tên người
PLACEHOLDER = {
    "", "khong co", "khong", "trong", "n a", "na", "none", "null",
    "khong co ai", "chua co", "-", "--", "0",
}

# Ký tự bắt đầu một dòng công việc
BULLETS = "-*•+–—"

# "việc @tuấn: dọn kho" -> khớp cả chuỗi
_ONE_LINE_RE = re.compile(r"^\s*\S+\s+@[^:]+:\s*(\S.*)$")


def is_work_command(lines):
    """
    True nếu các dòng này là một lệnh `việc`.

    Nhận 2 dạng:
      - nhiều dòng: dòng đầu bắt đầu bằng 'việc' + có ít nhất 1 dòng gạch đầu dòng
      - MỘT dòng:   "việc @tuấn: dọn kho"  (dấu ':' ngay sau tên người)
    """
    lines = [l.strip() for l in lines if l.strip()]
    if not lines:
        return False
    head = lines[0].lower()
    if not (head.startswith("việc") or head.startswith("viec")):
        return False
    if len(lines) >= 2 and lines[1][:1] in BULLETS:
        return True
    return bool(_ONE_LINE_RE.match(lines[0]))


def _expand_one_line(lines):
    """Biến 'việc @tuấn: dọn kho' thành ['việc @tuấn', '- dọn kho']."""
    if len(lines) != 1:
        return lines
    s = lines[0]
    i = s.find(":")
    if i == -1 or "@" not in s[:i]:
        return lines
    return [s[:i].strip() + ":", "- " + s[i + 1:].strip()]


def is_placeholder(name):
    """True nếu chuỗi không phải tên người (vd 'Không có')."""
    return normalize_person_key(name) in PLACEHOLDER


def split_names(cell):
    """Tách một ô sheet thành danh sách tên đã làm sạch và khử trùng."""
    if not cell:
        return []
    out, seen = [], set()
    for token in re.split(r"[,\n;•+]", str(cell)):
        name = clean_person_name(token)
        if not name or name.isdigit() or len(name) < 2:
            continue
        if is_placeholder(name):
            continue
        key = normalize_person_key(name)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(name)
    return out


def extract_code(name):
    """Lấy mã số trong tên dạng 'Thảo-156494*' -> '156494'."""
    m = re.search(r"[-–]\s*(\d{2,8})", str(name))
    return m.group(1) if m else None


def build_token_index(names):
    """
    Trả về list [(token, tên_đầy_đủ)] sắp theo độ dài GIẢM DẦN.
    token gồm: tên đầy đủ, mã số, và 'tên gọi' (phần trước dấu gạch).
    Sắp dài trước để '@Yến Sony' không bị cắt thành '@Yến'.
    """
    idx = []
    for nm in names:
        idx.append((nm, nm))
        code = extract_code(nm)
        if code:
            idx.append((code, nm))
        alias = re.split(r"\s*[-–]\s*", nm)[0].strip()
        if alias and alias != nm and len(alias) >= 2:
            idx.append((alias, nm))
    idx.sort(key=lambda t: len(t[0]), reverse=True)
    return idx


def build_known_tokens(index):
    """Danh sách token để dò sau dấu @, dài trước ngắn sau."""
    toks = [t for t, _ in index] + list(GROUP_KEYWORDS.keys())
    return sorted(set(toks), key=len, reverse=True)


def hint(err, names):
    if err == "ambiguous":
        return " Trùng %d người: %s." % (len(names), ", ".join(names))
    if err == "empty":
        return " Nhóm này đang không có ai trong danh sách."
    return " Không có trong danh sách nhân sự của siêu thị."


def resolve_target(token, roster_names, index):
    """
    Trả về (danh_sách_tên, lỗi). lỗi ∈ (None, 'empty').

    QUY ƯỚC ANH DƯƠNG CHỐT 22/09/2026:
      • @all / @nv / @pg / @st / @ql / @tc
            -> ĐỌC TỪ SHEET (3 cột E/F/G). Chỉ nhóm mới cần khớp sheet.
      • Gọi ĐÍCH DANH một người (@tuấn, @Yến Sony, @Khánh-97536)
            -> DÙNG NGUYÊN VĂN thứ anh gõ. KHÔNG đối chiếu sheet, KHÔNG báo lỗi.
               Anh biết mình gọi ai; bot chỉ ghi lại đúng tên đó.
      • Riêng token là SỐ THUẦN (@156494) thì tra mã trong sheet cho ra tên thật,
        vì để nguyên "156494" trên thẻ thì vô nghĩa. Không tra được thì giữ nguyên.
    """
    t = str(token).strip()
    low = t.lower()

    if low in GROUP_KEYWORDS:
        out = []
        for grp in GROUP_KEYWORDS[low]:
            for nm in roster_names.get(grp, []):
                if nm not in out:
                    out.append(nm)
        return (out, None) if out else ([], "empty")

    # Số thuần -> tra mã nhân viên
    if re.fullmatch(r"\d{2,8}", t):
        cand = []
        for tok, nm in index:
            if tok == t and nm not in cand:
                cand.append(nm)
        if len(cand) == 1:
            return (cand, None)

    # Mặc định: dùng NGUYÊN VĂN
    return ([t], None) if t else ([], None)


def split_line(line, known_tokens, known_norm=None, allow_colon=False):
    """
    Tách 1 dòng thành (nội_dung_việc, [đích...]). @ nhận ở BẤT KỲ vị trí nào.

    Tên dài bao nhiêu từ? Bot dò các TIỀN TỐ 1..4 từ rồi đem so với danh sách
    tên trong sheet (bỏ dấu, không phân biệt hoa thường) để biết tên kết thúc ở
    đâu. Nhờ vậy:
      • "@yen sony dán tem" tách đúng thành tên "yen sony" + việc "dán tem"
        (cách so startswith cũ chỉ lấy được 1 từ vì tên trong sheet có dấu).
      • CHỮ ANH GÕ được giữ nguyên — "@yến sony" ra "yến sony", không bị thay
        bằng "Yến Sony" trong sheet.

    allow_colon=True (CHỈ dùng cho DÒNG ĐẦU): nếu không khớp tên nào trong sheet
    thì dấu ':' KẾT THÚC TÊN — cứu tên nhiều từ chưa có trong sheet.
    KHÔNG bật cho các dòng việc, nếu không "- @tuấn việc: gấp" sẽ bị hiểu thành
    tên "tuấn việc".
    """
    if known_norm is None:
        known_norm = {normalize_person_key(t) for t in known_tokens}

    spans, tokens = [], []
    i = 0
    while True:
        at = line.find("@", i)
        if at == -1:
            break
        rest = line[at + 1:]

        # 1) Dò tiền tố 1..4 từ xem có khớp tên nào trong sheet không.
        best = ""
        words = re.split(r"\s+", rest)
        for n in range(min(4, len(words)), 0, -1):
            cand = " ".join(words[:n]).rstrip(",;:.")
            if cand and normalize_person_key(cand) in known_norm:
                best = cand
                break

        # 2) Dòng đầu + không khớp sheet -> dấu ':' kết thúc tên
        if not best and allow_colon:
            colon = rest.find(":")
            nxt = rest.find("@")
            if colon != -1 and (nxt == -1 or colon < nxt):
                cand = rest[:colon].strip()
                if cand:
                    best = cand

        # 3) Vẫn không ra -> lấy 1 từ.
        if not best:
            m = re.match(r"[^\s,;:@]+", rest)
            best = m.group(0) if m else ""

        spans.append((at, at + 1 + len(best)))
        tokens.append(best)
        i = at + 1 + len(best)

    text = line
    for a, b in reversed(spans):
        text = text[:a] + " " + text[b:]
    text = re.sub(r"[:;,\-–—•+]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text, [t for t in tokens if t]


def parse_work_command(lines, roster_names):
    """
    Phân tích toàn bộ lệnh `việc`.

    Trả về dict:
      job          : tên công việc chung ở dòng đầu (None nếu dòng đầu là đích)
      header_names : người nhận việc lấy từ dòng đầu
      assignments  : [(tên_việc, [người...])]
      errors       : [chuỗi mô tả lỗi]
    """
    index = build_token_index(
        [nm for grp in roster_names.values() for nm in grp])
    known = build_known_tokens(index)
    known_norm = {normalize_person_key(t) for t in known}

    # Dạng 1 dòng "việc @tuấn: dọn kho" -> tách thành dòng đầu + 1 dòng việc
    lines = _expand_one_line([l.strip() for l in lines if l.strip()])

    header = (lines[0] if lines else "").strip()
    header_body = re.sub(r"^(việc|viec)\b", "", header, flags=re.IGNORECASE).strip()
    header_text, header_tokens = split_line(header_body, known, known_norm,
                                            allow_colon=True)

    header_names, errors = [], []
    _seen_h = set()
    for t in header_tokens:
        names, err = resolve_target(t, roster_names, index)
        if err:
            errors.append("Dòng đầu: không hiểu “@%s”.%s" % (t, hint(err, names)))
            continue
        for nm in names:
            # Khử trùng theo tên chuẩn hoá nhưng GIỮ cách viết lần đầu anh gõ,
            # để "@tuấn @Tuấn @TUAN" không thành 3 dòng việc.
            # normalize_person_key('') với chuỗi chỉ có chữ số, nên phải có
            # fallback, nếu không mã số lạ sẽ bị bỏ âm thầm.
            k = normalize_person_key(nm) or nm.lower().strip()
            if not k or k in _seen_h:
                continue
            _seen_h.add(k)
            header_names.append(nm)

    job = header_text or None
    assignments = []

    for raw in lines[1:]:
        s = raw.strip()
        if not s or s[0] not in BULLETS:
            continue
        body = s[1:].strip()
        if not body:
            continue

        text, tokens = split_line(body, known, known_norm)

        names = []
        _seen_n = set()
        for t in tokens:
            rn, err = resolve_target(t, roster_names, index)
            if err:
                errors.append("Dòng “%s”: không hiểu “@%s”.%s" % (s, t, hint(err, rn)))
                continue
            for nm in rn:
                k = normalize_person_key(nm) or nm.lower().strip()
                if not k or k in _seen_n:
                    continue
                _seen_n.add(k)
                names.append(nm)

        if not names:
            if tokens:
                # Dòng CÓ ghi "@..." nhưng không ai khớp -> TUYỆT ĐỐI không được
                # tự ý gán cho cả nhóm ở dòng đầu, vì như vậy là giao việc sai
                # người mà người gửi không hề biết. Bỏ dòng, chỉ báo lỗi.
                continue
            names = list(header_names)
        if not names:
            errors.append("Dòng “%s”: chưa chỉ định ai làm." % s)
            continue

        if job:
            task = ("%s | %s" % (job, text)) if text else job
        else:
            task = text
        if not task:
            errors.append("Dòng “%s”: thiếu nội dung công việc." % s)
            continue

        assignments.append((task, names))

    # Dạng GỌN một dòng: "việc @tuấn giá tv" — dòng đầu đã có cả người nhận
    # lẫn nội dung việc, không cần dòng "-" nào nữa. Trước đây rơi vào lỗi
    # "Chưa có dòng công việc nào" nên anh Dương gõ mà bot không nhận.
    if not assignments and not errors and job and header_names:
        assignments.append((job, list(header_names)))
        job = None  # để plan_assignments trả thẻ CÔNG VIỆC CHUNG như cũ

    if not assignments and not errors:
        errors.append("Chưa có dòng công việc nào (mỗi việc bắt đầu bằng dấu “-”).")

    return {"job": job, "header_names": header_names,
            "assignments": assignments, "errors": errors}


def plan_assignments(parsed):
    """
    Quyết định bot sẽ LƯU việc như thế nào (thuần logic, test được offline).

    Vì sao cần: nếu mỗi dòng việc gọi add_all_adhoc_tasks() một lần thì mỗi
    dòng sinh một mã nhóm khác nhau -> thẻ Flex chỉ hiện được nhóm CUỐI và nút
    🗑 cũng chỉ hủy được nhóm cuối. Nên mọi trường hợp nhiều dòng phải gom vào
    MỘT nhóm.

    Trả về dict:
      mode : 'all'   -> gọi add_all_adhoc_tasks(group_id, names, task)
             'multi' -> gọi add_multi_adhoc_tasks(group_id, job, pairs)
      job  : tên hiện trên thẻ Flex
      task : (mode 'all') tên việc
      names: (mode 'all') danh sách người nhận
      pairs: (mode 'multi') [(việc_con, người), ...]
    """
    assigns = parsed["assignments"]
    job = parsed["job"]

    # Dòng đầu chỉ có đích + đúng 1 dòng việc -> giữ thẻ "CÔNG VIỆC CHUNG" cũ.
    if job is None and len(assigns) == 1:
        task, names = assigns[0]
        return {"mode": "all", "job": task, "task": task, "names": list(names)}

    card_job = job if job else "Việc phát sinh"
    pairs = []

    # Dòng đầu có CẢ đích LẪN nội dung việc, mà bên dưới vẫn có dòng "-":
    # việc ở dòng đầu là giao cho cả nhóm đó. Phải thêm vào, nếu không nhóm
    # ở dòng đầu bị bỏ âm thầm (xem P11 trong tools/).
    if job and parsed["header_names"]:
        for n in parsed["header_names"]:
            pairs.append((job, n))

    for task_name, names in assigns:
        sub = task_name.split(" | ", 1)[-1]
        for n in names:
            pairs.append((sub, n))
    return {"mode": "multi", "job": card_job, "pairs": pairs}
