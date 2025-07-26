import os
import asyncio
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ConversationHandler, ContextTypes
from playwright.async_api import async_playwright

# --- CÁC HẰNG SỐ ---
# URL trang dashboard chính
URL_DASHBOARD = "https://ganang1312.github.io/DMX-Savico-Dashboard/trang-chu.html"
# URL trang Báo cáo kinh doanh (cũng là trang đăng nhập)
URL_BCKD = "https://bi.thegioididong.com/sieu-thi-con?id=16753&tab=bcdtnh&rt=2&dm=1"
# URL trang Thi đua
URL_THIDUA = "https://bi.thegioididong.com/thi-dua-st?id=16753&tab=1&rt=2&dm=2&mt=1"
# Lấy token từ biến môi trường của Render
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')

# --- TRẠNG THÁI HỘI THOẠI ---
GET_USERNAME, GET_SSO = range(2)


# --- HÀM TỰ ĐỘNG HÓA VỚI PLAYWRIGHT ---
async def run_automation_workflow(username, sso_token):
    """
    Hàm này thực hiện toàn bộ quy trình tự động hóa trên trình duyệt.
    """
    print("Bắt đầu quy trình tự động hóa...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        try:
            # === PHẦN 1: ĐĂNG NHẬP ===
            print("Đang truy cập trang đăng nhập...")
            # Truy cập trang và tăng thời gian chờ lên 90 giây
            await page.goto(URL_BCKD, timeout=90000)
            await page.wait_for_load_state('networkidle')

            # Điền Tên đăng nhập
            await page.fill("#txtUsername", username)

            # Điền mã SSO vào từng ô nhỏ
            for index, digit in enumerate(sso_token):
                selector = f"#digitnumber{index + 1}"
                await page.fill(selector, digit)

            # Bấm nút đăng nhập
            await page.click(".button-submit")
            print("Đăng nhập thành công.")
            # Chờ trang sau đăng nhập tải xong
            await page.wait_for_load_state('networkidle')

            # === PHẦN 2: LẤY VÀ DÁN DỮ LIỆU BÁO CÁO KINH DOANH ===
            print("Đang xử lý Báo cáo Kinh doanh...")
            await page.goto(URL_BCKD, timeout=90000)
            await page.wait_for_load_state('networkidle')
            bckd_data = await page.content()

            await page.goto(URL_DASHBOARD)
            await page.wait_for_load_state('networkidle')
            await page.fill("#mainDataInput", bckd_data)
            await page.click('button[data-target="mainDataInput"]')
            print("Đã dán dữ liệu Báo cáo Kinh doanh.")

            # === PHẦN 3: LẤY VÀ DÁN DỮ LIỆU THI ĐUA ===
            print("Đang xử lý Dữ liệu Thi đua...")
            await page.goto(URL_THIDUA, timeout=90000)
            await page.wait_for_load_state('networkidle')
            thidua_data = await page.content()

            # Quay lại dashboard để dán dữ liệu Thi đua
            await page.goto(URL_DASHBOARD)
            await page.wait_for_load_state('networkidle')
            await page.fill("#contestDataInput", thidua_data)
            await page.click('button[data-target="contestDataInput"]')
            print("Đã dán dữ liệu Thi đua.")

            # === PHẦN 4: TẠO DASHBOARD, CHỤP ẢNH VÀ LẤY NHẬN XÉT ===
            print("Đang tạo Dashboard và chụp ảnh...")
            # Bấm nút tạo Dashboard
            await page.click('#analyzeBtn')
            # Chờ 5 giây để dashboard có thời gian render
            await page.wait_for_timeout(5000)

            # Chờ và bắt sự kiện download file ảnh
            async with page.expect_download() as download_info:
                await page.click('#captureBtdLuyKe')

            download = await download_info.value
            # Lưu ảnh vào thư mục tạm của hệ thống
            image_path = f"/tmp/{download.suggested_filename}"
            await download.save_as(image_path)
            print(f"Ảnh đã được tải về tại: {image_path}")

            # Bấm nút sao chép và lấy nhận xét từ clipboard
            await page.click('.copySummaryBtn')
            comment_text = await page.evaluate("() => navigator.clipboard.readText()")
            print(f"Đã sao chép nhận xét: {comment_text}")

            # Đóng trình duyệt và trả về kết quả
            await browser.close()
            return image_path, comment_text

        except Exception as e:
            # Bắt tất cả các lỗi xảy ra trong quá trình tự động hóa
            print(f"Đã xảy ra lỗi: {e}")
            await browser.close()
            return None, None


# --- CÁC HÀM XỬ LÝ LỆNH TELEGRAM ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Bắt đầu cuộc trò chuyện khi người dùng gửi /start."""
    await update.message.reply_text(
        "Chào bạn! Tôi là bot tự động hóa báo cáo.\n"
        "Để bắt đầu, vui lòng gửi Tên đăng nhập của bạn."
    )
    return GET_USERNAME

async def get_username(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Lưu tên đăng nhập và hỏi mã SSO."""
    context.user_data['username'] = update.message.text
    await update.message.reply_text("Cảm ơn! Bây giờ hãy gửi Mã định danh SSO của bạn.")
    return GET_SSO

async def get_sso_and_run(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Lưu mã SSO và bắt đầu quy trình tự động hóa."""
    sso_token = update.message.text
    username = context.user_data.get('username')

    if not username:
        await update.message.reply_text("Có lỗi xảy ra, không tìm thấy tên đăng nhập. Vui lòng bắt đầu lại với /start.")
        return ConversationHandler.END
        
    await update.message.reply_text("Đã nhận đủ thông tin. Bắt đầu xử lý, vui lòng chờ trong giây lát...")

    # Chạy hàm tự động hóa chính
    image_path, comment_text = await run_automation_workflow(username, sso_token)

    if image_path and comment_text:
        await update.message.reply_text("Đã xử lý xong! Dưới đây là kết quả:")
        # Gửi ảnh
        await context.bot.send_photo(chat_id=update.effective_chat.id, photo=open(image_path, 'rb'))
        # Gửi nhận xét
        await context.bot.send_message(chat_id=update.effective_chat.id, text=comment_text)
    else:
        await update.message.reply_text("Rất tiếc, đã có lỗi xảy ra trong quá trình tự động hóa. Vui lòng thử lại sau.")

    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Hủy cuộc trò chuyện."""
    await update.message.reply_text("Tác vụ đã được hủy.")
    return ConversationHandler.END


# --- HÀM CHÍNH ĐỂ CHẠY BOT ---
def main() -> None:
    """Hàm chính để thiết lập và chạy bot."""
    if not TELEGRAM_TOKEN:
        print("Lỗi: Chưa thiết lập biến môi trường TELEGRAM_TOKEN")
        return

    application = Application.builder().token(TELEGRAM_TOKEN).build()

    # Thiết lập ConversationHandler để hỏi tuần tự Tên đăng nhập và SSO
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            GET_USERNAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_username)],
            GET_SSO: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_sso_and_run)],
        },
        fallbacks=[CommandHandler('cancel', cancel)],
    )

    application.add_handler(conv_handler)

    # Chạy bot
    print("Bot đang chạy...")
    application.run_polling()

if __name__ == '__main__':
    main()
