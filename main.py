import os
import asyncio
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ConversationHandler, ContextTypes
from playwright.async_api import async_playwright

# Các hằng số
URL_DASHBOARD = "https://ganang1312.github.io/DMX-Savico-Dashboard/trang-chu.html"
URL_BCKD = "https://bi.thegioididong.com/sieu-thi-con?id=16753&tab=bcdtnh&rt=2&dm=1"
URL_THIDUA = "https://bi.thegioididong.com/thi-dua-st?id=16753&tab=1&rt=2&dm=2&mt=1"
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN') # Lấy token từ biến môi trường của Railway

# Các trạng thái của cuộc hội thoại
GET_USERNAME, GET_SSO = range(2)

# --- Phần logic tự động hóa với Playwright ---
async def run_automation_workflow(username, sso_token):
    """
    Hàm này thực hiện toàn bộ quy trình tự động hóa trên trình duyệt.
    """
    async with async_playwright() as p:
        # Khởi chạy trình duyệt Chromium ở chế độ ẩn danh (headless)
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        try:
            # === BƯỚC 1 & 2: Xử lý Dữ liệu Báo cáo Kinh doanh ===
            print("Đang xử lý Báo cáo Kinh doanh...")
            # Truy cập trang đăng nhập trước
            await page.goto(URL_BCKD)
            await page.wait_for_load_state('networkidle')

            # --- BẮT ĐẦU THAY THẾ TỪ ĐÂY ---

            # Điền Tên đăng nhập
            await page.fill("#txtUsername", username)
            
            # Điền mã SSO vào từng ô nhỏ
            for index, digit in enumerate(sso_token):
                selector = f"#digitnumber{index + 1}"
                await page.fill(selector, digit)
            
            # Bấm nút đăng nhập cuối cùng
            await page.click(".button-submit") 
            
            # --- KẾT THÚC THAY THẾ TẠI ĐÂY ---

            # Chờ đăng nhập thành công và tải lại trang BI
            await page.wait_for_load_state('networkidle')
            await page.goto(URL_BCKD)
            await page.wait_for_load_state('networkidle')

            # Lấy toàn bộ nội dung HTML của trang
            bckd_data = await page.content()

            # Quay lại trang dashboard chính
            await page.goto(URL_DASHBOARD)
            await page.wait_for_load_state('networkidle')

            # Bấm nút dán tương ứng
            await page.click('button[data-target="mainDataInput"]')


            # === BƯỚC 3: Xử lý Dữ liệu Thi đua ===
            print("Đang xử lý Dữ liệu Thi đua...")
            await page.goto(URL_THIDUA)
            await page.wait_for_load_state('networkidle')

            # Vì đã đăng nhập nên có thể vào thẳng
            thidua_data = await page.content()

            # Quay lại trang dashboard chính
            await page.goto(URL_DASHBOARD)
            await page.wait_for_load_state('networkidle')

            # Bấm nút dán tương ứng
            await page.click('button[data-target="contestDataInput"]')

            # === BƯỚC 4: Tạo Dashboard, chụp ảnh và lấy nhận xét ===
            print("Đang tạo Dashboard và chụp ảnh...")
            
            # Bấm nút tạo dashboard - THAY THẾ DÒNG NÀY
            await page.click('#analyzeBtn') 
            await page.wait_for_timeout(5000) # Chờ 5s để dashboard render

            # Bắt sự kiện download và bấm nút chụp ảnh
            async with page.expect_download() as download_info:
            # THAY THẾ SELECTOR TRONG DÒNG NÀY
            await page.click('#captureBtdLuyKe')
        
            download = await download_info.value
            image_path = f"/tmp/{download.suggested_filename}"
            await download.save_as(image_path)
            print(f"Ảnh đã được tải về tại: {image_path}")

            # Bấm nút sao chép nhận xét - THAY THẾ DÒNG NÀY
            await page.click('.copySummaryBtn')
            
            # Lấy nội dung từ clipboard của trình duyệt ảo
            comment_text = await page.evaluate("() => navigator.clipboard.readText()")
            print(f"Đã sao chép nhận xét: {comment_text}")

            await browser.close()
            return image_path, comment_text

        except Exception as e:
            print(f"Đã xảy ra lỗi: {e}")
            await browser.close()
            return None, None

# --- Phần xử lý lệnh Telegram ---

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
    username = context.user_data['username']

    await update.message.reply_text("Đã nhận đủ thông tin. Bắt đầu xử lý, vui lòng chờ trong giây lát...")

    # Chạy hàm tự động hóa
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

def main() -> None:
    """Hàm chính để chạy bot."""
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
