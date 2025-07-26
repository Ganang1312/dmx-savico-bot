from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import os
import time

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))
login_data = {"user": "", "sso": ""}

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("Bạn không có quyền dùng bot này.")
        return
    await update.message.reply_text("Nhập tài khoản đăng nhập:")

async def handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global login_data
    if not login_data["user"]:
        login_data["user"] = update.message.text
        await update.message.reply_text("Nhập mã định danh SSO:")
    else:
        login_data["sso"] = update.message.text
        await update.message.reply_text("Đang xử lý, vui lòng chờ...")
        await run_bot(context, update)

async def run_bot(context, update):
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=options)

    def login(url):
        driver.get(url)
        time.sleep(2)
        if "txtUserName" in driver.page_source:
            driver.find_element(By.ID, "txtUserName").send_keys(login_data['user'])
            driver.find_element(By.ID, "txtMaDinhDanh").send_keys(login_data['sso'])
            driver.find_element(By.ID, "btnLogin").click()
            time.sleep(5)

    # Mở trang dashboard
    driver.get("https://ganang1312.github.io/DMX-Savico-Dashboard/trang-chu.html")
    time.sleep(2)

    # BCKD
    driver.execute_script("window.open('https://bi.thegioididong.com/sieu-thi-con?id=16753&tab=bcdtnh&rt=2&dm=1', '_blank');")
    driver.switch_to.window(driver.window_handles[1])
    login(driver.current_url)
    bckd = driver.find_element(By.TAG_NAME, "body").text
    driver.close()
    driver.switch_to.window(driver.window_handles[0])
    driver.execute_script(f"document.querySelector('#textareaKD').value = `{bckd}`;")
    driver.find_element(By.ID, "btnPasteKD").click()
    time.sleep(1)

    # Thi đua
    driver.execute_script("window.open('https://bi.thegioididong.com/thi-dua-st?id=16753&tab=1&rt=2&dm=2&mt=1', '_blank');")
    driver.switch_to.window(driver.window_handles[1])
    login(driver.current_url)
    td = driver.find_element(By.TAG_NAME, "body").text
    driver.close()
    driver.switch_to.window(driver.window_handles[0])
    driver.execute_script(f"document.querySelector('#textareaTD').value = `{td}`;")
    driver.find_element(By.ID, "btnPasteTD").click()
    time.sleep(1)

    # Tạo dashboard & chụp ảnh
    driver.find_element(By.ID, "btnTaoDashboard").click()
    time.sleep(2)
    driver.save_screenshot("dashboard.png")
    driver.find_element(By.ID, "btnSaoChepNhanXet").click()
    nhanxet = driver.execute_script("return document.querySelector('#textareaNhanXet').value")
    driver.quit()

    await context.bot.send_photo(chat_id=update.effective_chat.id, photo=open("dashboard.png", "rb"))
    await context.bot.send_message(chat_id=update.effective_chat.id, text="📋 Nhận xét:\n" + nhanxet)

if __name__ == '__main__':
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handler))
    app.run_polling()
