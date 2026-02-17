import os
import asyncio
import logging
import base64
import psycopg2
from psycopg2.extras import RealDictCursor
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
from groq import Groq
import requests

DATABASE_URL = os.environ.get("DATABASE_URL")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "https://email-webhook-production-887d.up.railway.app")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

groq_client = Groq(api_key=GROQ_API_KEY)
conversations = {}
notified_emails = set()


def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def setup_database():
    logger.info("Ensuring tables exist...")
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE IF NOT EXISTS bots ("
                "id SERIAL PRIMARY KEY, "
                "user_id BIGINT NOT NULL, "
                "bot_token TEXT NOT NULL UNIQUE, "
                "bot_username TEXT, "
                "bot_name TEXT, "
                "model TEXT DEFAULT 'llama', "
                "personality TEXT, "
                "is_active BOOLEAN DEFAULT true, "
                "created_at TIMESTAMP DEFAULT NOW())"
            )
            cur.execute(
                "CREATE TABLE IF NOT EXISTS emails ("
                "id SERIAL PRIMARY KEY, "
                "bot_id INTEGER REFERENCES bots(id), "
                "from_email TEXT NOT NULL, "
                "to_email TEXT NOT NULL, "
                "subject TEXT, "
                "body_plain TEXT, "
                "body_html TEXT, "
                "received_at TIMESTAMP DEFAULT NOW(), "
                "read BOOLEAN DEFAULT FALSE, "
                "notified BOOLEAN DEFAULT FALSE)"
            )
            cur.execute("""
                DO $$ 
                BEGIN 
                    ALTER TABLE emails ADD COLUMN notified BOOLEAN DEFAULT FALSE;
                EXCEPTION WHEN duplicate_column THEN NULL;
                END $$;
            """)
            cur.execute(
                "CREATE TABLE IF NOT EXISTS files ("
                "id SERIAL PRIMARY KEY, "
                "bot_id INTEGER REFERENCES bots(id), "
                "user_id BIGINT NOT NULL, "
                "file_id TEXT NOT NULL, "
                "file_unique_id TEXT NOT NULL, "
                "file_name TEXT, "
                "file_type TEXT, "
                "file_size INTEGER, "
                "caption TEXT, "
                "drive_link TEXT, "
                "uploaded_at TIMESTAMP DEFAULT NOW())"
            )
            # Add drive_link column if missing
            cur.execute("""
                DO $$ 
                BEGIN 
                    ALTER TABLE files ADD COLUMN drive_link TEXT;
                EXCEPTION WHEN duplicate_column THEN NULL;
                END $$;
            """)
            conn.commit()
    logger.info("Database ready!")


def get_active_bots():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM bots WHERE is_active = true")
            bots = cur.fetchall()
            logger.info("Found %d active bots", len(bots))
            return bots


# ============== EMAIL FUNCTIONS ==============

def get_emails_for_bot(bot_id, limit=5, unread_only=False):
    with get_db() as conn:
        with conn.cursor() as cur:
            if unread_only:
                cur.execute(
                    "SELECT * FROM emails WHERE bot_id = %s AND read = FALSE ORDER BY received_at DESC LIMIT %s",
                    (bot_id, limit)
                )
            else:
                cur.execute(
                    "SELECT * FROM emails WHERE bot_id = %s ORDER BY received_at DESC LIMIT %s",
                    (bot_id, limit)
                )
            return cur.fetchall()


def get_unnotified_emails(bot_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM emails WHERE bot_id = %s AND notified = FALSE ORDER BY received_at ASC",
                (bot_id,)
            )
            return cur.fetchall()


def mark_email_notified(email_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE emails SET notified = TRUE WHERE id = %s", (email_id,))
            conn.commit()


def mark_email_read(email_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE emails SET read = TRUE WHERE id = %s", (email_id,))
            conn.commit()


def get_email_by_id(email_id, bot_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM emails WHERE id = %s AND bot_id = %s", (email_id, bot_id))
            return cur.fetchone()


# ============== FILE FUNCTIONS ==============

def store_file(bot_id, user_id, file_id, file_unique_id, file_name, file_type, file_size, caption, drive_link=None):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO files (bot_id, user_id, file_id, file_unique_id, file_name, file_type, file_size, caption, drive_link)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                (bot_id, user_id, file_id, file_unique_id, file_name, file_type, file_size, caption, drive_link)
            )
            file_db_id = cur.fetchone()["id"]
            conn.commit()
            return file_db_id


def get_files_for_bot(bot_id, limit=10):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM files WHERE bot_id = %s ORDER BY uploaded_at DESC LIMIT %s",
                (bot_id, limit)
            )
            return cur.fetchall()


def get_file_by_id(file_db_id, bot_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM files WHERE id = %s AND bot_id = %s", (file_db_id, bot_id))
            return cur.fetchone()


def search_files(bot_id, query):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT * FROM files WHERE bot_id = %s 
                   AND (LOWER(file_name) LIKE %s OR LOWER(caption) LIKE %s)
                   ORDER BY uploaded_at DESC LIMIT 10""",
                (bot_id, f"%{query.lower()}%", f"%{query.lower()}%")
            )
            return cur.fetchall()


# ============== GOOGLE DRIVE FUNCTIONS ==============

def check_drive_connected(bot_id, user_id):
    """Check if user has connected Google Drive"""
    try:
        response = requests.get(
            f"{WEBHOOK_URL}/oauth/status",
            params={'bot_id': bot_id, 'user_id': user_id, 'provider': 'google'},
            timeout=5
        )
        if response.status_code == 200:
            return response.json().get('connected', False)
    except Exception as e:
        logger.error(f"Error checking Drive status: {e}")
    return False


def get_drive_auth_url(bot_id, user_id):
    """Get Google OAuth URL"""
    try:
        response = requests.get(
            f"{WEBHOOK_URL}/oauth/start",
            params={'bot_id': bot_id, 'user_id': user_id, 'provider': 'google'},
            timeout=5
        )
        if response.status_code == 200:
            return response.json().get('auth_url')
    except Exception as e:
        logger.error(f"Error getting auth URL: {e}")
    return None


def upload_to_drive(bot_id, user_id, file_name, file_content, folder_name="CrabPass"):
    """Upload file to user's Google Drive"""
    try:
        response = requests.post(
            f"{WEBHOOK_URL}/drive/upload",
            json={
                'bot_id': bot_id,
                'user_id': user_id,
                'file_name': file_name,
                'file_content': base64.b64encode(file_content).decode('utf-8'),
                'folder_name': folder_name
            },
            timeout=30
        )
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        logger.error(f"Error uploading to Drive: {e}")
    return None


# ============== COMMAND HANDLERS ==============

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot_username = context.bot_data.get("bot_username", "")
    name = context.bot.first_name
    email = bot_username.lower() + "@crabpass.ai" if bot_username else "your-bot@crabpass.ai"
    
    bot_id = context.bot_data.get("bot_id", 0)
    user_id = update.effective_user.id
    drive_connected = check_drive_connected(bot_id, user_id)
    
    drive_status = "✅ Connected" if drive_connected else "❌ Not connected (/connect to set up)"
    
    await update.message.reply_text(
        f"Hey! I'm {name}. How can I help?\n\n"
        f"📧 Email: {email}\n"
        f"📁 Google Drive: {drive_status}\n\n"
        f"Commands:\n"
        f"/connect - Connect Google Drive\n"
        f"/emails - Check inbox\n"
        f"/files - List stored files\n"
        f"/find <query> - Search files"
    )


async def connect_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Connect Google Drive"""
    bot_id = context.bot_data.get("bot_id", 0)
    user_id = update.effective_user.id
    
    # Check if already connected
    if check_drive_connected(bot_id, user_id):
        await update.message.reply_text(
            "✅ Google Drive is already connected!\n\n"
            "Files you send me will be saved to your Drive."
        )
        return
    
    # Get auth URL
    auth_url = get_drive_auth_url(bot_id, user_id)
    
    if not auth_url:
        await update.message.reply_text(
            "❌ Google Drive connection is not configured yet.\n\n"
            "The bot admin needs to set up Google OAuth credentials."
        )
        return
    
    keyboard = [[InlineKeyboardButton("🔗 Connect Google Drive", url=auth_url)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "📁 Connect your Google Drive\n\n"
        "Click the button below to authorize access. "
        "Files you send me will be saved to a 'CrabPass' folder in your Drive.",
        reply_markup=reply_markup
    )


async def emails_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot_id = context.bot_data.get("bot_id", 0)
    emails = get_emails_for_bot(bot_id, limit=5)
    
    if not emails:
        bot_username = context.bot_data.get("bot_username", "yourbot")
        await update.message.reply_text(
            f"📭 No emails yet.\n\nYour email: {bot_username.lower()}@crabpass.ai"
        )
        return
    
    lines = ["📬 Recent emails:\n"]
    for email in emails:
        status = "🔵" if not email["read"] else "⚪"
        subject = email["subject"] or "(no subject)"
        from_addr = email["from_email"][:30]
        lines.append(f"{status} #{email['id']}: {subject}\n   From: {from_addr}")
    
    keyboard = []
    for email in emails[:5]:
        subject = (email["subject"] or "(no subject)")[:20]
        keyboard.append([InlineKeyboardButton(f"📖 Read #{email['id']}: {subject}", callback_data=f"read_{email['id']}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("\n".join(lines), reply_markup=reply_markup)


async def read_email_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot_id = context.bot_data.get("bot_id", 0)
    
    if not context.args:
        emails = get_emails_for_bot(bot_id, limit=1, unread_only=True)
        if emails:
            email = emails[0]
            await show_email(update.message, email)
            return
        else:
            await update.message.reply_text("No unread emails. Use /emails to see all.")
            return
    
    try:
        email_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Invalid email ID. Use /emails to see your inbox.")
        return
    
    email = get_email_by_id(email_id, bot_id)
    if not email:
        await update.message.reply_text("Email not found")
        return
    
    await show_email(update.message, email)


async def show_email(message_or_query, email):
    """Display an email"""
    mark_email_read(email["id"])
    
    body = email["body_plain"] or email["body_html"] or "(empty)"
    if not email["body_plain"] and email["body_html"]:
        import re
        body = re.sub('<[^<]+?>', '', body)
    
    if len(body) > 2000:
        body = body[:2000] + "...(truncated)"
    
    msg = (
        f"📧 Email #{email['id']}\n\n"
        f"From: {email['from_email']}\n"
        f"Subject: {email['subject'] or '(no subject)'}\n"
        f"Date: {email['received_at']}\n\n"
        f"{body}"
    )
    
    if hasattr(message_or_query, 'reply_text'):
        await message_or_query.reply_text(msg)
    else:
        await message_or_query.edit_message_text(msg)


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle inline button callbacks"""
    query = update.callback_query
    await query.answer()
    
    bot_id = context.bot_data.get("bot_id", 0)
    data = query.data
    
    if data.startswith("read_"):
        try:
            email_id = int(data.split("_")[1])
            email = get_email_by_id(email_id, bot_id)
            if email:
                await show_email(query, email)
            else:
                await query.edit_message_text("Email not found")
        except (ValueError, IndexError):
            await query.edit_message_text("Invalid email")
    
    elif data.startswith("get_"):
        try:
            file_id = int(data.split("_")[1])
            await retrieve_file(query.message, context, file_id)
        except (ValueError, IndexError):
            await query.edit_message_text("Invalid file")


async def files_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot_id = context.bot_data.get("bot_id", 0)
    files = get_files_for_bot(bot_id, limit=10)
    
    if not files:
        await update.message.reply_text("📁 No files stored yet.\n\nSend me a file, photo, or document to store it!")
        return
    
    lines = ["📁 Your files:\n"]
    keyboard = []
    
    for f in files:
        icon = "📄"
        if f["file_type"] == "photo":
            icon = "🖼️"
        elif f["file_type"] == "video":
            icon = "🎬"
        elif f["file_type"] == "audio":
            icon = "🎵"
        elif f["file_type"] == "voice":
            icon = "🎤"
        
        name = f["file_name"] or f["caption"] or f["file_type"]
        size = f["file_size"] or 0
        size_str = f"{size // 1024}KB" if size > 0 else ""
        drive_icon = "☁️" if f.get("drive_link") else ""
        lines.append(f"{icon} #{f['id']}: {name} {size_str} {drive_icon}")
        
        short_name = (name[:15] + "...") if len(name) > 18 else name
        keyboard.append([InlineKeyboardButton(f"{icon} Get #{f['id']}: {short_name}", callback_data=f"get_{f['id']}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("\n".join(lines), reply_markup=reply_markup)


async def get_file_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /get <file_id>\n\nOr use /files to see buttons.")
        return
    
    try:
        file_db_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Invalid file ID")
        return
    
    await retrieve_file(update.message, context, file_db_id)


async def retrieve_file(message, context, file_db_id):
    """Retrieve and send a file"""
    bot_id = context.bot_data.get("bot_id", 0)
    file_record = get_file_by_id(file_db_id, bot_id)
    
    if not file_record:
        await message.reply_text("File not found")
        return
    
    # If has Drive link, show that too
    drive_link = file_record.get("drive_link")
    if drive_link:
        await message.reply_text(f"☁️ Google Drive: {drive_link}")
    
    try:
        file_id = file_record["file_id"]
        file_type = file_record["file_type"]
        caption = file_record["caption"] or ""
        
        if file_type == "photo":
            await message.reply_photo(file_id, caption=caption)
        elif file_type == "video":
            await message.reply_video(file_id, caption=caption)
        elif file_type == "audio":
            await message.reply_audio(file_id, caption=caption)
        elif file_type == "voice":
            await message.reply_voice(file_id, caption=caption)
        elif file_type == "video_note":
            await message.reply_video_note(file_id)
        else:
            await message.reply_document(file_id, caption=caption)
            
    except Exception as e:
        logger.error(f"Error retrieving file: {e}")
        if drive_link:
            await message.reply_text(f"Telegram file expired, but here's the Drive link:\n{drive_link}")
        else:
            await message.reply_text("Error retrieving file. It may have expired.")


async def find_files_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot_id = context.bot_data.get("bot_id", 0)
    
    if not context.args:
        await update.message.reply_text("Usage: /find <search query>")
        return
    
    query = " ".join(context.args)
    files = search_files(bot_id, query)
    
    if not files:
        await update.message.reply_text(f"No files found matching '{query}'")
        return
    
    lines = [f"🔍 Files matching '{query}':\n"]
    keyboard = []
    
    for f in files:
        name = f["file_name"] or f["caption"] or f["file_type"]
        lines.append(f"📄 #{f['id']}: {name}")
        short_name = (name[:15] + "...") if len(name) > 18 else name
        keyboard.append([InlineKeyboardButton(f"📄 Get #{f['id']}: {short_name}", callback_data=f"get_{f['id']}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("\n".join(lines), reply_markup=reply_markup)


# ============== FILE HANDLER ==============

async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle incoming files - save to Telegram and optionally Google Drive"""
    bot_id = context.bot_data.get("bot_id", 0)
    user_id = update.effective_user.id
    message = update.message
    
    file_obj = None
    file_id = None
    file_unique_id = None
    file_name = None
    file_type = None
    file_size = None
    caption = message.caption or ""
    
    if message.document:
        file_obj = message.document
        file_id = message.document.file_id
        file_unique_id = message.document.file_unique_id
        file_name = message.document.file_name
        file_type = "document"
        file_size = message.document.file_size
    elif message.photo:
        photo = message.photo[-1]
        file_obj = photo
        file_id = photo.file_id
        file_unique_id = photo.file_unique_id
        file_name = "photo.jpg"
        file_type = "photo"
        file_size = photo.file_size
    elif message.video:
        file_obj = message.video
        file_id = message.video.file_id
        file_unique_id = message.video.file_unique_id
        file_name = message.video.file_name or "video.mp4"
        file_type = "video"
        file_size = message.video.file_size
    elif message.audio:
        file_obj = message.audio
        file_id = message.audio.file_id
        file_unique_id = message.audio.file_unique_id
        file_name = message.audio.file_name or message.audio.title or "audio"
        file_type = "audio"
        file_size = message.audio.file_size
    elif message.voice:
        file_obj = message.voice
        file_id = message.voice.file_id
        file_unique_id = message.voice.file_unique_id
        file_name = "voice.ogg"
        file_type = "voice"
        file_size = message.voice.file_size
    elif message.video_note:
        file_obj = message.video_note
        file_id = message.video_note.file_id
        file_unique_id = message.video_note.file_unique_id
        file_name = "video_note.mp4"
        file_type = "video_note"
        file_size = message.video_note.file_size
    
    if file_id:
        await message.reply_text("📤 Processing file...")
        
        drive_link = None
        
        # Check if Google Drive is connected
        if check_drive_connected(bot_id, user_id):
            try:
                # Download file from Telegram
                tg_file = await context.bot.get_file(file_id)
                file_bytes = await tg_file.download_as_bytearray()
                
                # Upload to Google Drive
                result = upload_to_drive(bot_id, user_id, file_name, bytes(file_bytes))
                if result and result.get('status') == 'ok':
                    drive_link = result.get('web_link')
            except Exception as e:
                logger.error(f"Drive upload failed: {e}")
        
        file_db_id = store_file(bot_id, user_id, file_id, file_unique_id, file_name, file_type, file_size, caption, drive_link)
        
        # Build response
        response_lines = [
            f"✅ File saved!",
            f"",
            f"📄 {file_name}",
            f"🆔 ID: #{file_db_id}"
        ]
        
        if drive_link:
            response_lines.append(f"☁️ Drive: {drive_link}")
        
        keyboard = [[InlineKeyboardButton(f"📥 Get this file", callback_data=f"get_{file_db_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await message.reply_text("\n".join(response_lines), reply_markup=reply_markup)


# ============== MESSAGE HANDLER ==============

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot_id = context.bot_data.get("bot_id", 0)
    user_id = update.effective_user.id
    key = str(bot_id) + ":" + str(user_id)
    
    if key not in conversations:
        conversations[key] = []
    
    history = conversations[key]
    history.append({"role": "user", "content": update.message.text})
    history = history[-20:]
    conversations[key] = history
    
    personality = context.bot_data.get("personality", "You are a helpful assistant.")
    bot_username = context.bot_data.get("bot_username", "")
    email_addr = bot_username.lower() + "@crabpass.ai" if bot_username else ""
    
    system_prompt = personality
    if email_addr:
        system_prompt += f"\n\nYou have an email address: {email_addr}. Users can email you there."
    system_prompt += "\n\nUsers can send you files to store. Use /files to list, /get <id> to retrieve."
    system_prompt += "\n\nUsers can connect their Google Drive with /connect for permanent file storage."
    
    await update.message.chat.send_action("typing")
    
    try:
        response = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "system", "content": system_prompt}] + history,
            max_tokens=1024
        )
        reply = response.choices[0].message.content
        history.append({"role": "assistant", "content": reply})
        conversations[key] = history[-20:]
        await update.message.reply_text(reply)
    except Exception as e:
        logger.error("Error: %s", e)
        await update.message.reply_text("Hit a snag. Try again!")


# ============== EMAIL CHECK ==============

async def check_new_emails(app, bot_id, user_id):
    """Check for new emails and notify user"""
    emails = get_unnotified_emails(bot_id)
    
    for email in emails:
        email_key = f"{bot_id}:{email['id']}"
        if email_key in notified_emails:
            continue
        
        notified_emails.add(email_key)
        mark_email_notified(email['id'])
        
        try:
            subject = email['subject'] or '(no subject)'
            keyboard = [[InlineKeyboardButton("📖 Read this email", callback_data=f"read_{email['id']}")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await app.bot.send_message(
                chat_id=user_id,
                text=f"📬 New email!\n\nFrom: {email['from_email']}\nSubject: {subject}",
                reply_markup=reply_markup
            )
        except Exception as e:
            logger.error(f"Failed to notify user: {e}")


async def email_checker_loop(app, bot_id, user_id):
    """Background task to check for new emails"""
    while True:
        try:
            await check_new_emails(app, bot_id, user_id)
        except Exception as e:
            logger.error(f"Email check error: {e}")
        await asyncio.sleep(30)


# ============== BOT RUNNER ==============

async def run_bot(bot_config):
    token = bot_config["bot_token"]
    bot_id = bot_config["id"]
    user_id = bot_config["user_id"]
    bot_username = bot_config.get("bot_username", "")
    personality = bot_config.get("personality") or "You are a helpful assistant."
    
    app = Application.builder().token(token).build()
    app.bot_data["bot_id"] = bot_id
    app.bot_data["bot_username"] = bot_username
    app.bot_data["personality"] = personality
    app.bot_data["user_id"] = user_id
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("connect", connect_command))
    app.add_handler(CommandHandler("emails", emails_command))
    app.add_handler(CommandHandler("read", read_email_command))
    app.add_handler(CommandHandler("files", files_command))
    app.add_handler(CommandHandler("get", get_file_command))
    app.add_handler(CommandHandler("find", find_files_command))
    
    app.add_handler(CallbackQueryHandler(callback_handler))
    
    app.add_handler(MessageHandler(
        filters.PHOTO | filters.Document.ALL | filters.VIDEO | filters.AUDIO | filters.VOICE | filters.VIDEO_NOTE,
        handle_file
    ))
    
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    bot_info = await app.bot.get_me()
    logger.info("Starting bot: @%s (email: %s@crabpass.ai)", bot_info.username, bot_info.username.lower())
    
    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    
    asyncio.create_task(email_checker_loop(app, bot_id, user_id))
    
    return app


async def main():
    logger.info("Multi-bot runner starting...")
    
    setup_database()
    
    running_bots = {}
    
    while True:
        bots = get_active_bots()
        
        for bot in bots:
            bot_id = bot["id"]
            if bot_id not in running_bots:
                try:
                    app = await run_bot(bot)
                    running_bots[bot_id] = app
                    logger.info("Started bot ID %d", bot_id)
                except Exception as e:
                    logger.error("Failed to start bot %s: %s", bot_id, e)
        
        logger.info("Running %d bots, checking again in 30s", len(running_bots))
        await asyncio.sleep(30)


if __name__ == "__main__":
    asyncio.run(main())
