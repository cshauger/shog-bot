import os
import asyncio
import logging
import base64
import json
import re
import psycopg2
from psycopg2.extras import RealDictCursor
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
from groq import Groq
import requests

DATABASE_URL = os.environ.get("DATABASE_URL")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "https://email-webhook-production-887d.up.railway.app")
SENDGRID_API_KEY = os.environ.get("SENDGRID_API_KEY")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

groq_client = Groq(api_key=GROQ_API_KEY)
conversations = {}
notified_emails = set()
pending_emails = {}  # Store pending email confirmations


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
                DO $$ BEGIN 
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
            cur.execute("""
                DO $$ BEGIN 
                    ALTER TABLE files ADD COLUMN drive_link TEXT;
                EXCEPTION WHEN duplicate_column THEN NULL;
                END $$;
            """)
            cur.execute(
                "CREATE TABLE IF NOT EXISTS sent_emails ("
                "id SERIAL PRIMARY KEY, "
                "bot_id INTEGER REFERENCES bots(id), "
                "user_id BIGINT NOT NULL, "
                "to_email TEXT NOT NULL, "
                "subject TEXT, "
                "body TEXT, "
                "in_reply_to INTEGER REFERENCES emails(id), "
                "sent_at TIMESTAMP DEFAULT NOW())"
            )
            conn.commit()
    logger.info("Database ready!")


def get_active_bots():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM bots WHERE is_active = true")
            return cur.fetchall()


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


def send_email_via_sendgrid(from_email, to_email, subject, body):
    """Send email via SendGrid API"""
    if not SENDGRID_API_KEY:
        return False, "SendGrid not configured"
    
    try:
        response = requests.post(
            "https://api.sendgrid.com/v3/mail/send",
            headers={
                "Authorization": f"Bearer {SENDGRID_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "personalizations": [{"to": [{"email": to_email}]}],
                "from": {"email": from_email},
                "subject": subject,
                "content": [{"type": "text/plain", "value": body}]
            }
        )
        
        if response.status_code in [200, 201, 202]:
            return True, "Sent"
        else:
            return False, response.text
    except Exception as e:
        return False, str(e)


def store_sent_email(bot_id, user_id, to_email, subject, body, in_reply_to=None):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO sent_emails (bot_id, user_id, to_email, subject, body, in_reply_to)
                   VALUES (%s, %s, %s, %s, %s, %s) RETURNING id""",
                (bot_id, user_id, to_email, subject, body, in_reply_to)
            )
            email_id = cur.fetchone()["id"]
            conn.commit()
            return email_id


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
    
    drive_status = "✅ Connected" if drive_connected else "❌ Not connected"
    
    await update.message.reply_text(
        f"Hey! I'm {name}. How can I help?\n\n"
        f"📧 Email: {email}\n"
        f"☁️ Google Drive: {drive_status}\n\n"
        f"I understand natural language! Try:\n"
        f"• 'Email john@example.com saying hello'\n"
        f"• 'Send me my files'\n"
        f"• 'Check my inbox'\n\n"
        f"Commands: /connect /emails /files /email /reply"
    )


async def connect_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot_id = context.bot_data.get("bot_id", 0)
    user_id = update.effective_user.id
    
    if check_drive_connected(bot_id, user_id):
        await update.message.reply_text("✅ Google Drive is already connected!")
        return
    
    auth_url = get_drive_auth_url(bot_id, user_id)
    
    if not auth_url:
        await update.message.reply_text("❌ Google Drive connection is not configured yet.")
        return
    
    keyboard = [[InlineKeyboardButton("🔗 Connect Google Drive", url=auth_url)]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "📁 Connect your Google Drive\n\n"
        "Click below to authorize. Files will be saved to a 'CrabPass' folder.",
        reply_markup=reply_markup
    )


async def email_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send a new email: /email recipient@example.com subject | body"""
    bot_id = context.bot_data.get("bot_id", 0)
    bot_username = context.bot_data.get("bot_username", "bot")
    user_id = update.effective_user.id
    
    if not context.args:
        await update.message.reply_text(
            "📧 Send an email:\n\n"
            "/email to@example.com subject | message body\n\n"
            "Or just say: 'Email john@example.com saying hello!'"
        )
        return
    
    # Parse: first arg is recipient, rest is "subject | body" or just body
    to_email = context.args[0]
    rest = " ".join(context.args[1:])
    
    if "|" in rest:
        subject, body = rest.split("|", 1)
        subject = subject.strip()
        body = body.strip()
    else:
        subject = "Message from " + bot_username
        body = rest.strip()
    
    if not body:
        await update.message.reply_text("Please include a message body.")
        return
    
    # Store pending and ask for confirmation
    pending_key = f"{bot_id}:{user_id}"
    pending_emails[pending_key] = {
        "to": to_email,
        "subject": subject,
        "body": body,
        "reply_to": None
    }
    
    keyboard = [
        [InlineKeyboardButton("✅ Send", callback_data="send_email")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_email")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        f"📧 Ready to send:\n\n"
        f"To: {to_email}\n"
        f"Subject: {subject}\n\n"
        f"{body[:500]}{'...' if len(body) > 500 else ''}",
        reply_markup=reply_markup
    )


async def reply_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Reply to an email: /reply email_id message"""
    bot_id = context.bot_data.get("bot_id", 0)
    bot_username = context.bot_data.get("bot_username", "bot")
    user_id = update.effective_user.id
    
    if len(context.args) < 2:
        await update.message.reply_text(
            "📧 Reply to an email:\n\n"
            "/reply <email_id> your message here\n\n"
            "Use /emails to see email IDs."
        )
        return
    
    try:
        email_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Invalid email ID.")
        return
    
    original = get_email_by_id(email_id, bot_id)
    if not original:
        await update.message.reply_text("Email not found.")
        return
    
    body = " ".join(context.args[1:])
    to_email = original["from_email"]
    # Extract just email from "Name <email>" format
    match = re.search(r'[\w.-]+@[\w.-]+', to_email)
    if match:
        to_email = match.group(0)
    
    subject = original["subject"] or ""
    if not subject.lower().startswith("re:"):
        subject = "Re: " + subject
    
    pending_key = f"{bot_id}:{user_id}"
    pending_emails[pending_key] = {
        "to": to_email,
        "subject": subject,
        "body": body,
        "reply_to": email_id
    }
    
    keyboard = [
        [InlineKeyboardButton("✅ Send", callback_data="send_email")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel_email")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        f"📧 Ready to reply:\n\n"
        f"To: {to_email}\n"
        f"Subject: {subject}\n\n"
        f"{body[:500]}",
        reply_markup=reply_markup
    )


async def emails_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot_id = context.bot_data.get("bot_id", 0)
    emails = get_emails_for_bot(bot_id, limit=5)
    
    if not emails:
        bot_username = context.bot_data.get("bot_username", "yourbot")
        await update.message.reply_text(f"📭 No emails yet.\n\nYour email: {bot_username.lower()}@crabpass.ai")
        return
    
    lines = ["📬 Recent emails:\n"]
    keyboard = []
    for email in emails:
        status = "🔵" if not email["read"] else "⚪"
        subject = email["subject"] or "(no subject)"
        from_addr = email["from_email"][:30]
        lines.append(f"{status} #{email['id']}: {subject}\n   From: {from_addr}")
        keyboard.append([InlineKeyboardButton(f"📖 #{email['id']}: {subject[:20]}", callback_data=f"read_{email['id']}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("\n".join(lines), reply_markup=reply_markup)


async def files_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot_id = context.bot_data.get("bot_id", 0)
    files = get_files_for_bot(bot_id, limit=10)
    
    if not files:
        await update.message.reply_text("📁 No files stored yet.\n\nSend me a file to store it!")
        return
    
    lines = ["📁 Your files:\n"]
    keyboard = []
    
    for f in files:
        icon = {"photo": "🖼️", "video": "🎬", "audio": "🎵", "voice": "🎤"}.get(f["file_type"], "📄")
        name = f["file_name"] or f["caption"] or f["file_type"]
        drive_icon = "☁️" if f.get("drive_link") else ""
        lines.append(f"{icon} #{f['id']}: {name} {drive_icon}")
        keyboard.append([InlineKeyboardButton(f"{icon} #{f['id']}: {name[:15]}", callback_data=f"get_{f['id']}")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("\n".join(lines), reply_markup=reply_markup)


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    bot_id = context.bot_data.get("bot_id", 0)
    bot_username = context.bot_data.get("bot_username", "bot")
    user_id = query.from_user.id
    data = query.data
    
    if data == "send_email":
        pending_key = f"{bot_id}:{user_id}"
        email_data = pending_emails.pop(pending_key, None)
        
        if not email_data:
            await query.edit_message_text("❌ Email expired. Please try again.")
            return
        
        from_email = f"{bot_username.lower()}@crabpass.ai"
        success, msg = send_email_via_sendgrid(
            from_email, email_data["to"], email_data["subject"], email_data["body"]
        )
        
        if success:
            store_sent_email(bot_id, user_id, email_data["to"], email_data["subject"], 
                           email_data["body"], email_data.get("reply_to"))
            await query.edit_message_text(f"✅ Email sent to {email_data['to']}!")
        else:
            await query.edit_message_text(f"❌ Failed to send: {msg}")
    
    elif data == "cancel_email":
        pending_key = f"{bot_id}:{user_id}"
        pending_emails.pop(pending_key, None)
        await query.edit_message_text("❌ Email cancelled.")
    
    elif data.startswith("read_"):
        email_id = int(data.split("_")[1])
        email = get_email_by_id(email_id, bot_id)
        if email:
            await show_email(query, email, bot_id)
        else:
            await query.edit_message_text("Email not found")
    
    elif data.startswith("reply_"):
        email_id = int(data.split("_")[1])
        await query.edit_message_text(f"To reply, use:\n/reply {email_id} your message here")
    
    elif data.startswith("get_"):
        file_id = int(data.split("_")[1])
        file_record = get_file_by_id(file_id, bot_id)
        if file_record:
            await send_file(query.message, file_record)
        else:
            await query.edit_message_text("File not found")


async def show_email(query, email, bot_id):
    mark_email_read(email["id"])
    
    body = email["body_plain"] or email["body_html"] or "(empty)"
    if not email["body_plain"] and email["body_html"]:
        body = re.sub('<[^<]+?>', '', body)
    
    if len(body) > 1500:
        body = body[:1500] + "...(truncated)"
    
    keyboard = [[InlineKeyboardButton("↩️ Reply", callback_data=f"reply_{email['id']}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        f"📧 Email #{email['id']}\n\n"
        f"From: {email['from_email']}\n"
        f"Subject: {email['subject'] or '(no subject)'}\n"
        f"Date: {email['received_at']}\n\n"
        f"{body}",
        reply_markup=reply_markup
    )


async def send_file(message, file_record):
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
        else:
            await message.reply_document(file_id, caption=caption)
    except Exception as e:
        if drive_link:
            await message.reply_text(f"Telegram file expired. Drive link: {drive_link}")
        else:
            await message.reply_text("Error retrieving file.")


# ============== FILE HANDLER ==============

async def handle_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot_id = context.bot_data.get("bot_id", 0)
    user_id = update.effective_user.id
    message = update.message
    
    file_obj = None
    file_id = file_unique_id = file_name = file_type = None
    file_size = None
    caption = message.caption or ""
    
    if message.document:
        file_obj = message.document
        file_id, file_unique_id = file_obj.file_id, file_obj.file_unique_id
        file_name, file_type, file_size = file_obj.file_name, "document", file_obj.file_size
    elif message.photo:
        file_obj = message.photo[-1]
        file_id, file_unique_id = file_obj.file_id, file_obj.file_unique_id
        file_name, file_type, file_size = "photo.jpg", "photo", file_obj.file_size
    elif message.video:
        file_obj = message.video
        file_id, file_unique_id = file_obj.file_id, file_obj.file_unique_id
        file_name, file_type, file_size = file_obj.file_name or "video.mp4", "video", file_obj.file_size
    elif message.audio:
        file_obj = message.audio
        file_id, file_unique_id = file_obj.file_id, file_obj.file_unique_id
        file_name, file_type, file_size = file_obj.file_name or "audio", "audio", file_obj.file_size
    elif message.voice:
        file_obj = message.voice
        file_id, file_unique_id = file_obj.file_id, file_obj.file_unique_id
        file_name, file_type, file_size = "voice.ogg", "voice", file_obj.file_size
    
    if file_id:
        await message.reply_text("📤 Saving file...")
        
        drive_link = None
        if check_drive_connected(bot_id, user_id):
            try:
                tg_file = await context.bot.get_file(file_id)
                file_bytes = await tg_file.download_as_bytearray()
                result = upload_to_drive(bot_id, user_id, file_name, bytes(file_bytes))
                if result and result.get('status') == 'ok':
                    drive_link = result.get('web_link')
            except Exception as e:
                logger.error(f"Drive upload failed: {e}")
        
        file_db_id = store_file(bot_id, user_id, file_id, file_unique_id, file_name, file_type, file_size, caption, drive_link)
        
        response = f"✅ Saved!\n📄 {file_name}\n🆔 #{file_db_id}"
        if drive_link:
            response += f"\n☁️ {drive_link}"
        
        keyboard = [[InlineKeyboardButton("📥 Get file", callback_data=f"get_{file_db_id}")]]
        await message.reply_text(response, reply_markup=InlineKeyboardMarkup(keyboard))


# ============== MESSAGE HANDLER WITH NLP ==============

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot_id = context.bot_data.get("bot_id", 0)
    bot_username = context.bot_data.get("bot_username", "")
    user_id = update.effective_user.id
    user_message = update.message.text
    key = f"{bot_id}:{user_id}"
    
    if key not in conversations:
        conversations[key] = []
    
    history = conversations[key]
    history.append({"role": "user", "content": user_message})
    history = history[-20:]
    conversations[key] = history
    
    personality = context.bot_data.get("personality", "You are a helpful assistant.")
    email_addr = f"{bot_username.lower()}@crabpass.ai" if bot_username else ""
    
    system_prompt = f"""{personality}

You are a personal assistant bot with these capabilities:
- Email: You can send emails from {email_addr}
- Files: Users can send you files to store
- Google Drive: Files can sync to user's Drive

IMPORTANT: When the user wants to send an email, respond with a JSON block like this:
```json
{{"action": "send_email", "to": "recipient@example.com", "subject": "Subject line", "body": "Email body text"}}
```

When user wants to reply to email #N, include "reply_to": N in the JSON.

For normal conversation, just respond naturally without JSON.

Examples of email requests:
- "Email john@test.com saying I'll be late" -> extract and return JSON
- "Send an email to sarah@company.com about the meeting" -> ask for more details or compose
- "Reply to email 3 saying thanks" -> return JSON with reply_to: 3
"""

    await update.message.chat.send_action("typing")
    
    try:
        response = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "system", "content": system_prompt}] + history,
            max_tokens=1024
        )
        reply = response.choices[0].message.content
        
        # Check if response contains email action JSON
        json_match = re.search(r'```json\s*(\{.*?\})\s*```', reply, re.DOTALL)
        if not json_match:
            json_match = re.search(r'\{[^{}]*"action"\s*:\s*"send_email"[^{}]*\}', reply)
        
        if json_match:
            try:
                email_action = json.loads(json_match.group(1) if '```' in reply else json_match.group(0))
                if email_action.get("action") == "send_email":
                    to_email = email_action.get("to", "")
                    subject = email_action.get("subject", f"Message from {bot_username}")
                    body = email_action.get("body", "")
                    reply_to = email_action.get("reply_to")
                    
                    if to_email and body:
                        pending_emails[key] = {
                            "to": to_email,
                            "subject": subject,
                            "body": body,
                            "reply_to": reply_to
                        }
                        
                        keyboard = [
                            [InlineKeyboardButton("✅ Send", callback_data="send_email")],
                            [InlineKeyboardButton("❌ Cancel", callback_data="cancel_email")]
                        ]
                        
                        await update.message.reply_text(
                            f"📧 Ready to send:\n\n"
                            f"To: {to_email}\n"
                            f"Subject: {subject}\n\n"
                            f"{body[:500]}",
                            reply_markup=InlineKeyboardMarkup(keyboard)
                        )
                        history.append({"role": "assistant", "content": "I've prepared your email. Please confirm to send."})
                        conversations[key] = history[-20:]
                        return
            except json.JSONDecodeError:
                pass
        
        # Normal response
        history.append({"role": "assistant", "content": reply})
        conversations[key] = history[-20:]
        await update.message.reply_text(reply)
        
    except Exception as e:
        logger.error("Error: %s", e)
        await update.message.reply_text("Hit a snag. Try again!")


# ============== EMAIL CHECK ==============

async def check_new_emails(app, bot_id, user_id):
    emails = get_unnotified_emails(bot_id)
    
    for email in emails:
        email_key = f"{bot_id}:{email['id']}"
        if email_key in notified_emails:
            continue
        
        notified_emails.add(email_key)
        mark_email_notified(email['id'])
        
        try:
            keyboard = [[InlineKeyboardButton("📖 Read", callback_data=f"read_{email['id']}")]]
            await app.bot.send_message(
                chat_id=user_id,
                text=f"📬 New email!\n\nFrom: {email['from_email']}\nSubject: {email['subject'] or '(no subject)'}",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception as e:
            logger.error(f"Failed to notify: {e}")


async def email_checker_loop(app, bot_id, user_id):
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
    app.add_handler(CommandHandler("email", email_command))
    app.add_handler(CommandHandler("reply", reply_command))
    app.add_handler(CommandHandler("emails", emails_command))
    app.add_handler(CommandHandler("files", files_command))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(
        filters.PHOTO | filters.Document.ALL | filters.VIDEO | filters.AUDIO | filters.VOICE,
        handle_file
    ))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    bot_info = await app.bot.get_me()
    logger.info("Starting bot: @%s", bot_info.username)
    
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
            if bot["id"] not in running_bots:
                try:
                    app = await run_bot(bot)
                    running_bots[bot["id"]] = app
                except Exception as e:
                    logger.error("Failed to start bot %s: %s", bot["id"], e)
        await asyncio.sleep(30)


if __name__ == "__main__":
    asyncio.run(main())
