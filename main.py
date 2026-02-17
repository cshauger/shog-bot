import os
import asyncio
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from groq import Groq

DATABASE_URL = os.environ.get("DATABASE_URL")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

groq_client = Groq(api_key=GROQ_API_KEY)
conversations = {}
last_email_check = {}


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
                "read BOOLEAN DEFAULT FALSE)"
            )
            conn.commit()
    logger.info("Database ready!")


def get_active_bots():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM bots WHERE is_active = true")
            bots = cur.fetchall()
            logger.info("Found %d active bots", len(bots))
            return bots


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


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot_username = context.bot_data.get("bot_username", "")
    name = context.bot.first_name
    email = bot_username.lower() + "@crabpass.ai" if bot_username else "your-bot@crabpass.ai"
    await update.message.reply_text(
        f"Hey! I'm {name}. How can I help?\n\n"
        f"📧 My email: {email}\n\n"
        f"Commands:\n"
        f"/emails - Check your inbox\n"
        f"/read <id> - Read an email"
    )


async def emails_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot_id = context.bot_data.get("bot_id", 0)
    emails = get_emails_for_bot(bot_id, limit=5)
    
    if not emails:
        bot_username = context.bot_data.get("bot_username", "yourbot")
        await update.message.reply_text(
            f"📭 No emails yet.\n\n"
            f"Your email address: {bot_username.lower()}@crabpass.ai"
        )
        return
    
    lines = ["📬 Recent emails:\n"]
    for email in emails:
        status = "🔵" if not email["read"] else "⚪"
        subject = email["subject"] or "(no subject)"
        from_addr = email["from_email"][:30]
        lines.append(f"{status} #{email['id']}: {subject}\n   From: {from_addr}")
    
    lines.append(f"\nUse /read <id> to read an email")
    await update.message.reply_text("\n".join(lines))


async def read_email_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot_id = context.bot_data.get("bot_id", 0)
    
    if not context.args:
        await update.message.reply_text("Usage: /read <email_id>")
        return
    
    try:
        email_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Invalid email ID")
        return
    
    email = get_email_by_id(email_id, bot_id)
    if not email:
        await update.message.reply_text("Email not found")
        return
    
    mark_email_read(email_id)
    
    body = email["body_plain"] or email["body_html"] or "(empty)"
    if len(body) > 2000:
        body = body[:2000] + "...(truncated)"
    
    msg = (
        f"📧 Email #{email['id']}\n\n"
        f"From: {email['from_email']}\n"
        f"Subject: {email['subject'] or '(no subject)'}\n"
        f"Date: {email['received_at']}\n\n"
        f"{body}"
    )
    await update.message.reply_text(msg)


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
        system_prompt += f"\n\nYou have an email address: {email_addr}. Users can send you emails there."
    
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


async def check_new_emails(app, bot_id, user_id):
    """Check for new emails and notify user"""
    global last_email_check
    key = str(bot_id)
    
    emails = get_emails_for_bot(bot_id, limit=1, unread_only=True)
    if emails:
        email = emails[0]
        email_key = f"{bot_id}:{email['id']}"
        
        if email_key not in last_email_check:
            last_email_check[email_key] = True
            try:
                await app.bot.send_message(
                    chat_id=user_id,
                    text=f"📬 New email!\n\nFrom: {email['from_email']}\nSubject: {email['subject'] or '(no subject)'}\n\nUse /read {email['id']} to read it."
                )
            except Exception as e:
                logger.error(f"Failed to notify user: {e}")


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
    app.add_handler(CommandHandler("emails", emails_command))
    app.add_handler(CommandHandler("read", read_email_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    bot_info = await app.bot.get_me()
    logger.info("Starting bot: @%s (email: %s@crabpass.ai)", bot_info.username, bot_info.username.lower())
    
    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    
    # Start email checker
    asyncio.create_task(email_checker_loop(app, bot_id, user_id))
    
    return app


async def email_checker_loop(app, bot_id, user_id):
    """Background task to check for new emails"""
    while True:
        try:
            await check_new_emails(app, bot_id, user_id)
        except Exception as e:
            logger.error(f"Email check error: {e}")
        await asyncio.sleep(30)


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
