Multi-Bot Runner - VERSION: NUKE
"""
import os, asyncio, logging, psycopg2
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

def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

def ensure_tables():
    with get_db() as conn:
        with conn.cursor() as cur:
            logger.info("🔥 NUKING bots table...")
            cur.execute("DROP TABLE IF EXISTS bots CASCADE")
            cur.execute("""CREATE TABLE bots (
                id SERIAL PRIMARY KEY, user_id BIGINT NOT NULL, bot_token TEXT NOT NULL UNIQUE,
                bot_username TEXT, bot_name TEXT, model TEXT DEFAULT 'llama',
                personality TEXT, is_active BOOLEAN DEFAULT true, created_at TIMESTAMP DEFAULT NOW())""")
            conn.commit()
            logger.info("✅ Fresh table created!")

def get_active_bots():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM bots WHERE is_active = true")
            return cur.fetchall()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"👋 Hey! I'm {context.bot.first_name}. How can I help?")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    key = f"{context.bot_data.get('bot_id', 0)}:{update.effective_user.id}"
    if key not in conversations: conversations[key] = []
    conversations[key].append({"role": "user", "content": update.message.text})
    conversations[key] = conversations[key][-20:]
    await update.message.chat.send_action("typing")
    try:
        response = groq_client.chat.completions.create(model="llama-3.3-70b-versatile",
            messages=[{"role": "system", "content": context.bot_data.get('personality', "You are a helpful assistant.")}] + conversations[key], max_tokens=1024)
        reply = response.choices[0].message.content
        conversations[key].append({"role": "assistant", "content": reply})
        await update.message.reply_text(reply)
    except Exception as e:
        logger.error(f"Error: {e}")
        await update.message.reply_text("😅 Try again!")

async def run_bot(bot_config):
    app = Application.builder().token(bot_config['bot_token']).build()
    app.bot_data['bot_id'] = bot_config['id']
    app.bot_data['personality'] = bot_config.get('personality') or "You are a helpful assistant."
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    logger.info(f"🤖 Starting @{(await app.bot.get_me()).username}")
    await app.initialize(); await app.start(); await app.updater.start_polling()
    return app

async def main():
    logger.info("🚀 Starting...")
    ensure_tables()
    while True:
        bots = get_active_bots()
        logger.info(f"Found {len(bots)} bots")
        if bots:
            for bot in bots:
                try: await run_bot(bot)
                except Exception as e: logger.error(f"Failed: {e}")
            break
        await asyncio.sleep(30)
    while True: await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())
