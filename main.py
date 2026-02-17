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


def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def setup_database():
    logger.info("Setting up database...")
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS bots CASCADE")
            cur.execute(
                "CREATE TABLE bots ("
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
            # INSERT TEST BOT DIRECTLY
            cur.execute(
                "INSERT INTO bots (user_id, bot_token, bot_username, bot_name) VALUES (%s, %s, %s, %s)",
                (8259734518, "8154043410:AAGMwqlcLQGi6-6exAD5TFDDZaL1oNSajrE", "Shog99Bot", "Shog")
            )
            conn.commit()
    logger.info("Database ready with test bot!")


def get_active_bots():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM bots WHERE is_active = true")
            bots = cur.fetchall()
            logger.info("Found %d active bots", len(bots))
            return bots


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = context.bot.first_name
    await update.message.reply_text("Hey! I'm " + name + ". How can I help?")


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
    
    await update.message.chat.send_action("typing")
    
    try:
        response = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "system", "content": personality}] + history,
            max_tokens=1024
        )
        reply = response.choices[0].message.content
        history.append({"role": "assistant", "content": reply})
        conversations[key] = history[-20:]
        await update.message.reply_text(reply)
    except Exception as e:
        logger.error("Error: %s", e)
        await update.message.reply_text("Hit a snag. Try again!")


async def run_bot(bot_config):
    token = bot_config["bot_token"]
    bot_id = bot_config["id"]
    personality = bot_config.get("personality") or "You are a helpful assistant."
    
    app = Application.builder().token(token).build()
    app.bot_data["bot_id"] = bot_id
    app.bot_data["personality"] = personality
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    bot_info = await app.bot.get_me()
    logger.info("Starting bot: @%s", bot_info.username)
    
    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    return app


async def main():
    logger.info("Multi-bot runner starting...")
    
    setup_database()
    
    bots = get_active_bots()
    
    apps = []
    for bot in bots:
        try:
            app = await run_bot(bot)
            apps.append(app)
        except Exception as e:
            logger.error("Failed to start bot %s: %s", bot.get("id"), e)
    
    logger.info("Running %d bots", len(apps))
    
    while True:
        await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
