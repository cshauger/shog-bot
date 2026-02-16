"""
Multi-Bot Runner - Runs multiple Telegram bots from database
VERSION: 2026-02-16-debug
"""
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Debug: Print database URL (masked)
if DATABASE_URL:
    masked = DATABASE_URL[:30] + "..." + DATABASE_URL[-20:] if len(DATABASE_URL) > 50 else DATABASE_URL
    logger.info(f"DATABASE_URL: {masked}")
else:
    logger.error("DATABASE_URL not set!")

groq_client = Groq(api_key=GROQ_API_KEY)

conversations = {}

def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

def ensure_tables():
    """Create tables if they don't exist"""
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bots (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER,
                    bot_token TEXT NOT NULL,
                    bot_username VARCHAR(255),
                    bot_name VARCHAR(255),
                    model VARCHAR(50) DEFAULT 'llama-3.3-70b',
                    personality TEXT,
                    is_active BOOLEAN DEFAULT true,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            conn.commit()
            logger.info("✅ Tables ready")

def get_active_bots():
    """Get all active bots from database"""
    with get_db() as conn:
        with conn.cursor() as cur:
            # Debug: Check table exists and columns
            cur.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'bots' ORDER BY ordinal_position
            """)
            columns = [row['column_name'] for row in cur.fetchall()]
            logger.info(f"📋 Table 'bots' columns: {columns}")
            
            # Debug: Check total count
            cur.execute("SELECT COUNT(*) as count FROM bots")
            total = cur.fetchone()['count']
            logger.info(f"📊 Total rows in bots table: {total}")
            
            # Debug: Check is_active values
            cur.execute("SELECT id, is_active, bot_token IS NOT NULL as has_token FROM bots")
            for row in cur.fetchall():
                logger.info(f"   Row {row['id']}: is_active={row['is_active']} (type: {type(row['is_active']).__name__}), has_token={row['has_token']}")
            
            # Now get active bots
            cur.execute("SELECT * FROM bots WHERE is_active = true")
            bots = cur.fetchall()
            logger.info(f"🤖 Active bots query returned: {len(bots)} bots")
            
            return bots

def get_history_key(bot_id, user_id):
    return f"{bot_id}:{user_id}"

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot_name = context.bot.first_name
    await update.message.reply_text(f"👋 Hey! I'm {bot_name}, your personal assistant. How can I help?")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot_id = context.bot_data.get('bot_id', 0)
    user_id = update.effective_user.id
    user_message = update.message.text
    
    key = get_history_key(bot_id, user_id)
    if key not in conversations:
        conversations[key] = []
    
    history = conversations[key]
    history.append({"role": "user", "content": user_message})
    history = history[-20:]
    conversations[key] = history
    
    bot_name = context.bot.first_name
    personality = context.bot_data.get('personality', f"You are {bot_name}, a helpful personal assistant. Be friendly and concise.")
    
    await update.message.chat.send_action("typing")
    
    try:
        response = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "system", "content": personality}] + history,
            max_tokens=1024,
        )
        
        reply = response.choices[0].message.content
        history.append({"role": "assistant", "content": reply})
        conversations[key] = history[-20:]
        
        await update.message.reply_text(reply)
    except Exception as e:
        logger.error(f"Error: {e}")
        await update.message.reply_text("😅 Hit a snag. Try again!")

async def run_bot(bot_config):
    """Run a single bot"""
    logger.info(f"🔧 Attempting to start bot with config: {dict(bot_config)}")
    
    token = bot_config['bot_token']
    bot_id = bot_config['id']
    personality = bot_config.get('personality') or "You are a helpful personal assistant. Be friendly and concise."
    
    if not token:
        logger.error(f"Bot {bot_id} has no token!")
        return None
    
    app = Application.builder().token(token).build()
    app.bot_data['bot_id'] = bot_id
    app.bot_data['personality'] = personality
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    bot_info = await app.bot.get_me()
    logger.info(f"🤖 Starting bot: @{bot_info.username} (ID: {bot_id})")
    
    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    
    return app

async def main():
    logger.info("🚀 Multi-Bot Runner starting... (DEBUG VERSION)")
    
    try:
        ensure_tables()
    except Exception as e:
        logger.error(f"Failed to ensure tables: {e}")
        raise
    
    bots = get_active_bots()
    logger.info(f"Found {len(bots)} active bots")
    
    if not bots:
        logger.warning("No active bots found. Will poll every 30s...")
        while True:
            await asyncio.sleep(30)
            logger.info("Polling for bots...")
            bots = get_active_bots()
            if bots:
                logger.info(f"Found {len(bots)} bots, starting them...")
                break
    
    apps = []
    for bot in bots:
        try:
            app = await run_bot(bot)
            if app:
                apps.append(app)
        except Exception as e:
            logger.error(f"Failed to start bot {bot.get('id', '?')}: {e}")
    
    logger.info(f"✅ Running {len(apps)} bots")
    
    while True:
        await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())
