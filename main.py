import os
import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from groq import Groq

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Debug: print what we got
logger.info(f"TELEGRAM_TOKEN set: {bool(TELEGRAM_TOKEN)}")
logger.info(f"GROQ_API_KEY set: {bool(GROQ_API_KEY)}")

if not GROQ_API_KEY:
    raise ValueError(f"GROQ_API_KEY not set. Env vars: {list(os.environ.keys())}")

groq_client = Groq(api_key=GROQ_API_KEY)
history = []

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👋 Hey! I'm Shog, your personal assistant. How can I help?")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global history
    user_message = update.message.text
    history.append({"role": "user", "content": user_message})
    history = history[-20:]
    
    await update.message.chat.send_action("typing")
    
    response = groq_client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "system", "content": "You are Shog, a helpful personal assistant. Be friendly and concise."}] + history,
        max_tokens=1024,
    )
    
    reply = response.choices[0].message.content
    history.append({"role": "assistant", "content": reply})
    await update.message.reply_text(reply)

def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    print("🤖 Shog starting...")
    app.run_polling()

if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()
