"""
Multi-Bot Runner - Runs multiple Telegram bots from database
VERSION: 2026-02-17-vision
Features: Vision extraction, document storage, email sending
"""
import os
import asyncio
import logging
import psycopg2
import base64
import json
import httpx
from psycopg2.extras import RealDictCursor
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from groq import Groq

DATABASE_URL = os.environ.get("DATABASE_URL")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
SENDGRID_API_KEY = os.environ.get("SENDGRID_API_KEY")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

if DATABASE_URL:
    masked = DATABASE_URL[:30] + "..." + DATABASE_URL[-20:] if len(DATABASE_URL) > 50 else DATABASE_URL
    logger.info(f"DATABASE_URL: {masked}")

groq_client = Groq(api_key=GROQ_API_KEY)

conversations = {}
user_documents = {}

def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

def ensure_tables():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bots (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    bot_token TEXT NOT NULL UNIQUE,
                    bot_username TEXT,
                    bot_name TEXT,
                    model TEXT DEFAULT 'llama',
                    personality TEXT,
                    is_active BOOLEAN DEFAULT true,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_documents (
                    id SERIAL PRIMARY KEY,
                    bot_id INTEGER,
                    user_id BIGINT NOT NULL,
                    doc_type TEXT,
                    extracted_data JSONB,
                    file_id TEXT,
                    file_name TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            conn.commit()
            logger.info("Tables ready")

def get_active_bots():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM bots WHERE is_active = true")
            return cur.fetchall()

def save_document(bot_id, user_id, doc_type, extracted_data, file_id, file_name=None):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO user_documents (bot_id, user_id, doc_type, extracted_data, file_id, file_name)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (bot_id, user_id, doc_type, json.dumps(extracted_data), file_id, file_name))
            conn.commit()

def get_user_documents(bot_id, user_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM user_documents WHERE bot_id = %s AND user_id = %s ORDER BY created_at", (bot_id, user_id))
            return cur.fetchall()

def get_history_key(bot_id, user_id):
    return f"{bot_id}:{user_id}"

async def extract_document_with_vision(image_bytes, filename=None):
    base64_image = base64.b64encode(image_bytes).decode('utf-8')
    mime_type = 'image/jpeg'
    if filename:
        ext = filename.lower().split('.')[-1]
        mime_type = {'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'png': 'image/png'}.get(ext, 'image/jpeg')
    
    prompt = """Analyze this tax document. Return JSON:
{"doc_type": "W-2/1099-INT/1099-DIV/1099-MISC/1098/receipt/other",
 "payer_name": "name",
 "tax_year": "year",
 "amounts": {"wages": 0, "federal_withheld": 0, "state_withheld": 0, "interest_income": 0, "dividend_income": 0},
 "summary": "brief description"}
Return valid JSON only."""

    try:
        response = groq_client.chat.completions.create(
            model="llama-3.2-90b-vision-preview",
            messages=[{"role": "user", "content": [
                {"type": "image_url", "image_url": {"url": f"data:{mime_type};base64,{base64_image}"}},
                {"type": "text", "text": prompt}
            ]}],
            max_tokens=1024,
        )
        result_text = response.choices[0].message.content
    except Exception as e:
        logger.warning(f"Groq vision failed: {e}")
        if not OPENAI_API_KEY:
            return {"error": str(e), "doc_type": "unknown"}
        async with httpx.AsyncClient() as client:
            resp = await client.post("https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
                json={"model": "gpt-4o-mini", "messages": [{"role": "user", "content": [
                    {"type": "image_url", "image_url": {"url": f"data:{mime_type};base64,{base64_image}"}},
                    {"type": "text", "text": prompt}
                ]}], "max_tokens": 1024}, timeout=30.0)
            result_text = resp.json()["choices"][0]["message"]["content"]
    
    try:
        if "```json" in result_text:
            result_text = result_text.split("```json")[1].split("```")[0]
        elif "```" in result_text:
            result_text = result_text.split("```")[1].split("```")[0]
        return json.loads(result_text.strip())
    except:
        return {"doc_type": "unknown", "summary": result_text[:200]}

async def send_email_with_attachments(to_email, subject, body, attachments=None):
    email_data = {
        "personalizations": [{"to": [{"email": to_email}]}],
        "from": {"email": "assistant@crabpass.ai", "name": "Tax Assistant"},
        "subject": subject,
        "content": [{"type": "text/plain", "value": body}]
    }
    if attachments:
        email_data["attachments"] = attachments
    async with httpx.AsyncClient() as client:
        resp = await client.post("https://api.sendgrid.com/v3/mail/send",
            headers={"Authorization": f"Bearer {SENDGRID_API_KEY}", "Content-Type": "application/json"},
            json=email_data, timeout=30.0)
        return resp.status_code in [200, 202]

def generate_tax_summary(documents):
    if not documents:
        return "No documents collected yet."
    summary = "TAX DOCUMENT SUMMARY\n" + "="*40 + "\n\n"
    totals = {"wages": 0, "federal_withheld": 0, "state_withheld": 0, "interest_income": 0, "dividend_income": 0}
    for doc in documents:
        data = doc.get('extracted_data', {})
        if isinstance(data, str):
            try: data = json.loads(data)
            except: data = {}
        doc_type = data.get('doc_type', 'Unknown')
        payer = data.get('payer_name', 'Unknown')
        summary += f"📄 {doc_type} - {payer}\n"
        amounts = data.get('amounts', {})
        for key, val in amounts.items():
            if val and isinstance(val, (int, float)) and val > 0:
                summary += f"   {key.replace('_', ' ').title()}: ${val:,.2f}\n"
                if key in totals: totals[key] += val
        summary += "\n"
    summary += "="*40 + "\nTOTALS:\n"
    for key, val in totals.items():
        if val > 0:
            summary += f"   {key.replace('_', ' ').title()}: ${val:,.2f}\n"
    return summary

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot_name = context.bot.first_name
    await update.message.reply_text(
        f"👋 Hey! I'm {bot_name}.\n\n"
        "📸 Send photos of tax docs (W-2s, 1099s) and I'll extract the data.\n"
        "📧 Say 'email summary to [address]' to send to your accountant.\n"
        "📊 Say 'show summary' to see what I've collected.")

async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot_id = context.bot_data.get('bot_id', 0)
    user_id = update.effective_user.id
    await update.message.reply_text("📸 Analyzing document...")
    await update.message.chat.send_action("typing")
    try:
        photo = update.message.photo[-1]
        file = await context.bot.get_file(photo.file_id)
        photo_bytes = await file.download_as_bytearray()
        extracted = await extract_document_with_vision(bytes(photo_bytes))
        save_document(bot_id, user_id, extracted.get('doc_type', 'unknown'), extracted, photo.file_id)
        doc_type = extracted.get('doc_type', 'Document')
        payer = extracted.get('payer_name', '')
        response = f"📄 **{doc_type}**"
        if payer: response += f" from {payer}"
        response += "\n"
        amounts = extracted.get('amounts', {})
        for key, val in amounts.items():
            if val and isinstance(val, (int, float)) and val > 0:
                response += f"• {key.replace('_', ' ').title()}: ${val:,.2f}\n"
        docs = get_user_documents(bot_id, user_id)
        response += f"\n✅ {len(docs)} doc(s) collected. Send more or say 'show summary'."
        await update.message.reply_text(response, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Photo error: {e}")
        await update.message.reply_text("😅 Had trouble with that image. Try again.")

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot_id = context.bot_data.get('bot_id', 0)
    user_id = update.effective_user.id
    doc = update.message.document
    await update.message.reply_text(f"📎 Got {doc.file_name}. Storing...")
    save_document(bot_id, user_id, "pdf", {"file_name": doc.file_name}, doc.file_id, doc.file_name)
    docs = get_user_documents(bot_id, user_id)
    await update.message.reply_text(f"✅ Saved! {len(docs)} doc(s) collected.")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot_id = context.bot_data.get('bot_id', 0)
    user_id = update.effective_user.id
    text = update.message.text.lower().strip()
    
    if 'show summary' in text or 'tax summary' in text:
        docs = get_user_documents(bot_id, user_id)
        summary = generate_tax_summary(docs)
        await update.message.reply_text(f"```\n{summary}\n```", parse_mode='Markdown')
        return
    
    if 'email' in text and '@' in text:
        import re
        emails = re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', text)
        if emails:
            to_email = emails[0]
            docs = get_user_documents(bot_id, user_id)
            summary = generate_tax_summary(docs)
            await update.message.reply_text(f"📧 Sending to {to_email}...")
            if await send_email_with_attachments(to_email, f"Tax Summary - {update.effective_user.first_name}", summary):
                await update.message.reply_text(f"✅ Sent to {to_email}!")
            else:
                await update.message.reply_text("😅 Email failed.")
            return
    
    if 'clear' in text and 'document' in text:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM user_documents WHERE bot_id = %s AND user_id = %s", (bot_id, user_id))
                conn.commit()
        await update.message.reply_text("🗑️ Cleared!")
        return
    
    key = get_history_key(bot_id, user_id)
    if key not in conversations: conversations[key] = []
    history = conversations[key]
    history.append({"role": "user", "content": update.message.text})
    history = history[-20:]
    conversations[key] = history
    bot_name = context.bot.first_name
    personality = context.bot_data.get('personality', f"You are {bot_name}, a helpful assistant.")
    await update.message.chat.send_action("typing")
    try:
        response = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "system", "content": personality}] + history, max_tokens=1024)
        reply = response.choices[0].message.content
        history.append({"role": "assistant", "content": reply})
        conversations[key] = history[-20:]
        await update.message.reply_text(reply)
    except Exception as e:
        logger.error(f"Error: {e}")
        await update.message.reply_text("😅 Hit a snag.")

async def run_bot(bot_config):
    token = bot_config['bot_token']
    bot_id = bot_config['id']
    personality = bot_config.get('personality') or "You are a helpful assistant."
    if not token: return None
    app = Application.builder().token(token).build()
    app.bot_data['bot_id'] = bot_id
    app.bot_data['personality'] = personality
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    bot_info = await app.bot.get_me()
    logger.info(f"🤖 Starting @{bot_info.username} (ID: {bot_id}) with vision")
    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    return app

async def main():
    logger.info("🚀 Multi-Bot Runner (VISION) starting...")
    ensure_tables()
    bots = get_active_bots()
    if not bots:
        logger.warning("No bots. Polling...")
        while True:
            await asyncio.sleep(30)
            bots = get_active_bots()
            if bots: break
    apps = []
    for bot in bots:
        try:
            app = await run_bot(bot)
            if app: apps.append(app)
        except Exception as e:
            logger.error(f"Failed bot {bot.get('id')}: {e}")
    logger.info(f"✅ Running {len(apps)} bots with vision")
    while True:
        await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())
