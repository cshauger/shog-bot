"""
Multi-Bot Runner - EMAIL AWARE
VERSION: 2026-02-17-email
"""
import os
import asyncio
import logging
import psycopg2
import base64
import json
import httpx
import re
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

groq_client = Groq(api_key=GROQ_API_KEY)

conversations = {}
running_bots = {}

def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

def ensure_tables():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""CREATE TABLE IF NOT EXISTS bots (
                id SERIAL PRIMARY KEY, user_id BIGINT NOT NULL, bot_token TEXT NOT NULL UNIQUE,
                bot_username TEXT, bot_name TEXT, model TEXT DEFAULT 'llama',
                personality TEXT, is_active BOOLEAN DEFAULT true, created_at TIMESTAMP DEFAULT NOW())""")
            cur.execute("""CREATE TABLE IF NOT EXISTS user_documents (
                id SERIAL PRIMARY KEY, bot_id INTEGER, user_id BIGINT NOT NULL,
                doc_type TEXT, extracted_data JSONB, file_id TEXT, file_name TEXT,
                created_at TIMESTAMP DEFAULT NOW())""")
            conn.commit()

def get_active_bots():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM bots WHERE is_active = true AND (railway_service_id IS NULL OR railway_service_id = '')")
            return cur.fetchall()

def save_document(bot_id, user_id, doc_type, extracted_data, file_id, file_name=None):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO user_documents (bot_id, user_id, doc_type, extracted_data, file_id, file_name)
                VALUES (%s, %s, %s, %s, %s, %s)""", (bot_id, user_id, doc_type, json.dumps(extracted_data), file_id, file_name))
            conn.commit()

def get_user_documents(bot_id, user_id):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM user_documents WHERE bot_id = %s AND user_id = %s ORDER BY created_at", (bot_id, user_id))
            return cur.fetchall()

def get_history_key(bot_id, user_id):
    return f"{bot_id}:{user_id}"

def get_bot_email(username):
    """Derive email from bot username: @NeatlySFbot → neatlysf@crabpass.ai"""
    if not username:
        return None
    # Remove @, remove 'bot' suffix, lowercase
    email_name = username.lower().replace('@', '').replace('bot', '').replace('_', '')
    return f"{email_name}@crabpass.ai"

async def check_owner(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user_id = update.effective_user.id
    owner_id = context.bot_data.get('owner_id')
    if owner_id and user_id != owner_id:
        await update.message.reply_text("🔒 This is a private assistant.\n\nWant your own? Visit @CrabPassBot!")
        return False
    return True

async def extract_document_with_vision(image_bytes, filename=None):
    base64_image = base64.b64encode(image_bytes).decode('utf-8')
    prompt = """Analyze this tax document. Return JSON:
{"doc_type": "W-2/1099-INT/1099-DIV/1099-MISC/1098/receipt/other",
 "payer_name": "name", "tax_year": "year",
 "amounts": {"wages": 0, "federal_withheld": 0, "state_withheld": 0, "interest_income": 0, "dividend_income": 0},
 "summary": "brief description"}"""
    try:
        response = groq_client.chat.completions.create(
            model="llama-3.2-90b-vision-preview",
            messages=[{"role": "user", "content": [
                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}},
                {"type": "text", "text": prompt}
            ]}], max_tokens=1024)
        result_text = response.choices[0].message.content
    except Exception as e:
        if not OPENAI_API_KEY: return {"error": str(e), "doc_type": "unknown"}
        async with httpx.AsyncClient() as client:
            resp = await client.post("https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
                json={"model": "gpt-4o-mini", "messages": [{"role": "user", "content": [
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}},
                    {"type": "text", "text": prompt}
                ]}], "max_tokens": 1024}, timeout=30.0)
            result_text = resp.json()["choices"][0]["message"]["content"]
    try:
        if "```json" in result_text: result_text = result_text.split("```json")[1].split("```")[0]
        elif "```" in result_text: result_text = result_text.split("```")[1].split("```")[0]
        return json.loads(result_text.strip())
    except:
        return {"doc_type": "unknown", "summary": result_text[:200]}

async def send_email_with_attachments(to_email, subject, body):
    async with httpx.AsyncClient() as client:
        resp = await client.post("https://api.sendgrid.com/v3/mail/send",
            headers={"Authorization": f"Bearer {SENDGRID_API_KEY}", "Content-Type": "application/json"},
            json={"personalizations": [{"to": [{"email": to_email}]}],
                  "from": {"email": "assistant@crabpass.ai", "name": "Tax Assistant"},
                  "subject": subject, "content": [{"type": "text/plain", "value": body}]}, timeout=30.0)
        return resp.status_code in [200, 202]

def generate_tax_summary(documents):
    if not documents: return "No documents collected yet."
    summary = "TAX DOCUMENT SUMMARY\n" + "="*40 + "\n\n"
    totals = {"wages": 0, "federal_withheld": 0, "state_withheld": 0, "interest_income": 0, "dividend_income": 0}
    for doc in documents:
        data = doc.get('extracted_data', {})
        if isinstance(data, str):
            try: data = json.loads(data)
            except: data = {}
        summary += f"{data.get('doc_type', 'Unknown')} - {data.get('payer_name', 'Unknown')}\n"
        for key, val in data.get('amounts', {}).items():
            if val and isinstance(val, (int, float)) and val > 0:
                summary += f"   {key.replace('_', ' ').title()}: ${val:,.2f}\n"
                if key in totals: totals[key] += val
        summary += "\n"
    summary += "="*40 + "\nTOTALS:\n"
    for key, val in totals.items():
        if val > 0: summary += f"   {key.replace('_', ' ').title()}: ${val:,.2f}\n"
    return summary

def is_tax_help_request(text):
    return any(kw in text.lower() for kw in ['tax', 'taxes', 'w-2', 'w2', '1099', 'refund', 'irs', 'accountant', 'filing', '1098'])

def is_email_question(text):
    """Check if user is asking about their email address"""
    patterns = ['my email', 'what email', 'email address', 'whats my email', "what's my email", 
                'my address', 'receive email', 'send me email', 'email me at']
    return any(p in text.lower() for p in patterns)

TAX_HELP_PROMPT = """📋 **Tax Document Assistant**

1️⃣ **Send photos** of W-2s, 1099s, receipts
2️⃣ I'll **extract the numbers**
3️⃣ Say "**show summary**" to review
4️⃣ Say "**email summary to you@email.com**" to send

Ready! 📸"""

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_owner(update, context): return
    bot_name = context.bot.first_name
    bot_email = context.bot_data.get('bot_email', 'your-bot@crabpass.ai')
    await update.message.reply_text(
        f"Hey! I'm {bot_name}, your private assistant.\n\n"
        f"📧 My email: **{bot_email}**\n"
        f"Forward emails here and I'll notify you!\n\n"
        f"I can also help with taxes - just send photos of your docs.", parse_mode='Markdown')

async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_owner(update, context): return
    bot_id = context.bot_data.get('bot_id', 0)
    user_id = update.effective_user.id
    await update.message.reply_text("📸 Analyzing...")
    await update.message.chat.send_action("typing")
    try:
        photo = update.message.photo[-1]
        file = await context.bot.get_file(photo.file_id)
        photo_bytes = await file.download_as_bytearray()
        extracted = await extract_document_with_vision(bytes(photo_bytes))
        save_document(bot_id, user_id, extracted.get('doc_type', 'unknown'), extracted, photo.file_id)
        response = f"📄 **{extracted.get('doc_type', 'Document')}**"
        if extracted.get('payer_name'): response += f" from {extracted['payer_name']}"
        response += "\n\n"
        for key, val in extracted.get('amounts', {}).items():
            if val and isinstance(val, (int, float)) and val > 0:
                response += f"• {key.replace('_', ' ').title()}: ${val:,.2f}\n"
        docs = get_user_documents(bot_id, user_id)
        response += f"\n✅ {len(docs)} doc(s) collected."
        await update.message.reply_text(response, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Photo error: {e}")
        await update.message.reply_text("Had trouble. Try a clearer photo.")

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_owner(update, context): return
    bot_id = context.bot_data.get('bot_id', 0)
    user_id = update.effective_user.id
    doc = update.message.document
    save_document(bot_id, user_id, "pdf", {"file_name": doc.file_name}, doc.file_id, doc.file_name)
    docs = get_user_documents(bot_id, user_id)
    await update.message.reply_text(f"📎 Saved {doc.file_name}! ({len(docs)} total)")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_owner(update, context): return
    bot_id = context.bot_data.get('bot_id', 0)
    user_id = update.effective_user.id
    text = update.message.text
    text_lower = text.lower().strip()
    
    # Check for email question
    if is_email_question(text):
        bot_email = context.bot_data.get('bot_email', 'your-bot@crabpass.ai')
        await update.message.reply_text(
            f"📧 **Your email address:** `{bot_email}`\n\n"
            f"Anyone can send emails to this address and I'll forward them to you here!",
            parse_mode='Markdown')
        return
    
    if 'show summary' in text_lower:
        docs = get_user_documents(bot_id, user_id)
        await update.message.reply_text(f"```\n{generate_tax_summary(docs)}\n```", parse_mode='Markdown')
        return
    if 'email' in text_lower and '@' in text_lower and 'my email' not in text_lower:
        emails = re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', text_lower)
        if emails:
            docs = get_user_documents(bot_id, user_id)
            await update.message.reply_text(f"📧 Sending to {emails[0]}...")
            if await send_email_with_attachments(emails[0], "Tax Summary", generate_tax_summary(docs)):
                await update.message.reply_text("✅ Sent!")
            else:
                await update.message.reply_text("❌ Failed.")
            return
    if 'clear' in text_lower and 'document' in text_lower:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM user_documents WHERE bot_id = %s AND user_id = %s", (bot_id, user_id))
                conn.commit()
        await update.message.reply_text("🗑️ Cleared!")
        return
    if is_tax_help_request(text):
        await update.message.reply_text(TAX_HELP_PROMPT, parse_mode='Markdown')
        return
    
    key = get_history_key(bot_id, user_id)
    if key not in conversations: conversations[key] = []
    history = conversations[key]
    history.append({"role": "user", "content": text})
    conversations[key] = history[-20:]
    
    # Include bot email in system prompt so LLM knows it
    bot_email = context.bot_data.get('bot_email', '')
    personality = context.bot_data.get('personality', "You are a helpful assistant.")
    if bot_email:
        personality += f"\n\nYour email address is {bot_email}. Users can receive emails at this address."
    
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
        await update.message.reply_text("Hit a snag.")

async def run_bot(bot_config):
    token = bot_config['bot_token']
    bot_id = bot_config['id']
    owner_id = bot_config['user_id']
    personality = bot_config.get('personality') or "You are a helpful assistant."
    if not token: return None
    
    app = Application.builder().token(token).build()
    
    # Get bot username to derive email
    bot_info = await app.bot.get_me()
    bot_email = get_bot_email(bot_info.username)
    
    app.bot_data['bot_id'] = bot_id
    app.bot_data['owner_id'] = owner_id
    app.bot_data['personality'] = personality
    app.bot_data['bot_email'] = bot_email  # Store email for handlers
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    logger.info(f"Starting @{bot_info.username} (email: {bot_email}, owner: {owner_id})")
    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    return app

async def check_for_new_bots():
    global running_bots
    while True:
        await asyncio.sleep(30)
        try:
            for bot in get_active_bots():
                if bot['id'] not in running_bots:
                    try:
                        app = await run_bot(bot)
                        if app: running_bots[bot['id']] = app
                    except Exception as e:
                        logger.error(f"Failed: {e}")
        except Exception as e:
            logger.error(f"Check error: {e}")

async def main():
    global running_bots
    logger.info("Multi-Bot Runner (EMAIL AWARE) starting...")
    ensure_tables()
    for bot in get_active_bots():
        try:
            app = await run_bot(bot)
            if app: running_bots[bot['id']] = app
        except Exception as e:
            logger.error(f"Failed bot {bot.get('id')}: {e}")
    logger.info(f"Running {len(running_bots)} bots")
    asyncio.create_task(check_for_new_bots())
    while True:
        await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())
