# ==============================================================================
# QUOTEX OTC ENTERPRISE-GRADE MASTER ALGORITHMIC RESEARCH BOT (RENDER WEB SERVICE EDITION)
# ==============================================================================

import os
import re
import json
import logging
import sqlite3
import hashlib
import asyncio
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from google import genai
from google.genai import types

# Render ওয়েব সার্ভিসের পোর্ট বাইন্ডিং পূরণের জন্য ফাস্টএপিআই ও উভিকর্ন
from fastapi import FastAPI
import uvicorn


# ==============================================================================
# SECTION 1: SYSTEM CONFIGURATION & ENVIRONMENT INITIALIZATION
# ==============================================================================

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash").strip()
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
DB_FILE = os.getenv("DATABASE_FILE", "quotex_otc_enterprise_master.db")
PORT = int(os.getenv("PORT", 10000))  # রেন্ডার থেকে ডায়নামিক পোর্ট নেবে

MAX_IMAGE_BYTES = 10 * 1024 * 1024  # 10MB Maximum Threshold

SUPPORTED_OTC_ASSETS = {
    "EURUSD_OTC", "GBPUSD_OTC", "AUDUSD_OTC", "USDJPY_OTC", 
    "GOLD_OTC", "USDCAD_OTC", "NZDUSD_OTC", "EURJPY_OTC",
    "GBPJPY_OTC", "EURGBP_OTC", "USDCHF_OTC", "SILVER_OTC"
}

OTC_ASSET_ALIASES = {
    "EURUSD": "EURUSD_OTC", "EURUSD_OTC": "EURUSD_OTC",
    "GBPUSD": "GBPUSD_OTC", "GBPUSD_OTC": "GBPUSD_OTC",
    "AUDUSD": "AUDUSD_OTC", "AUDUSD_OTC": "AUDUSD_OTC",
    "USDJPY": "USDJPY_OTC", "USDJPY_OTC": "USDJPY_OTC",
    "GOLD": "GOLD_OTC", "GOLD_OTC": "GOLD_OTC",
    "USDCAD": "USDCAD_OTC", "USDCAD_OTC": "USDCAD_OTC",
    "NZDUSD": "NZDUSD_OTC", "NZDUSD_OTC": "NZDUSD_OTC",
    "EURJPY": "EURJPY_OTC", "EURJPY_OTC": "EURJPY_OTC",
    "GBPJPY": "GBPJPY_OTC", "GBPJPY_OTC": "GBPJPY_OTC",
    "EURGBP": "EURGBP_OTC", "EURGBP_OTC": "EURGBP_OTC",
    "USDCHF": "USDCHF_OTC", "USDCHF_OTC": "USDCHF_OTC",
    "SILVER": "SILVER_OTC", "SILVER_OTC": "SILVER_OTC"
}


# ==============================================================================
# SECTION 2: ADVANCED LOGGING & DIAGNOSTIC CONFIGURATION
# ==============================================================================

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s (Line: %(lineno)d): %(message)s",
)
logger = logging.getLogger("QuotexOTCEnterpriseBotDeepCore")


# ==============================================================================
# SECTION 3: SYSTEM INTEGRITY VALIDATION
# ==============================================================================

if not TELEGRAM_BOT_TOKEN:
    logger.critical("TELEGRAM_BOT_TOKEN is missing. System startup aborted.")
    raise RuntimeError("CRITICAL ERROR: TELEGRAM_BOT_TOKEN is missing from environment variables.")

if not GEMINI_API_KEY:
    logger.critical("GEMINI_API_KEY is missing. System startup aborted.")
    raise RuntimeError("CRITICAL ERROR: GEMINI_API_KEY is missing from environment variables.")


# ==============================================================================
# SECTION 4: CLIENT, DISPATCHER & FASTAPI WEB SERVER INITIALIZATION
# ==============================================================================

gemini_client = genai.Client(api_key=GEMINI_API_KEY)
bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()

# Render পোর্ট ওপেন রাখার জন্য ফাস্টএপিআই ইনস্ট্যান্স
app = FastAPI()

@app.get("/")
def health_check():
    return {"status": "Quotex OTC Enterprise Bot is running live!", "timestamp": datetime.now(timezone.utc).isoformat()}


# ==============================================================================
# SECTION 5: ENTERPRISE DATABASE ARCHITECTURE (WAL + MULTI-TABLE MIGRATION)
# ==============================================================================

db_lock = asyncio.Lock()

def get_db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def initialize_enterprise_database() -> None:
    conn = get_db_connection()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS enterprise_analyses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                username TEXT,
                asset TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                decision TEXT NOT NULL,
                confidence_score REAL NOT NULL,
                market_state TEXT,
                full_json_payload TEXT,
                image_hash TEXT UNIQUE,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS paper_validation_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                analysis_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                candle_horizon INTEGER NOT NULL,
                outcome_status TEXT NOT NULL,
                notes TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (analysis_id) REFERENCES enterprise_analyses(id)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS audit_system_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                action_type TEXT,
                details TEXT,
                created_at TEXT NOT NULL
            )
        """)
        conn.commit()
        logger.info("Enterprise Database schema successfully verified and initialized.")
    finally:
        conn.close()

async def log_audit_event(user_id: Optional[int], action_type: str, details: str) -> None:
    async with db_lock:
        conn = get_db_connection()
        try:
            conn.execute(
                "INSERT INTO audit_system_logs (user_id, action_type, details, created_at) VALUES (?, ?, ?, ?)",
                (user_id, action_type, details, datetime.now(timezone.utc).isoformat())
            )
            conn.commit()
        except Exception:
            logger.exception("Failed to write system audit log.")
        finally:
            conn.close()


# ==============================================================================
# SECTION 6: UTILITIES, PARSING & NORMALIZATION SUB-SYSTEM
# ==============================================================================

def get_current_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def normalize_otc_asset_symbol(text: str) -> str:
    if not text:
        return "EURUSD_OTC"
    cleaned = text.strip().upper().replace(" ", "").replace("-", "")
    return OTC_ASSET_ALIASES.get(cleaned, cleaned if cleaned.endswith("_OTC") else f"{cleaned}_OTC")

def compute_sha256_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def extract_clean_json_from_ai_response(raw_text: str) -> Optional[Dict[str, Any]]:
    if not raw_text:
        return None
    cleaned = raw_text.strip()
    cleaned = re.sub(r"^```(?:json)?", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"```$", "", cleaned).strip()
    
    try:
        return json.loads(cleaned)
    except Exception:
        pass

    match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except Exception:
            return None
    return None

def clamp_confidence_score(val: Any) -> float:
    try:
        f = float(val)
    except Exception:
        return 0.0
    return max(0.0, min(100.0, f))


# ==============================================================================
# SECTION 7: PHD-GRADE OTC ALGORITHMIC FORENSIC VISION PROMPT
# ==============================================================================

ENTERPRISE_OTC_ANALYSIS_PROMPT = r"""
You are an elite Chief Algorithmic Architect, Synthetic Market Forensic Specialist, and Quantitative Binary Options Researcher with profound engineering insight into Quotex OTC broker mechanics.

SYNTHETIC MARKET DECONSTRUCTION & BROKER ARCHITECTURE:
- Quotex OTC feeds operate entirely on synthetic, mathematical generation algorithms managed by internal pseudo-random sequence engines and volatility distribution matrices.
- There is zero true interbank liquidity, zero macroeconomic dependency, and zero physical order book depth. Price respects solely the mathematical geometry, support/resistance reaction limits, and cyclic reset points programmed by the platform architecture.
- Market Phase Cycles: Price rotates through structured phases: Expansion (momentum blocks) -> Exhaustion (wick rejections) -> Consolidation (artificial ranging boxes) -> Reset (abrupt directional inversion).
- Retail Traps & Manipulation Patterns: The algorithm purposefully generates fake breakouts past obvious swing highs/lows, traps breakout traders, induces false confidence via hammer/shooting star wicks, and induces micro-slippages near expiration boundaries.

Perform an exhaustive forensic audit of the uploaded chart screenshot across these critical analytical dimensions:
1. **Algorithmic Phase & Cycle Evaluation:** Determine whether the synthetic feed is pushing an artificial trend wave, trapping retail participants in a consolidation box, or triggering a breakout pivot reset.
2. **Sureshot Geometry & Liquidity Traps:** Pinpoint engineered double tops/bottoms, inducement wicks, fake breakout zones, and liquidity sweeps designed by the broker algorithm.
3. **Execution Risk, Latency & Volatility Stability:** Evaluate if the price action flow is smooth and mathematically stable for short-term entry, or if it shows high probability of erratic micro-spikes, platform freezing, or slippage danger.

OUTPUT FORMAT REQUIREMENT:
You must respond with a strict, valid JSON object containing exactly these keys:
{
  "asset": "Matched asset string (e.g. EURUSD_OTC)",
  "timeframe": "Detected timeframe (e.g. 1M, 5M)",
  "chart_quality": "STABLE_ALGO / CHOPPY_NOISE / HIGH_MANIPULATION_OR_LAG_RISK",
  "market_state": "ALGORITHMIC_EXPANSION / RETAIL_TRAP_ZONE / CONSOLIDATION_RESET",
  "confidence_score": 0.0 to 100.0,
  "decision": "UP / DOWN / SKIP",
  "target_expectation": "Comprehensive short-term projection and candle trajectory for the next 1-3 candles",
  "algorithmic_reasoning": "Deep, granular technical breakdown explaining how the internal broker algorithm, price geometry, and liquidity cycles are behaving on this specific chart view",
  "execution_risk_advisory": "Explicit warning regarding slippage, freeze risk, structural manipulation, or timer anomalies associated with this chart state"
}
"""


# ==============================================================================
# SECTION 8: GEMINI AI VISION EXECUTION PIPELINE
# ==============================================================================

async def execute_gemini_vision_audit(image_bytes: bytes, mime_type: str, asset_hint: str) -> Dict[str, Any]:
    full_prompt = ENTERPRISE_OTC_ANALYSIS_PROMPT + f"\n\nUSER ASSET HINT PROVIDED: {asset_hint}"
    try:
        response = await gemini_client.aio.models.generate_content(
            model=GEMINI_MODEL,
            contents=[
                types.Part.from_bytes(data=image_bytes, mime_type=mime_type),
                full_prompt,
            ],
            config=types.GenerateContentConfig(
                temperature=0.15,
                response_mime_type="application/json",
            ),
        )
        raw_output = response.text or ""
        parsed_data = extract_clean_json_from_ai_response(raw_output)
        if not parsed_data:
            return {"error": "AI returned unparseable formatting structure.", "raw_output": raw_output[:1500]}
        return parsed_data
    except Exception as e:
        logger.exception("Gemini Enterprise Vision execution encountered a fatal exception.")
        return {"error": str(e)}


# ==============================================================================
# SECTION 9: TELEGRAM DYNAMIC KEYBOARD & REPORT FORMATTER
# ==============================================================================

def build_paper_inline_keyboard(analysis_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Correct (1M Win)", callback_data=f"paper_{analysis_id}_1_CORRECT"),
                InlineKeyboardButton(text="❌ Wrong (1M Loss)", callback_data=f"paper_{analysis_id}_1_WRONG")
            ],
            [
                InlineKeyboardButton(text="⚖️ Neutral / Tie", callback_data=f"paper_{analysis_id}_1_NEUTRAL")
            ]
        ]
    )

def build_enterprise_telegram_report(data: Dict[str, Any], analysis_id: Optional[int] = None) -> str:
    if "error" in data:
        return f"❌ **Enterprise Forensic Error**\n\n`{data['error']}`"

    asset = data.get("asset", "OTC_ASSET")
    timeframe = data.get("timeframe", "1M")
    quality = data.get("chart_quality", "UNKNOWN")
    state = data.get("market_state", "UNKNOWN")
    score = clamp_confidence_score(data.get("confidence_score", 0))
    decision = data.get("decision", "SKIP")
    target = data.get("target_expectation", "No target specified.")
    reasoning = data.get("algorithmic_reasoning", "No breakdown provided.")
    risk_advisory = data.get("execution_risk_advisory", "Exercise caution against synthetic slippage.")

    decision_banner = "⚠️ **SKIP / HIGH RISK ZONE**"
    if decision == "UP":
        decision_banner = "🟢 **ACTION: UP (CALL) [ALGO SIGNAL]**"
    elif decision == "DOWN":
        decision_banner = "🔴 **ACTION: DOWN (PUT) [ALGO SIGNAL]**"

    lines = [
        "🏛️ **QUOTEX OTC ENTERPRISE RESEARCH REPORT**",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"💱 **Asset:** `{asset}` | **TF:** `{timeframe}`",
        f"📊 **Market Structure:** `{state}`",
        f"⚙️ **Feed Stability:** `{quality}`",
        f"🧠 **Algorithmic Confidence:** `{score:.1f}%`",
        "",
        f"🚀 **Execution Decision:**\n{decision_banner}",
        "",
        f"🎯 **Short-Term Trajectory:**\n{target}",
        "",
        "🔬 **Granular Algorithmic Breakdown:**",
        f"{reasoning}",
        "",
        f"⚠️ **Platform Risk Advisory:**\n{risk_advisory}",
    ]

    if analysis_id:
        lines.append("")
        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        lines.append(f"📌 **Audit Reference ID:** `#{analysis_id}`")
        lines.append("💡 *Log outcome instantly using buttons below:*")

    return "\n".join(lines)


# ==============================================================================
# SECTION 10: DATABASE PERSISTENCE & ANALYTICS LAYER
# ==============================================================================

async def save_analysis_to_db(user_id: int, username: str, asset: str, timeframe: str, data: Dict[str, Any], img_hash: str) -> Optional[int]:
    async with db_lock:
        conn = get_db_connection()
        try:
            score = clamp_confidence_score(data.get("confidence_score", 0))
            decision = str(data.get("decision", "SKIP"))
            market_state = str(data.get("market_state", "UNKNOWN"))

            cursor = conn.execute(
                """
                INSERT INTO enterprise_analyses (
                    user_id, username, asset, timeframe, decision, 
                    confidence_score, market_state, full_json_payload, image_hash, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id, username, asset, timeframe, decision,
                    score, market_state, json.dumps(data, ensure_ascii=False),
                    img_hash, get_current_utc_iso()
                )
            )
            conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            row = conn.execute("SELECT id FROM enterprise_analyses WHERE image_hash = ?", (img_hash,)).fetchone()
            return row["id"] if row else None
        finally:
            conn.close()

async def fetch_enterprise_statistics() -> Dict[str, Any]:
    async with db_lock:
        conn = get_db_connection()
        try:
            total_scans = conn.execute("SELECT COUNT(*) AS c FROM enterprise_analyses").fetchone()["c"]
            actionable_signals = conn.execute("SELECT COUNT(*) AS c FROM enterprise_analyses WHERE decision != 'SKIP'").fetchone()["c"]
            skipped_zones = conn.execute("SELECT COUNT(*) AS c FROM enterprise_analyses WHERE decision = 'SKIP'").fetchone()["c"]

            outcomes = conn.execute(
                """
                SELECT 
                    COUNT(*) AS total,
                    SUM(CASE WHEN outcome_status = 'CORRECT' THEN 1 ELSE 0 END) AS correct,
                    SUM(CASE WHEN outcome_status = 'WRONG' THEN 1 ELSE 0 END) AS wrong,
                    SUM(CASE WHEN outcome_status = 'NEUTRAL' THEN 1 ELSE 0 END) AS neutral
                FROM paper_validation_results
                """
            ).fetchone()

            o_total = outcomes["total"] or 0
            correct = outcomes["correct"] or 0
            wrong = outcomes["wrong"] or 0
            neutral = outcomes["neutral"] or 0
            decided = correct + wrong
            accuracy = (correct / decided * 100) if decided > 0 else 0.0

            return {
                "total_scans": total_scans,
                "actionable_signals": actionable_signals,
                "skipped_zones": skipped_zones,
                "outcome_total": o_total,
                "correct": correct,
                "wrong": wrong,
                "neutral": neutral,
                "accuracy": accuracy,
            }
        finally:
            conn.close()


# ==============================================================================
# SECTION 11: TELEGRAM COMMAND ROUTERS & CALLBACK HANDLERS
# ==============================================================================

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await log_audit_event(message.from_user.id, "command_start", "/start invoked")
    welcome_text = """
🤖 **QUOTEX OTC ENTERPRISE RESEARCH BOT (v5.0 PHD EDITION)**

Built with absolute architectural depth to scan **Quotex synthetic price feeds, algorithmic loops, platform latency risks, and liquidity traps**.

📸 **How to use:**
Send a clean screenshot of your Quotex OTC chart. The engine will perform a full forensic scan of the underlying synthetic algorithm.

📋 **Core Commands:**
• `/start` - Launch system interface
• `/help` - View complete documentation
• `/status` - Check enterprise system health
• `/stats` - View aggregate precision analytics
• `/recent` - Review recent audit scans
• `/paper ID HORIZON RESULT` - Log paper outcome

⚠️ *Disclaimer: For educational research and algorithmic analysis only.*
""".strip()
    await message.answer(welcome_text)


@dp.message(Command("help"))
async def cmd_help(message: Message):
    help_text = """
📚 **ENTERPRISE COMMAND REFERENCE**

• `/start` - Initial bot menu
• `/status` - Check database WAL and polling status
• `/stats` - View global performance metrics and success rates
• `/recent` - List last 10 analyzed charts
• `/paper ID HORIZON RESULT` - Register test outcome

**Accepted Outcomes:** `CORRECT` | `WRONG` | `NEUTRAL`
*Example:* `/paper 45 1 CORRECT`
""".strip()
    await message.answer(help_text)


@dp.message(Command("status"))
async def cmd_status(message: Message):
    try:
        get_db_connection().close()
        db_health = "ONLINE (WAL Mode & Synchronous Optimal)"
    except Exception as e:
        db_health = f"ERROR: {e}"

    status_report = (
        "🟢 **ENTERPRISE SYSTEM STATUS REPORT**\n\n"
        f"• Telegram Dispatcher: ACTIVE (Webhook/Polling Hybrid)\n"
        f"• Vision Engine Model: `{GEMINI_MODEL}`\n"
        f"• Database Engine: {db_health}\n"
        f"• Core Architecture: Quotex Algorithmic Forensic Suite\n"
        f"• UTC Timestamp: {get_current_utc_iso()}"
    )
    await message.answer(status_report)


@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    stats = await fetch_enterprise_statistics()
    stats_text = f"""
📈 **ENTERPRISE PRECISION METRICS**

• Total Audited Charts: {stats['total_scans']}
• Actionable Signals Generated: {stats['actionable_signals']}
• Filtered / Skipped Zones: {stats['skipped_zones']}

📊 **Paper Validation Database:**
• Total Recorded Logs: {stats['outcome_total']}
• Correct Outcomes: {stats['correct']}
• Wrong Outcomes: {stats['wrong']}
• Neutral Outcomes: {stats['neutral']}

🎯 **Net Algorithmic Accuracy:** `{stats['accuracy']:.2f}%`
""".strip()
    await message.answer(stats_text)


@dp.message(Command("recent"))
async def cmd_recent(message: Message):
    async with db_lock:
        conn = get_db_connection()
        try:
            rows = conn.execute(
                """
                SELECT id, asset, timeframe, confidence_score, decision, created_at
                FROM enterprise_analyses
                ORDER BY id DESC
                LIMIT 10
                """
            ).fetchall()
        finally:
            conn.close()

    if not rows:
        await message.answer("No charts have been scanned and recorded yet.")
        return

    lines = ["🧾 **RECENT 10 AUDITED CHARTS**", ""]
    for r in rows:
        lines.append(
            f"#{r['id']} | `{r['asset']}` | {r['timeframe']} | "
            f"Conf: {r['confidence_score']:.1f}% | **{r['decision']}**"
        )
    await message.answer("\n".join(lines))


@dp.message(Command("paper"))
async def cmd_paper(message: Message):
    parts = (message.text or "").split()
    if len(parts) < 4:
        await message.answer(
            "❌ **Invalid Command Structure:**\n"
            "`/paper ID HORIZON RESULT`\n\n"
            "*Example:* `/paper 12 1 CORRECT`\n"
            "*Valid Results:* `CORRECT` / `WRONG` / `NEUTRAL`"
        )
        return

    try:
        analysis_id = int(parts[1])
        horizon = int(parts[2])
        outcome = parts[3].upper()
    except ValueError:
        await message.answer("❌ Numeric parsing failed in arguments.")
        return

    if outcome not in {"CORRECT", "WRONG", "NEUTRAL"}:
        await message.answer("❌ Invalid outcome status. Use CORRECT, WRONG, or NEUTRAL.")
        return

    async with db_lock:
        conn = get_db_connection()
        try:
            row = conn.execute("SELECT id FROM enterprise_analyses WHERE id = ? AND user_id = ?", (analysis_id, message.from_user.id)).fetchone()
            if not row:
                await message.answer("❌ Analysis ID not found or not owned by your user ID.")
                return

            conn.execute(
                """
                INSERT INTO paper_validation_results (analysis_id, user_id, candle_horizon, outcome_status, notes, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (analysis_id, message.from_user.id, horizon, outcome, "", get_current_utc_iso())
            )
            conn.commit()
            success = True
        except Exception:
            success = False
        finally:
            conn.close()

    if success:
        await message.answer(
            f"✅ **Paper Validation Logged Successfully!**\n\n"
            f"• Audit Reference: `#{analysis_id}`\n"
            f"• Candle Horizon: {horizon}\n"
            f"• Logged Result: `{outcome}`"
        )
    else:
        await message.answer("❌ Database write failed while saving paper result.")


@dp.callback_query(F.data.startswith("paper_"))
async def callback_paper_handler(callback: CallbackQuery):
    parts = callback.data.split("_")
    if len(parts) < 5:
        await callback.answer("Invalid callback payload.", show_alert=True)
        return

    try:
        analysis_id = int(parts[1])
        horizon = int(parts[2])
        outcome = parts[3].upper()
    except ValueError:
        await callback.answer("Parsing error.", show_alert=True)
        return

    async with db_lock:
        conn = get_db_connection()
        try:
            conn.execute(
                """
                INSERT INTO paper_validation_results (analysis_id, user_id, candle_horizon, outcome_status, notes, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (analysis_id, callback.from_user.id, horizon, outcome, "Inline Button Log", get_current_utc_iso())
            )
            conn.commit()
            success = True
        except Exception:
            success = False
        finally:
            conn.close()

    if success:
        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.message.answer(f"✅ **Paper Result Logged via Button:** `#{analysis_id}` -> **{outcome}**")
        await callback.answer("Logged successfully!")
    else:
        await callback.answer("Failed to log into database.", show_alert=True)


# ==============================================================================
# SECTION 12: IMAGE UPLOAD & FORENSIC PIPELINE HANDLER
# ==============================================================================

@dp.message(F.photo | F.document)
async def handle_chart_image_upload(message: Message):
    status_msg = await message.answer("🔄 **Executing Enterprise Forensic Audit...**\nAnalyzing synthetic price loops, algorithmic momentum, and lag/freeze risks.")

    try:
        file_id = None
        mime_type = "image/jpeg"

        if message.photo:
            file_id = message.photo[-1].file_id
        elif message.document and message.document.mime_type and message.document.mime_type.startswith("image/"):
            file_id = message.document.file_id
            mime_type = message.document.mime_type

        if not file_id:
            await status_msg.edit_text("❌ Could not extract image file from message.")
            return

        file_info = await bot.get_file(file_id)
        if not file_info.file_path:
            await status_msg.edit_text("❌ File path resolution failed.")
            return

        file_io = await bot.download_file(file_info.file_path)
        image_bytes = file_io.read()

        if len(image_bytes) > MAX_IMAGE_BYTES:
            await status_msg.edit_text("❌ Image file size exceeds the 10MB enterprise threshold.")
            return

        img_hash = compute_sha256_hash(image_bytes)

        async with db_lock:
            conn = get_db_connection()
            existing = conn.execute("SELECT id, created_at FROM enterprise_analyses WHERE image_hash = ?", (img_hash,)).fetchone()
            conn.close()

        if existing:
            await status_msg.edit_text(
                f"♻️ **Duplicate Chart Audit Detected**\n\n"
                f"This exact chart screenshot has already been evaluated.\n"
                f"• Reference ID: `#{existing['id']}`\n"
                f"• Initial Scan Time: `{existing['created_at']}`"
            )
            return

        caption_text = message.caption or ""
        detected_hint = normalize_otc_asset_symbol(caption_text)

        ai_response_dict = await execute_gemini_vision_audit(image_bytes, mime_type, detected_hint)

        if "error" in ai_response_dict:
            await status_msg.edit_text(f"❌ **AI Forensic Audit Failed:**\n\n`{str(ai_response_dict['error'])[:1200]}`")
            return

        final_asset = normalize_otc_asset_symbol(ai_response_dict.get("asset", detected_hint))
        final_timeframe = str(ai_response_dict.get("timeframe", "1M"))

        analysis_id = await save_analysis_to_db(
            user_id=message.from_user.id,
            username=message.from_user.username or "Anonymous",
            asset=final_asset,
            timeframe=final_timeframe,
            data=ai_response_dict,
            img_hash=img_hash
        )

        report_markdown = build_enterprise_telegram_report(ai_response_dict, analysis_id)
        keyboard = build_paper_inline_keyboard(analysis_id) if analysis_id else None

        await status_msg.edit_text(report_markdown, parse_mode="Markdown", reply_markup=keyboard)
        await log_audit_event(message.from_user.id, "chart_audit_complete", f"analysis_id={analysis_id}, asset={final_asset}")

    except Exception as e:
        logger.exception("Unexpected error during chart image processing pipeline.")
        try:
            await status_msg.edit_text(f"❌ **System Exception:**\n\n`{str(e)[:1200]}`")
        except Exception:
            pass


# ==============================================================================
# SECTION 13: TEXT FALLBACK HANDLER
# ==============================================================================

@dp.message(F.text)
async def handle_text_message_fallback(message: Message):
    txt = (message.text or "").strip()
    if not txt:
        return
    if txt.lower() in {"hi", "hello", "hey", "salam"}:
        await message.answer("👋 Hello! Send a Quotex OTC chart screenshot to run the enterprise algorithmic audit.")
        return
    await message.answer("📸 Please send a clean screenshot of your Quotex OTC chart to initiate analysis, or type `/help` for guidance.")


# ==============================================================================
# SECTION 14: GLOBAL DISPATCHER ERROR HANDLER
# ==============================================================================

@dp.errors()
async def global_dispatcher_error_handler(event):
    logger.exception("Global Telegram dispatcher error: %s", getattr(event, "exception", event))


# ==============================================================================
# SECTION 15: BACKGROUND RUNNER & MAIN APPLICATION LIFECYCLE
# ==============================================================================

async def run_telegram_bot():
    initialize_enterprise_database()
    logger.info("==================================================")
    logger.info("Quotex OTC Enterprise Research Bot Initialized (v5.0)")
    logger.info("Configured Gemini Model: %s", GEMINI_MODEL)
    logger.info("Active Database Target: %s", DB_FILE)
    logger.info("==================================================")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(run_telegram_bot())


if __name__ == "__main__":
    try:
        uvicorn.run(app, host="0.0.0.0", port=PORT)
    except KeyboardInterrupt:
        logger.info("Enterprise Bot gracefully terminated via keyboard interrupt.")

