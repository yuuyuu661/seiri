import os
import io
import json
import math
import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

import discord
from discord.ext import commands
from discord import app_commands

# ========= 環境変数 =========
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")  # 必須
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
GUILD_IDS = [int(x.strip()) for x in os.getenv("GUILD_IDS", "1398607685158440991").split(",") if x.strip().isdigit()]
PRIMARY_GUILD_ID = GUILD_IDS[0] if GUILD_IDS else None

# ========= ログ =========
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="(%(asctime)s) [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("vc_text_archiver")

# ========= 永続 =========
DATA_DIR = "data_vc_text"
os.makedirs(DATA_DIR, exist_ok=True)
SETTINGS_FILE = os.path.join(DATA_DIR, "settings.json")

# ========= Intents / Bot =========
intents = discord.Intents.default()
intents.message_content = True   # ポータルで MESSAGE CONTENT をONに
intents.guilds = True

bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree

# ========= 設定・状態 =========
# guild_settings = {
#   guild_id: {
#       "log_channel_id": int|None,
#       "max_messages_per_channel": int,
#       "category_whitelist": [int, ...],  # 追加：記録対象カテゴリーID（空=全VC）
#   }
# }
guild_settings: Dict[int, Dict] = {}
vc_text_buffer: Dict[int, List[Dict]] = {}

DEFAULT_SETTINGS = {
    "log_channel_id": None,
    "max_messages_per_channel": 5000,
    "category_whitelist": [],  # ← 新規
}

# ========= ユーティリティ =========
def channel_file_path(channel_id: int) -> str:
    return os.path.join(DATA_DIR, f"{channel_id}.json")

def load_settings():
    global guild_settings
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            guild_settings = {int(k): v for k, v in data.items()}
        except Exception as e:
            log.exception("設定ファイル読み込み失敗: %s", e)
            guild_settings = {}
    else:
        guild_settings = {}

def save_settings():
    try:
        with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(guild_settings, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.exception("設定ファイル保存失敗: %s", e)

def guild_conf(guild_id: int) -> Dict:
    conf = guild_settings.get(guild_id)
    if not conf:
        conf = DEFAULT_SETTINGS.copy()
        guild_settings[guild_id] = conf
        save_settings()
    # 欠けキー埋め
    for k, v in DEFAULT_SETTINGS.items():
        conf.setdefault(k, v)
    # 型整備
    if not isinstance(conf.get("category_whitelist"), list):
        conf["category_whitelist"] = []
    return conf

def append_message_to_disk(channel_id: int, record: Dict):
    path = channel_file_path(channel_id)
    data = []
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = []
    data.append(record)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception as e:
        log.exception("VCテキストの書き込み失敗: %s", e)

def dedup(records: List[Dict]) -> List[Dict]:
    seen = set()
    out = []
    for r in records:
        key = (
            r.get("message_id"),
            r.get("ts"),
            r.get("content"),
            bool(r.get("edited")),
            bool(r.get("deleted")),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out

def load_channel_records(channel_id: int) -> List[Dict]:
    mem = vc_text_buffer.get(channel_id, [])
    disk = []
    path = channel_file_path(channel_id)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                disk = json.load(f)
        except Exception:
            disk = []
    return dedup(disk + mem)

def remove_channel_disk(channel_id: int):
    path = channel_file_path(channel_id)
    if os.path.exists(path):
        try:
            os.remove(path)
        except Exception:
            pass

def is_voice_like(ch: discord.abc.GuildChannel) -> bool:
    return getattr(ch, "type", None) in (discord.ChannelType.voice, discord.ChannelType.stage_voice)

def is_voice_message(message: discord.Message) -> bool:
    return getattr(message.channel, "type", None) in (discord.ChannelType.voice, discord.ChannelType.stage_voice)

def in_target_categories(guild_id: int, channel: discord.abc.GuildChannel) -> bool:
    """カテゴリー制限（ホワイトリスト）。空なら全許可。"""
    conf = guild_conf(guild_id)
    wl = conf.get("category_whitelist") or []
    if not wl:
        return True
    parent = getattr(channel, "category", None)
    if parent is None:
        return False
    return parent.id in wl

def fmt_record(rec: Dict) -> str:
    t = rec.get("ts")
    try:
        ts = datetime.fromisoformat(t).astimezone()
        ts_s = ts.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        ts_s = t
    flags = []
    if rec.get("edited"):
        flags.append("(edited)")
    if rec.get("deleted"):
        flags.append("(deleted)")
    flag_s = " ".join(flags) if flags else ""
    base = f"[{ts_s}] {rec.get('author_name')}({rec.get('author_id')}): {rec.get('content')}"
    atts = rec.get("attachments") or []
    if atts:
        base += "\n  attachments:\n" + "\n".join([f"  - {u}" for u in atts])
    if flag_s:
        base += f"  {flag_s}"
    return base

def build_txt(parts: List[Dict]) -> str:
    return "\n".join(fmt_record(r) for r in parts)

async def send_chunked_logs(
    guild: discord.Guild,
    dest_channel_id: Optional[int],
    deleted_channel: discord.abc.GuildChannel,
    all_records: List[Dict],
):
    if not dest_channel_id:
        log.warning("ログ送信先未設定のためスキップ（guild=%s, channel=%s）", guild.id, deleted_channel.id)
        return

    dest = guild.get_channel(dest_channel_id)
    if not dest:
        try:
            dest = await guild.fetch_channel(dest_channel_id)
        except Exception:
            log.warning("指定のログチャンネルが見つかりません: %s", dest_channel_id)
            return

    all_records = dedup(all_records)

    text = build_txt(all_records)
    raw = text.encode("utf-8", errors="ignore")
    MAX = 7_500_000
    chunks = max(1, math.ceil(len(raw) / MAX)) if raw else 1

    header = (
        f"🔔 **VCテキストログ（チャンネル削除検知）**\n"
        f"- ギルド: {guild.name} ({guild.id})\n"
        f"- 削除チャンネル: {deleted_channel.name} ({deleted_channel.id})\n"
        f"- カテゴリー: {(deleted_channel.category.name if deleted_channel.category else 'なし')} "
        f"({deleted_channel.category.id if deleted_channel.category else '—'})\n"
        f"- 総メッセージ: {len(all_records)} 件\n"
        f"- 生成: {datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S')}\n"
    )
    await dest.send(header)

    if not raw:
        await dest.send("（メッセージは記録されていませんでした）")
        return

    for i in range(chunks):
        start = i * MAX
        end = min((i + 1) * MAX, len(raw))
        buf = io.BytesIO(raw[start:end])
        buf.name = f"vc_text_{deleted_channel.id}_part{i+1}of{chunks}.txt"
        await dest.send(file=discord.File(buf))

# ========= イベント =========
@bot.event
async def on_ready():
    load_settings()
    if GUILD_IDS:
        for gid in GUILD_IDS:
            try:
                await tree.sync(guild=discord.Object(id=gid))
                log.info("Slash commands synced for guild %s", gid)
            except Exception as e:
                log.warning("Guild %s へのsyncに失敗: %s", gid, e)
    else:
        try:
            await tree.sync()
            log.info("Global slash commands synced")
        except Exception as e:
            log.warning("Global sync failed: %s", e)
    log.info("Logged in as %s (%s)", bot.user, bot.user.id)

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return
    if not is_voice_message(message):
        return
    if not in_target_categories(message.guild.id, message.channel):
        return

    rec = {
        "ts": message.created_at.astimezone().isoformat(),
        "author_id": str(message.author.id),
        "author_name": f"{message.author.display_name}",
        "content": message.content or "",
        "attachments": [a.url for a in message.attachments] if message.attachments else [],
        "edited": False,
        "deleted": False,
        "message_id": str(message.id),
    }
    vc_text_buffer.setdefault(message.channel.id, []).append(rec)
    append_message_to_disk(message.channel.id, rec)

@bot.event
async def on_message_edit(before: discord.Message, after: discord.Message):
    if not is_voice_message(after):
        return
    if not in_target_categories(after.guild.id, after.channel):
        return
    rec = {
        "ts": (after.edited_at or datetime.now().astimezone()).isoformat(),
        "author_id": str(after.author.id),
        "author_name": f"{after.author.display_name}",
        "content": f"(編集後) {after.content or ''}",
        "attachments": [a.url for a in after.attachments] if after.attachments else [],
        "edited": True,
        "deleted": False,
        "message_id": str(after.id),
    }
    vc_text_buffer.setdefault(after.channel.id, []).append(rec)
    append_message_to_disk(after.channel.id, rec)

@bot.event
async def on_message_delete(message: discord.Message):
    if not is_voice_message(message):
        return
    if not in_target_categories(message.guild.id, message.channel):
        return
    rec = {
        "ts": datetime.now().astimezone().isoformat(),
        "author_id": str(message.author.id) if message.author else "unknown",
        "author_name": getattr(message.author, "display_name", "unknown"),
        "content": "(このメッセージは削除されました)",
        "attachments": [],
        "edited": False,
        "deleted": True,
        "message_id": str(message.id),
    }
    vc_text_buffer.setdefault(message.channel.id, []).append(rec)
    append_message_to_disk(message.channel.id, rec)

@bot.event
async def on_guild_channel_delete(channel: discord.abc.GuildChannel):
    if not is_voice_like(channel):
        return
    if not in_target_categories(channel.guild.id, channel):
        return
    guild = channel.guild
    conf = guild_conf(guild.id)

    all_records = load_channel_records(channel.id)
    max_keep = int(conf.get("max_messages_per_channel", 5000))
    if len(all_records) > max_keep:
        all_records = all_records[-max_keep:]

    try:
        await send_chunked_logs(guild, conf.get("log_channel_id"), channel, all_records)
    finally:
        vc_text_buffer.pop(channel.id, None)
        remove_channel_disk(channel.id)

# ========= コマンド =========
class GuildConfGroup(app_commands.Group):
    def __init__(self):
        super().__init__(name="vcchatlog", description="VCテキスト削除時ログの設定")

    @app_commands.checks.has_permissions(manage_guild=True)
    @app_commands.command(name="set_log_channel", description="ログ送信先チャンネルを設定")
    async def set_log_channel(self, interaction: discord.Interaction, channel: discord.TextChannel):
        conf = guild_conf(interaction.guild_id)
        conf["log_channel_id"] = channel.id
        save_settings()
        await interaction.response.send_message(f"✅ ログ送信先を {channel.mention} に設定しました。", ephemeral=True)

    @app_commands.checks.has_permissions(manage_guild=True)
    @app_commands.command(name="set_max", description="1チャンネルあたり保持する最大メッセージ数")
    async def set_max(self, interaction: discord.Interaction, count: app_commands.Range[int, 100, 200000] = 5000):
        conf = guild_conf(interaction.guild_id)
        conf["max_messages_per_channel"] = int(count)
        save_settings()
        await interaction.response.send_message(f"✅ 最大保持件数を {count} に設定しました。", ephemeral=True)

    # ---- 新規: カテゴリー制御 ----
    @app_commands.checks.has_permissions(manage_guild=True)
    @app_commands.command(name="add_category", description="記録対象カテゴリーを追加")
    async def add_category(self, interaction: discord.Interaction, category: discord.CategoryChannel):
        conf = guild_conf(interaction.guild_id)
        wl = conf.get("category_whitelist") or []
        if category.id not in wl:
            wl.append(category.id)
            conf["category_whitelist"] = wl
            save_settings()
            await interaction.response.send_message(f"✅ 追加: {category.name}（ID: {category.id}）", ephemeral=True)
        else:
            await interaction.response.send_message(f"ℹ️ すでに追加済み: {category.name}", ephemeral=True)

    @app_commands.checks.has_permissions(manage_guild=True)
    @app_commands.command(name="remove_category", description="記録対象カテゴリーを削除")
    async def remove_category(self, interaction: discord.Interaction, category: discord.CategoryChannel):
        conf = guild_conf(interaction.guild_id)
        wl = conf.get("category_whitelist") or []
        if category.id in wl:
            wl.remove(category.id)
            conf["category_whitelist"] = wl
            save_settings()
            await interaction.response.send_message(f"🗑️ 削除: {category.name}（ID: {category.id}）", ephemeral=True)
        else:
            await interaction.response.send_message(f"ℹ️ 見つかりませんでした: {category.name}", ephemeral=True)

    @app_commands.checks.has_permissions(manage_guild=True)
    @app_commands.command(name="list_categories", description="記録対象カテゴリーの一覧を表示")
    async def list_categories(self, interaction: discord.Interaction):
        conf = guild_conf(interaction.guild_id)
        wl = conf.get("category_whitelist") or []
        if not wl:
            await interaction.response.send_message("📄 現在の対象: **すべてのカテゴリー**（ホワイトリスト未設定）", ephemeral=True)
            return
        lines = []
        for cid in wl:
            cat = interaction.guild.get_channel(cid)
            name = cat.name if isinstance(cat, discord.CategoryChannel) else "（不明 or 権限不足）"
            lines.append(f"- {name}（ID: {cid}）")
        await interaction.response.send_message("📄 記録対象カテゴリー:\n" + "\n".join(lines), ephemeral=True)

    @app_commands.checks.has_permissions(manage_guild=True)
    @app_commands.command(name="clear_categories", description="記録対象カテゴリーをすべて解除（全VC対象に戻す）")
    async def clear_categories(self, interaction: discord.Interaction):
        conf = guild_conf(interaction.guild_id)
        conf["category_whitelist"] = []
        save_settings()
        await interaction.response.send_message("🧹 クリアしました。以後は**全カテゴリー**が対象になります。", ephemeral=True)
    # ---- ここまで新規 ----

    @app_commands.checks.has_permissions(manage_guild=True)
    @app_commands.command(name="status", description="現在の設定を表示")
    async def status(self, interaction: discord.Interaction):
        conf = guild_conf(interaction.guild_id)
        log_ch = f"<#{conf['log_channel_id']}>" if conf.get("log_channel_id") else "未設定"
        wl = conf.get("category_whitelist") or []
        if not wl:
            cat_info = "すべてのカテゴリー（制限なし）"
        else:
            names = []
            for cid in wl:
                cat = interaction.guild.get_channel(cid)
                names.append(cat.name if isinstance(cat, discord.CategoryChannel) else f"ID:{cid}")
            cat_info = ", ".join(names)
        await interaction.response.send_message(
            f"**VCテキスト自動ログ 設定**\n"
            f"- ログ送信先: {log_ch}\n"
            f"- 最大保持件数: {conf.get('max_messages_per_channel', 5000)}\n"
            f"- 対象カテゴリー: {cat_info}\n",
            ephemeral=True
        )

    @app_commands.checks.has_permissions(manage_guild=True)
    @app_commands.command(name="purge_cache", description="一時保存とJSONを全削除（重複が溜まったとき等）")
    async def purge_cache(self, interaction: discord.Interaction):
        vc_text_buffer.clear()
        for fn in os.listdir(DATA_DIR):
            if fn.endswith(".json") and fn != os.path.basename(SETTINGS_FILE):
                try:
                    os.remove(os.path.join(DATA_DIR, fn))
                except Exception:
                    pass
        await interaction.response.send_message("🧹 一時保存とJSONをクリアしました。", ephemeral=True)

# ギルド即時登録（PRIMARY_GUILD_IDがあれば）
tree.add_command(GuildConfGroup(), guild=discord.Object(id=PRIMARY_GUILD_ID) if PRIMARY_GUILD_ID else None)

# ========= 実行 =========
if __name__ == "__main__":
    if not DISCORD_TOKEN:
        raise RuntimeError("DISCORD_TOKEN が未設定です。")
    bot.run(DISCORD_TOKEN)
