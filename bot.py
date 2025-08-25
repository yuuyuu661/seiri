import os
import io
import json
import math
import gzip
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Iterable

import discord
from discord.ext import commands, tasks
from discord import app_commands

# ========= 環境変数 =========
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")  # 必須
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# 即時反映したいサーバーIDをカンマ区切りで（例: "1398607685158440991,123..."）
GUILD_IDS = [int(x.strip()) for x in os.getenv("GUILD_IDS", "1398607685158440991").split(",") if x.strip().isdigit()]
PRIMARY_GUILD_ID = GUILD_IDS[0] if GUILD_IDS else None

# ========= 権限ロール（このロール保持者のみ設定系/バックアップ系コマンドを使える） =========
ALLOWED_ROLE_ID = int(os.getenv("ALLOWED_ROLE_ID", "1398724601256874014"))

# ========= バックアップ関連 環境変数 =========
BACKUP_DIR = os.getenv("BACKUP_DIR", "./data/backups")
REPORT_CHANNEL_ID = int(os.getenv("REPORT_CHANNEL_ID", "0"))  # スナップショット送信先チャンネルID（未設定可）
DEFAULT_MESSAGE_CHANNEL_IDS = os.getenv("BACKUP_MESSAGE_CHANNEL_IDS", "")  # 例: "123,456"

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
intents.message_content = True   # 開発者ポータルで MESSAGE CONTENT を ON
intents.guilds = True
intents.members = True           # メンバー一覧バックアップのため Server Members Intent を ON
intents.messages = True
intents.reactions = True

bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree

# ========= タイムゾーン（JST） =========
JST = timezone(timedelta(hours=9))

# ========= 設定・状態 =========
# guild_settings = {
#   guild_id: {
#       "log_channel_id": int|None,
#       "max_messages_per_channel": int,
#       "category_whitelist": [int, ...],  # 記録対象カテゴリーID（空=全VC）
#   }
# }
guild_settings: Dict[int, Dict] = {}
# { channel_id: [ {ts, author_id, author_name, content, attachments, edited, deleted, message_id}, ... ] }
vc_text_buffer: Dict[int, List[Dict]] = {}

DEFAULT_SETTINGS = {
    "log_channel_id": None,
    "max_messages_per_channel": 5000,
    "category_whitelist": [],
}

# ========= 共通ユーティリティ =========
def parse_id_list(text: Optional[str]) -> List[int]:
    if not text:
        return []
    out: List[int] = []
    for part in text.replace(" ", "").split(","):
        if part.isdigit():
            out.append(int(part))
    return out

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def jst_now_iso() -> str:
    return datetime.now(JST).isoformat()

# ========= 既存：VCテキスト保存関連 =========
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
    for k, v in DEFAULT_SETTINGS.items():
        conf.setdefault(k, v)
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
    """message_id / ts / content / edited / deleted をキーに重複排除"""
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
    """ディスク+メモリをマージし、重複除去して返す"""
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
    MAX = 7_500_000  # Discord添付分割の安全閾値
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

# ========= バックアップ（スナップショット）機能 =========

def _ow_serialize(ow: Dict[Any, discord.PermissionOverwrite]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for target, perm in ow.items():
        allow, deny = perm.pair()
        out.append({
            "type": "role" if isinstance(target, discord.Role) else "member",
            "id": target.id,
            "allow": allow.value,
            "deny": deny.value
        })
    return out

def _ch_serialize(ch: discord.abc.GuildChannel) -> Dict[str, Any]:
    base = {
        "id": ch.id,
        "name": ch.name,
        "type": str(ch.type),
        "position": getattr(ch, "position", 0),
        "nsfw": getattr(ch, "nsfw", False),
        "overwrites": _ow_serialize(ch.overwrites),
        "topic": getattr(ch, "topic", None),
        "parent_id": ch.category_id if hasattr(ch, "category_id") else None,
        "extra": {}
    }
    if isinstance(ch, discord.TextChannel):
        base["extra"] = {"slowmode_delay": ch.slowmode_delay, "default_thread_auto_archive_duration": ch.default_auto_archive_duration}
    elif isinstance(ch, discord.VoiceChannel):
        base["extra"] = {"bitrate": ch.bitrate, "user_limit": ch.user_limit}
    elif isinstance(ch, discord.ForumChannel):
        base["extra"] = {"default_thread_slowmode_delay": ch.default_thread_slowmode_delay}
    return base

async def dump_guild_structure(guild: discord.Guild) -> Dict[str, Any]:
    roles = [
        {
            "id": r.id, "name": r.name, "color": r.color.value, "hoist": r.hoist,
            "managed": r.managed, "mentionable": r.mentionable,
            "permissions": r.permissions.value, "position": r.position
        }
        for r in sorted(guild.roles, key=lambda x: x.position, reverse=False)
    ]

    categories = []
    for cat in sorted([c for c in guild.channels if isinstance(c, discord.CategoryChannel)], key=lambda x: x.position):
        cat_data = {
            "id": cat.id,
            "name": cat.name,
            "position": cat.position,
            "overwrites": _ow_serialize(cat.overwrites),
            "children": []
        }
        for ch in sorted(cat.channels, key=lambda x: x.position):
            cat_data["children"].append(_ch_serialize(ch))
        categories.append(cat_data)

    uncategorized = [
        _ch_serialize(ch)
        for ch in sorted(guild.channels, key=lambda x: getattr(x, "position", 0))
        if ch.category is None and not isinstance(ch, discord.CategoryChannel)
    ]

    return {
        "meta": {
            "guild_id": guild.id,
            "guild_name": guild.name,
            "icon_url": guild.icon.url if guild.icon else None,
            "preferred_locale": guild.preferred_locale,
            "afk_timeout": guild.afk_timeout,
            "verification_level": str(guild.verification_level),
            "system_channel_id": guild.system_channel.id if guild.system_channel else None,
            "rules_channel_id": guild.rules_channel.id if guild.rules_channel else None,
            "public_updates_channel_id": guild.public_updates_channel.id if guild.public_updates_channel else None,
            "exported_at": jst_now_iso(),
        },
        "roles": roles,
        "categories": categories,
        "uncategorized_channels": uncategorized,
    }

async def dump_members(guild: discord.Guild) -> List[Dict[str, Any]]:
    members: List[Dict[str, Any]] = []
    async for m in guild.fetch_members(limit=None):
        members.append({
            "id": m.id,
            "name": m.name,
            "global_name": m.global_name,
            "display_name": m.display_name,
            "discriminator": m.discriminator,
            "bot": m.bot,
            "roles": [r.id for r in m.roles if r.name != "@everyone"],
            "joined_at": m.joined_at.astimezone(JST).isoformat() if m.joined_at else None,
            "premium_since": m.premium_since.astimezone(JST).isoformat() if m.premium_since else None,
        })
    return members

def _serialize_message(msg: discord.Message) -> Dict[str, Any]:
    return {
        "id": msg.id,
        "channel_id": msg.channel.id if msg.channel else None,
        "author_id": msg.author.id if msg.author else None,
        "author_name": getattr(msg.author, "name", None),
        "author_discriminator": getattr(msg.author, "discriminator", None),
        "content": msg.content,
        "created_at": msg.created_at.astimezone(JST).isoformat(),
        "edited_at": msg.edited_at.astimezone(JST).isoformat() if msg.edited_at else None,
        "mentions": [u.id for u in msg.mentions],
        "role_mentions": [r.id for r in msg.role_mentions],
        "attachments": [
            {"url": a.url, "filename": a.filename, "size": a.size, "content_type": a.content_type}
            for a in msg.attachments
        ],
        "embeds": [e.to_dict() for e in msg.embeds],
        "reactions": [
            {"emoji": str(r.emoji), "count": r.count, "me": r.me}
            for r in msg.reactions
        ],
        "reference": ({
            "message_id": msg.reference.message_id,
            "channel_id": msg.reference.channel_id,
            "guild_id": msg.reference.guild_id,
            "type": msg.reference.type.name if msg.reference.type else None
        } if msg.reference else None)
    }

async def append_jsonl(path: str, obj: Dict[str, Any]):
    ensure_dir(os.path.dirname(path))
    line = json.dumps(obj, ensure_ascii=False)
    with gzip.open(path, "at", encoding="utf-8") as f:
        f.write(line + "\n")

async def write_json(path: str, data: Any):
    ensure_dir(os.path.dirname(path))
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

async def dump_messages_for_channels(guild: discord.Guild, since_utc: datetime, channel_ids: Iterable[int], base_dir: str) -> Dict[int, int]:
    """
    指定チャンネルIDのみ、since_utc 以降のメッセージを JSONL.GZ で書き出す。
    return: {channel_id: dumped_count}
    """
    ensure_dir(base_dir)
    dumped: Dict[int, int] = {}

    id_to_textch: Dict[int, discord.TextChannel] = {c.id: c for c in guild.text_channels}

    for cid in channel_ids:
        ch = id_to_textch.get(cid)
        if not ch:
            log.warning(f"channel id {cid} not found or not a TextChannel in guild {guild.id}")
            continue
        if not ch.permissions_for(guild.me).read_messages:
            log.warning(f"no read permission for #{ch.name} ({cid})")
            continue

        path = os.path.join(base_dir, f"messages-{cid}.jsonl.gz")
        count = 0
        async for msg in ch.history(limit=None, after=since_utc, oldest_first=True):
            await append_jsonl(path, _serialize_message(msg))
            count += 1
            if count % 1000 == 0:
                await asyncio.sleep(0)
        dumped[cid] = count
        log.info(f"dumped {count} messages from #{ch.name} ({cid})")

    return dumped

def snapshot_dir_for(guild_id: int) -> str:
    stamp = datetime.now(JST).strftime("%Y%m%d-%H%M%SJST")
    return os.path.join(BACKUP_DIR, str(guild_id), stamp)

async def create_snapshot(guild: discord.Guild, message_channel_ids: List[int]) -> str:
    snap_dir = snapshot_dir_for(guild.id)
    ensure_dir(snap_dir)

    # 1) 構造
    structure = await dump_guild_structure(guild)
    await write_json(os.path.join(snap_dir, "guild.json.gz"), structure)

    # 2) メンバー
    members = await dump_members(guild)
    await write_json(os.path.join(snap_dir, "members.json.gz"), members)

    # 3) メッセージ（過去7日）
    since_utc = datetime.now(timezone.utc) - timedelta(days=7)
    msg_dir = os.path.join(snap_dir, "messages")
    dumped = await dump_messages_for_channels(guild, since_utc, message_channel_ids, msg_dir)

    # 4) マニフェスト
    manifest = {
        "guild_id": guild.id,
        "guild_name": guild.name,
        "exported_at": jst_now_iso(),
        "message_channels": message_channel_ids,
        "message_counts": dumped,
    }
    await write_json(os.path.join(snap_dir, "manifest.json.gz"), manifest)

    return snap_dir

async def send_snapshot_summary(
    guild: discord.Guild,
    target: Optional[discord.abc.Messageable],   # ← 型修正済み
    snap_dir: str,
    via_followup: Optional[discord.Webhook] = None
):
    """
    guild.json.gz / members.json.gz / manifest.json.gz と messages/*.jsonl.gz を分割送信。
    via_followup（interaction.followup）を渡すと実行者にも送信。
    target が None の場合はチャンネル送信をスキップ。
    """
    def collect_files() -> List[str]:
        files: List[str] = []
        for root, _, filenames in os.walk(snap_dir):
            for fn in filenames:
                if fn.endswith(".gz"):
                    files.append(os.path.join(root, fn))
        return files

    files = collect_files()
    head = [p for p in files if p.endswith("guild.json.gz") or p.endswith("members.json.gz") or p.endswith("manifest.json.gz")]
    msg_files = [p for p in files if "/messages/" in p.replace("\\", "/")]

    header = f"📦 週次スナップショットを作成しました\n- Guild: **{guild.name}** ({guild.id})\n- Path: `{snap_dir}`"

    async def send_fn(content: Optional[str] = None, filepaths: Optional[List[str]] = None):
        # 実行者へのフォローアップ
        if via_followup is not None:
            if filepaths:
                await via_followup.send(content or discord.utils.MISSING,
                                        files=[discord.File(p, filename=os.path.basename(p)) for p in filepaths])
            else:
                await via_followup.send(content or discord.utils.MISSING)
        # チャンネルへ
        if target is not None:
            if filepaths:
                await target.send(content or discord.utils.MISSING,
                                  files=[discord.File(p, filename=os.path.basename(p)) for p in filepaths])
            else:
                await target.send(content or discord.utils.MISSING)

    await send_fn(header, head[:10])

    CHUNK = 10
    for i in range(0, len(msg_files), CHUNK):
        chunk = msg_files[i:i+CHUNK]
        await send_fn(f"messages part {i//CHUNK + 1}", chunk)
        await asyncio.sleep(1.0)

# ========= イベント =========
@bot.event
async def on_ready():
    load_settings()
    # ギルド即時反映
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

    ensure_dir(BACKUP_DIR)
    if not weekly_backup_task.is_running():
        weekly_backup_task.start()

    log.info("Logged in as %s (ID: %s)", bot.user, bot.user.id)

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
        "author_name": f"{after.display_name if hasattr(after, 'display_name') else after.author.display_name}",
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
def has_allowed_role():
    async def predicate(interaction: discord.Interaction) -> bool:
        if not interaction.user or not isinstance(interaction.user, discord.Member):
            return False
        return any(r.id == ALLOWED_ROLE_ID for r in interaction.user.roles)
    return app_commands.check(predicate)

class GuildConfGroup(app_commands.Group):
    def __init__(self):
        super().__init__(name="vcchatlog", description="VCテキスト削除時ログの設定")

    @has_allowed_role()
    @app_commands.command(name="set_log_channel", description="ログ送信先チャンネルを設定")
    async def set_log_channel(self, interaction: discord.Interaction, channel: discord.TextChannel):
        conf = guild_conf(interaction.guild_id)
        conf["log_channel_id"] = channel.id
        save_settings()
        await interaction.response.send_message(f"✅ ログ送信先を {channel.mention} に設定しました。", ephemeral=True)

    @has_allowed_role()
    @app_commands.command(name="set_max", description="1チャンネルあたり保持する最大メッセージ数")
    async def set_max(self, interaction: discord.Interaction, count: app_commands.Range[int, 100, 200000] = 5000):
        conf = guild_conf(interaction.guild_id)
        conf["max_messages_per_channel"] = int(count)
        save_settings()
        await interaction.response.send_message(f"✅ 最大保持件数を {count} に設定しました。", ephemeral=True)

    # ---- カテゴリー制御 ----
    @has_allowed_role()
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

    @has_allowed_role()
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

    @has_allowed_role()
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

    @has_allowed_role()
    @app_commands.command(name="clear_categories", description="記録対象カテゴリーをすべて解除（全VC対象に戻す）")
    async def clear_categories(self, interaction: discord.Interaction):
        conf = guild_conf(interaction.guild_id)
        conf["category_whitelist"] = []
        save_settings()
        await interaction.response.send_message("🧹 クリアしました。以後は**全カテゴリー**が対象になります。", ephemeral=True)

    @has_allowed_role()
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

    @has_allowed_role()
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

# ---- バックアップ用コマンド ----
class BackupGroup(app_commands.Group):
    def __init__(self):
        super().__init__(name="backup", description="サーバーバックアップ（スナップショット出力と送信）")

    @has_allowed_role()
    @app_commands.command(name="now", description="今すぐスナップショットを作成して送信（チャンネルIDはカンマ区切り）")
    @app_commands.describe(channels="例: 123,456（省略時は環境変数 BACKUP_MESSAGE_CHANNEL_IDS を使用）")
    async def backup_now(self, interaction: discord.Interaction, channels: Optional[str] = None):
        await interaction.response.defer(ephemeral=True)
        guild = interaction.guild
        if guild is None:
            return await interaction.followup.send("ギルド内で実行してください。", ephemeral=True)

        ch_ids = parse_id_list(channels) or parse_id_list(DEFAULT_MESSAGE_CHANNEL_IDS)
        snap_dir = await create_snapshot(guild, ch_ids)

        # 送信先：REPORT_CHANNEL_ID があればそちら、なければ実行チャンネル
        target_channel = guild.get_channel(REPORT_CHANNEL_ID) if REPORT_CHANNEL_ID else None
        via_followup = interaction.followup  # 実行者へも送る
        await send_snapshot_summary(guild, target_channel or interaction.channel, snap_dir, via_followup=via_followup)
        await interaction.followup.send("✅ スナップショット送信を完了しました。", ephemeral=True)

    @has_allowed_role()
    @app_commands.command(name="status", description="バックアップ設定の確認")
    async def backup_status(self, interaction: discord.Interaction):
        chs = parse_id_list(DEFAULT_MESSAGE_CHANNEL_IDS)
        rep = f"<#{REPORT_CHANNEL_ID}>" if REPORT_CHANNEL_ID else "未設定（実行チャンネルに送信）"
        await interaction.response.send_message(
            "🧾 **バックアップ設定**\n"
            f"- BACKUP_DIR: `{BACKUP_DIR}`\n"
            f"- REPORT_CHANNEL_ID: {rep}\n"
            f"- BACKUP_MESSAGE_CHANNEL_IDS: {', '.join(map(str, chs)) if chs else '（未設定）'}\n"
            f"- 週次スケジュール: 毎週 月曜 04:00 JST（±10分枠）",
            ephemeral=True
        )

tree.add_command(BackupGroup(), guild=discord.Object(id=PRIMARY_GUILD_ID) if PRIMARY_GUILD_ID else None)

# ========= 週次スケジュール（JST・毎週月曜 04:00 ±10分） =========
@tasks.loop(minutes=10.0)
async def weekly_backup_task():
    now = datetime.now(JST)
    if now.weekday() == 0 and now.hour == 4 and now.minute < 10:
        for guild in bot.guilds:
            try:
                ch_ids = parse_id_list(DEFAULT_MESSAGE_CHANNEL_IDS)
                snap_dir = await create_snapshot(guild, ch_ids)
                # 送信先
                target_channel = guild.get_channel(REPORT_CHANNEL_ID) if REPORT_CHANNEL_ID else None
                if target_channel is None:
                    target_channel = guild.system_channel or (guild.text_channels[0] if guild.text_channels else None)
                if target_channel:
                    await send_snapshot_summary(guild, target_channel, snap_dir)
            except Exception as e:
                log.exception(f"weekly backup failed for {guild.id}: {e}")

@weekly_backup_task.before_loop
async def before_weekly():
    await bot.wait_until_ready()

# ========= 実行 =========
if __name__ == "__main__":
    if not DISCORD_TOKEN:
        raise RuntimeError("DISCORD_TOKEN が未設定です。")
    bot.run(DISCORD_TOKEN)
