import os
import asyncio
from typing import List

import discord
from discord.ext import commands
from discord import app_commands

# ====== 環境設定 ======
TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_IDS = [int(x.strip()) for x in os.getenv("GUILD_IDS", "1398607685158440991").split(",") if x.strip().isdigit()]
ALLOWED_ROLE_ID = 1398724601256874014  # 許可ロール（不要なら存在しないIDでOK）

intents = discord.Intents.default()
intents.guilds = True
bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree

# ---------------- utils ----------------
def parse_ids(s: str) -> List[int]:
    items: List[int] = []
    for part in s.replace(",", " ").split():
        if part.isdigit():
            items.append(int(part))
    seen = set()
    out: List[int] = []
    for i in items:
        if i not in seen:
            out.append(i); seen.add(i)
    return out

async def safe_edit_position(ch: discord.abc.GuildChannel, pos: int, reason: str):
    await ch.edit(position=pos, reason=reason)
    await asyncio.sleep(0.25)

# ---------------- core ----------------
async def reorder_category_exact(category: discord.CategoryChannel, order_ids: List[int], strict: bool) -> str:
    current = sorted(category.channels, key=lambda c: c.position)
    id2ch = {c.id: c for c in current}

    unknown = [cid for cid in order_ids if cid not in id2ch]
    valid_ids = [cid for cid in order_ids if cid in id2ch]
    if not valid_ids:
        return "指定IDの中に、このカテゴリに属するチャンネルがありません。"

    if strict:
        missing_ids = [c.id for c in current if c.id not in set(order_ids)]
        if missing_ids:
            return ("strict=true のため失敗：このカテゴリの全チャンネルを ids に含めてください。"
                    f" 未指定: {', '.join(map(str, missing_ids[:20]))}"
                    + (f"…(+{len(missing_ids)-20})" if len(missing_ids) > 20 else ""))

    listed = [id2ch[cid] for cid in valid_ids]
    final_order = listed if strict else listed + [c for c in current if c.id not in set(valid_ids)]

    if [c.id for c in current] == [c.id for c in final_order]:
        note = f"\n※カテゴリ外のIDは無視: {', '.join(map(str, unknown))}" if unknown else ""
        return "すでに指定どおりの順序です。" + note

    base_pos = min(ch.position for ch in current)
    offset = 1000  # 退避のための十分大きいオフセット

    # 退避（逆順で衝突回避）
    for i, ch in enumerate(reversed(current), start=1):
        await safe_edit_position(ch, base_pos + offset + i, reason="reorder_exact: temp shift")

    # 確定
    for i, ch in enumerate(final_order):
        await safe_edit_position(ch, base_pos + i, reason="reorder_exact: final order")

    note = f"\n※カテゴリ外のIDは無視: {', '.join(map(str, unknown))}" if unknown else ""
    return f"カテゴリ **{category.name}** の順序を更新しました。" + note

# ---------------- commands (関数定義) ----------------
@app_commands.command(name="reorder_exact", description="渡したチャンネルIDの順番どおりにカテゴリ内の順序を並べ替えます。")
@app_commands.describe(
    ids="上からの最終順になるようチャンネルIDをカンマ/空白区切りで（例: 111,222,333）",
    category_id="対象カテゴリID（省略時は最初のIDのチャンネルから推定）",
    strict="trueにするとカテゴリ内の全チャンネルがidsに含まれている必要があります",
)
async def reorder_exact_cmd(interaction: discord.Interaction, ids: str, category_id: str = None, strict: bool = False):
    if not interaction.guild:
        return await interaction.response.send_message("サーバー内で実行してください。", ephemeral=True)

    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    if not member:
        return await interaction.response.send_message("メンバー情報が取得できませんでした。", ephemeral=True)
    if not (member.guild_permissions.manage_channels or any(r.id == ALLOWED_ROLE_ID for r in member.roles)):
        return await interaction.response.send_message("『チャンネルの管理』権限、または許可ロールが必要です。", ephemeral=True)

    id_list = parse_ids(ids)
    if not id_list:
        return await interaction.response.send_message("有効なチャンネルIDを入力してください。", ephemeral=True)

    if category_id:
        try:
            cid = int(category_id)
        except ValueError:
            return await interaction.response.send_message("category_id は数値IDで指定してください。", ephemeral=True)
        cat = interaction.guild.get_channel(cid)
        if not isinstance(cat, discord.CategoryChannel):
            return await interaction.response.send_message("有効なカテゴリIDを指定してください。", ephemeral=True)
        category = cat
    else:
        first_ch = interaction.guild.get_channel(id_list[0])
        if not first_ch or not getattr(first_ch, "category", None):
            return await interaction.response.send_message("最初のチャンネルIDがカテゴリに属していません。", ephemeral=True)
        category = first_ch.category

    await interaction.response.defer(ephemeral=True, thinking=True)
    msg = await reorder_category_exact(category, id_list, strict=strict)
    await interaction.followup.send(msg, ephemeral=True)

# 管理者用：強制再同期
@app_commands.command(name="slash_sync", description="（管理者）このBotのスラッシュコマンドを対象ギルドに再同期します。")
async def slash_sync_cmd(interaction: discord.Interaction):
    if not interaction.guild:
        return await interaction.response.send_message("サーバー内で実行してください。", ephemeral=True)
    # 管理者のみ
    m = interaction.user if isinstance(interaction.user, discord.Member) else None
    if not m or not m.guild_permissions.administrator:
        return await interaction.response.send_message("管理者のみ実行できます。", ephemeral=True)

    await interaction.response.defer(ephemeral=True, thinking=True)
    await setup_guild_commands()
    await interaction.followup.send("スラッシュコマンドを再同期しました。", ephemeral=True)

# ---------------- dynamic guild-only registration ----------------
async def setup_guild_commands():
    # グローバル側は一切登録しない（＝キャッシュ問題を回避）
    # まず対象ギルドのコマンドをクリアしてから、改めて追加＆同期
    for gid in GUILD_IDS:
        gobj = discord.Object(id=gid)
        tree.clear_commands(guild=gobj)
        # guild専用として追加
        tree.add_command(reorder_exact_cmd, guild=gobj)
        tree.add_command(slash_sync_cmd, guild=gobj)
        await tree.sync(guild=gobj)
        print(f"[slash] synced commands to guild {gid}")

# ---------------- on_ready ----------------
@bot.event
async def on_ready():
    try:
        await setup_guild_commands()
    except Exception as e:
        print(f"[slash] setup error: {e}")
    print(f"Logged in as {bot.user} (ID: {bot.user.id})")

# ---------------- main ----------------
if __name__ == "__main__":
    if not TOKEN:
        raise RuntimeError("Please set DISCORD_TOKEN env var.")
    bot.run(TOKEN)
