import os
import asyncio
from typing import List

import discord
from discord.ext import commands
from discord import app_commands

TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_IDS = [int(x.strip()) for x in os.getenv("GUILD_IDS", "").split(",") if x.strip().isdigit()]

intents = discord.Intents.default()
intents.guilds = True
bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree

def parse_ids(s: str) -> List[int]:
    items = []
    for part in s.replace(",", " ").split():
        if part.isdigit():
            items.append(int(part))
    # 重複削除（先勝ち）
    seen = set()
    out = []
    for i in items:
        if i not in seen:
            out.append(i); seen.add(i)
    return out

async def reorder_category_by_ids(
    category: discord.CategoryChannel,
    ordered_ids: List[int],
    place_front: bool = True,
) -> str:
    # 現在のカテゴリ内チャンネル（position順）
    current = sorted(category.channels, key=lambda c: c.position)
    id2ch = {c.id: c for c in current}

    # 有効なIDだけに絞る（同一カテゴリに属さないIDは除外）
    valid = [cid for cid in ordered_ids if cid in id2ch]
    if not valid:
        return "指定されたIDの中に、このカテゴリに属するチャンネルが見つかりませんでした。"

    # 指定分 / 非指定分に分割（非指定分は相対順維持）
    specified = [id2ch[cid] for cid in valid]
    remaining = [c for c in current if c.id not in set(valid)]

    final_order = (specified + remaining) if place_front else (remaining + specified)

    # すでに理想順なら何もしない
    if [c.id for c in current] == [c.id for c in final_order]:
        return "すでに指定どおりの順序になっています。"

    base_pos = min(ch.position for ch in current)
    # 順にpositionを詰め直し（軽いクールダウン）
    for i, ch in enumerate(final_order):
        try:
            await ch.edit(position=base_pos + i, reason="Manual order by IDs")
            await asyncio.sleep(0.25)
        except discord.Forbidden:
            return f"権限不足で {ch.name} を並べ替えできませんでした（Botに Manage Channels が必要）。"
        except discord.HTTPException:
            await asyncio.sleep(1.0)
    return f"カテゴリ **{category.name}** の順序を更新しました。"

@tree.command(name="reorder_channels", description="チャンネルID列を渡してカテゴリ内の順序を好きに並べ替えます。")
@app_commands.describe(
    ids="チャンネルIDをカンマまたは空白区切りで（例: 111,222,333）",
    place="指定IDを前(front)か後(back)に寄せる",
    category_id="対象カテゴリID（省略時は最初のIDのチャンネルが属するカテゴリ）",
)
@app_commands.choices(place=[
    app_commands.Choice(name="front（先頭に寄せる）", value="front"),
    app_commands.Choice(name="back（末尾に寄せる）", value="back"),
])
async def reorder_channels(
    interaction: discord.Interaction,
    ids: str,
    place: app_commands.Choice[str],
    category_id: str = None,
):
    if not interaction.guild:
        return await interaction.response.send_message("サーバー内で実行してください。", ephemeral=True)

    # 実行者の権限チェック（安全のため）
    me = interaction.user
    if isinstance(me, discord.Member):
        if not me.guild_permissions.manage_channels:
            return await interaction.response.send_message("実行者に『チャンネルの管理』権限が必要です。", ephemeral=True)

    id_list = parse_ids(ids)
    if not id_list:
        return await interaction.response.send_message("有効なチャンネルIDを入力してください。", ephemeral=True)

    # カテゴリ決定
    category = None
    if category_id:
        try:
            cid = int(category_id)
        except ValueError:
            return await interaction.response.send_message("category_id は数値IDで指定してください。", ephemeral=True)
        ch = interaction.guild.get_channel(cid)
        if not isinstance(ch, discord.CategoryChannel):
            return await interaction.response.send_message("有効なカテゴリIDを指定してください。", ephemeral=True)
        category = ch
    else:
        # 最初のIDのチャンネルからカテゴリ推定
        first_ch = interaction.guild.get_channel(id_list[0])
        if not first_ch or not getattr(first_ch, "category", None):
            return await interaction.response.send_message("category_id を省略する場合、最初のチャンネルIDはカテゴリに属している必要があります。", ephemeral=True)
        category = first_ch.category

    # 全IDが同一カテゴリに属しているか軽く検証（属してないものは自動除外）
    bad_ids = []
    for cid in id_list:
        ch = interaction.guild.get_channel(cid)
        if not ch or getattr(ch, "category_id", None) != category.id:
            bad_ids.append(cid)
    if bad_ids:
        # 同一カテゴリ外は無視する旨を案内（続行）
        note = f"\n※同一カテゴリ外のIDは無視しました: {', '.join(map(str, bad_ids))}"
    else:
        note = ""

    await interaction.response.defer(ephemeral=True, thinking=True)
    msg = await reorder_category_by_ids(
        category,
        [cid for cid in id_list if cid not in bad_ids],
        place_front=(place.value == "front"),
    )
    await interaction.followup.send(msg + note, ephemeral=True)

@bot.event
async def on_ready():
    try:
        if GUILD_IDS:
            for gid in GUILD_IDS:
                await tree.sync(guild=discord.Object(id=gid))
            print(f"Synced to guilds: {GUILD_IDS}")
        else:
            await tree.sync()
            print("Synced globally")
    except Exception as e:
        print(f"Slash sync error: {e}")
    print(f"Logged in as {bot.user} (ID: {bot.user.id})")

if __name__ == "__main__":
    if not TOKEN:
        raise RuntimeError("Please set DISCORD_TOKEN env var.")
    bot.run(TOKEN)
