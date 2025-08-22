# reorder.py
import os
import asyncio
from typing import List

import discord
from discord.ext import commands
from discord import app_commands

# ====== 環境設定 ======
TOKEN = os.getenv("DISCORD_TOKEN")
# 即時ギルド同期（未設定ならグローバル同期）
GUILD_IDS = [int(x.strip()) for x in os.getenv("GUILD_IDS", "1398607685158440991").split(",") if x.strip().isdigit()]

# 許可ロール（このロール所持者は権限なしでも実行可）※不要なら存在しないIDでOK
ALLOWED_ROLE_ID = 1398724601256874014

intents = discord.Intents.default()
intents.guilds = True
bot = commands.Bot(command_prefix="!", intents=intents)
tree = bot.tree


# ====== Utilities ======
def parse_ids(s: str) -> List[int]:
    """カンマ/空白区切りのID列をパースし、重複除去（先勝ち）"""
    items: List[int] = []
    for part in s.replace(",", " ").split():
        if part.isdigit():
            items.append(int(part))
    seen = set()
    out: List[int] = []
    for i in items:
        if i not in seen:
            out.append(i)
            seen.add(i)
    return out


async def safe_edit_position(ch: discord.abc.GuildChannel, pos: int, reason: str):
    """位置更新の安定化：小休止＋例外を握りつつ継続"""
    await ch.edit(position=pos, reason=reason)
    await asyncio.sleep(0.3)  # 軽めにクールダウン（環境により0.2〜0.5）


async def reorder_category_by_ids(
    category: discord.CategoryChannel,
    ordered_ids: List[int],
    place_front: bool = True,
) -> str:
    """カテゴリ内で、指定ID群を先頭/末尾に寄せて並び替え（残りは相対順維持）。
       ※ 個別 position 更新を二段階で実行して確実に反映。
    """
    # 現在のカテゴリ内チャンネル（position順）※Forum/Mediaも含む
    current = sorted(category.channels, key=lambda c: c.position)
    id2ch = {c.id: c for c in current}

    # 同一カテゴリに存在するIDのみ有効
    valid = [cid for cid in ordered_ids if cid in id2ch]
    if not valid:
        return "指定IDのうち、このカテゴリにあるチャンネルが見つかりません。"

    # 並び順（Forum/Media含め“そのまま”動かす設計ならここで分岐可能）
    specified = [id2ch[cid] for cid in valid]
    remaining = [c for c in current if c.id not in set(valid)]
    final_order = (specified + remaining) if place_front else (remaining + specified)

    # 既に理想順なら何もしない
    if [c.id for c in current] == [c.id for c in final_order]:
        return "すでに指定どおりの順序です。"

    # ---- 重要：二段階移動で確実に反映させる ----
    # 1) 退避フェーズ：全チャンネルを“大きい位置”に一旦ずらす
    #    （category 内の最小positionを基準に +1000 オフセット）
    base_pos = min(ch.position for ch in current)
    offset = 1000
    try:
        # 退避は“逆順”に動かすと衝突が起きにくい
        for i, ch in enumerate(reversed(current), start=1):
            await safe_edit_position(ch, base_pos + offset + i, reason="reorder: temp shift")
    except discord.Forbidden:
        return "権限不足（Botに『チャンネルの管理 / Manage Channels』が必要です）。"
    except discord.HTTPException as e:
        # 続行すると崩れるのでここで止める
        return f"退避中に更新エラーが発生しました: {e}"

    # 2) 確定フェーズ：目的の順で base_pos から詰め直す
    try:
        for i, ch in enumerate(final_order):
            await safe_edit_position(ch, base_pos + i, reason="reorder: final order")
    except discord.Forbidden:
        return "権限不足（Botに『チャンネルの管理 / Manage Channels』が必要です）。"
    except discord.HTTPException as e:
        return f"最終反映中に更新エラーが発生しました: {e}"

    return f"カテゴリ **{category.name}** の順序を更新しました。"


# ====== /reorder_channels ======
@tree.command(name="reorder_channels", description="チャンネルID列を渡してカテゴリ内の順序を並べ替えます。")
@app_commands.describe(
    ids="チャンネルIDをカンマ/空白区切りで（例: 111,222,333）",
    place="指定IDを前(front)か後(back)に寄せる",
    category_id="対象カテゴリID（省略時は最初のIDのチャンネルから推定）",
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

    # 実行者の権限/ロールチェック
    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    if not member:
        return await interaction.response.send_message("メンバー情報が取得できませんでした。", ephemeral=True)

    has_perm = member.guild_permissions.manage_channels
    has_role = any(r.id == ALLOWED_ROLE_ID for r in member.roles)
    if not (has_perm or has_role):
        return await interaction.response.send_message(
            "実行者に『チャンネルの管理』権限、または許可ロールが必要です。",
            ephemeral=True,
        )

    id_list = parse_ids(ids)
    if not id_list:
        return await interaction.response.send_message("有効なチャンネルIDを入力してください。", ephemeral=True)

    # カテゴリの決定
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
            return await interaction.response.send_message(
                "category_id を省略する場合、最初のチャンネルIDはカテゴリに属している必要があります。",
                ephemeral=True,
            )
        category = first_ch.category

    # 同一カテゴリ外のIDは除外して続行（注記表示）
    bad_ids = []
    for cid in id_list:
        ch = interaction.guild.get_channel(cid)
        if not ch or getattr(ch, "category_id", None) != category.id:
            bad_ids.append(cid)
    note = f"\n※同一カテゴリ外のIDは無視しました: {', '.join(map(str, bad_ids))}" if bad_ids else ""

    await interaction.response.defer(ephemeral=True, thinking=True)
    msg = await reorder_category_by_ids(
        category,
        [cid for cid in id_list if cid not in bad_ids],
        place_front=(place.value == "front"),
    )
    await interaction.followup.send(msg + note, ephemeral=True)


# ====== 起動時：スラッシュ同期 ======
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
