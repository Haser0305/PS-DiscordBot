import discord
import os
import time
import asyncio
import random
import re
from ws4py import messaging
import threading
import psycopg2

from discord_slash import SlashCommand

client = discord.Client()

slash = SlashCommand(client, sync_commands=False)

db_error_code_message = {
    23503: '請先註冊會員',
}

used_emoji = ['1️⃣', '2️⃣', '3️⃣']


# ------------- feature functions ------------------

def generate_games_message(games):
    embed_list = list()

    for game in games:
        embed = discord.Embed(
            color=0x2083d9,
            title=game[2],
            url=f'https://store.playstation.com/zh-hant-tw/product/{game[0]}'
        )
        embed.add_field(name="現在價格", value=game[4], inline=True)
        embed.add_field(name="歷史最低價", value=game[6], inline=True)
        embed.set_footer(text="資料日期：" + str(game[9]))

        embed_list.append(embed)

    # embed.add_reaction()  # read api reference
    return embed_list


def generate_wish_list(games):
    message = ''
    index = 0
    for game in games:
        index += 1
        message += f'{index}. {game[0]}\n'
    if not message:
        return 'no wish list'
    return message


def strQ2B(s):
    # 把字串全形轉半形
    rstring = ""
    for uchar in s:
        u_code = ord(uchar)
        if u_code == 12288:  # 全形空格直接轉換
            u_code = 32
        elif 65281 <= u_code <= 65374:  # 全形字元（除空格）根據關係轉化
            u_code -= 65248
        rstring += chr(u_code)
    return rstring


def connect_database():
    connect = psycopg2.connect(os.environ['databaseURI'])
    return connect


def add_wish_list(userID, productID):
    connect = psycopg2.connect(os.environ['databaseURI'])
    cursor = connect.cursor()

    cursor.callproc('add_wish_list', [int(f'1{userID}'), productID])
    result = cursor.fetchone()
    connect.commit()
    connect.close()
    return result[0]


def registry_member(user_name: str, user_id: int) -> bool:
    connect = connect_database()
    cursor = connect.cursor()
    cursor.callproc('register_member', [user_id, user_name])
    response = cursor.fetchone()[0]
    connect.commit()
    connect.close()
    return response


async def send_directly_message(user_id: int, games_info: list):
    # send_message = list()
    # loop = asyncio.get_event_loop()
    if not games_info:
        return
    send_user = await client.fetch_user(user_id)
    await send_user.send('願望清單特價')
    for game in games_info:
        embed = discord.Embed(
            color=0x2083d9,
            title=game[1],
            url=f'https://store.playstation.com/zh-hant-tw/product/{game[0]}'
        )
        embed.add_field(
            name='原價',
            value=game[2],
            inline=True
        )
        embed.add_field(
            name='特價',
            value=game[3]
        )
        await send_user.send(embed=embed)
        # await loop.run_in_executor(None, partial(send_user.send, embed=embed))

        # asyncio.run(send_user.send(embed=embed))

        # send_message.append(send_user.send(embed=embed))
    # await asyncio.gather(*send_message, return_exceptions=True)


@DeprecationWarning
def send_mail_to_user(mail: str, mail_content: dict):
    # todo send mail to user
    pass


async def notify_user():
    connect = connect_database()
    cursor = connect.cursor()

    cursor.callproc('get_notify_member_list', [1, 1])
    mail_list = cursor.fetchall()
    for user_info in mail_list:
        cursor.callproc('get_notify_games_list', [f'1{user_info[0]}'])
        # mail_content = {
        #     'userName': user_info[1],
        #     'games': cursor.fetchall()
        # }
        # send_mail_to_user(user_info[2], mail_content)
        await send_directly_message(user_info[0], cursor.fetchall())

    connect.commit()
    connect.close()


@DeprecationWarning
def notify_on_sale():
    connect = connect_database()
    cursor = connect.cursor()
    # estimate how much size about 100 to 500 members' information
    cursor.callproc('', [])  # get subscribe members list
    subscribe_members = cursor.fetchall()

    for user in subscribe_members:
        notify_user(user)


# ------------- feature functions end ---------------

# ------------- Slash Command --------------------

# @slash.slash(name="search", guild_ids=[272343817531817986])
@slash.slash(name="search")
async def _search_games(ctx, name):
    print('get search request')
    # await ctx.defer()
    connect = psycopg2.connect(os.environ['databaseURI'])
    cursor = connect.cursor()
    cursor.callproc('findGames', [strQ2B(name)])
    response = cursor.fetchall()
    connect.close()
    # for message in generate_games_message(response):
    #     send_message = await ctx.send(embed=message)
    #     await send_message.add_reaction('👍')
    send_messages = await ctx.send(embeds=generate_games_message(response))
    for i in range(len(send_messages.embeds)):
        await send_messages.add_reaction(used_emoji[i])


@slash.slash(name="wishlist")
async def _get_wishlist(ctx):
    await ctx.defer()
    connect = psycopg2.connect(os.environ['databaseURI'])
    cursor = connect.cursor()
    cursor.callproc('get_wishlist', [int(f'1{ctx.author.id}')])
    response = cursor.fetchall()

    await ctx.send(generate_wish_list(response))


@slash.slash(name="notify")
async def _notify(ctx, notify_type):
    await ctx.defer()
    connect = connect_database()
    cursor = connect.cursor()

    cursor.execute(f'call update_notification_type(1{ctx.author.id}, {notify_type});')
    connect.commit()
    connect.close()
    await ctx.send('OK')


@slash.slash(name="delete_wishlist_item")
async def _delete_wishlist_item(ctx, delete_number):
    await ctx.defer()
    connect = connect_database()
    cursor = connect.cursor()

    cursor.execute(f'call delete_wishlist(1{ctx.author.id}, {delete_number});')
    connect.commit()
    connect.close()

    await ctx.send('OK')


@slash.slash(name="test")
async def _test(ctx):
    await ctx.defer()
    await notify_user()
    await ctx.send('ok')


# ------------- Slash Command end ------------------


@client.event
async def on_ready():
    print('We have logged in as {0.user}'.format(client))


@client.event
async def on_message(message):
    print('get message: ', message)


@client.event
async def on_raw_reaction_add(reaction):
    # my user id 272338829682147329 ?
    if reaction.member.bot:
        return
    print(reaction)
    # if reaction.emoji.name == '👍':
    if reaction.emoji.name in used_emoji:
        index = used_emoji.index(reaction.emoji.name)
        message = await client.get_channel(reaction.channel_id).fetch_message(reaction.message_id)
        userID = reaction.user_id
        productID = re.search(r'[^\/]*$', message.embeds[index].url).group()
        result = add_wish_list(userID, productID)
        if not result:
            print('finished')
        else:
            if result == 23503:
                print('need register')
                if registry_member(reaction.member.display_name, userID):
                    result = add_wish_list(userID, productID)
                    if result:
                        # can try send the message to this channel directly.
                        # client.get_channel()

                        # or use member to find the user.
                        # await client.get_guild(reaction.guild_id).get_channel(reaction.channel_id).send(
                        #     f'@{reaction.user_id} need registry manually.')
                        await reaction.member.send('need registry manually')


client.run(os.environ['discord_token'])
