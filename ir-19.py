import config

import sys
import time
import json
import queue
import shelve
import asyncio
import datetime
import discord

from discord.ext import commands, tasks
from threading import Thread

from minecraft import authentication
from minecraft.exceptions import YggdrasilError
from minecraft.networking.connection import Connection, PlayingReactor
from minecraft.networking import packets

########
# misc #
########


buffer = 100
ds_queue = queue.Queue(buffer)
account_cache = []


def timestring():
    mtime = datetime.datetime.now()
    return "[{:%H:%M:%S}]".format(mtime)


def datestring():
    mtime = datetime.datetime.now()
    return "[{:%d/%m/%y}]".format(mtime)


def record_account(acct):
    if acct in account_cache:
        return
    with shelve.open("data/accounts.shelf") as accountshelf:
        if acct in accountshelf.keys():
            account_cache.append(acct)
        else:
            accountshelf[acct] = {"discord id":None, "activity":[]}


def set_discord_id(acct, did, force=False):
    with shelve.open("data/accounts.shelf") as accountshelf:
        if acct.lower() not in accountshelf.keys():
            return "account not found"
        elif accountshelf[acct.lower()]["discord id"] == did:
            return "account already has discord id set"
        elif accountshelf[acct.lower()]["discord id"] is not None:
            return "account already has different discord id set"
        else:
            accountshelf[acct.lower()]["discord id"] = did
    with shelve.open("data/associations.shelf") as associationshelf:
        if did in associationshelf.keys():
            accts = associationshelf[did]["accounts"]
            accts.append(acct.lower())
            associationshelf[did]["accounts"] = accts


def add_association(acct1, acct2):
    with shelve.open("data/accounts.shelf") as accountshelf:
        accountkeys = accountshelf.keys()
        acct1id, acct2id = None, None
        if acct1.lower() in accountkeys:
            acct1id = accountshelf[acct1.lower()]["discord id"]
        if acct2.lower() in accountkeys:
            acct2id = accountshelf[acct2.lower()]["discord id"]
        if acct1id is not None and acct2id is None:
            with shelve.open("data/associations.shelf") as associationshelf:
                acct1idalts = associationshelf[acct1id]["accounts"]
                acct1idalts.append(acct2.lower())
                associationshelf[acct1id]["accounts"] = acct1idalts
                accountshelf[acct2.lower()]["discord id"] = acct1id
            return "associated " + acct2.lower() + " with " + acct1.lower()
        elif acct1id is None and acct2id is not None:
            with shelve.open("data/associations.shelf") as associationshelf:
                acct2idalts = associationshelf[acct2id]["accounts"]
                acct2idalts.append(acct1.lower())
                associationshelf[acct2id]["accounts"] = acct2idalts
                accountshelf[acct1.lower()]["discord id"] = acct2id
            return "associated " + acct1.lower() + " with " + acct2.lower()
        elif acct1id is not None and acct2id is not None:
            return "cannot associate: both accounts have existing discord id"
        else:
            return "cannot associate: neither account has existing discord id"


def get_associations(acct):
    with shelve.open("data/accounts.shelf") as accountshelf:
        accounts = accountshelf.keys()
        if acct.lower() in accounts:
            acctid = accountshelf[acct.lower()]["discord id"]
    if acctid is not None:
        with shelve.open("data/associations.shelf") as associationshelf:
            return associationshelf[acctid]["accounts"]
    else:
        return []


def get_discord_id(acct):
    with shelve.open("data/accounts.shelf") as accountshelf:
        accounts = accountshelf.keys()
        if acct.lower() in accounts:
            return accountshelf[acct.lower()]["discord id"]


def get_accounts(did):
    with shelve.open("data/associations.shelf") as associationshelf:
        if did in associationshelf.keys():
            return associationshelf[did]["accounts"]
        else:
            return []


def parse(obj):
    if isinstance(obj, str):
        return obj
    if isinstance(obj, list):
        return "".join((parse(e) for e in obj))
    if isinstance(obj, dict):
        text = ""
        if "text" in obj:
            text += obj["text"]
        if "announcement" in obj:
            text += obj["announcement"]
        if "extra" in obj:
            text += parse(obj["extra"])
        return text


def clean(text):
    text = text.replace("_", "\_")
    text = text.replace("*", "\*")
    text = text.replace("~~", "\~~")
    return text

#################
# discord stuff #
#################


class Loops(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.process_discord_queue.start()
        self.update_tablists.start()
        self.check_online.start()

    @tasks.loop(seconds=1)
    async def process_discord_queue(self):
        if ds_queue.qsize() > 0:
            package = ds_queue.get()
            if package["type"] == "CHAT":
                await self.bot.get_channel(config.spam_channel).send(clean(package["message"]))

    @tasks.loop(seconds=15)
    async def update_tablists(self):
        # print("tablist update loop debug message")
        content = []
        for uuid in connection.player_list.players_by_uuid.keys():
            name = str(connection.player_list.players_by_uuid[uuid].name)
            content.append(name)
            try:
                record_account(name)
            except Exception as e:
                print(e)
        content = clean("\n".join(sorted(content, key=str.casefold)))
        with open("tablists.txt", "r") as tablistfile:
            for tablist in tablistfile.readlines():
                channel, messageid = tablist.strip().split(" ")
                try:
                    message = await self.bot.get_channel(int(channel)).fetch_message(int(messageid))
                    if connection.connected:
                        await message.edit(content="**online players**\n\n"+content)
                    else:
                        await message.edit(content="connection error")
                except discord.errors.NotFound:
                    pass
                except Exception as e:
                    print(timestring(), type(e), e)

    @tasks.loop(seconds=30)
    async def check_online(self):
        try:
            if not connection.connected:
                await self.bot.change_presence(activity=None)
                print(timestring(), "minecraft disconnected, reconnecting in", config.reconnect_timer, "seconds")
                connection.auth_token.authenticate(config.username, config.password)
                await asyncio.sleep(config.reconnect_timer)
                try:
                    print(timestring(), "connecting...")
                    connection.connect()
                except ConnectionRefusedError:
                    print(timestring(), "host refused connection")
            else:
                await self.bot.change_presence(activity=discord.Game("mc.civclassic.com"))
        except Exception as e:
            print(timestring(), e)

    @process_discord_queue.before_loop
    async def wait(self):
        await self.bot.wait_until_ready()

    @update_tablists.before_loop
    async def wait(self):
        await self.bot.wait_until_ready()

    @check_online.before_loop
    async def wait(self):
        await self.bot.wait_until_ready()


nl_ranks = ["none", "members", "mods", "admins", "owner"]
bot = commands.Bot(command_prefix=config.prefix, description=config.motd)
bot.add_cog(Loops(bot))


async def roleconfig_update():
    return
    # batch = []
    # for group in nllm["data"].keys():
    #     print(timestring(), "updating group permissions for", group)
    #
    #     roleconfigs = {}
    #     with open("data/roleconfig.txt") as rc:
    #         for line in rc.readlines():
    #             try:
    #                 i = line.lower().strip("\n").split(" ")
    #                 try:
    #                     roleconfigs[int(i[0])][i[1]] = i[2]
    #                 except KeyError:
    #                     roleconfigs[int(i[0])] = {i[1]: i[2]}
    #             except:
    #                 pass
    #     # print(roleconfigs)
    #
    #     groupconfigs = {}
    #     for member in bot.get_guild(config.guild).members:
    #         hrank = 0
    #         for role in member.roles:
    #             for a in get_associations(member.id):
    #                 account = a.lower()
    #                 if not len(account) > 16:
    #                     try:
    #                         if account in groupconfigs.keys():
    #                             if group in groupconfigs[account].keys():
    #                                 rank = nl_ranks.index(roleconfigs[role.id][group])
    #                                 if rank > hrank:
    #                                     groupconfigs[account][group] = roleconfigs[role.id][group]
    #                                 else:
    #                                     pass
    #                             else:
    #                                 groupconfigs[account][group] = roleconfigs[role.id][group]
    #                         else:
    #                             groupconfigs[account] = {group: roleconfigs[role.id][group]}
    #                     except:
    #                         pass
    #     # print(groupconfigs)
    #
    #     for a in groupconfigs.keys():
    #         account = a.lower()
    #         if not len(account) > 16:
    #             try:
    #                 cfg = groupconfigs[account][group].lower()
    #                 try:
    #                     nlg = nllm["data"][group][account]
    #                     if not cfg == nlg:
    #                         batch.append("/nlpp " + group + " " + account + " " + groupconfigs[account][group])
    #                 except:
    #                     batch.append("/nlip " + group + " " + account + " " + groupconfigs[account][group])
    #             except KeyError:
    #                 pass
    #
    #     for account in nllm["data"][group].keys():
    #         try:
    #             assert groupconfigs[account][group]
    #         except KeyError:
    #             batch.append("/nlrm " + group + " " + account)
    # global chat_batch
    # chat_batch += batch
    # m = "the following commands will be queued for execution:"
    # for i in batch:
    #     m += "\n" + i
    # print(m)
    # await bot.get_channel(config.spam_channel).send(clean(m))


@bot.event
async def on_ready():
    print(timestring(), "connected to discord as", bot.user.name)
    print(timestring(), "spam channel registered as", bot.get_channel(config.spam_channel).name)


# @bot.event
# async def on_error(e):
#     print(timestring(), e)


@bot.event
async def on_disconnect():
    print(timestring(), "disconnected from discord")


@bot.event
async def on_message(message):
    if message.content == "player list placeholder message":
        with open("tablists.txt", "a") as tablistfile:
            tablistfile.write(str(message.channel.id) + " " + str(message.id) + "\n")
    else:
        await bot.process_commands(message)


@bot.command(pass_context=True)
async def test(ctx):
    """test"""
    await ctx.channel.send(".")


@bot.command(pass_context=True)
@commands.has_permissions(administrator=True)
async def send(ctx, *, arg):
    """send ingame chat"""
    await ctx.channel.send("saying `" + clean(arg) + "` ingame")
    send_chat(arg)


@bot.command(pass_context=True)
@commands.has_permissions(administrator=True)
async def shutdown(ctx):
    """shuts the bot down"""
    await ctx.channel.send("emergency shutdown invoked")
    connection.disconnect(immediate=True)
    await bot.close()


@bot.command(pass_context=True)
async def maketablist(ctx):
    """posts a message and periodically updates it with a list of online players"""
    await ctx.channel.send("player list placeholder message")


@bot.command(pass_context=True)
@commands.has_permissions(manage_messages=True)
async def set_id(ctx, *args):
    """`set_id <discord id> <account name>` sets a discord id for a given minecraft account"""
    if len(args) == 2:
        did, acct = args[0], args[1]
        if did == "me":
            did = str(ctx.message.author.id)
        else:
            try:
                assert bot.get_user(int(did))
                n = add_association(did, acct)
                await ctx.channel.send(n)
            except:
                await ctx.channel.send("invalid discord id")
    else:
        await ctx.channel.send("usage: `set_id <discord id> <account name>`")



@bot.command(pass_context=True)
@commands.has_permissions(manage_messages=True)
async def associate(ctx, *args):
    """associates two given minecraft accounts"""
    if len(args) == 2:
        n = associate(args[0], args[1])
    else:
        await ctx.channel.send("usage: `associate <account 1> <account 2>`")


@bot.command(pass_context=True)
async def associations(ctx, *args):
    """get the current account associations for a given minecraft account"""
    if len(args) > 0:
        for arg in args:
            try:
                assc = get_associations(arg)
                message = ""
                for a in assc:
                    message += a + ", "
                await ctx.channel.send(message[:-2])
            except:
                await ctx.channel.send("no associations found")
    else:
        await ctx.channel.send("usage: `associations <account name>`")


@bot.command(pass_context=True)
async def accounts(ctx, *, arg):
    """get the current account associations for a given discord id"""
    try:
        assc = get_accounts(arg)
        message = ""
        for a in assc:
            message += a + ", "
        await ctx.channel.send(message[:-2])
    except:
        await ctx.channel.send("no associations found")


@bot.group(pass_context=True)
async def roleconfig(ctx):
    """manage role-group configurations"""
    await ctx.channel.send("roleconfig disabled, please contact your local information request officer if you believe this is in error")
    return
    # if ctx.invoked_subcommand is None:
    #     await ctx.channel.send("invalid subcommand")


@roleconfig.command(pass_context=True)
async def get(ctx):
    """return current role-group configuration"""
    with open("data/roleconfig.txt", "r") as rc:
        message = ""
        for line in rc.readlines():
            if line == "\n":
                pass
            else:
                i = line.strip("\n").split(" ")
                role = int(i[0])
                group = i[1]
                rank = i[2]
                message += bot.get_guild(config.guild).get_role(role).name + " " + group + " " + rank + "\n"
        await ctx.channel.send(message)


@roleconfig.command(pass_context=True)
@commands.has_permissions(manage_messages=True)
async def add(ctx, *args):
    """add a line to role-group configuration"""
    with open("data/roleconfig.txt", "r") as rc:
        rcs = rc.readlines()
    e = False
    for line in rcs:
        if line == "\n":
            pass
        elif line.strip("\n").split(" ")[0].lower() == args[0].lower() and line.strip("\n").split(" ")[1] == args[1]:
            rcs[rcs.index(line)] = " ".join(args)
            e = True
    if not e:
        rcs.append(" ".join(args) + "\n")
    with open("data/roleconfig.txt", "w") as rc:
        for line in rcs:
            rc.write(line.lower())
    await ctx.channel.send("added config `" + " ".join(args) + "`")


@roleconfig.command(pass_context=True)
@commands.has_permissions(manage_messages=True)
async def update(ctx, *args):
    """updates"""
    await ctx.channel.send("roleconfig disabled, please contact your local information request officer if you believe this is in error")
    # queue = []
    # with open ("data/roleconfig.txt", "r") as rc:
    #     for line in rc.readlines():
    #         try:
    #             g = line.strip("\n").split(" ")[1]
    #             if not g.lower() in queue:
    #                 queue.append(g.lower())
    #         except:
    #             pass
    # nllm["queue"] = queue
    # await ctx.channel.send("updating roleconfig for " + ", ".join(queue))
    # send_chat("/nllm " + queue.pop())


#############
# minecraft #
#############


auth_token = authentication.AuthenticationToken()
nllm = {"queue": [], "group": "", "time":0, "data": {}}
chat_batch = []
chat_timer = 0


def authenticate():
    try:
        auth_token.authenticate(config.username, config.password)
    except YggdrasilError as e:
        print(e)
        sys.exit()
    print(timestring(), "yggdrassil authenticated...")


def handle_error(exc):
    print(exc)
    if not connection.connected:
        print(timestring(), "connection lost")
    else:
        print(timestring(), "connection not lost")


authenticate()
connection = Connection(config.host, config.port, auth_token=auth_token, handle_exception=handle_error)


def send_chat(message):
    # sm = message.split(" ")
    # if len(sm) == 2 and sm[0] == "/nllm":
    #     print(timestring(), "waiting for nllm data for group", sm[1])
    #     nllm["group"] = sm[1].lower()
    #     nllm["data"][sm[1].lower()] = {}
    #     nllm["time"] = time.time()
    print(timestring(), "ingame:", message)
    packet = packets.serverbound.play.ChatPacket()
    packet.message = message
    connection.write_packet(packet)


def on_incoming(incoming_packet):
    # if not nllm["group"] == "":
    #     if nllm["time"] < time.time() - config.nllm_timeout:
    #         if nllm["data"][nllm["group"]] == {}:
    #             print(timestring(), "nllm timed out for group", nllm["group"])
    #         else:
    #             print(timestring(), "nllm data received for group", nllm["group"])
    #         if len(nllm["queue"]) == 0:
    #             print(timestring(), "nllm data collected for groups", ", ".join([key if nllm["data"][key] != {} else ""
    #                                                                              for key in nllm["data"].keys()]))
    #             bot.loop.create_task(roleconfig_update())
    #             nllm["group"] = ""
    #         else:
    #             nllm["group"] = nllm["queue"].pop()
    #             send_chat("/nllm " + nllm["group"])
    if len(chat_batch) > 1:
        global chat_timer
        if chat_timer < time.time() - config.batch_chat_delay:
            send_chat(chat_batch.pop())
            chat_timer = time.time()


def respawn():
    packet = packets.serverbound.play.ClientStatusPacket()
    packet.action_id = packets.serverbound.play.ClientStatusPacket.RESPAWN
    connection.write_packet(packet)


def on_join_game(join_game_packet):
    print(timestring(), "connected to", config.host, "as", auth_token.profile.name)
    connection.__setattr__("player_list", packets.clientbound.play.PlayerListItemPacket.PlayerList())


def on_chat(chat_packet):
    source = chat_packet.field_string('position')
    raw_chat = json.loads(str(chat_packet.json_data))
    chat = parse(raw_chat)
    if chat[:2] == "§6":
        parse_snitch(chat)
    print(timestring(), source, chat)
    if config.relay_chat:
        ds_queue.put({"type": "CHAT", "channel": config.spam_channel, "message": chat})
    # if not nllm["group"] == "":
    #     if len(words) == 2 and words[1] in ["(OWNER)", "(ADMINS)", "(MODS)", "(MEMBERS)"]:
    #         nllm["data"][nllm["group"]][words[0].lower()] = words[1].lower().strip("()")


def on_player_list_item(player_list_item_packet):
    try:
        player_list_item_packet.apply(connection.player_list)
    except Exception as e:
        print(e)


def on_mc_disconnect(disconnect_packet):
    print(timestring(), "logged out from", config.host)
    connection.__setattr__("player_list", packets.clientbound.play.PlayerListItemPacket.PlayerList())


def parse_snitch(chat):
    split_chat = [i.strip() for i in chat.split("")]
    action = split_chat[1][1:]
    account = split_chat[2][1:]
    snitch_name = split_chat[3][1:]
    distance = split_chat[5][2:][:-1]
    coords = split_chat[4][2:][:-1].split()


connection.register_packet_listener(on_incoming, packets.Packet, early=True)
connection.register_packet_listener(on_join_game, packets.clientbound.play.JoinGamePacket)
connection.register_packet_listener(on_chat, packets.clientbound.play.ChatMessagePacket)
connection.register_packet_listener(on_mc_disconnect, packets.clientbound.play.DisconnectPacket)
connection.register_packet_listener(on_player_list_item, packets.clientbound.play.PlayerListItemPacket)


if __name__ == "__main__":
    print(timestring(), "starting up")
    a = time.time()
    with shelve.open("data/accounts.shelf") as accountshelf:
        for acct in accountshelf.keys():
            account_cache.append(acct)
    print(timestring(), "account cache populated in", time.time()-a, "seconds")
    connection.connect()
    discordThread = Thread(target=bot.run, args=[config.token])
    discordThread.run()
