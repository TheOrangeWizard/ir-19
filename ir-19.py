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
from minecraft.networking.connection import Connection
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
    if acct.lower() not in account_cache:
        with shelve.open("data/accounts.shelf") as accountshelf:
            if acct.lower() in accountshelf.keys():
                account_cache.append(acct.lower())
            else:
                accountshelf[acct.lower()] = {"discord id": None, "activity": []}


def set_discord_id(acct, did):
    with shelve.open("data/accounts.shelf") as accountshelf:
        # print([k for k in accountshelf.keys()])
        if acct.lower() not in accountshelf.keys():
            return "account not found"
        elif accountshelf[acct.lower()]["discord id"] == did:
            return "account already has discord id set"
        elif accountshelf[acct.lower()]["discord id"] is not None:
            return "account already has different discord id set"
        else:
            acctdata = accountshelf[acct.lower()]
            acctdata["discord id"] = did
            accountshelf[acct.lower()] = acctdata
            n = "id for " + acct.lower() + " set to " + did
    with shelve.open("data/associations.shelf") as associationshelf:
        if did in associationshelf.keys():
            acctdata = associationshelf[did]
            acctdata["accounts"].append(acct.lower())
            associationshelf[did] = acctdata
            return n + " and account added to list"
        else:
            associationshelf[did] = {"accounts": [acct.lower()]}
            return n + " and id entry created"


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
                acct1iddata = associationshelf[acct1id]
                acct1iddata["accounts"].append(acct2.lower())
                associationshelf[acct1id] = acct1iddata
                acct2data = accountshelf[acct2.lower()]
                acct2data["discord id"] = acct1id
                accountshelf[acct2.lower()] = acct2data
            return "associated " + acct2.lower() + " with " + acct1.lower()
        elif acct1id is None and acct2id is not None:
            with shelve.open("data/associations.shelf") as associationshelf:
                acct2iddata = associationshelf[acct2id]
                acct2iddata["accounts"].append(acct1.lower())
                associationshelf[acct2id] = acct2iddata
                acct1data = accountshelf[acct1.lower()]
                acct1data["discord id"] = acct2id
                accountshelf[acct1.lower()] = acct1data
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


def record_spam():
    with shelve.open("data/accounts.shelf") as accountshelf:
        for acct in accountshelf.keys():
            print(acct, accountshelf[acct])
    with shelve.open("data/associations.shelf") as associationshelf:
        for acct in associationshelf.keys():
            print(acct, associationshelf[acct])


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

    @tasks.loop(seconds=1)
    async def process_discord_queue(self):
        if ds_queue.qsize() > 0:
            package = ds_queue.get()
            if package["type"] == "CHAT":
                await self.bot.get_channel(config.spam_channel).send(clean(package["message"]))

    @tasks.loop(seconds=config.tablist_update_delay)
    async def update_tablists(self):
        # print("tablist update loop debug message")
        content = []
        n = 0
        if connection.spawned and connection.connected:
            for uuid in connection.player_list.players_by_uuid.keys():
                name = str(connection.player_list.players_by_uuid[uuid].name)
                content.append(name)
                try:
                    record_account(name)
                except Exception as e:
                    print(timestring(), type(e), e)
            n = str(len(content))
        content = clean("\n".join(sorted(content, key=str.casefold)))
        with open("tablists.txt", "r") as tablistfile:
            for tablist in tablistfile.readlines():
                channel, messageid = tablist.strip().split(" ")
                try:
                    message = await self.bot.get_channel(int(channel)).fetch_message(int(messageid))
                    if connection.spawned:
                        await message.edit(content="**" + n + " players:**\n"+content)
                    else:
                        await message.edit(content="connection error")
                except discord.errors.NotFound:
                    pass
                except Exception as e:
                    print(timestring(), type(e), e)

    @tasks.loop(seconds=30)
    async def check_online(self):
        try:
            if not connection.spawned and connection.connected:
                await self.bot.change_presence(activity=None)
                connection.disconnect()
                print(timestring(), "disconnected from", connection.options.address)
                print(timestring(), "reconnecting in", config.reconnect_timer, "seconds")
                await asyncio.sleep(config.reconnect_timer)
                connection.connect()
            else:
                await self.bot.change_presence(activity=discord.Game("mc.civclassic.com"))
        except Exception as e:
            print(timestring(), e)

    @tasks.loop(hours=2)
    async def update_roleconfig(self):
        try:
            await roleconfig_update_starter()
        except Exception as e:
            print(timestring(), e)

    # before loops

    @process_discord_queue.before_loop
    async def before_process_discord_queue(self):
        await self.bot.wait_until_ready()
        print(timestring(), "process discord queue loop has started")

    @update_tablists.before_loop
    async def before_update_tablists(self):
        await self.bot.wait_until_ready()
        print(timestring(), "tablist update loop has started")

    @check_online.before_loop
    async def before_check_online(self):
        await self.bot.wait_until_ready()
        print(timestring(), "check online loop has started")

    @update_roleconfig.before_loop
    async def before_update_roleconfig(self):
        await self.bot.wait_until_ready()
        print(timestring(), "roleconfig update loop has started")

    @process_discord_queue.after_loop
    async def process_discord_queue_finish(self):
        print(timestring(), "process discord queue loop has ended")
        self.process_discord_queue.start()

    @update_tablists.after_loop
    async def update_tablists_finish(self):
        print(timestring(), "tablist update loop has ended")
        self.update_tablists.start()

    @check_online.after_loop
    async def check_online_finish(self):
        print(timestring(), "check online loop has ended")
        self.check_online.start()

    @update_roleconfig.after_loop
    async def update_roleconfig_finish(self):
        print(timestring(), "roleconfig update loop has ended")
        self.update_roleconfig.start()


nl_ranks = ["none", "members", "mods", "admins", "owner"]
bot = commands.Bot(command_prefix=config.prefix, description=config.motd)
bot.add_cog(Loops(bot))


async def roleconfig_update_starter():
    queue = []
    with open("data/roleconfig.txt", "r") as rc:
        for line in rc.readlines():
            try:
                g = line.strip("\n").split(" ")[1]
                if not g.lower() in queue:
                    queue.append(g.lower())
            except:
                pass
    nllm["queue"] = queue
    await bot.get_channel(config.spam_channel).send("updating roleconfig for " + ", ".join(queue))
    send_chat("/nllm " + queue.pop())


async def roleconfig_update():
    batch = []
    for group in nllm["data"].keys():
        print(timestring(), "updating group permissions for", group)

        roleconfigs = {}
        with open("data/roleconfig.txt") as rc:
            for line in rc.readlines():
                try:
                    i = line.lower().strip("\n").split(" ")
                    try:
                        roleconfigs[int(i[0])][i[1]] = i[2]
                    except KeyError:
                        roleconfigs[int(i[0])] = {i[1]: i[2]}
                except:
                    pass
        # print(roleconfigs)

        groupconfigs = {}
        for member in bot.get_guild(config.guild).members:
            hrank = 0
            for role in member.roles:
                for a in get_accounts(str(member.id)):
                    account = a.lower()
                    if not len(account) > 16:
                        try:
                            if account in groupconfigs.keys():
                                if group in groupconfigs[account].keys():
                                    rank = nl_ranks.index(roleconfigs[role.id][group])
                                    if rank > hrank:
                                        groupconfigs[account][group] = roleconfigs[role.id][group]
                                    else:
                                        pass
                                else:
                                    groupconfigs[account][group] = roleconfigs[role.id][group]
                            else:
                                groupconfigs[account] = {group: roleconfigs[role.id][group]}
                        except:
                            pass
        # print(groupconfigs)

        for a in groupconfigs.keys():
            account = a.lower()
            if not len(account) > 16:
                try:
                    cfg = groupconfigs[account][group].lower()
                    try:
                        nlg = nllm["data"][group][account]
                        if not cfg == nlg:
                            batch.append("/nlpp " + group + " " + account + " " + groupconfigs[account][group])
                    except:
                        batch.append("/nlip " + group + " " + account + " " + groupconfigs[account][group])
                except KeyError:
                    pass

        for account in nllm["data"][group].keys():
            try:
                assert groupconfigs[account][group]
            except KeyError:
                batch.append("/nlrm " + group + " " + account)
    global chat_batch
    chat_batch += batch
    m = "the following commands have been queued for execution:"
    for i in batch:
        m += "\n" + i
    print(m)
    await bot.get_channel(config.spam_channel).send(clean(m))


@bot.event
async def on_ready():
    print(timestring(), "connected to discord as", bot.user.name)
    print(timestring(), "spam channel registered as", bot.get_channel(config.spam_channel).name)
    loops = bot.get_cog("Loops")
    if loops is not None:
        try:
            loops.process_discord_queue.start()
        except Exception as e:
            print(e)
        try:
            loops.update_tablists.start()
        except Exception as e:
            print(e)
        try:
            loops.check_online.start()
        except Exception as e:
            print(e)
    else:
        print(timestring(), "this shouldn't happen")


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
    await ctx.channel.send(str(connection.connected) + " " + str(connection.spawned))


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
@commands.has_permissions(administrator=True)
async def restart(ctx):
    """attempts to restart the minecraft connection"""
    connection.disconnect()
    await asyncio.sleep(15)
    connection.auth_token.authenticate(config.username, config.password)
    connection.connect()


@bot.command(pass_context=True)
@commands.has_permissions(administrator=True)
async def stop(ctx):
    """halts internal tasks for debug purposes"""
    loops = bot.get_cog("Loops")
    if loops is not None:
        try:
            loops.process_discord_queue.stop()
        except Exception as e:
            print(e)
        try:
            loops.update_tablists.stopt()
        except Exception as e:
            print(e)
        try:
            loops.check_online.stop()
        except Exception as e:
            print(e)
    else:
        print(timestring(), "this shouldn't happen")


@bot.command(pass_context=True)
async def maketablist(ctx):
    """posts a message and periodically updates it with a list of online players"""
    await ctx.channel.send("player list placeholder message")


@bot.command(pass_context=True)
@commands.has_permissions(manage_messages=True)
async def set_id(ctx, *args):
    """set_id <account name> <discord id> sets a discord id for a given minecraft account"""
    if len(args) == 2:
        did, acct = args[1], args[0]
        if did == "me":
            did = str(ctx.message.author.id)
        try:
            assert bot.get_user(int(did))
            n = set_discord_id(acct, did)
            await ctx.channel.send(n)
        except Exception as e:
            await ctx.channel.send(e)
    else:
        await ctx.channel.send("usage: `set_id <discord id> <account name>`")


@bot.command(pass_context=True)
@commands.has_permissions(manage_messages=True)
async def get_id(ctx, *, arg):
    """gets the discord id for a given minecraft account"""
    try:
        n = get_discord_id(arg)
        await ctx.channel.send(n)
    except Exception as e:
        await ctx.channel.send(e)


@bot.command(pass_context=True)
@commands.has_permissions(manage_messages=True)
async def associate(ctx, *args):
    """associates two given minecraft accounts"""
    if len(args) == 2:
        n = add_association(args[0], args[1])
        await ctx.channel.send(n)
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
    if ctx.invoked_subcommand is None:
        await ctx.channel.send("invalid subcommand")


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
                try:
                    message += bot.get_guild(config.guild).get_role(role).name + " " + group + " " + rank + "\n"
                except AttributeError:
                    message += "DELETED ROLE " + group + " " + rank + "\n"
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
    await roleconfig_update_starter()


@bot.group(pass_context=True)
async def debug(ctx):
    """debug stuff"""
    if ctx.invoked_subcommand is None:
        await ctx.channel.send("invalid subcommand")


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


connection = Connection(config.host, config.port, auth_token=auth_token, handle_exception=handle_error)


def send_chat(message):
    sm = message.split(" ")
    if len(sm) == 2 and sm[0] == "/nllm":
        print(timestring(), "waiting for nllm data for group", sm[1])
        nllm["group"] = sm[1].lower()
        nllm["data"][sm[1].lower()] = {}
        nllm["time"] = time.time()
    print(timestring(), "ingame:", message)
    packet = packets.serverbound.play.ChatPacket()
    packet.message = message
    connection.write_packet(packet)


def on_incoming(incoming_packet):
    if not nllm["group"] == "":
        if nllm["time"] < time.time() - config.nllm_timeout:
            if nllm["data"][nllm["group"]] == {}:
                print(timestring(), "nllm timed out for group", nllm["group"])
            else:
                print(timestring(), "nllm data received for group", nllm["group"])
            if len(nllm["queue"]) == 0:
                print(timestring(), "nllm data collected for groups",
                ", ".join([key if nllm["data"][key] != {} else "" for key in nllm["data"].keys()]))
                bot.loop.create_task(roleconfig_update())
                nllm["group"] = ""
            else:
                nllm["group"] = nllm["queue"].pop()
                send_chat("/nllm " + nllm["group"])
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
    words = chat.split(" ")
    if not source == "GAME_INFO":
        print(timestring(), source, chat)
        if chat[:2] == "§6":
            parse_snitch(chat)
        if config.relay_chat:
            ds_queue.put({"type": "CHAT", "channel": config.spam_channel, "message": chat})
        if not nllm["group"] == "":
            if len(words) == 2 and words[1] in ["(OWNER)", "(ADMINS)", "(MODS)", "(MEMBERS)"]:
                nllm["data"][nllm["group"]][words[0].lower()] = words[1].lower().strip("()")


def on_player_list_item(player_list_item_packet):
    try:
        player_list_item_packet.apply(connection.player_list)
    except Exception as e:
        print(e)


def on_mc_disconnect(disconnect_packet):
    print(timestring(), "logged out from", config.host)
    connection.__setattr__("player_list", packets.clientbound.play.PlayerListItemPacket.PlayerList())


def parse_snitch(chat):
    split_chat = [i.strip() for i in chat.split("  ")]
    action = split_chat[0][2:]
    account = split_chat[1][2:]
    snitch_name = split_chat[2][2:]
    distance = split_chat[4].split(" ")[0][2:][:-1]
    direction = split_chat[4].split(" ")[1][1:][:-2]
    coords = split_chat[3][3:][:-1].split(" ")
    print(account, action, "at", snitch_name, coords)


connection.register_packet_listener(on_incoming, packets.Packet, early=True)
connection.register_packet_listener(on_join_game, packets.clientbound.play.JoinGamePacket)
connection.register_packet_listener(on_chat, packets.clientbound.play.ChatMessagePacket)
connection.register_packet_listener(on_mc_disconnect, packets.clientbound.play.DisconnectPacket)
connection.register_packet_listener(on_player_list_item, packets.clientbound.play.PlayerListItemPacket)


if __name__ == "__main__":
    print(timestring(), "starting up")
    # record_spam()
    a = time.time()
    with shelve.open("data/accounts.shelf") as accountshelf:
        for acct in accountshelf.keys():
            account_cache.append(acct)
    print(timestring(), "account cache populated in", time.time()-a, "seconds")
    discordThread = Thread(target=bot.run, args=[config.token])
    discordThread.run()
