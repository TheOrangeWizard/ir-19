import config

import sys
import time
import json
import aiohttp
import asyncio
import aiofiles
import datetime
import discord
from discord.ext import commands
from PIL import Image, ImageEnhance

from minecraft import authentication
from minecraft.exceptions import YggdrasilError
from minecraft.networking.connection import Connection
from minecraft.networking import packets

########
# misc #
########


def timestring():
    mtime = datetime.datetime.now()
    return "[{:%H:%M:%S}]".format(mtime)


def datestring():
    mtime = datetime.datetime.now()
    return "[{:%d/%m/%y}]".format(mtime)


def add_association(acct1, acct2):
    with open("data/accounts.txt", "r") as accts:
        lines = accts.readlines()
    e = False
    for line in lines:
        if acct1.lower() in line:
            e = True
            if acct2.lower() in line:
                return False
            else:
                lines[lines.index(line)] = line.strip("\n") + " " + acct2.lower() + "\n"
    if not e:
        lines.append(acct1.lower() + " " + acct2.lower() + "\n")
    with open("data/accounts.txt", "w") as accts:
        for line in lines:
            accts.write(line.lower())
    return True


def get_associations(acct):
    with open("data/accounts.txt", "r") as accts:
        lines = accts.readlines()
    for line in lines:
        if str(acct).lower() in line:
            return line.strip("\n").split(" ")
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


newstart = True
prefix = "$"
motd = "large chungi"
nl_ranks = ["none", "members", "mods", "admins", "owner"]

bot = commands.Bot(command_prefix=prefix, description=motd)


async def download_file(url, outurl):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                f = await aiofiles.open(outurl, mode='wb')
                await f.write(await resp.read())
                await f.close()


async def find_a_posted_image(ctx):
    if len(ctx.message.attachments) == 0:
        check_above = await ctx.message.channel.history(limit=14).flatten()
        for o in check_above:
            if len(o.attachments) != 0:
                await download_file(o.attachments[0].url, 'output.png')
                return True
    else:
        await download_file(ctx.message.attachments[0].url, 'output.png')
        return True
    return


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
                for a in get_associations(member.id):
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
    # m = "the following commands will be queued for execution:"
    # for i in batch:
    #     m += "\n" + i
    # print(m)
    # await bot.get_channel(config.spam_channel).send(clean(m))


async def check_online():
    global bot, newstart
    print(timestring(), "discord ready debug message: check_online loop started")
    while bot.is_ready():
        try:
            if not connection.connected:
                await bot.change_presence(activity=None)
                print(timestring(), "disconnected, reconnecting in", config.reconnect_timer, "seconds")
                await asyncio.sleep(config.reconnect_timer)
                try:
                    connection.connect()
                except ConnectionRefusedError:
                    print(timestring(), "target machine refused connection")
            else:
                await bot.change_presence(activity=discord.Game("mc.civclassic.com"))
            await asyncio.sleep(10)
        except Exception as e:
            print(e)
    print(timestring(), "discord disconnect debug message: check_online loop ended")
    newstart = True


@bot.event
async def on_ready():
    global newstart
    print(timestring(), "connected to discord as", bot.user.name)
    print(timestring(), "spam channel registered as", bot.get_channel(config.spam_channel).name)
    if newstart:
        bot.loop.create_task(check_online())
        newstart = False


@bot.event
async def on_error(e):
    print(timestring(), e)


@bot.event
async def on_disconnect():
    print(timestring(), "disconnected from discord")


@bot.command(pass_context=True)
async def test(ctx):
    """test"""
    await ctx.channel.send(".")


@bot.command(pass_context=True)
async def enhance(ctx, *, arg=25):
    """enhance"""
    await find_a_posted_image(ctx)
    try:
        factor = int(arg)
    except:
        await ctx.channel.send("factor must be int")
        return
    output = Image.open("output.png")
    enhancer = ImageEnhance.Brightness(output)
    output = enhancer.enhance(1 + factor / 100)
    output.save("output.png")
    await ctx.channel.send(file=discord.File("output.png"))


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
@commands.has_permissions(manage_messages=True)
async def associate(ctx, *args):
    """associates the second given account with the first"""
    if len (args) > 1:
        arg1 = args[0]
        if arg1 == "me":
            for arg in args[1:]:
                me = bot.get_user(int(ctx.message.author.id)).name + "#" + bot.get_user(int(ctx.message.author.id)).discriminator
                if add_association(str(ctx.message.author.id), arg):
                    await ctx.channel.send(me + " associcated with " + arg)
                else:
                    await ctx.channel.send(me + " already associcated with " + arg)
        else:
            for arg in args[1:]:
                try:
                    n = bot.get_user(int(arg1)).name + "#" + bot.get_user(int(arg1)).discriminator
                except:
                    n = arg1
                if add_association(str(arg1), arg):
                    await ctx.channel.send(n + " associcated with " + arg)
                else:
                    await ctx.channel.send(n + " already associcated with " + arg)


@bot.command(pass_context=True)
async def associations(ctx, *args):
    """get the current account associations for a given minecraft username or discord id"""
    if len (args) > 0:
        for arg in args:
            try:
                assc = get_associations(arg)
                message = ""
                for a in assc:
                    try:
                        message += bot.get_user(int(a)).name + "#" + bot.get_user(int(a)).discriminator + ", "
                    except:
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
    queue = []
    with open ("data/roleconfig.txt", "r") as rc:
        for line in rc.readlines():
            try:
                g = line.strip("\n").split(" ")[1]
                if not g.lower() in queue:
                    queue.append(g.lower())
            except:
                pass
    nllm["queue"] = queue
    await ctx.channel.send("updating roleconfig for " + ", ".join(queue))
    send_chat("/nllm " + queue.pop())


#############
# minecraft #
#############


nllm = {"queue": [], "group": "", "time":0, "data": {}}
auth_token = authentication.AuthenticationToken()
chat_batch = []
chat_timer = 0


def handle_error(exc):
    print(exc)
    if not connection.connected:
        print(timestring(), "connection lost")
    else:
        print(timestring(), "connection not lost")


try:
    auth_token.authenticate(config.username, config.password)
except YggdrasilError as e:
    print(e)
    sys.exit()

print(timestring(), "authenticated...")
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
                print(timestring(), "nllm data collected for groups", ", ".join([key if nllm["data"][key] != {} else ""
                                                                                 for key in nllm["data"].keys()]))
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


def on_chat(chat_packet):
    source = chat_packet.field_string('position')
    raw_chat = json.loads(str(chat_packet.json_data))
    chat = parse(raw_chat)
    words = chat.split(" ")
    print(timestring(), source, chat)
    if config.relay_chat:
        bot.loop.create_task(bot.get_channel(config.spam_channel).send(chat))
    if not nllm["group"] == "":
        if len(words) == 2 and words[1] in ["(OWNER)", "(ADMINS)", "(MODS)", "(MEMBERS)"]:
            nllm["data"][nllm["group"]][words[0].lower()] = words[1].lower().strip("()")


def on_disconnect(disconnect_packet):
    print(timestring(), "logged out from", config.host)


connection.register_packet_listener(on_incoming, packets.Packet, early=True)
connection.register_packet_listener(on_join_game, packets.clientbound.play.JoinGamePacket)
connection.register_packet_listener(on_chat, packets.clientbound.play.ChatMessagePacket)
connection.register_packet_listener(on_disconnect, packets.clientbound.play.DisconnectPacket)

connection.connect()
bot.run(config.token)
