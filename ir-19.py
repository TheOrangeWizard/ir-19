from quarry.net.auth import Profile
from quarry.net.client import ClientFactory, SpawningClientProtocol
from twisted.internet import reactor, defer
from twisted.internet.protocol import ReconnectingClientFactory
import discord
from discord.ext import commands
import time
import datetime
import threading
import config
import queue

buffer = 100
mc_queue = queue.Queue(buffer)
ds_queue = queue.Queue(buffer)

def timestring():
    mtime = datetime.datetime.now()
    return "[{:%H:%M:%S}] ".format(mtime)

def datestring():
    mtime = datetime.datetime.now()
    return "[{:%d/%m/%y}] ".format(mtime)


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
            accts.write(line)
    return True


def get_associations(acct):
    with open("data/accounts.txt", "r") as accts:
        lines = accts.readlines()
    for line in lines:
        if acct.lower() in line:
            return line.strip("\n")


class IRClientProtocol(SpawningClientProtocol):
    def setup(self):
        self.players = {}
        print("irclientprotocol setup debug message")

    # server bound packets
    def send_chat(self, text):
        if len(text) > 225:
            print("message too long")
        else:
            self.send_packet("chat_message", self.buff_type.pack_string(text))

    # client bound packets
    def packet_chat_message(self, buff):
        p_text = buff.unpack_chat()
        p_position = buff.unpack("B")
        print(timestring() + str(p_text))
        l_text = str(p_text).split()

    def packet_player_list_item(self, buff):
        login_time = str(int(time.time()))
        p_action = buff.unpack_varint()
        p_count = buff.unpack_varint()
        for i in range(p_count):
            p_uuid = buff.unpack_uuid()
            if p_action == 0:  # ADD_PLAYER
                p_player_name = buff.unpack_string()
                p_properties_count = buff.unpack_varint()
                p_properties = {}
                for j in range(p_properties_count):
                    p_property_name = buff.unpack_string()
                    p_property_value = buff.unpack_string()
                    p_property_is_signed = buff.unpack('?')
                    if p_property_is_signed:
                        p_property_signature = buff.unpack_string()
                    p_properties[p_property_name] = p_property_value
                p_gamemode = buff.unpack_varint()
                p_ping = buff.unpack_varint()
                p_has_display_name = buff.unpack('?')
                if p_has_display_name:
                    p_display_name = buff.unpack_chat()
                else:
                    p_display_name = None
                if p_ping != -1:
                    self.players[p_uuid] = {"name": p_player_name,
                                            "properties": p_properties,
                                            "gamemode": p_gamemode,
                                            "ping": p_ping,
                                            "display_name": p_display_name,
                                            "login_time": login_time}
            elif p_action == 1:  # UPDATE_GAMEMODE
                p_gamemode = buff.unpack_varint()
                if p_uuid in self.players:
                    self.players[p_uuid]['gamemode'] = p_gamemode
            elif p_action == 2:  # UPDATE_LATENCY
                p_ping = buff.unpack_varint()
                if p_uuid in self.players:
                    self.players[p_uuid]['ping'] = p_ping
            elif p_action == 3:  # UPDATE_DISPLAY_NAME
                p_has_display_name = buff.unpack('?')
                if p_has_display_name:
                    p_display_name = buff.unpack_chat()
                else:
                    p_display_name = None
                if p_uuid in self.players:
                    self.players[p_uuid]['display_name'] = p_display_name
            elif p_action == 4:  # REMOVE_PLAYER
                if p_uuid in self.players and self.players[p_uuid]["ping"] != -1:
                    del self.players[p_uuid]

    def packet_disconnect(self, buff):
        p_text = buff.unpack_chat()
        print(timestring() + str(p_text))

    # callbacks
    def player_joined(self):
        print(timestring() + "joined the game as " + self.factory.profile.display_name + ".")
        self.ticker.add_loop(10, self.process_mc_queue)

    # methods
    def process_mc_queue(self):
        if not mc_queue.empty():
            package = mc_queue.get()
            if package["key"] == "test":
                self.send_chat("/g sa-ii test")


class IRClientFactory(ReconnectingClientFactory, ClientFactory):
    protocol = IRClientProtocol

    def startedConnecting(self, connector):
        self.maxDelay = 60
        print(timestring() + "connecting to " + connector.getDestination().host + "...")
        # print (self.__getstate__())

    def clientConnectionFailed(self, connector, reason):
        print("connection failed: " + str(reason))
        ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

    def clientConnectionLost(self, connector, reason):
        print(timestring() + "disconnected:" + str(reason).split(":")[-1][:-2])
        ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

@defer.inlineCallbacks
def mc_main():
    profile = yield Profile.from_credentials(config.username, config.password)
    factory = IRClientFactory(profile)
    try:
        factory = yield factory.connect(config.host, config.port)
    except Exception as e:
        print(e)

#################
# discord stuff #
#################
prefix = "$"
motd = "large chungi"

bot = commands.Bot(command_prefix=prefix, description=motd)


async def process_discord_queue():
    await bot.wait_until_ready()
    while not bot.is_logged_in or not bot.is_closed:
        # print ("discord loop alive")
        if not ds_queue.empty():
            package = ds_queue.get()


@bot.command(pass_context=True)
async def test(ctx):
    """test"""
    await ctx.channel.send("test")
    mc_queue.put({"key":"test"})


@bot.command(pass_context=True)
async def associate(ctx, *args):
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
    if len (args) > 0:
        for arg in args:
            try:
                assc = get_associations(arg).split(" ")
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
            rc.write(line)
    await ctx.channel.send("added config `" + " ".join(args) + "`")

#######
# run #
#######

if __name__ == "__main__":
    mc_main()
    mcThread = threading.Thread(target=reactor.run, kwargs={"installSignalHandlers":0})
    mcThread.start()
    bot.run(config.token)