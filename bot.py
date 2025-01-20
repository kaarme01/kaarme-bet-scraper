import logging
from io import BytesIO
from multiprocessing.connection import Listener
from multiprocessing.managers import BaseManager

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from prettytable import PrettyTable
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

import controller
import database_connector

scanning : bool = False

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = """
    This is käärme"s betting bot. This should be considered a pre-alpha software, and there may be bugs.\n
    Report any bugs to @kaarme01. Access is limited to whitelisted users.
    \n\n
    /sql - to run an SQL query, enter in quotes or enter a query name(readonly by default)
    /save - save previous sql query. 
    /userstatus - display your privilege level
    /preferences - manage bookmaker and market preferences
    /alerts - manage alerts for arbitrage opportunities
    /topodds - display todays top odds, add "-f" -"m" -"b" for unfiltered fields, \n
    markets and bookmakers respectively, or "-fmb" for short
    /getmarket - search an event by participants, league or sport
    /listbooks - list of all supported bookmaker keys
    /scan - force a rescan of a given bookmaker
    """
    await update.message.reply_text(text)
    database_connector.saveTelegramChatId(update.effective_user.username, update.effective_chat.id)

async def scan(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global scanning
    args =  update.message.text.split()[1:]
    arg = args[0] if len(args) > 0 else None
    thisUser = update.effective_user.username
    if arg is None:
        await update.message.reply_text("Bookmaker missing from command")
        return
    if arg not in controller.wrapperDict:
        await update.message.reply_text("Bookmaker not supported")
        return
    if scanning is True:
        await update.message.reply_text("Already scanning, try again later")
    wrapper = controller.wrapperDict[arg]()
    try:
        scanning = True
        await notifyAdmins("Scanning " + arg + " initiated by " + thisUser, context, thisUser)
        await update.message.reply_text("Beginning scan")
        await wrapper.run()
    except Exception as e: 
        logging.exception(e)
        await update.message.reply_text("Scan failure, some markets may be missing")
        await notifyAdmins(e + " \n Initiated by " + update.effective_user.username, context)
    else:
        await update.message.reply_text("Scan complete")
    scanning = False

async def getTopOdds(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    arg =  update.message.text.split()[1:]
    try: 
        topOdds = database_connector.getTopOddsInfo()
        buf = await oddsToImage(topOdds)
        #await update.message.reply_text(text, parse_mode="HTML")
        await context.bot.send_photo(chat_id=update.effective_chat.id, photo=buf)
    except Exception as e:
        logging.exception(e)
        await notifyAdmins(e + " \n Initiated by " + update.effective_user.username, context)
        await update.message.reply_text("Error getting top odds")

async def oddsToImage(data, figsize = (11, 6)):
    fig=plt.figure(figsize=figsize)
    ax = fig.add_subplot(111)
    ax.axis('off')
    df = pd.DataFrame(data, columns =["League", "Home", "Away", "Bet Type", "Price", "Point", "Outcome"])
    pd.plotting.table(ax, df, loc="center")
    plt.tight_layout(pad=0)
    buf = BytesIO()
    plt.savefig(buf, format="png", dpi=300)
    buf.seek(0)
    return buf

async def notifyAdmins(message, context : ContextTypes.DEFAULT_TYPE, ignoreUser = None):
    adminUsers = database_connector.getAdmins()
    for user in adminUsers:
        if user[0] != ignoreUser:
            await context.bot.send_message(chat_id=user[1], text=message)
        

async def sendAlerts():
    users = database_connector.getUsers()
    #TODO: maybe cache odds?


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    database_connector.connectDb()

    app = ApplicationBuilder().token("yourtokenhere").build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("scan", scan))
    app.add_handler(CommandHandler("topodds", getTopOdds))

    class MyManager(BaseManager):
        pass

    MyManager.register('get_function', callable=lambda: sendAlerts)
    manager = MyManager(address=('', 50000), authkey=b'abc')
    server = manager.get_server()
    server.serve_forever()
        

    app.run_polling()

    
