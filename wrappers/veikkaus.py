from __future__ import annotations
import datetime
import re
from typing import List, Optional
import nodriver as uc
import time
import asyncio

import pytz
import database_connector
import schemas
from schemas import BetType, BookmakerScanParameters
import controller
from enum import Enum
from betting_wrapper import BettingWrapper
import contextlib
import logging
import concurrent.futures
from lxml import html, etree


def run_coroutine(coro):
    asyncio.run(coro)

class VeikkausWrapper(BettingWrapper):

    def __init__(self):
        self.bookmaker = "veikkaus"

    async def test(self):
        print("veikkauswrapper test")
    
    links = [
        "https://www.veikkaus.fi/fi/vedonlyonti/pitkaveto?pelilista=1",
        "https://www.veikkaus.fi/fi/vedonlyonti/pitkaveto?pelilista=1&timerange=1",
        "https://www.veikkaus.fi/fi/vedonlyonti/pitkaveto?pelilista=1&timerange=2",
        "https://www.veikkaus.fi/fi/vedonlyonti/liveveto?laji",
        "https://www.veikkaus.fi/fi/vedonlyonti/pitkaveto?timerange=1&t=1-5-1_LaLiga",
        "https://www.veikkaus.fi/fi/vedonlyonti/pitkaveto?t=1-2-1_Valioliiga",
        f"https://www.veikkaus.fi/fi/vedonlyonti/pitkaveto?t=1-37-1_Brasilian%20liiga"
    ]

    link_dict = {
        "https://www.veikkaus.fi/fi/vedonlyonti/pitkaveto?t=1-2-1_Valioliiga" : "Premier League",
        f"https://www.veikkaus.fi/fi/vedonlyonti/pitkaveto?t=1-37-1_Brasilian%20liiga" : "Brazil League",
        "https://www.veikkaus.fi/fi/vedonlyonti/pitkaveto?timerange=1&t=1-5-1_LaLiga" : "LaLeague",
        f"https://www.veikkaus.fi/fi/vedonlyonti/pitkaveto?t=1-12-1_Portugalin%20liiga" : "Portugal League",
        f"https://www.veikkaus.fi/fi/vedonlyonti/pitkaveto?t=1-4-6_Saksan%20cup" : "German Cup"
    }

    list_link : str | None = None
    link_category : str | None = None

    market_names = ["1X2", "Aasialainen tasoitus", "Tasoitus", "Yli/Alle"]

    weekdayDict = {"ma": 0, "ti": 1, "ke": 2, "to": 3, "pe": 4, "la": 5, "su": 6}
    tab_count = 25
    sport = None
    browser = None
    browser_conn = None
    requires_browser = True

    async def run(self, params : BookmakerScanParameters) -> bool:
        if self.browser_conn is None:
            logging.error("Missing browser connection")
            return
        await self.initBrowser()
        self.list_link = params.link
        self.sport = params.sport
        self.link_category = params.category
        return await self.scrapeSite()
    
    async def rescanEvent(self, link : str):
        if self.browser_conn is None:
            logging.error("Missing browser connection")
            return
        await self.initBrowser()
        event_id, bookmaker_key = database_connector.getEventFromUrl(link)
        event = database_connector.getEventById(event_id)
        await self.scrapeGame((self.browser.config.host, self.browser.config.port), link, event,  is_event_url=True)
    
    async def initBrowser(self):
        self.browser = await uc.start(headless=False,
                        user_data_dir=self.browser_conn[2],host=self.browser_conn[0],
                        port=self.browser_conn[1], 
                        browser_args=["--blink-settings=imagesEnabled=false"])
        return self.browser

    async def scrapeSite(self):
        await self.scrapeEventPage(self.list_link)
        return True

    async def eventPagePreprocess(self, page : uc.Tab):
        if self.sport is None:
            return
        sportbutton = await page.select(".SportFilterContainer-module__sports--Zn1mx[title=" + self.sport + "] button", 30)
        await sportbutton.click()
        await page.wait(3)


    async def scrapeEventPage(self, link: str):
        markets = []
        page = await self.browser.get(link)
        await page.maximize()
        await page.wait(1)
        await page
        
        print("|" + link + "|" + page.url+"|")
        if page.url != link:
            logging.error("Invalid link: |" + link + "|" + page.url+"|")
            return
        elems = None

        try:
            gdprbutton = await page.select("#save-necessary-action", 0.5)
            await gdprbutton.click()
        except TimeoutError:
            pass
        await self.eventPagePreprocess(page)
        
        for i in range(3):
            await page.scroll_down(300)
            await page.wait(1)

        content = str(await page.get_content())
        tree = html.fromstring(content)
        elems = tree.xpath('//*[@class="subpage-game-row"]')
        if elems is None or len(elems) == 0:
            return
        processList = []
        gameCount = len(elems)
        print(gameCount)
        if self.browser is None:
            return
        with concurrent.futures.ThreadPoolExecutor() as executor:
            tasks = []
            for i in range(gameCount):
                event_id, event_url, oghome, ogaway  = await self.eventFromGame(elems[i]) # recover event id to avoid researching
                event = database_connector.getEventById(event_id)
                if event_url is None:
                    task = self.scrapeGame(link, event, oghome, ogaway,is_event_url=False, index=i)
                    tasks.append(task)
                else:
                    task = self.scrapeGame(event_url, event, oghome, ogaway, is_event_url=True)
                    tasks.append(task)
                    # append coroutine that opens the odds page directly
                if (i != 0 and(i % self.tab_count == 0)) or i == gameCount - 1:
                    futures = [executor.submit(run_coroutine, coro) for coro in tasks]
                    await asyncio.gather(*[asyncio.wrap_future(future) for future in futures])
                    tasks.clear()
        #await page.close()

    async def eventFromGame(self, game : html.HtmlElement):
        home = controller.get_text_by_xpath(game, ".//*[contains(@class, 'gameinfo-teams-team--home')][1]")
        away = controller.get_text_by_xpath(game, ".//*[contains(@class, 'gameinfo-teams-team--away')][1]")
        category = self.link_category if self.link_category is not None else controller.get_text_by_xpath(game, ".//*[contains(@class, 'teams-description')][1]")
        category_id = database_connector.searchOrAddCategory(re.sub(r"^\d+\s*", "", category))
        day = None
        try:
            day = controller.get_text_by_xpath(game, ".//*[@class='pitkaveto-subpage-game-row__gameinfo--time--day'][1]")
        except:
            pass
        if day == []:
            day = None
        today = datetime.datetime.today()
    
        
        weekday = today
        if day is not None:
            weekday_number = self.weekdayDict[re.sub(r'[\d.\s]+', '', str(day).lower())]
            weekday = weekday + datetime.timedelta(
                (weekday_number - today.weekday()) % 7
            )  # get the correct date from given weekday"
        time = controller.get_text_by_xpath(game, ".//*[@class='pitkaveto-subpage-game-row__gameinfo--time--time'][1]")
        game_datetime = None
        if time is None:
            game_datetime = datetime.datetime.now()
        else:
            game_time_list = time.split(".")
            game_time = datetime.time(int(game_time_list[0]), int(game_time_list[1]))
            game_datetime = datetime.datetime.combine(weekday, game_time, tzinfo=pytz.timezone("Europe/Helsinki")).astimezone(tz=pytz.utc)
        event_id = database_connector.searchOrAddEvent(home, away, category_id, game_datetime)
        event_url = database_connector.getEventUrl(event_id, self.bookmaker)
        print("event found", home, away)
        return (event_id, event_url, home, away)

    async def scrapeGame(self, link: str, event : schemas.Event,\
                        oghome : str, ogaway : str,  is_event_url : bool, index : int | None = None, disable_scroll : bool = True):
        browser = await self.initBrowser()
        page = await browser.get(link, new_window=True)
        await page.maximize()
        await page
        if not is_event_url:
            print("not event url")
            await self.eventPagePreprocess(page)
            if not disable_scroll:
                for i in range(4):
                    await page.scroll_down(300)
                    await page.wait(1)
            
            game = (await page.select_all(".subpage-game-row"))[index]
            await game.click()
            await page

            database_connector.addBookmakerEvent(event.event_id, self.bookmaker, page.url, oghome, ogaway)

        await self.scrapeOddsPage(page, event, oghome, ogaway)
        await page.close()

    def matchMarketTitle(self, title: str, oghome: str, ogaway: str) -> tuple[BetType | None, str | None]:
        # Replace team names with placeholders
        regular_title = re.sub(re.escape(oghome), "{home}", title, flags=re.IGNORECASE)
        regular_title = re.sub(re.escape(ogaway), "{away}", regular_title, flags=re.IGNORECASE)
        
        # Define the dictionary mapping
        market_mapping = {
            "1X2": (BetType.h2h, None),
            "Tasoitus": (BetType.spreads, None),
            "Aasialainen tasoitus": (BetType.asian_spreads, None),
            "Yli/Alle": (BetType.totals, None),
            "{home}: Yli/Alle - Joukkue": (BetType.totals, "home"),
            "{away}: Yli/Alle - Joukkue": (BetType.totals, "away"),
            "Aasialainen tasoitus - Lisäkohteet": (BetType.asian_spreads, None),
            # Add more mappings as needed
        }
        
        # Return the corresponding value from the dictionary, or (None, None) if not found
        return market_mapping.get(regular_title, (None, None))
    

    async def scrapeOddsPage(self, gametab: uc.Tab, event : schemas.Event, oghome : str, ogaway : str):
        try:
            await gametab.find("Suosituimmat", timeout=60)
        except: 
            pass
        content = str(await gametab.get_content())
        tree = html.fromstring(content)
        market_titles : list[html.HtmlElement] = tree.xpath("//h2[contains(@class, 'sub-rows-card__header--market-name')]")
        markets : list[tuple[BetType, str | None]] = []
        market_cards : list[html.HtmlElement] = []
        for i, market_title in enumerate(market_titles):
            market_type, market_name = self.matchMarketTitle(market_title.text, oghome, ogaway)
            if market_type is None:
                continue
            market_cards.append(market_title.getparent().getparent().getparent())
            markets.append((market_type, market_name))
        for i, market_card in enumerate(market_cards):
            bet_type = markets[i][0]
            description = markets[i][1]
            outcome_buttons : list[html.HtmlElement] = market_card.xpath(".//button[contains(@class, 'bet-selection-button')]")
            outcomes = []
            for outcome_button in outcome_buttons:
                if len(outcome_button) == 0:
                    continue # empty spaces are implemented as buttons without the usual div inside. (pretty odd ik)
                button_divider = outcome_button.xpath("./div[contains(@class, 'button-content-divided')]")[0]
                name : str = button_divider[0].text
                point = None
                pointString = ""
                outcome_name = ""
                if "tasapeli" in name.lower():
                    outcome_name = "draw"
                elif oghome in name:
                    outcome_name = "home"
                elif ogaway in name:
                    outcome_name = "away"
                else:
                    outcome_name = name
                if bet_type in (BetType.spreads, BetType.asian_spreads): 
                    pointString = (
                        name.replace(oghome, "").replace(ogaway, "").replace("tasapeli,", "")
                        .replace(" ", "")
                        .replace("+", "")
                    )

                    point = float(pointString)
                    if outcome_name == "away":
                        point = -point
                name = name.lower()
                if bet_type is BetType.totals:
                    if "yli" in name:
                        outcome_name = "over"
                    elif "alle" in name:
                        outcome_name = "under"
                    pointString = (
                        name.replace("yli", "").replace("alle", "").replace(" ", "").replace(",", ".")
                    )
                    point = float(pointString)
                priceElement : html.HtmlElement = button_divider[1]
                priceString = priceElement.text.replace(",", ".")
                if len(name) != len(pointString):
                    name = name.replace(pointString, "").rstrip()
                if name != "" and priceString != "":
                    price = float(priceString)
                    outcomes.append(
                        schemas.Outcome(
                            name=outcome_name,
                            price=price,
                            description="",
                            point=point,
                        )
                    )
            logging.debug(f"addOrUpdateMarket {description} {bet_type}")
            database_connector.addOrUpdateMarket(
                schemas.Market(
                    event_id=event.event_id,
                    bookmaker_key="veikkaus",
                    last_update=datetime.datetime.now(pytz.utc),
                    description=description,
                    bet_type_id=bet_type,
                    outcomes=outcomes,
                )
            )   
    def scrapeLeagues(self):
        pass

