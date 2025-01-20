from __future__ import annotations
import datetime
import re
from typing import List
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

class CoolbetWrapper(BettingWrapper):

    def __init__(self):
        self.bookmaker = "coolbet"

    async def test(self):
        print("veikkauswrapper test")
    
    links = [
        "https://www.coolbet.com/en/sports/soon/1",
        "https://www.coolbet.com/en/sports/soon/6",
        "https://www.coolbet.com/en/sports/live",
        "https://www.coolbet.com/en/sports/football/spain/la-liga",
        "https://www.coolbet.com/en/sports/football/england/premier-league",
        "https://www.coolbet.com/en/sports/football/brazil"
    ]

    market_names = ["Match Result",
                    "Match Winner",
                    "Match Result (1X2)",
                    "Total Goals",
                    "Handicap (3 Way)",
                    "Asian Handicap"]
    tab_count = 5
    link_index  = None
    link_category = None
    browser = None
    chrome_path = "/home/Projects/Betting/kaarme-scraper/chromeprofile"
    game_element_class = "styles-sc-1qce5z3-0"
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

    async def scrapeEventPage(self, link: str):
        markets = []
        page = await self.browser.get(link)
        elems = None
        await page.wait(5)
        await page
        for i in range(3):
            await page.scroll_down(300)
            await page.wait(1)

        content = str(await page.get_content())
        tree = html.fromstring(content)
        elems : list[html.HtmlElement] = tree.xpath("//*[contains(@class, '"+self.game_element_class+"')]")
        await page
        if not elems:
            return


        processList = []
        gameCount = len(elems)
        print(gameCount)
        if self.browser is None:
            return

        with concurrent.futures.ThreadPoolExecutor() as executor:
            tasks = []
            for i in range(gameCount):
                outright_name = elems[i].xpath('.//*[contains(@class, "match-outright-name")]')
                match_teams = elems[i].xpath('.//*[contains(@class, "match-teams")]')
                if (not outright_name) and match_teams:
                    event_id, event_url, oghome, ogaway = await self.eventFromGame(elems[i]) # recover event id to avoid researching
                    event = database_connector.getEventById(event_id)
                    if event_url is None:
                        task = self.scrapeGame(link, event, oghome, ogaway,is_event_url=False, index=i)
                        tasks.append(task)
                    else:
                        task = self.scrapeGame(event_url, event, oghome, ogaway, is_event_url=True)
                        tasks.append(task)
                        # append coroutine that opens the odds page directly
                if i % self.tab_count == 0 or i == gameCount - 1:
                    futures = [executor.submit(run_coroutine, coro) for coro in tasks]
                    await asyncio.gather(*[asyncio.wrap_future(future) for future in futures])
                    tasks.clear()
        # await page.close()
      #  print("pageclose")


    async def eventFromGame(self, game : html.HtmlElement):
        home = controller.get_text_by_xpath(game, ".//*[contains(@class, 'team-home')]//*[contains(@class, 'name')][1]").strip()
        away = controller.get_text_by_xpath(game, ".//*[contains(@class, 'team-away')]//*[contains(@class, 'name')][1]").strip()
        category = self.link_category if self.link_category is not None else \
            controller.get_text_by_xpath(game, "(.//*[contains(@class, 'category-name')]//a)[1]").strip()
        category_id = database_connector.searchOrAddCategory(re.sub(r"^\d+\s*", "", category))
        is_live = False
        try:
            live_element = game.xpath(".//*[contains(@class, 'live-info')][1]")[0]
            if live_element is not None:
                is_live = True
        except:
            pass

        datetime_element = None
        if not is_live:
            elements = game.xpath("(.//*[contains(@class, 'match-time')]//*[contains(@class, 'styles-sc-99wlb8-0')])[1]")
            datetime_element = elements[0] if len(elements) > 0 else None

        today = datetime.date.today()
        
        event_datetime = None
        if datetime_element is not None and not is_live:
            current_year = datetime.datetime.now().year
            time = datetime_element.xpath("(.//span)[1]")[0].text
            date_str_with_year = f"{datetime_element.text} {current_year} {time}"
            event_datetime = datetime.datetime.strptime(date_str_with_year, "%d %b, %Y %H:%M")
        elif is_live:
            event_datetime = datetime.datetime.now(pytz.utc)
        else:
            logging.error("Date and time element not found for game: " + home + " vs " + away)


        event_id = database_connector.searchOrAddEvent(home, away, category_id, event_datetime)
        event_url = database_connector.getEventUrl(event_id, self.bookmaker)
        print("event found", home, away)
        return (event_id, event_url, home, away)

    async def scrapeGame(self, link: str, event : schemas.Event,\
                        oghome : str, ogaway : str,  is_event_url : bool, index : int | None = None, disable_scroll : bool = True):
        browser = await self.initBrowser()
        page = await browser.get(link, new_window=True)
        try:
            test = await page.find("Additional security check is required", timeout=1)
            page.wait(20)
        except:
            pass
        markets = await page.find_elements_by_text(self.game_element_class)
        await page
        if not is_event_url:
            if not disable_scroll:
                for i in range(4):
                    if (len(markets) > index):
                        break
                    await page.scroll_down(300)
                    await page.wait(1)
            await page.wait(5)
            game = await self.getGameElement(page, index)
            if game is None:
                return

            await game
            click_area = await game.query_selector(".match-teams")
            await click_area.click()
            await page.wait(1.5)
            await page
            game = await self.getGameElement(page, index)
            if game is None:
                return
            moreOdds = controller.verifyElement(await game.query_selector(".more-odds-buttons")).children[0]
            moreOdds = controller.verifyElement(moreOdds)
            await moreOdds.click()
            await page.wait(1)
            await page
            expand = None
            try: 
                expand = (await page.select("button[data-test='expand-all markets']"))
            except:
                pass
            if expand is not None:
                await expand.click()
            await page

            database_connector.addBookmakerEvent(event.event_id, self.bookmaker, page.url, oghome, ogaway)
        await self.scrapeOddsPage(page, event, oghome, ogaway)
        await page.close()
    
    async def getGameElement(self, page : uc.Tab, index : int) -> uc.Element | None:
        game_list = await page.find_elements_by_text(self.game_element_class)
        game = None
        if len(game_list) -1  >= index:
            game = controller.verifyElement(game_list[index])
        return game
    
    def matchMarketTitle(self, title: str, oghome: str, ogaway: str) -> tuple[BetType | None, str | None]:
        # Replace team names with placeholders
        regular_title = re.sub(re.escape(oghome), "{home}", title, flags=re.IGNORECASE)
        regular_title = re.sub(re.escape(ogaway), "{away}", regular_title, flags=re.IGNORECASE)
        
        # Define the dictionary mapping
        market_mapping = {
            "Match Result (1X2)": (BetType.h2h, None),
            "Handicap (3 Way)": (BetType.spreads, None),
            "Asian Handicap": (BetType.asian_spreads, None),
            "Total Goals": (BetType.totals, None),
            "{home} Total Goals": (BetType.totals, "home"),
            "{away} Total Goals": (BetType.totals, "away"),
            # Add more mappings as needed
        }
        
        # Return the corresponding value from the dictionary, or (None, None) if not found
        return market_mapping.get(regular_title, (None, None))

    async def scrapeOddsPage(self, gametab: uc.Tab, event : schemas.Event, oghome : str, ogaway : str):
        await gametab.wait(10)
        await gametab
        # TODO redo this to use lxml
        content = str(await gametab.get_content())
        tree = html.fromstring(content)
        market_titles : list[html.HtmlElement] = tree.xpath("//div[contains(@class, 'sidebets-layout')]//div[contains(@class, 'sidebet-name')]")
        markets : list[tuple[BetType, str]] = []
        market_cards : list[html.HtmlElement] = []

        for i, market_title in enumerate(market_titles):
            market_text_element : html.HtmlElement = market_title.xpath(".//*[contains(@class, 'name')]")[0]
            market_text = market_text_element.text
            market_type, market_description = self.matchMarketTitle(market_text, oghome, ogaway)
            if market_type is None:
                continue
            market_cards.append(market_title.getparent().getparent().getparent())
            markets.append((market_type, market_description))
        for i, market_card in enumerate(market_cards):
            bet_type = markets[i][0]
            description = markets[i][1]
            outcome_buttons : list[html.HtmlElement] = market_card.xpath(".//*[contains(@class, 'odds-button')]")
            outcome_names : list[html.HtmlElement] = market_card.xpath(".//*[contains(@class, 'sidebet-outcome-name')]")
            outcome_points : list[html.HtmlElement] = market_card.xpath(".//*[contains(@class, 'table-outcome-name')]/div/div")
            outcomes = []
            points_len = len(outcome_points)
            for opoint in outcome_points:
                continue
            for index, outcome_button in enumerate(outcome_buttons):
                if "data-test='button-odds-disabled'" in outcome_button.attrib:
                    continue # skip disabled buttons
                name = None
                column = int(index / points_len) if points_len > 0 else index
                row = index - column * points_len
                name = outcome_names[column].text.strip()
                point_str = None
                if points_len > 0:
                    point_str = outcome_points[row].text_content()
                outcome_name = ""
                if oghome in name:
                    outcome_name = "home"
                elif ogaway in name:
                    outcome_name = "away"
                elif "draw" in name.lower():
                    outcome_name = "draw"
                else:
                    outcome_name = name.lower().strip()
                point = None
                if point_str is not None and "-" in point_str:
                    points = point_str.split("-")
                    point = float(points[0]) - float(points[1])
                        
                elif point_str is not None:
                    point = float(point_str)
                price_element = outcome_button.xpath(".//*[contains(@class, 'outcome-value')]")[0]
                price_string = price_element.text or ""
                if name is not None and price_string is not None and name != "" and price_string != "":
                    price = float(price_string)
                    outcomes.append(
                        schemas.Outcome(
                            name=outcome_name,
                            price=price,
                            description="",
                            point=point,
                        )
                    )
            database_connector.addOrUpdateMarket(
                schemas.Market(
                    event_id=event.event_id,
                    bookmaker_key=self.bookmaker,
                    last_update=datetime.datetime.now(pytz.utc),
                    description=description,
                    bet_type_id=bet_type,
                    outcomes=outcomes,
                )
            )
