from __future__ import annotations
import datetime
import http
import http.cookiejar
import json
import re
from typing import Any, List
import nodriver as uc
import time
import asyncio

import pytz
import requests.cookies
import database_connector
import schemas
from schemas import BetType, BookmakerScanParameters, Outcome
import controller
from enum import Enum
from betting_wrapper import BettingWrapper
import contextlib
import logging
import concurrent.futures
from lxml import html, etree
import requests

import utils


def run_coroutine(coro):
    asyncio.run(coro)

class CoolbetWrapperV2(BettingWrapper):

    def __init__(self):
        self.bookmaker = "coolbetv2"

    async def test(self):
        print("veikkauswrapper test")
    

    market_names = ["Match Result",
                    "Match Winner",
                    "Match Result (1X2)",
                    "Total Goals",
                    "Handicap (3 Way)",
                    "Asian Handicap"]
    tab_count = 5
    link_category = None
    browser = None
    chrome_path = "/home/Projects/Betting/kaarme-scraper/chromeprofile"
    browser_conn = None
    requires_browser = True
    can_update_all = True
    requests_session : requests.Session = requests.Session()
    start_url = "https://www.coolbet.com/en/sports/football"
    headers : dict[str, str] = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/113.0',
        }
    proxy_dict = None

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
                        uses_custom_data_dir=True,
                        port=self.browser_conn[1], 
                        browser_args=["--blink-settings=imagesEnabled=false"])
        page = await self.browser.get(self.start_url)
        try:
            await page.find("Additional security check is required", timeout=1)
            await page.wait(60)
        except: 
            pass
        await page.wait(5)
        cookies = await self.browser.cookies.get_all(requests_cookie_format=True)
        for cookie in cookies:
            assert isinstance(cookie, http.cookiejar.Cookie)
            self.requests_session.cookies.set_cookie(cookie)
        return self.browser

    async def scrapeSite(self):
        leagues = await self.scrapeCategories()
        raw_events = []
        tasks = []
        for league in leagues:
            bookmaker_category_id = str(league["id"])
            slug = league["fullSlug"]
            if slug is None or slug == "":
                logging.debug(f"Null slug found, skipping {bookmaker_category_id}")
                continue
            category_id = database_connector.searchOrAddCategory(slug, self.bookmaker, bookmaker_category_id)
            if category_id is None:
                logging.info(f"Category not found: {slug} for {self.bookmaker}")
                continue
            raw_events = await self.scrapeEvents(bookmaker_category_id, category_id)
            for raw_event in raw_events:
                tasks.append(self.scrapeEvent(raw_event, category_id))
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for batch in utils.chunk_list(tasks, 10):
                futures = [executor.submit(run_coroutine, coro) for coro in batch]
                await asyncio.gather(*[asyncio.wrap_future(future) for future in futures])
        return True
    
    async def scrapeCategories(self) -> list[dict[str, str]]:
        url = "https://www.coolbet.com/s/sbgate/category/fo-tree/en?country=FI"
        response = self.requests_session.get(url, headers=self.headers, proxies=self.proxy_dict)
        if response.status_code != 200:
            logging.error(f"Request failed with status code: {response.status_code}\n{url}")
        data = json.loads(response.text)
        return [
            {'fullSlug': league['fullSlug'], 'id': league['id']}
            for sport in data['children']
            for region in sport['children']
            for league in region['children']
        ]
    
    def run_coroutine(coro):
        asyncio.run(coro)
    
    async def scrapeEvents(self, api_category_id : str, category_id : int) -> list[Any]:
        url = f"https://www.coolbet.com/s/sbgate/sports/fo-category/?categoryId={api_category_id}&country=NL&isMobile=0&language=en&layout=EUROPEAN&limit=99"
        response = self.requests_session.get(url, headers=self.headers, proxies=self.proxy_dict)
        if response.status_code != 200:
            logging.error(f"Request failed with status code: {response.status_code} \n{url}")
        response.encoding = 'utf-8'
        data = json.loads(response.text)
        if len(data) == 0:
            return []
        data = data[0]
        matches = [
            match
            for match in data['matches']
        ]
        return matches
    
    async def scrapeEvent(self, match, category_id):
        oghome : str | None = match['home_team_name']
        ogaway : str | None = match['away_team_name']
        if oghome == "" or ogaway == "" or oghome is None or ogaway is None:
            logging.debug(f"Null teams found, skipping ({match})")
            return
        event_id, event_url, oghome, ogaway = await self.eventFromGame(oghome, ogaway,\
                                            match['match_start'], category_id)
        event = database_connector.getEventById(event_id)
        database_connector.addBookmakerEvent(event.event_id, self.bookmaker, None, oghome, ogaway)
        markets = self.scrapeEventMarkets(match['id'], self.headers)
        outcome_ids : list[int] = []
        market_info : dict[int, tuple[BetType | None, str | None]] = {}
        for market in markets:
            info_tuple = self.matchMarketTitle(market['market_type_name'], oghome, ogaway)
            market_info[market['id']] = info_tuple
            if info_tuple[0] is None:
                continue
            outcome_ids = outcome_ids + [outcome['id'] for outcome in market['outcomes']]
        market_ids = [market['id'] for market in markets]
        if len(market_ids) == 0:
            return
        outcome_odds = self.scrapeOutcomeOdds(market_ids)

        for market in markets:
            market_id = market['id']
            market_type, market_description = market_info[market_id]
            if market_type is None:
                continue
            outcomes : list[Outcome] = []
            for outcome in market['outcomes']:
                outcome_bookmaker_id = str(outcome['id'])
                if outcome_bookmaker_id not in outcome_odds:
                    continue
                outcomes.append(
                    self.outcomeDataToOutcome(outcome['result_key'], market['raw_line'],\
                                            outcome_odds[outcome_bookmaker_id], oghome, ogaway, outcome_bookmaker_id)
                )
            database_connector.addOrUpdateMarket(
                schemas.Market(
                    event_id=event.event_id,
                    bookmaker_key=self.bookmaker,
                    last_update=datetime.datetime.now(pytz.utc),
                    description=market_description,
                    bet_type_id=market_type,
                    outcomes=outcomes,
                    market_bookmaker_id=str(market_id),
                )
            )
    
    async def updateOdds(self):
        await self.initBrowser()
        markets = database_connector.getBookmakerMarkets(self.bookmaker)
        market_dict = {int(item[1]): item[0] for item in markets}
        market_api_keys = [int(market[1]) for market in markets]
        print(markets)
        odds = self.scrapeOutcomeOdds(market_api_keys)
        update : dict[int, float] = {}
        for item in odds.items():
            if item[0] in market_dict:
                update[market_dict[item[0]] : item[1]]
        database_connector.updateOddsByOutcomeId(update)
    
    def scrapeEventMarkets(self, api_event_id : int, headers : dict[str, str]) -> list:
        url = f"https://www.coolbet.com/s/sbgate/sports/fo-market/sidebets?country=NL&language=en&layout=EUROPEAN&matchId={api_event_id}&matchStatus=OPEN"
        response = self.requests_session.get(url, headers=headers, proxies=self.proxy_dict)
        print(url)
        if response.status_code != 200:
            logging.error(f"Request failed with status code: {response.status_code}\n{url}\n{headers}\n{response.text}")
        data = json.loads(response.text)
        top = data['markets']
        markets = []
        for market in top:
            if 'markets' in market:
                markets.extend(market['markets'])
        return markets

    def scrapeOutcomeOdds(self, market_ids : list[int]) -> dict[str, float]:
        split_market_ids = self.split_list(market_ids, 60)
        odds_dict : dict[str, float] = {}
        for id_list in split_market_ids:
            url = "https://www.coolbet.com/s/sb-odds/odds/current/fo-line/"
            market_ids_json = {"marketIds": [id_list] }
            response = self.requests_session.post(url, headers=self.headers, json=market_ids_json, proxies=self.proxy_dict)
            if response.status_code != 200:
                logging.error(f"Request failed with status code: {response.status_code}\n{url}\n{market_ids_json}")
            data = json.loads(response.text)
            for i in data:
                odds_dict[i] = data[i]["value"]
        return odds_dict
    def split_list(self, input_list: list[int], max_size: int) -> list[list[int]]:
        return [input_list[i:i + max_size] for i in range(0, len(input_list), max_size)]

    async def eventFromGame(self, home : str, away : str, start_time : str, category_id : int, inplay : bool = False):
        is_live = False
        today = datetime.date.today()
        event_datetime = datetime.datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S%z")
        event_id = database_connector.searchOrAddEvent(home, away, category_id, event_datetime)
        event_url = database_connector.getEventUrl(event_id, self.bookmaker)
        print("event found", home, away)
        return (event_id, event_url, home, away)
    


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
            "[Home] Total Goals": (BetType.totals, "home"),
            "[Away] Total Goals": (BetType.totals, "away"),
            "1st Half [Home] Goals": (BetType.totals, "home;h1"),
            "1st Half [Away] Goals": (BetType.totals, "away;h1"),
            "Total Corners": (BetType.totals, ";corners"),
            "[Home] Corners": (BetType.totals, "home;corners"),
            "[Away] Corners": (BetType.totals, "away;corners"),
            # Add more mappings as needed
        }
        
        # Return the corresponding value from the dictionary, or (None, None) if not found
        return market_mapping.get(regular_title, (None, None))
    
    def outcomeDataToOutcome(self, name : str, point : float | None, price : float, \
                            oghome : str, ogaway : str, outcome_bookmaker_id : str) -> Outcome:
        outcome_name = ""
        if name == "[Home]":
            outcome_name = "home"
        elif name == "[Away]": 
            outcome_name = "away"
        elif name == "[Draw]":
            outcome_name = "draw"
        else:
            outcome_name = name.lower().strip()
        if point == 0:
            point = None
        return Outcome(
                    name=outcome_name,
                    price=price,
                    description="",
                    point=point,
                    outcome_bookmaker_id=outcome_bookmaker_id
                )
