from __future__ import annotations
from collections import defaultdict
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


def run_coroutine(coro):
    asyncio.run(coro)

class PinnacleWrapper(BettingWrapper):

    def __init__(self):
        self.bookmaker = "pinnacle"

    async def test(self):
        print("pinnaclewrapper test")
    

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
    start_url = "https://www.pinnacle.com/en/"
    headers : dict[str, str] = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/113.0',
            'X-API-Key' : 'CmX2KcMrXuFmNg6YFbmTxE0y9CIrOi0r',
            'X-Device-UUID' : 'null',
            'Origin' : 'https://www.pinnacle.com',
        }

    async def run(self, params : BookmakerScanParameters) -> bool:
        if self.browser_conn is None:
            logging.error("Missing browser connection")
            return
        await self.initBrowser()
        self.list_link = params.link
        self.sport = params.sport
        self.link_category = params.category
        category_only = params.categories_only
        return await self.scrapeSite(category_only)
    
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

    async def scrapeSite(self, category_only = False):
        leagues = await self.scrapeCategories()
        for league in leagues:
            bookmaker_category_id = str(league["id"])
            name = league["name"]
            if name is None or name == "":
                logging.debug(f"Null name found, skipping {bookmaker_category_id}")
                continue
            category_id = database_connector.searchOrAddCategory(name, self.bookmaker, bookmaker_category_id)
            if category_id is None:
                logging.info(f"Category not found: {name} for {self.bookmaker}")
                continue
            if not category_only:
                await self.scrapeEvents(bookmaker_category_id, category_id)
        return True
    
    async def scrapeCategories(self) -> list[dict[str, str]]:
        #sports : list [int] = self.scrapeSports()  
        sports = [ 1, 3, 4, 6, 10, 12, 15, 19, 22, 28, 29, 33, 34, 37, 40 ]
        result_data = []
        for sport in sports:
            url = f"https://guest.api.arcadia.pinnacle.com/0.1/sports/{sport}/leagues?all=false&brandId=0"
            response = self.requests_session.get(url, headers=self.headers)
            if response.status_code != 200:
                #print(url)
                #print(response.text)
                logging.debug(f"Request failed with status code: {response.status_code}")
                continue
            else:
                print(url)
            data = json.loads(response.text)
            for league_obj in data:
                result_data.append({
                    'name': league_obj['name'],
                    'id': league_obj['id'],
                })
        return result_data
    
    def scrapeSports(self) -> list:
        url = "https://guest.api.arcadia.pinnacle.com/0.1/sports?brandId=0"
        response = self.requests_session.get(url, headers=self.headers)
        if response.status_code != 200:
            logging.error(f"Request failed with status code: {response.status_code} \n url: {url}")
        data = json.loads(response.text)
        result_data = []
        for obj in data:
            result_data.append(obj['id'])
        return result_data
    
    async def scrapeEvents(self, api_category_id : str, category_id : int):
        url = f"https://guest.api.arcadia.pinnacle.com/0.1/leagues/{api_category_id}/matchups?brandId=0"
        print(url)
        headers = self.headers.copy()
        headers.pop("Origin")
        response = self.requests_session.get(url, headers=headers)
        if response.status_code != 200:
            logging.debug(f"Request failed with status code: {response.status_code} \n url: {url}")
            return
        response.encoding = 'utf-8'
        data = json.loads(response.text)
        if len(data) == 0:
            return
        parents = []
        for matchup in data:
            if "parent" in matchup and matchup["parent"] is None:
                parents.append(matchup)

        for match in parents:
            oghome : str | None = None
            ogaway : str | None = None
            for participant in match['participants']:
                if participant['alignment'] == "home":
                    oghome = participant['name']
                if participant['alignment'] == "away":
                    ogaway = participant['name']
            if oghome == "" or ogaway == "" or oghome is None or ogaway is None:
                logging.debug(f"Null teams found, skipping ({match})")
                continue
            event_id, event_url, oghome, ogaway = await self.eventFromGame(oghome, ogaway,\
                                                match['startTime'], category_id)
            event = database_connector.getEventById(event_id)
            database_connector.addBookmakerEvent(event.event_id, self.bookmaker, None, oghome, ogaway)
            markets = self.scrapeEventMarkets(match['id'], self.headers, event, oghome, ogaway)
    
    async def updateOdds(self):
        await self.initBrowser()
        markets = database_connector.getBookmakerMarkets(self.bookmaker)
        market_dict = {int(item[1]): item[0] for item in markets}
        market_api_keys = [int(market[1]) for market in markets]
        odds = self.scrapeOutcomeOdds(market_api_keys)
        update : dict[int, float] = {}
        for item in odds.items():
            if item[0] in market_dict:
                update[market_dict[item[0]] : item[1]]
        database_connector.updateOddsByOutcomeId(update)


    
    def scrapeEventMarkets(self, api_event_id : int, headers : dict[str, str], event : schemas.Event, oghome : str, ogaway : str) -> list:
        headers2 = headers.copy()
        headers2.pop("Origin")
        url = f"https://guest.api.arcadia.pinnacle.com/0.1/matchups/{api_event_id}/markets/related/straight"
        api_markets = self.loadUrl(url, headers)
        if api_markets is None:
            return []
        url2 = f"https://guest.api.arcadia.pinnacle.com/0.1/matchups/{api_event_id}/related"
        matchups = self.loadUrl(url2, headers2)
        if matchups is None:
            return []

        matchup_dict = {}
        matchups_by_description : dict[str, list] = {}
        for raw_matchup in matchups:
            description = raw_matchup.get("special", {}).get("description")
            point = None
            if description is not None:
                match = re.search(r"(-?\d+)(?!.*\d)", description)
                point = match.group(0) if match else None
                description = re.sub(r"\([^)]*\)| [-+]?\d+$", "", description) if description is not None else None
                description = controller.replaceTeamNames(description, oghome, ogaway)
            id = raw_matchup.get("id", "")
            matchup = {
                "description": description,
                "id": id,
                "points": point,
                "participants": raw_matchup["participants"],
                "units": raw_matchup["units"],
            }
            matchup_dict[id] = matchup
            if description is not None:
                if matchups_by_description.get(description) is None:
                    matchups_by_description[description] = [matchup]
                else:
                    matchups_by_description[description].append(matchup)

        market_dict : dict[tuple[str, str, int], Any] = {}
        for api_market in api_markets:
            market_type = api_market["type"]
            matchup_id = api_market.get("matchupId", None)
            key = api_market['key']
            key_parts = key.split(';')
            half = int(key_parts[1])
            matchup = matchup_dict.get(matchup_id)
            if matchup is None:
                continue
            description = matchup['description']
            units = matchup['units']
            matchup_key = description or matchup_id
            side = api_market.get("side", None)
            market = market_dict.get((market_type, matchup_key, half, side), None)
            matchup_points = matchup.get("points")
            outcomes : list[schemas.Outcome] = []
            for price in api_market['prices']:
                print(price)
                name = None
                participant_id = price.get("participantId")
                if participant_id is None:
                    name = price.get("designation")
                else:
                    name, alignment = self.getParticipantInfo(
                        matchup, price.get("participantId"), oghome, ogaway,
                    )
                price_point = price.get('points')
                point = price_point or matchup_points
                if price_point and name == "away":
                    point = -point
                outcomes.append(schemas.Outcome(
                    name=name,
                    price=controller.american_to_decimal(price['price']),
                    point=point,
                ))
            if market is None:
                market_dict[(market_type, matchup_id, half, side)] = \
                    {
                        "type" : market_type,
                        "key" : key,
                        "side" : side,
                        "outcomes" : outcomes,
                        "description" : matchup['description'],
                        "half" : half,
                        "side" : side,
                        "units" : units,
                    }
            else:
                market['outcomes'].extend(outcomes)


        
        for market in market_dict.values():
            market_type, market_description = self.matchMarketTitle(
                market["type"],
                market["description"],
                market["side"],
                market["units"],
                market["half"])
            if market_type is None:
                continue
            database_connector.addOrUpdateMarket(schemas.Market(
                event_id=event.event_id,
                bookmaker_key=self.bookmaker,
                last_update=datetime.datetime.now(pytz.utc),
                description=market_description,
                bet_type_id=market_type,
                outcomes=market["outcomes"],
            ))
        return market_dict
    
    def getParticipantInfo(self, matchup, participant_id : str, oghome : str, ogaway : str):
        for participant in matchup['participants']:
            id  = participant.get("id")
            if id == participant_id:
                name = re.sub(r"\([^)]*\)| [-+]?\d+$", "", participant["name"])
                name = controller.replaceTeamNames(name, oghome, ogaway)
                return (participant['alignment'], name)
        return None

    def loadUrl(self, url : str, headers : dict[str, str]):
        url = url
        response = self.requests_session.get(url, headers=headers)
        print(url)
        if response.status_code != 200:
            logging.debug(f"Request failed with status code: {response.status_code} \n url: {url}")
            return None
        return json.loads(response.text)


    def scrapeOutcomeOdds(self, market_ids : list[int]) -> dict[str, float]:
        split_market_ids = self.split_list(market_ids, 60)
        odds_dict : dict[str, float] = {}
        for id_list in split_market_ids:
            url = "https://www.coolbet.com/s/sb-odds/odds/current/fo-line/"
            market_ids_json = {'marketIds': [id_list] }
            response = self.requests_session.post(url, headers=self.headers, json=market_ids_json)
            if response.status_code != 200:
                logging.error(f"Request failed with status code: {response.status_code}")
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
    
    def matchMarketTitle(self, type : str, description : str | None, side : str | None, units : str, half : int) -> tuple[BetType | None, str | None]:
        # Replace team names with placeholders
        #regular_title = re.sub(r'[-]?\d+$', "{number}", regular_title, flags=re.IGNORECASE)
        market_info : tuple[BetType | None, str | None] = (None, None)
        if type == "moneyline" and description is None:
            market_info = (BetType.h2h, None)
        elif type == "moneyline" and description == "3-Way Handicap":
            market_info = (BetType.spreads, None)
        elif type == "total" and description is None:
            market_info = (BetType.totals, None)
        elif type == "team_total":
            market_info = (BetType.totals, side)
        elif type == "spread" and description is None:
            market_info = (BetType.asian_spreads, None)
        if half != 0:
            market_info = (market_info[0], (market_info[1] or "") + ";h" + str(half))
        if units == "Corners":
            market_info = (market_info[0], (market_info[1] or "") + ";corners")
        elif units == "Bookings":
            market_info = (market_info[0], (market_info[1] or "") + ";bookings")
        elif units != "Regular":
            return (None, None)
        return market_info
    
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
        if name == "away":
            point = -point
        if point == 0:
            point = None
        return Outcome(
                    name=outcome_name,
                    price=price,
                    description="",
                    point=point,
                    outcome_bookmaker_id=outcome_bookmaker_id
                )
