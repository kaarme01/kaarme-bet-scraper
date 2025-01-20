from __future__ import annotations
from collections import defaultdict
import datetime
import json
import re
from typing import List
import nodriver as uc
import time
import asyncio

import pytz
import database_connector
import schemas
from schemas import BetType
import controller
from enum import Enum
from betting_wrapper import BettingWrapper
import contextlib
import logging
import concurrent.futures

import os

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds
from dotenv import load_dotenv
from py_clob_client.constants import POLYGON

import requests


def run_coroutine(coro):
    asyncio.run(coro)

class PolymarketWrapper(BettingWrapper):

    def __init__(self):
        self.bookmaker = "polymarket"

    async def test(self):
        print("polymarketwrapper test")
    

    host = "https://clob.polymarket.com"

    key = os.getenv("PK")
    chain_id = POLYGON

# Create CLOB client and get/set API credentials
    #client = ClobClient(host, key=key, chain_id=chain_id)

    async def run(self, sport : str | None, linkIndex : int) -> bool:
        return await self.scrapeSite()
    
    async def scrapeSite(self):
        queryTime = datetime.datetime.now(pytz.utc) - datetime.timedelta(days=7)
        queryIsoTime = queryTime.isoformat().replace("+00:00", "Z")
        url = "https://gamma-api.polymarket.com/markets?start_date_min="+queryIsoTime+"&closed=false&limit=500"
        markets = requests.get(url).json()
        fields = database_connector.getFieldNames()
        field_names = list(map(lambda x: x[1], fields))
        market_dict = defaultdict(list)
        if "error" in markets:
            logging.error("Polymarket Gamma API Error: " + str(markets))
        filtered_data = [(obj, s) for obj in markets for s in field_names 
                        if s in obj['description']
                        and "game" in obj['description']
                        and "win" in obj['description']
                        ]
        print (url)
        for obj, name in filtered_data:
            if name != "NBA":
                continue
            field_id = next(tup[0] for tup in fields if tup[1] == name)
            outcomes = json.loads(obj['outcomes'])
            home = outcomes[1]
            away = outcomes[0]
            prices = json.loads(obj['outcomePrices'])
            print(prices)
            homeOdds = round((1.0/(float(prices[1]))), 2)
            awayOdds = round(1.0/(float(prices[0])), 2)
            print (home, away, homeOdds, awayOdds)
            events = database_connector.getEvents(home, away, field_id, queryTime)
            event_id = None
            if events is not None and len(events) > 0:
                event_id = database_connector.getEvents(home, away, field_id, queryTime)[0].event_id
            if event_id is None:
                logging.info("No event found for " + home + " vs " + away + " in field: " + name)
                continue
                
            database_connector.addOrUpdateMarket(
                schemas.Market(
                    event_id=event_id,
                    bookmaker_key=self.bookmaker,
                    bet_type_id=BetType.h2h,
                    description="",
                    outcomes=[schemas.Outcome(name=home, price=homeOdds),
                              schemas.Outcome(name=away, price=awayOdds)]))
            database_connector.addBookmakerEvent(event_id=event_id, bookmaker_key=self.bookmaker, event_url=obj['id'])
            market_dict[field_id].append(obj)
        market_dict = dict