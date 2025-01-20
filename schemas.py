from __future__ import annotations
from datetime import datetime
from enum import Enum, IntEnum
from typing import List, Union

from pydantic import BaseModel

class Outcome(BaseModel):
    name: str = ''
    description: str = ''
    price: float
    point: Union[float, None] = None
    outcome_bookmaker_id : str | None = None

class Market(BaseModel):
    market_id: Union[int, None] = None
    event_id: str
    bookmaker_key: str
    bet_type_id: int
    last_update: datetime = datetime.now()
    description: Union[str, None] = None
    outcomes: list[Outcome]
    market_bookmaker_id : str | None = None

class Bookmaker(BaseModel):
    key: str = ''
    title: str = ''
    markets: List[Market]

class Event(BaseModel):
    event_id: Union[str, None] = None
    category_id: int = 1
    commence_time: datetime = datetime.now()
    description: str = ''
    home: int
    away: int

class category(BaseModel):
    category_id: Union[int, None] = None
    category_group_id: int = 1
    category_name: str = ''

class EventPageData(BaseModel):
    event_id: str
    bookmaker: str
    event_url: str
    oghome: str
    ogaway: str

class BetType(IntEnum):
    h2h = 1
    spreads = 2
    totals = 3
    outrights = 4
    asian_spreads = 5
    misc = 6

class BetInfoType(IntEnum):
    arbs = 1
    evs = 2

class BetInfoParameters(BaseModel):
    event: Event | None = None
    names: list[str] | None = None
    bet_type: BetType = BetType.h2h
    point: float | None = None
    description: str | None = None
    category_id : int | None = None
    user_id: str | None = None
    sharp_book: str | None = None

class PositiveEVBet(BaseModel):
    outcome : Outcome
    event : Event
    description : str | None
    bet_type : BetType
    book : str
    edge : float
    fair_odds : float
    kelly_criterion : float

class BookmakerScanParameters(BaseModel):
    link : str | None = None
    category : str | None = None
    sport : str | None = None
    categories_only : bool = False
