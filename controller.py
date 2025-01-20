from __future__ import annotations

import datetime
import logging
import re
from typing import Any, Optional

import pytz
from lxml import etree, html
from nodriver import Element

import database_connector
import schemas
from schemas import BetInfoParameters, BetInfoType, BetType, Event, Outcome, PositiveEVBet
from wrappers.coolbet import CoolbetWrapper
from wrappers.coolbetV2 import CoolbetWrapperV2
from wrappers.pinnacle import PinnacleWrapper
from wrappers.polymarket import PolymarketWrapper
from wrappers.veikkaus import VeikkausWrapper

wrapperDict = {
        "veikkaus" : VeikkausWrapper,
        "coolbet" : CoolbetWrapper,
        "coolbetv2" : CoolbetWrapperV2,
        "polymarket" : PolymarketWrapper,
        "pinnacle" : PinnacleWrapper,
    }

def findInfo(info_type : BetInfoType, info_params : BetInfoParameters):
    ip = info_params
    dt = datetime.datetime.now(pytz.utc) - datetime.timedelta(hours=1)
    events = None
    if info_params.event is not None:
        events = [info_params.event]
    else:
        events = database_connector.getEvents(category_id=ip.category_id, dt = dt)
    outcome_combinations : tuple[Event, list[Outcome, str], float] = []
    results : list[PositiveEVBet] = []
    for event in events:
        names = database_connector.getMarketOutcomeNames(event, ip.bet_type)
        variants = database_connector.getMarketVariants(event, bet_type=ip.bet_type) or []
        for variant in variants:
            if ip.bet_type == BetType.totals:
                points = list[float]
                points = database_connector.getMarketPoints(event, ip.bet_type, description=variant)
                for point in points:
                    if type(point) is not float:
                        logging.error("point is not a float")
                        return
                    params = info_params.model_copy(update={
                        "event" : event,
                        "names" : names,
                        "description" : variant,
                        "point" : point
                    })
                    results.extend(findInfoForEvent(info_type, params, outcome_combinations))
            if ip.bet_type in [BetType.spreads, BetType.asian_spreads]:
                if ip.description is None:
                    points : list[float] = []
                    points = database_connector.getMarketPoints(event, ip.bet_type, name="home")
                    for point in points:
                        if type(point) is not float:
                            logging.error("point is not a float")
                            return
                        params = info_params.model_copy(update={
                            "event" : event,
                            "names" : names,
                            "description" : variant,
                            "point" : point,
                        })
                        results.extend(findInfoForEvent(info_type, params, outcome_combinations))
            elif ip.bet_type == BetType.h2h:
                params = info_params.model_copy(update={
                            "event" : event,
                            "names" : names,
                            "description" : variant,
                            "point" : None,
                        })
                results.extend(findInfoForEvent(info_type, params, outcome_combinations))
        # logging.info("outcome combinations for event:")
        if len(results) > 0:
            for bet in results:
                commence_time = bet.event.commence_time.replace(tzinfo=pytz.utc)
                now = datetime.datetime.now(pytz.utc)
                future_limit = now + datetime.timedelta(hours=24)
                home_str = database_connector.getTeamName(bet.event.home)
                away_str = database_connector.getTeamName(bet.event.away)
                bet_type_str = database_connector.getBetTypeName(bet.bet_type)
                print(f"Event: {home_str} vs {away_str} market: {bet_type_str} {ip.description or ''} category: {bet.event.category_id}")
                print(f"Time: {commence_time}")
                print(f"{bet.outcome.name} ({bet.outcome.point or ''!s}) at {round(bet.outcome.price,3)!s} from {bet.book}")
                edge = round((bet.edge - 1)*100, 2)
                print(f"Edge: {edge!s}% Fair Odds: {round(bet.fair_odds, 2)!s}")
                print(f"Kelly Criterion: {round(bet.kelly_criterion, 3)!s}\n")

        if len(outcome_combinations) > 0:
            print("Arbs found: ", len(outcome_combinations))
            for outcome_combination in outcome_combinations:
                event = outcome_combination[0]
                outcomes = outcome_combination[1]
                probability = outcome_combination[2]
                home_str = database_connector.getTeamName(event.home)
                away_str = database_connector.getTeamName(event.away)
                bet_type_str = database_connector.getBetTypeName(ip.bet_type)
                print(f"Event: {home_str} vs {away_str} market: {bet_type_str} {ip.description or ''}")
                for (i,name) in enumerate(names):
                    print(f"({str(outcomes[i][0].point or '')}) {name} {str(outcomes[i][0].price)} from {outcomes[i][1]}")
                print("Total: " + str(probability) + "\n")
        outcome_combinations = []
    
def findInfoForEvent(info_type : BetInfoType, info_params : BetInfoParameters, outcome_combinations : list | None) -> list[PositiveEVBet] | None:
    if info_type == BetInfoType.arbs:
        findArbsForEvent(info_params, outcome_combinations)
    elif info_type == BetInfoType.evs:
        return findPositiveEVForEvent(info_params)
    return None

def findArbsForEvent(info_params : BetInfoParameters, outcome_combinations : list):
    ip = info_params
    outcomes : list[schemas.Outcome, str] = []
    probability : float = 0
    if len(ip.names) == 0:
        logging.debug("names is empty, event lacks data for a market")
        return
    for name in ip.names:
        outcome_point = ip.point
        outcome, book = database_connector.getUniversalOutcomeTopOdds(ip.event, ip.bet_type, name, ip.user_id, outcome_point, ip.description)
        if outcome is None or book is None:
            log = f"odds not found for event with id {ip.event.event_id} for bet type {ip.bet_type} description: {ip.description} point: {ip.point} name: {name}"
            logging.debug(log)
            return
        probability += 1./outcome.price
        outcomes.append((outcome, book))
    if probability < 1.0:
        outcome_combinations.append((ip.event, outcomes, probability))
    
def findPositiveEVForEvent(info_params : BetInfoParameters):
    ip = info_params
    fair_odds = getFairOddsForEvent(ip.event, ip.sharp_book, ip.names, ip.bet_type, ip.point, ip.description)
    if fair_odds is None:
        return []
    top_bets : list[schemas.PositiveEVBet] = []
    if len(ip.names) == 0:
        logging.debug("names is empty, event lacks data for a market")
        return []
    for index, name in enumerate(ip.names):
        outcome_point = ip.point
        #if (ip.bet_type in [BetType.spreads, BetType.asian_spreads]) and name == "away":
            #outcome_point = -outcome_point
        outcome, book = database_connector.getUniversalOutcomeTopOdds(ip.event, ip.bet_type, name, ip.user_id, outcome_point, ip.description)
        #if ip.bet_type == BetType.totals:
            #print(book)
        if outcome is None or book is None:
            log = f"odds not found for event with id {ip.event.event_id} for bet type {ip.bet_type} description: {ip.description} point: {ip.point} name: {name}"
            logging.info(log)
            return []
        edge = outcome.price / fair_odds[index].price

        odds = fair_odds[index].price
        kelly_criterion = getKellyCriterion(fair_odds[index].price, odds)

        if edge > 1.01 and round(kelly_criterion, 3) > 0.005:#and odds < 3.0
            top_bets.append(schemas.PositiveEVBet(outcome=outcome, event=ip.event, description=ip.description, bet_type = ip.bet_type,book=book, \
                                                fair_odds=odds, edge=edge, kelly_criterion=kelly_criterion))


    return top_bets

def getKellyCriterion(odds : float, fair_odds : float, fraction = 0.25) -> float:
    p = 1./fair_odds
    q = 1. - p
    b = odds
    return (p - q/b)*fraction

def getFairOddsForEvent(event : schemas.Event, sharp_book : str, names : list[str], betType : BetType = BetType.h2h, point : float | None = None, \
                        description : str | None = None) ->  list[schemas.Outcome] | None:
    if len(names) == 0:
        logging.debug("names is empty, event lacks data for a market")
        return
    outcomes : list[schemas.Outcome] = []
    probability : float = 0
    for name in names:
        outcome_point = point
        #if (betType in [BetType.spreads, BetType.asian_spreads]) and name == "away":
            #outcome_point = -point 
        outcome = database_connector.getBookmakerOutcome(event, sharp_book, betType, name, outcome_point, description)
        if outcome is None:
            home = database_connector.getTeamName(event.home)
            away = database_connector.getTeamName(event.away)
            logging.debug(f"Fair odds search error for {home} vs {away}")
            return None
        probability += 1./outcome.price
        outcomes.append(outcome)
    for outcome in outcomes:
        if outcome is not None:
            outcome.price = 1/((1./outcome.price) / probability) # this removes the "vig"
    return outcomes
        
def verifyElement(variable : Any | Element):
    if isinstance(variable, Element):
        return variable
    else:
        raise TypeError("Invalid element") 
    
def get_text_by_xpath(tree: html.HtmlElement, xpath_query: str) -> Optional[str]:
    """
    Perform the XPath query and return the text content of the first matched element.
    Return None if no elements are found.
    """
    elements = tree.xpath(xpath_query)
    if elements:
        return elements[0].text_content()
    else:
        return None
    
def print_tree(tree: html.HtmlElement):
    html_string = etree.tostring(tree, pretty_print=True).decode()
    print(html_string)

def translateTeam(team : str):
    return team

def replaceTeamNames(text : str, oghome : str, ogaway : str):
    text = re.sub(re.escape(oghome), "{home}", text, flags=re.IGNORECASE)
    text = re.sub(re.escape(ogaway), "{away}", text, flags=re.IGNORECASE)
    return text

def american_to_decimal(american_odds):
    dec = 0
    if american_odds > 0:
        dec = (american_odds / 100) + 1
    elif american_odds < 0:
        dec = (100 / abs(american_odds)) + 1
    else:
        return 1  # Neutral odds
    return round(dec, 3)