from __future__ import annotations
import configparser
from datetime import datetime
import hashlib
import inspect
import logging
import time
from typing import List, Optional, Tuple, Union

import psycopg2

from psycopg2 import sql
from psycopg2 import pool
from psycopg2.extensions import connection
import pytz
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import spacy


import schemas

config = configparser.ConfigParser()
config.read('db_config.ini')

# Extract the connection details
db_params = config['postgresql']

nlp = spacy.load("en_core_web_sm")

def connectDb():
    db_params = config['postgresql']
    global pool
    pool = pool.ThreadedConnectionPool( minconn=1,
                    maxconn=20,
                    user=db_params['user'],
                    password=db_params['password'],
                    host=db_params['host'],
                    port=db_params['port'],
                    database=db_params['database'])


def get_connection() -> connection:
    while True:
        try:
            conn =  pool.getconn()
            return conn
        except : 
            pass
            #logging.error(e)
        time.sleep(1)

def release_connection(conn : connection):
    caller = inspect.stack()[1].function
    pool.putconn(conn)

def disconnectDb():
    pool.closeall()

def get_closest_match(search_term : str, input_strings : list[str]):
    input_team_doc = nlp(search_term)
    similarities = [(name, input_team_doc.similarity(nlp(name))) for name in input_strings]
    return max(similarities, key=lambda x: x[1])

# TODO: implement RESTful api using flask for all the get methods
# use api keys
def getCategories() -> list[int]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT distinct category_id FROM bookmaker_categories;")
    rows = cur.fetchall()
    cur.close()
    conn.commit()
    release_connection(conn)

    return [int(row[0]) for row in rows]

def getCategoryNames():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT text FROM bookmaker_categories WHERE category_id IS NOT NULL;")
    rows = cur.fetchall()
    cur.close()
    release_connection(conn)
    return rows

def getUnmatchedCategories():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT bookmaker_category_id, text, bookmaker_key FROM bookmaker_categories WHERE category_id IS NULL;")
    rows = cur.fetchall()
    cur.close()
    release_connection(conn)
    return rows

def getEventById(event_id : str) -> schemas.Event | None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * 
        FROM events e 
        WHERE e.event_id = %s
        """,
        (event_id,),
    )
    row = cur.fetchone()
    release_connection(conn)
    if row is not None:
        return eventFromDbRow(row)
    return None

def getEvents(home : Optional[str] = None, away : Optional[str] = None, category_id : Optional[int] = None, dt: Optional[datetime] = None) ->  List[schemas.Event]:  # eg. games, elections
    conn = get_connection()
    cur = conn.cursor()
    afterTime = (dt if (dt is not None) else 
                datetime.now(pytz.utc))
    params = []
    query = """
        SELECT * 
        FROM events e
        WHERE e.commence_time > %s
        """
    params.append(afterTime)

    if home is not None:
        query += " AND position(%s in e.home)>0"
        params.append(home)
    if away is not None:
        query += " AND position(%s in e.away)>0"
        params.append(away)
    if category_id is not None:
        query += " AND e.category_id = %s"
        params.append(category_id)
    cur.execute(
        query,
        params
    )
    rows = cur.fetchall()
    events = []
    for row in rows:
        events.append(eventFromDbRow(row))
    conn.commit()
    cur.close()
    release_connection(conn)
    return events


def getTopOddsInfo(limit = 25, eventId : Optional[str] = None, dt: Optional[datetime] = None) :  # eg. games, elections
    conn = get_connection()
    cur = conn.cursor()
   # afterTime = (dt if (dt != None) else datetime.now).isoformat()
    cur.execute(
    """
    SELECT 
    f.category_name, 
    e.home, 
    e.away, 
    bt.key,
    o.price, 
    o.point,
    o.name
    FROM 
    outcomes o
    JOIN 
    markets m ON o.market_id = m.market_id
    JOIN 
    events e ON m.event_id = e.event_id
    JOIN 
    categories f ON e.category_id = f.category_id
    JOIN bet_types bt ON m.bet_type_id = bt.bet_type_id
    WHERE m.bet_type_id IN (1, 5) AND e.commence_time > NOW()
    ORDER BY 
    o.price DESC
    LIMIT %s;
    """, (limit,)
    )
    rows = cur.fetchall()
    conn.commit()
    cur.close()
    release_connection(conn)
    return rows


def addOrUpdateMarket(market: schemas.Market) -> int:  # TODO: decide how to do ids
    logging.info(market)
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT  market_id FROM markets
        WHERE event_id = %s AND
        bookmaker_key = %s AND
        coalesce(description, '') = %s AND
        bet_type_id = %s;
        """,
        (
            market.event_id,
            market.bookmaker_key,
            market.description or "",
            market.bet_type_id,
        ),
    )
    row = cur.fetchone()
    id = row[0] if row is not None else None
    if id is None:
        id = addMarket(market, conn)
    
    updateMarketOutcomes(id, market.outcomes)
    conn.commit()
    cur.close()
    release_connection(conn)
    return id

def getMarketPoints(event : schemas.Event, betType : int, name : str | None = None, description : str = "") -> list[float]:
    conn = get_connection()
    cur = conn.cursor()
    params = [event.event_id, betType, description]
    query = """
        SELECT DISTINCT point FROM outcomes o
        JOIN markets m ON o.market_id = m.market_id
        AND m.event_id = %s
        AND m.bet_type_id = %s
        AND NOT (m.description IS DISTINCT FROM %s)
        WHERE o.point IS NOT NULL
        """
    if name != None and name != "":
        query += "AND o.name = %s"
        params.append(name)

    cur.execute(
        query,
        params
    )
    rows = cur.fetchall()
    conn.commit()
    cur.close()
    release_connection(conn)
    return [float(x[0]) for x in rows]

def getMarketOutcomeNames(event : schemas.Event, betType : int, description : str = "") -> list[str]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT name FROM outcomes o
        JOIN markets m ON o.market_id = m.market_id
        AND m.event_id = %s
        AND m.bet_type_id = %s
        """,
        (event.event_id, betType)
    )
    rows = cur.fetchall()
    conn.commit()
    cur.close()
    release_connection(conn)
    return [str(row[0]) for row in rows]

def getMarketVariants(event : schemas.Event, bet_type : int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT m.description 
        FROM outcomes o
        JOIN markets m ON o.market_id = m.market_id
        AND m.bet_type_id = 3
        AND m.description <> ''
        AND m.event_id = %s
        """,
        (event.event_id,)
    )
    rows = cur.fetchall()
    conn.commit()
    cur.close()
    release_connection(conn)
    return [None] + [str(row[0]) for row in rows]

def addBookmakerEvent(event_id: str, bookmaker_key: str, event_url : str | None, oghome : str, ogaway : str):
    conn = get_connection()
    cur = conn.cursor()
# Define the query string with named parameters
    query = """
        INSERT INTO event_urls (event_id, bookmaker_key, event_url, oghome, ogaway)
        VALUES (%(event_id)s, %(bookmaker_key)s, %(event_url)s, %(oghome)s, %(ogaway)s)
        ON CONFLICT (event_id, bookmaker_key) DO UPDATE SET event_url = %(event_url)s;
    """

    # Define the parameters as a dictionary
    params = {
        "event_id": event_id,
        "bookmaker_key": bookmaker_key,
        "event_url": event_url,
        "oghome": oghome,
        "ogaway": ogaway,
    }

    # Execute the query with the parameters
    cur.execute(query, params)

    conn.commit()
    cur.close()
    release_connection(conn)


def getEventUrl(event_id: str, bookmaker_key: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """SELECT event_url FROM event_urls WHERE event_id = %s AND bookmaker_key = %s""",
        (event_id, bookmaker_key),
    )
    conn.commit()
    row = cur.fetchone()
    cur.close()
    release_connection(conn)
    if row is not None:
        return str(row[0])
    return None

def getEventPageData(event_id: str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """SELECT bookmaker_key, event_url, oghome, ogaway FROM event_urls WHERE event_id = %s""",
        (event_id,),
    )
    conn.commit()
    rows = cur.fetchall()
    cur.close()
    release_connection(conn)
    data = [tupleToEventPageData(row, event_id) for row in rows]
    return data

def getEventFromUrl(url : str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """SELECT event_id, bookmaker_key FROM event_urls WHERE event_url = %s""",
        (url),
    )
    conn.commit()
    row = cur.fetchone()
    cur.close()
    release_connection(conn)
    if row is not None:
        return (row[0], row[1])
    return None, None

def addMarket(market: schemas.Market, connection : connection | None) -> str:
    conn = connection or get_connection()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO markets (event_id, bookmaker_key, bet_type_id, description, last_update, market_bookmaker_id)
            VALUES(%s, %s, %s, %s, %s, %s)
            RETURNING market_id;""",
        (
            market.event_id,
            market.bookmaker_key,
            market.bet_type_id,
            market.description,
            market.last_update,
            market.market_bookmaker_id,
        ),
    )
    eventId = cur.fetchone()[0]
    conn.commit()
    cur.close()
    if connection is None:
        release_connection(conn)
    return eventId

def eventFromDbRow(row: tuple):
    return schemas.Event(
        event_id=row[0],
        category_id=row[1],
        commence_time=row[2],
        description=row[3],
        home=row[4],
        away=row[5],
    )

def marketFromDbRow(row: tuple):
    return schemas.Market(
        market_id=row[0],
        event_id=row[1],
        bookmaker_key=row[2],
        bet_type_id=row[3],
        last_update=row[4],
    )

def addEvent(event: schemas.Event, connection : connection | None) -> str:
    conn = connection or get_connection()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO events (event_id, category_id, commence_time, description, home, away)
        VALUES(gen_random_uuid (), %s, %s, %s, %s, %s) 
        RETURNING event_id;""",
        (
            event.category_id,
            event.commence_time,
            event.description,
            event.home,
            event.away,
        ),
    )
    eventId = cur.fetchone()[0]
    conn.commit()
    cur.close()
    if connection is None:
        release_connection(conn)
    return eventId

def addBookmakerCategory(category: schemas.category | str, bookmaker : str, bookmaker_category_key : str | None) -> int:
    category_row = None
    if type(category) is str:
        category_row = schemas.category(category_group_id=1, category_name=category)
    elif type(category) is schemas.category:
        category_row = schemas.category(category)
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO bookmaker_categories (text, category_id, bookmaker_key, category_bookmaker_key)
        VALUES(%s, %s, %s, %s)
        RETURNING bookmaker_category_id;""",
        (
            category_row.category_name,
            None,
            bookmaker,
            bookmaker_category_key,
        ),
    )
    conn.commit()
    cur.close()
    release_connection(conn)
    return

def addCategory(category: schemas.category | str, bookmaker_category_id : int | None = None) -> int:
    category_row = None
    if type(category) is str:
        category_row = schemas.category(category_group_id=1, category_name=category)
    elif type(category) is schemas.category:
        category_row = category
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO categories (category_group_id, category_name)
        VALUES(%s, %s)
        RETURNING category_id;""",
        (
            category_row.category_group_id,
            category_row.category_name,
        ),
    )
    id = cur.fetchone()[0]
    cur.execute(
        """
        UPDATE bookmaker_categories
        SET category_id = %s
        WHERE bookmaker_category_id = %s
        """,
        (int(id), bookmaker_category_id),
    )
    conn.commit()
    cur.close()
    release_connection(conn)
    return id

def associateCategory(bookmaker_category_id : int, category_id : int):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE bookmaker_categories
        SET category_id = %s
        WHERE bookmaker_category_id = %s
        """,
        (category_id, bookmaker_category_id),
    )
    conn.commit()
    cur.close()
    release_connection(conn)

def searchOrAddCategory(search_term: str, bookmaker : str, api_id : str | None) -> int:
    id = searchCategoryId(search_term, api_id)
    if id is None:
        addBookmakerCategory(search_term, bookmaker, api_id)
    return id

def searchCategoryId(slug: str, api_id : str | None = None) -> int | None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT f.category_id, f.category_group_id, f.category_name
        FROM categories f
        JOIN bookmaker_categories fd ON fd.category_id = f.category_id
        WHERE fd.text = %s;
    """,
        (slug,),
    )
    rows = cur.fetchone()
    conn.commit()
    cur.close()
    release_connection(conn)
    if rows is None:
        return None
    return int(rows[0])

def getBookmakerMarkets(bookmaker : str) -> list[tuple[int, str]]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT market_id, market_bookmaker_id FROM markets
        WHERE bookmaker_key = %s;
        """,
        (bookmaker,),
    )
    rows = cur.fetchall()
    conn.commit()
    cur.close()
    release_connection(conn)
    return [(int(row[0]), row[1]) for row in rows]

def getBookmakerOutcomeIds(bookmaker : str) -> list[tuple[int, str]]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT outcome_id, bookmaker_outcome_id FROM outcomes o
        JOIN markets m ON o.market_id = m.market_id
        WHERE m.bookmaker_key = %s;
        """,
        (bookmaker,),
    )
    rows = cur.fetchall()
    conn.commit()
    cur.close()
    release_connection(conn)
    return [(int(row[0]), row[1]) for row in rows]

def getTeamsInCategory(category_id: int) -> list[str]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT ON (team_id) text
        FROM team_dict
        WHERE category_id = %s
        """,
        (category_id,),
    )
    rows = cur.fetchall()
    conn.commit()
    cur.close()
    release_connection(conn)
    teams = []
    return [str(team[0]) for team in rows]

def searchTeamId(search_term : str, category_id : int) -> int | None :
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT team_id FROM team_dict
        WHERE text = %s
        AND category_id = %s;
        """,
        (search_term, category_id),
    )
    rows = cur.fetchone()
    conn.commit()
    cur.close()
    release_connection(conn)
    return int(rows[0]) if rows is not None and rows[0] is not None else None

def getTeamName(team_id : int) -> str:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT text FROM team_dict
        WHERE team_id = %s;
        """,
        (team_id,),
    )
    rows = cur.fetchone()
    conn.commit()
    cur.close()
    release_connection(conn)
    return str(rows[0]) if rows is not None and rows[0] is not None else None

def getBetTypeName(bet_type_id : int) -> str:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT key FROM bet_types
        WHERE bet_type_id = %s;
        """,
        (bet_type_id,),
    )
    rows = cur.fetchone()
    conn.commit()
    cur.close()
    release_connection(conn)
    return str(rows[0]) if rows is not None and rows[0] is not None else None

def searchSimilarCategoryId(search_term : str) -> tuple[int, str] | None :
    categories = getCategoryNames()
    if len(categories) == 0:
        return None
    result = process.extractOne(search_term, categories)
    if result[1] > 80:
        match = searchCategoryId(result[0])
        return (match, result[0])
    return None


def searchSimilarTeamId(search_term : str, category_id : int) -> int | None :
    teams = getTeamsInCategory(category_id)
    if len(teams) == 0:
        return None
    result = get_closest_match(search_term, teams)
    if result[1] > 0.98:
        print(search_term, result)
        match = searchTeamId(result[0], category_id)
        id = addTeam(search_term, category_id, match)
        if id is None:
            return searchTeamId(search_term, category_id) # team has been added while trying to add it
        return match
    return None

def addTeam(name : str, category_id : int, team_id : int = None) -> int | None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO team_dict (text, category_id)
        VALUES(%s, %s)
        ON CONFLICT (text, category_id) DO NOTHING
        RETURNING team_id;
        """,
        (name, category_id),
    )
    row = cur.fetchone()
    if row is None:
        return None
    logging.info(f"Added team name {name} with id {team_id}")
    conn.commit()
    cur.close()
    release_connection(conn)
    return team_id
    

def getNextTeamId():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT MAX(team_id) FROM team_dict;
        """,
    )
    rows = cur.fetchone()
    conn.commit()
    cur.close()
    release_connection(conn)
    return int(rows[0])+1 if rows[0] is not None else 0

def searchOrAddTeam(name : str, category_id : int) -> int:
    id = searchTeamId(name, category_id)
    if id is None:
        id = searchSimilarTeamId(name, category_id)
    if id  is None:
        id = addTeam(name, category_id)
        if id is None:
            id = searchTeamId(name, category_id)
    return id


def searchBetTypeId(search_term : str) -> int:  # TODO: finish
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT bet_type_id FROM bet_type_dict
        WHERE text = %s;
        """,
        (search_term,),
    )
    rows = cur.fetchall()
    id = rows[0][0] if len(rows) != 0 and rows[0] is not None else None
    if id is None:
        id = 6
    conn.commit()
    cur.close()
    release_connection(conn)
    return id


def searchOrAddEvent(home : str, away : str, category: int, gameDateTime: datetime) -> str:
    home_id = searchOrAddTeam(home, category)
    away_id = searchOrAddTeam(away, category)
    id = None
    if home_id is None or away_id is None:
        logging.error("Exception when looking up teams for event {home} vs {away}")
        return ""

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT event_id FROM events
        WHERE category_id = %s AND
        commence_time::date = %s AND
        home = %s AND
        away = %s;
    """,
        (
            category,
            gameDateTime.date(),
            home_id,
            away_id,
        ),
    )
    row = cur.fetchone()
    id = row[0] if row is not None else None
    print(home, away, id, category, gameDateTime.date(), gameDateTime.isoformat(), home_id, away_id)
    if id is None:
        id = addEvent(
            schemas.Event(category_id=category, commence_time=gameDateTime, home=home_id, away=away_id), conn
        )
    conn.commit()
    cur.close()
    release_connection(conn)
    return id


def removecategoryDuplicates():
    """Remove duplicate categories in the database based on synonyms found in the category dictionary
    """
    print("todo")


def updateMarketOutcomes(market_id, outcomes: List[schemas.Outcome]):
    conn = get_connection()
    cur = conn.cursor()
    tuples = list(map(lambda outcome: outcomeToTuple(outcome, market_id), outcomes))
    cur.executemany(
        """
        INSERT INTO outcomes (name, description, market_id, price, point, outcome_bookmaker_id) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (name, market_id, point) DO UPDATE SET (price) = ROW(EXCLUDED.price);
        """,
        tuples,
    )
    cur.execute(
        """
        UPDATE markets SET last_update = %s WHERE market_id = %s
        """,
        (datetime.now(pytz.utc), market_id),
    )
    conn.commit()
    cur.close()
    release_connection(conn)

def updateOddsByOutcomeId(update : dict[int, float]):
    conn = get_connection()
    cur = conn.cursor()
    cur.executemany(
        """
        UPDATE outcomes SET price = %s WHERE outcome_id = %s;
        """,
        update.items(),
    )
    conn.commit()
    cur.close()
    release_connection(conn)

def outcomeToTuple(outcome : schemas.Outcome, marketId):
    return (
        outcome.name,
        outcome.description,
        marketId,
        outcome.price,
        outcome.point,
        outcome.outcome_bookmaker_id,
    )

def tupleToEventPageData(row : tuple, event_id : str) -> schemas.EventPageData: 
    return schemas.EventPageData(
        event_id=event_id,
        bookmaker=row[0],
        event_url=row[1],
        oghome=row[2],
        ogaway=row[3],
    )


def saveTelegramChatId(userName, chatId):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
            INSERT INTO users (user_name, telegram_chat_id) VALUES (%s, %s)
            ON CONFLICT (user_name, telegram_chat_id) DO UPDATE SET telegram_chat_id = %s;""",
        (
            userName,
            chatId,
            chatId,
        ),
    )

    conn.commit()
    cur.close()
    release_connection(conn)


def getAdmins():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
            SELECT user_name, telegram_chat_id
            FROM users WHERE is_admin = True;""",
    )
    rows = cur.fetchall()
    conn.commit()
    cur.close()
    release_connection(conn)
    return rows

def printUsers():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM users;
        """,
    )
    print(cur.fetchall())
    release_connection(conn)

def searchUserId(username : str):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT user_id
        FROM users
        WHERE user_name = %s;
        """,
        (username,),
    )
    rows = cur.fetchone()
    conn.commit()
    cur.close()
    release_connection(conn)
    return rows[0] if rows is not None else None

def materialized_view_exists(cursor, view_name):
    cursor.execute(sql.SQL("SELECT 1 FROM pg_matviews WHERE matviewname = %s"), [view_name])
    return cursor.fetchone() is not None

def refresh_materialized_view(cursor, view_name):
    cursor.execute(sql.SQL("REFRESH MATERIALIZED VIEW {}").format(sql.Identifier(view_name)))
    
def prepareUniversalOutcomeTopOdds():
    conn = get_connection()
    cur = conn.cursor()
    if materialized_view_exists(cur, "universal_outcomes"):
        refresh_materialized_view(cur, "universal_outcomes")
    else:
        query = """
            CREATE MATERIALIZED VIEW universal_outcomes AS
            SELECT md5(concat(o1.name, m1.description, o1.point, m1.bet_type_id, m1.event_id)) AS composite_hash,
            o1.name, o1.description,  o1.market_id, o1.price, o1.point, 
            m1.bookmaker_key, m1.description AS market_description, m1.event_id, m1.bet_type_id
            FROM outcomes o1
            JOIN markets m1 ON o1.market_id = m1.market_id;
            CREATE INDEX idx_universal_outcomes_hash ON universal_outcomes (composite_hash); 
        """
        cur.execute(query)
        conn.commit()
    if materialized_view_exists(cur, "top_universal_outcomes"):
        refresh_materialized_view(cur, "top_universal_outcomes")
    else:
        query = """
            CREATE MATERIALIZED VIEW top_universal_outcomes AS
            SELECT tuo.* FROM universal_outcomes tuo
            JOIN (
                SELECT composite_hash, MAX(price) as maxprice
                FROM universal_outcomes
                GROUP BY composite_hash
            ) top 
            ON tuo.composite_hash = top.composite_hash and tuo.price = top.maxprice;
            CREATE INDEX idx__top_universal_outcomes_hash ON universal_outcomes (composite_hash); 
        """
        cur.execute(query)
        conn.commit()
    cur.close()
    release_connection(conn)

def getUniversalOutcomeTopOdds(event : schemas.Event,
                            bet_type_id : int,
                            name : str = "",
                            user_id : Optional[str] = None,
                            point : Optional[float] = None,
                            description : Optional[str] = None
                            ): 
    conn = get_connection()
    cur = conn.cursor() 
    point_str = (str(int(point)) if point.is_integer() else str(point)) if point is not None else ''
    concatenated_string = f"{name or ''}{description or ''}{point_str}{bet_type_id}{event.event_id or ''}"
    md5_hash = hashlib.md5(concatenated_string.encode('utf-8')).hexdigest()
    query = """
        SELECT tuo.name, tuo.description, tuo.market_id, tuo.price, tuo.point, tuo.bookmaker_key
        FROM top_universal_outcomes tuo
        WHERE tuo.composite_hash = %s
        LIMIT 1
    """
    params = (md5_hash,)
    """
        params = {"user_id": user_id,
            "bet_type_id": int(bet_type_id),
            "description": description,
            "name": name,
            "point": point,
            "event_id": event.event_id}
    """

    cur.execute(
        query, params
    )
    #print(query, params)
    row = cur.fetchone()
    bookmaker = row[5] if row is not None else None
    cur.close()
    release_connection(conn)
    return tupleToOutcome(row) if row is not None else None, bookmaker



def getBookmakerOutcome(event : schemas.Event,
                        bookmaker: str,
                        bet_type_id : int,
                        name : str = "",
                        point : Optional[float] = None,
                        description : Optional[str] = None
                        ):
    conn = get_connection()
    cur = conn.cursor()
    query = """
        SELECT o.name, o.description, o.market_id, o.price, o.point FROM outcomes o
        JOIN markets m ON o.market_id = m.market_id
        AND m.event_id = %(event_id)s
        AND m.bet_type_id = %(bet_type_id)s
        AND o.name = %(name)s
        AND m.bookmaker_key = %(bookmaker_key)s
        AND NOT(o.point IS DISTINCT FROM %(point)s)
        AND NOT(m.description IS DISTINCT FROM %(description)s)
        LIMIT 1
        """
    params = {
            "bet_type_id": int(bet_type_id),
            "description": description,
            "name": name,
            "point": point,
            "event_id": event.event_id,
            "bookmaker_key": bookmaker}
    cur.execute(
        query, params
    )
    row = cur.fetchone()
    cur.close()
    release_connection(conn)
    return tupleToOutcome(row) if row is not None else None

def getUniversalMarket(event : schemas.Event,
                        bet_type_id : int,
                        user_id : Optional[str] = None): #TODO: finish this
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT * FROM outcomes o
        JOIN markets m ON m.market_id = o.market_id
        WHERE m.event_id = {event.event_id}
        AND m.bet_type_id = {bet_type_id}
        """
    )
    rows = cur.fetchall()
    cur.close()
    release_connection(conn)
    return rows

def getTopOdds(categories=Union[List[str], None], amount=20):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM outcomes
        ORDER BY price DESC
        LIMIT %s; 
    """,
        (amount,),
    )
    cur.close()
    release_connection(conn)
    outcomes = list(map(tupleToOutcome, cur.fetchall()))
    return outcomes


def tupleToOutcome(tuple):
    return schemas.Outcome(
        name=tuple[0],
        description=tuple[1],
        market_id=tuple[2],
        price=tuple[3],
        point=tuple[4],
    )
