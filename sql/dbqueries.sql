SELECT MAX(price) 
FROM outcomes o2
JOIN markets m2 ON m2.market_id = o2.market_id
JOIN events e ON m2.event_id = 'bea90ca8-8dd4-4a5c-82d1-f60bbe1ae1b2'
AND m2.bet_type_id = 1
LIMIT 1;


SELECT o.name, o.price, m.bookmaker_key FROM outcomes o
JOIN events e ON e.home = 'West Ham'
JOIN markets m on m.event_id = e.event_id
WHERE o.market_id = m.market_id;

SELECT m.bookmaker_key, m.market_id from markets m
JOIN events e ON e.home = 'West Ham'
WHERE m.event_id = e.event_id;

SELECT DISTINCT concat(e.home, e.away) FROM events e
WHERE e.field_key = %s;

SELECT td.name FROM team_dict td
WHERE  td.team_id = %s
ORDER BY td.team_id
LIMIT 1;

UPDATE field_dictionary SET field_id = 292 WHERE text = 'La Liga';