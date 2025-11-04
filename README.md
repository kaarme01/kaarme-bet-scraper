# Käärme's betting scraper 

This project is a set of scripts and a design for a database for scraping multiple sportsbooks and
structuring and analyzing the information.

The aim of the project is to create a framework with similar functionality to sites like oddsjam 
and surebet.com. 

While making this I realized this might be too big of 
a task for me alone, as it is still too unoptimized and requires too much manual intervention
to compete with the likes of surebet.com and oddsjam. In particular I lack a solution for matching 
team / player names. 

This project is currently more of a prototype than useable software.
Feel free to contribute and make a pull request.

Use runscan.py to scrape odds and betinfo.py to get ev bets or arbs. 
Requires a postgres instance with betting_db.sql imported and a user called "bettingbot" 
with permissions to all used tables.
