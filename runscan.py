from __future__ import annotations
import json
import logging
import random
import sys
from betting_wrapper import BettingWrapper
import controller
import asyncio
import nodriver as uc

import database_connector
import argparse

import schemas
import utils

from Proxy_List_Scrapper import Scrapper, Proxy, ScrapperException

browser = None
chrome_path = "./chromeprofile"
#browser_args = ["--blink-settings=imagesEnabled=false"]
browser_args = [""]
filename = "wrappers/event_lists.json" 

#scrapper = Scrapper(category='ALL')
#proxies = scrapper.getProxies()

def get_random_proxy(proxies): 
  proxy = random.choice(proxies) 
  return f"http://{proxy.ip}:{proxy.port}"

#proxy = get_random_proxy(proxies.proxies)

proxy_dict = None



async def main():
  parser = argparse.ArgumentParser()

  parser.add_argument("-e", "--eventId")
  parser.add_argument("-u", "--userId")
  parser.add_argument("-b", "--bookmaker")
  parser.add_argument("-f", "--field")
  parser.add_argument("-t", "--betType")
  parser.add_argument("-c", "--tabcount")
  parser.add_argument("-a", "--all")
  parser.add_argument("-hb", "--headless", action='store_true')
  parser.add_argument("-kp", "--knownPages", action='store_true')
  parser.add_argument("-uc", "--useConfig", action='store_true')
  parser.add_argument("-co", "--categoriesOnly", action='store_true')
  args = parser.parse_args()
  # logging.basicConfig(level=logging.INFO)
  database_connector.connectDb()

  browser = await initBrowser(args.headless)
  browser_conn = (browser.config.host, browser.config.port, chrome_path,)
  data = load_json_file(filename)
  arg_bookmaker = args.bookmaker
  arg_field = args.field
  arg_tabcount = int(args.tabcount) if args.tabcount is not None else None
  categories_only = args.categoriesOnly
  print(args.knownPages)
  if args.knownPages:
    await scanKnownPages(browser_conn, arg_bookmaker, arg_tabcount)
    return
  if args.useConfig is False and arg_bookmaker is not None:
    await scanBookmaker(arg_bookmaker, browser_conn, categories_only)
    return
  for bookmaker, categories in data.items():
    if arg_bookmaker is not None and bookmaker != arg_bookmaker:
      continue
    print(f"Site: {bookmaker}")
    if "leagues" in categories:
      print("  Leagues:")
      for name, url in categories["leagues"].items():
        if arg_field is not None and name != arg_field:
          continue
        print(f"    {name}: {url}")
        await scanBookmaker(bookmaker, browser_conn, categories_only, url, name, args.tabcount)
  
  """
  wrapper = controller.wrapperDict[args.bookmaker]()
  sport = None

  if wrapper.requires_browser:
    wrapper.browser_conn = browser_conn

  await wrapper.run(sport, browser_conn, )
  """
  #  await browser_pool.release_all()

async def scanBookmaker(bookmaker : str, browser_conn : tuple, categories_only : bool, link : str | None = None,\
                        field : str | None = None, tabcount : int | None = None):
  wrapper = controller.wrapperDict[bookmaker]()
  wrapper.proxy_dict = proxy_dict
  if wrapper.requires_browser:
    wrapper.browser_conn = browser_conn
    wrapper.tab_count = tabcount or wrapper.tab_count
  await wrapper.run(schemas.BookmakerScanParameters(link=link, bookmaker=bookmaker, field=field, categories_only=categories_only))



async def scanKnownPages(browser_conn : tuple, bookmaker : str | None, tab_count : int | None):
  scannable_wrappers = ['veikkaus', 'coolbet', 'coolbetv2']
  if bookmaker is not None and bookmaker in scannable_wrappers:
    scannable_wrappers = [bookmaker]
  instance_dict : dict[str, BettingWrapper] = {key: controller.wrapperDict[key]() for key in scannable_wrappers}
  for instance in instance_dict.values():
    if instance.can_update_all:
      if instance.requires_browser:
        instance.browser_conn = browser_conn
      await instance.updateOdds()
      return
  events = database_connector.getEvents()
  event_pages : list[schemas.EventPageData] = []
  for event in events:
    event_pages = event_pages + database_connector.getEventPageData(event.event_id)
  tasks = []
  for page in event_pages:
    for key in instance_dict:
      if page.bookmaker == key:
        wrapper = instance_dict[key]
        if wrapper.can_update_all:
          continue
        if wrapper.requires_browser:
          wrapper.browser_conn = browser_conn
        event = database_connector.getEventById(page.event_id)
        tasks.append(wrapper.scrapeGame(page.event_url, event, page.oghome, page.ogaway, True))
  batch_size = tab_count or 20
  for batch in utils.chunk_list(tasks, batch_size):
    await asyncio.gather(*batch)

async def initBrowser(headless : bool):
  browser = await uc.start(
                          headless=headless,
                          user_data_dir=chrome_path,
                          uses_custom_data_dir=True,
                          browser_args=browser_args)
  return browser

def load_json_file(filename):
    with open(filename, 'r') as file:
        return json.load(file)


if __name__ == '__main__':
  # since asyncio.run never worked (for me)
  loop = asyncio.new_event_loop()
  loop.run_until_complete(main())


