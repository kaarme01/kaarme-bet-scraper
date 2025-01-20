import logging
import sys
import controller
import asyncio
import nodriver as uc
import argparse
import schemas


import database_connector

SHARP_BOOK = "pinnacle"

def main():
   print(sys.argv)

   logging.basicConfig(level=logging.INFO)
   database_connector.connectDb()
   parser = argparse.ArgumentParser()

   parser.add_argument("-e", "--eventId")
   parser.add_argument("-u", "--userId")
   parser.add_argument("-f", "--category")
   parser.add_argument("-t", "--betType")
   parser.add_argument("-a", "--all")
   parser.add_argument("-A", "--arbitrage", action='store_true')

   args = parser.parse_args()
   info_type = schemas.BetInfoType.arbs if args.arbitrage else schemas.BetInfoType.evs
   provideData(info_type, args.eventId, args.userId, args.category, args.betType)

def provideData(info_type : schemas.BetInfoType, event_id : str | None, user : str | None, category : str | None, bet_type : str | None = None):
   print(bet_type)
   database_connector.prepareUniversalOutcomeTopOdds()
   bet_type_id = database_connector.searchBetTypeId(bet_type) if bet_type is not None else None
   category_id = None
   if category is not None:
      category_id = int(category)
      #category_id = database_connector.searchCategoryId(category)
   user_id = database_connector.searchUserId(user)
   categories = [category_id] if category_id is not None else [None]
   event = database_connector.getEventById(event_id)
   if event_id is not None and event is None:
      logging.error("Invalid event id provided")
      return
   print(categories)
   for category_id in categories:
      if bet_type is None:
         for bet_type_id in schemas.BetType:
            controller.findInfo(info_type, schemas.BetInfoParameters(event=event, category_id=category_id,\
                                                                     bet_type=bet_type_id, user_id=user_id, sharp_book=SHARP_BOOK))
      else:
         controller.findInfo(info_type, schemas.BetInfoParameters(event=event, category_id=category_id,\
                                                                  bet_type=bet_type_id, user_id=user_id, sharp_book=SHARP_BOOK))




if __name__ == '__main__':
   # since asyncio.run never worked (for me)
   main()


