import argparse
import logging
import sys
import schemas

import database_connector

def main():
   parser = argparse.ArgumentParser()
   parser.add_argument("-a", "--addAll")
   args = parser.parse_args()
   add_all_bookmaker = args.addAll

   #logging.basicConfig(level=logging.INFO)
   database_connector.connectDb()
   entries = database_connector.getUnmatchedCategories()
   for entry in entries:
      bookmaker_category_id = int(entry[0])
      print(entry)
      text : str = entry[1]
      split = text.rsplit('/', 1)
      text = split[len(split)-1]
      bookmaker = entry[2]
      if add_all_bookmaker is not None:
         if bookmaker != add_all_bookmaker:
            continue
         database_connector.addCategory(schemas.category(category_name=entry[1]), bookmaker_category_id)
         continue
      result = database_connector.searchSimilarCategoryId(text)
      if result is not None:
         print(f"Possible match: {result[0]} {result[1]}")
      print("Entry:")
      print(str(bookmaker_category_id) + " " + text + " " + bookmaker)
      user_input = input("Enter a category id or \"n\" for creating a new category or enter to ignore:\n")
      parsed = parseInt(user_input)
      if user_input in ["exit", "q", "quit"]:
         return
      if user_input == "":
         continue
      if user_input == "n":
         database_connector.addCategory(schemas.category(category_name=text), bookmaker_category_id)
      elif parsed is not None:
         database_connector.associateCategory(bookmaker_category_id, parsed)

   #  await browser_pool.release_all()

def parseInt(string):
   try: 
      return int(string)
   except ValueError:
      return None

if __name__ == '__main__':
   # since asyncio.run never worked (for me)
   main()


