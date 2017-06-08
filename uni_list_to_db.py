# Program to read the World University URLS from world_universities.json
# file and populate in the db for further processing.

from db_handler import DbHandler
import json
import sys

__DB_FILE__ = "world_universities.db"
__JSON_FILE__ = "world_universities.json"

db_handler = DbHandler(__DB_FILE__)

json_obj = json.load(open(__JSON_FILE__))
count = 1
for country in json_obj:
    country['name'] = country['name'][:country['name'].rfind(' ')]
    country_url_list = []
    for university in country['universities']:
        new_url_tuple = (
            university['url'],
            university['name'],
            country['code'],
            country['name']
        )
        country_url_list.append(new_url_tuple)
    count = count + len(country_url_list)
    print(str(count))
    db_handler.createURL(country_url_list)
