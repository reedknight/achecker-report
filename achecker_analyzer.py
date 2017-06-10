from db_handler import DbHandler
import multiprocessing as mp
import requests
import xmltodict
import csv
import json
import sys
import time
import logging
import random
logging.basicConfig(filename='process.log',level=logging.DEBUG)
# logging.debug('This message should go to the log file')
# logging.info('So should this')
# logging.warning('And this, too')

def millis():
  return int(1)
#round(time.time() * 1000)
def processUniversityURL(uni):
    global db_handler
    start_time = millis()
    logging.info("UNI ID : {uni_id} : Request Sent : {ts}ms".format(uni_id=str(uni[0]), ts=str(start_time)))
    report = analyzeURL(uni[1])
    report[0] = uni[0]
    print(str(report))
    db_handler.insertAcheckerReport(report)
    db_handler.releaseLock(uni[0])
    logging.info("UNI ID : {uni_id} : LOCK RELEASED : {ts}ms".format(uni_id=str(uni[0]), ts=str(millis())))
    logging.info("UNI ID : {uni_id} : Report : {report}".format(uni_id=str(uni[0]), report=str(report)))
    logging.info("UNI ID : {uni_id} : Time To Completion : {ts}ms".format(
        uni_id=str(uni[0]), ts=str(millis() - start_time)
    ))

def analyzeURL(uri):
    achecker_api_url = 'http://achecker.ca/checkacc.php'

    payload = {
        'uri': uri,
        'id': 'a02a01e25cfd4a1e20d22ed07566fe6932ed4104',
        'output': 'rest',
        'guide': 'WCAG2-AA',
        'offset': '10'
    }

    reponse_list = [0, -1, -1, -1]

    try:
        response = requests.get(achecker_api_url, payload)
        response_parsed = xmltodict.parse(response.text)
        print(str(response_parsed['resultset']['summary']))
        reponse_list[1] = response_parsed['resultset']['summary']['NumOfErrors']
        reponse_list[2] = response_parsed['resultset']['summary']['NumOfLikelyProblems']
        reponse_list[3] = response_parsed['resultset']['summary']['NumOfPotentialProblems']
    except:
        logging.info("Could not process URI : " + uri);

    return reponse_list


## PROGRAM EXECUTION STARTS HERE

__DB_FILE__ = "world_universities.db"

db_handler = DbHandler(__DB_FILE__)

db_handler.createACheckerSchema()

start_time = millis()
logging.info("\nPROGRAM STARTED AT : " + str(start_time) + " ms\n")

universities = db_handler.getURLSNotAnalyzedByAcheckerWithLock(limit = 7358)
# processUniversityURL(universities[0])
# sys.exit()
if len(universities) > 0:
    try:
        pool = mp.Pool(processes=4)
        results = pool.map(processUniversityURL, universities)
    except:
        raise
    finally:
        db_handler.createLockSchema()
else:
    print "No Universities Left"

print "Process Complete"
logging.info("\nPROGRAM FINISHED AT : " + str(millis() - start_time) + " ms\n")
