import requests
import xmltodict
import csv

def analyzeURL(uri):
    achecker_api_url = 'http://achecker.ca/checkacc.php'

    payload = {
        'uri': uri,
        'id': 'a02a01e25cfd4a1e20d22ed07566fe6932ed4104',
        'output': 'rest',
        'guide': 'WCAG2-AA',
        'offset': '10'
    }

    reponse_dict = {
        'NumOfErrors': -1,
        'NumOfLikelyProblems': -1,
        'NumOfPotentialProblems': -1,
    }

    try:
        response = requests.get(achecker_api_url, payload)
        response_parsed = xmltodict.parse(response.text)
        reponse_dict['NumOfErrors'] = response_parsed['resultset']['summary']['NumOfErrors']
        reponse_dict['NumOfLikelyProblems'] = response_parsed['resultset']['summary']['NumOfLikelyProblems']
        reponse_dict['NumOfPotentialProblems'] = response_parsed['resultset']['summary']['NumOfPotentialProblems']
    except:
        print("Could not process URI : " + uri);

    return reponse_dict

# URL, UNI NAME, COUNTRY, ERRORS, LIKELY, PROBLEMS, TIMESTAMP
# row = [
#     "http://www.ccsu.ac.in",
#     "CCSU",
#     "ABC",
#     2,
#     3,
#     5,
#     1232422324343,
# ]
# report_file  = open('achecker_report.csv', "wb")
# file_writer = csv.writer(report_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL)
# file_writer.writerow(row)
#
# report_file.close()

# print json.dumps(analyzeURL("http://www.ccsu.ac.in"))
