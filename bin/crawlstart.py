#!/usr/local/bin/python3

import sys
import requests
import urllib.parse

crawlingURL = sys.argv[1]
parsed_url = urllib.parse.urlparse(crawlingURL)
crawlingHost = parsed_url.netloc
crawlingProtocol = parsed_url.scheme

data = {
    'cachePolicy': 'iffresh',
    'collection': 'testcollection',
    'crawlingstart': 'Start crawling',
    'crawlingMode': 'url',
    'crawlingQ': 'on',
    'crawlingDepth': 1,
    'crawlingDepthExtension': '',
    'crawlingURL': crawlingURL,
    'deleteIfOlderNumber': 1,
    'deleteIfOlderUnit': 'day',
    'deleteold': 'age',
    'indexmustmatch': '^{0}.*'.format(crawlingURL),
    'indexmustnotmatch':  '',
    'indexMedia': 'on',
    'mustmatch': '^{protocol}://{host}/.*'.format(protocol=crawlingProtocol, host=crawlingHost),
    'mustnotmatch': '',
    'indexText': 'on',
    'range': 'wide',
    'recrawl': 'reload',
    'reloadIfOlderNumber': 0,
    'reloadIfOlderUnit': 'day',
    'storeHTCache': 'on',
    'xsstopw': 'on',
    'priority': 0
}

res = requests.get('http://localhost:8300/yacy/grid/crawler/crawlStart.json', params=data)

if res.status_code != 200:
    print("ERR :: error starting the crawler")
    print(res.text)
else:
    print("INF :: successfully sent '{0}' to crawler".format(crawlingURL))
