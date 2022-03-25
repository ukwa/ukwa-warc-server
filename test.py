#!/usr/bin/env python3

import urllib3
import hashlib
import gzip

def check_response(response_data):
    data = gzip.decompress(response_data)
    # Calculate hash of the whole record:
    m = hashlib.sha256()
    m.update(data)
    hd = m.hexdigest()
    print(hd)

    if hd == 'ec1626efb5592c4bfa7e214f7ce7a3bb9d707767df8bf70783ce74b2926371de':
        print("Gunzipped WARC record hash matches expected value")
    else:
        raise Exception("Gunzipped WARC record hash did not match expected value")


url = "http://localhost:8008/by-filename/wikipage.warc.gz"
http = urllib3.PoolManager()
response = http.request('GET', url, headers={'Range':'bytes=424957-460686'})
#print(data.decode("UTF-8"))
check_response(response.data)

response = http.request('GET', url, fields={'offset': 424957, 'length': 35730})
check_response(response.data)

