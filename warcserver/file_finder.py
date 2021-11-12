import requests
import time
import csv
import os
import logging
import threading
import atexit
import time

logger = logging.getLogger(__name__)

# The global to hold the shared list:
known_files = {}
shared_lock = threading.Lock()

# Where to look:
WARC_PATHS = os.environ.get('WARC_PATHS','.')
TRACKDB_URL = os.environ.get('TRACKDB_URL', 'http://solr8.api.wa.bl.uk/solr/tracking')
TRACKDB_FETCH_LIMIT = os.environ.get('TRACKDB_FETCH_LIMIT', 1_000_000_000)

#
def start_looking():
    logger.debug("Starting Looking for files...")
    update_fs_thread = threading.Thread(target=update_from_filesystem, daemon=True)
    update_fs_thread.start()
    update_tb_thread = threading.Thread(target=update_from_trackdb, daemon=True)
    update_tb_thread.start()

# 
def update_from_filesystem():
    global known_files

    while True:
        try:
            logger.debug("Scanning filesystem for files...")
            count = 0
            with shared_lock:
                for location in WARC_PATHS.split(","):
                    for root, dirnames, filenames in os.walk(location):
                        for filename in filenames:
                            known_files[filename] = {
                                'type': 'local',
                                'file_path': os.path.join(root, filename)
                            }
                            count += 1
                # Cope if files get renamed from .open:
                renamed = set()
                for filename in known_files:
                    filename_open = "%s.open" % filename
                    if filename_open in known_files:
                        renamed.add(filename_open)
                for filename_open in renamed:
                    known_files.pop(filename_open)
            logger.debug("Scanning filesystem found %i files." % count)
        except Exception as e:
            logger.exception("Exception when scanning for file(s):", e)
        # Sleep briefly before updating
        time.sleep(5)
#
def update_from_trackdb():
    global known_files

    while True:
        r = None
        try:
            logger.debug("Pulling file list from TrackDB...")
            limit = TRACKDB_FETCH_LIMIT
            url = TRACKDB_URL + '/select'
            params = { 
                'fl': 'file_name_s,file_path_s,access_url_s',
                'q': 'kind_s:warcs AND file_path_s:[* TO *]',
                'rows': limit,
                # Add refresh timestamp, most recent last, to ensure we get the most recent copy even if moved clusters:
                'sort': 'file_name_s desc, refresh_timestamp_dt asc', 
                'wt': 'csv'
            }
            r = requests.get(url, params=params, stream=True)

            if r.status_code != 200:
                raise Exception(f"Call to TrackDB failed! Status Code {r.status_code}:\n{r.text[0:1000]}")

            if r.encoding is None:
                r.encoding = 'utf-8'

            count = 0
            with shared_lock:
                for row in csv.reader(r.iter_lines(decode_unicode=True)):
                    if row[0] != 'file_name_s':
                        known_files[row[0]] = {
                            'type': 'hdfs',
                            'file_path': row[1], 
                            'access_url': row[2]
                        }
                        count += 1
            logger.debug("Updated %i files from TrackDB." % count)

        except Exception as e:
            logger.exception("Exception when talking to TrackDB!",e)
            if r:
                logger.error("Response from Solr was: %s" % r.text[0:1000])
        
        # Sleep a long time before updating:
        time.sleep(5*60)


# Push to a sqllite3 db, each few rows a transaction, updating a 'updated_at' field with a time.time() value.
# At the end, remove any rows that have older updated_at values.
# Clients open the same sqllite3 file independently, using read-only connections.
#
# OR
# 
# Just use a filesystem cache like the cachelib one used in ukwa-access-api
# https://werkzeug.palletsprojects.com/en/0.16.x/contrib/cache/
# Would use timeouts so would rely on updates. 
# Perhaps rather clumsy if there's a million entries?	


