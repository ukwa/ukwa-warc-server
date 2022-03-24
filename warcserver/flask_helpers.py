import os
import re
from urllib.parse import urlparse, parse_qsl
import logging
import requests
from flask import Response, request, stream_with_context, send_file
from warcserver.file_finder import known_files

WEBHDFS_PREFIX = os.environ.get('WEBHDFS_PREFIX', 'http://hdfs.api.wa.bl.uk/webhdfs/v1')

logger = logging.getLogger(__name__)

def get_byte_range():
    """
    Determines the byte range for this request, either via parameters or HTTP Range requests.

    :return: the (offset, length) tuple, using None if unspecified
    """

    # Default:
    offset = None
    length = None

    # Get any range header:
    range_header = request.headers.get('Range', None)

    # First check for explicit parameters (WebHDFS-API offset=<LONG>[&length=<LONG>])
    if request.args.get('offset', None):
        offset = int(request.args.get('offset'))
        length = request.args.get('length', None)
        if length is not None:
            length = int(length)

    # Otherwise, check for Range header:
    elif range_header:
        m = re.search('(\d+)-(\d*)', range_header)
        g = m.groups()
        if g[0]: offset = int(g[0])
        if g[1]:
            offset2 = int(g[1])
        else:
            offset2 = None
        # Default length to None
        length = None
        if offset2 is not None:
            length = offset2 + 1 - offset

    return offset, length


def send_file_partial(path, offset, length, is_range):
    """
        Simple wrapper around send_file which handles HTTP 206 Partial Content
        (byte ranges)
    """

    def generate():
        with open(path, "rb") as f:
            f.seek(offset)
            to_send = length
            while to_send > 0:
                data = f.read(min(1024,to_send))
                yield data
                to_send -= len(data)

    # Fix up the size:
    size = os.path.getsize(path)
    if length is None:
        length = size - offset

    # Generate a suitable response:
    rv = Response(generate(),
                  200, # Should be 206 when appropriate!
                  mimetype="application/octet-stream",
                  direct_passthrough=True)

    # Add range headers:
    if not is_range:
        rv.headers.add('Accept-Ranges', 'bytes')
    else:
        rv.headers.add('Content-Range', 'bytes {0}-{1}/{2}'.format(offset, offset + length - 1, size))

    logger.info(f"Returning response from send_file_partial: {rv}")

    return rv


def find_file(filename):
    # Strip off any leading path info:
    filename = os.path.basename(filename)

    # To allow us to check for 'open' files:
    filename_open = "%s.open" % filename

    # Look
    for tryf in [filename, filename_open]:
        if tryf in known_files:
            return known_files[tryf]

    # No match:
    return None


def from_webhdfs(item, offset, length, is_range):
    logger.debug(f"Looking for {item}")
    if 'access_url' in item and item['access_url']:
        logger.debug("Found access_url in item record.")
        # Extract the parameters from the URL into an array:
        parsed_url = urlparse(item['access_url'])
        params = dict(parse_qsl(parsed_url.query))
        # Reconstruct the URL without the parameters:
        parsed_url = parsed_url._replace(query='')
        url = parsed_url.geturl()
    else:
        logger.debug("No access_url found - using default WebHDFS service.")
        url = WEBHDFS_PREFIX + item['file_path']
        params = {
            'user.name': 'access',
            'op': 'OPEN'
        }

    # Add range parameters:
    params['offset'] = offset
    if length:
        params['length'] = length

    # And run the request
    logger.debug(f"Requesting URL {url} with parameters {params}...")
    req = requests.get(url, params=params, stream=True)
    response = Response(stream_with_context(req.iter_content(chunk_size=1024)), content_type=req.headers['content-type'])
    if not is_range:
        response.headers.add('Accept-Ranges', 'bytes')
    else:
        # As above, should the app only do this on HTTP requests, not WebHDFS ones?
        if length:
            response.headers.add('Content-Range', 'bytes {0}-{1}/*'.format(offset, offset + length - 1))
        else:
            # This response is not strictly valid, but we don't currently have access to the length of the original:
            response.headers.add('Content-Range', 'bytes {0}-*/*'.format(offset))

    logger.info(f"Returning response from from_webhdfs: {response}")

    return response


#    # Grab the payload from the WARC and return it.
#    url = "%s%s?op=OPEN&user.name=%s&offset=%s" % (WEBHDFS_PREFIX, warc_filename, WEBHDFS_USER, warc_offset)
#    if compressedendoffset and int(compressedendoffset) > 0:
#        url = "%s&length=%s" % (url, compressedendoffset)
#    r = requests.get(url, stream=True)
#    # We handle decoding etc.
#    r.raw.decode_content = False
#    logger.debug("Loading from: %s" % r.url)
#    logger.debug("Got status code %s" % r.status_code)

