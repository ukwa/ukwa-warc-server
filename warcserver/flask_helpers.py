import os
import re
import requests
from flask import Response, request, stream_with_context, send_file


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


def send_file_partial(path, offset, length):
    """
        Simple wrapper around send_file which handles HTTP 206 Partial Content
        (byte ranges)
    """

    if offset is None:
        response = send_file(path)
        response.headers.add('Accept-Ranges', 'bytes')
        return response

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
    #Do this only when it's a proper range request? Not a WebHDFS mapped request?
    #rv.headers.add('Content-Range', 'bytes {0}-{1}/{2}'.format(offset, offset + length - 1, size))

    return rv


def find_file(filename):
    # Strip off any leading path info:
    filename = os.path.basename(filename)

    # To allow us to check for 'open' files:
    filename_open = "%s.open" % filename

    # Places to look:
    locations = os.environ.get('WARC_PATHS','.')
    for location in locations.split(","):
        for root, dirnames, filenames in os.walk(location):
            if filename in filenames:
                return os.path.join(root, filename)
            if filename_open in filenames:
                return os.path.join(root, filename_open)

    # No match:
    return None


def from_webhdfs(url, offset, length):
    req = requests.get(url, params={'offset': offset, 'length': length}, stream=True)
    return Response(stream_with_context(req.iter_content(chunk_size=1024)), content_type=req.headers['content-type'])


