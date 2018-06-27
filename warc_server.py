import os
import re
import requests
from flask import Flask, Response, request, stream_with_context, send_file, abort

app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello, World!\n'


@app.route('/by-filename/<path:warc_filename>')
def warc_by_filename(warc_filename):
    # Work out the range:
    offset, length = get_byte_range()
    app.logger.info("Looking for range %s-%s of %s..." % (offset, length, warc_filename))
    # Look up the file:
    path_to_file = find_file(warc_filename)
    if path_to_file:
        return send_file_partial(path_to_file, offset, length)
    else:
        return abort(404)


@app.route('/webhdfs/v1/by-filename/<path:warc_filename>')
def warc_webhdfs_by_filename(warc_filename):
    """Just a handy alias..."""
    return warc_by_filename(warc_filename)


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



if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
