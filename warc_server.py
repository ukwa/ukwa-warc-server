import io
import logging
from flask import Flask, Response, request, stream_with_context, send_file, abort, url_for
from flask.logging import default_handler
from warcserver.cdx import lookup_in_cdx
from warcserver.flask_helpers import *
from warcserver.file_finder import start_looking
from warcio.archiveiterator import ArcWarcRecordLoader, DecompressingBufferedReader

# Setup app
app = Flask(__name__)

# Integrate with gunicorn logging if present:
if "gunicorn" in os.environ.get("SERVER_SOFTWARE", ""):
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

# Integrate root and module logging with Flask:
root = logging.getLogger()
root.addHandler(default_handler)
root.setLevel(app.logger.level)

# Start looking for files:
start_looking()

#
# The app routes
#

@app.route('/')
def hello_world():
    return 'Hello, World!\n'


@app.route('/by-filename/<path:warc_filename>')
def warc_by_filename(warc_filename):
    # Work out the range:
    offset, length = get_byte_range()
    if offset == None:
        is_range = False
        offset = 0
    else:
        is_range = True
    app.logger.debug(f"Looking for range {is_range} ({offset}-{length}) of {warc_filename}...")
    # Look up the file:
    path_to_file = find_file(warc_filename)
    if path_to_file:
        if path_to_file['type'] == 'hdfs':
            return from_webhdfs(path_to_file, offset, length, is_range)
        else:
            return send_file_partial(path_to_file['file_path'], offset, length, is_range)
    else:
        return abort(404)


@app.route('/webhdfs/v1/by-filename/<path:warc_filename>')
def warc_webhdfs_by_filename(warc_filename):
    """Just a handy alias..."""
    return warc_by_filename(warc_filename)


@app.route('/get-rendered-original')
def get_rendered_original():
    """
    Grabs a rendered resource.

    Only reason Wayback can't do this is that it does not like the extended URIs
    i.e. 'screenshot:http://' and replaces them with 'http://screenshot:http://'
    """
    url = request.args.get('url')
    app.logger.debug("Got URL: %s" % url)
    #
    type = request.args.get('type', 'screenshot')
    app.logger.debug("Got type: %s" % type)

    # Query URL
    qurl = "%s:%s" % (type, url)
    # Query CDX Server for the item
    (warc_filename, warc_offset) = lookup_in_cdx(qurl)

    # If not found, say so:
    if warc_filename is None:
        abort(404)

    # Grab the payload from the WARC and return it.
    r = requests.get("%s?offset=%i" % (url_for('warc_by_filename',path=warc_filename), warc_offset))
    app.logger.info("Loading from: %s" % r.url)
    r.raw.decode_content = False
    rl = ArcWarcRecordLoader()
    record = rl.parse_record_stream(DecompressingBufferedReader(stream=io.BytesIO(r.content)))
    print(record)
    print(record.length)
    print(record.stream.limit)

    return send_file(record.stream, mimetype=record.content_type)

    #return "Test %s@%s" % (warc_filename, warc_offset)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
