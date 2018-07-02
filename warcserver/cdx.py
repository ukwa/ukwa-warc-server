import logging
import xml.dom.minidom
from requests.utils import quote
import requests

logger = logging.getLogger(__name__)


def lookup_in_cdx(qurl, cdx_server='http://bigcdx:8080/data-heritrix'):
    """
    Checks if a resource is in the CDX index.
    :return:
    """
    query = "%s?q=type:urlquery+url:%s" % (cdx_server, quote(qurl))
    r = requests.get(query)
    print(r.url)
    logger.debug("Availability response: %d" % r.status_code)
    print(r.status_code, r.text)
    # Is it known, with a matching timestamp?
    if r.status_code == 200:
        try:
            dom = xml.dom.minidom.parseString(r.text)
            for result in dom.getElementsByTagName('result'):
                file = result.getElementsByTagName('file')[0].firstChild.nodeValue
                compressedoffset = result.getElementsByTagName('compressedoffset')[0].firstChild.nodeValue
                return file, compressedoffset
        except Exception as e:
            logger.error("Lookup failed for %s!" % qurl)
            logger.exception(e)
        #for de in dom.getElementsByTagName('capturedate'):
        #    if de.firstChild.nodeValue == self.ts:
        #        # Excellent, it's been found:
        #        return
    return None, None
