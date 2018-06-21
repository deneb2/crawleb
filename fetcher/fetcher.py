"""Implementation of a url fetcher function"""
import re
import sys
import logging
import requests
import threading

import utils.requests_wrapper as requests_wrapper

# in case of ConnectionError we retry a specified number of times
MAX_RETRIES = 5


class Status():
    ''' Define some interal error codes.'''
    Success = 0
    ConnectionError = 200
    SkipUrl = 300
    GenericError = 100

def fetch(headers, doc_metadata, wait_on_fail = 600):
    '''
    Fetch the url specified on doc_metadata. Return updated metadata.

    Keyword arguments:
    headers      --  (dict) used to modify requests heders.
    doc_metadata -- (DocumentMetadata) cointains doc metadata (url etc.)
    wait_on_fail -- (int) seconds to wait before retrying in case
                    of ConnectionError
    '''
    fail_counter = 0
    sleep = threading.Event()
    while not doc_metadata.response and MAX_RETRIES > fail_counter:
        try:
            doc_metadata.response = requests_wrapper.requests_retry_session(headers) \
                                                    .get(doc_metadata.url)
            doc_metadata.status = Status.Success
            return doc_metadata
        except (requests.exceptions.TooManyRedirects,
                requests.exceptions.HTTPError,
                requests.exceptions.Timeout,
                requests.exceptions.InvalidSchema) as e:
            logging.warning("%s - Skip URL" %str(e))
            doc_metadata.status = Status.SkipUrl
            return None
        except requests.ConnectionError as e:
            logging.error("%s  - Fail (%d of %d)\n" %(str(e), fail_counter, MAX_RETRIES))
            sleep.wait(wait_on_fail * fail_counter)
            fail_counter += 1
            doc_metadata.status = Status.ConnectionError
        except Exception, e:
            logging.error("Generic server error. %s - " %str(e))
            doc_metadata.status = Status.GenericError
            return None
    return doc_metadata
