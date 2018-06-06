"""Request wrapper function."""
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


def requests_retry_session(
        headers = None,
        retries=10,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 504),
        session=None,
):
    """Helper function to open a session."""
    session = session or requests.Session()
    # todo: these should be spider config
    session.headers.update(headers)
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def quote(url):
    """Quote urls. usefull before requests."""
    return requests.utils.quote(url)
