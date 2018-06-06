"""Test spider."""
from spiders.base_spider import BaseSpider

class TestSpider(BaseSpider):
    """
    Spider just for test.
    Define some rules to download from mediagol website.
    """
    # name is mandatory. It must be uniq since is used to
    # crate queues and datastores
    name = "mediagol"

    # here is possible to change default behaviour.
    # the full set of customizable variable is
    # documented in base_spider.py
    use_canonical = False

    # this is a minumum example:
    # Allowed domain permits to limit the crawling domains
    # The idea of this project was to create one spider for
    # each news website. Is highly suggested to set allowed_domains
    allowed_domains = [
        "www.mediagol.it",
    ]

    # it is possible to exclude some pages if we are not interested on them
    exclude_pages = [
        ".*http://www\.mediagol\.it/foto/.*",
        ".*http://www\.mediagol\.it/video/.*",
    ]

    # the start_urls are some priority url that are fetched more often
    # setting this variable to the homepage permits to discover new content
    start_urls = [
        "http://www.mediagol.it",
    ]
