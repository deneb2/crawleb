"""
Implement a base spider.

A spider is intended as a set of configuration and function usefull to
download from a web source.
"""
import re
import json
import logging
import datetime
import robotparser

from sets import Set
from simhash import fingerprint
from bs4 import BeautifulSoup
from urlparse import urlparse, urlunparse, parse_qs, urldefrag, urljoin
from urllib import urlencode

from spiders.document import Document


class BaseSpider(object):
    '''The base spider defines default function for crawling and parsing'''
    # The following are some options that can be cusomized on new spiders ##
    # It is highly recommended to not change the values here but on the new derived classes ##
    ###########################################################################################
    # empty allowed_domains means all domains allowed (be carefull)
    allowed_domains = []

    # list of pages to not crawl. regex permitted
    exclude_pages = []

    # empty start_urls means no crawling
    start_urls = []

    # How deep from the initial list you want to go. depth = 0 means no limits (be carefull)
    depth = 2

    # setup how often you want to crawl the start_urls (seconds)
    restart_delay = 120

    # use the canonical url in the web pages as a synonim of the original url
    use_canonical = True

    # follow the rules of nofollow
    nofollow_compliant = True

    # read the robots during crawling
    robots_compliant = True

    # delay between to fetch two page in the current spider (seconds)
    delay = 20

    # parameters to normalize urls.
    # Example:
    #    normalize_params = ['ref']
    #    will normalize http://www.domain.com?ref=value to http://www.domain.com
    normalize_params = []

    # a file to bootstrap the crawler, if you already have a list.
    # This urls will be downloaded at the beginning and they will be refetched using
    # common refetching_rules set in the config file
    urllist_filename = None

    # is important to setup user-aget at least
    headers = {
    }

    # In the case the encoding is missing in the header, we use a default option
    # TODO: in the future is possible to use also the content of the page to infer encoding,
    #       it is anyway a good to have option since each spider should be used for a single website.
    default_encoding = "utf-8"

    # END OF CUSTOMIZABLE OPTIONS
    # you shouldn't change the following
    #####################################

    robotparser_cache = {}
    urllist = []

    def get_canonical(self, root):
        """Retrieve eventually the canonical url."""
        if self.use_canonical:
            canonical = root.find('link', {"rel": "canonical"}, href=True)
            if canonical:
                url = canonical.get("href")
                url = self.normalize_url(url)
                return url

    def set_config(self):
        """Configure the spider before using it."""
        if self.urllist_filename:
            try:
                fd = open(self.urllist_filename, "r")
                for i in fd:
                    json_obj = json.loads(i)
                    url = json_obj["url"]
                    # INFO: filtering possible errors in the list.
                    #       Removing not alowed domains and removing
                    #       starting_urls because they are already
                    #       in the system.
                    if self.get_domain(url) in self.allowed_domains \
                       and url not in self.start_urls:
                        self.urllist.append(json_obj)
            except Exception as e:
                logging.warning(str(e) +
                                "File specified by urllist does not exist;"
                                "This parameter is not mandatory;"
                                "Please delete or fix this configuration.")

    def normalize_url(self, url):
        """
        Normalization url function.

        Some urls contains useless parameters or slashes.
        We remove them to discover duplicates.
        """
        u = urlparse(url)
        query = parse_qs(u.query)
        for i in self.normalize_params:
            query.pop(i, None)
        if len(query) > 1:
            query = sorted(query.items())
        try:
            u = u._replace(query=urlencode(query, True))
        except Exception as e:
            logging.error("error replacing query in url %s" % (e,))

        normalized = urldefrag(urlunparse(u))[0]
        # INFO: not removing trailing slash to avoid too many redirections
        # normalized = normalized.rstrip("/")
        return normalized

    def get_tree_dom(self, content):
        """Get the parsed html."""
        # TODO: extrnal class could be usefull if we eventually
        #       want to use a different parser
        return BeautifulSoup(content, "lxml")
        
    def check_and_normalize(self, url):
        """
        Check if we are interested in the url and normalize it.

        Return two value:
        string  -- normalized_url
        bool    -- specifies if we need to remove original url from seen
                   (maybe because rules are changed)
        """
        if any([True for ex in self.exclude_pages if re.match(ex, url)]):
            logging.warning("skip URL because page excluded: {}".format(url))
            return url, True
        domain = self.get_domain(url)
        if all([ad != domain for ad in self.allowed_domains]):
            logging.warning(" DOMAIN ({}) discarded: {}".format(domain, url))
            return url, True
        nurl = self.normalize_url(url)
        if nurl != url:
            logging.warning("URL normlized: {} to {}".format(url, nurl))
            return nurl, True
        return url, False
    
    def get_domain(self, url):
        """Return the domain of the url."""
        return urlparse(url).netloc

    def get_meta(self, root, name):
        """Extract metadata from a parsed html"""
        if root is not None:
            attributes = ["name", "property", "http-equiv"]
            for attr in attributes:
                data = root.find("meta", {attr: name})
                if data:
                    return data.get("content")

    def get_links(self, soup, base_url):
        """Extract the urls from a parsed html."""
        base = soup.find("base", href=True)
        if base:
            base_url = urljoin(base_url, base.get("href"))

        all_links = [urljoin(base_url, i.get('href').strip())
                     for i in soup.find_all('a', href=True)]

        # I remove urls starting with "/"
        # For debugging purpose I use a long version of the following:
        # links = [l for l in all_links if not l.startwith("/")]
        links = []
        for l in all_links:
            if l.startswith("/"):
                logging.debug("skipping url starting with '/'.base: %s link: %s"
                              % (base_url, l))
            else:
                links.append(l)

        if self.nofollow_compliant is True:
            nofollow = Set(
                [urljoin(base_url, i.get('href'))
                 for i in soup.find_all('a', {"rel": "nofollow"}, href=True)]
            )
            links = [l for l in links if l not in nofollow]

        allowed_links = Set()
        domains = Set()
        for l in links:
            domain = self.get_domain(l)
            if len(self.allowed_domains) == 0 or domain in self.allowed_domains:
                norm_l = self.normalize_url(l)
                if [True for ex in self.exclude_pages if re.match(ex, norm_l)]:
                    continue
                rb = None
                domains.add(domain)
                if self.robots_compliant:
                    rb = self.robotparser_cache.get(domain)
   
                    if rb is None:
                        rb = robotparser.RobotFileParser()
                        # TODO: fix this
                        rb.set_url("http://" + domain + "/robots.txt")
                        rb.read()
                        self.robotparser_cache[domain] = rb

                # INFO: this try-catch is here because sometimes
                #       can_fatch returns unicode problems. It seemd
                #       to be a bug of the library though.
                ################################################################
                try:
                    # TODO: ho messo l'* perche' non so  lo user-agent qui per ora
                    if not rb or rb.can_fetch("*", norm_l):
                        allowed_links.add(norm_l)

                except KeyError:
                    pass

        return list(allowed_links)

    def parse(self, dmeta):
        """Return a Document and the Metadata containing the date extracted from the page."""
        # import pickle
        # pickle.dump(dmeta, open("save.p", "a"))
        self.root = self.get_tree_dom(dmeta.response.content)
        data = Document()

        # INFO: using the response url as a primary url
        nurl = self.normalize_url(dmeta.response.url)
        data.url = nurl
        if not dmeta.response.encoding:
            dmeta.response.encoding = self.default_encoding
        data.raw_html = dmeta.response.content.decode(dmeta.response.encoding, "ignore")
        data.fetched_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M")
        data.status = dmeta.response.status_code

        text = self.root.get_text().lower()
        cleaned_text = re.split(r'[^\w]+', text)
        doc_hash = fingerprint(map(hash, cleaned_text))
        data.dhash = doc_hash
        data.domain = self.get_domain(nurl)

        # INFO: updating meta info
        dmeta.url = nurl
        dmeta.dhash = data.dhash
        canonical = self.get_canonical(self.root)
        if canonical:
            dmeta.alternatives.append(canonical)
        if dmeta.depth < self.depth:
            dmeta.links = self.get_links(self.root, nurl)
        return data, dmeta
