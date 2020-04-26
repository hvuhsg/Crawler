import requests
from bs4 import BeautifulSoup

from Crawler.core.base_worker import BaseWorker


class Worker(BaseWorker):
    def __init__(self, storage):
        self.session = requests.session()
        self.session.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/46.0',
        }
        super().__init__(storage)

    def clean_link(self, link):
        if link.startswith('http'):
            link = link[link.find('/')+2:]
        return link

    def find_sublinks(self, link):
        link = self.clean_link(link)
        pure_link = link
        if link.find('/') != -1:
            pure_link = link[:link.find('/')]

        sublinks = []
        result = self.session.get('http://'+link, timeout=10)
        if result.status_code != 200:
            return sublinks
        html = result.text
        soup = BeautifulSoup(html, features="lxml")

        title_tag = soup.find("title")
        if title_tag: print(title_tag.string)

        for sublink in [h.get('href') for h in soup.find_all('a')]:
            if sublink:
                sublink = sublink.lower()
            if not sublink:
                continue
            elif sublink.startswith('http'):
                protocol = sublink[:sublink.find('//')+2]
                if sublink.startswith(protocol+pure_link):
                    sublinks.append(sublink.replace(protocol, ''))
            elif sublink.startswith('/'):
                sublinks.append(pure_link+sublink)
        return sublinks


