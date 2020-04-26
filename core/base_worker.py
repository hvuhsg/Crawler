from threading import Thread
from abc import ABC, abstractmethod


class BaseWorker:
    def __init__(self, storage):
        self.storage = storage
        self._stop = False
        self._thread = Thread(target=self.loop)

    def run(self):
        self._stop = False
        self._thread.start()

    def stop(self):
        self._stop = True

    def loop(self):
        while not self._stop:
            self.round()

    def round(self):
        link, link_depth = self.storage.get_link()
        if link_depth is None:
            self._stop = True
            return
        sublinks = self.find_sublinks(link)
        self.storage.push_links(sublinks, link_depth + 1, link)

    @abstractmethod
    def find_sublinks(self, link: str) -> list:
        """
        :param link: <str> link to web page
        :return: list of links<str>
        """
        raise NotImplemented
