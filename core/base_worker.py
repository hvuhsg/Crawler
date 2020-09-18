from threading import Thread
from abc import abstractmethod
from typing import Tuple, List


class BaseWorker:
    def __init__(self, storage):
        self.storage = storage
        self._stop = False
        self._thread = Thread(target=self.loop)

    def run(self):
        """
        Run the worker thread
        :return:
        """
        self._stop = False
        self._thread.start()

    def stop(self):
        """
        Stop the worker thread
        :return:
        """
        self._stop = True

    def loop(self):
        """
        While the worker run's call round method
        :return:
        """
        while not self._stop:
            self.round()

    def round(self):
        """
        Get link, find all it's sub links and save them
        """
        link, link_depth = self.storage.get_link()
        if link is None:
            self._stop = True
            return
        sublinks, link_data = self.find_sublinks(link)
        self.process_link_data(link_data)
        self.storage.push_links(sublinks, link_depth + 1, link)

    def process_link_data(self, link_data):
        """
        Do what ever you want with the data from the link (search for emails in the html page for example)
        :param link_data: the data from the link
        """
        return NotImplemented

    @abstractmethod
    def find_sublinks(self, link: str) -> Tuple[List[str], object]:
        """
        :param link: <str> link to web page
        :return: (list of links<str>, link_data)
        """
        raise NotImplemented
