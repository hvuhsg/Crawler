from abc import ABC, abstractmethod
from time import sleep
from queue import Queue, Empty, Full
from threading import Thread

QUEUE_MAX_SIZE = 10
EMPTY_LINK = (None, None)


class BaseStorage(ABC):
    def __init__(self, db_name, base_url, max_depth):
        self.db_name = db_name
        self.base_url = base_url
        self.max_depth = max_depth

        self._links_queue = Queue(maxsize=QUEUE_MAX_SIZE)
        self._pushed_links_queue = Queue()
        self._stop_db_service = False
        self._db_service = Thread(target=self._db_service_loop)

    def setup(self):
        """
        setup storage
        connect to db for example
        :return: None
        """
        self._db_service.start()

    def get_link(self) -> (str, int):
        """
        return (link<str>, link depth<int>)
        link is an unique url
        If you run out of link's return (None, None)
        """
        try:
            return self._links_queue.get(timeout=3)
        except Empty:
            return None, None

    def push_links(self, links, links_depth, father_link):
        """
        store new links

        :param father_link: The link that contains all the pushed links
        :param links_depth: The level of the links
        :param links: (list of links, the depth of the links)
        :return: None
        """

        self._pushed_links_queue.put((links, links_depth, father_link), timeout=3)

    @abstractmethod
    def get_link_from_db(self):
        raise NotImplemented

    @abstractmethod
    def push_links_to_db(self, links, depth, father_link):
        raise NotImplemented

    def _db_service_loop(self):
        while not self._stop_db_service:
            sleep(0.5)
            while not self._pushed_links_queue.empty():
                args = self._pushed_links_queue.get(timeout=3)
                self.push_links_to_db(*args)

            if queue_size := self._links_queue.qsize() < QUEUE_MAX_SIZE:
                for _ in range(QUEUE_MAX_SIZE - queue_size):
                    link = self.get_link_from_db()
                    if link is EMPTY_LINK:
                        break
                    try:
                        self._links_queue.put(link, timeout=3)
                    except Full:
                        pass
        self.cleanup()

    def cleanup(self):
        pass

    def stop(self):
        self._stop_db_service = True
