import os
import sqlite3
from threading import Thread
from queue import Queue, Empty, Full
from time import sleep

from ..core.base_storage import BaseStorage


QUEUE_MAX_SIZE = 50
EMPTY_LINK = (None, None)


class Storage(BaseStorage):
    def __init__(self, db_name, base_url, max_depth):
        super().__init__(db_name, base_url, max_depth)
        self.db_name = db_name
        self.base_url = base_url
        self.max_depth = max_depth

        self._db_connection = sqlite3.connect(db_name)
        self._cursor = self._db_connection.cursor()

        self._links_queue = Queue(maxsize=QUEUE_MAX_SIZE)
        self._pushed_links_queue = Queue()
        self._stop_db_service = False
        self._db_service = Thread(target=self._db_service_loop)

        self.setup()

    def setup(self):
        """
        Create tables
        """
        if os.stat(self.db_name).st_size == 0:
            # Create table
            self._cursor.execute(
                '''CREATE TABLE links (url text, depth integer, finish_scan integer, middle_of_scan integer)'''
            )
            # Create index
            self._cursor.execute('''CREATE UNIQUE INDEX url_index ON links (url)''')
            # Insert first link
            self._cursor.execute(
                "INSERT INTO links (url, depth, finish_scan, middle_of_scan) VALUES (?, ?, ?, ?)",
                (self.base_url, 0, 0, 0)
            )
            self._db_connection.commit()
        self._db_connection.close()
        self._db_service.start()

    def get_link(self):
        try:
            return self._links_queue.get(timeout=3)
        except Empty:
            return None, None

    def push_links(self, links, links_depth, father_link):
        try:
            self._pushed_links_queue.put((links, links_depth, father_link), timeout=3)
        except Full:
            pass

    def _push_links_to_db(self, links, depth, father_link):
        self._cursor.execute("UPDATE links SET middle_of_scan=0, finish_scan=1 WHERE url = ?", (father_link,))
        self._db_connection.commit()

        for link in links:
            try:
                self._cursor.execute(
                    "INSERT INTO links (url, depth, finish_scan, middle_of_scan) VALUES (?, ?, ?, ?)",
                    (link, depth, 0, 0)
                )
            except sqlite3.IntegrityError:
                pass
        self._db_connection.commit()

    def _get_link_from_db(self):
        self._cursor.execute("SELECT * FROM links WHERE finish_scan=0 AND middle_of_scan=0 ORDER BY depth")
        link = self._cursor.fetchone()
        if link is None or link[1] >= self.max_depth:
            return None, None

        link = {"url": link[0], "depth": link[1], "finish_scan": link[2], "middle_scan": link[3]}

        self._cursor.execute("UPDATE links SET middle_of_scan=1 WHERE url = ?", (link["url"], ))
        self._db_connection.commit()
        return link["url"], link["depth"]

    def _db_service_loop(self):
        self._db_connection = sqlite3.connect(self.db_name)
        self._cursor = self._db_connection.cursor()

        while not self._stop_db_service:
            sleep(0.5)
            while not self._pushed_links_queue.empty():
                args = self._pushed_links_queue.get(timeout=3)
                self._push_links_to_db(*args)

            if queue_size := self._links_queue.qsize() < QUEUE_MAX_SIZE:
                for _ in range(QUEUE_MAX_SIZE - queue_size):
                    link = self._get_link_from_db()
                    if link is EMPTY_LINK:
                        break
                    try:
                        self._links_queue.put(link, timeout=3)
                    except Full:
                        pass
        self._db_connection.commit()
        self._db_connection.close()

    def stop(self):
        self._stop_db_service = True
