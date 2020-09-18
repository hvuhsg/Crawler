import pymongo
from pymongo.errors import DuplicateKeyError
from time import sleep
from threading import Thread
from queue import Queue, Empty, Full


from ..core.base_storage import BaseStorage


QUEUE_MAX_SIZE = 50
EMPTY_LINK = (None, None)


class Storage(BaseStorage):
    def __init__(self, db_name, base_url, max_depth, username, password, ip='localhost', port=27017,
                 timeout=5000):
        super().__init__(db_name, base_url, max_depth)
        self.collection_name = base_url
        self.ip = ip
        self.port = port
        self.timeout = timeout  # 1000 = 1 second
        self.username = username
        self.password = password

        self._links_queue = Queue(maxsize=QUEUE_MAX_SIZE)
        self._pushed_links_queue = Queue()
        self._stop_db_service = False
        self._db_service = Thread(target=self._db_service_loop)

        self.client, self.db, self.links_collection = self.setup()

    def connect_to_mongo(self):
        client = pymongo.MongoClient(
            self.ip,
            self.port,
            username=self.username,
            password=self.password,
            authSource=self.db_name,
            serverSelectionTimeoutMS=self.timeout,
        )
        db = client[self.db_name]
        return client, db

    def setup(self):
        client, db = self.connect_to_mongo()
        if self.collection_name not in db.list_collection_names():
            collection = db[self.collection_name]
            collection.create_index("url", unique=True)
            collection.insert_one({"url": self.base_url, "depth": 0, "finish_scan": False, "middle_of_scan": False})
        else:
            collection = db[self.collection_name]
            collection.update_many({"middle_of_scan": True}, {"$set": {"middle_of_scan": False}})
        self._db_service.start()
        return client, db, collection

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

    def _get_link_from_db(self):
        link = list(self.links_collection.find({"finish_scan": False, "middle_of_scan": False})
                    .sort('depth', pymongo.ASCENDING)
                    .limit(1)
                    )
        if link:
            link = link[0]
        else:
            return EMPTY_LINK
        if link["depth"] >= self.max_depth:
            return EMPTY_LINK
        self.links_collection.update_one({"_id": link["_id"]}, {"$set": {"middle_of_scan": True}})
        return link["url"], link["depth"]

    def _push_links_to_db(self, links, depth, father_link):
        self.links_collection.update_one({"url": father_link}, {"$set": {"finish_scan": True, "middle_of_scan": False}})
        count = 0
        for link in links:
            try:
                self.links_collection.insert_one(
                    {"url": link,
                     "depth": depth,
                     "finish_scan": False,
                     "middle_of_scan": False,
                     }
                )
            except DuplicateKeyError:
                pass
            else:
                count += 1

    def _db_service_loop(self):
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
        self.client.close()

    def stop(self):
        self._stop_db_service = True
