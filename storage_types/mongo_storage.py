import pymongo
from pymongo.errors import DuplicateKeyError

from ..core.base_storage import BaseStorage


class Storage(BaseStorage):
    def __init__(self, db_name, base_url, max_depth, username, password, ip='localhost', port=27017,
                 timeout=5000):
        super().__init__(db_name, base_url, max_depth)
        self.collection_name = base_url
        self.ip = ip
        self.port = port
        self.timeout = timeout # 1000 = 1 second
        self.username = username
        self.password = password
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
        return client, db, collection

    def get_link(self):
        link = list(self.links_collection.find({"finish_scan": False, "middle_of_scan": False}).sort('depth', pymongo.ASCENDING).limit(1))
        if link:
            link = link[0]
        else:
            raise Exception("No links")
        if link["depth"] >= self.max_depth:
            return '', None
        self.links_collection.update_one({"_id": link["_id"]}, {"$set": {"middle_of_scan": True}})
        return link["url"], link["depth"]

    def push_links(self, links, depth, father_link):
        self.links_collection.update_one({"url": father_link}, {"$set": {"finish_scan": True, "middle_of_scan": False}})
        count = 0
        for link in links:
            try:
                self.links_collection.insert_one(
                    {"url": link,
                     "depth": depth,
                     "finish_scan":False,
                     "middle_of_scan": False,
                     }
                )
            except DuplicateKeyError:
                pass
            else:
                count += 1
        print(count)
