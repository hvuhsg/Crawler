from Crawler.StorageTypes.mongo_storage import Storage
from Crawler.crawler import Crawler
from worker import Worker


def main():
    base_url = 'kiryat4.org.il'
    depth = 5

    mongo_storage = Storage(base_url=base_url,
                            db_name='crawlerDB',
                            username='crawler_username',
                            password="crawler_password",
                            max_depth=depth)
    crawler = Crawler(base_url=base_url, depth=depth, storage=mongo_storage, worker_class=Worker, workers_number=5)
    crawler.create_workers()
    crawler.run_workers()
    crawler.idle()

main()
