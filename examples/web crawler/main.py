from Crawler.storage_types.sqlite_storage import Storage
from Crawler.crawler import Crawler
from worker import Worker


def main():
    base_url = 'en.wikipedia.org/wiki/Main_Page'
    depth = 2

    sqlite_storage = Storage(db_name="storage.db", base_url=base_url, max_depth=depth)
    crawler = Crawler(base_url=base_url, depth=depth, storage=sqlite_storage, worker_class=Worker, workers_number=2)
    crawler.create_workers()
    crawler.run_workers()
    crawler.idle()

main()
