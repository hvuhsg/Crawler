# Crawler
crawler library that allow you to focus on the part of getting the information from the target
and not worrying about the db or the threads.

You can find examples on the [examples](./examples) folder.

#### DB
The library is supporting 2 types for now.
1. MongoDB
2. Sqlite

#### Code Example
```python
from Crawler.storage_types.sqlite_storage import Storage
from Crawler.crawler import Crawler
from worker import Worker  # This is your costume worker.


def main():
    base_url = "en.wikipedia.org/wiki/Main_Page"
    depth = 2

    sqlite_storage = Storage(db_name="storage.db", base_url=base_url, max_depth=depth)
    crawler = Crawler(
        base_url=base_url,
        depth=depth,
        storage=sqlite_storage,
        worker_class=Worker,
        workers_number=2,
    )
    crawler.create_workers()
    crawler.run_workers()
    crawler.idle()


main()
```