class BaseStorage:
    def __init__(self, db_name, base_url, max_depth):
        self.db_name = db_name
        self.base_url = base_url
        self.max_depth = max_depth

    def setup(self):
        """
        setup storage
        connect to db for example
        :return: None
        """
        raise NotImplemented

    def get_link(self) -> (str, int):
        """
        return (link<str>, link depth<int>)
        link is an unique url
        """
        raise NotImplemented

    def push_links(self, links, links_depth, father_link):
        """
        store new links

        :param links: (list of links, the depth of the links)
        :return: None
        """
        raise NotImplemented
