import abc
from typing import Set, List


class Index(metaclass=abc.ABCMeta):
    """
    API for searching into local entities
    """
    @abc.abstractmethod
    async def search(self, query: bytes) -> List[bytes]:
        """
        search method from a simple query
        :param query: string query
        :return:
        """


class ElasticsearchIndex(Index):
    async def search(self, query: bytes) -> List[bytes]:
        pass


class MemoryIndex(Index):
    def __init__(self, entities: Set[str]):
        self.entities = entities

    async def search(self, query: bytes) -> List[bytes]:
        term_list = set(query.decode().split())
        return list(map(lambda s: s.encode(), term_list.intersection(self.entities)))