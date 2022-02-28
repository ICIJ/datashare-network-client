import abc
from typing import Set, List

from elasticsearch import AsyncElasticsearch


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

    """
    close index connection
    """
    @abc.abstractmethod
    async def close(self) -> None:
        """
        closes the connection(s) to the index server
        """


class ElasticsearchIndex(Index):
    def __init__(self, aes: AsyncElasticsearch):
        self.aes = aes

    async def search(self, query: bytes) -> List[bytes]:
        resp = await self.aes.search(index="local-datashare", body=self.query_body_from_string(query.decode()))
        return [hit["_source"]["mention"].encode() for hit in resp["hits"]["hits"]]

    def query_body_from_string(self, query: str) -> dict:
        return {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "type": "NamedEntity"
                            }
                        },
                        {
                            "query_string": {
                                "query": query
                            }
                        }
                    ]
                }
            }
        }

    async def close(self):
        await self.aes.close()


class MemoryIndex(Index):
    def __init__(self, entities: Set[str]):
        self.entities = entities

    async def search(self, query: bytes) -> List[bytes]:
        term_list = set(query.decode().split())
        return list(map(lambda s: s.encode(), term_list.intersection(self.entities)))

    async def close(self) -> None:
        pass

