import abc
from json import dumps
from typing import Set

from elasticsearch import AsyncElasticsearch


class Index(metaclass=abc.ABCMeta):
    """
    API for searching into local entities
    """
    @abc.abstractmethod
    async def search(self, query: bytes) -> bytes:
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


class LuceneIndex(Index):
    def __init__(self, aes: AsyncElasticsearch, index_name: str = "local-datashare"):
        self.index_name = index_name
        self.aes = aes

    async def search(self, query: bytes) -> bytes:
        resp = await self.aes.search(index=self.index_name, body=self.query_body_from_string(query.decode()))
        return dumps([hit["_source"]["mention"] for hit in resp["hits"]["hits"]]).encode()

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

    async def search(self, query: bytes) -> bytes:
        term_list = set(query.decode().split())
        return dumps(list(term_list.intersection(self.entities))).encode()

    async def close(self) -> None:
        pass

