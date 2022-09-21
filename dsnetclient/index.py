import abc
from enum import Enum
from json import dumps
from typing import Set, List, Generator, Optional

from elasticsearch import AsyncElasticsearch


class NamedEntityCategory(Enum):
    ORGANIZATION = "ORGANIZATION"
    PERSON = "PERSON"
    LOCATION = "LOCATION"
    UNKNOW = "UNKNOWN"

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.__repr__()


class NamedEntity:

    def __init__(self, document_id: str, category: NamedEntityCategory, mention: str):
        self.document_id = document_id
        self.mention = mention
        self.category = category
        self.type = "NamedEntity"

    def __repr__(self):
        return f"{self.category}:{self.mention}"


class Document:
    def __init__(self, id: str, named_entities: List[NamedEntity]) -> None:
        self.identifier = id
        self.named_entities = named_entities


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

    @abc.abstractmethod
    async def publish(self) -> Generator[NamedEntity, None, None]:
        """
        publish method to publish the current documents' named entities
        :return: A generator iterating over the named entities contained in a document.
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

    async def publish(self) -> Generator[NamedEntity, None, None]:
        resp = await self.aes.search(index=self.index_name, body=self.query_body_from_string("*"))
        for hit in resp["hits"]["hits"]:
            yield NamedEntity(
                hit["_routing"],
                NamedEntityCategory[hit["_source"]["category"]],
                hit["_source"]["mention"]
            )

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

