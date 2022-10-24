import abc
from json import dumps
from typing import List, Generator, Tuple, Dict, Iterator

from dsnet.mspsi import Document, NamedEntity, NamedEntityCategory
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

    @abc.abstractmethod
    async def publish(self) -> Tuple[int, Iterator[NamedEntity]]:
        """
        publish method to publish the current documents' named entities
        :return: A generator iterating over the named entities contained in a document.
        """

    @abc.abstractmethod
    async def get_documents(self) -> List[Document]:
        """
        Retrieve documents.
        :return: Dict of documents by document id.
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

    async def get_documents(self) -> List[Document]:
        resp = await self.aes.search(index=self.index_name, body=self.query_documents_body())
        return [
            Document(hit["_id"], hit["_source"]["creationDate"]) for hit in resp["hits"]["hits"]
        ]

    async def publish(self) -> Tuple[int, Iterator[NamedEntity]]:
        resp = await self.aes.search(index=self.index_name, body=self.query_body_from_string("*"))
        return resp["hits"]["total"]["value"], (NamedEntity(
            hit["_routing"],
            NamedEntityCategory[hit["_source"]["category"]],
            hit["_source"]["mention"]
        ) for hit in resp["hits"]["hits"])

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

    def query_documents_body(self) -> dict:
        return {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "type": "Document"
                            }
                        },
                        {
                            "has_child": {
                                "type": "NamedEntity",
                                "query": {
                                    "match_all": {}
                                }
                            }
                        }
                    ]
                }
            }
        }

    async def close(self):
        await self.aes.close()


class MemoryIndex(Index):
    def __init__(self, entities: List[NamedEntity], documents: List[Document]):
        self.entities = entities
        self.documents = documents

    async def publish(self) -> Tuple[int, Iterator[NamedEntity]]:
        return len(self.entities), iter(self.entities)

    async def search(self, query: bytes) -> bytes:
        term_list = set(query.decode().split())
        entities = set((e.mention for e in self.entities))
        return dumps(list(term_list.intersection(entities))).encode()

    async def get_documents(self) -> List[Document]:
        return self.documents

    async def close(self) -> None:
        pass

