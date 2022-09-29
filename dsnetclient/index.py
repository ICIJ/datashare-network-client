import abc
import datetime

from enum import Enum
from json import dumps
from typing import List, Generator, Tuple, Dict

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
    def __init__(self, id: str, creation_date: datetime.datetime, content: str = "") -> None:
        self.type = "Document"
        self.identifier = id
        self.content = content
        self.creation_date = creation_date


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
    async def publish(self) -> Tuple[int, Generator[NamedEntity, None, None]]:
        """
        publish method to publish the current documents' named entities
        :return: A generator iterating over the named entities contained in a document.
        """

    @abc.abstractmethod
    async def get_documents(self) -> Dict[str, Document]:
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

    async def get_documents(self) -> Dict[str, Document]:
        resp = await self.aes.search(index=self.index_name, body=self.query_documents_body())
        return {
            hit["_id"]: Document(hit["_id"], hit["_source"]["creationDate"]) for hit in resp["hits"]["hits"]
        }

    async def publish(self) -> Tuple[int, Generator[NamedEntity, None, None]]:
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

    async def publish(self) -> Generator[NamedEntity, None, None]:
        return (e for e in self.entities)

    async def search(self, query: bytes) -> bytes:
        term_list = set(query.decode().split())
        entities = set((e.mention for e in self.entities))
        return dumps(list(term_list.intersection(entities))).encode()

    async def get_documents(self) -> Dict[str, Document]:
        return { d.identifier: d for d in self.documents }

    async def close(self) -> None:
        pass

