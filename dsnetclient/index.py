import abc
from json import dumps
from typing import List, Tuple, Iterator, Optional

from dsnet.mspsi import Document, NamedEntity, NamedEntityCategory, MSPSIDocumentOwner
from elasticsearch import AsyncElasticsearch
from sscred import unpackb, packb

from dsnetclient.repository import Repository


class Index(metaclass=abc.ABCMeta):
    """
    API for searching into local entities
    """
    @abc.abstractmethod
    async def search(self, packb_kwds: bytes) -> Optional[bytes]:
        """
        search method from a simple query
        :param packb_kwds: keywords to search packb encoded
        :return:
        """

    """
    processing search results when the query sender receives the document owner response
    """
    @abc.abstractmethod
    async def process_search_results(self, results: bytes) -> bytes:
        """
        search method from a simple query
        :param results: packb encoded results
        :return: payload to store in local state
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

    async def process_search_results(self, results: bytes) -> bytes:
        return dumps(unpackb(results)).encode()

    async def get_documents(self) -> List[Document]:
        resp = await self.aes.search(index=self.index_name, body=self.query_documents_body())
        return [
            Document(hit["_id"], hit["_source"]["extractionDate"]) for hit in resp["hits"]["hits"]
        ]

    async def publish(self) -> Tuple[int, Iterator[NamedEntity]]:
        resp = await self.aes.search(index=self.index_name, body=self.query_body_from_string("*"))
        return resp["hits"]["total"]["value"], (NamedEntity(
            hit["_routing"],
            NamedEntityCategory[hit["_source"]["category"]],
            hit["_source"]["mention"]
        ) for hit in resp["hits"]["hits"])

    async def search(self, kwds_packb: bytes) -> bytes:
        query = b' '.join(unpackb(kwds_packb))
        resp = await self.aes.search(index=self.index_name, body=self.query_body_from_string(query.decode()))
        return packb([hit["_source"]["mention"] for hit in resp["hits"]["hits"]])

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

    async def process_search_results(self, results: bytes) -> bytes:
        return results

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


class MspsiIndex(Index):
    def __init__(self, repository: Repository, es_index: LuceneIndex):
        self.es_index = es_index
        self.repository = repository

    async def process_search_results(self, results: bytes) -> bytes:
        pass

    async def publish(self) -> Tuple[int, Iterator[NamedEntity]]:
        return await self.es_index.publish()

    async def search(self, query: bytes) -> Optional[bytes]:
        kwds = unpackb(query)
        publications = await self.repository.get_publications()
        return packb(MSPSIDocumentOwner.reply(publications[0].secret, kwds)) if publications else None

    async def get_documents(self) -> List[Document]:
        return await self.es_index.get_documents()

    async def close(self) -> None:
        pass

