from abc import ABC, abstractmethod
from enum import Enum
from typing import Tuple, List

from dsnet.mspsi import MSPSIQuerier
from petlib.bn import Bn
from sscred import packb, unpackb

SearchMode = Enum("SearchMode", "DPSI Lucene")


class QueryEncoder(ABC):
    """Query encoder"""
    @abstractmethod
    def search_mode(self) -> SearchMode:
        """
        return the search mode associated with the encoder
        """

    @abstractmethod
    def encode(self, query: List[bytes]) -> Tuple[Bn, bytes]:
        """
        @param query : list of keywords ascii bytes
        return a tuple payload, secret
        """

    @abstractmethod
    def decode(self, secret: Bn, reply: bytes) -> List[bytes]:
        """
        decode reply
        return a list of decoded keywords (or kwds hashes)
        """


class MSPSIEncoder(QueryEncoder):
    def search_mode(self) -> SearchMode:
        return SearchMode.DPSI

    def encode(self, kwds: List[bytes]) -> Tuple[Bn, bytes]:
        secret, kwds_enc = MSPSIQuerier.query(kwds)
        return secret, packb(kwds_enc)

    def decode(self, secret: Bn, reply: bytes) -> List[bytes]:
        return MSPSIQuerier.decode_reply(secret, unpackb(reply))


class LuceneEncoder(QueryEncoder):
    def search_mode(self) -> SearchMode:
        return SearchMode.Lucene

    def encode(self, kwds: List[bytes]) -> Tuple[Bn, bytes]:
        return Bn(), packb(kwds)

    def decode(self, _secret: Bn, reply: bytes) -> List[bytes]:
        return unpackb(reply)
