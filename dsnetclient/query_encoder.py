from abc import ABC, abstractmethod
from typing import Tuple, List

from dsnet.mspsi import MSPSIQuerier
from petlib.bn import Bn


class QueryEncoder(ABC):
    """Query encoder"""

    @abstractmethod
    def encode(self, query: List[str]) -> Tuple[bytes, Bn]:
        """
        encode query
        return a tuple payload, secret
        """


class MSPSIEncoder(QueryEncoder):
    def encode(self, kwds: List[str]) -> Tuple[bytes, Bn]:
        secret, kwds_enc = MSPSIQuerier.query(kwds)
        return b'\n'.join(kwds_enc), secret


class LuceneEncoder(QueryEncoder):
    def encode(self, query: List[str]) -> Tuple[bytes, Bn]:
        return '\n'.join(query).encode(), Bn()
