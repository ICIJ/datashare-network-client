from __future__ import  annotations
import json
from pathlib import Path

import pytest
import pytest_asyncio
from elasticsearch import AsyncElasticsearch

from dsnetclient.index import LuceneIndex, Index

lucene: Index


@pytest_asyncio.fixture
async def lucene_index():
    global lucene
    mappings = (Path(__file__).parent / "data" / "datashare_index_mappings.json").read_text()
    settings = (Path(__file__).parent / "data" / "datashare_index_settings.json").read_text()
    body = f'{{ "mappings": {mappings}, "settings": {settings} }}'
    elasticsearch = AsyncElasticsearch("http://elasticsearch:9200")
    await elasticsearch.indices.create(index="test-datashare", body=body)
    lucene = LuceneIndex(elasticsearch, index_name="test-datashare")
    yield
    await elasticsearch.indices.delete(index="test-datashare")
    await lucene.close()


@pytest.mark.asyncio
async def test_search_no_result(lucene_index):
    resp = json.loads(await lucene.search(b"donald AND dock"))
    assert len(resp) == 0


@pytest.mark.asyncio
async def test_search_one_result(lucene_index):
    await TestIndexer(lucene.aes).create_named_entity("donald duck").commit()
    resp = json.loads(await lucene.search(b'donald duck'))
    assert len(resp) == 1


class TestIndexer:
    def __init__(self, aes: AsyncElasticsearch, index_name: str = "test-datashare"):
        self.aes = aes
        self.index_name = index_name
        self.mention = None

    def create_named_entity(self, mention: str) -> TestIndexer:
        self.mention = mention
        return self

    async def commit(self) -> None:
        body = {
            "type": "NamedEntity",
            "mention": self.mention,
            "mentionNormTextLength": len(self.mention)
        }
        await self.aes.index(self.index_name, body=body, params={"refresh": "true"})