from __future__ import  annotations
import json
from pathlib import Path

import pytest
import pytest_asyncio
from elasticsearch import AsyncElasticsearch, JSONSerializer

from dsnetclient.index import LuceneIndex, Index, NamedEntityCategory, NamedEntity

lucene: Index


@pytest_asyncio.fixture
async def lucene_index():
    global lucene
    mappings = (Path(__file__).parent / "data" / "datashare_index_mappings.json").read_text()
    settings = (Path(__file__).parent / "data" / "datashare_index_settings.json").read_text()
    body = f'{{ "mappings": {mappings}, "settings": {settings} }}'
    elasticsearch = AsyncElasticsearch("http://elasticsearch:9200", serializer=NamedEntityEncoder())
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
    await TestIndexer(lucene.aes).add_named_entity("doc_id", "donald duck", NamedEntityCategory.PERSON).commit()
    resp = json.loads(await lucene.search(b'donald duck'))
    assert len(resp) == 1


@pytest.mark.asyncio
async def test_search_publish(lucene_index):
    indexer = TestIndexer(lucene.aes)
    await (
        indexer
        .add_named_entity("doc_id", "donald duck", NamedEntityCategory.PERSON)
        .add_named_entity("doc_id", "mickey mouse", NamedEntityCategory.PERSON)
        .commit()
    )

    result = []
    async for resp in lucene.publish():
        result.append(resp)

    assert len(result) == 2
    assert result[0].mention == "donald duck"
    assert result[1].mention == "mickey mouse"


class NamedEntityEncoder(JSONSerializer):
    def default(self, obj):
        if isinstance(obj, NamedEntityCategory):
            return str(obj)
        else:
            super().default(obj)


class TestIndexer:
    def __init__(self, aes: AsyncElasticsearch, index_name: str = "test-datashare"):
        self.aes = aes
        self.index_name = index_name
        self.named_entities = []

    def add_named_entity(self, doc_id: str, mention: str, category: NamedEntityCategory) -> TestIndexer:
        self.named_entities.append(NamedEntity(doc_id, category, mention))
        return self

    async def commit(self) -> None:
        for ne in self.named_entities:
            await self.aes.index(self.index_name, body=ne.__dict__, params={"refresh": "true", "routing": ne.document_id})