from __future__ import  annotations

import datetime
import json
from pathlib import Path

import pytest
import pytest_asyncio
from elasticsearch import AsyncElasticsearch, JSONSerializer
from elasticsearch.exceptions import RequestError
from dsnetclient.index import LuceneIndex, Index, NamedEntityCategory, NamedEntity, Document

lucene: Index


@pytest_asyncio.fixture
async def lucene_index():
    global lucene
    mappings = (Path(__file__).parent / "data" / "datashare_index_mappings.json").read_text()
    settings = (Path(__file__).parent / "data" / "datashare_index_settings.json").read_text()
    body = f'{{ "mappings": {mappings}, "settings": {settings} }}'
    elasticsearch = AsyncElasticsearch("http://elasticsearch:9200", serializer=NamedEntityEncoder())
    try:
        await elasticsearch.indices.create(index="test-datashare", body=body)
    except RequestError:
        await elasticsearch.indices.delete(index="test-datashare")
        await elasticsearch.indices.create(index="test-datashare", body=body)
    lucene = LuceneIndex(elasticsearch, index_name="test-datashare")

    yield

    # await elasticsearch.indices.delete(index="test-datashare")
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

    results = []
    nb, results_generator = await lucene.publish()
    for resp in results_generator:
        results.append(resp)

    assert len(results) == nb
    assert results[0].mention == "donald duck"
    assert results[1].mention == "mickey mouse"


@pytest.mark.asyncio
async def test_search_documents(lucene_index):
    indexer = TestIndexer(lucene.aes)
    await (
        indexer
            .add_document("doc_id1", "donald duck", datetime.datetime.fromisoformat('2022-09-16T14:44:17.052'))
            .add_document("doc_id2", "mickey mouse", datetime.datetime.fromisoformat('2022-09-16T14:55:17.052'))
            .add_named_entity("doc_id1", "donald", NamedEntityCategory.PERSON)
            .add_named_entity("doc_id2", "mickey", NamedEntityCategory.PERSON)
            .commit()
    )
    documents = await lucene.get_documents()
    assert len(documents) == 2


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
        self.documents = []

    def add_named_entity(self, doc_id: str, mention: str, category: NamedEntityCategory) -> TestIndexer:
        self.named_entities.append(NamedEntity(doc_id, category, mention))
        return self

    def add_document(self, doc_id: str, content: str, creation_date: datetime.datetime) -> TestIndexer:
        self.documents.append(Document(doc_id, creation_date, content))
        return self

    async def commit(self) -> None:
        for doc in self.documents:
            print(doc)
            await self.aes.index(self.index_name,  id=doc.identifier, body={"creationDate": doc.creation_date.isoformat(), "type": doc.type, "join": {"name": "Document"}})
        for ne in self.named_entities:
            print(ne)
            await self.aes.index(self.index_name,
                                 body={**ne.__dict__, "join": {"parent": ne.document_id, "name": "NamedEntity"}},
                                 params={"refresh": "true", "routing": ne.document_id})
