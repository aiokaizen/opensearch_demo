import json
from typing import Any
from fastapi import FastAPI
from opensearchpy import OpenSearch
from opensearchpy.exceptions import NotFoundError, RequestError
from opensearchpy.helpers import bulk

from os_db_manager import opensearch_manager

app = FastAPI()

KMOUAD_INDEX_NAME = "dev.mouadk.index"


def clear_cache(client: OpenSearch, index: str):
    # Clear Index Cache
    client.indices.clear_cache(index=index)
    print(f"Cache for '{index}' index cleared!")
    client.indices.clear_cache(index=index, query=True)
    print(f"Query cache for '{index}' index cleared!")
    client.indices.clear_cache(index=index, fielddata=True, request=True)
    print(f"Field data and request cache for '{index}' index cleared!")


@app.get("/")
def health_check():
    return {"status": "success"}


@app.post("/api/v1/create_index")
def create_index():
    index_body = {"settings": {"index": {"number_of_shards": 1}}}
    client = opensearch_manager.client
    try:
        response = client.indices.create(KMOUAD_INDEX_NAME, index_body)
        if response["acknowledged"] is True:
            return {"status": "success", "message": "Index created successfully."}
    except RequestError as e:
        if e.error == "resource_already_exists_exception":
            return {"status": "error", "message": "Index already exists."}
        raise e

    return {"status": "error", "object": response}


@app.post("/api/v1/index_doc")
def index_document(document: dict[str, Any]):
    client = opensearch_manager.client
    response = client.index(KMOUAD_INDEX_NAME, document)
    if response["result"] == "created":
        return {"status": "success", "message": "Document created successfully."}
    return {"status": "error", "object": response}


@app.put("/api/v1/update_doc/{id}")
def update_document(id: str, new_data: dict[str, Any]):
    client = opensearch_manager.client
    try:
        # Check if id exists in the database.
        client.get(KMOUAD_INDEX_NAME, id)

        # If NotFoundError is not raised, we proceed with the update.
        response = client.index(KMOUAD_INDEX_NAME, new_data, id)
        if response["result"] == "updated":
            return {"status": "success", "message": "Document updated successfully."}
        return {"status": "error", "object": response}
    except NotFoundError:
        return {"status": "error", "message": "Document does not exist."}


@app.post("/api/v1/bulk_index")
def bulk_index():
    data = []
    with open("mock_data.json", "r") as f:
        data = json.load(f)

    bulk_data = ""
    for item in data:
        bulk_data += f'{{ "index": {{ "_index": "{KMOUAD_INDEX_NAME}" }} }}\n'
        bulk_data += f"{json.dumps(item)}\n"

    client = opensearch_manager.client
    response = client.bulk(bulk_data, KMOUAD_INDEX_NAME)
    if response["errors"] is False:
        return {"status": "success", "message": "Bulk actions performed successfully."}
    return {"status": "error", "object": response}


@app.get("/api/v1/search_exact")
def search_documents_with_exact_value(q: str = "", skip: int = 0, size: int = 10):
    if not q:
        return {"status": "error", "message": "`q` is mandatory"}
    else:
        query = {
            "size": size,
            "query": {
                "match": {"name.keyword": q}
            },  # name.keyword => Match exact matches only.
        }
    client = opensearch_manager.client
    items = client.search(body=query, index=KMOUAD_INDEX_NAME)
    return {"page": 1, "items": items}


@app.get("/api/v1/search")
def search_documents(q: str = "", skip: int = 0, size: int = 10):
    if not q:
        query = {"size": size, "query": {"match_all": {}}}
    else:
        query = {
            "size": size,
            "query": {"multi_match": {"query": q, "fields": ["name", "entity_name"]}},
        }
    client = opensearch_manager.client
    items = client.search(body=query, index=KMOUAD_INDEX_NAME)
    return {"page": 1, "items": items}


@app.get("/api/v1/visa_fees")
def visa_fees(q: str = "", skip: int = 0, size: int = 10):
    if not q:
        query = {"size": size, "query": {"match_all": {}}}
    else:
        query = {
            "size": size,
            "query": {"multi_match": {"query": q, "fields": ["name", "entity_name"]}},
        }
    client = opensearch_manager.client
    items = client.search(body=query, index="dev.paytic.visa_fees")
    return {"page": 1, "items": items}
