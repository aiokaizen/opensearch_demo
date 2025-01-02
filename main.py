import json
from typing import Any
from fastapi import FastAPI
from opensearchpy import OpenSearch
from opensearchpy.exceptions import ConflictError, NotFoundError, RequestError

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


@app.get("/api/v1")
def health_check():
    client = opensearch_manager.client
    return client.indices.get(KMOUAD_INDEX_NAME)


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


@app.post("/api/v1/delete_document/{id}")
def delete_document(id: str):
    client = opensearch_manager.client
    client = opensearch_manager.client
    deleted = client.delete(index=KMOUAD_INDEX_NAME, id=id)
    if deleted["deleted"] >= 0:
        return {
            "status": "success",
            "message": "The index has been cleared successfully.",
        }
    return {"status": "error", "message": "", "object": deleted}


@app.post("/api/v1/clear_index")
def clear_index():
    """
    PLEASE DON'T USE THIS ENDPOINT IN TESTING, OR PRODUCTION CODE.
    """
    client = opensearch_manager.client
    query = {
        "query": {"match_all": {}},
    }
    client = opensearch_manager.client
    deleted = client.delete_by_query(index=KMOUAD_INDEX_NAME, body=query)
    if deleted["deleted"] >= 0:
        return {
            "status": "success",
            "message": "The index has been cleared successfully.",
        }
    return {"status": "error", "message": "", "object": deleted}


@app.post("/api/v1/create_document/{id}")
def create_document(id: str, document: dict[str, Any]):
    client = opensearch_manager.client
    try:
        response = client.create(index=KMOUAD_INDEX_NAME, id=f"p{id}", body=document)
        if response["result"] == "created":
            return {"status": "success", "message": "Document created successfully."}
        return {"status": "error", "message": "", "object": response}
    except ConflictError:
        return {"status": "error", "message": "ID already exists."}


@app.put("/api/v1/update_mapping")
def update_mapping():
    client = opensearch_manager.client
    # Check if id exists in the database.
    new_mapping = {
        "properties": {
            "Hobbies": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
            },
            "age": {"type": "long"},
            "experience": {
                "properties": {
                    "company": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                    },
                    "end_date": {"type": "date"},
                    "start_date": {"type": "date"},
                }
            },
            "home_town": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
            },
            "name": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
            },
        }
    }
    response = client.indices.put_mapping(new_mapping, KMOUAD_INDEX_NAME)

    print("response:", response)
    if response["acknowledged"]:
        return {"status": "success", "message": "Mapping updated successfully."}
    return {"status": "error", "object": response}


@app.get("/api/v1/dashboard")
def dashboard():
    client = opensearch_manager.client

    # City count Aggregation
    city_count_query = {
        "size": 0,
        "aggs": {
            "cities_count": {
                "terms": {"field": "home_town.keyword", "size": 10}
            }  # Return only the 10 highest cities
        },
    }
    result = client.search(city_count_query, KMOUAD_INDEX_NAME)
    cities_count = result.get("aggregations", {}).get("cities_count", {}).get("buckets")

    # Hobby count Aggregation
    hobby_count_query = {
        "size": 0,
        "aggs": {"hobbies_count": {"terms": {"field": "Hobbies.keyword"}}},
    }
    result = client.search(hobby_count_query, KMOUAD_INDEX_NAME)
    hobbies_count = (
        result.get("aggregations", {}).get("hobbies_count", {}).get("buckets")
    )

    # Age stats Aggregation
    age_stats_query = {"size": 0, "aggs": {"age_stats": {"stats": {"field": "age"}}}}
    result = client.search(age_stats_query, KMOUAD_INDEX_NAME)
    age_stats = result.get("aggregations", {}).get("age_stats", {})

    # Start date histogram Aggregation
    start_date_histogram_query = {
        "size": 0,
        "aggs": {
            "start_date_histogram": {
                "date_histogram": {
                    "field": "experience.start_date",
                    "calendar_interval": "year",
                }
            }
        },
    }
    result = client.search(start_date_histogram_query, KMOUAD_INDEX_NAME)
    start_date_histogram = (
        result.get("aggregations", {}).get("start_date_histogram", {}).get("buckets")
    )

    # End date histogram Aggregation
    end_date_histogram_query = {
        "size": 0,
        "aggs": {
            "end_date_histogram": {
                "date_histogram": {
                    "field": "experience.end_date",
                    "calendar_interval": "year",
                }
            }
        },
    }
    result = client.search(end_date_histogram_query, KMOUAD_INDEX_NAME)
    end_date_histogram = (
        result.get("aggregations", {}).get("end_date_histogram", {}).get("buckets")
    )

    # Age histogram Aggregation
    age_histogram_query = {
        "size": 0,
        "aggs": {"age_histogram": {"histogram": {"field": "age", "interval": 10}}},
    }
    result = client.search(age_histogram_query, KMOUAD_INDEX_NAME)
    age_histogram = (
        result.get("aggregations", {}).get("age_histogram", {}).get("buckets")
    )

    return {
        "cities_count": cities_count,
        "hobbies_count": hobbies_count,
        "age_stats": age_stats,
        "start_date_histogram": start_date_histogram,
        "end_date_histogram": end_date_histogram,
        "age_histogram": age_histogram,
    }


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
    for i, item in enumerate(data):
        bulk_data += (
            f'{{ "index": {{ "_index": "{KMOUAD_INDEX_NAME}", "_id": "p{i + 1}" }} }}\n'
        )
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
            "from": skip,
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
        query = {"from": skip, "size": size, "query": {"match_all": {}}}
    else:
        query = {
            "from": skip,
            "size": size,
            "query": {"multi_match": {"query": q, "fields": ["name", "entity_name"]}},
        }
    client = opensearch_manager.client
    items = client.search(body=query, index=KMOUAD_INDEX_NAME)
    return {"page": 1, "items": items}
