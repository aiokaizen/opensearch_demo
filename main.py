from fastapi import FastAPI

from os_db_manager import opensearch_manager

app = FastAPI()


@app.get("/")
def read_root():
    return {"status": "success"}


@app.get("/api/v1/create_index")
def create_index(index_name: str):
    index_body = {"settings": {"index": {"number_of_shards": 4}}}
    client = opensearch_manager.client
    response = client.indices.create(index_name, index_body)
    print("response:", type(response), "\n\n", response, "\n\n", dir(response), "\n\n")
    return response


@app.get("/api/v1/visa_fees")
def visa_fees(q: str = "", skip: int = 0, size: int = 10):
    # query = {"query": {"match_all": {}}}
    query = {
        "size": size,
        "query": {"multi_match": {"query": q, "fields": ["name", "entity_name"]}},
    }
    client = opensearch_manager.client
    items = client.search(body=query, index="dev.paytic.visa_fees")
    return {"page": 1, "items": items}
