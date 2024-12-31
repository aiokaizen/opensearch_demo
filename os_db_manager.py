from threading import Lock

from opensearchpy import OpenSearch, RequestsHttpConnection
from opensearchpy.exceptions import OpenSearchException, ConnectionError

from configs import app_config


class OSDatabaseManager:
    _instance = None
    _client: OpenSearch | None = None
    _lock = Lock()

    def __new__(cls):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
                    cls._client = cls._get_client()

        return cls._instance

    @classmethod
    def _get_client(cls):
        if cls._client:
            return cls._client

        try:
            return OpenSearch(
                hosts=[{"host": app_config.OS_DB_URL, "port": app_config.OS_DB_PORT}],
                http_auth=(app_config.OS_DB_USER, app_config.OS_DB_PWD),
                # http_compress=True,  # enables gzip compression for request bodies
                use_ssl=True,
                verify_certs=True,
                # ssl_assert_hostname=False,
                # ssl_show_warn=False,
                connection_class=RequestsHttpConnection,
                pool_maxsize=20,
            )
        except (OpenSearchException, ConnectionError) as e:
            print("Error raised while creating the OpenSearch client.")
            raise e

    @property
    def client(self):
        if not self._client:
            raise Exception("Client is not created.")
        return self._client


opensearch_manager = OSDatabaseManager()
