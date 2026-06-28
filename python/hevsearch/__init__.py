"""hevsearch: vector + full-text search on object storage, embedded in-process.

No server, no infrastructure:

    import hevsearch

    db = hevsearch.connect()                 # writes to ./hevsearch_data
    db.add([{"id": 1, "vector": [0.1, 0.2, 0.3], "text": "hello world"}])
    hits = db.search("hello")           # text, vector, or both (hybrid)
"""

from ._native import (
    Client,
    Collection,
    HevSearchError,
    Hit,
    StorageError,
    TenantError,
    UnsupportedError,
    ValidationError,
    connect,
)

__all__ = [
    "connect",
    "Client",
    "Collection",
    "Hit",
    "HevSearchError",
    "StorageError",
    "TenantError",
    "ValidationError",
    "UnsupportedError",
]

__version__ = "0.1.0"
