from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from google.cloud import storage


class InferenceLogger:
    def __init__(self, bucket_name: str, prefix: str) -> None:
        self.bucket_name = bucket_name
        self.prefix = prefix.strip("/")
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)

    def log(self, event: dict[str, Any]) -> None:
        now = datetime.now(UTC)
        date = now.date().isoformat()
        event_id = uuid4().hex

        blob_name = f"{self.prefix}/date={date}/{now.isoformat()}_{event_id}.json"
        blob = self.bucket.blob(blob_name)

        blob.upload_from_string(
            json.dumps(event, default=str),
            content_type="application/json",
        )
