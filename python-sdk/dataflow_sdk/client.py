import json
import traceback
import uuid
from typing import Any
from urllib.request import Request, urlopen


class EventClient:
    def __init__(self, api_url: str, project: str, timeout: float = 2.0) -> None:
        if not api_url.strip():
            raise ValueError("api_url is required")
        if not project.strip():
            raise ValueError("project is required")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        self._events_url = api_url.strip().rstrip("/") + "/api/events"
        self._project = project.strip()
        self._timeout = timeout

    def capture_exception(self, error: BaseException) -> dict[str, Any]:
        event = {
            "eventId": str(uuid.uuid4()),
            "project": self._project,
            "exceptionType": type(error).__name__,
            "message": str(error) or type(error).__name__,
            "stackTrace": "".join(
                traceback.format_exception(type(error), error, error.__traceback__)
            ),
        }
        request = Request(
            self._events_url,
            data=json.dumps(event).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(request, timeout=self._timeout) as response:
            return json.loads(response.read().decode("utf-8"))
