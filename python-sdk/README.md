# DataFlow Python event client

A dependency-free example client for sending exceptions to the DataFlow event
intake API. It uses Python's standard library and deliberately leaves network
errors visible to the caller.

## Install locally

```bash
python -m pip install -e .
```

## Capture an exception

Start the Java API on `http://localhost:8080`, then run:

```python
from dataflow_sdk import EventClient

client = EventClient("http://localhost:8080", project="checkout")

try:
    int("not-an-integer")
except ValueError as error:
    receipt = client.capture_exception(error)
    print(receipt["issue"]["issueId"])
```

This teaching SDK sends synchronously and has no retry queue, transport
redaction, or automatic exception hook. Production telemetry clients should
add those capabilities and avoid sending secrets or personal data.

## Test

From this directory:

```bash
python -m unittest discover -s tests -v
```
