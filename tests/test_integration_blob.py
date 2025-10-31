# tests/test_integration_blob.py
import os, pytest
from azure.storage.blob import BlobServiceClient

@pytest.mark.integration
def test_list_containers():
    conn = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    svc = BlobServiceClient.from_connection_string(conn)
    names = [c.name for c in svc.list_containers()]
    assert "raw" in names
