# tests/test_integration_sql.py
import os, pytest
from databricks import sql

@pytest.mark.integration
@pytest.mark.skipif(not os.environ.get("DATABRICKS_HTTP_PATH"), reason="No SQL Warehouse HTTP Path")
def test_sql_1():
    with sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://",""),
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"]
    ) as c:
        with c.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone()[0] == 1
