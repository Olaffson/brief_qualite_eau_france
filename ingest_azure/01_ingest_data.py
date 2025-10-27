import os
import requests
from urllib.parse import urlparse
from azure.storage.blob import BlobClient

CONN_STR = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
ACCOUNT_NAME = "saokqualiteeaufr"
CONTAINER = "raw"

URLS = [
    "https://static.data.gouv.fr/resources/resultats-du-controle-sanitaire-de-leau-distribuee-commune-par-commune/20251001-103424/dis-2025-dept.zip",
    "https://static.data.gouv.fr/resources/resultats-du-controle-sanitaire-de-leau-distribuee-commune-par-commune/20230811-065325/dis-2021-dept.zip",
    "https://static.data.gouv.fr/resources/resultats-du-controle-sanitaire-de-leau-distribuee-commune-par-commune/20230707-102607/dis-2022-dept.zip",
    "https://static.data.gouv.fr/resources/resultats-du-controle-sanitaire-de-leau-distribuee-commune-par-commune/20241014-073334/dis-2023-dept.zip",
    "https://static.data.gouv.fr/resources/resultats-du-controle-sanitaire-de-leau-distribuee-commune-par-commune/20250329-074506/dis-2024-dept.zip"
]

for url in URLS:
    filename = os.path.basename(urlparse(url).path)  # ex: dis-2025-dept.zip
    # extraction de l'année à partir du nom (ex: dis-2025-dept.zip → 2025)
    year = filename.split("-")[1]  

    # destination logique dans raw
    blob_path = f"zip/{filename}"

    print(f"Téléchargement : {url}")
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        blob = BlobClient.from_connection_string(CONN_STR, container_name=CONTAINER, blob_name=blob_path)
        blob.upload_blob(r.raw, overwrite=True)

    print("Upload OK ->", f"abfss://{CONTAINER}@{ACCOUNT_NAME}.dfs.core.windows.net/{blob_path}\n")

print("✅ Tous les fichiers ont été chargés avec succès !")
