import os
import requests
from urllib.parse import urlparse
from azure.storage.blob import BlobClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

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

    # destination logique dans raw
    blob_path = f"zip/{filename}"

    blob = BlobClient.from_connection_string(CONN_STR, container_name=CONTAINER, blob_name=blob_path)

    # ✅ Vérifier si le blob existe déjà
    try:
        blob.get_blob_properties()
        print(f"⏩ SKIP : {filename} déjà présent dans Azure → {blob_path}")
        continue     # on passe au fichier suivant
    except ResourceNotFoundError:
        pass  # il n'existe pas → on télécharge

    print(f"⬇️ Téléchargement : {url}")
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        blob.upload_blob(r.raw, overwrite=False)

    print(f"✅ Upload OK -> abfss://{CONTAINER}@{ACCOUNT_NAME}.dfs.core.windows.net/{blob_path}\n")

print("🎉 Ingestion terminée (les fichiers existants n'ont pas été re-téléchargés).")
