# pip install azure-storage-blob
import os
import io
import mimetypes
import posixpath
import tempfile
import zipfile
from datetime import datetime, timezone

from azure.storage.blob import BlobServiceClient, BlobClient, ContentSettings
from azure.core.exceptions import ResourceNotFoundError

CONN_STR   = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
CONTAINER  = "raw"
ZIP_PREFIX = "zip/"     # où 01_ingest_data.py a déposé les .zip
OUT_PREFIX = "unzip/"   # où on écrit les fichiers extraits

# crée un marker après succès pour éviter de retraiter le même zip
def _marker_path(zip_basename: str) -> str:
    return f"{OUT_PREFIX}{zip_basename}/_SUCCESS"

def _blob_exists(container_client, blob_path: str) -> bool:
    try:
        container_client.get_blob_client(blob_path).get_blob_properties()
        return True
    except ResourceNotFoundError:
        return False

def _guess_content_type(name: str) -> str:
    return mimetypes.guess_type(name)[0] or "application/octet-stream"

def main():
    svc = BlobServiceClient.from_connection_string(CONN_STR)
    container = svc.get_container_client(CONTAINER)

    # 1) Lister tous les .zip sous raw/zip/
    zip_blobs = list(container.list_blobs(name_starts_with=ZIP_PREFIX))
    if not zip_blobs:
        print("Aucun ZIP trouvé sous raw/zip/. Rien à faire.")
        return

    print(f"{len(zip_blobs)} archive(s) à traiter.")
    processed = 0
    skipped = 0

    for b in zip_blobs:
        zip_blob_path = b.name                    # ex: zip/dis-2024-dept.zip
        zip_filename  = posixpath.basename(b.name)
        zip_base, _   = os.path.splitext(zip_filename)   # ex: dis-2024-dept
        target_root   = f"{OUT_PREFIX}{zip_base}/"       # ex: unzip/dis-2024-dept/

        # 2) Skip si déjà traité (présence du marker)
        marker = _marker_path(zip_base)
        if _blob_exists(container, marker):
            print(f"⏩ SKIP {zip_filename} : déjà décompressé (marker présent).")
            skipped += 1
            continue

        print(f"⬇️ Téléchargement de {zip_blob_path} …")
        # 3) Télécharger le zip en fichier temporaire (plus robuste que tout en mémoire)
        blob_client: BlobClient = container.get_blob_client(zip_blob_path)
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
            downloader = blob_client.download_blob(max_concurrency=4)
            downloader.readinto(tmp)  # stream -> file
            tmp_path = tmp.name

        extracted_count = 0
        # 4) Extraire et uploader chaque entrée
        with zipfile.ZipFile(tmp_path, "r") as zf:
            for info in zf.infolist():
                if info.is_dir():
                    continue
                # chemin interne normalisé (POSIX)
                internal = info.filename.lstrip("./\\").replace("\\", "/")
                dest_path = f"{target_root}{internal}"

                # Idempotence : si le fichier existe déjà, passer
                if _blob_exists(container, dest_path):
                    print(f"   • existe déjà, on saute : {dest_path}")
                    continue

                # Ouvrir l'entrée du zip en stream et uploader
                with zf.open(info, "r") as member_stream:
                    content_type = _guess_content_type(internal)
                    dest_client = container.get_blob_client(dest_path)
                    dest_client.upload_blob(
                        member_stream,
                        overwrite=False,
                        content_settings=ContentSettings(content_type=content_type),
                    )
                print(f"   ✅ upload : {dest_path}")
                extracted_count += 1

        # 5) Créer un marker _SUCCESS avec quelques métadonnées (json/texte)
        ts = datetime.now(timezone.utc).isoformat()
        marker_body = (
            f"zip={zip_blob_path}\n"
            f"output_prefix={target_root}\n"
            f"files_extracted={extracted_count}\n"
            f"timestamp_utc={ts}\n"
        ).encode("utf-8")
        container.get_blob_client(marker).upload_blob(marker_body, overwrite=True,
                                                      content_settings=ContentSettings(content_type="text/plain"))
        print(f"🏁 FIN {zip_filename} → {extracted_count} fichier(s) extraits. Marker : {marker}")
        processed += 1

        # Nettoyage du fichier temporaire
        try:
            os.remove(tmp_path)
        except Exception:
            pass

    print(f"\nRésumé : {processed} zip(s) traités, {skipped} zip(s) ignorés (déjà décompressés).")

if __name__ == "__main__":
    main()
