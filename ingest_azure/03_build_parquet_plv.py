# pip install azure-storage-blob pandas pyarrow chardet
import os
import io
import re
import sys
import posixpath
import pandas as pd
from typing import List, Optional
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceNotFoundError

CONN_STR    = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
CONTAINER   = "raw"
UNZIP_ROOT  = "unzip"     # dossiers: unzip/dis-2021-dept, unzip/dis-2022-dept, ...
PARQUET_ROOT= "parquet_plv"   # sortie:   parquet_plv/dis-2021-dept.parquet, etc.
SKIP_IF_PARQUET_EXISTS = True

YEAR_DIR_PATTERN = re.compile(r"^unzip/dis-(\d{4})-dept/?$")
PLV_FILE_REGEX   = re.compile(r".*/DIS_PLV.*\.txt$", re.IGNORECASE)

def blob_exists(container_client, path: str) -> bool:
    try:
        container_client.get_blob_client(path).get_blob_properties()
        return True
    except ResourceNotFoundError:
        return False

def list_year_dirs(container_client) -> List[str]:
    """Retourne les prefixes 'unzip/dis-YYYY-dept/' existants."""
    prefixes = set()
    for b in container_client.list_blobs(name_starts_with=f"{UNZIP_ROOT}/"):
        # On isole "unzip/dis-YYYY-dept/" depuis le chemin du blob
        parts = b.name.split("/")
        if len(parts) >= 2 and parts[0] == UNZIP_ROOT and parts[1].startswith("dis-") and parts[1].endswith("-dept"):
            prefixes.add("/".join(parts[:2]) + "/")
    return sorted(prefixes)

def detect_sep(sample: str) -> str:
    # essaie ; , \t, | — ajuste si besoin
    candidates = [";", ",", "\t", "|"]
    counts = {c: sample.count(c) for c in candidates}
    sep = max(counts, key=counts.get)
    return sep if counts[sep] > 0 else ";"

def read_txt_blob_to_df(container_client, blob_path: str) -> Optional[pd.DataFrame]:
    # télécharge en mémoire (si trop gros, on peut streamer par chunks)
    downloader = container_client.get_blob_client(blob_path).download_blob(max_concurrency=4)
    raw = downloader.readall()

    # encodage : UTF-8 puis fallback Latin-1
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            txt = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        print(f"   ⚠️ encodage inconnu pour {blob_path}, on saute.")
        return None

    # détection séparateur sur un petit échantillon
    head = "\n".join(txt.splitlines()[:5])
    sep = detect_sep(head)

    # lecture pandas; tout en string pour éviter les surprises de typage
    try:
        df = pd.read_csv(io.StringIO(txt), sep=sep, dtype=str)
        # normalise les noms de colonnes
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"   ❌ échec lecture {blob_path}: {e}")
        return None

def process_year(container_client, year_dir: str):
    """
    year_dir ex: 'unzip/dis-2021-dept/'
    """
    m = YEAR_DIR_PATTERN.match(year_dir.rstrip("/"))
    if not m:
        print(f"   ⚠️ pattern inattendu: {year_dir}, on saute.")
        return
    year = m.group(1)
    out_name = f"dis-plv-{year}.parquet"
    out_blob = f"{PARQUET_ROOT}/{out_name}"

    if SKIP_IF_PARQUET_EXISTS and blob_exists(container_client, out_blob):
        print(f"⏩ SKIP année {year}: {out_blob} existe déjà.")
        return

    # lister tous les .txt qui contiennent DIS_PLV
    txt_paths = []
    for b in container_client.list_blobs(name_starts_with=year_dir):
        if PLV_FILE_REGEX.match(b.name):
            txt_paths.append(b.name)
    txt_paths.sort()

    if not txt_paths:
        print(f"   ⚠️ aucun fichier DIS_PLV*.txt trouvé sous {year_dir}")
        return

    print(f"➡️  Année {year}: {len(txt_paths)} fichiers DIS_PLV*.txt à assembler…")

    dfs = []
    for p in txt_paths:
        print(f"   • {p}")
        df = read_txt_blob_to_df(container_client, p)
        if df is not None and len(df) > 0:
            # Ajoute colonnes de traçabilité
            df["_source_blob"] = p
            df["_year"] = year
            dfs.append(df)

    if not dfs:
        print(f"   ⚠️ aucune donnée lisible pour {year}.")
        return

    full = pd.concat(dfs, ignore_index=True)

    # écriture parquet en local puis upload
    import tempfile, pyarrow as pa, pyarrow.parquet as pq
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        table = pa.Table.from_pandas(full, preserve_index=False)
        pq.write_table(table, tmp.name)
        tmp_path = tmp.name

    print(f"   ⬆️ upload vers: {out_blob}")
    with open(tmp_path, "rb") as f:
        container_client.get_blob_client(out_blob).upload_blob(
            f, overwrite=True,
            content_settings=ContentSettings(content_type="application/octet-stream")
        )

    try:
        os.remove(tmp_path)
    except Exception:
        pass

    print(f"✅ année {year} → {out_blob} (rows={len(full)})")

def main():
    svc = BlobServiceClient.from_connection_string(CONN_STR)
    container = svc.get_container_client(CONTAINER)

    year_dirs = list_year_dirs(container)
    if not year_dirs:
        print("Aucun dossier 'unzip/dis-YYYY-dept/' trouvé.")
        sys.exit(0)

    print(f"{len(year_dirs)} dossier(s) année trouvé(s): {year_dirs}")
    for d in year_dirs:
        process_year(container, d)

    print("🎉 Assemblage Parquet terminé.")

if __name__ == "__main__":
    main()
