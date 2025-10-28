# pip install azure-storage-blob pandas pyarrow
import os
import io
import re
import tempfile
import pandas as pd
from typing import List, Optional
from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceNotFoundError

# ==== Config ====
CONN_STR        = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
CONTAINER       = "raw"
UNZIP_ROOT      = "unzip"            # dossiers: unzip/dis-2021-dept/, ...
OUT_ROOT        = "parquet_result"   # sortie:   parquet_result/dis_result_YYYY.parquet
SKIP_IF_EXISTS  = True               # saute l'année si le .parquet existe déjà
FORCE_SEP       = None               # ex ";" si tu connais le séparateur ; sinon auto
FORCE_ENCODING  = None               # ex "utf-8" ou "latin-1" ; sinon auto
# ===============

YEAR_DIR_RE     = re.compile(r"^unzip/dis-(\d{4})-dept/?$")
RESULT_FILE_RE  = re.compile(r".*/DIS_RESULT.*\.txt$", re.IGNORECASE)

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
        parts = b.name.split("/")
        if len(parts) >= 2 and parts[0] == UNZIP_ROOT and parts[1].startswith("dis-") and parts[1].endswith("-dept"):
            prefixes.add("/".join(parts[:2]) + "/")
    return sorted(prefixes)

def detect_sep(sample: str) -> str:
    if FORCE_SEP:
        return FORCE_SEP
    candidates = [";", ",", "\t", "|"]
    counts = {c: sample.count(c) for c in candidates}
    best = max(counts, key=counts.get)
    return best if counts[best] > 0 else ";"

def decode_bytes(raw: bytes) -> str:
    if FORCE_ENCODING:
        return raw.decode(FORCE_ENCODING)
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    # dernier recours : ne pas échouer
    return raw.decode("latin-1", errors="replace")

def read_txt_to_df(container_client, blob_path: str) -> Optional[pd.DataFrame]:
    downloader = container_client.get_blob_client(blob_path).download_blob(max_concurrency=4)
    raw = downloader.readall()
    text = decode_bytes(raw)
    head = "\n".join(text.splitlines()[:5])
    sep = detect_sep(head)
    try:
        df = pd.read_csv(io.StringIO(text), sep=sep, dtype=str)
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"   ❌ lecture KO {blob_path}: {e}")
        return None

def process_year(container_client, year_dir: str):
    m = YEAR_DIR_RE.match(year_dir.rstrip("/"))
    if not m:
        print(f"   ⚠️ pattern inattendu: {year_dir}")
        return
    year = m.group(1)
    out_blob = f"{OUT_ROOT}/dis_result_{year}.parquet"

    if SKIP_IF_EXISTS and blob_exists(container_client, out_blob):
        print(f"⏩ SKIP {year}: {out_blob} existe déjà.")
        return

    # collecter les fichiers DIS_RESULT*.txt
    txt_paths = []
    for b in container_client.list_blobs(name_starts_with=year_dir):
        if RESULT_FILE_RE.match(b.name):
            txt_paths.append(b.name)
    txt_paths.sort()

    if not txt_paths:
        print(f"   ⚠️ aucun DIS_RESULT*.txt sous {year_dir}")
        return

    print(f"➡️  Année {year}: {len(txt_paths)} fichier(s) à assembler → {out_blob}")

    dfs = []
    for p in txt_paths:
        print(f"   • {p}")
        df = read_txt_to_df(container_client, p)
        if df is not None and len(df) > 0:
            df["_source_blob"] = p
            df["_year"] = year
            dfs.append(df)

    if not dfs:
        print(f"   ⚠️ aucune donnée lisible pour {year}")
        return

    full = pd.concat(dfs, ignore_index=True)

    # écrire un UNIQUE fichier parquet temporaire puis uploader vers Azure
    import pyarrow as pa, pyarrow.parquet as pq
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        table = pa.Table.from_pandas(full, preserve_index=False)
        pq.write_table(table, tmp.name)
        tmp_path = tmp.name

    print(f"   ⬆️ upload vers: {out_blob} (rows={len(full)})")
    with open(tmp_path, "rb") as f:
        container_client.get_blob_client(out_blob).upload_blob(
            f, overwrite=True,
            content_settings=ContentSettings(content_type="application/octet-stream")
        )
    try:
        os.remove(tmp_path)
    except Exception:
        pass

    print(f"✅ Année {year} → {out_blob}")

def main():
    svc = BlobServiceClient.from_connection_string(CONN_STR)
    container = svc.get_container_client(CONTAINER)

    year_dirs = list_year_dirs(container)
    # Garde uniquement les années attendues (facultatif)
    year_dirs = [d for d in year_dirs if re.search(r"dis-(2021|2022|2023|2024|2025)-dept/?$", d)]

    if not year_dirs:
        print("Aucun dossier 'unzip/dis-YYYY-dept/' trouvé.")
        return

    print(f"{len(year_dirs)} dossier(s) trouvés : {year_dirs}")
    for yd in year_dirs:
        process_year(container, yd)

    print("🎉 Assemblage Parquet DIS_RESULT terminé.")

if __name__ == "__main__":
    main()
