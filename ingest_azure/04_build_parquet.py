import os
import io
import re
import posixpath
import pandas as pd
from math import ceil
from typing import List, Optional

from azure.storage.blob import BlobServiceClient, ContentSettings
from azure.core.exceptions import ResourceNotFoundError

# ========== Config ==========
CONN_STR        = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
CONTAINER       = "raw"
UNZIP_ROOT      = "unzip"           # dossiers: unzip/dis-2021-dept/, ...
OUT_ROOT        = "parquet_result"  # sortie:   parquet_result/dis-2021-dept/part-00000.parquet, ...
SKIP_IF_SUCCESS = True              # saute l'année si _SUCCESS présent
FORCE_SEP       = None              # ex: ";" pour forcer ; sinon auto-détection
CHUNK_ROWS      = 100_000           # lignes par fichier de sortie
# ============================

YEAR_DIR_RE     = re.compile(r"^unzip/dis-(\d{4})-dept/?$")
RESULT_FILE_RE  = re.compile(r".*/DIS_RESULT.*\.txt$", re.IGNORECASE)

def blob_exists(container_client, path: str) -> bool:
    try:
        container_client.get_blob_client(path).get_blob_properties()
        return True
    except ResourceNotFoundError:
        return False

def list_year_dirs(container_client) -> List[str]:
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

def read_txt_to_df(container_client, blob_path: str) -> Optional[pd.DataFrame]:
    downloader = container_client.get_blob_client(blob_path).download_blob(max_concurrency=4)
    raw = downloader.readall()

    # encodage : UTF-8 -> UTF-8-SIG -> Latin-1
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            text = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        print(f"   ⚠️ encodage inconnu pour {blob_path}, on saute.")
        return None

    head = "\n".join(text.splitlines()[:5])
    sep = detect_sep(head)

    try:
        df = pd.read_csv(io.StringIO(text), sep=sep, dtype=str)
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"   ❌ lecture KO {blob_path}: {e}")
        return None

def write_parquet_parts(container_client, out_dir: str, df: pd.DataFrame):
    import tempfile, pyarrow as pa, pyarrow.parquet as pq

    total = len(df)
    num_parts = max(1, ceil(total / CHUNK_ROWS))
    print(f"   Écriture {num_parts} part(s) -> {out_dir}")

    for i in range(num_parts):
        part = df.iloc[i*CHUNK_ROWS:(i+1)*CHUNK_ROWS]
        if part.empty:
            continue
        table = pa.Table.from_pandas(part, preserve_index=False)
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            pq.write_table(table, tmp.name)
            tmp_path = tmp.name

        part_blob = f"{out_dir}part-{i:05d}.parquet"
        with open(tmp_path, "rb") as f:
            container_client.get_blob_client(part_blob).upload_blob(
                f, overwrite=True,
                content_settings=ContentSettings(content_type="application/octet-stream")
            )
        os.remove(tmp_path)
        print(f"     ⬆️ {part_blob} ({len(part)} lignes)")

def process_year(container_client, year_dir: str):
    m = YEAR_DIR_RE.match(year_dir.rstrip("/"))
    if not m:
        print(f"   ⚠️ pattern inattendu: {year_dir}")
        return
    year = m.group(1)
    out_dir = f"{OUT_ROOT}/dis-{year}-dept/"
    success_marker = f"{out_dir}_SUCCESS"

    if SKIP_IF_SUCCESS and blob_exists(container_client, success_marker):
        print(f"⏩ SKIP {year}: déjà assemblé ({success_marker})")
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

    print(f"➡️  Année {year}: {len(txt_paths)} fichier(s) à assembler")

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

    # écrire en Parquet multi-fichiers
    write_parquet_parts(container_client, out_dir, full)

    # marker _SUCCESS
    body = f"year={year}\nrows={len(full)}\n".encode("utf-8")
    container_client.get_blob_client(success_marker).upload_blob(
        body, overwrite=True, content_settings=ContentSettings(content_type="text/plain")
    )

    print(f"✅ Année {year} → {out_dir} (rows={len(full)})")

def main():
    svc = BlobServiceClient.from_connection_string(CONN_STR)
    container = svc.get_container_client(CONTAINER)

    year_dirs = list_year_dirs(container)
    if not year_dirs:
        print("Aucun dossier 'unzip/dis-YYYY-dept/' trouvé.")
        return

    print(f"{len(year_dirs)} dossier(s) trouvés : {year_dirs}")
    for yd in year_dirs:
        process_year(container, yd)

    print("🎉 Assemblage Parquet DIS_RESULT terminé.")

if __name__ == "__main__":
    main()
