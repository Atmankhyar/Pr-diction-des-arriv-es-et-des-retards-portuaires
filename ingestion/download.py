"""Télécharger le dataset AIS2ETA (licence Apache 2.0) dans data/raw/all_data_final.csv.

Source : https://github.com/cut-dicl/ais2eta (fichier zip versionné dans le dépôt des auteurs).
L'empreinte SHA-256 du CSV est vérifiée pour garantir que tout le groupe travaille sur les mêmes données.
"""
import argparse
import hashlib
from pathlib import Path
import shutil
import tempfile
import urllib.request
import zipfile

from ingestion.prepare import ROOT, sha256_file

URL = ("https://github.com/cut-dicl/ais2eta/raw/main/"
       "dataset%20generation%20python%20code/all_data_final.zip")
CSV_SHA256 = "b60bff0bd0e0b7b49bcfc24d064d382ce256fab8312b56354dda1c64d3e4c1d9"


def download(target, url=URL, force=False):
    target = Path(target)
    if target.exists() and not force:
        if sha256_file(target) == CSV_SHA256:
            return target, "déjà présent"
        raise ValueError(f"{target} existe mais son empreinte diffère ; utiliser --force pour le remplacer")
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=target.parent) as temporary:
        archive = Path(temporary) / "all_data_final.zip"
        with urllib.request.urlopen(url, timeout=120) as response, archive.open("wb") as output:
            shutil.copyfileobj(response, output)
        with zipfile.ZipFile(archive) as content:
            member = next(name for name in content.namelist() if name.endswith("all_data_final.csv"))
            extracted = Path(temporary) / "all_data_final.csv"
            with content.open(member) as source, extracted.open("wb") as output:
                shutil.copyfileobj(source, output)
        digest = sha256_file(extracted)
        if digest != CSV_SHA256:
            raise ValueError(f"Empreinte inattendue du CSV : {digest}")
        extracted.replace(target)
    return target, "téléchargé"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "data/raw/all_data_final.csv")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    try:
        path, status = download(args.output, force=args.force)
    except (OSError, ValueError, StopIteration) as exc:
        parser.exit(1, f"Téléchargement impossible : {exc}\n")
    print(f"{path} : {status} (sha256 {CSV_SHA256[:12]}…)")


if __name__ == "__main__":
    main()
