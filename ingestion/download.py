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
    """Télécharger l'archive AIS2ETA, extraire le CSV et vérifier son empreinte SHA-256.

    Étapes :
    1. Si le fichier cible existe déjà et que force=False, vérifier son empreinte SHA-256.
       S'il correspond à l'empreinte de référence, le conserver sans retélécharger.
    2. Télécharger l'archive zip dans un dossier temporaire isolé.
    3. Extraire le fichier all_data_final.csv de l'archive.
    4. Calculer l'empreinte SHA-256 du CSV extrait et la comparer à CSV_SHA256.
    5. Déplacer atomiquement le fichier vérifié vers son emplacement cible final.

    Args:
        target: Chemin du fichier CSV de sortie (ex. data/raw/all_data_final.csv).
        url: URL source de téléchargement du fichier ZIP (dépôt cut-dicl/ais2eta).
        force: Si True, force le retéléchargement même si le fichier existe déjà.

    Returns:
        tuple[Path, str]: Chemin du fichier final et statut ('déjà présent' ou 'téléchargé').

    Raises:
        ValueError: Si le fichier cible existant a une empreinte invalide ou si le CSV
                    téléchargé ne correspond pas à l'empreinte SHA-256 attendue.
    """
    target = Path(target)
    if target.exists() and not force:
        if sha256_file(target) == CSV_SHA256:
            return target, "déjà présent"
        raise ValueError(f"{target} existe mais son empreinte diffère ; utiliser --force pour le remplacer")
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=target.parent) as temporary:
        archive = Path(temporary) / "all_data_final.zip"
        # Téléchargement avec un timeout de 120 secondes
        with urllib.request.urlopen(url, timeout=120) as response, archive.open("wb") as output:
            shutil.copyfileobj(response, output)
        # Extraction du CSV depuis l'archive zip
        with zipfile.ZipFile(archive) as content:
            member = next(name for name in content.namelist() if name.endswith("all_data_final.csv"))
            extracted = Path(temporary) / "all_data_final.csv"
            with content.open(member) as source, extracted.open("wb") as output:
                shutil.copyfileobj(source, output)
        # Contrôle d'intégrité cryptographique
        digest = sha256_file(extracted)
        if digest != CSV_SHA256:
            raise ValueError(f"Empreinte inattendue du CSV : {digest}")
        extracted.replace(target)
    return target, "téléchargé"


def main():
    """Point d'entrée CLI pour télécharger et contrôler le dataset brut."""
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
