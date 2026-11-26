"""Extraire d'un lot préparé une fenêtre temporelle à rejouer comme flux "temps réel".

Usage typique : après l'entraînement, rejouer lentement les observations de la période de test
(jamais vues par le modèle) pour la démonstration de la voie chaude.

Le lot produit a la même structure qu'un lot de prepare.py (observations, labels, rejects, manifest)
et passe donc les mêmes contrôles d'intégrité du producteur et de verify.py.
"""
import argparse
import json
from pathlib import Path

from ingestion.contract import loads, utc_time
from ingestion.prepare import sha256_file
from ingestion.producer import validate_batch


def slice_batch(source_dir, output_dir, from_time=None, until_time=None, limit=0):
    source_dir, output_dir = Path(source_dir), Path(output_dir)
    manifest, _ = validate_batch(source_dir)  # refuse un lot source modifié
    start = utc_time(from_time) if from_time else None
    end = utc_time(until_time) if until_time else None
    output_dir.mkdir(parents=True, exist_ok=False)

    def inside(event):
        moment = utc_time(event["event_time"])
        return (start is None or moment >= start) and (end is None or moment < end)

    kept_ids, counts = set(), {"observations": 0, "labels": 0}
    with (source_dir / "observations.jsonl").open(encoding="utf-8") as source, \
            (output_dir / "observations.jsonl").open("w", encoding="utf-8", newline="\n") as target:
        for line in source:
            event = loads(line)
            if inside(event):
                target.write(line)
                kept_ids.add(event["event_id"])
                counts["observations"] += 1
                if limit and counts["observations"] >= limit:
                    break
    with (source_dir / "labels.jsonl").open(encoding="utf-8") as source, \
            (output_dir / "labels.jsonl").open("w", encoding="utf-8", newline="\n") as target:
        for line in source:
            if loads(line)["event_id"] in kept_ids:
                target.write(line)
                counts["labels"] += 1
    (output_dir / "rejects.jsonl").write_text("", encoding="utf-8")
    if not counts["observations"]:
        raise ValueError("Aucune observation dans la fenêtre demandée")
    sliced = dict(manifest, counts=dict(manifest["counts"], **counts),
                  slice={"source_batch": str(source_dir.resolve()), "from_time": from_time,
                         "until_time": until_time, "limit": limit},
                  files={name: sha256_file(output_dir / name)
                         for name in ("observations.jsonl", "labels.jsonl", "rejects.jsonl")})
    (output_dir / "manifest.json").write_text(json.dumps(sliced, ensure_ascii=False, indent=2),
                                              encoding="utf-8")
    return sliced


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch", type=Path, required=True, help="Lot source produit par prepare.py")
    parser.add_argument("--output", type=Path, required=True, help="Nouveau dossier (ne doit pas exister)")
    parser.add_argument("--from-time", help="Début inclus, ISO UTC, ex. 2020-08-20T00:00:00Z")
    parser.add_argument("--until-time", help="Fin exclue, ISO UTC")
    parser.add_argument("--limit", type=int, default=0, help="Nombre max d'observations ; 0 = toutes")
    args = parser.parse_args()
    try:
        result = slice_batch(args.batch, args.output, args.from_time, args.until_time, args.limit)
    except (OSError, ValueError, KeyError) as exc:
        parser.exit(1, f"Extraction impossible : {exc}\n")
    print(json.dumps({"counts": result["counts"], "slice": result["slice"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
