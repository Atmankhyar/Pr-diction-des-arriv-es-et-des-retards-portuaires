import argparse
import csv
import hashlib
import itertools
import json
import math
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import re
import sqlite3
from zoneinfo import ZoneInfo


ROOT = Path(__file__).resolve().parents[1]
OBSERVATION_COLUMNS = (
    "received_timestamp", "mmsi", "longitude", "latitude", "sog", "cog",
    "rot", "nav_status", "true_heading", "destination", "draught", "breadth",
    "length", "vesselType", "ShipCategory",
)
LABEL_COLUMNS = ("arrival_time_exists", "arrival_time", "actual_eta_in_min")


def dumps(value):
    """Sérialiser en JSON déterministe, sans nombres NaN ou infinis."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False,
                      separators=(",", ":"))


def stable_id(value):
    """Calculer un identifiant SHA-256 indépendant de l'ordre des clés JSON."""
    return hashlib.sha256(dumps(value).encode("utf-8")).hexdigest()


def sha256_file(path):
    """Calculer l'empreinte d'un fichier par blocs, avec une mémoire bornée."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def get_timezone(name):
    """Résoudre le fuseau déclaré ; aucune déduction géographique implicite."""
    return timezone.utc if name == "UTC" else ZoneInfo(name)


def parse_time(value, zone):
    """Convertir une date AIS2ETA en UTC, en refusant les ambiguïtés DST."""
    naive = datetime.strptime(value.strip(), "%d/%m/%Y %H:%M")
    first = naive.replace(tzinfo=zone, fold=0)
    second = naive.replace(tzinfo=zone, fold=1)
    if first.utcoffset() != second.utcoffset():
        raise ValueError("date locale ambigue ou inexistante (changement d'heure)")
    utc = first.astimezone(timezone.utc)
    if utc.astimezone(zone).replace(tzinfo=None) != naive:
        raise ValueError("date locale inexistante")
    return utc


def iso(value):
    """Formater une date UTC à la seconde, précision du contrat historique."""
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def number(value):
    """Lire un nombre fini ou lever ValueError pour le traitement des rejets."""
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("nombre non fini")
    return result


def observation(row, zone, timezone_name):
    """Construire une observation sans cible à partir d'un enregistrement CSV."""
    mmsi = row["mmsi"].strip()
    if not re.fullmatch(r"[1-9][0-9]{8}", mmsi):
        raise ValueError("mmsi doit contenir 9 chiffres")
    event_time = parse_time(row["received_timestamp"], zone)
    latitude, longitude = number(row["latitude"]), number(row["longitude"])
    if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
        raise ValueError("position hors limites")
    flags = []

    def optional(column, minimum, maximum, *, integer=False):
        try:
            result = number(row[column])
            if not minimum <= result <= maximum or (integer and not result.is_integer()):
                raise ValueError("hors limites")
            return int(result) if integer else result
        except (TypeError, ValueError):
            flags.append(column + ":missing_or_invalid")
            return None

    # L'identité dépend des champs observables, jamais des labels ni du numéro de ligne.
    identity = {key: row[key].strip() for key in OBSERVATION_COLUMNS}
    event_id = stable_id(["ais2eta.observation.v1", timezone_name, identity])
    return {
        "schema_version": 1, "source": "ais2eta", "event_id": event_id,
        "event_time": iso(event_time), "source_timezone": timezone_name,
        "source_event_time": row["received_timestamp"],
        "vessel_id": mmsi, "voyage_id": None,
        "latitude": latitude, "longitude": longitude,
        "speed_over_ground_knots": optional("sog", 0, 102.2),
        "course_over_ground_deg": optional("cog", 0, 359.9),
        # Les codes extrêmes décodés restent dans le CSV ; ce ne sont pas des mesures fiables.
        "rate_of_turn_deg_min": optional("rot", -708, 708),
        "navigation_status": optional("nav_status", 0, 14, integer=True),
        "true_heading_deg": optional("true_heading", 0, 359, integer=True),
        "destination": row["destination"].strip() or None,
        "draught_m": optional("draught", 0.1, 25.5),
        "breadth_m": optional("breadth", 0.1, 126),
        "length_m": optional("length", 0.1, 1022),
        "vessel_type": row["vesselType"].strip() or None,
        "ship_category": row["ShipCategory"].strip() or None,
        "quality_flags": flags,
    }


def historical_label(row, event, zone):
    """Conserver la cible historique séparément ; elle ne conditionne pas l'observation."""
    if row.get("arrival_time_exists", "").strip().lower() != "yes":
        raise ValueError("arrivee historique absente")
    arrival = parse_time(row["arrival_time"], zone)
    observed = datetime.fromisoformat(event["event_time"].replace("Z", "+00:00"))
    remaining_minutes = (arrival - observed).total_seconds() / 60
    supplied_minutes = number(row["actual_eta_in_min"])
    if remaining_minutes < 0 or supplied_minutes < 0:
        raise ValueError("duree restante negative")
    if abs(remaining_minutes - supplied_minutes) > 1.01:
        raise ValueError("cible et horodatages divergent de plus d'une minute")
    return {
        "schema_version": 1, "event_id": event["event_id"],
        "vessel_id": event["vessel_id"], "event_time": event["event_time"],
        "voyage_id": stable_id(["ais2eta.historical_portcall.v1", event["vessel_id"],
                                event["destination"], iso(arrival)]),
        "voyage_id_method": "historical_mmsi_destination_arrival_time",
        "actual_arrival_time": iso(arrival),
        "remaining_hours": remaining_minutes / 60,
        "source_actual_eta_minutes": supplied_minutes,
        "label_difference_minutes": remaining_minutes - supplied_minutes,
    }


def prepare(input_path, output_dir, timezone_name, limit=5000, timezone_note="Non verifie aupres du producteur"):
    """Créer un nouveau lot sur disque et retourner son manifeste de validation.

    limit borne les enregistrements source ; zéro signifie le fichier entier.
    Un dossier existant est refusé. Le manifeste n'apparaît qu'après succès.
    """
    if limit < 0:
        raise ValueError("limit doit etre positif ou nul (0 = tout le CSV)")
    zone = get_timezone(timezone_name)
    input_path, output_dir = Path(input_path), Path(output_dir)
    # Vérifier l'en-tête avant de créer les sorties du lot.
    with input_path.open(encoding="utf-8-sig", newline="") as stream:
        columns = next(csv.reader(stream), [])
    if not columns:
        raise ValueError("Fichier CSV vide : en-tete absent")
    missing = set(OBSERVATION_COLUMNS + LABEL_COLUMNS) - set(columns)
    if missing:
        raise ValueError("Colonnes manquantes : " + ", ".join(sorted(missing)))
    if len(set(columns)) != len(columns):
        raise ValueError("Noms de colonnes dupliques")
    source_hash = sha256_file(input_path)
    output_dir.mkdir(parents=True, exist_ok=False)
    counts = Counter(rows_read=0, rejected_observations=0, duplicate_observations=0,
                     rejected_labels=0, conflicting_label_events=0)
    normalized = Counter()
    database_path = output_dir / "staging.sqlite"
    connection = sqlite3.connect(database_path)
    try:
        connection.execute("CREATE TABLE events (event_id TEXT PRIMARY KEY, event_time TEXT, "
                           "observation TEXT, label TEXT, conflict INTEGER DEFAULT 0)")
        with input_path.open(encoding="utf-8-sig", newline="") as source, (output_dir / "rejects.jsonl").open("w", encoding="utf-8", newline="\n") as rejects:
            rows = csv.DictReader(source, strict=True)
            selected = itertools.islice(rows, limit) if limit else rows

            def reject(kind, line, reason, raw):
                if isinstance(raw, dict) and None in raw:
                    raw = {(key if key is not None else "_extra_fields"): value for key, value in raw.items()}
                rejects.write(dumps({"kind": kind, "source_record": line,
                                     "source_line_end": rows.line_num,
                                     "reason": str(reason), "raw": raw}) + "\n")

            for record, row in enumerate(selected, start=2):
                counts["rows_read"] += 1
                try:
                    if None in row or any(value is None for value in row.values()):
                        raise ValueError("nombre de champs CSV incorrect")
                    event = observation(row, zone, timezone_name)
                except (TypeError, ValueError, KeyError) as exc:
                    counts["rejected_observations"] += 1
                    reject("observation", record, exc, row)
                    continue
                normalized.update(event["quality_flags"])
                try:
                    label = dumps(historical_label(row, event, zone))
                except (TypeError, ValueError, KeyError) as exc:
                    counts["rejected_labels"] += 1
                    reject("label", record, exc, row)
                    label = None
                previous = connection.execute("SELECT label, conflict FROM events WHERE event_id = ?", (event["event_id"],)).fetchone()
                if previous is None:
                    connection.execute("INSERT INTO events(event_id, event_time, observation, label) VALUES(?, ?, ?, ?)",
                                       (event["event_id"], event["event_time"], dumps(event), label))
                else:
                    counts["duplicate_observations"] += 1
                    if not previous[1] and previous[0] != label:
                        # Ne jamais choisir arbitrairement entre deux labels contradictoires.
                        counts["conflicting_label_events"] += 1
                        reject("label_conflict", record, "labels differents pour un meme event_id", {"event_id": event["event_id"], "previous_label": previous[0], "row": row})
                        connection.execute("UPDATE events SET label = NULL, conflict = 1 WHERE event_id = ?", (event["event_id"],))
                if counts["rows_read"] % 10000 == 0:
                    connection.commit()
        connection.commit()
        connection.execute("CREATE INDEX events_order ON events(event_time, event_id)")
        counts["observations"] = 0
        counts["labels"] = 0
        first_time = last_time = None
        with (output_dir / "observations.jsonl").open("w", encoding="utf-8", newline="\n") as observations, (output_dir / "labels.jsonl").open("w", encoding="utf-8", newline="\n") as labels:
            for event_time, event_json, label_json in connection.execute("SELECT event_time, observation, label FROM events ORDER BY event_time, event_id"):
                first_time = first_time or event_time
                last_time = event_time
                observations.write(event_json + "\n")
                counts["observations"] += 1
                if label_json is not None:
                    labels.write(label_json + "\n")
                    counts["labels"] += 1
    finally:
        connection.close()
    database_path.unlink()
    manifest = {
        "schema_version": 1, "source_path": str(input_path.resolve()),
        "source_sha256": source_hash, "source_timezone": timezone_name,
        "timezone_note": timezone_note, "source_limit": limit,
        "created_at": iso(datetime.now(timezone.utc)), "counts": dict(counts),
        "normalized_values": dict(normalized), "first_event_time": first_time,
        "last_event_time": last_time,
        "files": {name: sha256_file(output_dir / name) for name in ("observations.jsonl", "labels.jsonl", "rejects.jsonl")},
    }
    # Marqueur de fin : le producteur refuse un lot sans manifeste.
    (output_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=ROOT / "data/raw/all_data_final.csv")
    parser.add_argument("--output", type=Path, required=True, help="Nouveau dossier de lot (ne doit pas exister)")
    parser.add_argument("--source-timezone", required=True, help="Fuseau source IANA, par exemple UTC ou Asia/Nicosia")
    parser.add_argument("--timezone-note", default="Non verifie aupres du producteur")
    parser.add_argument("--limit", type=int, default=5000, help="Lignes source a lire ; 0 = fichier entier")
    args = parser.parse_args()
    try:
        result = prepare(args.input, args.output, args.source_timezone, args.limit, args.timezone_note)
    except (OSError, ValueError, KeyError, csv.Error) as exc:
        parser.exit(1, f"Preparation impossible : {exc}\n")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
