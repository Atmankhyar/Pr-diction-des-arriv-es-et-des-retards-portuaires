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
    """Sérialiser un objet Python en chaîne JSON déterministe et compacte.

    Garanties :
    - Clés triées alphabétiquement (sort_keys=True) pour garantir l'unicité du hachage.
    - Caractères UTF-8 préservés sans échappement (ensure_ascii=False).
    - Constantes NaN, Infinity et -Infinity formellement interdites (allow_nan=False).
    - Séparateurs minimaux sans espaces superflus (',', ':').

    Args:
        value: Dictionnaire, liste ou type primitif JSON.

    Returns:
        str: Chaîne JSON normalisée.
    """
    return json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False,
                      separators=(",", ":"))


def stable_id(value):
    """Calculer un identifiant SHA-256 déterministe et reproductible pour un objet.

    L'objet est d'abord sérialisé via dumps() pour éliminer toute variation liée
    à l'ordre des clés dans les dictionnaires.

    Args:
        value: Objet Python (dictionnaire, liste, tuple de valeurs primitives).

    Returns:
        str: Empreinte hexadécimale SHA-256 de 64 caractères.
    """
    return hashlib.sha256(dumps(value).encode("utf-8")).hexdigest()


def sha256_file(path):
    """Calculer l'empreinte SHA-256 d'un fichier par blocs de 1 Mo (mémoire bornée).

    Args:
        path (Path | str): Chemin d'accès au fichier sur le disque.

    Returns:
        str: Empreinte hexadécimale SHA-256 en minuscules.
    """
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def get_timezone(name):
    """Résoudre un nom de fuseau horaire IANA en objet timezone Python.

    Args:
        name (str): Nom du fuseau horaire (ex. 'UTC', 'Asia/Nicosia').

    Returns:
        timezone | ZoneInfo: Objet de fuseau horaire correspondant.
    """
    return timezone.utc if name == "UTC" else ZoneInfo(name)


def parse_time(value, zone):
    """Convertir une date du dataset AIS2ETA (format JJ/MM/AAAA HH:MM) en UTC strict.

    Détecte et rejette les ambiguïtés de passage à l'heure d'été/hiver (DST fold)
    ou les heures locales inexistantes.

    Args:
        value (str): Chaîne horodatée source.
        zone (timezone | ZoneInfo): Fuseau horaire d'interprétation.

    Returns:
        datetime: Objet datetime aware calé sur UTC (+00:00).

    Raises:
        ValueError: Si la date est ambiguë ou inexistante lors d'un changement d'heure.
    """
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
    """Formater un objet datetime UTC au format ISO 8601 strict à la seconde (suffixe 'Z').

    Args:
        value (datetime): Objet date et heure en UTC.

    Returns:
        str: Chaîne au format 'AAAA-MM-JJTHH:MM:SSZ'.
    """
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def number(value):
    """Convertir une chaîne en flottant fini en rejetant formellement NaN et infinis.

    Args:
        value: Valeur numérique ou chaîne convertible en float.

    Returns:
        float: Valeur flottante finie.

    Raises:
        ValueError: Si le nombre n'est pas fini (math.isnan ou math.isinf).
    """
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("nombre non fini")
    return result


def observation(row, zone, timezone_name):
    """Construire un dictionnaire d'observation AIS observable à partir d'une ligne du CSV.

    Règles anti-fuite et qualité :
    1. Validation du MMSI (vessel_id) : 9 chiffres requis.
    2. Conversion de la date 'received_timestamp' en UTC.
    3. Contrôle des coordonnées géographiques (-90 <= lat <= 90, -180 <= lon <= 180).
    4. Validation des mesures capteurs (sog, cog, rot, nav_status, true_heading, dimensions).
       Les valeurs hors bornes ou manquantes sont consignées dans 'quality_flags' sans invalider
       toute l'observation si la position et l'horodatage restent exploitables.
    5. Calcul de l'event_id stable à partir des champs observables uniquement.
    6. Exclut formellement les colonnes cibles (arrival_time, actual_eta_in_min).

    Args:
        row (dict): Ligne de données extraite du CSV.
        zone: Objet fuseau horaire pour la conversion UTC.
        timezone_name (str): Nom du fuseau d'origine.

    Returns:
        dict: Observation normalisée conforme au schéma 'ais2eta.observation.v1'.

    Raises:
        ValueError: Si le MMSI ou les coordonnées géographiques sont invalides.
    """
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
    """Créer le label historique (vérité terrain) associé à une observation AIS.

    Garanties :
    - Vérifie la présence de l'arrivée portuaire effective ('arrival_time_exists' == 'yes').
    - Calcule remaining_hours = (arrival_time - observation_time) en heures.
    - Contrôle de cohérence : l'écart entre le temps calculé et la colonne source
      'actual_eta_in_min' doit être inférieur ou égal à 1,01 minute.
    - Associe un voyage_id déterministe pour grouper les observations d'une même escale.

    Args:
        row (dict): Ligne de données extraite du CSV.
        event (dict): Dictionnaire de l'observation correspondante.
        zone: Fuseau horaire source.

    Returns:
        dict: Dictionnaire du label historique associé à l'event_id.

    Raises:
        ValueError: Si l'arrivée est absente, si remaining_hours est négatif,
                    ou si le label calculé diverge de la colonne source.
    """
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
    """Préparer un lot de données complet à partir du CSV brut et générer le manifeste d'audit.

    Étapes principales :
    1. Validation des en-têtes et des colonnes obligatoires dans le CSV source.
    2. Calcul de l'empreinte SHA-256 du fichier d'entrée.
    3. Création d'une base SQLite temporaire (staging.sqlite) pour :
       - Stocker les événements et détecter les doublons sur event_id.
       - Gérer les conflits de labels (si deux lignes identiques ont des cibles divergentes).
       - Trier les événements chronologiquement par (event_time, event_id).
    4. Écriture des fichiers de sortie :
       - 'observations.jsonl' : observations sans cible.
       - 'labels.jsonl' : cibles associées.
       - 'rejects.jsonl' : lignes rejetées avec le motif précis de l'échec.
    5. Nettoyage de la base temporaire et génération de 'manifest.json' (compteurs, dates, SHA-256).

    Args:
        input_path (Path | str): Chemin du CSV brut source.
        output_dir (Path | str): Dossier destination (ne doit pas exister préalablement).
        timezone_name (str): Fuseau horaire source (ex. 'UTC').
        limit (int): Nombre maximum de lignes à lire (0 = fichier complet).
        timezone_note (str): Note documentaire associée au fuseau.

    Returns:
        dict: Manifeste complet du lot préparé.

    Raises:
        ValueError: Si limit est négatif, si le dossier existe déjà, ou si le CSV est invalide.
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
