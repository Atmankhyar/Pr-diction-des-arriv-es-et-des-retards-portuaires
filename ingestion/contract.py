from datetime import datetime
import json
import math
import re


NUMERIC_FIELDS = {
    "latitude": (-90, 90), "longitude": (-180, 180),
    "speed_over_ground_knots": (0, 102.2), "course_over_ground_deg": (0, 359.9),
    "rate_of_turn_deg_min": (-708, 708), "navigation_status": (0, 14),
    "true_heading_deg": (0, 359), "draught_m": (0.1, 25.5),
    "breadth_m": (0.1, 126), "length_m": (0.1, 1022),
}
TEXT_FIELDS = ("destination", "vessel_type", "ship_category")
ENVELOPE_FIELDS = {"ingestion_time", "ingestion_run_id", "ingestion_batch_id"}


def loads(value):
    """Décoder du JSON strict : NaN et Infinity sont refusés."""
    def invalid_constant(constant):
        raise ValueError(f"Constante JSON interdite : {constant}")
    return json.loads(value, parse_constant=invalid_constant)


def utc_time(value):
    """Valider une date ISO 8601 UTC et retourner son objet datetime."""
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError("Horodatage ISO 8601 UTC attendu avec suffixe Z")
    return datetime.fromisoformat(value[:-1] + "+00:00")


def validate_topic(topic):
    """Refuser les noms incompatibles avec Kafka avant tout appel réseau."""
    if not re.fullmatch(r"[A-Za-z0-9._-]{1,249}", topic) or topic in {".", ".."}:
        raise ValueError(f"Nom de topic Kafka invalide : {topic!r}")
    return topic


def validate_observation(event, *, published=False):
    """Vérifier champs, types et bornes sans convertir ni inventer de valeur.

    La liste fermée de champs exclut les labels futurs et les colonnes non vérifiées.
    """
    required = {"schema_version", "source", "event_id", "event_time", "source_timezone",
                "vessel_id", "voyage_id", "quality_flags", *NUMERIC_FIELDS, *TEXT_FIELDS}
    allowed = required | {"source_event_time"}
    if published:
        required |= ENVELOPE_FIELDS
        allowed |= ENVELOPE_FIELDS
    if not isinstance(event, dict) or required - event.keys() or event.keys() - allowed:
        raise ValueError("Champs absents ou inattendus dans l'observation")
    if type(event["schema_version"]) is not int or event["schema_version"] != 1 or event["source"] != "ais2eta":
        raise ValueError("Source ou version de schéma incorrecte")
    if not isinstance(event["event_id"], str) or not re.fullmatch(r"[0-9a-f]{64}", event["event_id"]):
        raise ValueError("event_id SHA-256 attendu")
    if not isinstance(event["vessel_id"], str) or not re.fullmatch(r"[1-9][0-9]{8}", event["vessel_id"]):
        raise ValueError("vessel_id doit rester un identifiant texte à 9 chiffres")
    utc_time(event["event_time"])
    if not isinstance(event["source_timezone"], str) or not event["source_timezone"] or event["voyage_id"] is not None:
        raise ValueError("Fuseau déclaré requis ; voyage_id indisponible dans les observations v1")
    if "source_event_time" in event and not isinstance(event["source_event_time"], str):
        raise ValueError("source_event_time doit être du texte")
    for field, (minimum, maximum) in NUMERIC_FIELDS.items():
        value = event[field]
        if value is None and field not in {"latitude", "longitude"}:
            continue
        if type(value) not in (int, float) or not math.isfinite(value) or not minimum <= value <= maximum:
            raise ValueError(f"Valeur numérique invalide : {field}")
        if field in {"navigation_status", "true_heading_deg"} and type(value) is not int:
            raise ValueError(f"Entier requis : {field}")
    for field in TEXT_FIELDS:
        if event[field] is not None and not isinstance(event[field], str):
            raise ValueError(f"Texte ou null requis : {field}")
    if not isinstance(event["quality_flags"], list) or not all(isinstance(flag, str) for flag in event["quality_flags"]):
        raise ValueError("quality_flags doit être une liste de textes")
    if published:
        utc_time(event["ingestion_time"])
        for field in ("ingestion_run_id", "ingestion_batch_id"):
            if not isinstance(event[field], str) or not event[field]:
                raise ValueError(f"Métadonnée manquante : {field}")
