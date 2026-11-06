import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import sys
import threading
import time
import uuid

from ingestion.contract import loads, validate_observation, validate_topic
from ingestion.prepare import ROOT, dumps, sha256_file, stable_id

OBSERVATIONS_TOPIC = "maritime.observations.v1"
LABELS_TOPIC = "maritime.labels.v1"


def records(batch_dir, topic=OBSERVATIONS_TOPIC, labels_topic=LABELS_TOPIC, include_labels=True):
    """Parcourir le lot ligne à ligne ; les labels utilisent un topic distinct."""
    files = [("observations.jsonl", topic)]
    if include_labels:
        files.append(("labels.jsonl", labels_topic))
    for name, destination in files:
        with (Path(batch_dir) / name).open(encoding="utf-8") as source:
            for line in source:
                yield destination, loads(line)


def validate_batch(batch_dir):
    """Vérifier intégrité, contrat et ordre du lot avant toute connexion Kafka."""
    batch_dir = Path(batch_dir)
    manifest = loads((batch_dir / "manifest.json").read_text(encoding="utf-8"))
    for filename in ("observations.jsonl", "labels.jsonl", "rejects.jsonl"):
        if sha256_file(batch_dir / filename) != manifest["files"][filename]:
            raise ValueError(f"Lot modifié ou incomplet : {filename}")
    counts, previous = Counter(), {}
    for topic, event in records(batch_dir):
        if topic == OBSERVATIONS_TOPIC:
            validate_observation(event)
        elif event.get("schema_version") != 1 or not event.get("event_id") or not event.get("vessel_id"):
            raise ValueError("Label sans version, event_id ou vessel_id")
        ordering = (event["event_time"], event["event_id"])
        if topic in previous and ordering <= previous[topic]:
            raise ValueError("Lot non trié ou identifiants dupliqués")
        previous[topic] = ordering
        counts[topic] += 1
    if counts[OBSERVATIONS_TOPIC] != manifest["counts"]["observations"] or counts[LABELS_TOPIC] != manifest["counts"]["labels"]:
        raise ValueError("Comptages différents du manifeste")
    if counts[OBSERVATIONS_TOPIC] == 0:
        raise ValueError("Le lot ne contient aucune observation valide")
    return manifest, counts


def delivery_counts():
    """Initialiser les compteurs avant la connexion, y compris pour un échec réseau."""
    return Counter(enqueued=0, delivered=0, failed=0, enqueue_failed=0, pending=0, stopped=0)


def deliver(producer, messages, receipts, failures, run_id, batch_id, rate=0,
            timeout=30, *, counts=None, stop_requested=lambda: False):
    """Publier puis vider la file, même après erreur ou demande d'arrêt.

    counts est modifié sur place pour conserver un bilan si une exception survient.
    Chaque confirmation conserve l'empreinte exacte du JSON envoyé et son offset.
    """
    counts = delivery_counts() if counts is None else counts

    def callback(event_id, topic, payload_digest):
        def on_delivery(error, message):
            if error is not None:
                counts["failed"] += 1
                failures.write(dumps({"event_id": event_id, "topic": topic, "error": str(error)}) + "\n")
                failures.flush()
            else:
                counts["delivered"] += 1
                receipts.write(dumps({"event_id": event_id, "topic": message.topic(),
                                      "partition": message.partition(), "offset": message.offset(),
                                      "payload_sha256": payload_digest,
                                      "ingestion_run_id": run_id}) + "\n")
                receipts.flush()
        return on_delivery

    try:
        for topic, original in messages:
            if stop_requested():
                counts["stopped"] = 1
                break
            if counts["failed"]:
                raise RuntimeError("Kafka a rejeté un message ; consulter les échecs de livraison")
            event = dict(original, ingestion_run_id=run_id, ingestion_batch_id=batch_id,
                         ingestion_time=datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"))
            try:
                payload = dumps(event).encode("utf-8")
                digest = hashlib.sha256(payload).hexdigest()
                deadline = time.monotonic() + timeout
                while True:
                    if stop_requested():
                        counts["stopped"] = 1
                        break
                    try:
                        # Clé MMSI : ordre par navire. Le temps historique reste dans event_time.
                        producer.produce(topic, key=event["vessel_id"].encode("utf-8"), value=payload,
                                         on_delivery=callback(event["event_id"], topic, digest))
                        counts["enqueued"] += 1
                        break
                    except BufferError:
                        if time.monotonic() >= deadline:
                            raise TimeoutError("File du producteur Kafka saturée")
                        producer.poll(min(0.1, max(0, deadline - time.monotonic())))
            except Exception:
                counts["enqueue_failed"] += 1
                raise
            if counts["stopped"]:
                break
            producer.poll(0)
            if rate:
                # Attentes courtes : un faible débit ne doit pas empêcher Ctrl+C.
                next_send = time.monotonic() + 1 / rate
                while not stop_requested() and time.monotonic() < next_send:
                    producer.poll(min(0.1, max(0, next_send - time.monotonic())))
    except KeyboardInterrupt:
        counts["stopped"] = 1
    finally:
        # Une attente expirée n'est jamais considérée comme une livraison réussie.
        counts["pending"] = max(0, counts["enqueued"] - counts["delivered"] - counts["failed"])
        try:
            counts["pending"] = producer.flush(timeout)
        except BaseException:
            # Des callbacks peuvent avoir abouti avant une erreur du flush.
            counts["pending"] = max(0, counts["enqueued"] - counts["delivered"] - counts["failed"])
            raise
    if counts["failed"] or counts["pending"] or counts["delivered"] != counts["enqueued"]:
        raise RuntimeError("Livraison Kafka incomplète : " + dumps(counts))
    return dict(counts)


def wait_for_topics(producer, topics, timeout, stop_requested=lambda: False):
    """Attendre les métadonnées avec une échéance globale, sans créer de topic."""
    deadline, last_error = time.monotonic() + timeout, "topics absents"
    while time.monotonic() < deadline:
        if stop_requested():
            raise InterruptedError("Arrêt demandé avant publication")
        try:
            metadata = producer.list_topics(timeout=min(3, max(0.01, deadline - time.monotonic())))
            missing = [topic for topic in topics if topic not in metadata.topics or metadata.topics[topic].error]
            if not missing:
                return
            last_error = "Topics absents ou indisponibles : " + ", ".join(missing)
        except Exception as exc:
            last_error = str(exc)
        time.sleep(min(0.1, max(0, deadline - time.monotonic())))
    raise TimeoutError(f"Kafka indisponible après {timeout:g} s : {last_error}")


def publish(batch_dir, bootstrap_servers, report_dir, *, topic=OBSERVATIONS_TOPIC,
            labels_topic=LABELS_TOPIC, include_labels=False, rate=100, timeout=30,
            stop_requested=lambda: False, producer_factory=None):
    """Retourner et enregistrer le bilan d'une tentative de publication.

    Les erreurs locales de contrat sont levées avant la connexion. Les erreurs
    réseau et les interruptions produisent un bilan sans annoncer de succès.
    """
    if not math.isfinite(rate) or rate < 0 or not math.isfinite(timeout) or timeout < 0.2:
        raise ValueError("Débit fini >= 0 et délai fini >= 0,2 s requis")
    validate_topic(topic)
    validate_topic(labels_topic)
    if topic == labels_topic:
        raise ValueError("Observations et labels doivent utiliser des topics différents")
    manifest, _ = validate_batch(batch_dir)
    expected = {topic: manifest["counts"]["observations"]}
    if include_labels:
        expected[labels_topic] = manifest["counts"]["labels"]
    report_dir = Path(report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    run_id, counts = uuid.uuid4().hex, delivery_counts()
    summary = {"ingestion_run_id": run_id, "ingestion_batch_id": stable_id(manifest["files"]),
               "batch": str(Path(batch_dir).resolve()), "topic": topic, "labels_topic": labels_topic,
               "include_labels": include_labels, "expected": expected,
               "preparation": manifest["counts"], "delivery": counts, "status": "failed"}
    receipt_path = report_dir / f"{run_id}.receipts.jsonl"
    summary_path = report_dir / f"{run_id}.summary.json"
    summary["receipts"], summary["summary"] = str(receipt_path.resolve()), str(summary_path.resolve())
    with receipt_path.open("x", encoding="utf-8") as receipts, (report_dir / f"{run_id}.failures.jsonl").open("x", encoding="utf-8") as failures:
        try:
            if producer_factory is None:
                from confluent_kafka import Producer
                producer_factory = Producer
            producer = producer_factory({"bootstrap.servers": bootstrap_servers,
                                         "client.id": "ais2eta-replay", "enable.idempotence": True,
                                         "acks": "all", "compression.type": "gzip", "linger.ms": 10,
                                         "delivery.timeout.ms": int(timeout * 1000),
                                         "request.timeout.ms": min(10000, int(timeout * 1000) - 100)})
            wait_for_topics(producer, expected, timeout, stop_requested)
            deliver(producer, records(batch_dir, topic, labels_topic, include_labels), receipts,
                    failures, run_id, summary["ingestion_batch_id"], rate, timeout,
                    counts=counts, stop_requested=stop_requested)
            summary["status"] = "interrupted" if counts["stopped"] else "delivered"
        except (KeyboardInterrupt, InterruptedError) as exc:
            counts["stopped"] = 1
            summary.update(status="interrupted", error=str(exc) or "Arrêt demandé")
        except Exception as exc:
            summary["error"] = str(exc)
        finally:
            counts["not_attempted"] = sum(expected.values()) - counts["enqueued"] - counts["enqueue_failed"]
            summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch", type=Path, required=True)
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_OBSERVATIONS_TOPIC", OBSERVATIONS_TOPIC))
    parser.add_argument("--labels-topic", default=os.getenv("KAFKA_LABELS_TOPIC", LABELS_TOPIC))
    parser.add_argument("--include-labels", action="store_true", help="Publier aussi les labels dans leur topic séparé")
    parser.add_argument("--publish", action="store_true", help="Publier ; sinon valider sans connexion Kafka")
    parser.add_argument("--rate", type=float, default=100, help="Messages/s ; 0 = sans limitation volontaire")
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--report-dir", type=Path, default=ROOT / "data/reports")
    args = parser.parse_args()
    stopped, previous_handlers = threading.Event(), {}

    def request_stop(signum, frame):
        stopped.set()

    try:
        if not args.publish:
            manifest, counts = validate_batch(args.batch)
            print(dumps({"mode": "validation_only", "counts": counts,
                         "source_timezone": manifest["source_timezone"], "timezone_note": manifest["timezone_note"]}))
            return 0
        for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
            signum = getattr(signal, name, None)
            if signum is not None:
                previous_handlers[signum] = signal.signal(signum, request_stop)
        summary = publish(args.batch, args.bootstrap_servers, args.report_dir, topic=args.topic,
                          labels_topic=args.labels_topic, include_labels=args.include_labels,
                          rate=args.rate, timeout=args.timeout, stop_requested=stopped.is_set)
        print(dumps(summary))
        if summary["status"] != "delivered":
            print(summary.get("error", "Publication interrompue ; consulter le bilan"), file=sys.stderr)
        return {"delivered": 0, "interrupted": 130}.get(summary["status"], 1)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print(f"Publication impossible : {exc}", file=sys.stderr)
        return 1
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    sys.exit(main())
