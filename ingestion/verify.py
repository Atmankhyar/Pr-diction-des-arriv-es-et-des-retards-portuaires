import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
import uuid

from ingestion.contract import ENVELOPE_FIELDS, loads, utc_time, validate_observation
from ingestion.prepare import dumps, stable_id
from ingestion.producer import records, validate_batch


class ReceptionAudit:
    """Auditeur mémoire-borné vérifiant l'exactitude des messages Kafka reçus par rapport au lot attendu.

    Utilise une base de données SQLite temporaire sur disque pour comparer unitairement
    chaque message reçu sans saturer la RAM, même pour des millions de lignes.
    """

    def __init__(self, database, summary):
        """Initialiser la base d'audit et charger les attentes à partir du rapport de publication."""
        self.summary = summary
        self.db = sqlite3.connect(database)
        try:
            self._load(summary)
        except BaseException:
            self.db.close()
            raise

    def _load(self, summary):
        """Indexer les messages attendus du lot et les associer aux accusés de réception du producteur.

        Vérifie qu'un reçu de livraison valide existe pour chaque événement avant d'écouter Kafka.
        """
        self.db.execute("CREATE TABLE expected (topic TEXT, event_id TEXT, vessel_id TEXT, body TEXT, "
                        "partition INTEGER, offset INTEGER, digest TEXT, seen INTEGER DEFAULT 0, "
                        "PRIMARY KEY(topic, event_id), UNIQUE(topic, partition, offset))")
        for topic, event in records(summary["batch"], summary["topic"], summary["labels_topic"], summary["include_labels"]):
            self.db.execute("INSERT INTO expected(topic,event_id,vessel_id,body) VALUES(?,?,?,?)",
                            (topic, event["event_id"], event["vessel_id"], dumps(event)))
        self.expected_count = self.db.execute("SELECT count(*) FROM expected").fetchone()[0]
        if self.expected_count != sum(summary["expected"].values()):
            raise ValueError("Le bilan ne correspond pas au lot attendu")
        with Path(summary["receipts"]).open(encoding="utf-8") as stream:
            for line in stream:
                receipt = loads(line)
                if receipt["ingestion_run_id"] != summary["ingestion_run_id"]:
                    raise ValueError("Confirmation appartenant à une autre exécution")
                updated = self.db.execute("UPDATE expected SET partition=?,offset=?,digest=? "
                                          "WHERE topic=? AND event_id=? AND digest IS NULL",
                                          (receipt["partition"], receipt["offset"], receipt["payload_sha256"],
                                           receipt["topic"], receipt["event_id"]))
                if updated.rowcount != 1:
                    raise ValueError("Confirmation inattendue ou dupliquée")
        if self.db.execute("SELECT count(*) FROM expected WHERE digest IS NULL").fetchone()[0]:
            raise ValueError("Certaines confirmations de livraison manquent")
        self.db.commit()

    def accept(self, topic, key, payload, partition, offset):
        """Vérifier un message lu depuis Kafka et confirmer son intégrité stricte.

        Contrôles effectués :
        1. Ignore les messages ne correspondant pas au run_id audité.
        2. Valide la présence des champs enveloppe d'ingestion.
        3. Vérifie que la clé Kafka correspond exactement au MMSI attendu.
        4. Vérifie que le corps du message est identique au JSON préparé d'origine.
        5. Vérifie que les coordonnées (partition, offset) concordent avec le reçu du producteur.
        6. Vérifie l'empreinte SHA-256 du payload.
        7. Détecte et interdit formellement les doublons d'event_id au sein d'une même exécution.

        Args:
            topic (str): Nom du topic lu.
            key (bytes): Clé du message Kafka (MMSI encodé).
            payload (bytes): Corps JSON du message.
            partition (int): Numéro de partition Kafka.
            offset (int): Numéro d'offset Kafka.

        Returns:
            bool: True si le message appartient à l'exécution et a été validé, False sinon.

        Raises:
            ValueError: Si une altération, coordonnée incorrecte ou doublon est détecté.
        """
        event = loads(payload)
        if not isinstance(event, dict):
            raise ValueError("Objet JSON attendu dans Kafka")
        if event.get("ingestion_run_id") != self.summary["ingestion_run_id"]:
            return False
        if topic == self.summary["topic"]:
            validate_observation(event, published=True)
        utc_time(event["ingestion_time"])
        if event["ingestion_batch_id"] != self.summary["ingestion_batch_id"]:
            raise ValueError("Identifiant de lot reçu incorrect")
        expected = self.db.execute("SELECT vessel_id,body,partition,offset,digest,seen FROM expected "
                                   "WHERE topic=? AND event_id=?", (topic, event["event_id"])).fetchone()
        if expected is None:
            raise ValueError("event_id inattendu dans cette exécution")
        vessel_id, body, wanted_partition, wanted_offset, digest, seen = expected
        if key != vessel_id.encode("utf-8"):
            raise ValueError("Clé Kafka différente du MMSI attendu")
        original = {field: value for field, value in event.items() if field not in ENVELOPE_FIELDS}
        if dumps(original) != body:
            raise ValueError("Le contenu reçu diffère de l'observation préparée")
        if (partition, offset) != (wanted_partition, wanted_offset):
            raise ValueError("Coordonnées Kafka différentes de la confirmation")
        if hashlib.sha256(payload).hexdigest() != digest:
            raise ValueError("Empreinte du JSON reçu différente du JSON envoyé")
        if seen:
            raise ValueError("event_id reçu plusieurs fois dans cette exécution")
        self.db.execute("UPDATE expected SET seen=1 WHERE topic=? AND event_id=?", (topic, event["event_id"]))
        return True

    def result(self):
        """Retourner le décompte des messages attendus, reçus et manquants."""
        received = self.db.execute("SELECT count(*) FROM expected WHERE seen=1").fetchone()[0]
        return {"expected": self.expected_count, "received": received,
                "missing": self.expected_count - received, "content_verified": True}

    def close(self):
        """Fermer proprement la connexion à la base d'audit SQLite."""
        self.db.close()


def verify(summary_path, bootstrap_servers, timeout=60):
    """Contrôler de bout en bout une exécution de publication en lisant Kafka directement.

    Fonctionnement :
    1. Charge le fichier de résumé de publication (summary.json).
    2. Instancie un consommateur Kafka éphémère (sans commit d'offsets pour ne pas perturber les autres consommateurs).
    3. Calcule la plage d'offsets [low, high] (watermarks) sur chaque partition des topics cibles.
    4. Lit tous les messages de la plage et les valide avec ReceptionAudit.accept().
    5. Valide que 100 % des messages attendus ont été reçus sans aucune perte ni corruption.

    Args:
        summary_path (Path | str): Chemin du fichier de bilan .summary.json.
        bootstrap_servers (str): Adresses des brokers Kafka.
        timeout (float): Délai d'attente maximum en secondes.

    Returns:
        dict: Résultat de l'audit avec statut 'verified', expected, received, missing.

    Raises:
        RuntimeError: Si des messages manquent ou si une partition est corrompue.
    """
    from confluent_kafka import Consumer, KafkaError, TopicPartition
    summary_path = Path(summary_path)
    summary = loads(summary_path.read_text(encoding="utf-8"))
    if summary["status"] != "delivered":
        raise ValueError("Publication incomplète : contrôler le bilan puis rejouer le lot")
    manifest, _ = validate_batch(summary["batch"])
    if stable_id(manifest["files"]) != summary["ingestion_batch_id"]:
        raise ValueError("Le lot ne correspond plus à la publication")
    consumer = Consumer({"bootstrap.servers": bootstrap_servers,
                         "group.id": "ais2eta-audit-" + uuid.uuid4().hex,
                         "enable.auto.commit": False, "enable.partition.eof": True,
                         "allow.auto.create.topics": False})
    deadline = time.monotonic() + timeout

    def remaining():
        value = deadline - time.monotonic()
        if value <= 0:
            raise TimeoutError("Échéance du contrôle Kafka atteinte")
        return value

    try:
        # Aucun offset de groupe n'est modifié : le contrôle est répétable.
        metadata = consumer.list_topics(timeout=remaining())
        assignments, ends = [], {}
        for topic in summary["expected"]:
            if topic not in metadata.topics or metadata.topics[topic].error:
                raise RuntimeError(f"Topic absent ou inaccessible : {topic}")
            for partition in metadata.topics[topic].partitions:
                low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=remaining())
                if low < high:
                    assignments.append(TopicPartition(topic, partition, low))
                    ends[(topic, partition)] = high
        with tempfile.TemporaryDirectory(prefix="reception-", dir=summary_path.resolve().parent) as temporary:
            audit = ReceptionAudit(str(Path(temporary) / "audit.sqlite"), summary)
            try:
                if assignments:
                    consumer.assign(assignments)
                while ends:
                    message = consumer.poll(min(1, remaining()))
                    if message is None:
                        continue
                    coordinate = (message.topic(), message.partition())
                    if coordinate not in ends:
                        continue
                    if message.error():
                        if message.error().code() != KafkaError._PARTITION_EOF:
                            raise RuntimeError(str(message.error()))
                        if message.offset() >= ends[coordinate]:
                            del ends[coordinate]
                        continue
                    high = ends[coordinate]
                    if message.offset() < high:
                        audit.accept(message.topic(), message.key(), message.value(), message.partition(), message.offset())
                    if message.offset() + 1 >= high:
                        del ends[coordinate]
                result = audit.result()
                if result["missing"]:
                    raise RuntimeError("Messages attendus absents : " + dumps(result))
                return dict(result, ingestion_run_id=summary["ingestion_run_id"], status="verified")
            finally:
                audit.close()
    finally:
        consumer.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    parser.add_argument("--timeout", type=float, default=60)
    args = parser.parse_args()
    if not 0 < args.timeout < float("inf"):
        parser.error("timeout doit être fini et strictement positif")
    try:
        result = verify(args.summary, args.bootstrap_servers, args.timeout)
        print(json.dumps(result, ensure_ascii=False))
        return 0
    except Exception as exc:
        print(f"Vérification échouée : {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
