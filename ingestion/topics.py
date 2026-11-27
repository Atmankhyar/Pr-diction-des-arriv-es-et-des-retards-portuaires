import argparse
import os
import sys

from ingestion.contract import validate_topic
from ingestion.producer import wait_for_topics


def create_topics(bootstrap_servers, topics, partitions=3, timeout=30):
    """Créer les topics Kafka avec le nombre de partitions et la rétention voulus, puis valider leur disponibilité.

    Étapes :
    1. Valider syntaxiquement chaque nom de topic.
    2. Utiliser confluent_kafka.admin.AdminClient pour créer les topics de manière idempotente.
    3. Rétention fixée à 7 jours (604 800 000 ms), facteur de réplication = 1 (cluster local).
    4. Attendre la propagation des métadonnées sur le broker KRaft.
    5. Vérifier que chaque topic existe, est sans erreur et possède exactement le nombre de partitions attendu.

    Args:
        bootstrap_servers (str): Adresse(s) d'accès aux brokers Kafka (ex. 'kafka:9092').
        topics (list[str]): Noms des topics à créer (ex. ['maritime.observations.v1', 'maritime.labels.v1']).
        partitions (int): Nombre de partitions par topic (défaut = 3 pour paralléliser).
        timeout (float): Délai maximal d'attente en secondes.

    Raises:
        RuntimeError: Si un topic est introuvable, signale une erreur ou n'a pas le bon nombre de partitions.
    """
    from confluent_kafka import KafkaError, KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic
    for topic in topics:
        validate_topic(topic)
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=partitions, replication_factor=1,
                  config={"retention.ms": "604800000"}) for topic in topics],
        request_timeout=timeout,
    )
    for topic, future in futures.items():
        try:
            future.result(timeout=timeout)
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
    # La création confirmée peut précéder la propagation des métadonnées.
    wait_for_topics(admin, topics, timeout)
    metadata = admin.list_topics(timeout=timeout)
    for topic in topics:
        actual = metadata.topics.get(topic)
        if actual is None or actual.error or len(actual.partitions) != partitions:
            raise RuntimeError(f"Nombre de partitions inattendu ou topic inaccessible : {topic}")
        if any(len(partition.replicas) != 1 for partition in actual.partitions.values()):
            raise RuntimeError(f"Réplication différente de 1 : {topic}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    parser.add_argument("--topic", action="append", required=True)
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=30)
    args = parser.parse_args()
    if args.partitions < 1 or not 0 < args.timeout < float("inf") or len(set(args.topic)) != len(args.topic):
        parser.error("Partitions et délai positifs requis ; les topics doivent être distincts")
    try:
        create_topics(args.bootstrap_servers, args.topic, args.partitions, args.timeout)
        print("Topics prêts : " + ", ".join(args.topic))
        return 0
    except Exception as exc:
        print(f"Création des topics impossible : {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
