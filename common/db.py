"""Écritures dans PostgreSQL : remplacement de tables d'indicateurs et insertions idempotentes."""
import psycopg2

from common import config


def connect():
    return psycopg2.connect(**config.POSTGRES)


def execute(sql, params=None):
    with connect() as connection, connection.cursor() as cursor:
        cursor.execute(sql, params)


def replace_table(df, table):
    """Réécrire une table d'indicateurs en gardant le schéma créé par infra/postgres/init.sql."""
    (df.write.mode("overwrite").option("truncate", "true")
       .jdbc(config.JDBC_URL, table, properties=config.JDBC_PROPERTIES))


def insert_ignore_duplicates(df, table, key):
    """Insérer un DataFrame sans créer de doublon sur la clé.

    Les lignes passent par une table de transit puis INSERT ... ON CONFLICT DO NOTHING :
    rejouer un micro-batch après une reprise ne duplique aucune ligne.
    """
    staging = f"{table}_staging"
    (df.write.mode("overwrite")
       .jdbc(config.JDBC_URL, staging, properties=config.JDBC_PROPERTIES))
    columns = ", ".join(df.columns)
    execute(f"INSERT INTO {table} ({columns}) SELECT {columns} FROM {staging} "
            f"ON CONFLICT ({key}) DO NOTHING; DROP TABLE {staging};")
