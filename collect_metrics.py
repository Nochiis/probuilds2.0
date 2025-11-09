import os
import psycopg2
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import ssl
import socket

# -----------------------------------------------------------------------------
# CONFIGURACIÓN
# -----------------------------------------------------------------------------
PG_CONN = os.getenv("PG_CONN")  # tu cadena de conexión a Render (configurada en Secrets)
USER_AGENT = "MetricsBot/1.0 (+contact@example.com)"


# -----------------------------------------------------------------------------
# FUNCIONES DE CONEXIÓN Y CREACIÓN
# -----------------------------------------------------------------------------
def connect_db():
    return psycopg2.connect(PG_CONN)


def get_or_create_site(cur, site_name, base_url):
    cur.execute("SELECT site_id FROM sites WHERE base_url = %s", (base_url,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        "INSERT INTO sites (site_name, base_url) VALUES (%s, %s) RETURNING site_id",
        (site_name, base_url),
    )
    return cur.fetchone()[0]


def get_or_create_page(cur, site_id, path):
    full_url = path if path.startswith("http") else f"https://{path}"
