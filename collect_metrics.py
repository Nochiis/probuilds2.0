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
    cur.execute("SELECT page_id FROM pages WHERE full_url = %s", (full_url,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        "INSERT INTO pages (site_id, path, full_url) VALUES (%s, %s, %s) RETURNING page_id",
        (site_id, path, full_url),
    )
    return cur.fetchone()[0]


def create_run(cur, page_id):
    cur.execute(
        "INSERT INTO runs (page_id, user_agent) VALUES (%s, %s) RETURNING run_id",
        (page_id, USER_AGENT),
    )
    return cur.fetchone()[0]


# -----------------------------------------------------------------------------
# FUNCIONES DE INSERCIÓN
# -----------------------------------------------------------------------------
def insert_metric(cur, table, data):
    cols = ", ".join(data.keys())
    vals = ", ".join(["%s"] * len(data))
    sql = f"INSERT INTO {table} ({cols}) VALUES ({vals}) RETURNING id"
    cur.execute(sql, tuple(data.values()))
    return cur.fetchone()[0]


def insert_main_result(
    cur,
    site_id,
    page_id,
    run_id,
    metric_table,
    metric_id,
    metric_name,
    metric_value,
):
    sql = """
        INSERT INTO main_results
        (site_id, page_id, run_id, metric_table, metric_id, metric_name, metric_value, measured_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    cur.execute(
        sql,
        (
            site_id,
            page_id,
            run_id,
            metric_table,
            metric_id,
            metric_name,
            metric_value,
            datetime.now(),
        ),
    )


# -----------------------------------------------------------------------------
# FUNCIONES DE MÉTRICAS
# -----------------------------------------------------------------------------
def metric_ssl_validity_days(url):
    """Días restantes del certificado SSL."""
    try:
        hostname = url.replace("https://", "").split("/")[0]
        ctx = ssl.create_default_context()
        with ctx.wrap_socket(socket.socket(), server_hostname=hostname) as s:
            s.connect((hostname, 443))
            cert = s.getpeercert()
            exp = datetime.strptime(cert["notAfter"], "%b %d %H:%M:%S %Y %Z")
            days_left = (exp - datetime.utcnow()).days
            return days_left
    except Exception:
        return None


def metric_title_length(html):
    """Longitud del título HTML."""
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.string if soup.title else ""
    return len(title)


def metric_word_count(html):
    """Cantidad total de palabras visibles en la página."""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text()
    return len(text.split())


# -----------------------------------------------------------------------------
# PROCESO PRINCIPAL
# -----------------------------------------------------------------------------
def main():
    conn = connect_db()
    cur = conn.cursor()

    site_name = "Example"
    base_url = "https://example.com"

    site_id = get_or_create_site(cur, site_name, base_url)
    page_id = get_or_create_page(cur, site_id, base_url)
    run_id = create_run(cur, page_id)
    conn.commit()

    # Descargar la página
    resp = requests.get(base_url, headers={"User-Agent": USER_AGENT})
    html = resp.text

    # -----------------------
    # Métrica 1: SSL
    # -----------------------
    ssl_days = metric_ssl_validity_days(base_url)
    metric_id = insert_metric(
        cur,
        "metric_ssl_validity_days",
        {
            "site_id": site_id,
            "page_id": page_id,
            "run_id": run_id,
            "days_left": ssl_days,
            "measured_at": datetime.now(),
        },
    )
    insert_main_result(
        cur,
        site_id,
        page_id,
        run_id,
        "metric_ssl_validity_days",
        metric_id,
        "ssl_validity_days",
        ssl_days,
    )

    # -----------------------
    # Métrica 2: Longitud del título
    # -----------------------
    title_len = metric_title_length(html)
    metric_id = insert_metric(
        cur,
        "metric_title_length",
        {
            "site_id": site_id,
            "page_id": page_id,
            "run_id": run_id,
            "title_length": title_len,
            "measured_at": datetime.now(),
        },
    )
    insert_main_result(
        cur,
        site_id,
        page_id,
        run_id,
        "metric_title_length",
        metric_id,
        "title_length",
        title_len,
    )

    # -----------------------
    # Métrica 3: Palabras visibles
    # -----------------------
    word_count = metric_word_count(html)
    metric_id = insert_metric(
        cur,
        "metric_word_count",
        {
            "site_id": site_id,
            "page_id": page_id,
            "run_id": run_id,
            "word_count": word_count,
            "measured_at": datetime.now(),
        },
    )
    insert_main_result(
        cur,
        site_id,
        page_id,
        run_id,
        "metric_word_count",
        metric_id,
        "word_count",
        word_count,
    )

    # Confirma todos los cambios
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Todas las métricas se insertaron correctamente.")


# -----------------------------------------------------------------------------
if __name__ == "__main__":
    main()
