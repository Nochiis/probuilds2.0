import os
import psycopg2
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import ssl, socket, time, json
from datetime import datetime

PG_CONN = os.getenv("PG_CONN")  # postgres://usuario:clave@host:port/base

USER_AGENT = "MetricsBot/1.0 (+contact@example.com)"
HEADERS = {"User-Agent": USER_AGENT, "Accept": "text/html"}

# Sitios a analizar (puedes agregar varios)
SITES = [
    {
        "site_name": "Example Site",
        "base_url": "https://example.com",
        "pages": ["/", "/about"]
    }
]

def get_cert_days_left(hostname, port=443):
    try:
        ctx = ssl.create_default_context()
        with ctx.wrap_socket(socket.socket(), server_hostname=hostname) as s:
            s.settimeout(5)
            s.connect((hostname, port))
            cert = s.getpeercert()
            exp = ssl.cert_time_to_seconds(cert['notAfter'])
            return int((exp - time.time()) / 86400)
    except Exception:
        return None

def fetch_page(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        return r
    except Exception:
        return None

def analyze_page(base_url, html):
    soup = BeautifulSoup(html, "html.parser")
    result = {}

    # 1 SSL — handled separately in main
    # 2 internal/external links
    links = [a.get("href") for a in soup.find_all("a", href=True)]
    internal = [l for l in links if urlparse(l).netloc == "" or urlparse(l).netloc in base_url]
    external = [l for l in links if urlparse(l).netloc not in base_url and urlparse(l).netloc != ""]
    result["internal_links"] = len(internal)
    result["external_links"] = len(external)

    # 3 images without alt
    images = soup.find_all("img")
    result["images_without_alt"] = sum(1 for img in images if not img.get("alt"))

    # 4 script external ratio
    scripts = soup.find_all("script", src=True)
    external_scripts = [s for s in scripts if urlparse(s["src"]).netloc not in base_url]
    result["external_script_ratio"] = round((len(external_scripts) / len(scripts)) * 100, 2) if scripts else 0

    # 5 title length
    title_tag = soup.find("title")
    result["title_length"] = len(title_tag.text.strip()) if title_tag else 0

    # 6 meta keywords count
    meta_kw = soup.find("meta", attrs={"name": "keywords"})
    result["meta_keywords_count"] = len(meta_kw["content"].split(",")) if meta_kw and meta_kw.get("content") else 0

    # 7 H tag distribution
    result["h1_count"] = len(soup.find_all("h1"))
    result["h2_count"] = len(soup.find_all("h2"))
    result["h3_count"] = len(soup.find_all("h3"))

    # 8 word count
    text = soup.get_text(" ", strip=True)
    result["word_count"] = len(text.split())

    # 9 favicon presence
    favicon = soup.find("link", rel=lambda x: x and "icon" in x.lower())
    result["has_favicon"] = bool(favicon)

    return result

def insert_data(conn, table, data):
    with conn.cursor() as cur:
        keys = ", ".join(data.keys())
        values = ", ".join(["%s"] * len(data))
        sql = f"INSERT INTO {table} ({keys}) VALUES ({values})"
        cur.execute(sql, tuple(data.values()))
    conn.commit()

def ensure_site_page(conn, site, page):
    full_url = urljoin(site["base_url"], page)
    with conn.cursor() as cur:
        # site
        cur.execute("SELECT site_id FROM sites WHERE base_url=%s", (site["base_url"],))
        row = cur.fetchone()
        if row: site_id = row[0]
        else:
            cur.execute("INSERT INTO sites (site_name, base_url) VALUES (%s,%s) RETURNING site_id",
                        (site["site_name"], site["base_url"]))
            site_id = cur.fetchone()[0]
        # page
        cur.execute("SELECT page_id FROM pages WHERE full_url=%s", (full_url,))
        row = cur.fetchone()
        if row: page_id = row[0]
        else:
            cur.execute("INSERT INTO pages (site_id, path, full_url) VALUES (%s,%s,%s) RETURNING page_id",
                        (site_id, page, full_url))
            page_id = cur.fetchone()[0]
        conn.commit()
        return site_id, page_id, full_url

def main():
    conn = psycopg2.connect(PG_CONN)
    with conn.cursor() as cur:
        cur.execute("INSERT INTO runs (user_agent) VALUES (%s) RETURNING run_id", (USER_AGENT,))
        run_id = cur.fetchone()[0]
    conn.commit()

    for site in SITES:
        for page in site["pages"]:
            site_id, page_id, full_url = ensure_site_page(conn, site, page)
            host = urlparse(full_url).hostname
            days_left = get_cert_days_left(host)
            r = fetch_page(full_url)
            if not r: continue

            metrics = analyze_page(site["base_url"], r.text)
            measured_at = datetime.utcnow()

            # Inserta en cada tabla de métricas
            insert_data(conn, "metric_ssl_validity_days",
                        {"site_id": site_id, "page_id": page_id, "run_id": run_id,
                         "measured_at": measured_at, "days_left": days_left, "extra": json.dumps({"host": host})})

            insert_data(conn, "metric_total_internal_links",
                        {"site_id": site_id, "page_id": page_id, "run_id": run_id,
                         "measured_at": measured_at, "internal_links_count": metrics["internal_links"], "extra": None})

            insert_data(conn, "metric_total_external_links",
                        {"site_id": site_id, "page_id": page_id, "run_id": run_id,
                         "measured_at": measured_at, "external_links_count": metrics["external_links"], "extra": None})

            insert_data(conn, "metric_images_without_alt",
                        {"site_id": site_id, "page_id": page_id, "run_id": run_id,
                         "measured_at": measured_at, "images_without_alt": metrics["images_without_alt"], "extra": None})

            insert_data(conn, "metric_script_external_ratio",
                        {"site_id": site_id, "page_id": page_id, "run_id": run_id,
                         "measured_at": measured_at, "external_script_ratio": metrics["external_script_ratio"], "extra": None})

            insert_data(conn, "metric_title_length",
                        {"site_id": site_id, "page_id": page_id, "run_id": run_id,
                         "measured_at": measured_at, "title_length": metrics["title_length"], "extra": None})

            insert_data(conn, "metric_meta_keywords_count",
                        {"site_id": site_id, "page_id": page_id, "run_id": run_id,
                         "measured_at": measured_at, "keywords_count": metrics["meta_keywords_count"], "extra": None})

            insert_data(conn, "metric_h_tag_distribution",
                        {"site_id": site_id, "page_id": page_id, "run_id": run_id,
                         "measured_at": measured_at, "h1_count": metrics["h1_count"],
                         "h2_count": metrics["h2_count"], "h3_count": metrics["h3_count"], "extra": None})

            insert_data(conn, "metric_word_count",
                        {"site_id": site_id, "page_id": page_id, "run_id": run_id,
                         "measured_at": measured_at, "word_count": metrics["word_count"], "extra": None})

            insert_data(conn, "metric_favicon_presence",
                        {"site_id": site_id, "page_id": page_id, "run_id": run_id,
                         "measured_at": measured_at, "has_favicon": metrics["has_favicon"], "extra": None})

            # También guarda un resumen en main_results
            for name, val in {
                "ssl_validity_days": days_left,
                "internal_links": metrics["internal_links"],
                "external_links": metrics["external_links"],
                "images_without_alt": metrics["images_without_alt"],
                "script_external_ratio": metrics["external_script_ratio"],
                "title_length": metrics["title_length"],
                "meta_keywords_count": metrics["meta_keywords_count"],
                "h_tags_total": metrics["h1_count"] + metrics["h2_count"] + metrics["h3_count"],
                "word_count": metrics["word_count"],
                "has_favicon": int(metrics["has_favicon"])
            }.items():
                insert_data(conn, "main_results",
                            {"site_id": site_id, "page_id": page_id, "run_id": run_id,
                             "measured_at": measured_at, "metric_name": name, "metric_value": val, "notes": None})
    conn.close()

if __name__ == "__main__":
    main()
