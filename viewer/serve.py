"""Read-only warehouse viewer: browse tables and run SELECT queries in a
browser instead of exec-ing into a container every time. Adminer can't do
this job -- it only speaks server database protocols (MySQL, Postgres...)
and DuckDB is an embedded, file-based engine with no server to connect to.

DuckDB only allows one process to hold the database file open at a time
(see warehouse/schema.sql), so this opens a fresh read-only connection per
request and closes it right away -- that keeps the window in which it could
collide with a DAG task as short as possible, with a couple of retries for
the rare case it still lands on one."""
import html
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import duckdb

WAREHOUSE_PATH = "/opt/warehouse/indrive.duckdb"
PORT = 8090

PAGE = """<!doctype html>
<html><head><meta charset="utf-8"><title>inDrive warehouse</title>
<style>
  body {{ font-family: system-ui, sans-serif; margin: 2rem; color: #1a1a1a; }}
  h1 {{ font-size: 1.3rem; }}
  table {{ border-collapse: collapse; margin-top: 1rem; }}
  th, td {{ border: 1px solid #ccc; padding: 4px 10px; font-size: 0.9rem; text-align: left; }}
  th {{ background: #f0f0f0; }}
  a {{ color: #2563eb; text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
  .meta {{ color: #666; font-size: 0.85rem; }}
  textarea {{ width: 100%; font-family: monospace; font-size: 0.9rem; }}
  .error {{ color: #b91c1c; }}
</style></head>
<body>
<h1><a href="/">inDrive warehouse</a></h1>
<p><a href="/">tables</a> &middot; <a href="/query">SQL</a></p>
{body}
</body></html>"""

DEFAULT_QUERY = "SELECT * FROM fact_ride_requests ORDER BY requested_at DESC LIMIT 20"


def connect_with_retry(attempts=5, delay_seconds=0.3):
    """The warehouse is locked for the split second a DAG task is writing --
    retry a few times before giving up, instead of failing on the first try."""
    last_error = None
    for _ in range(attempts):
        try:
            return duckdb.connect(WAREHOUSE_PATH, read_only=True)
        except duckdb.IOException as exc:
            last_error = exc
            time.sleep(delay_seconds)
    raise last_error


def list_tables(con):
    return [r[0] for r in con.execute("SHOW TABLES").fetchall()]


def render_home(con):
    rows = []
    for name in list_tables(con):
        count = con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
        rows.append(f'<tr><td><a href="/table?name={name}">{name}</a></td><td>{count}</td></tr>')
    body = (
        '<p class="meta">Read-only view of indrive.duckdb -- refresh anytime, '
        "it never blocks the pipeline.</p>"
        "<table><tr><th>table</th><th>rows</th></tr>" + "".join(rows) + "</table>"
    )
    return PAGE.format(body=body)


def render_table(con, name):
    # name comes from the query string -- only trust it once it matches a
    # real table returned by SHOW TABLES, then it's safe to interpolate
    if name not in list_tables(con):
        return PAGE.format(body=f"<p>No such table: {html.escape(name)}</p>")
    cols = [c[0] for c in con.execute(f"DESCRIBE {name}").fetchall()]
    rows = con.execute(f"SELECT * FROM {name} LIMIT 200").fetchall()
    head = "".join(f"<th>{html.escape(c)}</th>" for c in cols)
    body_rows = "".join(
        "<tr>" + "".join(f"<td>{html.escape(str(v))}</td>" for v in row) + "</tr>" for row in rows
    )
    body = f'<p class="meta">{name} -- first 200 rows</p><table><tr>{head}</tr>{body_rows}</table>'
    return PAGE.format(body=body)


def render_query(con, sql):
    result_html = ""
    if sql.strip():
        try:
            cur = con.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchmany(500)
            head = "".join(f"<th>{html.escape(c)}</th>" for c in cols)
            body_rows = "".join(
                "<tr>" + "".join(f"<td>{html.escape(str(v))}</td>" for v in row) + "</tr>"
                for row in rows
            )
            result_html = f"<table><tr>{head}</tr>{body_rows}</table>"
        except duckdb.Error as exc:
            result_html = f'<p class="error">{html.escape(str(exc))}</p>'
    body = (
        '<p class="meta">Read-only connection: SELECT runs, INSERT/UPDATE/DDL are rejected '
        "by DuckDB itself, not by this page. Results capped at 500 rows.</p>"
        f'<form method="get" action="/query">'
        f'<textarea name="sql" rows="4">{html.escape(sql)}</textarea><br>'
        f'<button type="submit">Run</button></form>'
        + result_html
    )
    return PAGE.format(body=body)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        try:
            con = connect_with_retry()
        except duckdb.IOException:
            self._respond(503, PAGE.format(body="<p>Warehouse is busy, refresh in a second.</p>"))
            return
        try:
            if parsed.path == "/table":
                name = parse_qs(parsed.query).get("name", [""])[0]
                page = render_table(con, name)
            elif parsed.path == "/query":
                sql = parse_qs(parsed.query).get("sql", [DEFAULT_QUERY])[0]
                page = render_query(con, sql)
            else:
                page = render_home(con)
        finally:
            con.close()
        self._respond(200, page)

    def _respond(self, status, page):
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(page.encode("utf-8"))

    def log_message(self, fmt, *args):
        print(f"[viewer] {self.address_string()} {fmt % args}", flush=True)


if __name__ == "__main__":
    print(f"[viewer] serving {WAREHOUSE_PATH} on :{PORT}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
