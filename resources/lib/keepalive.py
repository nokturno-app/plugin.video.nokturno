"""Znovu použitá spojení pro čtení hlaviček souborů.

`mediainfo.probe()` čte u jednoho titulu desítky malých výřezů (`Range`) ze stejných
serverů. `urllib` na každý dotaz naváže nové spojení (TCP + TLS), takže zdroj vidí
desítky handshaků místo jednoho. Tady drží fond nečinných spojení na dvojici
(schéma, host:port) a dotazy je berou jedno po druhém.

Přesměrování se sleduje ručně a přihlašovací hlavičky (`Authorization`, `Cookie`)
se při přesměrování na jiný host zahazují, stejně jako v `safe_redirect.py`.
"""
import http.client
import socket
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

MAX_IDLE = 4          # nečinných spojení na host
IDLE_TTL = 20         # starší spojení server nejspíš už zavřel
MAX_REDIRECTS = 5
CITLIVE = ("authorization", "cookie", "proxy-authorization")

_LOCK = threading.Lock()
_IDLE = {}            # (schéma, netloc) → [(spojení, čas vrácení)]


def available():
    """Pool se nepoužije, když je nastavená proxy (`http.client` ji nezná)."""
    return not urllib.request.getproxies()


def _take(key):
    now = time.time()
    with _LOCK:
        lst = _IDLE.get(key) or []
        while lst:
            conn, ts = lst.pop()
            if now - ts <= IDLE_TTL:
                return conn
            conn.close()
    return None


def _give(key, conn):
    with _LOCK:
        lst = _IDLE.setdefault(key, [])
        if len(lst) < MAX_IDLE:
            lst.append((conn, time.time()))
            return
    conn.close()


def _new(scheme, netloc, timeout):
    cls = http.client.HTTPSConnection if scheme == "https" else http.client.HTTPConnection
    return cls(netloc, timeout=timeout)


def _once(url, headers, timeout, want):
    """Jeden dotaz → (status, data, hlavičky, Location nebo None)."""
    parts = urllib.parse.urlsplit(url)
    key = (parts.scheme, parts.netloc)
    path = urllib.parse.urlunsplit(("", "", parts.path or "/", parts.query, ""))
    for attempt in (0, 1):
        conn = _take(key) if attempt == 0 else None
        reused = conn is not None
        if conn is None:
            conn = _new(parts.scheme, parts.netloc, timeout)
        try:
            conn.timeout = timeout
            conn.request("GET", path, headers={"Host": parts.netloc, **headers})
            resp = conn.getresponse()
            data = resp.read(want + 1)
            # spojení jde znovu použít jen když je odpověď přečtená celá
            keep = resp.isclosed() and not resp.will_close
            result = (resp.status, data, resp.headers, resp.headers.get("Location"))
        except socket.timeout:
            conn.close()
            raise
        except (http.client.RemoteDisconnected, http.client.BadStatusLine,
                ConnectionError, TimeoutError, OSError):
            conn.close()
            if reused:
                continue          # spojení zatím zavřel server → zkusit na novém
            raise
        except Exception:
            conn.close()
            raise
        if keep:
            _give(key, conn)
        else:
            conn.close()
        return result


def fetch(url, headers, timeout, want):
    """GET s hlavičkami → (status, data, hlavičky); max `want` bajtů těla.
    Chyba spojení nebo HTTP stav ≥ 400 vyhodí stejnou výjimku jako `urllib`."""
    base_host = urllib.parse.urlsplit(url).hostname
    for _ in range(MAX_REDIRECTS + 1):
        status, data, hdrs, loc = _once(url, headers, timeout, want)
        if status in (301, 302, 303, 307, 308) and loc:
            nxt = urllib.parse.urljoin(url, loc)
            if urllib.parse.urlsplit(nxt).hostname != urllib.parse.urlsplit(url).hostname:
                headers = {k: v for k, v in headers.items() if k.lower() not in CITLIVE}
            url = nxt
            continue
        if status >= 400:
            raise urllib.error.HTTPError(url, status, "", hdrs, None)
        return status, data, hdrs
    raise urllib.error.URLError("příliš mnoho přesměrování")
