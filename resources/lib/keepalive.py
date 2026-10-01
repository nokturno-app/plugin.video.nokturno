"""Znovu použitá spojení pro čtení hlaviček souborů.

`mediainfo.probe()` čte u jednoho titulu desítky malých výřezů (`Range`) ze stejných
serverů. `urllib` na každý dotaz naváže nové spojení (TCP + TLS), takže zdroj vidí
desítky handshaků místo jednoho. Tady drží fond nečinných spojení na trojici
(schéma, host, port) a dotazy je berou jedno po druhém. Hlavičku `Host` skládá
`http.client` sám (bez přihlašovacích údajů z adresy, s hranatými závorkami u IPv6).

Přesměrování se sleduje ručně a přihlašovací hlavičky (`Authorization`, `Cookie`)
se při přesměrování na jiný host zahazují, stejně jako v `safe_redirect.py`.

Druhé použití: `urlopen()` je náhrada `urllib.request.urlopen` pro nezávislá volání API
(hledání ve zdrojích, TMDB, Cinemeta, Wikidata) — těch je u jednoho titulu desítky na pár
hostů a každé znovu platilo TCP + TLS handshake. Je **vypnutá**, dokud ji vstupní bod
produktu nezapne (`enable()`): testy, které podstrkují `urllib.request.urlopen`, tak dál
fungují a nic nejde omylem po síti. Vypnutá, s proxy, s jiným schématem než http(s)
a po přesměrování volá obyčejné `urllib.request.urlopen`.
"""
import http.client
import io
import socket
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

MAX_IDLE = 4          # nečinných spojení na host
IDLE_TTL = 20         # starší spojení server nejspíš už zavřel
MAX_REDIRECTS = 5
CITLIVE = ("authorization", "cookie", "proxy-authorization")

MAX_BODY = 32 * 1024 * 1024   # větší odpověď API se bere jako chyba, ne jako data do paměti

_LOCK = threading.Lock()
_IDLE = {}            # (schéma, host, port) → [(spojení, čas vrácení)]
_ENABLED = False


def enable(on=True):
    """Zapnout (nebo vypnout) obecné `urlopen()` přes fond spojení."""
    global _ENABLED
    _ENABLED = bool(on)


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


_CTX = None


def _ssl_context():
    """Jeden SSL kontext na proces. `HTTPSConnection` bez něj si staví nový při každém spojení
    (načtení systémových certifikátů, desítky ms) — `urllib` si ho drží v handleru, tady musíme sami."""
    global _CTX
    if _CTX is None:
        ctx = ssl.create_default_context()
        try:
            ctx.set_alpn_protocols(["http/1.1"])
        except (NotImplementedError, AttributeError):
            pass
        _CTX = ctx
    return _CTX


def _new(scheme, host, port, timeout):
    if scheme == "https":
        return http.client.HTTPSConnection(host, port, timeout=timeout, context=_ssl_context())
    return http.client.HTTPConnection(host, port, timeout=timeout)


def _once(url, headers, timeout, want, method="GET", body=None, retry=True):
    """Jeden dotaz → (status, data, hlavičky, Location nebo None). `retry`: zavřené nečinné
    spojení zkusit znovu na novém (u POST jen když je volání bezpečné zopakovat)."""
    parts = urllib.parse.urlsplit(url)
    key = (parts.scheme, parts.hostname, parts.port)
    path = urllib.parse.urlunsplit(("", "", parts.path or "/", parts.query, ""))
    for attempt in (0, 1):
        conn = _take(key) if attempt == 0 else None
        reused = conn is not None
        if conn is None:
            conn = _new(*key, timeout)
        try:
            conn.timeout = timeout
            if conn.sock is not None:
                conn.sock.settimeout(timeout)   # nečinné spojení si pamatuje timeout předchozího dotazu
            conn.request(method, path, body=body, headers=headers)
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
            if reused and retry:
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


class Response:
    """Přečtená odpověď se stejným rozhraním, jaké od `urlopen` čeká zbytek jádra."""

    def __init__(self, url, status, headers, data):
        self.url = url
        self.status = self.code = status
        self.headers = headers
        self._body = io.BytesIO(data)

    def read(self, n=-1):
        return self._body.read(n)

    def getcode(self):
        return self.status

    def geturl(self):
        return self.url

    def info(self):
        return self.headers

    def close(self):
        self._body.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def urlopen(req, timeout=None, idempotent=True):
    """Jako `urllib.request.urlopen(req, timeout)`, ale přes fond spojení.

    Chyby jsou ty, které vyhazuje urllib: `HTTPError` (s tělem) pro stav ≥ 400, `URLError` pro
    chybu spojení. Přesměrování a vše, co fond nezvládá, jde přes obyčejné `urlopen`.
    `idempotent=False`: POST se po zavřeném nečinném spojení nezkouší znovu."""
    if not isinstance(req, urllib.request.Request):
        req = urllib.request.Request(req)
    if not (_ENABLED and req.type in ("http", "https") and available()):
        return _plain(req, timeout)
    headers = {k: v for k, v in req.header_items()}
    body = req.data
    if body is not None and not any(k.lower() == "content-type" for k in headers):
        headers["Content-type"] = "application/x-www-form-urlencoded"
    url = req.full_url
    try:
        status, data, hdrs, _loc = _once(url, headers, timeout, MAX_BODY, req.get_method(), body, idempotent)
    except (OSError, http.client.HTTPException) as err:   # HTTPException není podtřída OSError
        raise urllib.error.URLError(err) from err
    if status in (301, 302, 303, 307, 308):
        return _plain(req, timeout)
    if len(data) > MAX_BODY:
        raise urllib.error.URLError("odpověď je příliš velká")
    if status >= 400:
        raise urllib.error.HTTPError(url, status, http.client.responses.get(status, ""), hdrs, io.BytesIO(data))
    return Response(url, status, hdrs, data)


def _plain(req, timeout):
    return urllib.request.urlopen(req) if timeout is None else urllib.request.urlopen(req, timeout=timeout)
