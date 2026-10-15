"""Vlastní úložiště přes SMB – sdílená složka Windows nebo Samba na NAS (P23).

Jádro samo SMB neumí, dostane backend se třemi metodami nad cestou uvnitř sdílené
složky (lomítka `/`, bez úvodního, kořen = ""):

    listdir(cesta)              → [(jméno, je_složka, velikost)]
    read(cesta, odkud, kolik)   → bajty
    size(cesta)                 → velikost souboru

Kodi má SMB v sobě (`xbmcvfs`), backend si staví doplněk sám. Home Assistant a Stremio
berou `PysmbBackend` nad knihovnou pysmb (SMB2, NTLMv2), přibalenou v `_vendor/` (viz
README tamtéž) – integrace ani aplikace nemají žádné `requirements`. Import je líný:
bez knihovny hlásí úložiště chybu, ostatní zdroje jedou dál.
"""
import io
import os
import re
import sys
import threading

CHUNK = 1024 * 1024   # po kolika bajtech posílá proxy soubor ze SMB přehrávači


class SmbUnavailable(OSError):
    """Knihovna pysmb tu není (starší zavaděč aplikace, neúplná instalace)."""


class SmbAuthError(OSError):
    """Server odmítl jméno nebo heslo."""


def _vendor_path():
    """Přibalené knihovny (`_vendor/`) na konec `sys.path`: pysmb importuje `nmb`, `pyasn1`
    a `tqdm` absolutně, a co je v systému nainstalované, má přednost."""
    vendor = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_vendor")
    if os.path.isdir(vendor) and vendor not in sys.path:
        sys.path.append(vendor)


def _split_user(username):
    """`DOMÉNA\\jméno` → (jméno, doména); bez domény (jméno, "")."""
    username = str(username or "")
    if "\\" in username:
        domain, _sep, user = username.partition("\\")
        return user, domain
    return username, ""


class PysmbBackend:
    """SMB přes pysmb. Spojení se drží v malém fondu: pysmb spojení není bezpečné pro
    víc vláken a procházení stromu čte složky souběžně (`storage_api.CRAWL_WORKERS`)."""

    def __init__(self, host, port, share, username, password, timeout):
        self.host, self.port, self.share = host, int(port or 445), share
        self.username, self.password = username or "", password or ""
        self.timeout = timeout
        self._idle = []
        self._lock = threading.Lock()

    def _connect(self):
        _vendor_path()
        try:
            from smb.SMBConnection import SMBConnection
        except ImportError as e:
            raise SmbUnavailable("SMB tu není k dispozici (chybí knihovna pysmb)") from e
        user, domain = _split_user(self.username)
        conn = SMBConnection(user or "guest", self.password, "nokturno", self.host, domain=domain,
                             use_ntlm_v2=True, is_direct_tcp=True)
        if not conn.connect(self.host, self.port, timeout=self.timeout):
            raise SmbAuthError("špatné jméno nebo heslo")
        return conn

    def _call(self, fn):
        """`fn(spojení)` nad spojením z fondu. Spojení, které mezitím server zavřel (NAS po
        nečinnosti), se zahodí a zkusí se jednou nové; chyba cesty spojení nekazí."""
        from_pool = True
        with self._lock:
            conn = self._idle.pop() if self._idle else None
        if conn is None:
            conn, from_pool = self._connect(), False
        while True:
            try:
                out = fn(conn)
            except Exception as e:  # noqa: BLE001 – pysmb má vlastní výjimky, rozliší se podle třídy
                if type(e).__name__ == "OperationFailure":
                    self._release(conn)   # cesta neexistuje / nejde otevřít – spojení je v pořádku
                    raise FileNotFoundError(str(e).split("\n")[0][:120]) from e
                _close(conn)
                if not from_pool:
                    raise
                conn, from_pool = self._connect(), False
                continue
            self._release(conn)
            return out

    def _release(self, conn):
        with self._lock:
            if len(self._idle) < 8:
                self._idle.append(conn)
                return
        _close(conn)

    def listdir(self, path):
        files = self._call(lambda c: c.listPath(self.share, "/" + path, timeout=self.timeout))
        return [(f.filename, bool(f.isDirectory), int(f.file_size or 0))
                for f in files if f.filename not in (".", "..")]

    def read(self, path, offset, length):
        buf = io.BytesIO()
        self._call(lambda c: c.retrieveFileFromOffset(self.share, "/" + path, buf, offset, length,
                                                      timeout=self.timeout))
        return buf.getvalue()

    def size(self, path):
        return int(self._call(lambda c: c.getAttributes(self.share, "/" + path, timeout=self.timeout)).file_size)


def _close(conn):
    try:
        conn.close()
    except Exception:  # noqa: BLE001 – zavírá se rozbité spojení
        pass


class SmbFile:
    """Soubor ze SMB pro proxy (Stremio, HA mimo Kodi) – přehrávač dostane HTTP s `Range`."""

    def __init__(self, backend, path, name=""):
        self.backend, self.path = backend, path
        self.name = name or path.rsplit("/", 1)[-1]

    def size(self):
        return self.backend.size(self.path)

    def chunks(self, start, end):
        """Bajty `start`–`end` (včetně) po `CHUNK`."""
        pos = start
        while pos <= end:
            data = self.backend.read(self.path, pos, min(CHUNK, end - pos + 1))
            if not data:
                return
            yield data
            pos += len(data)


def parse_range(header, size):
    """Hlavička `Range: bytes=a-b` → (a, b) včetně, `None` bez hlavičky.
    Vyhodí `ValueError`, když rozsah se souborem nesedí (proxy pak vrátí 416)."""
    if not header:
        return None
    m = re.fullmatch(r"\s*bytes=(\d*)-(\d*)\s*", str(header))
    if not m or not (m.group(1) or m.group(2)):
        raise ValueError("neplatný Range")
    if m.group(1):
        start = int(m.group(1))
        end = min(int(m.group(2)), size - 1) if m.group(2) else size - 1
    else:   # posledních N bajtů
        start, end = max(0, size - int(m.group(2))), size - 1
    if start > end or start >= size:
        raise ValueError("Range mimo soubor")
    return start, end
