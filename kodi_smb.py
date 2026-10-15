"""Vlastní úložiště přes SMB v Kodi (P23).

Jádro (`storage_api.SmbStorage`) dostane backend nad `xbmcvfs`: Kodi umí SMB samo (i SMB3
a podepisování, které knihovna pysmb v HA a Stremiu nemá), přehrává `smb://` přímo
a doplněk nic navíc nepotřebuje. Rozhraní backendu popisuje `nokturno_core/lib/smbfs.py`.
"""
import urllib.parse

import xbmcvfs

from storage_api import VIDEO_EXT  # noqa: E402 – kopie jádra v resources/lib


def _q(text):
    return urllib.parse.quote(str(text or ""), safe="")


class KodiSmbBackend:
    def __init__(self, host, port, share, username, password, timeout=None):
        domena, _sep, jmeno = str(username or "").rpartition("\\")   # Kodi chce „DOMÉNA;jméno“
        cred = ""
        if jmeno:
            cred = (_q(domena) + ";" if domena else "") + _q(jmeno) + ":" + _q(password) + "@"
        netloc = host + (f":{port}" if port and int(port) != 445 else "")
        self.base = f"smb://{cred}{netloc}/{_q(share)}/"

    def url(self, path, folder=False):
        if not path:
            return self.base
        return self.base + urllib.parse.quote(path) + ("/" if folder else "")

    def listdir(self, path):
        url = self.url(path, folder=True)
        dirs, files = xbmcvfs.listdir(url)
        if not dirs and not files and not xbmcvfs.exists(url):
            # xbmcvfs chybu nehlásí, nepřístupná složka vypadá jako prázdná
            if not path:
                raise OSError("složka nejde otevřít – zkontroluj adresu, jméno a heslo")
            raise FileNotFoundError(path)
        out = [(d, True, 0) for d in dirs]
        for f in files:
            # velikost jen u videí: Stat je dotaz na server a ostatní soubory se do seznamu nedostanou
            size = xbmcvfs.Stat(self.url(f"{path}/{f}" if path else f)).st_size() if f.lower().endswith(VIDEO_EXT) else 0
            out.append((f, False, size))
        return out

    def read(self, path, offset, length):
        f = xbmcvfs.File(self.url(path))
        try:
            if offset:
                f.seek(offset, 0)
            return bytes(f.readBytes(length))
        finally:
            f.close()

    def size(self, path):
        return xbmcvfs.Stat(self.url(path)).st_size()


class _Odpoved:
    """Soubor ze SMB jako odpověď `urlopen` pro stahovač (`service.Downloader`)."""

    def __init__(self, url, offset):
        self.f = xbmcvfs.File(url)
        self.offset = offset
        size = self.f.size()
        if offset:
            self.f.seek(offset, 0)
        self.headers = {"Content-Length": str(max(0, size - offset))}

    def getcode(self):
        return 206 if self.offset else 200

    def read(self, n):
        return bytes(self.f.readBytes(n))

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.f.close()
        return False


def open_range(url, offset=0):
    return _Odpoved(url, offset)
