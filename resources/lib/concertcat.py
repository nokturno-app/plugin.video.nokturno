"""Vlastní katalog koncertů podle hudebního žánru: seznam interpretů (pool) z Last.fm, hledání koncertů ve
zdrojích zapnutých u uživatele a seskupení nálezů do koncertů.

Všechno běží na zařízení uživatele s jeho vlastním klíčem Last.fm; server se nepoužívá a nálezy se
nesynchronizují (synchronizuje se jen definice katalogu, viz `mycat.py`). Sdílí Kodi, Home Assistant
(jen definici) a Stremio. Do jádra patří jen obecný filtr (`concertfilter.py`), žádné seznamy souborů.
"""
import json
import logging
import urllib.error
import urllib.parse
import urllib.request

# `from .x import y`, ne `from . import x` — plochá kopie v Kodi umí jen tenhle tvar
from abort import Aborted
from concertfilter import _key, concert_key, concert_title, is_concert
from fastshare_api import make_ref as fastshare_ref
from hellspy_api import HellspyRateLimited

_LOGGER = logging.getLogger(__name__)

LASTFM_URL = "https://ws.audioscrobbler.com/2.0/"
TAGS = ("czech", "slovak", "czech rock", "classic rock", "hard rock", "metal", "rock", "pop", "punk",
        "hip-hop", "jazz", "electronic", "folk", "classical", "reggae", "world")
PER_TAG = 100          # jmen z jednoho štítku
POOL_MAX = 200         # interpretů v katalogu
POOL_EVERY = 7 * 86400  # obnova poolu z Last.fm
MAX_FILES = 30         # souborů na interpreta (nejlepší podle velikosti)
SHOWS = ("pool", "found", "name")
WS_LIMIT, HS_LIMIT, HS_PAGES, FS_LIMIT = 100, 40, 2, 100
KEY_REJECTED = (4, 10, 26)   # chybové kódy Last.fm: špatný, neplatný nebo zablokovaný klíč


class ConcertError(Exception):
    def __init__(self, message, code=0):
        super().__init__(message)
        self.code = code


# --- Last.fm ---------------------------------------------------------------------------

def lastfm_top(key, tag, limit=PER_TAG, opener=urllib.request.urlopen):
    """Jména interpretů štítku (`tag.gettopartists`). Klíč se nikdy nedostane do výjimky ani do logu."""
    query = urllib.parse.urlencode({"method": "tag.gettopartists", "tag": tag, "limit": limit,
                                    "api_key": key, "format": "json"})
    req = urllib.request.Request(f"{LASTFM_URL}?{query}", headers={"User-Agent": "Nokturno"})
    try:
        with opener(req, timeout=20) as resp:
            data = json.load(resp)
    except urllib.error.HTTPError as err:
        try:   # Last.fm vrací chybu i s tělem JSON (např. 403 s kódem 10)
            data = json.load(err)
        except Exception:  # noqa: BLE001 – rozsypané tělo
            raise ConcertError(f"Last.fm: HTTP {err.code}") from None
    except Exception as err:  # noqa: BLE001 – síť, DNS, rozsypaný JSON
        raise ConcertError(f"Last.fm: {type(err).__name__}") from None
    if not isinstance(data, dict):
        raise ConcertError("Last.fm: neočekávaná odpověď")
    if data.get("error"):
        raise ConcertError(str(data.get("message") or "Last.fm: chyba")[:120], int(data["error"]))
    artists = (data.get("topartists") or {}).get("artist") or []
    return [str(a["name"]) for a in artists if isinstance(a, dict) and a.get("name")]


def check_key(key, opener=urllib.request.urlopen):
    """True = klíč funguje, False = Last.fm ho odmítlo. Výpadek sítě a jiné chyby vyhodí `ConcertError`."""
    try:
        lastfm_top(key, "rock", limit=1, opener=opener)
    except ConcertError as err:
        if err.code in KEY_REJECTED:
            return False
        raise
    return True


def pool(key, tags, opener=urllib.request.urlopen):
    """Interpreti ze štítků, prokládaně (1. z každého štítku, pak 2. …), `[{"id": "a:<klíč>", "name"}]`, nejvýš
    `POOL_MAX`. Vyřadí jména bez latinky, čistě číselná a kratší než 3 znaky – takové fulltext zdrojů nenajde nebo
    vrátí samé smetí. Selhaly-li všechny štítky, `ConcertError`; některé stačí."""
    lists, error = [], None
    for tag in [t for t in TAGS if t in set(tags or ())]:   # neznámé štítky se ignorují
        try:
            lists.append(lastfm_top(key, tag, opener=opener))
        except ConcertError as err:
            error = err
            _LOGGER.debug("Last.fm „%s“: %s", tag, err)
    if error is not None and not lists:
        raise error
    out, seen = [], set()
    for i in range(max((len(x) for x in lists), default=0)):
        for names in lists:
            if i >= len(names):
                continue
            k = _key(names[i])
            if len(k) < 3 or k.isdigit() or not any("a" <= c <= "z" for c in k) or k in seen:
                continue
            seen.add(k)
            out.append({"id": "a:" + k, "name": names[i]})
            if len(out) >= POOL_MAX:
                return out
    return out


# --- hledání ---------------------------------------------------------------------------

def _ws(api, artist):
    files, _total = api.search(artist, limit=WS_LIMIT, offset=0)
    return [{"ref": "ws:" + f["ident"], "name": f.get("name") or "", "size": int(f.get("size") or 0),
             "duration": 0, "source": "ws"} for f in files if f.get("ident")]


def _hs(api, artist):
    out, offset = [], 0
    for _ in range(HS_PAGES):
        files, nxt = api.search(artist, limit=HS_LIMIT, offset=offset)
        out += [{"ref": f"hs:{f['id']}:{f['hash']}", "name": f.get("name") or "", "size": int(f.get("size") or 0),
                 "duration": int(f.get("duration") or 0), "source": "hs"} for f in files]
        if not nxt or nxt <= offset:
            break
        offset = nxt
    return out


def _fs(api, artist):
    files, _total = api.search(artist, limit=FS_LIMIT)
    return [{"ref": fastshare_ref(f), "name": f.get("name") or "", "size": int(f.get("size") or 0),
             "duration": int(f.get("duration") or 0), "source": "fs"} for f in files]


SOURCES = (("webshare", "ws", _ws), ("hellspy", "hs", _hs), ("fastshare", "fs", _fs))


def search(engine, artist, rivals=(), should_stop=None):
    """Koncerty `artist` ve zdrojích, které má `engine` zapnuté (WebShare, HellSpy, FastShare): soubory
    `{"ref", "name", "size", "duration", "source"}`, nejvýš `MAX_FILES`, největší první. Chyba jednoho zdroje
    ho jen vynechá; selhaly-li všechny zapnuté (nebo žádný není zapnutý), vrací `None` = zkusit později."""
    on = engine.sources()
    tried = failed = 0
    found = []
    for source, attr, fn in SOURCES:
        if not on.get(source):
            continue
        if should_stop and should_stop():
            return None
        tried += 1
        api = getattr(engine, attr, None)
        if api is None:   # např. WebShare, kterému nešel login
            failed += 1
            continue
        try:
            found += fn(api, artist)
        except Aborted:
            raise
        except HellspyRateLimited:
            failed += 1
        except Exception as err:  # noqa: BLE001 – výpadek zdroje vynechá jen ten zdroj
            failed += 1
            _LOGGER.debug("koncerty, %s: %s", source, err)
    if not tried or failed == tried:
        return None
    out, seen = [], set()
    for f in sorted(found, key=lambda f: -f["size"]):
        if f["ref"] in seen or not is_concert(f["name"], artist, f["size"], f["duration"], rivals):
            continue
        seen.add(f["ref"])
        out.append(f)
    return out[:MAX_FILES]


def group(files, artist):
    """Soubory → koncerty `[{"title", "year", "files"}]`. Klíč = název bez kvality a tagů + rok; soubor bez roku
    se přiřadí ke stejnojmennému koncertu s rokem, je-li právě jeden. Rok sestupně (bez roku nakonec), pak název;
    uvnitř koncertu největší soubor první."""
    groups = {}
    for f in sorted(files, key=lambda f: -int(f.get("size") or 0)):
        title, year = concert_title(f.get("name") or "", artist)
        base = concert_key(title)
        groups.setdefault((base, year), {"title": title, "year": year, "files": []})["files"].append(f)
    for (base, year), g in list(groups.items()):
        if year is None:
            same = [k for k in groups if k[0] == base and k[1] is not None]
            if len(same) == 1:
                groups[same[0]]["files"] += g["files"]
                del groups[(base, year)]
    out = list(groups.values())
    for g in out:
        g["files"].sort(key=lambda f: -int(f.get("size") or 0))
    out.sort(key=lambda g: (g["year"] is None, -(g["year"] or 0), g["title"].casefold()))
    return out


def visible(index, sort="pool"):
    """Interpreti s nálezem (`ok` a soubory) jako `{"id", "name", "files"}`; `sort`: „pool“ = pořadí poolu,
    „found“ = nově nalezení první, „name“ = podle abecedy."""
    rows = []
    for mid, e in (index.get("items") or {}).items():
        if e.get("ok") is True and e.get("files") and (e.get("meta") or {}).get("name"):
            rows.append((e, {"id": mid, "name": e["meta"]["name"], "files": e["files"]}))
    if sort == "name":
        rows.sort(key=lambda r: r[1]["name"].casefold())
    elif sort == "found":
        rows.sort(key=lambda r: (-(r[0].get("found") or 0), r[0].get("rank", 0)))
    else:
        rows.sort(key=lambda r: r[0].get("rank", 0))
    return [r[1] for r in rows]
