"""Modul Koncerty: seznam interpretů (pool) z Last.fm podle vybraných hudebních žánrů, hledání koncertů ve
zdrojích zapnutých u uživatele a seskupení nálezů do koncertů.

Koncerty nejsou druh vlastního katalogu, ale jedna pevná konfigurace (`CONFIG`, jen vybrané žánry)
a jeden index (`INDEX`, struktura `catindex`). Všechno běží na zařízení uživatele s jeho vlastním klíčem
Last.fm; server se nepoužívá a nálezy se nesynchronizují (synchronizuje se jen konfigurace, okruh
`catalogs`, záznam `c:concerts`). Sdílí Kodi, Home Assistant (jen konfiguraci) a Stremio. Do jádra patří
jen obecný filtr (`concertfilter.py`), žádné seznamy souborů.
"""
import time
import json
import logging
import urllib.error
import urllib.parse
import urllib.request

# `from .x import y`, ne `from . import x` — plochá kopie v Kodi umí jen tenhle tvar
from abort import Aborted
from catindex import next_batch, record
from concertfilter import _key, concert_key, concert_title, is_concert, normalize_title, rivals
from fastshare_api import make_ref as fastshare_ref
from hellspy_api import HellspyError, HellspyRateLimited
from webshare_api import WebshareApiError

_LOGGER = logging.getLogger(__name__)

CONFIG = "concerts"            # {"tags": [...], "ts": int} – vybrané žánry (synchronizuje se)
INDEX = "concerts_index"       # struktura `catindex`; u položky `files` = nálezy, u souboru `t` = první nález
RETRY = 1800                   # po selhání načtení poolu z Last.fm další pokus nejdřív za 30 min
LASTFM_URL = "https://ws.audioscrobbler.com/2.0/"
TAGS = ("czech", "slovak", "czech rock", "classic rock", "hard rock", "metal", "rock", "pop", "punk",
        "hip-hop", "jazz", "electronic", "folk", "classical", "reggae", "world")
PER_TAG = 100          # jmen z jednoho štítku na stránku Last.fm
POOL_EVERY = 7 * 86400  # obnova první stránky z Last.fm (nová jména v žebříčku)
GROW_EVERY = 3600       # nejčastěji tak často přibude další stránka z Last.fm
GROW_LEFT = 20          # … a to, když zbývá míň neprověřených interpretů (fronta tak nikdy nevyschne)
ARTISTS_MAX = 3000      # ponytail: strop velikosti indexu (JSON se přepisuje po každé dávce); víc = SQLite
STALE_DAYS = 45         # soubor, který se tak dlouho neukázal v hledání, se zahodí (jako na dashboardu)
LINK_CHECKS = 10        # neviděných souborů na interpreta a kontrolu, které se ověří odkazem
MAX_FILES = 30         # souborů na interpreta (nejlepší podle velikosti)
SHOWS = ("pool", "found", "name")
WS_LIMIT, HS_LIMIT, HS_PAGES, FS_LIMIT = 100, 40, 2, 100
KEY_REJECTED = (4, 10, 26)   # chybové kódy Last.fm: špatný, neplatný nebo zablokovaný klíč


class ConcertError(Exception):
    def __init__(self, message, code=0):
        super().__init__(message)
        self.code = code


# --- Last.fm ---------------------------------------------------------------------------

def lastfm_top(key, tag, limit=PER_TAG, opener=urllib.request.urlopen, page=1):
    """Jména interpretů štítku (`tag.gettopartists`). Klíč se nikdy nedostane do výjimky ani do logu."""
    query = urllib.parse.urlencode({"method": "tag.gettopartists", "tag": tag, "limit": limit,
                                    "page": page, "api_key": key, "format": "json"})
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


def pool(key, tags, opener=urllib.request.urlopen, page=1):
    """Interpreti ze štítků (stránka `page` žebříčku Last.fm), prokládaně (1. z každého štítku, pak 2. …),
    `[{"id": "a:<klíč>", "name"}]`. Vyřadí jména bez latinky, čistě číselná a kratší než 3 znaky – takové fulltext zdrojů nenajde nebo
    vrátí samé smetí. Selhaly-li všechny štítky, `ConcertError`; některé stačí."""
    lists, error = [], None
    for tag in [t for t in TAGS if t in set(tags or ())]:   # neznámé štítky se ignorují
        try:
            lists.append((tag, lastfm_top(key, tag, opener=opener, page=page)))
        except ConcertError as err:
            error = err
            _LOGGER.debug("Last.fm „%s“: %s", tag, err)
    if error is not None and not lists:
        raise error
    out, seen = [], {}
    for i in range(max((len(x) for _t, x in lists), default=0)):
        for tag, names in lists:
            if i >= len(names):
                continue
            k = _key(names[i])
            if len(k) < 3 or k.isdigit() or not any("a" <= c <= "z" for c in k):
                continue
            if k in seen:   # týž interpret z víc štítků: štítky se sčítají
                if tag not in seen[k]["tags"]:
                    seen[k]["tags"].append(tag)
                continue
            seen[k] = {"id": "a:" + k, "name": names[i], "tags": [tag]}
            out.append(seen[k])
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
    on = dict(engine.sources())
    # FastShare bez kreditu a zdroje v pauze se na pozadí nevolají (viz `Engine.verify_sources`)
    for name in getattr(engine, "verify_sources", lambda: ())():
        on[name] = False
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


# --- konfigurace, index a pohledy --------------------------------------------------------

def _now():
    return int(time.time())


def config(store):
    """Uložená konfigurace `{"tags": [...], "ts": int}` – jen platné štítky; prázdné `tags` = nenastaveno."""
    cfg = store.load(CONFIG, {})
    cfg = cfg if isinstance(cfg, dict) else {}
    return {"tags": [t for t in cfg.get("tags") or [] if t in TAGS], "ts": int(cfg.get("ts") or 0)}


def configured(store):
    return bool(config(store)["tags"])


def configure(store, tags):
    """Uloží vybrané žánry (jen známé štítky, v pořadí `TAGS`) a zapíše čas změny pro synchronizaci."""
    chosen = [t for t in TAGS if t in set(tags or ())]
    store.save(CONFIG, {"tags": chosen, "ts": _now()})
    return chosen


def sig(tags):
    return ",".join(sorted(tags))


def load_index(store):
    """Index odpovídající dnešním žánrům, `{}` při jiných (neplatný)."""
    index = store.load(INDEX, {})
    return index if isinstance(index, dict) and index.get("sig") == sig(config(store)["tags"]) else {}


def collect_config(store):
    """Záznam pro synchronizaci (`c:concerts`) nebo None, dokud není nic nastaveno."""
    cfg = config(store)
    if not cfg["tags"]:
        return None
    rec = {"on": True, "ts": cfg["ts"], "cfg": {"tags": cfg["tags"]}}
    raw = store.load(CONFIG, {})
    if isinstance(raw, dict) and raw.get("rts"):
        rec["rts"] = int(raw["rts"])
    return rec


def apply_config(store, rec, stamp=0):
    """Přijme cizí konfiguraci, je-li novější; vrací 1 při přijetí. `stamp` = čas příjmu (jen střed, HA)."""
    cfg = rec.get("cfg") if isinstance(rec, dict) else None
    tags = [t for t in TAGS if t in set((cfg or {}).get("tags") or ())]
    try:
        ts = int(rec.get("ts") or 0)
    except (TypeError, ValueError, AttributeError):
        return 0
    if not tags or ts <= config(store)["ts"]:
        return 0
    data = {"tags": tags, "ts": ts}
    if stamp:
        data["rts"] = stamp
    store.save(CONFIG, data)
    return 1


def migrate_catalogs(store):
    """Beta 1 měla koncerty jako druh vlastního katalogu: jejich žánry se sjednotí do `CONFIG` (jen když ještě
    není) a katalogy se smažou (`mycat.delete` přes deník, ať se smazání synchronizuje). Vrací počet smazaných."""
    from mycat import DEFS, delete   # líný import: mycat importuje tenhle modul

    items = [c for c in store.load(DEFS, []) if isinstance(c, dict) and c.get("kind") == "concert"]
    if not items:
        return 0
    if not configured(store):
        tags = set()
        for c in items:
            tags |= set(c.get("tags") or ())
        if tags & set(TAGS):
            configure(store, tags)
    for c in items:
        delete(store, c.get("id"))
    return len(items)


def _add(index, metas):
    """Přidá nové interprety na konec poolu (`ok=None`, prověří se přednostně); u známých jen doplní štítky.
    Nikoho neodebírá – interpret, který vypadl z žebříčku Last.fm, má koncerty dál. Vrací počet nových."""
    items = index.setdefault("items", {})
    rank = max((int(e.get("rank") or 0) for e in items.values()), default=-1) + 1
    added = 0
    for meta in metas:
        mid = meta.get("id")
        if not mid:
            continue
        entry = items.get(mid)
        if entry is not None:
            old = entry.setdefault("meta", dict(meta))
            old["tags"] = list(old.get("tags") or []) + [t for t in meta.get("tags") or [] if t not in (old.get("tags") or [])]
            entry["genres"] = list(old["tags"])
            continue
        if len(items) >= ARTISTS_MAX:
            break
        items[mid] = {"ok": None, "ts": 0, "rank": rank, "genres": list(meta.get("tags") or []), "meta": meta}
        rank += 1
        added += 1
    return added


def link_ok(engine, ref):
    """Žije soubor? Zkusí vydat odkaz (jako `verify_missed` na dashboardu). True/False = ověřeno, None = nejde
    to poznat (FastShare vydá odkaz jen za kredit, zdroj vypnutý, výpadek sítě) – soubor pak zůstává."""
    try:
        if ref.startswith("ws:"):
            api = engine.ws
            return None if api is None else bool(api.file_link(ref[3:]))
        if ref.startswith("hs:"):
            api = engine.hs
            _hs, file_id, file_hash = ref.split(":", 2)
            return None if api is None else bool(api.file_link(file_id, file_hash))
    except HellspyRateLimited:
        return None
    except WebshareApiError:   # server soubor odmítl (FILE_NOT_FOUND); síťová chyba níž = neví se
        return False
    except HellspyError as err:
        return False if "HTTP 404" in str(err) or "HTTP 410" in str(err) else None
    except Exception:  # noqa: BLE001 – cokoli jiného = neví se
        return None
    return None


def _merge_files(engine, old, found, now, should_stop=None):
    """Nové hledání + dřívější soubory, které v něm chybí: ty se ověří odkazem (nejvýš `LINK_CHECKS`); mrtvý
    zmizí, neověřitelný zůstane, dokud se neukáže `STALE_DAYS`. `t` = první nález, `s` = naposledy viděn."""
    first = {f.get("ref"): f for f in old}
    out = []
    for f in found:
        f["t"] = int((first.get(f["ref"]) or {}).get("t") or now)
        f["s"] = now
        out.append(f)
    seen = {f["ref"] for f in out}
    checks = 0
    for f in sorted((f for f in old if f.get("ref") not in seen), key=lambda f: int(f.get("s") or f.get("t") or 0)):
        last = int(f.get("s") or f.get("t") or now)
        if now - last > STALE_DAYS * 86400:
            continue
        alive = None
        if checks < LINK_CHECKS and not (should_stop and should_stop()):
            checks += 1
            alive = link_ok(engine, f.get("ref") or "")
        if alive is False:
            continue
        if alive:
            f["s"] = now
        out.append(f)
    out.sort(key=lambda f: -int(f.get("size") or 0))
    return out[:MAX_FILES]


def refresh(engine, store, size=1, should_stop=None):
    """Jedna dávka: doplní pool z Last.fm a prohledá až `size` interpretů ve zdrojích zařízení. Pool roste:
    první stránka žebříčku se obnoví po `POOL_EVERY`, další stránka přibude po `GROW_EVERY`, jakmile zbývá
    méně než `GROW_LEFT` neprověřených. Vrací počet prohledaných. Bez konfigurace nebo klíče 0."""
    key = str(engine._opt("lastfm_key") or "").strip()
    tags = config(store)["tags"]
    if not key or not tags:
        return 0
    csig, now = sig(tags), _now()
    index = store.reload(INDEX, {})
    if index.get("sig") != csig:
        index = {}
    retry_ok = now - int(index.get("pool_try") or 0) >= RETRY
    page = None
    if now - int(index.get("pool_ts") or 0) >= POOL_EVERY:
        page = 1
    elif not index.get("pool_end") and now - int(index.get("grow_ts") or 0) >= GROW_EVERY \
            and sum(e.get("ok") is None for e in (index.get("items") or {}).values()) < GROW_LEFT \
            and len(index.get("items") or {}) < ARTISTS_MAX:
        page = int(index.get("page") or 1) + 1
    fresh = None
    if page and retry_ok:   # síť mimo zámek
        try:
            fresh = pool(key, tags, page=page)
        except ConcertError:
            fresh = None
    with store.updating(INDEX, {}) as index:
        if index.get("sig") != csig:
            index.clear()
            index["sig"] = csig
        if page and retry_ok:
            index["pool_try"] = now
        if fresh is not None:
            _add(index, fresh)
            if page == 1:
                index["pool_ts"] = now
                index.setdefault("page", 1)
            else:
                index["page"], index["grow_ts"] = page, now
                if not fresh:
                    index["pool_end"] = True   # žebříček došel
        batch = [(m, index["items"][m]["meta"].get("name") or "", list(index["items"][m].get("files") or []))
                 for m in next_batch(index, now, size)]
        names = [(e.get("meta") or {}).get("name") or "" for e in (index.get("items") or {}).values()]
    done = 0
    for mid, artist, old in batch:
        if should_stop and should_stop():
            break
        files = None
        if artist:
            try:
                with engine.background():
                    files = search(engine, artist, rivals(artist, names), should_stop)
                    if files is not None:
                        files = _merge_files(engine, old, files, _now(), should_stop)
            except Aborted:
                raise
            except Exception:  # noqa: BLE001 – výpadek zdroje = zkusit později
                files = None
        with store.updating(INDEX, {}) as index:
            entry = (index.get("items") or {}).get(mid)
            if index.get("sig") == csig and entry is not None:
                record(index, mid, None if files is None else bool(files), _now())
                if files is not None:
                    entry["misses"] = 0 if files else int(entry.get("misses") or 0) + 1
                    if files:
                        entry["files"] = files
                    else:
                        entry.pop("files", None)
        done += 1
    return done


def _artists(index):
    """Interpreti s nálezem: `[(entry, {"id", "name", "files"})]`."""
    rows = []
    for mid, e in (index.get("items") or {}).items():
        if e.get("ok") is True and e.get("files") and (e.get("meta") or {}).get("name"):
            rows.append((e, {"id": mid, "name": e["meta"]["name"], "files": e["files"]}))
    return rows


def recent(index, limit=50):
    """Nově přidané koncerty napříč interprety, nejnovější první (podle času prvního nálezu souboru):
    `[{"artist_id", "artist", "title", "year", "files", "t"}]`."""
    out = []
    for _e, a in _artists(index):
        for g in group(a["files"], a["name"]):
            out.append(dict(g, artist_id=a["id"], artist=a["name"], t=max(int(f.get("t") or 0) for f in g["files"])))
    out.sort(key=lambda c: (-c["t"], c["artist"].casefold(), c["title"].casefold()))
    return out[:limit]


def by_tag(index, tag):
    """Interpreti s daným štítkem (abecedně)."""
    rows = [a for e, a in _artists(index) if tag in ((e.get("meta") or {}).get("tags") or [])]
    return sorted(rows, key=lambda a: a["name"].casefold())


def tags_available(index):
    """Štítky, pod kterými je aspoň jeden interpret s nálezem, v pořadí `TAGS`."""
    have = {t for e, _a in _artists(index) for t in (e.get("meta") or {}).get("tags") or []}
    return [t for t in TAGS if t in have]


def letter_of(name):
    first = normalize_title(name or "")[:1]
    return first.upper() if first.isalpha() else "#"


def letters(index):
    """`{písmeno: počet interpretů}` (číslice a ostatní = `#`)."""
    out = {}
    for _e, a in _artists(index):
        out[letter_of(a["name"])] = out.get(letter_of(a["name"]), 0) + 1
    return dict(sorted(out.items(), key=lambda kv: (kv[0] == "#", kv[0])))


def by_letter(index, letter):
    rows = [a for _e, a in _artists(index) if letter_of(a["name"]) == letter]
    return sorted(rows, key=lambda a: a["name"].casefold())


def artist(index, mid):
    """Koncerty interpreta `[{"title", "year", "files"}]` (prázdné, není-li nalezen)."""
    for _e, a in _artists(index):
        if a["id"] == mid:
            return group(a["files"], a["name"])
    return []
