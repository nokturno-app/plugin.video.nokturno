"""Modul Koncerty: seznam interpretů (pool) z Last.fm podle vybraných hudebních žánrů, hledání koncertů
v uživatelově vlastním úložišti (WebDAV) a volitelně v úložištích třetích stran, která si sám zapnul
a nastavil (WebShare, HellSpy, FastShare), a seskupení nálezů do koncertů.

Koncerty nejsou druh vlastního katalogu, ale jedna pevná konfigurace (`CONFIG`, jen vybrané žánry)
a jeden index (`INDEX`, struktura `catindex`). Všechno běží na zařízení uživatele s jeho vlastním klíčem
Last.fm; server se nepoužívá. Konfigurace jde okruhem `catalogs` (záznam `c:concerts`), nálezy okruhem
`concerts` (záznamy `a:<id>`, max `SYNC_FILES` souborů na interpreta); každé zařízení ukáže jen soubory
ze zdrojů, které má samo zapnuté (`index["src"]`). Soubory z vlastního úložiště (`dav:`) se nesynchronizují –
odkaz platí jen pro úložiště toho zařízení. Interpreta jde přidat i ručně (`add_artist`). Sdílí Kodi,
Home Assistant a Stremio. Do jádra patří jen obecný filtr (`concertfilter.py`), žádné seznamy souborů.
"""
import copy
import re
import threading
import time
import json
import logging
import urllib.error
import urllib.parse
import urllib.request

# `from .x import y`, ne `from . import x` — plochá kopie v Kodi umí jen tenhle tvar
from abort import Aborted
from catindex import NO_RANK, next_batch, record
from concertfilter import MIN_SIZE, _key, concert_key, concert_title, is_concert, normalize_title, rivals
from fastshare_api import make_ref as fastshare_ref
from hellspy_api import HellspyError, HellspyRateLimited
from storage_api import StorageError  # noqa: F401 – i pro testy
from webshare_api import WebshareApiError

_LOGGER = logging.getLogger(__name__)

CONFIG = "concerts"            # {"tags": [...], "ts": int} – vybrané žánry (synchronizuje se)
SECTION = "concerts"           # sekce synchronizace (okruh `concerts`): nálezy `a:<id>`
SYNC_FILES = 10                # souborů na interpreta, které jdou do synchronizace (největší první)
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
COLLAB_RE = re.compile(r"\s[&x+]\s|/|,\s|\s(feat|ft)\.?\s", re.I)   # spolupráce ve výsledcích hledání
RENAME_BATCH = 5        # ručně přidaných interpretů, kterým se v jedné dávce dohledá jméno z Last.fm
KEY_REJECTED = (4, 10, 26)   # chybové kódy Last.fm: špatný, neplatný nebo zablokovaný klíč


class ConcertError(Exception):
    def __init__(self, message, code=0):
        super().__init__(message)
        self.code = code


# --- Last.fm ---------------------------------------------------------------------------

def _lastfm(key, params, opener=urllib.request.urlopen):
    """Dotaz na Last.fm (JSON). Klíč se nikdy nedostane do výjimky ani do logu."""
    query = urllib.parse.urlencode(dict(params, api_key=key, format="json"))
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
    return data


def lastfm_top(key, tag, limit=PER_TAG, opener=urllib.request.urlopen, page=1):
    """Jména interpretů štítku (`tag.gettopartists`)."""
    data = _lastfm(key, {"method": "tag.gettopartists", "tag": tag, "limit": limit, "page": page}, opener)
    artists = (data.get("topartists") or {}).get("artist") or []
    return [str(a["name"]) for a in artists if isinstance(a, dict) and a.get("name")]


def lastfm_name(key, name, opener=urllib.request.urlopen):
    """Jméno interpreta, jak ho vede Last.fm (`artist.getInfo` s opravou překlepů: „arakain“ → „Arakain“).
    None bez klíče, při neznámém interpretovi a výpadku – ruční přidání pak jede s tím, co uživatel napsal."""
    if not key:
        return None
    try:
        data = _lastfm(key, {"method": "artist.getinfo", "artist": name, "autocorrect": 1}, opener)
    except ConcertError as err:
        _LOGGER.debug("Last.fm jméno „%s“: %s", name, err)
        return None
    found = str((data.get("artist") or {}).get("name") or "").strip()
    return found or None


def lastfm_search(key, text, limit=100, opener=urllib.request.urlopen):
    """Kandidáti z Last.fm pro ruční hledání (`artist.search`): `[{"name", "listeners"}]` v pořadí Last.fm,
    bez duplicit podle `_key`. Last.fm vrací i podobná jména („lucie“ → Luci4, Lucid, St. Lucia) a Lucii Bílou
    až na 14. místě, proto se bere 100 výsledků a nechají se jména s hledaným slovem; nezbude-li nic (překlep),
    prvních 10 bez filtru. Bez klíče a při výpadku prázdné – hledání pak jede s tím, co uživatel napsal."""
    if not key or len(_key(text or "")) < 2:
        return []
    fix = {}   # oprava překlepu souběžně s hledáním – dva dotazy za sebou byly znát (~3 s)
    worker = threading.Thread(target=lambda: fix.setdefault("name", lastfm_name(key, text, opener)), daemon=True)
    worker.start()
    try:
        data = _lastfm(key, {"method": "artist.search", "artist": text, "limit": 100}, opener)
    except ConcertError as err:
        _LOGGER.debug("Last.fm hledání „%s“: %s", text, err)
        return []
    found = ((data.get("results") or {}).get("artistmatches") or {}).get("artist") or []
    out, seen = [], set()
    for a in found if isinstance(found, list) else []:
        name = str((a or {}).get("name") or "").strip() if isinstance(a, dict) else ""
        k = _key(name)
        if len(k) < 2 or k in seen:
            continue
        seen.add(k)
        try:
            listeners = int(a.get("listeners") or 0)
        except (TypeError, ValueError):
            listeners = 0
        out.append({"name": name, "listeners": listeners})
    # spolupráce („Arakain & Lucie Bílá“, „X/Y“) na konec – nevyhazovat, „Earth, Wind & Fire“ je kapela
    out.sort(key=lambda a: bool(COLLAB_RE.search(a["name"])))
    word = re.compile(r"(?<!\w)%s(?!\w)" % re.escape(normalize_title(text)))
    hits = [a for a in out if word.search(normalize_title(a["name"]))] or out[:10]
    worker.join(25)
    fixed = fix.get("name")   # překlep: „arakian“ → Arakain (Last.fm má i interpreta Arakian)
    if fixed and _key(fixed) not in {_key(a["name"]) for a in hits}:
        hits.insert(0, {"name": fixed, "listeners": 0})
    return hits[:limit]


def _nice(name):
    """Jméno psané celé malými (bez Last.fm) aspoň s velkým písmenem na začátku slov."""
    if name != name.lower():
        return name
    return " ".join(w[:1].upper() + w[1:] for w in name.split(" "))


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

def _dav(storages, artist):
    """Vlastní úložiště: soubory, v jejichž cestě je jméno interpreta. Filtr koncertu (`match`) dostane celou
    cestu, takže stačí složka „Koncerty/Kabát/…“; zobrazuje se název souboru, bez jména interpreta
    i se složkou nad ním. Vadné úložiště se vynechá, selžou-li všechna, vyhodí chybu."""
    out, errors = [], []
    for api in storages:
        try:
            files, _total = api.search(artist, limit=10 ** 6)
        except StorageError as err:
            errors.append(err)
            continue
        aname = normalize_title(artist)
        for f in files:
            parts = f["path"].split("/")
            name = f["name"]
            if len(parts) > 1 and f" {aname} " not in f" {normalize_title(name)} ":
                name = parts[-2] + " - " + name
            out.append({"ref": f"dav:{api.slot}:{f['path']}", "name": name, "size": int(f.get("size") or 0),
                        "duration": 0, "source": "dav", "img": "", "match": " - ".join(parts)})
    if errors and len(errors) == len(storages):
        raise errors[0]
    return out


def _ws(api, artist):
    files, _total = api.search(artist, limit=WS_LIMIT, offset=0)
    return [{"ref": "ws:" + f["ident"], "name": f.get("name") or "", "size": int(f.get("size") or 0),
             "duration": 0, "source": "ws", "img": f.get("img") or ""} for f in files if f.get("ident")]


def _hs(api, artist):
    out, offset = [], 0
    for _ in range(HS_PAGES):
        files, nxt = api.search(artist, limit=HS_LIMIT, offset=offset)
        out += [{"ref": f"hs:{f['id']}:{f['hash']}", "name": f.get("name") or "", "size": int(f.get("size") or 0),
                 "duration": int(f.get("duration") or 0), "source": "hs", "img": f.get("thumb") or ""} for f in files]
        if not nxt or nxt <= offset:
            break
        offset = nxt
    return out


def _fs(api, artist):
    files, _total = api.search(artist, limit=FS_LIMIT)
    return [{"ref": fastshare_ref(f), "name": f.get("name") or "", "size": int(f.get("size") or 0),
             "duration": int(f.get("duration") or 0), "source": "fs", "img": f.get("thumb") or ""} for f in files]


# Vlastní úložiště první; úložiště třetích stran jen ta, která si uživatel zapnul a nastavil.
SOURCES = (("storage", "dav", _dav), ("webshare", "ws", _ws), ("hellspy", "hs", _hs), ("fastshare", "fs", _fs))


def _on(engine):
    """Zdroje zapnuté pro koncerty; FastShare bez kreditu a zdroje v pauze se na pozadí nevolají
    (viz `Engine.verify_sources`)."""
    on = dict(engine.sources())
    for name in getattr(engine, "verify_sources", lambda: ())():
        on[name] = False
    return on


def src_of(engine):
    """Zkratky zdrojů, které `engine` pro koncerty použije (seřazené, čárkou)."""
    on = _on(engine)
    return ",".join(sorted(attr for source, attr, _fn in SOURCES if on.get(source)))


def search(engine, artist, rivals=(), should_stop=None):
    """Koncerty `artist` ve vlastním úložišti a ve zdrojích, které má `engine` zapnuté (WebShare, HellSpy,
    FastShare): soubory
    `{"ref", "name", "size", "duration", "source"}`, nejvýš `MAX_FILES`, největší první. Chyba jednoho zdroje
    ho jen vynechá; selhaly-li všechny zapnuté (nebo žádný není zapnutý), vrací `None` = zkusit později."""
    on = _on(engine)
    tried = failed = 0
    found = []
    for source, attr, fn in SOURCES:
        if not on.get(source):
            continue
        if should_stop and should_stop():
            return None
        tried += 1
        api = (getattr(engine, "storages", None) or None) if attr == "dav" else getattr(engine, attr, None)
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
        match = f.pop("match", None) or f["name"]
        size = f["size"] or (MIN_SIZE if f["source"] == "dav" else 0)   # HTML výpis úložiště velikost nezná
        if f["ref"] in seen or not is_concert(match, artist, size, f["duration"], rivals):
            continue
        seen.add(f["ref"])
        out.append(f)
    return out[:MAX_FILES]


def image(files):
    """Náhled ze zdroje (WebShare `img`, HellSpy a FastShare `thumb`) – první soubor, který ho má, jinak ""."""
    return next((f["img"] for f in files or () if f.get("img")), "")


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
    rows = _artists(index)
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


def retag(index, tags):
    """Změna žánrů nemaže nalezené koncerty: zůstanou interpreti, kteří mají aspoň jeden z vybraných žánrů
    (nebo byli přidáni ručně),
    seznam z Last.fm se stáhne znovu od první stránky (nové žánry). Mění `index` na místě a vrací ho."""
    keep = set(tags)
    items = index.get("items") or {}
    for mid in [m for m, e in items.items()
                if not keep & set((e.get("meta") or {}).get("tags") or []) and not (e.get("meta") or {}).get("manual")]:
        del items[mid]
    for k in ("pool_ts", "pool_try", "page", "grow_ts", "pool_end"):
        index.pop(k, None)
    index["sig"] = sig(tags)
    return index


def load_index(store):
    """Index koncertů; po změně žánrů jen interpreti s některým z dnešních žánrů (`retag`)."""
    index = store.load(INDEX, {})
    if not isinstance(index, dict):
        return {}
    tags = config(store)["tags"]
    return index if index.get("sig") == sig(tags) else retag(copy.deepcopy(index), tags)


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
            if old.get("manual") and meta.get("name"):   # ručně zadané jméno nahradí to z Last.fm
                old["name"] = meta["name"]
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
        if ref.startswith("dav:"):
            api, path = engine.storage_for(ref)
            return any(f["path"] == path for f in api.files())
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
        index = retag(copy.deepcopy(index), tags)
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
    renames = {}   # ručně přidaní z doby před 10.3.1 („arakain“): jméno jednou z Last.fm
    for mid, e in (index.get("items") or {}).items():
        meta = e.get("meta") or {}
        if len(renames) >= RENAME_BATCH or not retry_ok:
            break
        if meta.get("manual") and not meta.get("lf") and meta.get("name"):
            renames[mid] = lastfm_name(key, meta["name"]) or _nice(meta["name"])
    with store.updating(INDEX, {}) as index:
        if index.get("sig") != csig:
            retag(index, tags)
        for mid, name in renames.items():
            meta = ((index.get("items") or {}).get(mid) or {}).get("meta")
            if meta is not None:
                meta["name"], meta["lf"] = name, 1
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
        index["src"] = src_of(engine)
        if not index.get("img"):   # index z doby před náhledy (10.0.0 beta): interpreti s koncerty jednou znovu
            for e in (index.get("items") or {}).values():
                if e.get("files"):
                    e["ts"] = 0
            index["img"] = 1
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
                    entry["st"] = _now()
                    entry["misses"] = 0 if files else int(entry.get("misses") or 0) + 1
                    if files:
                        entry["files"] = files
                    else:
                        entry.pop("files", None)
        done += 1
    return done


def _artists(index):
    """Interpreti s nálezem: `[(entry, {"id", "name", "files"})]`. Soubory jen ze zdrojů zařízení
    (`index["src"]`; neznámé = všechny), takže cizí nálezy z jiného zařízení ukáže jen to, co jde přehrát."""
    on = set(filter(None, str(index.get("src") or "").split(",")))
    rows = []
    for mid, e in (index.get("items") or {}).items():
        files = [f for f in e.get("files") or [] if not on or f.get("source") in on]
        if files and (e.get("meta") or {}).get("name"):
            rows.append((e, {"id": mid, "name": e["meta"]["name"], "files": files}))
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


def find(index, text):
    """Interpreti s nálezem, jejichž jméno obsahuje `text` (bez diakritiky a velikosti písmen), abecedně, max 50."""
    q = normalize_title(text or "")
    if not q:
        return []
    rows = [a for _e, a in _artists(index) if q in normalize_title(a["name"])]
    return sorted(rows, key=lambda a: a["name"].casefold())[:50]


def status(index, text):
    """Stav jmen pro výpis hledání: `{_key(jméno): {"id", "name", "count", "searched"}}` pro interprety v indexu
    (i bez nálezu). `count` = počet koncertů ze zdrojů zařízení, `searched` = čas posledního prohledání (0 = nikdy)."""
    q = normalize_title(text or "")
    on = set(filter(None, str(index.get("src") or "").split(",")))
    out = {}
    for mid, e in (index.get("items") or {}).items():
        name = (e.get("meta") or {}).get("name") or ""
        if not name:
            continue
        files = [f for f in e.get("files") or [] if not on or f.get("source") in on]
        out[_key(name)] = {"id": mid, "name": name, "count": len(group(files, name)) if files else 0,
                           "searched": int(e.get("st") or (e.get("ts") if e.get("ok") is not None else 0) or 0), "match": bool(q) and q in normalize_title(name)}
    return out


def add_artist(engine, store, name, should_stop=None, exact=False):
    """Ruční přidání (nebo nové prohledání) interpreta: prohledá zdroje zařízení a uloží nálezy (nezávisle
    na žánrech). `exact` = jméno je už vybrané z Last.fm (`lastfm_search`), neopravuje se.
    Vrací id interpreta, nebo None při krátkém jménu a výpadku všech zdrojů."""
    name = (name or "").strip()
    if len(_key(name)) < 2:
        return None
    if not exact:
        name = lastfm_name(str(engine._opt("lastfm_key") or "").strip(), name) or _nice(name)
    key = _key(name)
    if len(key) < 2:
        return None
    mid, tags = "a:" + key, config(store)["tags"]
    csig = sig(tags)
    with store.updating(INDEX, {}) as index:
        if index.get("sig") != csig:
            retag(index, tags)
        items = index.setdefault("items", {})
        entry = items.get(mid)
        if entry is None:
            entry = items[mid] = {"ok": None, "ts": 0, "rank": NO_RANK, "genres": [],
                                  "meta": {"id": mid, "name": name, "tags": []}}
        meta = entry.setdefault("meta", {"id": mid, "name": name, "tags": []})
        if meta.get("manual") or not meta.get("name"):   # jméno z poolu Last.fm má přednost
            meta["name"] = name
        meta["manual"], meta["lf"] = True, 1
        index["src"] = src_of(engine)
        old = list(entry.get("files") or [])
        names = [(e.get("meta") or {}).get("name") or "" for e in items.values()]
    with engine.background():
        files = search(engine, name, rivals(name, names), should_stop)
        if files is not None:
            files = _merge_files(engine, old, files, _now(), should_stop)
    if files is None:
        return None
    with store.updating(INDEX, {}) as index:
        entry = (index.get("items") or {}).get(mid)
        if entry is not None:
            record(index, mid, bool(files), _now())
            entry["st"] = _now()
            entry["misses"] = 0 if files else int(entry.get("misses") or 0) + 1
            if files:
                entry["files"] = files
            else:
                entry.pop("files", None)
    return mid


# --- synchronizace nálezů (okruh `concerts`) ----------------------------------------------

def collect(store, since, seen):
    """Změny sekce `concerts` od `since`: konfigurace (`c`) a prověření interpreti (`a:<id>`)."""
    out = {}
    cfg = collect_config(store)
    if cfg and seen(cfg) >= since:
        out["c"] = cfg
    index = load_index(store)
    for mid, e in (index.get("items") or {}).items():
        meta = e.get("meta") or {}
        if e.get("ok") is None or not e.get("ts") or not meta.get("name"):
            continue
        if seen({"ts": e["ts"], "rts": e.get("rts")}) < since:
            continue
        files = sorted((f for f in e.get("files") or [] if f.get("source") != "dav"),
                       key=lambda f: -int(f.get("size") or 0))[:SYNC_FILES]
        m = {"name": meta["name"], "tags": list(meta.get("tags") or [])}
        if meta.get("manual"):
            m["manual"] = True
        out["a:" + mid.partition(":")[2]] = {
            "ts": int(e["ts"]), "ok": bool(e["ok"]), "misses": int(e.get("misses") or 0),
            "src": index.get("src") or "", "meta": m,
            "files": [[f.get("ref"), f.get("name"), int(f.get("size") or 0), int(f.get("duration") or 0),
                       f.get("source"), f.get("img") or "" if i == 0 else "", int(f.get("t") or 0)]
                      for i, f in enumerate(files)]}
    return out


def _foreign_files(rec):
    out = []
    for r in rec.get("files") or []:
        if not (isinstance(r, (list, tuple)) and len(r) >= 7 and isinstance(r[0], str) and r[0]) \
                or r[0].startswith("dav:"):
            continue
        try:
            out.append({"ref": r[0], "name": str(r[1] or ""), "size": int(r[2] or 0), "duration": int(r[3] or 0),
                        "source": str(r[4] or ""), "img": str(r[5] or ""), "t": int(r[6] or 0) or int(rec["ts"]),
                        "s": int(rec["ts"])})
        except (TypeError, ValueError):
            continue
    return out


def _wanted(items, mid, ts, manual, mtags, tags):
    """Přijme se záznam? Nový interpret jen s vybraným žánrem (nebo ruční), známý jen s novějším `ts`, než má
    vlastní kontrola i poslední přijatý cizí záznam (`fts` – od zařízení s jinými zdroji se `ts` nepřebírá,
    takže bez něj by týž záznam vypadal jako nový v každém kole)."""
    entry = items.get(mid)
    if entry is None:
        return manual or bool(set(mtags) & set(tags))
    return ts > max(int(entry.get("ts") or 0), int(entry.get("fts") or 0))


def apply(store, changes, stamp=0):
    """Slije cizí nálezy; vrací počet přijatých záznamů. Soubory se sjednotí vždy, plán kontrol (`ok`, `ts`,
    `misses`) se převezme jen od zařízení se stejnými zdroji – jinak si interpreta zařízení prohledá samo.

    Cizí zařízení posílá při každé změně celý svůj stav, takže většina záznamů je známá a stará. Index se proto
    čte a zapisuje jednou za celé kolo a jen když je opravdu co přijmout – zápis po každém interpretovi
    (stovky přepisů celého souboru) držel na HA minutu plné jádro každých 5 minut."""
    if not isinstance(changes, dict) or not changes:
        return 0
    applied = apply_config(store, changes["c"], stamp) if "c" in changes else 0
    recs = []
    for key, rec in changes.items():
        kind, _, k = str(key).partition(":")
        meta = rec.get("meta") if isinstance(rec, dict) else None
        if kind != "a" or not k or not isinstance(meta, dict) or not meta.get("name") or not rec.get("ts"):
            continue
        try:
            ts = int(rec["ts"])
        except (TypeError, ValueError):
            continue
        recs.append(("a:" + k, dict(rec, ts=ts), meta,
                     [t for t in TAGS if t in set(meta.get("tags") or ())], bool(meta.get("manual"))))
    if not recs:
        return applied
    tags = config(store)["tags"]
    csig = sig(tags)
    current = store.load(INDEX, {})
    if isinstance(current, dict) and current.get("sig") == csig:
        items = current.get("items") or {}
        recs = [r for r in recs if _wanted(items, r[0], r[1]["ts"], r[4], r[3], tags)]
        if not recs:
            return applied
    with store.updating(INDEX, {}) as index:
        if index.get("sig") != csig:
            retag(index, tags)
        items = index.setdefault("items", {})
        for mid, rec, meta, mtags, manual in recs:
            ts = rec["ts"]
            if not _wanted(items, mid, ts, manual, mtags, tags):
                continue
            entry = items.get(mid)
            if entry is None:
                m = {"id": mid, "name": str(meta["name"]), "tags": mtags}
                if manual:
                    m["manual"] = True
                entry = items[mid] = {"ok": None, "ts": 0, "rank": NO_RANK, "genres": list(mtags), "meta": m}
            elif manual:
                m = entry.setdefault("meta", {"id": mid, "name": str(meta["name"]), "tags": mtags})
                if m.get("manual"):   # opravené jméno (Last.fm) z jiného zařízení
                    m["name"] = str(meta["name"])
                m["manual"] = True
            merged = {f["ref"]: f for f in _foreign_files(rec)}
            merged.update({f["ref"]: f for f in entry.get("files") or []})
            files = sorted(merged.values(), key=lambda f: -int(f.get("size") or 0))[:MAX_FILES]
            if files:
                entry["files"] = files
            # s vlastním úložištěm si plán kontrol vede každé zařízení samo (úložiště má každé jiné)
            if rec.get("src") and rec.get("src") == index.get("src") and "dav" not in rec["src"].split(","):
                entry["ok"], entry["ts"], entry["misses"] = bool(rec.get("ok")), ts, int(rec.get("misses") or 0)
                entry["st"] = ts
                if not entry["ok"]:
                    entry.pop("files", None)
            entry["fts"] = ts
            if stamp:
                entry["rts"] = stamp
            applied += 1
    return applied
