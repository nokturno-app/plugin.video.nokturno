"""Vlastní katalogy: definice, ověřování dostupnosti streamů a synchronizace.

Společné pro Kodi, Home Assistant (ověřovatel) a Stremio. Katalog (`mycatalogs.json`, seznam dictů)
si uživatel poskládá z žánrů, jazyka, let a řazení; tituly skládá dashboard (`GET /discover`).
S `verify` se zobrazí jen tituly, ke kterým se našel stream podle požadavků (kvalita, zvuk, titulky,
5.1) – kandidáti i výsledky jsou v indexu `catindex` (`mycat_index_<id>`), který plní dávky na pozadí.

Synchronizace (`sync.py`, okruh `catalogs`) nese deník `mycatlog` (`{id: {"on", "ts"}}`), k němu celé
definice (`c:<id>`) a kompaktní výsledky ověření (`r:<id>`). Home Assistant ověřuje nepřetržitě;
zařízení, která vidí čerstvé cizí výsledky (`foreign_recent`), samo neověřuje a jen kreslí.
"""
import datetime
import time

# `from .x import y`, ne `from . import x` — plochá kopie v Kodi umí jen tenhle tvar
from abort import Aborted
from concertcat import ConcertError, POOL_EVERY as CONCERT_POOL_EVERY, TAGS as CONCERT_TAGS
from concertcat import pool as concert_pool, search as concert_search
from concertfilter import rivals as concert_rivals
from catindex import compact, counts, merge_pool, merge_results, next_batch, record, signature, visible  # noqa: F401

DEFS = "mycatalogs"          # seznam definic katalogů
LOG = "mycatlog"             # {"<id>": {"on": bool, "ts": int}} – deník pro synchronizaci
INDEX = "mycat_index_"       # + id katalogu
SECTION = "catalogs"         # jméno sekce ve změnách synchronizace
PAUSED = "catalogs_paused"   # {"on": bool} – pozastavené ověřování na tomhle zařízení (zatím jen HA)
KEYWORDS = {"fairy": "3205|329731|358931|351899"}
TRACKS = ("", "CZ", "SK", "CZ|SK", "EN", "HU")
QUALITIES = (0, 3, 3.5, 4)
SHOWS = ("pool", "found", "released")
POOL_PAGES = 10
POOL_EVERY = 6 * 3600
FOREIGN_FRESH = 2 * 3600     # cizí výsledky mladší než tohle = ověřuje jiné zařízení (HA)
MAX_VERIFIED = 20            # blob relaye má 128 kB: 20 katalogů × 200 titulů = ~51 kB po gzipu


def _now():
    return int(time.time())


def _ts(rec):
    try:
        return int((rec or {}).get("ts") or 0)
    except (TypeError, ValueError, AttributeError):
        return 0


def catalogs(store, kind=None):
    items = store.load(DEFS, [])
    items = items if isinstance(items, list) else []
    return [c for c in items if isinstance(c, dict) and c.get("id") and (kind is None or c.get("kind") == kind)]


def _touch(store, cid, on):
    with store.updating(LOG, {}) as log:
        log[cid] = {"on": bool(on), "ts": _now()}


def save(store, cat):
    """Vloží nebo nahradí katalog podle `id` a zapíše změnu do deníku (jinak by ji synchronizace neposlala)."""
    with store.updating(DEFS, []) as items:
        for i, c in enumerate(items):
            if isinstance(c, dict) and c.get("id") == cat.get("id"):
                items[i] = cat
                break
        else:
            items.append(cat)
    _touch(store, cat["id"], True)


def delete(store, cid):
    with store.updating(DEFS, []) as items:
        items[:] = [c for c in items if not (isinstance(c, dict) and c.get("id") == cid)]
    _touch(store, cid, False)
    store.save(INDEX + cid, {})


def params(cat, today=None):
    """Uložené volby → parametry `DashApi.discover`. `years` (posledních X let) se počítá při každém dotazu."""
    genres = [str(g) for g in cat.get("genres") or []]
    keywords = [KEYWORDS[k] for k in cat.get("keywords") or [] if k in KEYWORDS]
    years = cat.get("years")
    if isinstance(years, int) and not isinstance(years, bool) and 1 <= years <= 50:
        year_from, year_to = str((today or datetime.date.today()).year - years + 1), ""
    else:
        year_from, year_to = cat.get("year_from") or "", cat.get("year_to") or ""
    out = {"with_genres": ("|" if cat.get("join") == "or" else ",").join(genres),
           "with_keywords": "|".join(keywords),
           "with_original_language": cat.get("lang") or "", "sort_by": cat.get("sort") or "",
           "year_from": year_from, "year_to": year_to}
    return {k: v for k, v in out.items() if v}


def norm_quality(q):
    """Kanonická hodnota z `QUALITIES` (3.0 → 3, ať je otisk stejný z JSONu i ze Stremia), jinak 0."""
    try:
        return next(x for x in QUALITIES if x == float(q))
    except (TypeError, ValueError, StopIteration):
        return 0


def definition(cat):
    """(min_quality, 5.1, zvuk, titulky) pro `Engine.verify_title`; neplatné hodnoty = bez požadavku."""
    audio, subs = cat.get("audio") or "", cat.get("subs") or ""
    return (norm_quality(cat.get("q")), bool(cat.get("surround")),
            audio if audio in TRACKS else "", subs if subs in TRACKS else "")


def concert_tags(cat):
    """Žánry katalogu koncertů – jen známé štítky (`concertcat.TAGS`)."""
    return [t for t in cat.get("tags") or [] if t in CONCERT_TAGS]


def sig(cat):
    if cat.get("kind") == "concert":   # změna žánrů = nový index
        return signature("concert", False, ",".join(sorted(concert_tags(cat))))
    return signature(*definition(cat))


def verified(store, concerts=True):
    """Ověřované katalogy; `concerts=False` vynechá koncertní (HA je neověřuje)."""
    return [c for c in catalogs(store) if c.get("verify") and (concerts or c.get("kind") != "concert")][:MAX_VERIFIED]


def pool(dash, cat):
    """Kandidáti z `/discover` (jen `tt` id, bez duplicit); None = výpadek serveru, index se nemění."""
    return pool_for(dash, "series" if cat.get("kind") == "series" else "movie", params(cat))


def pool_for(dash, kind, discover_params):
    """Totéž pro hotové parametry – sdílí to Stremio, které má katalogy v jiném tvaru."""
    out, seen = [], set()
    page, pages = 1, 1
    while page <= min(pages, POOL_PAGES):
        metas, pages = dash.discover(kind, discover_params, page)
        if metas is None:
            return None if page == 1 else out
        for m in metas:
            mid = str(m.get("id") or "")
            if mid.startswith("tt") and mid not in seen:
                seen.add(mid)
                out.append(m)
        page += 1
    return out


def load_index(store, cat):
    """Index katalogu, `{}` při jiné definici (neplatný)."""
    index = store.load(INDEX + cat["id"], {})
    return index if isinstance(index, dict) and index.get("sig") == sig(cat) else {}


def foreign_recent(index, now=None):
    return (now or _now()) - int(index.get("foreign_ts") or 0) < FOREIGN_FRESH


def refresh(engine, store, dash, cid, size=1, verify=True, should_stop=None):
    """Jedna dávka: obnoví pool (po `POOL_EVERY`) a ověří až `size` titulů. `verify=False` = jen pool
    (zařízení s cizími výsledky potřebuje metadata, ne ověřování). Vrací počet ověřených titulů."""
    cat = next((c for c in catalogs(store) if c.get("id") == cid), None)
    if not cat or not cat.get("verify"):
        return 0
    if cat.get("kind") == "concert":
        return refresh_concert(engine, store, cat, size, verify, should_stop)
    kind = "series" if cat.get("kind") == "series" else "movie"
    name, csig, now = INDEX + cid, sig(cat), _now()
    index = store.reload(name, {})
    stale = index.get("sig") != csig or now - int(index.get("pool_ts") or 0) >= POOL_EVERY
    fresh = pool(dash, cat) if stale else None   # síť mimo zámek
    with store.updating(name, {}) as index:
        if index.get("sig") != csig:
            index.clear()
            index["sig"] = csig
        if fresh is not None:
            merge_pool(index, fresh, now)
            index["pool_ts"] = now
        batch = next_batch(index, now, size) if verify else []
    done = 0
    for mid in batch:
        if should_stop and should_stop():
            break
        try:
            with engine.background():
                result = engine.verify_title(kind, mid, *definition(cat))
        except Aborted:
            raise
        except Exception:  # noqa: BLE001 – výpadek zdroje = zkusit později
            result = None
        with store.updating(name, {}) as index:
            if index.get("sig") == csig:
                record(index, mid, result, _now())
        done += 1
    return done


CONCERT_RETRY = 1800   # po selhání načtení poolu z Last.fm další pokus nejdřív za 30 min


def refresh_concert(engine, store, cat, size=1, verify=True, should_stop=None):
    """Totéž pro katalog koncertů: pool interpretů z Last.fm (vlastní klíč `lastfm_key`) a hledání koncertů
    ve zdrojích zařízení. Soubory se ukládají k interpretovi v indexu a nikam se nesynchronizují."""
    key = str(engine._opt("lastfm_key") or "").strip()
    if not key:
        return 0
    name, csig, now = INDEX + cat["id"], sig(cat), _now()
    index = store.reload(name, {})
    stale = (index.get("sig") != csig or now - int(index.get("pool_ts") or 0) >= CONCERT_POOL_EVERY) \
        and now - int(index.get("pool_try") or 0 if index.get("sig") == csig else 0) >= CONCERT_RETRY
    fresh = None
    if stale:   # síť mimo zámek
        try:
            fresh = concert_pool(key, concert_tags(cat))
        except ConcertError:
            fresh = None
    with store.updating(name, {}) as index:
        if index.get("sig") != csig:
            index.clear()
            index["sig"] = csig
        if stale:
            index["pool_try"] = now
        if fresh is not None:
            merge_pool(index, fresh, now)
            index["pool_ts"] = now
        batch = [(m, index["items"][m]["meta"].get("name") or "") for m in next_batch(index, now, size)] if verify else []
        names = [(e.get("meta") or {}).get("name") or "" for e in (index.get("items") or {}).values()]
    done = 0
    for mid, artist in batch:
        if should_stop and should_stop():
            break
        files = None
        if artist:
            try:
                with engine.background():
                    files = concert_search(engine, artist, concert_rivals(artist, names), should_stop)
            except Aborted:
                raise
            except Exception:  # noqa: BLE001 – výpadek zdroje = zkusit později
                files = None
        with store.updating(name, {}) as index:
            entry = (index.get("items") or {}).get(mid)
            if index.get("sig") == csig and entry is not None:
                record(index, mid, None if files is None else bool(files), _now())
                if files:
                    entry["files"] = files
                else:
                    entry.pop("files", None)
        done += 1
    return done


def overview(store):
    """Stav pro HA (služba `nokturno.catalogs`, senzor): {"paused", "catalogs": [{id, name, kind, verified, matched,
    total, last_check}]} – jen ověřované katalogy; `last_check` = ISO čas posledního ověření nebo None."""
    rows = []
    for cat in verified(store, concerts=False):
        index = load_index(store, cat)
        checked, matched, total = counts(index)
        last = max((int(e.get("ts") or 0) for e in (index.get("items") or {}).values()), default=0)
        rows.append({"id": cat["id"], "name": cat.get("name") or cat["id"], "kind": cat.get("kind"),
                     "verified": checked, "matched": matched, "total": total,
                     "last_check": datetime.datetime.fromtimestamp(last, datetime.timezone.utc).isoformat(
                         timespec="seconds") if last else None})
    return {"paused": bool((store.load(PAUSED, {}) or {}).get("on")), "catalogs": rows}


# --- synchronizace ------------------------------------------------------------------

def _backfill(store):
    """Katalogy vytvořené před deníkem v něm chybí – jednou se doplní s aktuálním časem."""
    log = store.reload(LOG, {})
    missing = [c["id"] for c in catalogs(store) if c["id"] not in log]
    if missing:
        now = _now()
        with store.updating(LOG, {}) as data:
            for cid in missing:
                data.setdefault(cid, {"on": True, "ts": now})


def collect(store, since, seen):
    """Změny sekce `catalogs` od `since`. `seen(rec)` = kdy záznam dorazil (`sync._seen`)."""
    _backfill(store)
    cats = {c["id"]: c for c in catalogs(store)}
    out = {}
    for cid, rec in store.reload(LOG, {}).items():
        if not isinstance(rec, dict) or seen(rec) < since:
            continue
        if not rec.get("on"):
            out["c:" + cid] = {"on": False, "ts": _ts(rec)}
        elif cid in cats:
            out["c:" + cid] = {"on": True, "ts": _ts(rec), "cat": cats[cid]}
    for cat in verified(store, concerts=False):   # nálezy koncertů se nesynchronizují, jen definice
        index = load_index(store, cat)
        res = compact(index)
        if not res:
            continue
        ts = max(v[1] for v in res.values())
        if seen({"ts": ts, "rts": index.get("rts")}) >= since:
            out["r:" + cat["id"]] = {"ts": ts, "sig": sig(cat), "res": res}
    return out


def _valid(cat, cid):
    if not (isinstance(cat, dict) and cat.get("id") == cid and cat.get("kind") in ("movie", "series", "concert")):
        return False
    return cat.get("kind") != "concert" or bool(concert_tags(cat))


def apply(store, changes, stamp=0):
    """Slije cizí změny sekce `catalogs`. Vrací počet přijatých záznamů; `stamp` = čas příjmu (jen střed, HA)."""
    if not isinstance(changes, dict) or not changes:
        return 0
    applied = 0
    for key in sorted(changes, key=lambda k: not str(k).startswith("c:")):   # definice před výsledky
        rec = changes[key]
        kind, _, cid = str(key).partition(":")
        if kind not in ("c", "r") or not cid or not isinstance(rec, dict):
            continue
        if kind == "c":
            on, cat = bool(rec.get("on")), rec.get("cat")
            if on and not _valid(cat, cid):
                continue
            with store.updating(LOG, {}) as log, store.updating(DEFS, []) as defs:
                if _ts(rec) <= _ts(log.get(cid)):
                    continue
                log[cid] = dict({"on": on, "ts": _ts(rec)}, **({"rts": stamp} if stamp else {}))
                at = next((i for i, c in enumerate(defs) if isinstance(c, dict) and c.get("id") == cid), None)
                if on and at is None:
                    defs.append(cat)
                elif on:
                    defs[at] = cat
                elif at is not None:
                    del defs[at]
            if not on:
                store.save(INDEX + cid, {})
            applied += 1
        else:
            cat = next((c for c in catalogs(store) if c["id"] == cid), None)
            if not cat or cat.get("kind") == "concert" or rec.get("sig") != sig(cat):
                continue
            with store.updating(INDEX + cid, {}) as index:
                if index.get("sig") != rec["sig"]:
                    index.clear()
                    index["sig"] = rec["sig"]
                taken = merge_results(index, rec.get("res"))
                if taken and stamp:
                    index["rts"] = stamp
            applied += taken
    return applied
