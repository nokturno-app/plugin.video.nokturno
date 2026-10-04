"""Vlastní katalogy: definice, ověřování dostupnosti streamů a synchronizace.

Společné pro Kodi, Home Assistant (ověřovatel) a Stremio. Katalog (`mycatalogs.json`, seznam dictů)
si uživatel poskládá z žánrů, jazyka, let a řazení; tituly skládá dashboard (`GET /discover`).
S `verify` se zobrazí jen tituly, ke kterým se našel stream podle požadavků (kvalita, zvuk, titulky,
5.1) – kandidáti i výsledky jsou v indexu `catindex` (`mycat_index_<id>`), který plní dávky na pozadí.

Synchronizace (`sync.py`, okruh `catalogs`) nese deník `mycatlog` (`{id: {"on", "ts"}}`), k němu celé
definice (`c:<id>`) a kompaktní výsledky ověření (`r:<id>`). Home Assistant ověřuje nepřetržitě;
zařízení, která vidí čerstvé cizí výsledky (`foreign_recent`), samo neověřuje a jen kreslí.
Výsledky nesou otisk zdrojů (`Engine.verify_fingerprint`), převezme je jen zařízení se stejnými zdroji.
"""
import datetime
import time
import unicodedata
import uuid

# `from .x import y`, ne `from . import x` — plochá kopie v Kodi umí jen tenhle tvar
from abort import Aborted
from concertcat import apply_config as concert_apply, collect_config as concert_collect, CONFIG as CONCERT_CONFIG
from catindex import compact, counts, merge_pool, merge_results, next_batch, record, signature, visible  # noqa: F401

DEFS = "mycatalogs"          # seznam definic katalogů
LOG = "mycatlog"             # {"<id>": {"on": bool, "ts": int}} – deník pro synchronizaci
INDEX = "mycat_index_"       # + id katalogu
SECTION = "catalogs"         # jméno sekce ve změnách synchronizace
PAUSED = "catalogs_paused"   # {"on": bool} – pozastavené ověřování na tomhle zařízení (zatím jen HA)
# témata = klíčová slova TMDB (žánr to není); id ověřená 2026-10-03, pořadí = pořadí ve formulářích
KEYWORDS = {"fairy": "3205|329731|358931|351899", "christmas": "207317", "halloween": "3335", "newyear": "613",
            "truestory": "9672", "book": "818", "biography": "5565", "superhero": "9715", "serialkiller": "10714",
            "ww2": "1956", "martialarts": "779", "sport": "6075", "alien": "9951", "timetravel": "4379",
            "zombie": "12377", "ghost": "162846", "vampire": "3133", "postapo": "4458", "heist": "10051", "spy": "470",
            "survival": "10349", "dog": "15162", "dinosaur": "12616"}
MAX_KEYWORDS = 3   # víc témat = víc id v `with_keywords` (whitelist pustí nejvýš 10)
TRACKS = ("", "CZ", "SK", "CZ|SK", "EN", "HU")
QUALITIES = (0, 3, 3.5, 4)
SHOWS = ("pool", "found", "released")
POOL_PAGES = 10
POOL_EVERY = 6 * 3600
ALPHA = "title.asc"          # řazení podle abecedy: kandidáti podle oblíbenosti, seřazení až u klienta
FOREIGN_FRESH = 2 * 3600     # cizí výsledky mladší než tohle = ověřuje jiné zařízení (HA)
COUNTRIES = ("CZ", "SK", "XC", "US", "GB", "FR", "DE", "IT", "ES", "PL", "HU", "KR", "JP", "DK", "SE", "NO")
MAX_COUNTRIES = 5
# ikony ze standardní sady skinu (jen názvy, obrázky patří skinu; Kodi k nim doplní popisky)
ICONS = ("DefaultMovies.png", "DefaultTVShows.png", "DefaultVideoPlaylists.png", "DefaultFavourites.png",
         "DefaultMusicTop100.png", "DefaultGenre.png", "DefaultYear.png", "DefaultCountry.png", "DefaultSets.png",
         "DefaultRecentlyAddedMovies.png", "DefaultActor.png", "DefaultStudios.png")
DEFAULT_ICONS = {"movie": "DefaultMovies.png", "series": "DefaultTVShows.png"}
# velikosti dávek ověřování: automatická na pozadí, ruční „Načíst teď“, první po založení nebo změně definice
AUTO_BATCH = 8
MANUAL_BATCH = 20
FIRST_BATCH = 40
# staré `lang` (původní jazyk) → země původu; formulář už jazyk neukládá
LANG_COUNTRIES = {"cs": ["CZ"], "sk": ["SK"], "cs|sk": ["CZ", "SK"], "sk|cs": ["CZ", "SK"], "de": ["DE"],
                  "fr": ["FR"], "es": ["ES"], "it": ["IT"], "pl": ["PL"], "hu": ["HU"], "ko": ["KR"],
                  "ja": ["JP"], "en": ["US", "GB"]}
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
                if "menu" not in cat and "menu" in c:   # formulář umístění v menu nezná, úprava ho nesmí zahodit
                    cat = dict(cat, menu=c["menu"])
                items[i] = cat
                break
        else:
            items.append(cat)
    _touch(store, cat["id"], True)


# Předvolby z bety 10.0.0~beta1 – už se nezakládají, jen se uklízí (`beta1_untouched`). (id, kind, název cs, pole)
BETA1_PRESETS = (
    ("pre-movie-popular", "movie", "Populární", {"sort": "popularity.desc"}),
    ("pre-movie-top", "movie", "Nejlépe hodnocené", {"sort": "vote_average.desc"}),
    ("pre-movie-cz-dub", "movie", "Nové s CZ dabingem",
     {"sort": "popularity.desc", "years": 2, "verify": True, "audio": "CZ", "show": "found"}),
    ("pre-movie-hq", "movie", "Filmy ve vysoké kvalitě",
     {"sort": "popularity.desc", "verify": True, "q": 4, "show": "found"}),
    ("pre-movie-czech", "movie", "České filmy", {"sort": "popularity.desc", "lang": "cs"}),
    ("pre-series-popular", "series", "Populární", {"sort": "popularity.desc"}),
    ("pre-series-top", "series", "Nejlépe hodnocené", {"sort": "vote_average.desc"}),
    ("pre-series-cz-dub", "series", "Nové s CZ dabingem",
     {"sort": "popularity.desc", "years": 2, "verify": True, "audio": "CZ", "show": "found"}),
    ("pre-series-czech", "series", "České seriály", {"sort": "popularity.desc", "lang": "cs"}),
)

# pole záznamu a jejich výchozí hodnoty (stejná pole ukládá formulář v Kodi)
FIELD_DEFAULTS = {"genres": [], "keywords": [], "join": "and", "lang": "", "countries": [], "year_from": None,
                  "year_to": None, "years": None, "sort": "popularity.desc", "verify": False, "q": 0, "audio": "",
                  "subs": "", "surround": False, "show": "found"}


def beta1_untouched(cat):
    """Je to předvolba z bety 1, ve které uživatel nic kromě názvu (a umístění v menu) nezměnil?
    Chybějící pole se bere jako výchozí hodnota."""
    for pid, kind, _name, fields in BETA1_PRESETS:
        if cat.get("id") != pid:
            continue
        want = dict(FIELD_DEFAULTS, **fields)
        have = dict(FIELD_DEFAULTS, **{k: v for k, v in cat.items() if k in FIELD_DEFAULTS})
        return cat.get("kind") == kind and all(have[k] == want[k] for k in ("genres", "keywords", "join", "lang",
                                               "year_from", "year_to", "years", "sort", "verify", "q", "audio",
                                               "subs", "surround", "show")) and not have["countries"]
    return False


# Šablony (jen data, nic se nezakládá): na stránce z mobilu „Načíst šablonu“ vyplní formulář, uživatel upraví a uloží.
TEMPLATES = (
    {"key": "movie-popular", "kind": "movie", "name": "Populární filmy", "fields": {"sort": "popularity.desc"}},
    {"key": "series-popular", "kind": "series", "name": "Populární seriály", "fields": {"sort": "popularity.desc"}},
    {"key": "movie-top", "kind": "movie", "name": "Nejlépe hodnocené filmy", "fields": {"sort": "vote_average.desc"}},
    {"key": "series-top", "kind": "series", "name": "Nejlépe hodnocené seriály",
     "fields": {"sort": "vote_average.desc"}},
    {"key": "movie-new-cz-dub", "kind": "movie", "name": "Nové filmy s CZ dabingem",
     "fields": {"sort": "popularity.desc", "years": 2, "verify": True, "audio": "CZ", "show": "found"}},
    {"key": "series-new-cz-dub", "kind": "series", "name": "Nové seriály s CZ dabingem",
     "fields": {"sort": "popularity.desc", "years": 2, "verify": True, "audio": "CZ", "show": "found"}},
    {"key": "movie-4k-cz-dub", "kind": "movie", "name": "Filmy ve 4K s CZ dabingem",
     "fields": {"sort": "popularity.desc", "verify": True, "q": 4, "audio": "CZ", "show": "found"}},
    {"key": "movie-czech", "kind": "movie", "name": "České filmy",
     "fields": {"sort": "popularity.desc", "countries": ["CZ"]}},
    {"key": "series-czech", "kind": "series", "name": "České seriály",
     "fields": {"sort": "popularity.desc", "countries": ["CZ"]}},
    {"key": "movie-fairy-cz-dub", "kind": "movie", "name": "Pohádky s CZ dabingem",
     "fields": {"sort": "popularity.desc", "keywords": ["fairy"], "verify": True, "audio": "CZ", "show": "found"}},
)


def templates(kind=None):
    return [t for t in TEMPLATES if kind is None or t["kind"] == kind]


def template_fields(key):
    """Plná pole šablony (výchozí hodnoty + pole šablony) včetně `kind`, nebo None."""
    t = next((t for t in TEMPLATES if t["key"] == key), None)
    return None if t is None else dict(FIELD_DEFAULTS, kind=t["kind"], **t["fields"])


def icon_for(cat):
    """Ikona katalogu: platná z `ICONS`, jinak výchozí podle druhu."""
    icon = cat.get("icon")
    return icon if icon in ICONS else DEFAULT_ICONS.get(cat.get("kind"), "DefaultVideoPlaylists.png")


def countries_of(cat):
    """Platné kódy zemí katalogu (bez duplicit, nejvýš `MAX_COUNTRIES`)."""
    out = []
    for c in cat.get("countries") or []:
        if c in COUNTRIES and c not in out:
            out.append(c)
    return out[:MAX_COUNTRIES]


def migrate_lang(cat):
    """Země ze starého `lang` (původní jazyk), je-li katalog bez `countries`; jinak jeho země. Nic neukládá."""
    have = countries_of(cat)
    if have:
        return have
    return list(LANG_COUNTRIES.get(str(cat.get("lang") or "").lower(), []))


def auto_name(cat, labels, default="Vlastní katalog"):
    """Název z parametrů katalogu. Jádro nemá překlady, popisky dodává volající v `labels`:
    `genres` {id: název}, `keywords` {klíč: název}, `countries` {kód: název}, `qualities` {3: "Full HD", …},
    `dub` „{} dabing“, `subs` „titulky {}“, `surround` „5.1“, `years` „posledních {} let“,
    `year_from`/`year_to`/`year_range` „od {}“/„do {}“/„{}–{}“. Požadavky ověřování se berou jen s `verify`."""
    def lab(key, fallback):
        return labels.get(key) or fallback

    parts = []
    names = [(labels.get("genres") or {}).get(str(g)) for g in cat.get("genres") or []]
    names += [(labels.get("keywords") or {}).get(k) for k in cat.get("keywords") or []]
    names = [n for n in names if n][:3]
    if names:
        parts.append(", ".join(names))
    zeme = [(labels.get("countries") or {}).get(c) or c for c in countries_of(cat)[:2]]
    if zeme:
        parts.append(", ".join(zeme))
    if cat.get("verify"):
        audio, subs = cat.get("audio") or "", cat.get("subs") or ""
        if audio in TRACKS and audio:
            parts.append(lab("dub", "{} dabing").format(audio.replace("|", "/")))
        if subs in TRACKS and subs:
            parts.append(lab("subs", "titulky {}").format(subs.replace("|", "/")))
        q = norm_quality(cat.get("q"))
        if q:
            parts.append((labels.get("qualities") or {}).get(q) or {3: "Full HD", 3.5: "2K", 4: "4K"}[q])
        if cat.get("surround"):
            parts.append(lab("surround", "5.1"))
    years = cat.get("years")
    if isinstance(years, int) and not isinstance(years, bool) and 1 <= years <= 50:
        parts.append(lab("years", "posledních {} let").format(years))
    else:
        yf, yt = cat.get("year_from"), cat.get("year_to")
        if yf and yt:
            parts.append(lab("year_range", "{}–{}").format(yf, yt))
        elif yf:
            parts.append(lab("year_from", "od {}").format(yf))
        elif yt:
            parts.append(lab("year_to", "do {}").format(yt))
    return " · ".join(parts) or default


def delete(store, cid):
    with store.updating(DEFS, []) as items:
        items[:] = [c for c in items if not (isinstance(c, dict) and c.get("id") == cid)]
    _touch(store, cid, False)
    store.save(INDEX + cid, {})


def params(cat, today=None):
    """Uložené volby → parametry `DashApi.discover`. `years` (posledních X let) se počítá při každém dotazu."""
    genres = [str(g) for g in cat.get("genres") or []]
    keywords = [KEYWORDS[k] for k in cat.get("keywords") or [] if k in KEYWORDS][:MAX_KEYWORDS]
    years = cat.get("years")
    if isinstance(years, int) and not isinstance(years, bool) and 1 <= years <= 50:
        year_from, year_to = str((today or datetime.date.today()).year - years + 1), ""
    else:
        year_from, year_to = cat.get("year_from") or "", cat.get("year_to") or ""
    out = {"with_genres": ("|" if cat.get("join") == "or" else ",").join(genres),
           "with_keywords": "|".join(keywords),
           "with_origin_country": "|".join(countries_of(cat)),
           "with_original_language": cat.get("lang") or "",
           "sort_by": "popularity.desc" if cat.get("sort") == ALPHA else cat.get("sort") or "",
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


def sig(cat):
    return signature(*definition(cat))


def verified(store):
    """Ověřované katalogy (režim „jen tituly se streamem“). Koncertní katalogy z bety 1 (druh `concert`)
    se neověřují – koncerty jsou od 10.0.0 samostatný modul (`concertcat`)."""
    return [c for c in catalogs(store) if c.get("verify") and c.get("kind") in ("movie", "series")][:MAX_VERIFIED]


def pool(dash, cat):
    """Kandidáti z `/discover` (jen `tt` id, bez duplicit); None = výpadek serveru, index se nemění."""
    return pool_for(dash, "series" if cat.get("kind") == "series" else "movie", params(cat), cat.get("sort") == ALPHA)


def alpha_key(meta):
    """Klíč pro řazení podle abecedy: název bez diakritiky a velikosti písmen."""
    name = unicodedata.normalize("NFKD", str(meta.get("name") or ""))
    return "".join(c for c in name if not unicodedata.combining(c)).casefold()


def pool_for(dash, kind, discover_params, alpha=False):
    """Totéž pro hotové parametry – sdílí to Stremio, které má katalogy v jiném tvaru. `alpha` = výsledek
    (nejoblíbenější tituly podle filtrů, nejvýš `POOL_PAGES` stránek) seřadit podle abecedy."""
    out, seen = [], set()
    page, pages = 1, 1
    while page <= min(pages, POOL_PAGES):
        metas, pages = dash.discover(kind, discover_params, page)
        if metas is None:
            return None if page == 1 else (sorted(out, key=alpha_key) if alpha else out)
        for m in metas:
            mid = str(m.get("id") or "")
            if mid.startswith("tt") and mid not in seen:
                seen.add(mid)
                out.append(m)
        page += 1
    return sorted(out, key=alpha_key) if alpha else out


def load_index(store, cat):
    """Index katalogu, `{}` při jiné definici (neplatný)."""
    index = store.load(INDEX + cat["id"], {})
    return index if isinstance(index, dict) and index.get("sig") == sig(cat) else {}


def _foreign_ok(index):
    """Nálezy z jiného zařízení s jinými zdroji (`index["foreign"]`): {id: kdy vyhověl}."""
    res = (index.get("foreign") or {}).get("res") or {}
    out = {}
    for mid, rec in res.items():
        if isinstance(rec, (list, tuple)) and len(rec) >= 2 and rec[0]:
            try:
                out[mid] = int(rec[2] if len(rec) > 2 else rec[1])
            except (TypeError, ValueError):
                continue
    return out


def shown(index, sort="found"):
    """Tituly k zobrazení: vlastní nálezy a navíc nálezy jiného zařízení u titulů, které tohle zařízení ještě
    samo neověřilo (mobil po synchronizaci nečeká prázdný, než projde stovky titulů). Vlastní výsledek má přednost."""
    tentative = _foreign_ok(index)
    if tentative:
        items = {mid: dict(e, ok=True, found=e.get("found") or tentative[mid])
                 if e.get("ok") is None and mid in tentative else e
                 for mid, e in (index.get("items") or {}).items()}
        index = dict(index, items=items)
    return visible(index, sort=sort)


def foreign_recent(index, now=None):
    return (now or _now()) - int(index.get("foreign_ts") or 0) < FOREIGN_FRESH


def refresh(engine, store, dash, cid, size=1, verify=True, should_stop=None):
    """Jedna dávka: obnoví pool (po `POOL_EVERY`) a ověří až `size` titulů. `verify=False` = jen pool
    (zařízení s cizími výsledky potřebuje metadata, ne ověřování). Vrací počet ověřených titulů."""
    cat = next((c for c in catalogs(store) if c.get("id") == cid), None)
    if not cat or not cat.get("verify"):
        return 0
    kind = "series" if cat.get("kind") == "series" else "movie"
    name, csig, now = INDEX + cid, sig(cat), _now()
    src = engine.verify_fingerprint()
    index = store.reload(name, {})
    stale = index.get("sig") != csig or now - int(index.get("pool_ts") or 0) >= POOL_EVERY
    fresh = pool(dash, cat) if stale else None   # síť mimo zámek
    with store.updating(name, {}) as index:
        if index.get("sig") != csig:
            index.clear()
            index["sig"] = csig
        # ponytail: při změně zdrojů se staré výsledky nemažou, přeověří je běžná perioda (3/7 dní)
        index["src"] = src
        index.setdefault("salt", uuid.uuid4().hex[:8])
        if "pending" in index:   # tvar do 10.1.1
            index.setdefault("foreign", index.pop("pending"))
        held = index.get("foreign")
        if isinstance(held, dict) and held.get("src") == src:
            del index["foreign"]
            if merge_results(index, held.get("res")) and held.get("rstamp"):
                index["rts"] = now
        if fresh is not None:
            merge_pool(index, fresh, now)
            index["pool_ts"] = now
        batch = next_batch(index, now, size, first=_foreign_ok(index)) if verify else []
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


def overview(store):
    """Stav pro HA (služba `nokturno.catalogs`, senzor): {"paused", "catalogs": [{id, name, kind, verified, matched,
    total, last_check}]} – jen ověřované katalogy; `last_check` = ISO čas posledního ověření nebo None."""
    rows = []
    for cat in verified(store):
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
    cfg = concert_collect(store)   # koncerty: jen žánry, nálezy se nesynchronizují
    if cfg and seen(cfg) >= since:
        out["c:" + CONCERT_CONFIG] = cfg
    for cat in verified(store):
        index = load_index(store, cat)
        res = compact(index)
        if not res:
            continue
        ts = max(v[1] for v in res.values())
        if seen({"ts": ts, "rts": index.get("rts")}) >= since:
            out["r:" + cat["id"]] = {"ts": ts, "sig": sig(cat), "src": index.get("src") or "", "res": res}
    return out


def _valid(cat, cid):
    return isinstance(cat, dict) and cat.get("id") == cid and cat.get("kind") in ("movie", "series")


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
        if kind == "c" and cid == CONCERT_CONFIG:
            applied += concert_apply(store, rec, stamp)
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
            if not cat or rec.get("sig") != sig(cat):
                continue
            with store.updating(INDEX + cid, {}) as index:
                if index.get("sig") != rec["sig"]:
                    index.clear()
                    index["sig"] = rec["sig"]
                # zdroje neznámé (do prvního `refresh`) nebo jiné (Luna doma, mobil bez ní): výsledky se nepřevezmou,
                # zařízení ověřuje samo a do té doby ukazuje cizí nálezy jako prozatímní (`shown`)
                if index.get("src") != rec.get("src") or not index.get("src"):
                    if _ts(rec) >= _ts(index.get("foreign")):
                        index["foreign"] = dict(rec, rstamp=stamp)
                    continue
                taken = merge_results(index, rec.get("res"))
                if taken and stamp:
                    index["rts"] = stamp
            applied += taken
    return applied
