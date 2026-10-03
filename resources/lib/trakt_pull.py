"""Zhlédnuté a rozkoukané z Traktu do vlastní evidence (`watched.json`).

Nokturno na Trakt posílá samo (scrobble, ruční označení). Tohle je opačný směr:
co uživatel dokoukal nebo rozkoukal jinde — Stremio, Nuvio, cizí doplněk Kodi —
se objeví v Pokračovat ve sledování a Naposledy zhlédnuté.

- Nejdřív `/sync/last_activities`: když se na Traktu od minula nic nezměnilo,
  dál se nic nestahuje, kolo stojí jeden dotaz.
- Historie přírůstkově od posledního kola (`/sync/history?start_at=`), napoprvé
  jen `FIRST_DAYS` zpět. Rozkoukané (`/sync/playback`) vrací Trakt vždy celé a drží
  i měsíce staré pauzy — berou se jen ty za posledních `FIRST_DAYS` dní.
- Záznam se přepíše jen tehdy, když je na Traktu novější než u nás (`ts`).
  Vlastní scrobble se tak vrací jako ozvěna, kterou nic nezmění: dokoukaný titul
  už zhlédnutý je, a pozice rozkoukaného se liší o pár sekund (`SAME_POS`).
- Přijatý záznam dostane `rts` (čas příjmu), aby ho synchronizace poslala dál,
  i když jeho `ts` (čas na Traktu) je starší než poslední výměna.
- Odebrání zhlédnutí na Traktu se nepřenáší — jen přibývá.
- **Watchlist = Můj seznam** (`mirror_watchlist`). Týž dotaz `last_activities` řekne
  o změně Watchlistu (`watchlisted_at`); deník Mého seznamu (`favlog`) o změně u nás.
  Jen když se něco z toho pohnulo, stáhne se Watchlist a obě strany se srovnají.
"""
import calendar
import re
import time

from store import WATCHED_MAX

STATE = "trakt_pull"     # trakt_pull.json: {"activities": {...}, "history_at": ISO}
FIRST_DAYS = 90          # ponytail: napoprvé jen 90 dní historie, starší zhlédnutí Kodi nedostane
MAX_PAGES = 10           # po 100 záznamech; delší přírůstek dožene další kolo
SAME_POS = 60            # s — rozdíl pozice, který se bere jako ozvěna vlastního scrobble


def _epoch(iso):
    try:
        return calendar.timegm(time.strptime(str(iso)[:19], "%Y-%m-%dT%H:%M:%S"))
    except (TypeError, ValueError):
        return 0


def _iso(ts):
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(ts))


def _base(ids):
    ids = ids or {}
    if ids.get("imdb"):
        return str(ids["imdb"])
    if ids.get("tmdb"):
        return f"tmdb:{ids['tmdb']}"
    return None


def item_key(entry):
    """Klíč evidence z položky Traktu: `tt…` u filmu, `tt…:S:E` u dílu, jinak None."""
    if entry.get("type") == "movie" or ("movie" in entry and "episode" not in entry):
        return _base((entry.get("movie") or {}).get("ids"))
    ep, show = entry.get("episode") or {}, _base((entry.get("show") or {}).get("ids"))
    if not show or ep.get("season") is None or ep.get("number") is None:
        return None
    return f"{show}:{int(ep['season'])}:{int(ep['number'])}"


def _runtime(entry):
    node = entry.get("episode") or entry.get("movie") or {}
    return int(node.get("runtime") or (entry.get("show") or {}).get("runtime") or 0) * 60


def _activities(act):
    """Jen časy, které se týkají zhlédnutí a rozkoukání."""
    return {f"{k}.{f}": (act.get(k) or {}).get(f) for k in ("movies", "episodes") for f in ("watched_at", "paused_at")}


def apply_history(data, events, now):
    """Zhlédnutí do `data` (obsah watched.json). Vrací počet změněných klíčů."""
    n = 0
    for e in events:
        key, ts = item_key(e), _epoch(e.get("watched_at"))
        if not key or not ts:
            continue
        rec = data.get(key) or {}
        if (rec.get("playcount") and not float(rec.get("resume") or 0)) or int(rec.get("ts") or 0) >= ts:
            continue
        data[key] = dict(rec, playcount=max(1, int(rec.get("playcount") or 0)), resume=0, ts=ts, rts=now)
        n += 1
    return n


def apply_playback(data, items, now):
    """Rozkoukané do `data`. Bez stopáže se pozice spočítat nedá, takový titul se přeskočí."""
    n, oldest = 0, now - FIRST_DAYS * 86400
    for e in items:
        key, ts, total = item_key(e), _epoch(e.get("paused_at")), _runtime(e)
        if not key or ts < oldest or not total:
            continue
        pos = round(float(e.get("progress") or 0) / 100 * total, 1)
        rec = data.get(key) or {}
        if int(rec.get("ts") or 0) >= ts or pos <= 0:
            continue
        if not rec.get("playcount") and abs(float(rec.get("resume") or 0) - pos) < SAME_POS:
            continue
        data[key] = dict(rec, playcount=0, resume=pos, total=float(total), ts=ts, rts=now)
        n += 1
    return n


def _watchlist(act):
    return {k: (act.get(k) or {}).get("watchlisted_at") for k in ("movies", "shows")}


_TRAKT_ID = re.compile(r"^(tt\d+|tmdb:\d+)$")   # dílu ani souboru (`ws:`) Watchlist nerozumí


def _local_changes(store, since):
    """{klíč: zapnuto?} z deníku Mého seznamu po `since` (i co přišlo synchronizací)."""
    out = {}
    for key, rec in (store.reload("favlog", {}) or {}).items():
        if isinstance(rec, dict) and _TRAKT_ID.match(key) and \
                max(int(rec.get("ts") or 0), int(rec.get("rts") or 0)) > since:
            out[key] = bool(rec.get("on"))
    return out


def _kind(store, key):
    return "series" if (store.item(key) or {}).get("type") == "series" else "movie"


def mirror_watchlist(store, trakt):
    """Srovná Trakt Watchlist s Mým seznamem. Vrací počet změn v Mém seznamu.

    Co přibylo/ubylo na Traktu od minula (`wl`), se propíše do Mého seznamu; co
    přibylo/ubylo u nás od minula (`favlog` po `wl_ts`), se pošle na Trakt. Napoprvé
    jen sjednocení obou stran, nic se nemaže.
    ponytail: změna z jiného Kodi, která dorazí synchronizací se starším `ts` a bez
    `rts` (razí ho jen HA), se na Trakt nepošle — dožene ji HA, nebo ruční přidání.
    """
    state = store.reload(STATE, {}) or {}
    first = "wl" not in state
    prev = state.get("wl") or {}
    remote = {i["id"]: i for i in trakt.watchlist("movies") + trakt.watchlist("shows")}
    favs = set(store.favourites())
    local = {k: True for k in favs if _TRAKT_ID.match(k)} if first else _local_changes(store, state.get("wl_ts", 0))
    add = [k for k, on in local.items() if on and k not in remote]
    drop = [k for k, on in local.items() if not on and k in remote and not first]
    if add:
        trakt.watchlist_change(True, [(k, _kind(store, k)) for k in add])
    if drop:
        trakt.watchlist_change(False, [(k, remote[k]["type"]) for k in drop])
    n = 0
    # od nejstaršího, ať nejnovější přidání skončí v Mém seznamu nahoře
    for it in sorted(remote.values(), key=lambda i: i.get("listed_at") or ""):
        key = it["id"]
        if key not in prev and key not in favs and key not in local:
            store.toggle_favourite(key, {"type": it["type"], "id": key, "title": it.get("title") or key,
                                         "year": it.get("year"), "art": {}})
            n += 1
    if not first:
        for key in prev:
            if key not in remote and key in favs and key not in local:
                store.toggle_favourite(key)
                n += 1
    wl = {k: v["type"] for k, v in remote.items() if k not in drop}
    wl.update({k: _kind(store, k) for k in add})
    store.save(STATE, dict(store.reload(STATE, {}) or {}, wl=wl, wl_ts=int(time.time())))
    return n


def pull(store, trakt, now=None):
    """Jedno kolo. Vrací počet přijatých záznamů (zhlédnuté, rozkoukané, Můj seznam);
    výjimky Traktu (`TraktError`) letí ven."""
    now = int(now or time.time())
    state = store.reload(STATE, {}) or {}
    raw = trakt.last_activities()
    act, wl = _activities(raw), _watchlist(raw)
    mirrored = 0
    if "wl" not in state or wl != state.get("watchlist") or _local_changes(store, state.get("wl_ts", 0)):
        mirrored = mirror_watchlist(store, trakt)
        # po vlastním zápisu na Trakt se `watchlisted_at` posune — přečíst znovu,
        # jinak by příští kolo zrcadlilo zbytečně
        wl = _watchlist(trakt.last_activities())
        state = dict(store.reload(STATE, {}) or {}, watchlist=wl)
        store.save(STATE, state)
    if act == state.get("activities"):
        return mirrored
    start = state.get("history_at") or _iso(now - FIRST_DAYS * 86400)
    events = []
    for page in range(1, MAX_PAGES + 1):
        chunk = trakt.history(start, page=page)
        events += chunk
        if len(chunk) < 100:
            break
    playback = trakt.playback()
    # od nejstaršího, ať novější zhlédnutí téhož titulu vyhraje
    events.sort(key=lambda e: _epoch(e.get("watched_at")))
    with store.updating("watched", {}) as data:
        n = apply_history(data, events, now) + apply_playback(data, playback, now)
        if n:
            store._trim(data, WATCHED_MAX)
    newest = max([_epoch(e.get("watched_at")) for e in events] or [0])
    store.save(STATE, dict(state, activities=act, history_at=_iso(newest + 1) if newest else start))
    return n + mirrored
