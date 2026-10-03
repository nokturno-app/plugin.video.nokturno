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
- Týž dotaz `last_activities` řekne i o změně Watchlistu (`watchlisted_at`):
  `on_watchlist` pak spustí kontrolu Hlídaných hned, ne až v denním kole.
"""
import calendar
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


def pull(store, trakt, now=None, on_watchlist=None):
    """Jedno kolo. Vrací počet přijatých záznamů; výjimky Traktu (`TraktError`) letí ven.

    `on_watchlist()` se zavolá, když se od minulého kola změnil Watchlist (první
    kolo jen zapamatuje stav — denní kontrola Hlídaných ho stejně projde)."""
    now = int(now or time.time())
    state = store.reload(STATE, {}) or {}
    raw = trakt.last_activities()
    act, wl = _activities(raw), _watchlist(raw)
    if wl != state.get("watchlist"):
        known = "watchlist" in state
        state = dict(state, watchlist=wl)
        store.save(STATE, state)
        if known and on_watchlist:
            on_watchlist()
    if act == state.get("activities"):
        return 0
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
    store.save(STATE, {"activities": act, "watchlist": wl,
                       "history_at": _iso(newest + 1) if newest else start})
    return n
