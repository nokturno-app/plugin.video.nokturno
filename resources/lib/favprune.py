"""Zhlédnuté pryč z Mého seznamu.

- Film (i jednotlivý díl nebo soubor v seznamu) zmizí, jakmile je zhlédnutý **po** přidání
  (`watched[k].ts` > `favlog[k].ts`). Titul přidaný znovu po zhlédnutí zůstane, dokud ho
  uživatel nedokouká znovu.
- Seriál zmizí, až jsou zhlédnuté všechny dosud vydané díly (`watch.aired_episodes`: bez
  speciálů, díl bez data = nevydaný) a aspoň jeden z nich po přidání. Nový díl ho zpátky
  nepřidá — tady se nic nepřidává.
- Bez metadat (síť, chyba zdroje, prázdný seznam dílů) seriál zůstává. Konečné rozhodnutí se
  ověří nad metadaty ne staršími než `FRESH`, ať stará cache neschová nově vydaný díl.
- Položka bez záznamu v deníku `favlog` (doplňky před synchronizací) zůstává.

Odebrání jde přes `Store.remove_favourite` → deník `favlog`; synchronizace (`sync.py`) i Trakt
Watchlist (`trakt_pull.mirror_watchlist`) ho přenesou samy.
ponytail: klíče dílů se hledají jako `<id seriálu>:S:E` nebo `id` z metadat; seriál pod jiným
id než jeho díly (např. `tmdb:` vs `tt…`) se neodebere nikdy.
"""
import datetime
import logging

from foryou import split_episode_id
from watch import META_KEYS, _fresher, aired_episodes

_LOGGER = logging.getLogger(__name__)

FRESH = 24 * 3600   # s – stáří metadat pro konečné rozhodnutí u seriálu


def _ts(rec):
    try:
        return int((rec or {}).get("ts") or 0)
    except (TypeError, ValueError, AttributeError):
        return 0


def _seen_after(rec, added):
    return isinstance(rec, dict) and bool(rec.get("playcount")) and _ts(rec) > added


def added_at(store, key):
    """Kdy přišel `key` do Mého seznamu; 0 = není v deníku jako zapnutý."""
    rec = (store.load("favlog", {}) or {}).get(key)
    return _ts(rec) if isinstance(rec, dict) and rec.get("on") else 0


def started_since_added(store, sid):
    """Má seriál od přidání do Mého seznamu zhlédnutý díl? (bez sítě)"""
    added, prefix = added_at(store, sid), sid + ":"
    return any(k.startswith(prefix) and _seen_after(v, added)
               for k, v in (store.load("watched", {}) or {}).items())


def _all_seen(watched, sid, episodes, today):
    aired = aired_episodes(episodes or [], today)
    return bool(aired) and all(
        (watched.get(e.get("id")) or watched.get("%s:%s:%s" % (sid, e["season"], e["episode"])) or {}).get("playcount")
        for e in aired)


def series_done(store, engine, sid, today=None):
    """Zhlédnuté všechny vydané díly? Nejdřív z cache, potvrzení nad čerstvými metadaty."""
    today = today or datetime.date.today().isoformat()
    watched = store.load("watched", {}) or {}
    try:
        if not _all_seen(watched, sid, engine.episodes(sid), today):
            return False
        with _fresher(engine, FRESH, META_KEYS):
            return _all_seen(watched, sid, engine.episodes(sid), today)
    except Exception as err:  # noqa: BLE001 – bez metadat seriál zůstává
        _LOGGER.debug("Můj seznam %s: %s", sid, err)
        return False


def prune(store, engine=None, only=None, today=None):
    """Odebere z Mého seznamu, co je od přidání dokoukané. Vrací odebrané klíče.

    `engine`: cokoli s `episodes(sid)` (Engine); bez něj se seriály přeskočí.
    `only`: omezit na tyto klíče; klíč dílu `sid:S:E` znamená i jeho seriál."""
    favs = store.favourites()
    if only is not None:
        keys = set()
        for k in only:
            keys.add(str(k))
            keys.add(split_episode_id(k)[0])
        favs = [k for k in favs if k in keys]
    watched = store.load("watched", {}) or {}
    removed = []
    for key in favs:
        added = added_at(store, key)
        if not added:
            continue
        if split_episode_id(key)[1] is None and (store.item(key) or {}).get("type") == "series":
            if engine is None or not started_since_added(store, key) \
                    or not series_done(store, engine, key, today):
                continue
        elif not _seen_after(watched.get(key), added):
            continue
        if store.remove_favourite(key):
            removed.append(key)
    return removed
