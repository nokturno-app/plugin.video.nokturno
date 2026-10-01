"""Trvalý index „Filmy ve vysoké kvalitě": které filmy z poolu mají stream odpovídající definici.

Čistý Python bez xbmc – menu čte jen tenhle index, ověřování běží po dávkách na pozadí.
Tvar: {"sig": str, "items": {id: {"ok": bool|None, "ts": int, "genres": [str], "rank": int, "meta": dict}}}
"""
RECHECK_AFTER = 3 * 86400
RETRY_AFTER = 1800          # po selhání zkusit znovu za 30 min (`record`)


def signature(min_q, surround, audio, subs=""):
    return f"{min_q}:{int(bool(surround))}:{audio or ''}:{subs or ''}"


def merge_pool(index, pool_metas, now):
    """Nová id přidá (`ok=None`), id mimo pool odebere; pořadí poolu = `rank`. Jiná definice = prázdný index."""
    items = index.setdefault("items", {})
    ids = []
    for rank, meta in enumerate(pool_metas):
        mid = meta.get("id")
        if not mid or mid in ids:
            continue
        ids.append(mid)
        entry = items.get(mid)
        if entry is None:
            entry = items[mid] = {"ok": None, "ts": 0}
        entry["rank"] = rank
        entry["genres"] = list(meta.get("genres") or [])
        entry["meta"] = meta
    for mid in [m for m in items if m not in ids]:
        del items[mid]
    return index


def next_batch(index, now, size=8, recheck_after=RECHECK_AFTER):
    """Splatné titulky (`now - ts >= recheck_after`; nový má ts 0 a je splatný vždy): nejdřív neověřené podle pořadí, pak nejstarší."""
    due = [(m, e) for m, e in (index.get("items") or {}).items() if not e.get("ts") or now - e["ts"] >= recheck_after]
    due.sort(key=lambda me: (me[1].get("ok") is not None, me[1].get("rank", 0) if me[1].get("ok") is None
                             else me[1].get("ts") or 0))
    return [m for m, _e in due][:size]


def record(index, mid, result, now, recheck_after=RECHECK_AFTER):
    entry = (index.get("items") or {}).get(mid)
    if entry is None:
        return
    if result is None:   # selhání: `ok` zůstává, další pokus za 30 min
        entry["ts"] = now - recheck_after + RETRY_AFTER
    else:
        entry["ok"] = bool(result)
        entry["ts"] = now


def visible(index, genre=None):
    entries = [e for e in (index.get("items") or {}).values()
               if e.get("ok") is True and (not genre or genre in (e.get("genres") or []))]
    return [e["meta"] for e in sorted(entries, key=lambda e: e.get("rank", 0))]


def genres_available(index):
    return sorted({g for e in (index.get("items") or {}).values() if e.get("ok") is True
                   for g in e.get("genres") or []})
