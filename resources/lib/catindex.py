"""Index ověřených titulů – „Filmy ve vysoké kvalitě“ v Kodi i vlastní katalogy (`mycat.py`).

Čistý Python bez sítě – menu čte jen tenhle index, ověřování běží po dávkách na pozadí.
Tvar: {"sig": str, "items": {id: {"ok": bool|None, "ts": int, "found": int, "genres": [str],
"rank": int, "meta": dict}}, "foreign_ts": int}
`found` = kdy titul poprvé vyhověl; `foreign_ts` = kdy sem naposledy přišly výsledky z jiného zařízení.
"""
import time

RECHECK_AFTER = 3 * 86400   # titul, který nevyhověl: za kolik se zkusí znovu
RECHECK_FOUND = 7 * 86400   # titul, který vyhověl: jak často se ověřuje, jestli nález platí
RETRY_AFTER = 1800          # po selhání zkusit znovu za 30 min (`record`)
NO_RANK = 10 ** 6           # položka, která přišla jen synchronizací a v místním poolu ještě není


def signature(min_q, surround, audio, subs=""):
    return f"{min_q}:{int(bool(surround))}:{audio or ''}:{subs or ''}"


def merge_pool(index, pool_metas, now):
    """Nová id přidá (`ok=None`), id mimo pool odebere; pořadí poolu = `rank`. Výsledky (`ok`, `ts`, `found`)
    existujících položek zůstávají; položka z synchronizace bez `meta` ji tu dostane."""
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


def _period(entry, recheck_after, recheck_found):
    return recheck_found if entry.get("ok") is True else recheck_after


def next_batch(index, now, size=8, recheck_after=RECHECK_AFTER, recheck_found=RECHECK_FOUND):
    """Splatné tituly (nový má ts 0 a je splatný vždy; vyhovující se kontroluje po `recheck_found`, ostatní po
    `recheck_after`): nejdřív neověřené podle pořadí, pak nejstarší."""
    due = [(m, e) for m, e in (index.get("items") or {}).items()
           if not e.get("ts") or now - e["ts"] >= _period(e, recheck_after, recheck_found)]
    due.sort(key=lambda me: (me[1].get("ok") is not None, me[1].get("rank", 0) if me[1].get("ok") is None
                             else me[1].get("ts") or 0))
    return [m for m, _e in due][:size]


def record(index, mid, result, now, recheck_after=RECHECK_AFTER, recheck_found=RECHECK_FOUND):
    entry = (index.get("items") or {}).get(mid)
    if entry is None:
        return
    if result is None:   # selhání: `ok` zůstává, další pokus za 30 min (podle periody položky)
        entry["ts"] = now - _period(entry, recheck_after, recheck_found) + RETRY_AFTER
    else:
        if result and entry.get("ok") is not True:
            entry["found"] = now
        entry["ok"] = bool(result)
        entry["ts"] = now


def visible(index, genre=None, sort="pool"):
    """Metadata vyhovujících titulů (jen s `meta`). `sort`: „pool“ = pořadí výběru, „found“ = nově nalezené
    první, „released“ = nejnovější vydání první."""
    entries = [e for e in (index.get("items") or {}).values()
               if e.get("ok") is True and e.get("meta") and (not genre or genre in (e.get("genres") or []))]
    entries.sort(key=lambda e: e.get("rank", 0))
    if sort == "found":
        entries.sort(key=lambda e: -(e.get("found") or 0))
    elif sort == "released":
        entries.sort(key=lambda e: (e["meta"].get("released") or str(e["meta"].get("year") or "")), reverse=True)
    return [e["meta"] for e in entries]


def genres_available(index):
    return sorted({g for e in (index.get("items") or {}).values() if e.get("ok") is True
                   for g in e.get("genres") or []})


def counts(index):
    """(ověřeno, vyhovuje, celkem)."""
    items = (index.get("items") or {}).values()
    return (sum(1 for e in items if e.get("ok") is not None), sum(1 for e in items if e.get("ok") is True),
            len(items))


def compact(index):
    """Výsledky k synchronizaci: {id: [1, ts, found]} vyhovující, {id: [0, ts]} ostatní (jen ověřené)."""
    out = {}
    for mid, e in (index.get("items") or {}).items():
        if e.get("ok") is True:
            out[mid] = [1, int(e.get("ts") or 0), int(e.get("found") or 0)]
        elif e.get("ok") is False:
            out[mid] = [0, int(e.get("ts") or 0)]
    return out


def merge_results(index, res):
    """Slije výsledky z `compact` jiného zařízení: u každého titulu vyhrává novější `ts`. Vrací počet převzatých."""
    items = index.setdefault("items", {})
    taken = 0
    for mid, rec in (res or {}).items():
        if not isinstance(rec, (list, tuple)) or len(rec) < 2:
            continue
        try:
            ok, ts = bool(rec[0]), int(rec[1])
            found = int(rec[2]) if len(rec) > 2 else 0
        except (TypeError, ValueError):
            continue
        entry = items.get(mid)
        if entry is None:
            entry = items[mid] = {"ok": None, "ts": 0, "rank": NO_RANK, "genres": []}
        if ts <= int(entry.get("ts") or 0):
            continue
        entry["ok"], entry["ts"] = ok, ts
        if ok:
            entry["found"] = found or ts
        taken += 1
    if taken:
        index["foreign_ts"] = int(time.time())
    return taken
