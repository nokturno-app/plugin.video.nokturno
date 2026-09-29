"""Počítadla pro plné hlášení statistik: úspěšnost hledání a přehrání, doba načtení.

Plugin v Kodi je pokaždé nový proces, statistiky ale posílá služba. Počítadla
proto leží v profilu (`usage.json`) a zapisují se pod zámkem (`Store.updating`),
takže je může plnit plugin a vybírat služba.

Posílají se **jen počty a kódy** — žádná adresa ani id souboru. Jedinou výjimkou
je hledání bez výsledku (`miss()`): id titulu a jeho název, ať je vidět, co lidé
hledají a nenajdou (totéž id chodí u zhlédnutých titulů).
`take()` počítadla vybere a vynuluje; když se hlášení nepovede, `restore()` je
vrátí, aby se nic neztratilo ani nezapočítalo dvakrát.
"""
import re
import time

STORE = "usage"
MAX_TIMES = 200          # kolik posledních dob načtení se drží do dalšího hlášení
KEY_RE = re.compile(r"^[a-z_]{1,24}(:[a-z0-9_]{1,24})?$")
MISS_ID_RE = re.compile(r"^(tt\d{5,10}|tmdb:\d{1,9})(:\d{1,3}:\d{1,4})?$")
MAX_MISS = 50            # kolik různých titulů bez výsledku se drží do dalšího hlášení


def count(store, name, n=1):
    """Přičte `n` k počítadlu `name` (`search`, `play_ok:ws` …)."""
    if not KEY_RE.match(name or ""):
        return
    try:
        with store.updating(STORE, {}) as data:
            cnt = data.setdefault("cnt", {})
            cnt[name] = int(cnt.get(name, 0)) + n
    except (OSError, ValueError, TypeError):
        pass


def miss(store, item_id, title=""):
    """Hledání streamů bez výsledku: `item_id` (`tt…`, díl `tt…:S:E`) a název titulu."""
    item_id = str(item_id or "")
    if not MISS_ID_RE.match(item_id):
        return
    try:
        with store.updating(STORE, {}) as data:
            misses = data.setdefault("miss", {})
            if item_id not in misses and len(misses) >= MAX_MISS:
                return
            row = misses.setdefault(item_id, {"n": 0, "t": ""})
            row["n"] = int(row.get("n", 0)) + 1
            row["t"] = (str(title or "") or row.get("t") or "")[:80]
    except (OSError, ValueError, TypeError, AttributeError):
        pass


def timing(store, seconds):
    """Zapíše dobu načtení streamů (sekundy)."""
    try:
        with store.updating(STORE, {}) as data:
            times = data.setdefault("load", [])
            times.append(round(float(seconds), 2))
            del times[:-MAX_TIMES]
    except (OSError, ValueError, TypeError):
        pass


def mark_feature(store, name):
    """Zapamatuje si, kdy se naposledy použila funkce (`syncwatch`, `transfer` …)."""
    try:
        with store.updating(STORE, {}) as data:
            data.setdefault("feat", {})[name] = int(time.time())
    except (OSError, ValueError, TypeError):
        pass


def features(store, days=30):
    """Funkce použité za posledních `days` dní."""
    since = time.time() - days * 86400
    feat = (store.load(STORE, {}) or {}).get("feat") or {}
    return sorted(k for k, ts in feat.items() if (ts or 0) >= since)


def _pct(values, p):
    values = sorted(values)
    return values[min(len(values) - 1, int(p * len(values)))]


def take(store):
    """Vybere počítadla a doby načtení do hlášení a vynuluje je.

    Vrací `{"cnt": {...}, "load": {"n", "p50", "p95"}, "miss": {id: {"n", "t"}}}`
    (prázdné části vynechá).
    """
    out = {}
    try:
        with store.updating(STORE, {}) as data:
            cnt = data.pop("cnt", None) or {}
            times = data.pop("load", None) or []
            misses = data.pop("miss", None) or {}
    except (OSError, ValueError, TypeError):
        return out
    if cnt:
        out["cnt"] = cnt
    if misses:
        out["miss"] = misses
    if times:
        out["load"] = {"n": len(times), "p50": _pct(times, 0.5), "p95": _pct(times, 0.95)}
    out["_times"] = times
    return out


def restore(store, taken):
    """Vrátí počítadla z `take()` zpět, když se hlášení neodeslalo."""
    if not taken:
        return
    try:
        with store.updating(STORE, {}) as data:
            cnt = data.setdefault("cnt", {})
            for k, v in (taken.get("cnt") or {}).items():
                cnt[k] = int(cnt.get(k, 0)) + int(v)
            times = data.setdefault("load", [])
            times[:0] = taken.get("_times") or []
            del times[:-MAX_TIMES]
            misses = data.setdefault("miss", {})
            for k, row in (taken.get("miss") or {}).items():
                if k not in misses and len(misses) >= MAX_MISS:
                    continue
                cur = misses.setdefault(k, {"n": 0, "t": row.get("t", "")})
                cur["n"] = int(cur.get("n", 0)) + int(row.get("n", 0))
    except (OSError, ValueError, TypeError):
        pass


def payload(taken):
    """Část `take()`, která jde na server (bez surových časů)."""
    return {k: v for k, v in (taken or {}).items() if not k.startswith("_")}
