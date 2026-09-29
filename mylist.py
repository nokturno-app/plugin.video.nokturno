"""Vlastní seznam: položka menu s podkategoriemi a položkami z JSON souboru na adrese,
kterou si uživatel zadá v nastavení. Položka nese vnitřní odkazy (`hs:`, `ws:`, `fs:`…)
a přehrává se přes zdroje a účty doplňku. Obsah seznamu je věc uživatele.

Tvar souboru:
    {"version": 1, "title": "…", "thumb": "https://…",
     "groups": [{"title": "…", "thumb": "…", "groups": […], "items": […]}],
     "items": [{"title": "…", "year": 1980, "plot": "…", "thumb": "https://…",
                "duration": 5400, "refs": ["hs:…", "ws:…"]}]}

Čistý Python bez `xbmc*`, ať jde otestovat bez Kodi.
"""
import hashlib
import json
import re
import urllib.request

MAX_BYTES = 8 * 1024 * 1024
MAX_DEPTH = 3          # skupiny pod kořenem
MAX_NODES = 20000      # skupiny i položky dohromady
MAX_REFS = 10
TTL = 3600             # seznam se mění zřídka, hodina stačí
STALE_TTL = 14 * 86400  # výpadek serveru: poslední známý stav
TIMEOUT = 15
REF_RE = re.compile(r"^[a-z][a-z0-9]{1,11}:\S{1,500}$")
URL_RE = re.compile(r"^https?://\S{1,500}$", re.I)


class MylistError(Exception):
    pass


def parse_headers(lines):
    """`Název: hodnota` z nastavení na slovník; prázdné a vadné řádky se přeskočí."""
    out = {}
    for line in lines:
        name, sep, value = (line or "").partition(":")
        name, value = name.strip(), value.strip()
        if sep and value and re.match(r"^[A-Za-z0-9-]{1,64}$", name):
            out[name] = value
    return out


def fetch(url, headers=None, timeout=TIMEOUT):
    if not URL_RE.match(url or ""):
        raise MylistError("adresa musí začínat http:// nebo https://")
    # Cloudflare odmítá výchozí User-Agent Pythonu (chyba 1010)
    req = urllib.request.Request(url, headers={"User-Agent": "Nokturno", "Accept": "application/json",
                                               **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read(MAX_BYTES + 1)
    except Exception as e:  # noqa: BLE001 – síť, HTTP, TLS: vše = seznam teď nejde načíst
        raise MylistError(str(e)) from e
    if len(raw) > MAX_BYTES:
        raise MylistError("soubor je větší než 8 MB")
    try:
        data = json.loads(raw.decode("utf-8-sig"))
    except ValueError as e:
        raise MylistError("soubor není platný JSON") from e
    return validate(data)


def _text(value, limit):
    return " ".join(str(value).split())[:limit] if isinstance(value, (str, int, float)) else ""


def _url(value):
    return value if isinstance(value, str) and URL_RE.match(value) else ""


def _int(value, low, high):
    return value if isinstance(value, int) and not isinstance(value, bool) and low <= value <= high else 0


def validate(data):
    """Pročistí strom: zahodí neznámé klíče, vadné odkazy a položky bez odkazu,
    zkrátí texty a utne hloubku i počet uzlů. Nic z něj se neprovádí, jen kreslí."""
    if not isinstance(data, dict):
        raise MylistError("soubor musí být objekt JSON")
    budget = [MAX_NODES]

    def item(raw):
        if not isinstance(raw, dict):
            return None
        refs = [r for r in raw.get("refs") or () if isinstance(r, str) and REF_RE.match(r)][:MAX_REFS]
        title = _text(raw.get("title"), 200)
        if not refs or not title:
            return None
        return {"title": title, "year": _int(raw.get("year"), 1800, 2200),
                "plot": str(raw.get("plot") or "")[:4000] if isinstance(raw.get("plot"), str) else "",
                "thumb": _url(raw.get("thumb")), "duration": _int(raw.get("duration"), 1, 100 * 3600),
                "refs": refs}

    def group(raw, depth):
        node = {"title": _text(raw.get("title"), 200), "thumb": _url(raw.get("thumb")),
                "groups": [], "items": []}
        if depth < MAX_DEPTH:
            for g in raw.get("groups") or ():
                if budget[0] <= 0:
                    break
                if isinstance(g, dict):
                    budget[0] -= 1
                    sub = group(g, depth + 1)
                    if sub["title"] and (sub["groups"] or sub["items"]):
                        node["groups"].append(sub)
        for i in raw.get("items") or ():
            if budget[0] <= 0:
                break
            it = item(i)
            if it:
                budget[0] -= 1
                node["items"].append(it)
        return node

    tree = group(data, 0)
    if not tree["groups"] and not tree["items"]:
        raise MylistError("seznam je prázdný")
    return tree


def node(tree, path):
    """Uzel podle cesty indexů skupin (`"0.3.1"`, prázdná = kořen), nebo None."""
    current = tree
    for part in [p for p in (path or "").split(".") if p]:
        try:
            current = current["groups"][int(part)]
        except (ValueError, IndexError, KeyError, TypeError):
            return None
    return current


def cache_key(url):
    return "nokturno:mylist:" + hashlib.md5((url or "").encode("utf-8")).hexdigest()


def load(store, url, headers=None):
    """Strom seznamu: z cache (hodina), jinak ze sítě, při výpadku poslední známý stav.
    Vrací (strom, chyba); strom je None jen když není ani starý stav."""
    key = cache_key(url)
    error = []

    def loader():
        try:
            return fetch(url, headers)
        except MylistError as e:
            error.append(str(e))
            return None

    tree = store.cached_if(key, TTL, loader, ok=lambda d: d is not None)
    if tree is None:
        tree = store.peek_cached(key, STALE_TTL)
    return tree, (error[0] if error else "")


def cached_title(store, url):
    """Název kořene z poslední úspěšně načtené verze — do menu bez dotazu na síť."""
    tree = store.peek_cached(cache_key(url), STALE_TTL)
    return tree.get("title", "") if isinstance(tree, dict) else ""
