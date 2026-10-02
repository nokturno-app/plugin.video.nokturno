"""Filtr „je tohle soubor s koncertem daného interpreta?“ pro vlastní katalogy koncertů (`concertcat.py`).

Čistý Python bez sítě. Port filtru z dashboardu (`Dashboard/backend/concerts.py`), ale **přísnější**:
dashboard má za sebou ruční schvalování, klient ne. Změřeno na vzorku 4 552 schválených a 7 981
zamítnutých souborů: filtr dashboardu propustí 99 % schválených, ale přesnost je jen 37 % (zamítá se
hlavně film, jiný interpret, dokument, anime a klip). S povinným koncertním klíčovým slovem
(`has_keyword`) zachytí 70 % koncertů při přesnosti 98 %, proto tady platí tahle varianta.

Do jádra patří jen obecné vzory (regexy, délka, velikost) – žádné názvy ani odkazy na soubory.
"""
import functools
import re
import unicodedata

MIN_SIZE = 250 * 1024 * 1024      # pod tím je to klip nebo audio
MIN_DURATION = 20 * 60            # HellSpy délku posílá, ostatní ne
MAX_NAME = 200

# Pravá hranice slova schválně chybí: názvy bývají slité („liveatxyz2025“, „galakoncert“, „Záznam
# koncertu“) a `\b` je míjela. Levá hranice zůstává, jinak by „live“ chytlo „Oliver“. Čtyři výjimky
# vznikly z chyb měřených na ostrých datech: `tour` chytal „Tournament“, `turne` příjmení „Turner“,
# `gala` release groupu „…Galaxy…“ a `live` slovo „Lives“.
_KEYWORD = re.compile(r"\b(live(?!s\b)|koncert|concert|unplugged|tour(?!nament|ist)|turne(?!r)|zivak|zive|"
                      r"festival|wembley|woodstock|glastonbury|rock am ring|wacken|"
                      r"session|gala(?!xy)|open ?air|arena|stadium|mtv|"
                      # názvy slavných alb a festivalů bez koncertního slova
                      r"made in japan|rock in rio|montreux|roskilde|hellfest|tomorrowland|graspop|sziget)")
# tvary, které koncert nikdy nenese — díly seriálů, dabované filmy, klipy, dokumenty
_BLACKLIST = re.compile(r"\b(s\d{1,2}e\d{1,3}|\d{1,2}x\d{2,3}|epizoda|episode|\d+\s*dil|"
                        r"cz dab|sk dab|dabing|dubbing|titulky|(?:cz|sk) ?(?:tit|sub)\w*|trailer|ukazka|teaser|videoklip|"
                        r"official video|lyrics|karaoke|dokument|documentary|the movie|film|tribute|revival)\b")
# Jen u názvů bez klíčového slova: dabing/`Esub` nese film, koncert s klíčovým slovem ho občas taky.
_FILM_TAG = re.compile(r"\b((?:cz|sk) ?dab\w*|esub)\b")
# Číslo dílu na konci názvu: „Název - 01 [720p]“, „Název_05“, „Název+13“.
# Dvě číslice, ne `op 46` (opus). S klíčovým slovem to bývá rok („Tour 90“, „Live 80“).
_EPISODE_END = re.compile(r"(?<!\bop)(?:\s-\s?|[_+]|\s)\d{2}$", re.I)
_MEDIA_EXT = re.compile(r"\.(mkv|mp4|avi|m4v|mov|wmv|ts|webm|mpg|mpeg|vob|m2ts)$", re.I)
_TRAIL_TAGS = re.compile(r"(\s*(\[[^\]]*\]|\([^)]*\)|\b(?:480p|720p|1080p|2160p|cz|sk|en|hd|fhd)\b))+\s*$", re.I)
_YEAR = re.compile(r"\b(19[5-9]\d|20[0-4]\d)\b")
# co z názvu souboru nepatří do názvu koncertu: kvalita, kodek, kontejner, zdroj záznamu, uploader
_TAG = re.compile(r"^(\d{3,4}p|[248]k|hd|fullhd|fhd|uhd|sd|hdr|x26[45]|h26[45]|hevc|avc|xvid|divx|aac|ac3|dts|"
                  r"flac|mp3|mkv|mp4|avi|ts|m2ts|iso|bluray|blu ray|bdrip|brrip|dvdrip|dvd[59]?|dvdr|webrip|"
                  r"web dl|webdl|hdtv|tvrip|dvbt|dvb|t2|satrip|vhsrip|remux|multicam|proper|repack|by\w+|"
                  r"cz|sk|en|eng|cze|svk|multi|full|show|remastered|remaster|hq|lq|fps|\d+fps|kbit|\d+kbit|"
                  r"mpeg2|pcm|truehd|atmos|dd|dd5|\d ?1ch|ch|www|com|net|org|rip|"
                  r"mbluray|liquid|xi|audio|video|tit|novinka|mypage|nejlepsi|filmy|hudebni|klipy|bit|"
                  r"\d+|[a-z]*\d+[a-z]*)$")   # holá čísla (datum, „5 1“) a slepence písmen s číslicemi (DEV0, BDRip1080p)
_GROUP_TAG = re.compile(r"^\s*\[[^\]]{1,40}\]\s*")
_SEP = re.compile(r"[\s._\-/\\\[\]()\{\},;:!?'\"+*#~|©…]+")
# slova, která v klíči koncertu nic nerozlišují — „Koncert 1980“ a „in concert 1980“ je totéž
_STOP = frozenset("live koncert koncertni koncerty concert in at the a an and of on from zive zivak show".split())



# Koncertní slova, která dashboard nemá (jeho filtr klíčové slovo nevyžaduje): symfonické a akustické
# záznamy, pořady stanic a slavné sály. Bez nich by přísný filtr míjel třeba „Metallica with the San
# Francisco Symphony“ nebo „Helloween – United Alive“.
_EXTRA = re.compile(r"\b(alive|symphon|orchestr|filharmon|philharmon|lucern|full set|bbc|whistle test|rockpalast|"
                    r"later with|fillmore|royal albert|madison square|hammersmith|budokan|red rocks|o2 arena|"
                    r"acoustic|akustick|vystoupeni|recital)")


def normalize_title(text):
    """Název bez diakritiky, velikosti písmen a interpunkce – na porovnávání, ne na zobrazení."""
    if not text:
        return ""
    t = unicodedata.normalize("NFKD", text)
    t = "".join(c for c in t if not unicodedata.combining(c))
    return " ".join(re.sub(r"[^a-z0-9]+", " ", t.lower()).split())


def _key(text):
    """Jen písmena a číslice bez diakritiky — na porovnání interpreta s názvem souboru."""
    return re.sub(r"[^a-z0-9]", "", normalize_title(text))


def _tokens(name):
    """Slova názvu v původní velikosti písmen, bez oddělovačů a diakritiky (na klíč)."""
    return [t for t in _SEP.split(name) if t]


def is_concert(name, artist, size=0, duration=0, rivals=()):
    """Je soubor koncert interpreta `artist`? Interpret musí být v názvu jako celé slovo (fulltext
    zdrojů chytá slovo kdekoli), název nesmí nést nic z blacklistu, soubor nesmí být krátký a název
    musí nést koncertní klíčové slovo (`has_keyword`) – to je rozdíl proti dashboardu, viz docstring modulu.

    `rivals` jsou delší jména z poolu, která tímhle jménem začínají („Jméno Příjmení“ pro „Jméno“);
    bez nich by koncerty toho delšího spadly pod to kratší (viz `rivals`)."""
    # Známá délka rozhoduje sama: jinak projde každý klip přes 250 MB. Bez délky (WebShare ji
    # v API nemá) zůstává velikost jedinou zálohou.
    if duration:
        if duration < MIN_DURATION:
            return False
    elif size < MIN_SIZE:
        return False
    # Hranatá závorka na začátku je podpis release groupy, ne interpret.
    text = normalize_title(_GROUP_TAG.sub("", name))
    aname = normalize_title(artist)
    # celá slova, ne podřetězec: „Queen“ nesmí chytit „drag queens“ ani „Steve McQueen“
    if not aname or f" {aname} " not in f" {text} ":
        return False
    if any(_rival_re(r).search(text) for r in rivals):
        return False
    if _BLACKLIST.search(text):
        return False
    if not _KEYWORD.search(text) and (_FILM_TAG.search(text) or _episode_number(name)):
        return False
    return has_keyword(name)


def _episode_number(name):
    """Končí název číslem dílu? Nejdřív pryč přípona a koncové značky („[720p]“, „(2019)“, „CZ“).
    DVD tituly („…_Title_01_01“) jsou kapitoly koncertu, ne díly."""
    stem, prev = _MEDIA_EXT.sub("", name.strip()), None
    while prev != stem:
        prev, stem = stem, _TRAIL_TAGS.sub("", stem).rstrip(" .-_")
    return "title" not in stem.lower() and bool(_EPISODE_END.search(stem))


def has_keyword(name):
    """Nese název koncertní slovo (`_KEYWORD` nebo `_EXTRA`)? Tady je povinné, viz docstring modulu."""
    text = normalize_title(_GROUP_TAG.sub("", name))
    return bool(_KEYWORD.search(text) or _EXTRA.search(text))


@functools.lru_cache(maxsize=256)
def _rival_re(rival):
    """Rival i ve skloňovaném tvaru: „Lucie Bílá“ musí chytit „Vánoční galakoncert Lucie Bílé“
    (nalezeno na produkci 2026-09-23, tři soubory, které shoda na celé slovo minula). Česká
    deklinace mění konec slova, proto se u slov delších než tři znaky poslední písmeno pouští
    a zbytek smí pokračovat — kratší slova („The Who“) se berou celá, tam už by zbyl pahýl."""
    casti = [re.escape(w[:-1]) + r"\w*" if len(w) > 3 else re.escape(w) for w in rival.split()]
    return re.compile(r"\b" + r"\s+".join(casti))


def concert_title(name, artist):
    """Název koncertu a rok z názvu souboru: pryč jméno interpreta (jen ze začátku), rok,
    tagy kvality a přípona. Co zbude, je název; prázdný = „Live“.

    ponytail: dedup podle takto očištěného názvu — „Místo 86“ a „Live at Místo
    Stadium 1986“ jsou dva koncerty. Sloučit přes MusicBrainz nebo AI, až to bude bolet."""
    toks = _tokens(name)
    artist_toks = [normalize_title(t) for t in _tokens(artist)]
    # interpret bývá na začátku; smaž ho tam, i když je psaný jinak (jiný zápis oddělovačů)
    while toks and artist_toks and normalize_title(toks[0]) == artist_toks[0]:
        toks.pop(0)
        artist_toks.pop(0)
    if artist_toks and toks:
        # nesedělo po slovech (jméno slité do jednoho slova) — zkus první token jako celé jméno
        if _key(toks[0]) == _key(artist):
            toks.pop(0)
    year = None
    kept = []
    for t in toks:
        low = normalize_title(t)
        if year is None and _YEAR.fullmatch(low):
            year = int(low)
            continue
        if low == "by":
            break   # „by uploader“ — uploader a všechno za ním
        if _TAG.fullmatch(low) or not low:
            continue
        kept.append(t)
    title = " ".join(kept).strip()
    if not title:
        title = "Live"
    return title[:MAX_NAME], year


def concert_key(title):
    """Klíč na slučování koncertů jednoho interpreta: bez stopslov a bez roku (ten se
    přidává v `group`)."""
    return " ".join(t for t in normalize_title(title).split() if t not in _STOP)


def rivals(artist, names):
    """Jména z `names`, která začínají jménem `artist` a jsou delší – normalizovaná, k použití v `is_concert`."""
    aname = normalize_title(artist)
    if not aname:
        return ()
    found = (normalize_title(n) for n in names)
    return tuple(sorted({n for n in found if n.startswith(aname + " ")}))
