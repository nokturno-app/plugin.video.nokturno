"""Jak se doplněk aktualizuje — posílá se se statistikami (dashboard, Přehled →
Aktualizace doplňku), ať je vidět, kolik instalací novou verzi nikdy nedostane.

- `updates`: globální volba Kodi *Aktualizace doplňků* (`general.addonupdates`:
  0 = instalovat samo, 1 = jen upozornit, 2 = nekontrolovat), nebo `off`, když má
  uživatel automatické aktualizace vypnuté jen u Nokturna (`update_rules` v Kodi 20+,
  `blacklist` v Kodi 19);
- `origin`: odkud je doplněk nainstalovaný (`installed.origin` v `Addons*.db`) —
  `repo`, `beta`, `zip` (prázdný původ = instalace ze zipu, ta se neaktualizuje nikdy),
  jinak id cizího repozitáře.

Nic o uživateli, jen dvě krátké hodnoty. Databáze Kodi se jen čte (`query_only`),
stejně jako videodatabáze v `kodi_marks.py`.
"""
import json
import os
import re
import sqlite3

ADDON_ID = "plugin.video.nokturno"
_DB_RE = re.compile(r"^Addons(\d+)\.db$")
_SETTING = {0: "auto", 1: "notify", 2: "never"}
_ORIGINS = {"repository.nokturno": "repo", "repository.nokturno.beta": "beta", "": "zip"}


def find_db(db_dir):
    try:
        names = os.listdir(db_dir)
    except OSError:
        return None
    found = [(int(m.group(1)), name) for name in names for m in [_DB_RE.match(name)] if m]
    return os.path.join(db_dir, max(found)[1]) if found else None


def read_db(db_path, addon_id=ADDON_ID):
    """(původ, vypnuté aktualizace jen u doplňku). Původ None = nejde přečíst."""
    if not db_path or not os.path.isfile(db_path):
        return None, False
    try:
        conn = sqlite3.connect(db_path, timeout=2)
    except sqlite3.Error:
        return None, False
    try:
        conn.execute("PRAGMA query_only = ON")
        row = conn.execute("SELECT origin FROM installed WHERE addonID = ?", (addon_id,)).fetchone()
        origin = None if row is None else (row[0] or "")
        off = False
        for table in ("update_rules", "blacklist"):
            try:
                off = off or conn.execute(f"SELECT 1 FROM {table} WHERE addonID = ?", (addon_id,)).fetchone() is not None
            except sqlite3.Error:
                pass   # tabulka v téhle verzi Kodi není
        return origin, off
    except sqlite3.Error:
        return None, False
    finally:
        conn.close()


def global_setting(execute):
    """`execute` = `xbmc.executeJSONRPC`."""
    try:
        res = json.loads(execute(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "Settings.GetSettingValue",
                                             "params": {"setting": "general.addonupdates"}})))
        return _SETTING.get(int(res["result"]["value"]))
    except Exception:  # noqa: BLE001 – statistiky nesmí nic shodit
        return None


def info(execute, db_dir, addon_id=ADDON_ID):
    origin, off = read_db(find_db(db_dir), addon_id)
    out = {}
    updates = "off" if off else global_setting(execute)
    if updates:
        out["updates"] = updates
    if origin is not None:
        out["origin"] = _ORIGINS.get(origin, origin[:40])
    return out


OLD_REPO = "raw.githubusercontent.com/matata86/plugin.video.nokturno/"
NEW_REPO = "raw.githubusercontent.com/nokturno-app/plugin.video.nokturno/"
REPO_IDS = ("repository.nokturno", "repository.nokturno.beta")
_REPO_VERSION_RE = re.compile(r'(<addon\b[^>]*?\bversion=")[^"]*(")')


def heal_repos(addons_dir):
    """Přepojí repozitáře 1.0.x ze starého repa matata86 na nokturno-app.

    Staré repo nabízí jako poslední doplněk 9.0.0 a k tomu repozitář 1.1.0 s novými
    adresami. Doplněk se z něj aktualizoval, repozitář na části zařízení ne (2026-10-02:
    ~550 instalací stojí na 9.0.0 s repozitářem 1.0.3), takže se tu `addon.xml`
    přepíše na adresy a verzi 1.1.0. Vrací id přepsaných repozitářů."""
    healed = []
    for repo_id in REPO_IDS:
        path = os.path.join(addons_dir, repo_id, "addon.xml")
        try:
            with open(path, encoding="utf-8") as f:
                text = f.read()
            if OLD_REPO not in text:
                continue
            text = _REPO_VERSION_RE.sub(r"\g<1>1.1.0\2", text.replace(OLD_REPO, NEW_REPO), count=1)
            text = text.replace('provider-name="matata86"', 'provider-name="Nokturno"')
            with open(path + ".tmp", "w", encoding="utf-8") as f:
                f.write(text)
            os.replace(path + ".tmp", path)
            healed.append(repo_id)
        except OSError:
            continue
    return healed


def quality(addon, store):
    """K plnému hlášení: kódy stavu zdrojů, použité funkce, průvodce, skin a architektura.
    Jen kódy a názvy funkcí — nic, podle čeho by šlo poznat, co kdo sleduje."""
    import platform
    import xbmc
    import accounts as accounts_lib
    import usage
    out = {}
    try:
        acc = store.load(accounts_lib.STORE, {}) or {}
        out["acc"] = {k: str(v.get("code") or "")[:24] for k, v in acc.items()
                      if k in accounts_lib.SOURCES and isinstance(v, dict)}
        feat = set(usage.features(store))
        for name, files in (("watchlist", ("watchlist", "wantlist")), ("mylist", ("favourites",)),
                            ("downloads", ("downloads",))):
            if any(store.load(f, None) for f in files):
                feat.add(name)
        if addon.getSetting("sync_enabled") == "true":
            feat.add("sync")
        out["feat"] = sorted(feat)
        out["wiz"] = bool(store.load("wizard_done", False))
        out["skin"] = xbmc.getSkinDir()[:60]
        out["arch"] = platform.machine()[:20]
    except Exception:  # noqa: BLE001 – statistiky nesmí nic shodit
        pass
    return out
