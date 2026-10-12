#!/usr/bin/env python3
"""Estuary Nokturno – výchozí skin Kodi 21 (Omega) v barvách Nokturna.

Vezme zdroj Estuary (xbmc/xbmc, větev Omega, addons/skin.estuary, připnutý commit
ESTUARY_COMMIT) a postaví z něj samostatný skin `skin.estuary.nokturno`. Estuary
zůstává, jak je; mění se jen:

- barvy (paleta z loga Nokturna: noční modrá, zlatá),
- v seznamech vpravo vedle roku i hodnocení („★ 8.4   2026“),
- další sady písem k výběru v Nastavení → Rozhraní → Vzhled → Písma (Inter jako Arctic Fuse 3,
  Plus Jakarta Sans); stáhnou se z google/fonts a fontTools z nich udělá statický Regular a Bold
  (výchozí řez proměnných písem bývá nejtenčí). Outfit, Manrope a Figtree nemají ▲▼●○★, které
  Nokturno ve výpisech používá — v Kodi by z nich byly čtverečky,
- první položka hlavní nabídky „Nokturno“: nahoře dlaždice menu doplňku, pod nimi
  Pokračovat ve sledování, Nejsledovanější filmy a seriály a pak katalogy z dashboardu
  (Vánoce, Film pro dnešní den…) a vlastní katalogy vybrané tlačítkem v Nastavení vzhledu
  (`action=home_row&slot=N`, nadpis řady posílá doplněk ve Window(home).Property).
  Položka se schová, když doplněk není nainstalovaný, nebo vypínačem v Nastavení vzhledu.

    python3 skin/build.py                      # zip do repo/skin.estuary.nokturno/
    python3 skin/build.py --dir /tmp/skin      # jen rozbalený skin (test přes adb)
    python3 skin/build.py --src ~/xbmc/addons/skin.estuary   # bez stahování
"""
import argparse
import io
import urllib.request
import os
import re
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path

from PIL import Image, ImageChops, ImageDraw, ImageFilter

VERSION = "1.0.0"
ADDON_ID = "skin.estuary.nokturno"
ESTUARY_COMMIT = "f8815ee40f49a700c047982d752be4b2a61420e2"   # xbmc/xbmc Omega, Estuary 4.0.0
GOOGLE_FONTS = "https://raw.githubusercontent.com/google/fonts/bd8f81ddb5c74d5c8897b36ad88b440266245103/ofl/"
# sada písem (= název v nastavení Kodi) → soubor v google/fonts
FONTSETS = {
    "Inter": "inter/Inter%5Bopsz,wght%5D.ttf",
    "Plus Jakarta Sans": "plusjakartasans/PlusJakartaSans%5Bwght%5D.ttf",
}
HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
PLUGIN_MEDIA = ROOT / "resources" / "media"

# barvy z loga Nokturna: noční modrá (#2B3976 → #0D142D) a zlatá (#F5CB83 → #E4AE5A);
# klíče a průhlednosti jako v Estuary, mění se jen odstín
COLORS = {
    "primary_background": "FF1B2756",
    "secondary_background": "33E4AE5A",
    "dialog_tint": "FF141B38",
    "background": "FF000000",
    "bg_image": "FF909090",
    "bg_overlay": "30FFFFFF",
    "black": "FF000000",
    "white": "FFF0F0F0",
    "grey": "FFA0A0A0",
    "blue": "FFF5CB83",
    "red": "FFCE4421",
    "button_focus": "FF3D52B5",
    "button_alt_focus": "803D52B5",
    "text_shadow": "22000000",
    "border_alpha": "60FFFFFF",
    "disabled": "40FFFFFF",
    "selected": "FFF5CB83",
    "invalid": "FFFF0000",
}

# vlastní řetězce skinu (Estuary končí na 31611); ostatní popisky bere skin z doplňku přes $ADDON[…]
STRINGS = {
    31700: ("Nokturno menu", "Nabídka Nokturna", "Ponuka Nokturna", "Nokturno menü"),
    31701: ("- Nokturno rows on the home screen", "- Řady Nokturna na domovské obrazovce",
            "- Rady Nokturna na domovskej obrazovke", "- Nokturno sorok a kezdőképernyőn"),
}
LANGS = ("en_gb", "cs_cz", "sk_sk", "hu_hu")

P = "plugin://plugin.video.nokturno/"
A = lambda sid: f"$ADDON[plugin.video.nokturno {sid}]"   # řetězec z překladů doplňku
HOME_SLOTS = 8   # katalogy z dashboardu + vlastní v hlavním menu; prázdný slot se schová


def url(**q):
    return P + "?" + "&amp;".join(f"{k}={v}" for k, v in q.items())


TREND = lambda t: url(action="catalog", type=t, catalog="nejsledovanejsi", src="trend")
# řady pod položkou Nokturno: (typ, nadpis, adresa); "cat" = dlaždice složek, "poster" = plakáty
ROWS = [
    ("cat", "$LOCALIZE[31700]", P),
    ("poster", A(30063), url(action="continue")),
    ("poster", f"{A(30012)} – {A(30393)}", TREND("movie")),
    ("poster", f"{A(30013)} – {A(30393)}", TREND("series")),
] + [("poster", f"$INFO[Window(home).Property(nokturno.home.{n})]",
       url(action="home_row", slot=n) + "&amp;rev=$INFO[Window(home).Property(nokturno.home.rev)]")
     for n in range(1, HOME_SLOTS + 1)]   # rev: po výběru katalogů v nastavení se adresa změní a řady načtou znovu


# ---------------------------------------------------------------- zdroj Estuary

def fetch_estuary(dest):
    """Sparse checkout jen addons/skin.estuary na připnutém commitu (~25 MB)."""
    run = lambda *a: subprocess.run(a, cwd=dest, check=True, capture_output=True)
    run("git", "init", "-q")
    run("git", "remote", "add", "origin", "https://github.com/xbmc/xbmc.git")
    run("git", "sparse-checkout", "set", "addons/skin.estuary")
    run("git", "fetch", "-q", "--depth", "1", "--filter=blob:none", "origin", ESTUARY_COMMIT)
    run("git", "checkout", "-q", "FETCH_HEAD")
    return Path(dest) / "addons" / "skin.estuary"


def sidemenu_icon(path):
    """Bílá ikona Nokturna na průhledném pozadí jako ostatní ikony hlavní nabídky Estuary
    (95×95, LA): zlaté tahy loga → alfa, modré pozadí pryč."""
    r, _, b = Image.open(PLUGIN_MEDIA / "icon2.png").convert("RGB").split()
    # plná alfa (logo má zlatý přechod, v bílé by kruh po obvodu slábl); kruh o chlup silnější,
    # N zůstává, jinak by se zaslepily otvory filmového pásu
    a = ImageChops.subtract(r, b).point(lambda v: max(0, min(255, (v - 30) * 8)))
    x0, y0, x1, y1 = a.getbbox()
    cx, cy, rad = (x0 + x1) / 2, (y0 + y1) / 2, (x1 - x0) / 2
    ring = Image.new("L", a.size, 0)
    ImageDraw.Draw(ring).ellipse((cx - rad * 1.2, cy - rad * 1.2, cx + rad * 1.2, cy + rad * 1.2), fill=255)
    ImageDraw.Draw(ring).ellipse((cx - rad * 0.8, cy - rad * 0.8, cx + rad * 0.8, cy + rad * 0.8), fill=0)
    a = Image.composite(a.filter(ImageFilter.MaxFilter(7)), a, ring)
    a = a.crop(a.getbbox())
    w = 50
    h = round(a.height * w / a.width)
    a = a.resize((w * 4, h * 4), Image.LANCZOS).resize((w, h), Image.LANCZOS)
    layer = Image.new("LA", (w, h), (255, 0))
    layer.putalpha(a)
    out = Image.new("LA", (95, 95), (255, 0))
    out.paste(layer, ((95 - w) // 2, (95 - h) // 2 + 1), layer)
    out.save(path)


def round_badge(path):
    """Kulatý odznak hodnocení místo hvězdicové rozety Estuary (barví ho `colordiffuse`)."""
    big = Image.new("LA", (176, 176), (255, 0))
    ImageDraw.Draw(big).ellipse((8, 8, 167, 167), fill=(255, 255))
    big.resize((44, 44), Image.LANCZOS).save(path)


def fontsets(out):
    """Statické řezy Regular/Bold do fonts/ a nové <fontset> do Font.xml jako kopie sady Default:
    NotoSans → <písmo>-Regular, tučné fonty rovnou na Bold (bez umělého ztučnění)."""
    from fontTools.ttLib import TTFont
    from fontTools.varLib import instancer
    f = out / "xml/Font.xml"
    xml = f.read_text(encoding="utf-8")
    default = re.search(r'<fontset id="Default"[^>]*>(.*?)</fontset>', xml, re.S).group(1)
    sets = ""
    for name, path in FONTSETS.items():
        stem = name.replace(" ", "")
        data = urllib.request.urlopen(GOOGLE_FONTS + path, timeout=60).read()
        family = path.split("/")[0]
        lic = urllib.request.urlopen(GOOGLE_FONTS + family + "/OFL.txt", timeout=60).read()
        (out / "fonts" / f"{stem}-OFL.txt").write_bytes(lic)
        for weight, suffix in ((400, "Regular"), (700, "Bold")):
            font = TTFont(io.BytesIO(data))
            pins = {a.axisTag: a.defaultValue for a in font["fvar"].axes}
            pins["wght"] = weight
            instancer.instantiateVariableFont(font, pins, inplace=True)
            font.save(out / "fonts" / f"{stem}-{suffix}.ttf")

        def swap(m):
            block = m.group(0).replace("NotoSans-Regular.ttf", f"{stem}-Regular.ttf")
            if "<style>bold</style>" in block:
                block = block.replace(f"{stem}-Regular.ttf", f"{stem}-Bold.ttf").replace("\t\t\t<style>bold</style>\n", "")
            return block
        sets += f'\t<fontset id="{name}">' + re.sub(r"<font>.*?</font>", swap, default, flags=re.S) + "</fontset>\n"
    f.write_text(xml.replace("</fonts>", sets + "</fonts>"), encoding="utf-8")


# ---------------------------------------------------------------- XML

def sub1(text, old, new):
    """Náhrada, která musí trefit — po změně Estuary build spadne místo tichého skinu bez úpravy."""
    if old not in text:
        raise SystemExit(f"v Estuary chybí očekávaný úsek: {old[:80]!r}")
    return text.replace(old, new, 1)


def addon_xml(src):
    ax = (src / "addon.xml").read_text(encoding="utf-8")
    head = ax[:ax.index('<extension point="xbmc.addon.metadata">')]
    head = sub1(head, '<addon id="skin.estuary" version="4.0.0" name="Estuary" provider-name="phil65, Ichabod Fletchman">',
                f'<addon id="{ADDON_ID}" version="{VERSION}" name="Estuary Nokturno" '
                'provider-name="Nokturno (z Estuary od phil65, Ichabod Fletchman)">')
    return head + """<extension point="xbmc.addon.metadata">
		<platform>all</platform>
		<license>CC BY-SA 4.0, GNU GENERAL PUBLIC LICENSE Version 2.0</license>
		<source>https://github.com/nokturno-app/plugin.video.nokturno</source>
		<assets>
			<icon>resources/icon.png</icon>
			<fanart>resources/fanart.jpg</fanart>
			<screenshot>resources/screenshot-01.jpg</screenshot>
		</assets>
		<summary lang="en_GB">Kodi's default skin Estuary in Nokturno colours, with Nokturno first in the main menu.</summary>
		<summary lang="cs_CZ">Výchozí vzhled Kodi Estuary v barvách Nokturna, s Nokturnem na prvním místě hlavní nabídky.</summary>
		<summary lang="sk_SK">Predvolený vzhľad Kodi Estuary vo farbách Nokturna, s Nokturnom na prvom mieste hlavnej ponuky.</summary>
		<summary lang="hu_HU">A Kodi alapértelmezett Estuary felülete Nokturno színekben, a főmenü első helyén a Nokturnóval.</summary>
		<description lang="en_GB">Estuary as you know it, in Nokturno colours. The Nokturno item at the top of the main menu shows the Nokturno menu as tiles, Continue watching, the most watched movies and TV shows of the week and the catalogs from Nokturno (seasonal ones and your own catalogs picked in Skin settings).</description>
		<description lang="cs_CZ">Estuary, jak ho znáš, jen v barvách Nokturna. Položka Nokturno nahoře v hlavní nabídce ukáže menu Nokturna jako dlaždice, Pokračovat ve sledování, nejsledovanější filmy a seriály týdne a katalogy z Nokturna (sezónní i tvoje vlastní katalogy vybrané v Nastavení vzhledu).</description>
		<description lang="sk_SK">Estuary, ako ho poznáš, len vo farbách Nokturna. Položka Nokturno hore v hlavnej ponuke ukáže menu Nokturna ako dlaždice, Pokračovať v sledovaní, najsledovanejšie filmy a seriály týždňa a katalógy z Nokturna (sezónne aj tvoje vlastné katalógy vybrané v Nastaveniach vzhľadu).</description>
		<description lang="hu_HU">Az ismert Estuary, csak Nokturno színekben. A főmenü tetején lévő Nokturno elem csempékként mutatja a Nokturno menüt, a Megtekintés folytatását, a hét legnézettebb filmjeit és sorozatait, valamint a Nokturno katalógusait (szezonális és a Felület beállításaiban kiválasztott saját katalógusok).</description>
		<news>1.0.0 – první vydání: barvy Nokturna, Nokturno na prvním místě hlavní nabídky.</news>
	</extension>
</addon>
"""


def strings(out):
    for i, lang in enumerate(LANGS):
        f = out / "language" / f"resource.language.{lang}" / "strings.po"
        t = f.read_text(encoding="utf-8").rstrip("\n") + "\n"
        for sid, texts in STRINGS.items():
            msgstr = "" if lang == "en_gb" else texts[i]
            t += f'\nmsgctxt "#{sid}"\nmsgid "{texts[0]}"\nmsgstr "{msgstr}"\n'
        f.write_text(t, encoding="utf-8")


INFO_HEIGHT = 474   # pruh s popisem nad řadami (jako Arctic Fuse 3)


def info_panel(c, visible):
    """Název (nebo logo), rok · stopáž · hodnocení a děj titulu z kontejneru `c` nad řadami."""
    return f"""
					<control type="group">
						<left>60</left>
						<top>120</top>
						<visible>{visible}</visible>
						<animation effect="fade" time="200">VisibleChange</animation>
						<control type="image">
							<width>640</width>
							<height>140</height>
							<aspectratio align="left" aligny="bottom">keep</aspectratio>
							<texture background="true">$INFO[{c}.ListItem.Art(clearlogo)]</texture>
						</control>
						<control type="label">
							<top>60</top>
							<width>1300</width>
							<height>80</height>
							<aligny>bottom</aligny>
							<font>font52_title</font>
							<label>$INFO[{c}.ListItem.Title]</label>
							<shadowcolor>text_shadow</shadowcolor>
							<visible>String.IsEmpty({c}.ListItem.Art(clearlogo))</visible>
						</control>
						<control type="label">
							<top>155</top>
							<width>1300</width>
							<height>40</height>
							<font>font27</font>
							<textcolor>grey</textcolor>
							<label>$INFO[{c}.ListItem.Year]$INFO[{c}.ListItem.Duration(mins),  ·  , min]$INFO[{c}.ListItem.Rating,  ·  [COLOR selected]★[/COLOR] ]</label>
						</control>
						<control type="textbox">
							<top>200</top>
							<width>1300</width>
							<height>110</height>
							<font>font27</font>
							<shadowcolor>text_shadow</shadowcolor>
							<label>$INFO[{c}.ListItem.Plot]</label>
						</control>
					</control>"""


def nokturno_menu(home):
    """Položka Nokturno na prvním místě hlavní nabídky a její řady."""
    item = f"""<content>
						<item>
							<label>Nokturno</label>
							<onclick>ActivateWindow(Videos,{P},return)</onclick>
							<property name="menu_id">$NUMBER[30000]</property>
							<thumb>icons/sidemenu/nokturno.png</thumb>
							<property name="id">nokturno</property>
							<visible>System.HasAddon(plugin.video.nokturno) + !Skin.HasSetting(HomeMenuNoNokturnoButton)</visible>
						</item>"""
    home = sub1(home, "<content>\n\t\t\t\t\t\t<item>\n\t\t\t\t\t\t\t<label>$LOCALIZE[342]</label>", item +
                "\n\t\t\t\t\t\t<item>\n\t\t\t\t\t\t\t<label>$LOCALIZE[342]</label>")
    widgets = "".join(f"""
						<include content="{'WidgetListCategories' if kind == 'cat' else 'WidgetListPoster'}">
							<param name="content_path" value="{path}"/>
							<param name="widget_header" value="{header}"/>
							<param name="widget_target" value="videos"/>
							<param name="list_id" value="{30100 + i * 100}"/>
						</include>""" for i, (kind, header, path) in enumerate(ROWS))
    group = f"""<control type="group" id="30000">
					<visible>String.IsEqual(Container(9000).ListItem.Property(id),nokturno)</visible>
					<include content="Visible_Right_Delayed">
						<param name="id" value="nokturno"/>
					</include>{info_panel("Container", "ControlGroup(30001).HasFocus + !String.IsEmpty(Container.ListItem.Plot)")}{
                        info_panel("Container(30200)", "![ControlGroup(30001).HasFocus + !String.IsEmpty(Container.ListItem.Plot)]"
                                                       " + !String.IsEmpty(Container(30200).ListItem.Plot)")}
					<control type="group">
						<top>{INFO_HEIGHT}</top>
						<height>{1080 - INFO_HEIGHT}</height>
						<control type="grouplist" id="30001">
							<include>WidgetGroupListCommon</include>
							<pagecontrol>30010</pagecontrol>{widgets}
						</control>
					</control>
					<include content="WidgetScrollbar" condition="Skin.HasSetting(touchmode)">
						<param name="scrollbar_id" value="30010"/>
					</include>
				</control>
				<control type="group" id="5000">"""
    return sub1(home, '<control type="group" id="5000">', group)


def build(src, out):
    if out.exists():
        shutil.rmtree(out)
    shutil.copytree(src, out, ignore=shutil.ignore_patterns("screenshot-*", "changelog.txt"))

    (out / "addon.xml").write_text(addon_xml(src), encoding="utf-8")
    shutil.copy(PLUGIN_MEDIA / "icon2.png", out / "resources/icon.png")
    shutil.copy(PLUGIN_MEDIA / "fanart.jpg", out / "resources/fanart.jpg")
    for shot in sorted(HERE.glob("screenshot-*.jpg")):   # snímek z Office do náhledu v repozitáři
        shutil.copy(shot, out / "resources" / shot.name)
    strings(out)

    # barvy (jen klíče, které Estuary zná — jiný klíč by skin ignoroval)
    known = re.findall(r'<color name="([^"]+)"', (src / "colors/defaults.xml").read_text(encoding="utf-8"))
    (out / "colors/defaults.xml").write_text(
        '<?xml version="1.0" encoding="UTF-8"?>\n<colors>\n'
        + "".join(f'\t<color name="{k}">{COLORS[k]}</color>\n' for k in known if k in COLORS)
        + "</colors>\n", encoding="utf-8")

    # Nastavení vzhledu → Položky hlavní nabídky: Nokturno jako ostatní položky
    f = out / "xml/SkinSettings.xml"
    f.write_text(sub1(f.read_text(encoding="utf-8"), '<control type="radiobutton" id="611">', """<control type="radiobutton" id="6091">
					<label>Nokturno</label>
					<include>DefaultSettingButton</include>
					<selected>!Skin.HasSetting(HomeMenuNoNokturnoButton)</selected>
					<onclick>Skin.ToggleSetting(HomeMenuNoNokturnoButton)</onclick>
					<visible>System.HasAddon(plugin.video.nokturno)</visible>
				</control>
				<control type="button" id="6092">
					<label>$LOCALIZE[31701]</label>
					<include>DefaultSettingButton</include>
					<onclick>RunPlugin(plugin://plugin.video.nokturno/?action=home_pick)</onclick>
					<enable>!Skin.HasSetting(HomeMenuNoNokturnoButton)</enable>
					<visible>System.HasAddon(plugin.video.nokturno)</visible>
				</control>
				<control type="radiobutton" id="611">"""), encoding="utf-8")

    sidemenu_icon(out / "media/icons/sidemenu/nokturno.png")
    round_badge(out / "media/overlays/badge.png")

    fontsets(out)
    # oddělovač v záhlaví (Řadit podle ∙ 2 / 50): U+2219 Inter nemá, „·“ mají všechna písma
    for f in (out / "xml").glob("*.xml"):
        t = f.read_text(encoding="utf-8")
        if "∙" in t:
            f.write_text(t.replace("∙", "·"), encoding="utf-8")
    # vpravo v seznamu: hodnocení a rok (Estuary tam má jen rok nebo Label2)
    f = out / "xml/Variables.xml"
    f.write_text(sub1(f.read_text(encoding="utf-8"), '<value condition="Container.SortMethod(7) | Container.SortMethod(29)">$INFO[ListItem.Year]</value>',
                      '\t\t<value condition="!String.IsEmpty(ListItem.Rating) + !String.IsEmpty(ListItem.Year)">★ $INFO[ListItem.Rating]   $INFO[ListItem.Year]</value>\n'
                      '\t\t<value condition="Container.SortMethod(7) | Container.SortMethod(29)">$INFO[ListItem.Year]</value>'), encoding="utf-8")

    home = out / "xml/Home.xml"
    home.write_text(nokturno_menu(home.read_text(encoding="utf-8")), encoding="utf-8")


def zip_skin(skin_dir, repo):
    out_dir = repo / ADDON_ID
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{ADDON_ID}-{VERSION}.zip"
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in sorted(skin_dir.rglob("*")):
            if p.is_file():
                zf.write(p, f"{ADDON_ID}/{p.relative_to(skin_dir)}")
    # ikona, fanart a snímek na cestě z addon.xml — Kodi je při náhledu v repozitáři bere z datadiru
    shutil.rmtree(out_dir / "resources", ignore_errors=True)
    shutil.copytree(skin_dir / "resources", out_dir / "resources")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", type=Path, help="rozbalené addons/skin.estuary (jinak se stáhne)")
    ap.add_argument("--dir", type=Path, help="jen postavit skin do složky, bez zipu")
    ap.add_argument("--beta", action="store_true", help="zip do repo-beta/ (skin potřebuje betu doplňku)")
    a = ap.parse_args()
    with tempfile.TemporaryDirectory() as tmp:
        src = a.src or fetch_estuary(tmp)
        if a.dir:
            build(src, a.dir)
            print("hotovo:", a.dir)
            return
        skin_dir = Path(tmp) / ADDON_ID
        build(src, skin_dir)
        print("hotovo:", os.path.relpath(zip_skin(skin_dir, ROOT / ("repo-beta" if a.beta else "repo")), ROOT))


if __name__ == "__main__":
    main()
