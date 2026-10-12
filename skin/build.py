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

VERSION = "1.0.1"
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
    ("cat", "$LOCALIZE[31700]", P + "?home=1"),
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


BUSY_OLD = """				<control type="image">
					<centerleft>50%</centerleft>
					<centertop>50%</centertop>
					<width>80</width>
					<height>80</height>
					<aspectratio>keep</aspectratio>
					<animation effect="rotate" end="-45" center="auto" time="200" delay="600" loop="true" reversible="false" condition="true">Conditional</animation>
					<texture colordiffuse="button_focus">spinner.png</texture>
				</control>
				<control type="image">
					<centerleft>50%</centerleft>
					<centertop>50%</centertop>
					<width>50</width>
					<height>50</height>
					<aspectratio>keep</aspectratio>
					<animation effect="rotate" end="45" center="auto" time="200" delay="600" loop="true" reversible="false" condition="true">Conditional</animation>
					<texture flipx="true" colordiffuse="button_focus">spinner.png</texture>
				</control>"""
BUSY_NEW = """				<control type="image">
					<centerleft>50%</centerleft>
					<centertop>50%</centertop>
					<width>122</width>
					<height>122</height>
					<aspectratio>keep</aspectratio>
					<animation effect="fade" start="100" end="55" time="900" tween="sine" easing="inout" pulse="true" condition="true">Conditional</animation>
					<animation effect="zoom" start="100" end="90" center="auto" time="900" tween="sine" easing="inout" pulse="true" condition="true">Conditional</animation>
					<texture>icons/sidemenu/nokturno.png</texture>
				</control>
				<control type="image">
					<centerleft>50%</centerleft>
					<centertop>50%</centertop>
					<width>128</width>
					<height>128</height>
					<animation effect="rotate" start="0" end="-360" center="auto" time="1100" loop="true" reversible="false" condition="true">Conditional</animation>
					<texture colordiffuse="selected">dialogs/extendedprogress/loading.png</texture>
				</control>"""


LIST_BUSY_OLD = """				<control type="image">
					<top>$PARAM[posy]</top>
					<width>100</width>
					<height>100</height>
					<aspectratio aligny="center">keep</aspectratio>
					<texture colordiffuse="black">dialogs/extendedprogress/loading-back.png</texture>
				</control>
				<control type="image">
					<top>$PARAM[posy]</top>
					<width>100</width>
					<height>100</height>
					<aspectratio aligny="center">keep</aspectratio>
					<texture>dialogs/extendedprogress/loading.png</texture>
					<animation effect="rotate" center="auto" start="360" end="0" time="1500" loop="true" condition="true">Conditional</animation>
				</control>"""
LIST_BUSY_NEW = """				<control type="group">
					<top>$PARAM[posy]</top>
					<width>100</width>
					<height>100</height>
					<control type="image">   <!-- ikona má kolem loga průhledný okraj -->
						<left>3</left>
						<top>3</top>
						<width>94</width>
						<height>94</height>
						<aspectratio>keep</aspectratio>
						<animation effect="fade" start="100" end="55" time="900" tween="sine" easing="inout" pulse="true" condition="true">Conditional</animation>
						<animation effect="zoom" start="100" end="90" center="auto" time="900" tween="sine" easing="inout" pulse="true" condition="true">Conditional</animation>
						<texture>icons/sidemenu/nokturno.png</texture>
					</control>
					<control type="image">
						<width>100</width>
						<height>100</height>
						<animation effect="rotate" start="0" end="-360" center="auto" time="1100" loop="true" reversible="false" condition="true">Conditional</animation>
						<texture colordiffuse="selected">dialogs/extendedprogress/loading.png</texture>
					</control>
				</control>"""


PROGRESS_XML = """<?xml version="1.0" encoding="UTF-8"?>
<!-- Estuary Nokturno: karta načítání vpravo nahoře (hledání streamů a jiné dlouhé akce) -->
<window>
	<zorder>3</zorder>
	<animation effect="fade" start="0" end="100" time="300">WindowOpen</animation>
	<animation effect="fade" start="100" end="0" time="200">WindowClose</animation>
	<controls>
		<control type="group">
			<depth>DepthBars</depth>
			<right>30</right>
			<top>24</top>   <!-- hodiny se mezitím schovají (Includes.xml) -->
			<width>620</width>
			<height>100</height>
			<animation effect="slide" end="0,-130" time="300" tween="sine" easing="inout" condition="$EXP[infodialog_active]">conditional</animation>
			<animation effect="slide" end="0,-130" time="150" condition="Window.IsVisible(FullscreenVideo) | Window.IsVisible(FullscreenGame)">conditional</animation>
			<control type="image">
				<width>620</width>
				<height>100</height>
				<texture colordiffuse="B3141B38">colors/white.png</texture>
			</control>
			<control type="image">
				<left>22</left>
				<top>22</top>
				<width>56</width>
				<height>56</height>
				<aspectratio>keep</aspectratio>
				<animation effect="fade" start="100" end="55" time="900" tween="sine" easing="inout" pulse="true" condition="true">Conditional</animation>
				<texture>icons/sidemenu/nokturno.png</texture>
			</control>
			<control type="image">
				<left>18</left>
				<top>18</top>
				<width>64</width>
				<height>64</height>
				<animation effect="rotate" start="0" end="-360" center="auto" time="1100" loop="true" reversible="false" condition="true">Conditional</animation>
				<texture colordiffuse="selected">dialogs/extendedprogress/loading.png</texture>
			</control>
			<control type="label" id="31">
				<left>100</left>
				<top>14</top>
				<width>490</width>
				<height>34</height>
				<font>font12</font>
				<textcolor>grey</textcolor>
			</control>
			<control type="label" id="30">
				<left>100</left>
				<top>44</top>
				<width>490</width>
				<height>34</height>
				<font>font12</font>
				<scroll>true</scroll>
			</control>
			<control type="progress" id="32">
				<left>100</left>
				<top>80</top>
				<width>490</width>
				<height>4</height>
				<texturebg colordiffuse="33FFFFFF">colors/white.png</texturebg>
				<midtexture colordiffuse="selected">colors/white.png</midtexture>
				<visible>Integer.IsGreater(Control.GetLabel(32),0)</visible>
			</control>
		</control>
	</controls>
</window>
"""


NOTIFICATION_XML = """<?xml version="1.0" encoding="UTF-8"?>
<!-- Estuary Nokturno: hláška vpravo nahoře ve stejné kartě jako načítání (hranatá jako zbytek Estuary) -->
<window>
	<zorder>3</zorder>
	<animation effect="slide" start="660,0" end="0,0" time="300" tween="cubic" easing="out">WindowOpen</animation>
	<animation effect="fade" start="0" end="100" time="300">WindowOpen</animation>
	<animation effect="slide" start="0,0" end="660,0" time="250" tween="cubic" easing="in">WindowClose</animation>
	<animation effect="fade" start="100" end="0" time="250">WindowClose</animation>
	<controls>
		<control type="group">
			<depth>DepthMax</depth>
			<right>30</right>
			<top>24</top>   <!-- hodiny se mezitím schovají (Includes.xml) -->
			<width>620</width>
			<height>100</height>
			<visible>String.IsEmpty(Window(Home).Property(script.cinemavision.running))</visible>
			<animation effect="slide" end="0,116" time="200" condition="Window.IsVisible(extendedprogressdialog)">Conditional</animation>
			<control type="image">
				<width>620</width>
				<height>100</height>
				<texture colordiffuse="B3141B38">colors/white.png</texture>
			</control>
			<control type="image" id="400">
				<left>22</left>
				<top>22</top>
				<width>56</width>
				<height>56</height>
				<aspectratio>keep</aspectratio>
			</control>
			<control type="fadelabel" id="401">
				<left>100</left>
				<top>14</top>
				<width>490</width>
				<height>34</height>
				<font>font12</font>
				<textcolor>selected</textcolor>
				<scrollout>false</scrollout>
				<pauseatend>2000</pauseatend>
			</control>
			<control type="fadelabel" id="402">
				<left>100</left>
				<top>46</top>
				<width>490</width>
				<height>40</height>
				<font>font12</font>
				<scrollout>false</scrollout>
				<pauseatend>2000</pauseatend>
			</control>
		</control>
	</controls>
</window>
"""


MDI = "https://cdn.jsdelivr.net/npm/@mdi/svg@7.4.47/"   # Material Design Icons (Apache 2.0)
# obrázek Estuary v media/ → ikona MDI (outline); nahrazují se jen ikony, které jsou v Nokturnu vidět
MDI_ICONS = {
 "icons/power.png": "power", "icons/settings.png": "cog-outline", "icons/search.png": "magnify",
 "icons/sidemenu/addons.png": "puzzle-outline", "icons/sidemenu/android.png": "android",
 "icons/sidemenu/disc.png": "disc", "icons/sidemenu/download.png": "download-outline",
 "icons/sidemenu/favourites.png": "star-outline", "icons/sidemenu/games.png": "gamepad-variant-outline",
 "icons/sidemenu/livetv.png": "television-classic", "icons/sidemenu/manage.png": "cog-outline",
 "icons/sidemenu/movies.png": "movie-open-outline", "icons/sidemenu/music.png": "music-note-outline",
 "icons/sidemenu/musicvideos.png": "music-box-outline", "icons/sidemenu/pictures.png": "image-outline",
 "icons/sidemenu/programs.png": "apps", "icons/sidemenu/radio.png": "radio",
 "icons/sidemenu/tv.png": "television", "icons/sidemenu/videos.png": "filmstrip",
 "icons/sidemenu/weather.png": "weather-partly-cloudy",
 "DefaultMovies.png": "movie-open-outline", "DefaultVideoPlaylists.png": "playlist-play",
 "DefaultAddonProgram.png": "cog-outline", "DefaultTVShows.png": "television-classic",
 "DefaultAddonsSearch.png": "magnify", "DefaultYear.png": "calendar-outline",
 "DefaultFavourites.png": "star-outline", "DefaultAddonsUpdates.png": "update",
 "DefaultRecentlyAddedMovies.png": "movie-open-star-outline", "DefaultGenre.png": "drama-masks",
 "DefaultNetwork.png": "web", "DefaultUser.png": "account-multiple-outline", "DefaultFolder.png": "folder-outline",
 "DefaultMusicArtists.png": "account-music-outline", "DefaultIconError.png": "alert-circle-outline",
 "DefaultHardDisk.png": "harddisk", "DefaultActor.png": "account-outline", "DefaultVideo.png": "video-outline",
 "DefaultStudios.png": "domain", "DefaultSets.png": "layers-outline",
 "DefaultMusicGenres.png": "music-box-multiple-outline", "DefaultInProgressShows.png": "play-circle-outline",
 "DefaultCountry.png": "earth", "DefaultAddSource.png": "plus-circle-outline",
 "DefaultAddonVideo.png": "video-outline", "DefaultAddonService.png": "cog-play-outline",
 "DefaultAddonPVRClient.png": "television-guide", "DefaultVideoDeleted.png": "delete-outline",
 "DefaultRecentlyAddedMusicVideos.png": "music-box-outline", "DefaultRecentlyAddedEpisodes.png": "television-play",
 "DefaultMusicVideos.png": "music-box-outline", "DefaultMusicSongs.png": "music-note-outline",
 "DefaultMusicAlbums.png": "album", "DefaultIconWarning.png": "alert-outline",
 "DefaultIconInfo.png": "information-outline", "DefaultAddonsRecentlyUpdated.png": "update",
 "DefaultAddonRepository.png": "package-variant", "DefaultAddonMusic.png": "music-note-outline",
}


MDI_SCALE = {72: 1.0, 80: 0.92, 95: 0.52, 110: 0.78, 256: 0.6}   # velikost kresby podle velikosti obrázku (menu, hlášky, Default*)


def mdi_icons(out):
    """Ikony Estuary → bílé MDI outline, kresba stejně velká uprostřed (Estuary ji má v každé ikoně
    jinak velkou, ve výpisu pak třeba Pro Tebe vedle Náhodného filmu vypadala větší).
    Barví ji dál `colordiffuse` skinu."""
    import cairosvg
    svgs = {}
    for path, name in MDI_ICONS.items():
        if name not in svgs:
            svgs[name] = urllib.request.urlopen(f"{MDI}svg/{name}.svg", timeout=30).read().decode()
        f = out / "media" / path
        old = Image.open(f).convert("RGBA")
        w, h = old.size
        side = round(min(w, h) * MDI_SCALE.get(min(w, h), 0.6))
        svg = svgs[name].replace("<path ", '<path fill="#ffffff" ')
        icon = Image.open(io.BytesIO(cairosvg.svg2png(bytestring=svg.encode(), output_width=side * 4,
                                                      output_height=side * 4))).convert("RGBA")
        icon = icon.resize((side, side), Image.LANCZOS)
        new = Image.new("RGBA", old.size, (255, 255, 255, 0))
        new.paste(icon, ((w - side) // 2, (h - side) // 2), icon)
        new.save(f)
    (out / "media" / "MDI-LICENSE.txt").write_bytes(urllib.request.urlopen(MDI + "LICENSE", timeout=30).read())


def spinner_arc(path):
    """Načítání: zlatý oblouk 70° (barví ho `colordiffuse`, točí animace) kolem loga Nokturna."""
    n, size = 4, 128
    big = Image.new("LA", (size * n, size * n), (255, 0))
    ImageDraw.Draw(big).arc((4 * n, 4 * n, (size - 4) * n, (size - 4) * n), -90, -20, fill=(255, 255), width=5 * n)
    big.resize((size, size), Image.LANCZOS).save(path)


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


INFO_HEIGHT = 291   # pruh s popisem nad řadami (jako Arctic Fuse 3)
ROW_LEFT = 55       # nadpis řady jako v Estuary (55 / ROWS_ZOOM × 100), popis taky
INFO_LEFT = 55
ROWS_ZOOM = 100     # velikost řad v % — 100 = jako ostatní sekce Estuary (Doplňky…)
ROWS_HEIGHT = round((1080 - INFO_HEIGHT) * 100 / ROWS_ZOOM)   # oblast řad před zmenšením
# Grouplist posouvá jen tolik, aby byl vybraný prvek celý vidět — u spodního okraje by tak nad
# vybranou řadou visel useknutý kus předchozí a další by nebyla vidět. Každá řada je proto skupina
# s rezervou PEEK pod sebou a itemgap o PEEK menší (rozestup řad zůstává): vybraná řada se zarovná
# výš a v rezervě vykukuje další. Estuary: plakátová řada 644, kategorie 410, itemgap -160 —
# řady se překrývají a spodek předchozí (plakáty, popisky dlaždic) by vykukoval nad vybranou;
# rozestup je proto o ROW_SPACE větší.
ROW_SPACE = 45      # plakát i s odznakem končí ~40 pod horním okrajem další řady
HEAD_GAP = 15      # mezera nadpis řady → plakáty navíc oproti Estuary
POSTER_ROW = 644 + HEAD_GAP
PEEK = ROWS_HEIGHT - POSTER_ROW
# Dlaždice nabídky (první řada) nejsou v grouplistu, ale nahoře na místě popisu: popis je vidět, když
# je vybraný titul v řadě, jinak dlaždice. Jen se prolnou, řady se neposouvají.
DETAIL = "ControlGroup(30001).HasFocus"
ROW_HEIGHT = {"poster": POSTER_ROW + PEEK}


def info_panel(c, visible):
    """Název (nebo logo), rok · stopáž · hodnocení a děj titulu z kontejneru `c` nad řadami."""
    nologo = f"String.IsEmpty({c}.ListItem.Art(clearlogo))"
    plot = "".join(f"""
						<control type="textbox">
							<top>125</top>
							<width>1300</width>
							<height>{h}</height>
							<font>font27</font>
							<shadowcolor>text_shadow</shadowcolor>
							<label>$INFO[{c}.ListItem.Plot]</label>
							<visible>{v}</visible>
						</control>""" for h, v in ((115, "!" + nologo), (195, nologo)))   # 3 / 5 řádků
    return f"""
					<control type="group">
						<left>{INFO_LEFT}</left>
						<top>45</top>
						<visible>{visible}</visible>
						<animation effect="fade" time="200">VisibleChange</animation>
						<!-- bez loga je nadpis nižší: vše o 25 výš a děj o řádek delší -->
						<animation effect="slide" end="0,-25" time="0" condition="{nologo}">conditional</animation>
						<control type="image">
							<width>640</width>
							<height>80</height>
							<aspectratio align="left" aligny="bottom">keep</aspectratio>
							<texture background="true">$INFO[{c}.ListItem.Art(clearlogo)]</texture>
						</control>
						<control type="label">
							<top>0</top>
							<width>1300</width>
							<height>80</height>
							<aligny>bottom</aligny>
							<font>font52_title</font>
							<label>$INFO[{c}.ListItem.Title]</label>
							<shadowcolor>text_shadow</shadowcolor>
							<visible>{nologo}</visible>
						</control>
						<control type="label">
							<top>85</top>
							<width>1300</width>
							<height>40</height>
							<font>font27</font>
							<textcolor>grey</textcolor>
							<label>$INFO[{c}.ListItem.Year]$INFO[{c}.ListItem.Duration(mins),  ·  , min]$INFO[{c}.ListItem.Rating,  ·  [COLOR selected]★[/COLOR] ]</label>
						</control>{plot}
					</control>"""


def expand_widget(includes, name, params, extra):
    """Include řady z Estuary rozvinutý do textu s `extra` (navigace) přímo v seznamu.

    Řada je v obalu (skupina s rezervou PEEK) a šipku nahoru/dolů Kodi předá jen přímo
    seznamu — navigaci grouplistu obal nedostane. Include ji jako parametr nepřijme."""
    def expand(name, params):
        m = re.search(rf'<include name="{name}">(.*?)<definition>(.*?)</definition>', includes, re.S)
        values = dict(re.findall(r'<param name="(\w+)">([^<]*)</param>', m.group(1)))
        values.update(params)
        return re.sub(r"\$PARAM\[(\w+)\]", lambda p: values.get(p.group(1), ""), m.group(2))

    def label(m):
        """Nadpis řady rozvinutý taky, aby lícoval s levou hranou vybraného (zvětšeného) plakátu."""
        params = dict(re.findall(r'<param name="(\w+)">([^<]*)</param>', m.group(1)))
        params.update(re.findall(r'<param name="(\w+)" value="([^"]*)"/>', m.group(1)))
        return sub1(expand("CategoryLabel", params), "<left>55</left>",
                    f"<left>{ROW_LEFT}</left>\n\t\t\t\t<font>font13_rows</font>")

    body = re.sub(r'<include content="CategoryLabel">(.*?)</include>', label, expand(name, params), flags=re.S)
    # plakáty o HEAD_GAP dál od nadpisu, dlaždice o 10 (jako Kategorie v Doplňcích)
    top, gap = (115, HEAD_GAP) if name == "WidgetListPoster" else (110, 10)
    body = sub1(body, f"<left>0</left>\n\t\t\t\t<top>{top}</top>", f"<left>0</left>\n\t\t\t\t<top>{top + gap}</top>")
    return sub1(body, f'<control type="panel" id="{params["list_id"]}">',
                f'<control type="panel" id="{params["list_id"]}">{extra}')


def nokturno_menu(home, includes):
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
    ids = [30100 + i * 100 for i in range(len(ROWS))]
    shown = lambda lid: f"[Integer.IsGreater(Container({lid}).NumItems,0) | Container({lid}).IsUpdating]"

    def nav(tag, lids):
        """Šipka do nejbližší viditelné řady."""
        return "".join(f"""
							<{tag} condition="{' + '.join(['!' + shown(x) for x in lids[:k]] + [shown(lid)])}">SetFocus({lid})</{tag}>"""
                       for k, lid in enumerate(lids))

    def row(i):
        kind, header, path = ROWS[i]
        return expand_widget(includes, "WidgetListCategories" if kind == "cat" else "WidgetListPoster",
                             {"content_path": path, "widget_header": header, "widget_target": "videos",
                              "list_id": str(ids[i])},
                             nav("onup", ids[:i][::-1]) + nav("ondown", ids[i + 1:]))

    assert ROWS[0][0] == "cat" and all(kind == "poster" for kind, _, _ in ROWS[1:])
    tiles = f"""
					<control type="group">
						<top>{round(80 * (100 - ROWS_ZOOM) / 100)}</top>   <!-- nadpis dlaždic na výšce Estuary (80) -->
						<width>{round(1458 * 100 / ROWS_ZOOM)}</width>
						<animation effect="zoom" start="{ROWS_ZOOM}" end="{ROWS_ZOOM}" center="0,0" time="0" condition="true">Conditional</animation>
						<animation effect="fade" time="200">VisibleChange</animation>
						<visible allowhiddenfocus="true">!{DETAIL}</visible>{row(0)}
					</control>"""
    widgets = "".join(f"""
						<control type="group">
							<height>{ROW_HEIGHT["poster"]}</height>
							<visible>{shown(ids[i])}</visible>{row(i)}
						</control>""" for i in range(1, len(ROWS)))
    group = f"""<control type="group" id="30000">
					<visible>String.IsEqual(Container(9000).ListItem.Property(id),nokturno)</visible>
					<include content="Visible_Right_Delayed">
						<param name="id" value="nokturno"/>
					</include>{info_panel("Container", DETAIL)}{tiles}
					<control type="group">
						<top>{INFO_HEIGHT}</top>
						<width>{round(1458 * 100 / ROWS_ZOOM)}</width>
						<height>{ROWS_HEIGHT}</height>
						<animation effect="zoom" start="{ROWS_ZOOM}" end="{ROWS_ZOOM}" center="0,{INFO_HEIGHT}" time="0" condition="true">Conditional</animation>
						<control type="grouplist" id="30001">
							<itemgap>{-160 - PEEK + ROW_SPACE}</itemgap>
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
    mdi_icons(out)
    round_badge(out / "media/overlays/badge.png")
    spinner_arc(out / "media/dialogs/extendedprogress/loading.png")   # načítání řad (BusyListSpinner)
    (out / "xml/DialogExtendedProgressBar.xml").write_text(PROGRESS_XML, encoding="utf-8")
    (out / "xml/DialogNotification.xml").write_text(NOTIFICATION_XML, encoding="utf-8")
    # hodiny se schovají, dokud je vpravo nahoře karta hlášky nebo načítání
    hide = "<visible>!Window.IsVisible(extendedprogressdialog) + !Window.IsVisible(notification)</visible>"
    f = out / "xml/Includes.xml"
    f.write_text(sub1(f.read_text(encoding="utf-8"), "<font>font_clock</font>\n",
                      f"<font>font_clock</font>\n\t\t\t\t\t\t{hide}\n"
                      '\t\t\t\t\t\t<animation effect="fade" time="150">VisibleChange</animation>\n'), encoding="utf-8")
    f = out / "xml/Custom_1109_TopBarOverlay.xml"
    f.write_text(sub1(f.read_text(encoding="utf-8"), "<visible>!Window.IsVisible(extendedprogressdialog)</visible>", hide),
                 encoding="utf-8")
    # místní nabídka: Estuary ji na domovské obrazovce posouvá o 175 dolů — u řad pak přetéká.
    # Podmínka na položku menu nejde: v dialogu Container(9000) míří na dialog, ne na domovskou obrazovku.
    f = out / "xml/DialogContextMenu.xml"
    f.write_text(sub1(f.read_text(encoding="utf-8"),
                      '\t\t\t<animation effect="slide" end="0,175" time="0" condition="Window.IsActive(home)">conditional</animation>\n', ""),
                 encoding="utf-8")
    f = out / "xml/DialogBusy.xml"
    f.write_text(sub1(f.read_text(encoding="utf-8"), BUSY_OLD, BUSY_NEW), encoding="utf-8")
    # načítání řad na domovské obrazovce: totéž logo s obloukem
    f = out / "xml/Includes_Home.xml"
    f.write_text(sub1(f.read_text(encoding="utf-8"), LIST_BUSY_OLD, LIST_BUSY_NEW), encoding="utf-8")

    # nadpis řady zmenšené na ROWS_ZOOM: font13 (výchozí nadpis Estuary) zvětšený zpět na 100 %
    f = out / "xml/Font.xml"
    f.write_text(re.sub(r"\t\t<font>\n\t\t\t<name>font13</name>.*?</font>\n",
                        lambda m: m.group(0) + m.group(0).replace("font13", "font13_rows").replace(
                            re.search(r"<size>\d+</size>", m.group(0)).group(0),
                            f"<size>{round(int(re.search(r'<size>(\d+)', m.group(0)).group(1)) * 100 / ROWS_ZOOM)}</size>"),
                        f.read_text(encoding="utf-8"), flags=re.S), encoding="utf-8")
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
    # odznak hodnocení dole uvnitř plakátu — nahoře zasahoval do nadpisu řady, na hraně do další řady
    f = out / "xml/View_54_InfoWall.xml"
    t = sub1(f.read_text(encoding="utf-8"), "<left>135</left>\n\t\t\t\t<top>-8</top>",
             "<left>135</left>\n\t\t\t\t<top>339</top>")
    f.write_text(t, encoding="utf-8")

    home = out / "xml/Home.xml"
    home.write_text(nokturno_menu(home.read_text(encoding="utf-8"),
                                  (out / "xml/Includes_Home.xml").read_text(encoding="utf-8")), encoding="utf-8")


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
