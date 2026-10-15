"""Přesměrování bez přenosu přihlašovacích hlaviček na cizí host.

`urllib.request.HTTPRedirectHandler` v CPythonu při 30x vezme hlavičky původního
požadavku (`req.headers`) a pošle je i na nový cíl — včetně `Authorization` (Basic
auth vlastního úložiště) a `Cookie` (FastShare). WebDAV za reverzní proxy nebo CDN,
které přesměruje jinam, tak dostalo heslo uživatele (audit 2026-09-19).

`StripCredsRedirect` hlavičky s tajemstvím při přesměrování na jiný host odstraní;
na tentýž host (typicky jen `/` na konci nebo http→https) je nechá — WebDAV to
u složek dělá běžně a bez hlavičky by přišlo 401.
"""
import urllib.parse
import urllib.request

CITLIVE = ("Authorization", "Cookie", "Proxy-Authorization")


def _host(url):
    return (urllib.parse.urlsplit(url).hostname or "").lower()


class StripCredsRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        metoda = req.get_method()
        if metoda not in ("GET", "HEAD", "POST") and code in (301, 302, 307, 308):
            # WebDAV PROPFIND: CPython přesměrování jiné metody odmítne (HTTPError 301) a úložiště
            # za http→https nebo bez `/` na konci by nešlo vůbec – metoda i tělo se zachovají
            novy = urllib.request.Request(newurl.replace(" ", "%20"), data=req.data, method=metoda,
                                          headers=dict(req.headers),   # bez Host a spol. (unredirected)
                                          origin_req_host=req.origin_req_host, unverifiable=True)
        else:
            novy = super().redirect_request(req, fp, code, msg, headers, newurl)
        if novy is not None and _host(newurl) != _host(req.full_url):
            for jmeno in CITLIVE:
                novy.remove_header(jmeno)
                novy.remove_header(jmeno.lower())
        return novy


OPENER = urllib.request.build_opener(StripCredsRedirect)
