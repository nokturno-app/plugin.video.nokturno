"""Rozklíčování odkazu s „záložním“ pokusem, když se první loudá.

Přehrání se dělá na rozklíčovaném odkazu zdroje (`Engine.resolve`: dotaz na API zdroje).
Když zdroj neodpovídá, čekalo se na jeho timeout (desítky vteřin) a teprve potom se zkusila
další verze. Tady se po `HEDGE_AFTER` vteřinách rozběhne souběžně i další verze téhož souboru
a bere se, co se rozklíčuje dřív. Souběžně se zkouší jen **stejné verze** (`group` prvních
odkazů, ve výběru streamů sloučené do jednoho řádku) — jiná kvalita ani jazyk se nikdy
nepustí místo vybraného, jen když vybraný selže (jako dřív, jeden po druhém).

Pokusy běží v `daemon` vláknech: co se nestihlo, doběhne na pozadí a jeho výsledek se zahodí,
ale nikdy nedrží ukončení procesu (Kodi při vypnutí čeká na každé vlákno, které není daemon).
"""
import queue
import threading

HEDGE_AFTER = 3.0   # s — běžné rozklíčování trvá 0,3–1,5 s, déle už něco váhá


def first_success(urls, resolve, group=1, after=HEDGE_AFTER, errors=Exception, on_fail=None):
    """`(odkaz, výsledek)` prvního, který se rozklíčuje; když nejde žádný, vyhodí chybu toho prvního.

    `resolve(odkaz)` vrací výsledek nebo vyhodí jednu z `errors` (jiná výjimka projde ven hned).
    `group`: kolik prvních odkazů jsou tytéž verze — souběh se rozbíhá jen mezi nimi, po `after`
    vteřinách od předchozího startu, a hned po selhání některého z nich nastoupí další z nich.
    Odkaz mimo skupinu (jiná verze) se zkusí, až když všechny rozběhnuté selhaly, vždy po jednom.
    `on_fail(pořadí, odkaz, chyba)` se volá z volajícího vlákna za každé selhání.
    Vyhozená chyba je chyba odkazu s nejnižším pořadím (vybraný stream), ne ta, která přišla první."""
    urls = list(urls)
    if not urls:
        raise ValueError("žádný odkaz")
    failed = {}   # pořadí → chyba
    if group <= 1 or len(urls) < 2:
        for i, url in enumerate(urls):
            try:
                return url, resolve(url)
            except errors as err:
                if on_fail:
                    on_fail(i, url, err)
                failed[i] = err
        raise failed[min(failed)]
    results = queue.Queue()
    started = running = 0

    def attempt(i):
        try:
            results.put((i, True, resolve(urls[i])))
        except BaseException as err:   # noqa: BLE001 – i neočekávaná výjimka musí dojít volajícímu
            results.put((i, False, err))

    def start():
        nonlocal started, running
        threading.Thread(target=attempt, args=(started,), daemon=True, name="nokturno-hedge").start()
        started += 1
        running += 1
    start()
    while running:
        hedge = started < min(group, len(urls))
        try:
            i, ok, value = results.get(timeout=after if hedge else None)
        except queue.Empty:
            start()   # první se loudá: souběžně i další verze téhož souboru
            continue
        running -= 1
        if ok:
            return urls[i], value
        if not isinstance(value, errors):
            raise value
        if on_fail:
            on_fail(i, urls[i], value)
        failed[i] = value
        if started < len(urls) and (started < group or not running):
            start()   # další verze téhož souboru hned, jiná až po selhání všech rozběhnutých
    raise failed[min(failed)]
