"""Přerušení rozdělané práce na žádost hostitele.

Kodi při `Application.Quit` čeká, až doběhnou všechny skripty doplňků — a dlouhé
smyčky jádra (hledání napříč zdroji, čtení hlaviček, průchod úložiště, přepočet
katalogu s CZ dabingem po titulech) dřív běžely dál, dokud nedoběhly samy. Na TV
boxu to znamenalo vypínání trvající minuty (Office 2026-09-16: přes 2 minuty).

Jádro o Kodi nic neví (a nesmí — hlídá test), takže dostane jen zavolatelný
`should_stop()`; doplněk pro Kodi mu podstrčí `xbmc.Monitor().abortRequested`
(plus vlastní příznak zrušení dialogem), HA a Stremio nic — tam se nikdy
nepřeruší nic.

`Aborted` dědí z `BaseException`, ne z `Exception`, stejně jako `KeyboardInterrupt`:
jádro má na desítkách míst „výpadek jednoho zdroje nesmí shodit ostatní"
(`except Exception`), a přerušení má naopak projít úplně vším až ven k hostiteli,
který ví, jak skončit (Kodi: zavřít handle bez hlášky). Výsledek přerušené práce
se nikdy necachuje — výjimka projde i `Store.cached_if()` dřív, než zapíše.
"""
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait

STOP_POLL = 1.0   # s – jak často se při čekání na vlákna ptát should_stop()


class Aborted(BaseException):
    """Hostitel končí (nebo uživatel zrušil) — rozdělaná práce se zahazuje."""

    def __init__(self, message="Přerušeno na žádost hostitele."):
        super().__init__(message)


class BackgroundPool(ThreadPoolExecutor):
    """Fond pro práci, na kterou nikdo nečeká (`shutdown(wait=False)`): opozdilé zdroje,
    hlavičky na pozadí, obnova starého seznamu. `shutdown(wait=False)` nechá doběhnout
    i celou frontu úloh — a Kodi při vypnutí čeká na každé vlákno interpretu (Office
    2026-10-03: přes 6 minut). `cancel_background()` frontu zahodí; rozběhnuté síťové
    volání doběhne na vlastní timeout."""

    def submit(self, fn, *args, **kwargs):
        def run():
            # fronta se rozjíždí i dlouho po návratu hostitele (reuse invoker v Kodi) —
            # úloha, na kterou přišla řada až po žádosti o konec, se nespustí vůbec
            check(_HOST_STOP[0])
            return fn(*args, **kwargs)

        try:
            future = super().submit(run)
        except RuntimeError:
            # „can't start new thread" (Android, Kodi 10.7.4, náhodný titul): fond nemá ani
            # jedno vlákno, úloha by ve frontě visela navždy – udělá se hned, jen pomaleji
            future = Future()
            try:
                future.set_result(run())
            except BaseException as e:  # noqa: BLE001 – jako ve vlákně: chyba patří do future
                future.set_exception(e)
            return future
        with _LOCK:
            _PENDING.add(future)
        future.add_done_callback(_forget)
        return future

    def _adjust_thread_count(self):
        try:
            super()._adjust_thread_count()
        except RuntimeError:
            # nové vlákno nejde založit; úlohu ve frontě zpracuje některé z existujících
            if not self._threads:
                raise


_LOCK = threading.Lock()
_HOST_STOP = [None]   # `should_stop` hostitele pro úlohy na pozadí — `set_host_stop()`
_PENDING = set()   # nedokončené úlohy všech `BackgroundPool` v procesu


def _forget(future):
    with _LOCK:
        _PENDING.discard(future)


def set_host_stop(should_stop):
    """Hostitel (Kodi) řekne, jak poznat konec; jádro ho jinak nezná."""
    _HOST_STOP[0] = should_stop


def cancel_background():
    """Zruší všechny ještě nezačaté úlohy na pozadí. Vrací jejich počet."""
    with _LOCK:
        pending = list(_PENDING)
    return sum(1 for f in pending if f.cancel())


def never():
    """Výchozí `should_stop` — nikdy nepřerušit (HA, Stremio, testy bez hostitele)."""
    return False


def check(should_stop):
    """Vyhodí `Aborted`, když `should_stop()` říká, že je čas skončit."""
    if should_stop is not None and should_stop():
        raise Aborted()


def gather(pool, futures, should_stop, on_done=None, poll=STOP_POLL, deadline=None):
    """Počká na všechny `futures` z `pool` a mezi tím se každou `poll` sekundu
    ptá `should_stop()`. Nahrazuje `with ThreadPoolExecutor(...)` + `as_completed()`,
    které čekaly na úplně všechno bez možnosti přestat.

    Při přerušení zruší, co ještě ani nezačalo (`Future.cancel()`), executor
    zavře BEZ čekání na rozběhnutá vlákna (ta doběhnou sama na vlastní timeout —
    síťové volání ve stdlib zvenčí přerušit nejde) a vyhodí `Aborted`. Bez
    přerušení executor zavře normálně a vrátí `futures` v původním pořadí,
    všechny hotové. `on_done(future)` se volá za každou dokončenou, v pořadí
    dokončení — pro ukazatele průběhu. Vrátí-li `on_done` pravdivou hodnotu,
    bere se to jako „už máme dost": zbývající `pending` se zruší (co ještě
    nezačalo) stejně jako při přerušení, executor zavře BEZ čekání a vrátí se
    `futures` — rozběhnutá vlákna, která se nestihla zrušit, doběhnou na pozadí.
    Na rozdíl od `should_stop()` tohle nevyhazuje `Aborted`, je to řádný konec.
    `pool.shutdown(cancel_futures=)` je až od Pythonu 3.9, Kodi 20 má 3.8, proto
    se ruší ručně.

    `deadline` (s), je-li dán: po jeho uplynutí se dál nečeká a vrátí se `futures`,
    z nichž některé nemusí být hotové (volající se ptá `Future.done()`). Nic se neruší —
    executor se zavře bez čekání a rozběhnuté i čekající úlohy doběhnou na pozadí
    (čtení hlaviček si výsledek uloží do cache pro příště). `on_done` se pro ně už nevolá.
    Může to být i funkce `deadline(pending) -> s | None`: volá se před každým čekáním
    s množinou ještě nehotových úloh, takže každá z nich smí mít vlastní rozpočet
    (čeká se, dokud ho má aspoň jedna z těch, co zbývají).
    """
    pending = set(futures)
    started = time.monotonic()

    def limit():
        seconds = deadline(pending) if callable(deadline) else deadline
        return None if seconds is None else started + seconds
    try:
        while pending:
            end = limit()
            left = poll if end is None else min(poll, end - time.monotonic())
            if left <= 0:
                pool.shutdown(wait=False)
                return list(futures)
            done, pending = wait(pending, timeout=left, return_when=FIRST_COMPLETED)
            for future in done:
                if on_done and on_done(future):
                    for other in pending:
                        other.cancel()
                    pool.shutdown(wait=False)
                    return list(futures)
            if pending and should_stop is not None and should_stop():
                for future in pending:
                    future.cancel()
                raise Aborted()
    except BaseException:
        pool.shutdown(wait=False)
        raise
    pool.shutdown(wait=True)
    return list(futures)
