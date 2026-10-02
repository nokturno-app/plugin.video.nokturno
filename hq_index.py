"""Zpětná kompatibilita: index je od 9.11 obecný v jádru (`catindex`)."""
import os
import sys

_LIB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resources", "lib")
if _LIB not in sys.path:
    sys.path.insert(0, _LIB)
from catindex import *  # noqa: E402,F401,F403
from catindex import RECHECK_AFTER, RETRY_AFTER  # noqa: E402,F401
