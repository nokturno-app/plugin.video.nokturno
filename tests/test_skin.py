"""Skin Estuary Nokturno: každá adresa v hlavní nabídce a řadách vede na akci, kterou doplněk zná."""
import os
import re
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "skin"))


class TestSkinUrls(unittest.TestCase):
    def test_akce_existuji(self):
        try:
            import build
        except ImportError as e:   # PIL na CI Kodi chybět může, skin se staví lokálně
            self.skipTest(e)
        with open(os.path.join(ROOT, "default.py"), encoding="utf-8") as f:
            src = f.read()
        urls = [row[2] for row in build.ROWS]
        for u in urls:
            action = re.search(r"action=(\w+)", u)
            if not action:
                self.assertEqual(u, build.P)   # kořen doplňku
                continue
            a = action.group(1)
            self.assertTrue(re.search(rf'action == "{a}"|build_url\(action="{a}"', src), a)


if __name__ == "__main__":
    unittest.main()
