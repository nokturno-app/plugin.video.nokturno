"""hq_index – čistý index „Filmů ve vysoké kvalitě", bez Kodi."""
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
import hq_index as H  # noqa: E402


def M(i, genres=("Akční",)):
    return {"id": i, "name": i, "genres": list(genres)}


class T(unittest.TestCase):
    def test_merge_a_odebrani(self):
        idx = {}
        H.merge_pool(idx, [M("tt1"), M("tt2")], 0)
        self.assertEqual(idx["items"]["tt2"]["rank"], 1)
        H.merge_pool(idx, [M("tt2"), M("tt3")], 0)
        self.assertEqual(set(idx["items"]), {"tt2", "tt3"})

    def test_batch_nove_pak_stare(self):
        idx = {}
        H.merge_pool(idx, [M("tt1"), M("tt2"), M("tt3")], 0)
        H.record(idx, "tt1", True, 1000)
        H.record(idx, "tt2", False, 2000)
        now = 2000 + 4 * 86400
        self.assertEqual(H.next_batch(idx, now, size=8), ["tt3", "tt1", "tt2"])
        self.assertEqual(H.next_batch(idx, now, size=1), ["tt3"])
        self.assertEqual(H.next_batch(idx, 3000), ["tt3"])   # tt1/tt2 ještě čerstvé

    def test_selhani_zkusit_za_30_min(self):
        idx = {}
        H.merge_pool(idx, [M("tt1")], 0)
        H.record(idx, "tt1", None, 5000)
        self.assertIsNone(idx["items"]["tt1"]["ok"])
        self.assertEqual(H.next_batch(idx, 5000 + 1000), [])
        self.assertEqual(H.next_batch(idx, 5000 + 1900), ["tt1"])

    def test_visible_a_zanry(self):
        idx = {}
        H.merge_pool(idx, [M("tt1", ("Akční",)), M("tt2", ("Drama", "Akční")), M("tt3", ("Komedie",))], 0)
        H.record(idx, "tt2", True, 1)
        H.record(idx, "tt1", True, 1)
        H.record(idx, "tt3", False, 1)
        self.assertEqual([m["id"] for m in H.visible(idx)], ["tt1", "tt2"])
        self.assertEqual([m["id"] for m in H.visible(idx, "Drama")], ["tt2"])
        self.assertEqual(H.genres_available(idx), ["Akční", "Drama"])

    def test_signature(self):
        self.assertNotEqual(H.signature(4, True, "CZ"), H.signature(4, False, "CZ"))


if __name__ == "__main__":
    unittest.main()
