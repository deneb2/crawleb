import mock
import unittest
import mongomock

from core.seen_manager import canonize, SeenManager
from core.metadata import DocumentMetadata
from tests.test_base import BaseTestClass


class TestSeenManager(BaseTestClass):
    def test_canonize(self):
        input_url = ["http://www.google.com",
                     "http://www.google.com/",
                     "https://www.google.com",
                     "https://www.google.it/",
                     "https://www.google.it?test=10&q=1",
                     "https://www.google.it/test/test/",
                     "https://www.google.it/test/test",
        ]

        output = ["www.google.com",
                  "www.google.com",
                  "www.google.com",
                  "www.google.it",
                  "www.google.it%3Ftest%3D10%26q%3D1",
                  "www.google.it/test/test",
                  "www.google.it/test/test",
        ]

        for i, u in enumerate(input_url):
            self.assertEqual(canonize(u), output[i])

    @mock.patch('pymongo.MongoClient')
    def test_add_and_delete(self, mc):
        mc.return_value = mongomock.MongoClient()
        sm = SeenManager("test", "host", 0, "db")

        dmeta = DocumentMetadata("http://www.google.com")
        dmeta.alternatives = ["http://www.google.com",
                              "http://www.google2.com/",
                              "https://www.google3.com",
        ]
        dmeta.dhash = 2413242

        other_urls = ["www.prova.com",
                     "www.other.com",
        ]
        # adding urls
        sm.add(dmeta)

        # tring removing not present urls
        for o in other_urls:
            sm.delete(o)

        # checking presence
        for i in dmeta.alternatives:
            self.assertTrue(canonize(i) in sm.store)
        self.assertEqual(len(dmeta.alternatives), len(sm.store))

        # checking not precence
        for u in other_urls:
            self.assertFalse(canonize(u) in sm.store)

        # checking correctness
        for i in dmeta.alternatives:
            data = sm.store.get(canonize(i))
            self.assertEqual(data["count"], 1)
            self.assertEqual(data["page_hash"], dmeta.dhash)

        # deleting alternatives
        sm.delete(dmeta.alternatives[0])

        # checking empty db
        for i in dmeta.alternatives:
            self.assertFalse(canonize(i) in sm.store)
        self.assertEqual(0, len(sm.store))

    @mock.patch('pymongo.MongoClient')
    def test_update(self, mc):
        mc.return_value = mongomock.MongoClient()
        sm = SeenManager("test", "host", 0, "db")

        dmeta = DocumentMetadata("http://www.google.com?q=test")
        dmeta.alternatives = ["http://www.google.com?q=test",
                              "http://www.google2.com/",
                              "https://www.google3.com",
        ]
        dmeta.dhash = 2413242

        dmeta2 = DocumentMetadata("http://www.google2.com")
        dmeta2.alternatives = ["http://www.google2.com",
                              "https://www.google3.com",
        ]
        dmeta2.dhash = 12121212

        # adding urls
        sm.add(dmeta)
        sm.add(dmeta2)

        output_alternatives = dmeta.alternatives + dmeta2.alternatives
        output_alternatives = list(set(canonize(i) for i in output_alternatives))

        # checking presence and checking not double anonization
        for i in output_alternatives:
            self.assertTrue(i in sm.store)
            for i, v in enumerate(sm.store.get(i)['alternatives']):
                self.assertEqual(v, output_alternatives[i])

    @mock.patch('pymongo.MongoClient')
    def test_is_new(self, mc):
        mc.return_value = mongomock.MongoClient()
        sm = SeenManager("test", "host", 0, "db")

        dmeta = DocumentMetadata("http://www.google.com")
        dmeta.alternatives = ["http://www.google.com",
                              "http://www.google2.com/",
                              "https://www.google3.com",
        ]
        dmeta.dhash = 2413242

        other_urls = ["www.test.com",
                     "www.other.com",
        ]

        # adding urls
        sm.add(dmeta)

        for u in dmeta.alternatives:
            self.assertFalse(sm.is_new(canonize(u)))

        for u in other_urls:
            self.assertTrue(sm.is_new(canonize(u)))

    @mock.patch('pymongo.MongoClient')
    def test_incr_n(self, mc):
        mc.return_value = mongomock.MongoClient()
        sm = SeenManager("test", "host", 0, "db")

        dmeta = DocumentMetadata("http://www.google.com")
        dmeta.alternatives = ["http://www.google.com",
                              "http://www.google2.com/",
                              "https://www.google3.com",
        ]
        dmeta.dhash = 2413242

        # adding urls
        sm.add(dmeta)
        
        # increase counters
        sm.incr_n(dmeta.alternatives[0])

        for i in dmeta.alternatives:
            data = sm.store.get(canonize(i))
            self.assertEqual(data["count"], 2)

    @mock.patch('pymongo.MongoClient')
    def test_is_changed(self, mc):
        mc.return_value = mongomock.MongoClient()
        sm = SeenManager("test", "host", 0, "db")

        dmeta = DocumentMetadata("http://www.google.com")
        dmeta.alternatives = ["http://www.google.com",
                              "http://www.google2.com/",
                              "https://www.google3.com",
        ]
        dmeta.dhash = 2413242

        # adding urls
        sm.add(dmeta)

        for u in dmeta.alternatives:
            self.assertFalse(sm.is_changed(u, dmeta.dhash))

        for u in dmeta.alternatives:
            self.assertFalse(sm.is_changed(u, dmeta.dhash+2))

        for u in dmeta.alternatives:
            self.assertTrue(sm.is_changed(u, dmeta.dhash+3))

if __name__ == "__main__":
    unittest.main()
