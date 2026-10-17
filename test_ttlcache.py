import asyncio
import unittest

from ttlcache import TTLCache, async_cached


class FakeClock:
    def __init__(self): self.t = 0.0
    def __call__(self): return self.t


class TTLCacheTest(unittest.TestCase):
    def test_lru_eviction(self):
        c = TTLCache(maxsize=2)
        c.put("a", 1); c.put("b", 2)
        c.get("a")
        c.put("c", 3)
        self.assertIsNone(c.get("b"))
        self.assertEqual(c.get("a"), 1)

    def test_ttl_expiry(self):
        clock = FakeClock()
        c = TTLCache(ttl=5, clock=clock)
        c.put("k", "v")
        clock.t = 4.9
        self.assertEqual(c.get("k"), "v")
        clock.t = 5.1
        self.assertIsNone(c.get("k"))

    def test_no_stampede(self):
        calls = 0

        @async_cached()
        async def slow(x):
            nonlocal calls
            calls += 1
            await asyncio.sleep(0.01)
            return x * 2

        async def run():
            return await asyncio.gather(*(slow(21) for _ in range(50)))

        self.assertEqual(asyncio.run(run()), [42] * 50)
        self.assertEqual(calls, 1)


if __name__ == "__main__":
    unittest.main()
