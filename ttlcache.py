"""LRU-кэш с TTL на OrderedDict (O(1) get/put) + асинхронный декоратор с защитой от cache stampede."""
import asyncio
import functools
import time
from collections import OrderedDict

_MISSING = object()


class TTLCache:
    def __init__(self, maxsize=128, ttl=60.0, clock=time.monotonic):
        self.maxsize, self.ttl, self.clock = maxsize, ttl, clock
        self._data = OrderedDict()
        self.hits = self.misses = 0

    def get(self, key, default=None):
        item = self._data.get(key, _MISSING)
        if item is _MISSING or item[1] < self.clock():
            self._data.pop(key, None)
            self.misses += 1
            return default
        self._data.move_to_end(key)
        self.hits += 1
        return item[0]

    def put(self, key, value):
        self._data[key] = (value, self.clock() + self.ttl)
        self._data.move_to_end(key)
        if len(self._data) > self.maxsize:
            self._data.popitem(last=False)  # выкидываем самый старый

    def __len__(self):
        return len(self._data)


def async_cached(maxsize=128, ttl=60.0):
    """Кэширует корутину; параллельные вызовы с одним ключом ждут один и тот же запрос."""
    def decorator(fn):
        cache, inflight = TTLCache(maxsize, ttl), {}

        @functools.wraps(fn)
        async def wrapper(*args):
            value = cache.get(args, _MISSING)
            if value is not _MISSING:
                return value
            if args not in inflight:
                inflight[args] = asyncio.ensure_future(fn(*args))
            try:
                value = await inflight[args]
            finally:
                inflight.pop(args, None)
            cache.put(args, value)
            return value

        wrapper.cache = cache
        return wrapper
    return decorator


@async_cached(ttl=30)
async def exchange_rate(pair: str) -> float:
    """Имитация медленного внешнего API курсов валют."""
    await asyncio.sleep(0.1)
    return {"USD/KZT": 480.5, "EUR/KZT": 520.1}.get(pair, 0.0)


if __name__ == "__main__":
    async def demo():
        t = time.perf_counter()
        print(await asyncio.gather(*(exchange_rate("USD/KZT") for _ in range(100))))
        print(f"100 вызовов за {time.perf_counter() - t:.2f}s, hits={exchange_rate.cache.hits}")
    asyncio.run(demo())
