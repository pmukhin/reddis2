"""
Redis integration tests.
Requires a running server on localhost:6379.
Run with: pytest redis_test.py -v
"""

import pytest
import redis


@pytest.fixture(autouse=True)
def r():
    client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
    client.ping()
    client.flushdb()
    return client


# ── Strings ───────────────────────────────────────────────────────────────────

class TestStrings:
    def test_set_and_get(self, r):
        r.set("key", "Alice")
        assert r.get("key") == "Alice"

    def test_get_missing_key(self, r):
        assert r.get("missing") is None

    def test_incr(self, r):
        r.set("counter", 0)
        r.incr("counter")
        r.incr("counter")
        assert r.get("counter") == "2"

    def test_incrby(self, r):
        r.set("counter", 2)
        r.incrby("counter", 8)
        assert r.get("counter") == "10"

    def test_set_with_ex(self, r):
        r.set("temp", "bye", ex=5)
        assert r.ttl("temp") > 0

    def test_set_nx(self, r):
        r.set("nx", "first", nx=True)
        r.set("nx", "second", nx=True)
        assert r.get("nx") == "first"


# ── Lists ─────────────────────────────────────────────────────────────────────

class TestLists:
    def test_rpush_and_lrange(self, r):
        r.rpush("q", "a", "b", "c")
        assert r.lrange("q", 0, -1) == ["a", "b", "c"]

    def test_lpush(self, r):
        r.rpush("q", "a", "b")
        r.lpush("q", "z")
        assert r.lrange("q", 0, -1) == ["z", "a", "b"]

    def test_lpop(self, r):
        r.rpush("q", "a", "b", "c")
        assert r.lpop("q") == "a"
        assert r.lrange("q", 0, -1) == ["b", "c"]

    def test_rpop(self, r):
        r.rpush("q", "a", "b", "c")
        assert r.rpop("q") == "c"
        assert r.lrange("q", 0, -1) == ["a", "b"]

    def test_llen(self, r):
        r.rpush("q", "a", "b", "c")
        assert r.llen("q") == 3


# ── Hashes ────────────────────────────────────────────────────────────────────

class TestHashes:
    def test_hset_and_hget(self, r):
        r.hset("h", "name", "Alice")
        assert r.hget("h", "name") == "Alice"

    def test_hset_mapping_and_hgetall(self, r):
        r.hset("h", mapping={"name": "Alice", "email": "a@b.com", "age": "30"})
        assert r.hgetall("h") == {"name": "Alice", "email": "a@b.com", "age": "30"}

    def test_hmget(self, r):
        r.hset("h", mapping={"a": "1", "b": "2", "c": "3"})
        assert r.hmget("h", "a", "c") == ["1", "3"]

    def test_hincrby(self, r):
        r.hset("h", "score", "100")
        r.hincrby("h", "score", 50)
        assert r.hget("h", "score") == "150"

    def test_hincrby_missing_field(self, r):
        r.hset("h", "other", "x")
        r.hincrby("h", "score", 10)
        assert r.hget("h", "score") == "10"

    def test_hexists(self, r):
        r.hset("h", "name", "Alice")
        assert r.hexists("h", "name") is True
        assert r.hexists("h", "missing") is False

    def test_hkeys(self, r):
        r.hset("h", mapping={"a": "1", "b": "2", "c": "3"})
        assert set(r.hkeys("h")) == {"a", "b", "c"}


# ── Sets ──────────────────────────────────────────────────────────────────────

class TestSets:
    def test_sadd_and_smembers(self, r):
        r.sadd("s", "Alice", "Bob", "Carol")
        assert r.smembers("s") == {"Alice", "Bob", "Carol"}

    def test_sismember(self, r):
        r.sadd("s", "Alice", "Bob")
        assert r.sismember("s", "Alice") is True
        assert r.sismember("s", "Dave") is False

    def test_sinter(self, r):
        r.sadd("a", "Alice", "Bob", "Carol")
        r.sadd("b", "Bob", "Dave")
        assert r.sinter("a", "b") == {"Bob"}

    def test_sunion(self, r):
        r.sadd("a", "Alice", "Bob")
        r.sadd("b", "Bob", "Carol")
        assert r.sunion("a", "b") == {"Alice", "Bob", "Carol"}

    def test_sdiff(self, r):
        r.sadd("a", "Alice", "Bob", "Carol")
        r.sadd("b", "Bob", "Dave")
        assert r.sdiff("a", "b") == {"Alice", "Carol"}

    def test_scard(self, r):
        r.sadd("s", "Alice", "Bob", "Carol")
        assert r.scard("s") == 3


# ── Sorted Sets ───────────────────────────────────────────────────────────────

class TestSortedSets:
    def test_zadd_and_zrange(self, r):
        r.zadd("z", {"Alice": 900, "Bob": 750, "Carol": 870, "Dave": 600})
        assert r.zrange("z", 0, -1, withscores=True) == [
            ("Dave", 600.0), ("Bob", 750.0), ("Carol", 870.0), ("Alice", 900.0),
        ]

    def test_zrange_without_scores(self, r):
        r.zadd("z", {"Alice": 900, "Bob": 750, "Carol": 870})
        assert r.zrange("z", 0, -1) == ["Bob", "Carol", "Alice"]

    def test_zrevrange(self, r):
        r.zadd("z", {"Alice": 900, "Bob": 750, "Carol": 870})
        assert r.zrange("z", 0, -1, withscores=True, rev=True) == [
            ("Alice", 900.0), ("Carol", 870.0), ("Bob", 750.0),
        ]

    def test_zrank(self, r):
        r.zadd("z", {"Alice": 900, "Bob": 750, "Dave": 600})
        assert r.zrank("z", "Dave") == 0
        assert r.zrank("z", "Alice") == 2

    def test_zrevrank(self, r):
        r.zadd("z", {"Alice": 900, "Bob": 750, "Dave": 600})
        assert r.zrevrank("z", "Alice") == 0
        assert r.zrevrank("z", "Dave") == 2

    def test_zscore(self, r):
        r.zadd("z", {"Alice": 900, "Carol": 870})
        assert r.zscore("z", "Carol") == 870.0
        assert r.zscore("z", "missing") is None

    def test_zrangebyscore(self, r):
        r.zadd("z", {"Alice": 900, "Bob": 750, "Carol": 870, "Dave": 600})
        assert r.zrangebyscore("z", 700, 900, withscores=True) == [
            ("Bob", 750.0), ("Carol", 870.0), ("Alice", 900.0),
        ]

    def test_zincrby(self, r):
        r.zadd("z", {"Dave": 600})
        r.zincrby("z", 200, "Dave")
        assert r.zscore("z", "Dave") == 800.0

    def test_zadd_updates_existing(self, r):
        r.zadd("z", {"Alice": 100})
        r.zadd("z", {"Alice": 200})
        assert r.zscore("z", "Alice") == 200.0
        assert r.zcard("z") == 1


# ── Key Utilities ─────────────────────────────────────────────────────────────

class TestKeyUtilities:
    def test_exists(self, r):
        r.set("key", "value")
        assert r.exists("key") == 1
        assert r.exists("ghost") == 0

    def test_del(self, r):
        r.set("a", "1")
        r.set("b", "2")
        r.delete("a", "b")
        assert r.get("a") is None
        assert r.get("b") is None
