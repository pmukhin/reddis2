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
        r.rpush("q:test_rpush_and_lrange", "a", "b", "c")
        assert r.llen("q:test_rpush_and_lrange") == 3
        assert r.lrange("q:test_rpush_and_lrange", 0, -1) == ["a", "b", "c"]

    def test_lpush(self, r):
        r.rpush("q:test_lpush", "a", "b")
        r.lpush("q:test_lpush", "z")
        assert r.lrange("q:test_lpush", 0, -1) == ["z", "a", "b"]

    def test_lpop(self, r):
        assert r.rpush("q:test_lpop", "a", "b", "c") == 3
        assert r.lpop("q:test_lpop") == "a"
        assert r.lrange("q:test_lpop", 0, -1) == ["b", "c"]

    def test_rpop(self, r):
        r.rpush("q:test_rpop", "a", "b", "c")
        assert r.rpop("q:test_rpop") == "c"
        assert r.lrange("q:test_rpop", 0, -1) == ["a", "b"]

    def test_llen(self, r):
        r.rpush("q:test_llen", "a", "b", "c")
        assert r.llen("q:test_llen") == 3


# ── Hashes ────────────────────────────────────────────────────────────────────

class TestHashes:
    def test_hset_and_hget(self, r):
        r.hset("h:test_hset_and_hget", "name", "Alice")
        assert r.hget("h:test_hset_and_hget", "name") == "Alice"

    def test_hset_mapping_and_hgetall(self, r):
        r.hset("h:test_hset_mapping_and_hgetall", mapping={"name": "Alice", "email": "a@b.com", "age": "30"})
        assert r.hgetall("h:test_hset_mapping_and_hgetall") == {"name": "Alice", "email": "a@b.com", "age": "30"}

    def test_hmget(self, r):
        r.hset("h:test_hmget", mapping={"a": "1", "b": "2", "c": "3"})
        assert r.hmget("h:test_hmget", "a", "c") == ["1", "3"]

    def test_hincrby(self, r):
        r.hset("h:test_hincrby", "score", "100")
        r.hincrby("h:test_hincrby", "score", 50)
        assert r.hget("h:test_hincrby", "score") == "150"

    def test_hincrby_missing_field(self, r):
        r.hset("h:test_hincrby_missing_field", "other", "x")
        r.hincrby("h:test_hincrby_missing_field", "score", 10)
        assert r.hget("h:test_hincrby_missing_field", "score") == "10"

    def test_hexists(self, r):
        r.hset("h:test_hexists", "name", "Alice")
        assert r.hexists("h:test_hexists", "name") is True
        assert r.hexists("h:test_hexists", "missing") is False

    def test_hkeys(self, r):
        r.hset("h:test_hkeys", mapping={"a": "1", "b": "2", "c": "3"})
        assert set(r.hkeys("h:test_hkeys")) == {"a", "b", "c"}


# ── Sets ──────────────────────────────────────────────────────────────────────

class TestSets:
    def test_sadd_and_smembers(self, r):
        r.sadd("s:test_sadd_and_smembers", "Alice", "Bob", "Carol")
        assert r.smembers("s:test_sadd_and_smembers") == {"Alice", "Bob", "Carol"}

    def test_sismember(self, r):
        r.sadd("s:test_sadd_and_smembers", "Alice", "Bob")
        assert r.sismember("s:test_sadd_and_smembers", "Alice") is 1
        assert r.sismember("s:test_sadd_and_smembers", "Dave") is 0

    def test_sinter(self, r):
        r.sadd("a:test_sinter", "Alice", "Bob", "Carol")
        r.sadd("b:test_sinter", "Bob", "Dave")
        assert r.sinter("a:test_sinter", "b:test_sinter") == {"Bob"}

    def test_sunion(self, r):
        r.sadd("a:test_sunion", "Alice", "Bob")
        r.sadd("b:test_sunion", "Bob", "Carol")
        assert r.sunion("a:test_sunion", "b:test_sunion") == {"Alice", "Bob", "Carol"}

    def test_sdiff(self, r):
        r.sadd("a:test_sdiff", "Alice", "Bob", "Carol")
        r.sadd("b:test_sdiff", "Bob", "Dave")
        assert r.sdiff("a", "b") == {"Alice", "Carol"}

    def test_scard(self, r):
        r.sadd("s:test_scard", "Alice", "Bob", "Carol")
        assert r.scard("s:test_scard") == 3


# ── Sorted Sets ───────────────────────────────────────────────────────────────

class TestSortedSets:
    def test_zadd_and_zrange(self, r):
        r.zadd("z:test_zadd_and_zrange", {"Alice": 900, "Bob": 750, "Carol": 870, "Dave": 600})
        assert r.zrange("z:test_zadd_and_zrange", 0, -1, withscores=True) == [
            ("Dave", 600.0), ("Bob", 750.0), ("Carol", 870.0), ("Alice", 900.0),
        ]

    def test_zrange_without_scores(self, r):
        r.zadd("z:test_zrange_without_scores", {"Alice": 900, "Bob": 750, "Carol": 870})
        assert r.zrange("z:test_zrange_without_scores", 0, -1) == ["Bob", "Carol", "Alice"]

    def test_zrevrange(self, r):
        r.zadd("z:test_zrevrange", {"Alice": 900, "Bob": 750, "Carol": 870})
        assert r.zrange("z:test_zrevrange", 0, -1, withscores=True, rev=True) == [
            ("Alice", 900.0), ("Carol", 870.0), ("Bob", 750.0),
        ]

    def test_zrank(self, r):
        r.zadd("z:test_zrank", {"Alice": 900, "Bob": 750, "Dave": 600})
        assert r.zrank("z:test_zrank", "Dave") == 0
        assert r.zrank("z:test_zrank", "Alice") == 2

    def test_zrevrank(self, r):
        r.zadd("z:test_zrevrank", {"Alice": 900, "Bob": 750, "Dave": 600})
        assert r.zrevrank("z:test_zrevrank", "Alice") == 0
        assert r.zrevrank("z:test_zrevrank", "Dave") == 2

    def test_zscore(self, r):
        r.zadd("z:test_zscore", {"Alice": 900, "Carol": 870})
        assert r.zscore("z:test_zscore", "Carol") == 870.0
        assert r.zscore("z:test_zscore", "missing") is None

    def test_zrangebyscore(self, r):
        r.zadd("z:test_zrangebyscore", {"Alice": 900, "Bob": 750, "Carol": 870, "Dave": 600})
        assert r.zrangebyscore("z:test_zrangebyscore", 700, 900, withscores=True) == [
            ("Bob", 750.0), ("Carol", 870.0), ("Alice", 900.0),
        ]

    def test_zincrby(self, r):
        r.zadd("z:test_zincrby", {"Dave": 600})
        r.zincrby("z:test_zincrby", 200, "Dave")
        assert r.zscore("z:test_zincrby", "Dave") == 800.0

    def test_zadd_updates_existing(self, r):
        r.zadd("z:test_zadd_updates_existing", {"Alice": 100})
        r.zadd("z:test_zadd_updates_existing", {"Alice": 200})
        assert r.zscore("z:test_zadd_updates_existing", "Alice") == 200.0
        assert r.zcard("z:test_zadd_updates_existing") == 1


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
