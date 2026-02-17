#!/usr/bin/env python3
"""
Redis core features test script.
Requires: pip install redis
Assumes Redis is running on localhost:6379 (default).
"""

import redis
import sys

# ── Connection ─────────────────────────────────────────────────────────────────

r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

try:
    r.ping()
    print("✓ Connected to Redis\n")
except redis.ConnectionError:
    print("✗ Could not connect to Redis. Is it running?")
    sys.exit(1)

# Clean slate for repeatable runs
r.flushdb()

def section(title):
    print(f"── {title} {'─' * (50 - len(title))}")

def ok(label, value=None):
    suffix = f" → {value}" if value is not None else ""
    print(f"  ✓ {label}{suffix}")

def fail(label, e):
    print(f"  ✗ {label}: {e}")


# ── Strings: SET / GET / INCR / EXPIRE ────────────────────────────────────────

section("Strings")

r.set("user:1:name", "Alice")
ok("SET user:1:name", r.get("user:1:name"))

r.set("counter", 0)
r.incr("counter")
r.incr("counter")
r.incrby("counter", 8)
ok("INCR / INCRBY counter", r.get("counter"))           # → 10

r.set("temp_key", "bye", ex=5)                          # expires in 5 seconds
ok("SET with EX (TTL)", f"{r.ttl('temp_key')}s remaining")

r.set("nx_key", "first", nx=True)                       # only set if not exists
r.set("nx_key", "second", nx=True)                      # this is ignored
ok("SET NX (only if not exists)", r.get("nx_key"))      # → first

print()

# ── Lists: LPUSH / RPUSH / LRANGE / LPOP / RPOP ──────────────────────────────

section("Lists")

r.rpush("queue", "job1", "job2", "job3")
ok("RPUSH queue", r.lrange("queue", 0, -1))

r.lpush("queue", "job0")
ok("LPUSH queue (prepend)", r.lrange("queue", 0, -1))

ok("LPOP (dequeue from front)", r.lpop("queue"))
ok("RPOP (dequeue from back)", r.rpop("queue"))
ok("LRANGE after pops", r.lrange("queue", 0, -1))
ok("LLEN", r.llen("queue"))

print()

# ── Hashes: HSET / HGET / HMGET / HGETALL / HINCRBY ─────────────────────────

section("Hashes")

r.hset("user:1", mapping={
    "name":  "Alice",
    "email": "alice@example.com",
    "age":   "30",
    "score": "100",
})
ok("HSET user:1 (mapping)")
ok("HGET user:1 name",      r.hget("user:1", "name"))
ok("HMGET name+email",      r.hmget("user:1", "name", "email"))
ok("HGETALL",               r.hgetall("user:1"))
r.hincrby("user:1", "score", 50)
ok("HINCRBY score +50",     r.hget("user:1", "score"))
ok("HEXISTS age",           r.hexists("user:1", "age"))
ok("HKEYS",                 r.hkeys("user:1"))

print()

# ── Sets: SADD / SMEMBERS / SISMEMBER / SINTER / SUNION / SDIFF ──────────────

section("Sets")

r.sadd("team:alpha", "Alice", "Bob", "Carol")
r.sadd("team:beta",  "Bob",   "Dave", "Eve")
ok("SADD team:alpha",   r.smembers("team:alpha"))
ok("SADD team:beta",    r.smembers("team:beta"))
ok("SISMEMBER Alice",   r.sismember("team:alpha", "Alice"))
ok("SISMEMBER Dave",    r.sismember("team:alpha", "Dave"))
ok("SINTER (overlap)",  r.sinter("team:alpha", "team:beta"))
ok("SUNION (all)",      r.sunion("team:alpha", "team:beta"))
ok("SDIFF alpha-beta",  r.sdiff("team:alpha", "team:beta"))
ok("SCARD team:alpha",  r.scard("team:alpha"))

print()

# ── Sorted Sets: ZADD / ZRANGE / ZRANK / ZSCORE / ZRANGEBYSCORE ──────────────

section("Sorted Sets")

r.zadd("leaderboard", {"Alice": 900, "Bob": 750, "Carol": 870, "Dave": 600})
ok("ZADD leaderboard")
ok("ZRANGE (low→high)",        r.zrange("leaderboard", 0, -1, withscores=True))
ok("ZREVRANGE (high→low)",     r.zrange("leaderboard", 0, -1, withscores=True, rev=True))
ok("ZRANK Alice",              r.zrank("leaderboard", "Alice"))
ok("ZREVRANK Alice",           r.zrevrank("leaderboard", "Alice"))
ok("ZSCORE Carol",             r.zscore("leaderboard", "Carol"))
ok("ZRANGEBYSCORE 700-900",    r.zrangebyscore("leaderboard", 700, 900, withscores=True))
r.zincrby("leaderboard", 200, "Dave")
ok("ZINCRBY Dave +200",        r.zscore("leaderboard", "Dave"))

print()

# ── Key utilities: EXISTS / TYPE / RENAME / SCAN ─────────────────────────────

section("Key Utilities")

ok("EXISTS user:1",            r.exists("user:1"))
ok("EXISTS ghost",             r.exists("ghost"))
ok("TYPE user:1",              r.type("user:1"))
ok("TYPE queue",               r.type("queue"))
ok("TYPE leaderboard",         r.type("leaderboard"))

r.set("old_key", "value")
r.rename("old_key", "new_key")
ok("RENAME old_key → new_key", r.get("new_key"))

cursor, keys = r.scan(0, match="user:*", count=100)
ok("SCAN user:*",              keys)

print()

# ── Pub/Sub (quick fire-and-forget demo) ──────────────────────────────────────

section("Pub/Sub")

import threading, time

messages = []

def subscriber():
    sub = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
    p = sub.pubsub()
    p.subscribe("news")
    for msg in p.listen():
        if msg["type"] == "message":
            messages.append(msg["data"])
            break  # exit after first message

t = threading.Thread(target=subscriber, daemon=True)
t.start()
time.sleep(0.1)                        # give subscriber a moment to connect

r.publish("news", "hello from publisher")
t.join(timeout=2)
ok("PUBLISH / SUBSCRIBE", messages)

print()

# ── Pipeline (batched commands) ───────────────────────────────────────────────

section("Pipeline")

pipe = r.pipeline()
pipe.set("p:a", 1)
pipe.set("p:b", 2)
pipe.set("p:c", 3)
pipe.mget("p:a", "p:b", "p:c")
results = pipe.execute()
ok("Pipeline SET×3 + MGET", results[-1])   # last result is the MGET

print()
print("All tests passed ✓")
