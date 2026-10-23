#!/usr/bin/env python3
"""
E-commerce support gateway demo load script.

Three tenant tiers send realistic support questions through the gateway.
Runs in three phases to tell a story on the dashboard:
  Phase 1 (30s)  — cold start, all misses, real GPT latency visible
  Phase 2 (90s)  — cache warms, latency collapses, savings accumulate
  Phase 3 (30s)  — ShopEasy hits budget limit, others keep serving

Usage:
  python3 scripts/demo_load.py --admin-key $ADMIN_API_KEY
"""

import argparse
import json
import sys
import threading
import time
import urllib.error
import urllib.request

GATEWAY = "http://localhost:8080"
MODEL = "support"
SYSTEM_PROMPT = (
    "You are a helpful customer support agent for an e-commerce store. "
    "Answer concisely in 1-2 sentences."
)

QUESTIONS = [
    "What is your return policy?",
    "How do I track my order?",
    "Can I change my shipping address after ordering?",
    "How long does standard shipping take?",
    "Do you offer free shipping?",
    "How do I apply a discount code?",
    "What payment methods do you accept?",
    "How do I cancel my order?",
    "What do I do if my item arrived damaged?",
    "How do I contact customer support?",
]

TENANTS = [
    {"name": "shopease-support",  "rpm": 30,  "daily_cost_micros": 500_000,   "daily_tokens": 50_000},
    {"name": "megamart-support",  "rpm": 120, "daily_cost_micros": 5_000_000,  "daily_tokens": 500_000},
    {"name": "luxebrand-support", "rpm": 300, "daily_cost_micros": 20_000_000, "daily_tokens": 2_000_000},
]


def post(url, payload, token=None):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.loads(r.read()), r.status
    except urllib.error.HTTPError as e:
        return json.loads(e.read() or b"{}"), e.code
    except Exception as e:
        return {"error": str(e)}, 0


def create_keys(admin_key):
    keys = {}
    for t in TENANTS:
        body = {
            "name": t["name"],
            "rpm_limit": t["rpm"],
            "daily_cost_micros": t["daily_cost_micros"],
            "daily_tokens": t["daily_tokens"],
        }
        resp, status = post(f"{GATEWAY}/admin/keys", body, admin_key)
        if status not in (200, 201):
            print(f"  ✗ {t['name']}: {resp}", file=sys.stderr)
            sys.exit(1)
        keys[t["name"]] = resp["key"]
        print(f"  ✓ {t['name']} created")
    return keys


def send_request(key, question, stats, label):
    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": question},
        ],
        "temperature": 0,
        "max_tokens": 80,
    }
    t0 = time.time()
    resp, status = post(f"{GATEWAY}/v1/chat/completions", payload, key)
    ms = (time.time() - t0) * 1000

    with stats["lock"]:
        stats["total"] += 1
        if status == 200:
            stats["ok"] += 1
            stats["ms"].append(ms)
        elif status == 429:
            stats["rate_limited"] += 1
        elif status == 402:
            stats["budget_hit"][label] = stats["budget_hit"].get(label, 0) + 1
        else:
            stats["errors"] += 1


def run_phase(name, duration, keys, rps_per_tenant, stats):
    print(f"\n── {name} ({duration}s, {rps_per_tenant} req/s per tenant) ──")
    end = time.time() + duration
    q_idx = 0
    threads = []

    while time.time() < end:
        question = QUESTIONS[q_idx % len(QUESTIONS)]
        q_idx += 1
        for tenant_name, key in keys.items():
            t = threading.Thread(
                target=send_request,
                args=(key, question, stats, tenant_name),
                daemon=True,
            )
            t.start()
            threads.append(t)
        time.sleep(1.0 / rps_per_tenant)

    for t in threads:
        t.join(timeout=65)


def print_stats(stats):
    total = stats["total"]
    ok    = stats["ok"]
    ms    = sorted(stats["ms"])
    p50   = ms[int(len(ms) * 0.50)] if ms else 0
    p99   = ms[int(len(ms) * 0.99)] if ms else 0
    print(f"\n── Results ──")
    print(f"  Total requests : {total}")
    print(f"  Successful     : {ok}")
    print(f"  Rate limited   : {stats['rate_limited']}")
    print(f"  Budget blocked : {dict(stats['budget_hit'])}")
    print(f"  Errors         : {stats['errors']}")
    if ms:
        print(f"  Latency p50    : {p50:.0f}ms")
        print(f"  Latency p99    : {p99:.0f}ms")


def main():
    global GATEWAY
    parser = argparse.ArgumentParser()
    parser.add_argument("--admin-key", required=True)
    parser.add_argument("--gateway", default=GATEWAY)
    args = parser.parse_args()
    GATEWAY = args.gateway

    stats = {
        "lock": threading.Lock(),
        "total": 0, "ok": 0, "errors": 0,
        "rate_limited": 0, "budget_hit": {},
        "ms": [],
    }

    print("Creating tenant keys...")
    keys = create_keys(args.admin_key)

    # Phase 1 — cold cache, real latency visible
    run_phase("Phase 1: Cold start", 30, keys, rps_per_tenant=1, stats=stats)

    # Phase 2 — cache warms, latency collapses, savings grow
    run_phase("Phase 2: Cache warm-up", 90, keys, rps_per_tenant=3, stats=stats)

    # Phase 3 — high rate, ShopEasy budget pressured
    run_phase("Phase 3: Budget pressure", 30, keys, rps_per_tenant=6, stats=stats)

    print_stats(stats)


if __name__ == "__main__":
    main()
