#!/usr/bin/env python3
"""Check every simulated link and all five SSH routes after Compose is up."""

import concurrent.futures
import json
from pathlib import Path
import re
import subprocess


ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ["docker", "compose", "-f", "compose.test.yaml"]
LINKS = [
    ("client", "entry1", 100),
    ("client", "entry2", 125),
    ("client", "entry3", 150),
    ("client", "entry4", 175),
    ("client", "entry5", 200),
    ("entry1", "relay1", 150),
    ("entry2", "relay2", 200),
    ("entry3", "relay3", 250),
    ("entry4", "relay4", 300),
    ("entry5", "relay5", 100),
    ("relay1", "hub", 200),
    ("relay2", "hub", 250),
    ("relay3", "hub", 300),
    ("relay4", "relay4b", 100),
    ("relay5", "relay5b", 150),
    ("relay4b", "hub", 200),
    ("relay5b", "hub", 250),
    ("hub", "target", 200),
]


def run(*args):
    return subprocess.run(
        [*COMPOSE, *args], cwd=ROOT, check=True, capture_output=True, text=True,
        timeout=120,
    ).stdout


def check_link(link):
    source, target, expected = link
    output = run("exec", "-T", source, "ping", "-c", "3", "-W", "2", target)
    match = re.search(r"rtt min/avg/max/mdev = ([\d.]+)/", output)
    if match is None:
        raise RuntimeError(f"no RTT for {source} -> {target}: {output}")
    measured = float(match.group(1))
    if not expected - 15 <= measured <= expected + 35:
        raise RuntimeError(
            f"{source} -> {target}: {measured:.1f} ms, expected about {expected} ms"
        )
    return f"{source:8} -> {target:8} {measured:6.1f} ms (configured {expected} ms)"


def interface_to(source, target):
    address = run("exec", "-T", source, "getent", "hosts", target).split()[0]
    route = run("exec", "-T", source, "ip", "route", "get", address)
    match = re.search(r"\bdev (\S+)", route)
    if match is None:
        raise RuntimeError(f"no interface from {source} to {target}: {route}")
    return match.group(1)


def check_rate(source, target, kbit):
    dev = interface_to(source, target)
    qdiscs = json.loads(run("exec", "-T", source, "tc", "-j", "qdisc", "show", "dev", dev))
    actual = next(item["options"]["rate"]["rate"] for item in qdiscs if item["kind"] == "netem")
    expected = kbit * 1000 // 8
    if actual != expected:
        raise RuntimeError(f"{source} -> {target}: {actual * 8} bit/s, expected {kbit} kbit/s")
    return f"{source} -> {target}: {kbit} kbit/s"


def main():
    for source, target in [("client", "target"), ("entry4", "relay4b")]:
        isolated = subprocess.run(
            [*COMPOSE, "exec", "-T", source, "getent", "hosts", target],
            cwd=ROOT, capture_output=True, text=True, timeout=10,
        )
        if isolated.returncode == 0:
            raise RuntimeError(f"{source} unexpectedly resolves nonadjacent {target}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        for line in pool.map(check_link, LINKS):
            print(line)

    front_dev = interface_to("client", "entry1")
    classes = json.loads(run("exec", "-T", "client", "tc", "-j", "class", "show", "dev", front_dev))
    class_rates = {item["handle"]: item["rate"] for item in classes}
    second_lte_sources = ["relay1", "relay2", "relay3", "relay4b", "relay5b"]
    for i, kbit in enumerate([500, 750, 1000, 1250, 1500], start=1):
        expected = kbit * 1000 // 8
        assert class_rates[f"1:{10 + i}"] == expected, (i, class_rates)
        print(check_rate(f"entry{i}", "client", kbit))
        print(check_rate(second_lte_sources[i - 1], "hub", kbit))
        print(check_rate("hub", second_lte_sources[i - 1], kbit))
        print(check_rate(f"entry{i}", f"relay{i}", 1_000_000))
        print(check_rate(f"relay{i}", f"entry{i}", 1_000_000))
    for source, target in [("relay4", "relay4b"), ("relay5", "relay5b")]:
        print(check_rate(source, target, 1_000_000))
        print(check_rate(target, source, 1_000_000))
    print(check_rate("hub", "target", 1_000_000))
    print(check_rate("target", "hub", 1_000_000))

    report = json.loads(run(
        "exec", "-T", "client", "target/debug/mssh", "--config",
        "/data/config.toml", "status", "target", "--json",
    ))
    routes = {route["route"]: route for route in report["routes"]}
    assert set(routes) == {f"path{i}" for i in range(1, 6)}, routes.keys()
    for i in range(1, 6):
        route = routes[f"path{i}"]
        expected_hops = 4 if i <= 3 else 5
        assert route["state"] == "UP", route
        assert len(route["hop_timings"]) == expected_hops, route
        print(f"path{i}: UP, {expected_hops} SSH hops")


if __name__ == "__main__":
    main()
