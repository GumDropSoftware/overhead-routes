#!/usr/bin/env python3
"""Overhead's route list: where airline flights actually went, built each night from adsb.lol's public history.

  overhead_routes.py nightly [YYYY-MM-DD]  what the Pi runs: catch up, grade, write and publish yesterday's table
  overhead_routes.py airlines          refresh airlines-allow.txt (the operators VRS lists routes for: scheduled airlines)
  overhead_routes.py day YYYY-MM-DD    stream that day's archive from GitHub into work/legs/YYYY-MM-DD.csv.gz
  overhead_routes.py table YYYY-MM-DD [dir]  write routes/ and latest.json from the 7 days ending that day
  overhead_routes.py traces YYYY-MM-DD dir   save that day's track files for the planes in its flights (lab replays)
  overhead_routes.py grade YYYY-MM-DD  score the table of the 7 days before against that day's real flights

Standard library only, so it runs on a Raspberry Pi as is. Memory stays flat: the archive is read as a stream, one
plane's day at a time, and only the flights found are kept. Nothing is written to disk but the results.
"""
from __future__ import annotations

import csv
import gzip
import io
import json
import math
import os
import re
import resource
import subprocess
import sys
import tarfile
import time
import urllib.request
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.abspath(__file__))
WORK = os.path.join(ROOT, "work")
WINDOW = 7
DAY = 86400
UA = {"User-Agent": "overhead-routes (+https://ota-drop.vercel.app/apps/overhead/)"}


def log(*parts):
    print(*parts, file=sys.stderr, flush=True)


def peak_mb():
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / 1e6 if sys.platform == "darwin" else peak / 1e3


def day_path(kind, day, ext):
    return os.path.join(WORK, kind, f"{day.isoformat()}.{ext}")


# ---------------------------------------------------------------- geometry

def dist_nm(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    return 2 * 3440.065 * math.asin(min(1.0, math.sqrt(a)))


def bearing(lat1, lon1, lat2, lon2):
    p1, p2, dl = math.radians(lat1), math.radians(lat2), math.radians(lon2 - lon1)
    y = math.sin(dl) * math.cos(p2)
    x = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)
    return math.degrees(math.atan2(y, x)) % 360


def angle_off(a, b):
    return abs((a - b + 180) % 360 - 180)


# ---------------------------------------------------------------- airports

# Footprint and penalty per size, as in the app's AirportDatabase: big fields sprawl and win near-ties.
SIZES = {"L": (1.5, 0.0), "M": (0.8, 0.4), "S": (0.3, 1.2)}


class Airports:
    """The app's own Resources/airports.tsv, so every code the table names resolves on the phone."""

    def __init__(self, path):
        self.cells = defaultdict(list)
        with open(path, encoding="utf-8") as f:
            for line in f:
                p = line.rstrip("\n").split("\t")
                if len(p) < 8 or not p[1]:
                    continue
                try:
                    lat, lon = float(p[3]), float(p[4])
                    elevation = float(p[8]) if len(p) > 8 and p[8] else 0.0
                except ValueError:
                    continue
                scheduled = p[9] == "1" if len(p) > 9 else True
                self.cells[(math.floor(lat), math.floor(lon))].append((p[1], p[2], lat, lon, elevation, scheduled))

    def candidates(self, lat, lon, limit, airliner):
        """Airliners never use small fields; other airline flights only small fields with airline service. Fields
        without scheduled service cost extra: airline flights mostly don't use them."""
        found = []
        cy, cx = math.floor(lat), math.floor(lon)
        for y in (cy - 1, cy, cy + 1):
            for x in (cx - 1, cx, cx + 1):
                for code, size, alat, alon, elevation, scheduled in self.cells.get((y, (x + 180) % 360 - 180), ()):
                    if size == "S" and (airliner or not scheduled):
                        continue
                    d = dist_nm(lat, lon, alat, alon)
                    if d > limit:
                        continue
                    footprint, penalty = SIZES.get(size, SIZES["S"])
                    score = max(0.0, d - footprint) + penalty + (0.0 if scheduled else 1.5)
                    found.append((code, alat, alon, elevation, d, score))
        return found

    def near(self, lat, lon, limit, airliner):
        found = self.candidates(lat, lon, limit, airliner)
        return min(found, key=lambda c: c[5])[0] if found else None

    def along(self, lat, lon, track, limit, airliner, altitude, behind, per_nm):
        """The field behind a plane first heard climbing out, or ahead of one last heard coming down, within 50° of its
        track. Too high above a field to have just used it (more than `per_nm` ft per nm away, plus 2,000) rules it out."""
        direction = (track + 180) % 360 if behind else track
        best = None
        for code, alat, alon, elevation, d, score in self.candidates(lat, lon, limit, airliner):
            if altitude is not None and altitude - elevation > d * per_nm + 2000:
                continue
            if d >= 2:
                offset = angle_off(bearing(lat, lon, alat, alon), direction)
                if offset > 50:
                    continue
                score += offset / 25
            if best is None or score < best[1]:
                best = (code, score)
        return best[0] if best else None


# ---------------------------------------------------------------- flights in one plane's day

T, LAT, LON, ALT, GROUND, GS, TRACK, FLAGS, VRATE, CALLSIGN, CATEGORY = range(11)
CALLSIGN_RE = re.compile(r"^[A-Z]{3}[0-9][0-9A-Z]{0,4}$")
FLIGHT_BYTES_RE = re.compile(rb'"flight"\s*:\s*"([A-Z]{3})[0-9]')


def standing_data_callsign(callsign):
    """VRS's form of the number, as the app's FlightInfoService.standingDataCallsign: KAL081 → KAL81, KAL0 stays."""
    code, number = callsign[:3], callsign[3:]
    if not number.startswith("0"):
        return callsign
    trimmed = number.lstrip("0")
    return code + (trimmed if trimmed[:1].isdigit() else "0" + trimmed)


def samples_of(trace):
    base = trace["timestamp"]
    out = []
    for p in trace["trace"]:
        if len(p) < 4 or p[1] is None or p[2] is None:
            continue
        ground = p[3] == "ground"
        altitude = 0 if ground else (p[3] if isinstance(p[3], (int, float)) else None)
        details = p[8] if len(p) > 8 and isinstance(p[8], dict) else None
        callsign = (details.get("flight") or "").strip().upper() or None if details else None
        out.append((
            base + p[0], p[1], p[2], altitude, ground,
            p[4] if len(p) > 4 else None, p[5] if len(p) > 5 else None,
            p[6] if len(p) > 6 and isinstance(p[6], int) else 0,
            p[7] if len(p) > 7 and isinstance(p[7], (int, float)) else None,
            callsign, details.get("category") if details else None,
        ))
    out.sort(key=lambda s: s[T])
    return out


def landed_unseen(quiet, heard):
    """Quiet 20+ min in the air, then heard again far closer than its speed would have carried it: it landed somewhere
    receivers can't hear and took off again (the app's TraceParser.landedUnseen)."""
    gap = heard[T] - quiet[T]
    if gap <= 1200:
        return False
    speeds = [s for s in (quiet[GS], heard[GS]) if s]
    speed = max(min(speeds) if speeds else 200, 150)
    return dist_nm(quiet[LAT], quiet[LON], heard[LAT], heard[LON]) < speed * gap / 3600 * 0.4


def flew_on_unheard(quiet, heard, before, after):
    """Quiet at cruise for over 90 minutes, then heard again at cruise under the same callsign about as far on as its
    speed carries it: it crossed an ocean beyond the receivers, it didn't stop (London → Atlanta has both ends heard)."""
    if (quiet[ALT] or 0) < 20000 or (heard[ALT] or 0) < 20000 or not before or before != after:
        return False
    gap = heard[T] - quiet[T]
    speeds = [s for s in (quiet[GS], heard[GS]) if s]
    if gap > 16 * 3600 or not speeds:
        return False
    flown = dist_nm(quiet[LAT], quiet[LON], heard[LAT], heard[LON])
    return 0.7 * sum(speeds) / len(speeds) * gap / 3600 <= flown <= 1.3 * max(speeds) * gap / 3600


def split_legs(samples):
    """Airborne stretches between ground contact, readsb's new-leg flag, or a silence it can't have flown through
    (the app's TraceParser.legStart rules, plus ocean crossings bridged). Each leg: points, the ground point before
    it, the ground point after it, and whether a crossing was bridged (the app can't measure those takeoffs)."""
    upcoming = [None] * len(samples)
    callsign = None
    for i in range(len(samples) - 1, -1, -1):
        callsign = samples[i][CALLSIGN] or callsign
        upcoming[i] = callsign
    legs, points, departure, previous, current, bridged = [], [], None, None, None, False
    for i, s in enumerate(samples):
        if s[GROUND]:
            # A lone ground report at altitude is a glitch, not a landing.
            if points and (points[-1][ALT] or 0) > 8000 and s[T] - points[-1][T] < 120:
                continue
            if points:
                legs.append([points, departure, s, bridged])
                points = []
            departure = s
            previous = s
            continue
        if points:
            gap = s[T] - points[-1][T]
            both_low = (points[-1][ALT] or 0) < 10000 and (s[ALT] or 0) < 10000
            crossed = gap > 5400 and flew_on_unheard(points[-1], s, current, upcoming[i])
            if s[FLAGS] & 2 or (gap > 5400 and not crossed) or (gap > 900 and both_low) or landed_unseen(points[-1], s):
                legs.append([points, departure, None, bridged])
                points, departure = [], None
            elif crossed:
                bridged = True
        if not points:
            current, bridged = None, False
            if not (previous is not None and previous[GROUND]):
                departure = None
        current = s[CALLSIGN] or current
        points.append(s)
        previous = s
    if points:
        legs.append([points, departure, None, bridged])
    return legs


def first_track(points, newest=False):
    seq = reversed(points) if newest else points
    for s in seq:
        if s[TRACK] is not None:
            return s[TRACK]
    return None


def origin_of(points, departure, airports, airliner):
    first = points[0]
    if departure is not None and dist_nm(departure[LAT], departure[LON], first[LAT], first[LON]) < 5:
        return airports.near(departure[LAT], departure[LON], 6, airliner)
    if first[ALT] is None:
        return None
    # First heard already airborne: only a plane climbing away from a field it can't be far above came from there.
    climb_out = [s for s in points if s[T] - first[T] < 120]
    climbing = any(s[VRATE] is not None and s[VRATE] > 300 for s in climb_out) or \
        (climb_out[-1][ALT] is not None and climb_out[-1][ALT] - first[ALT] > 300)
    track = first_track(climb_out)
    if not climbing or track is None:
        return None
    return airports.along(first[LAT], first[LON], track, 10, airliner, first[ALT], behind=True, per_nm=700)


def destination_of(points, landing, airports, airliner):
    last = points[-1]
    if landing is not None and dist_nm(landing[LAT], landing[LON], last[LAT], last[LON]) < 5:
        return airports.near(landing[LAT], landing[LON], 6, airliner)
    if last[ALT] is None:
        return None
    # Last heard coming down toward a field: descending over the last few minutes, low enough to be landing there.
    recent = [s for s in points if last[T] - s[T] < 240 and s[ALT] is not None]
    descending = (last[VRATE] is not None and last[VRATE] < -300) or (len(recent) > 1 and recent[0][ALT] - last[ALT] > 500)
    track = first_track(points[-5:], newest=True)
    if not descending or track is None:
        return None
    return airports.along(last[LAT], last[LON], track, 10, airliner, last[ALT], behind=False, per_nm=400)


def leg_callsign(points):
    """Every callsign heard, counted: the most common one wins, so a few minutes of last flight's number don't."""
    return Counter(s[CALLSIGN] for s in points if s[CALLSIGN])


def airliner_of(points):
    categories = Counter(s[CATEGORY] for s in points if s[CATEGORY])
    category = categories.most_common(1)[0][0] if categories else None
    return category in ("A3", "A4", "A5")


# ---------------------------------------------------------------- one day of history

class NoArchive(SystemExit):
    """adsb.lol hasn't posted that day yet (or never will)."""


def release_urls(day):
    """Prod unless it's missing or much smaller than staging (adsb.lol's own advice)."""
    sizes = {}
    for pod in ("prod", "staging"):
        tag = f"v{day:%Y.%m.%d}-planes-readsb-{pod}-0"
        request = urllib.request.Request(
            f"https://api.github.com/repos/adsblol/globe_history_{day.year}/releases/tags/{tag}", headers=UA)
        if os.environ.get("GITHUB_TOKEN"):
            request.add_header("Authorization", f"Bearer {os.environ['GITHUB_TOKEN']}")
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                assets = json.load(response)["assets"]
        except Exception as error:  # noqa: BLE001 — a missing pod is normal
            log(f"{tag}: {error}")
            continue
        parts = sorted((a for a in assets if ".tar" in a["name"]), key=lambda a: a["name"])
        if parts:
            sizes[pod] = (sum(a["size"] for a in parts), [a["browser_download_url"] for a in parts])
    if not sizes:
        raise NoArchive(f"no archive for {day}")
    pod = "prod" if "prod" in sizes and sizes["prod"][0] >= 0.7 * sizes.get("staging", (0,))[0] else "staging"
    log(f"{day}: {pod}, {sizes[pod][0] / 1e9:.2f} GB in {len(sizes[pod][1])} parts")
    return sizes[pod][1]


def load_allowlist():
    with open(os.path.join(ROOT, "airlines-allow.txt"), encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def process_day(day, source=None):
    airports = Airports(os.path.join(ROOT, "airports.tsv"))
    allow = load_allowlist()
    allow_bytes = {code.encode() for code in allow}
    open_before = {}
    previous_open = day_path("open", day - timedelta(days=1), "json.gz")
    if os.path.exists(previous_open):
        with gzip.open(previous_open, "rt") as f:
            open_before = json.load(f)

    if source:
        stream, process = open(source, "rb"), None
    else:
        process = subprocess.Popen(["curl", "-sfL", *release_urls(day)], stdout=subprocess.PIPE, bufsize=1 << 20)
        stream = process.stdout

    end_of_day = datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp() + DAY
    counts = Counter()
    rows, still_open = [], {}
    started = time.time()
    with tarfile.open(fileobj=stream, mode="r|") as tar:
        for member in tar:
            if not member.isfile() or "/traces/" not in member.name:
                continue
            counts["planes"] += 1
            if counts["planes"] % 50000 == 0:
                log(f"  {counts['planes']:,} planes, {len(rows):,} flights, {time.time() - started:.0f}s, peak {peak_mb():.0f} MB")
            raw = tar.extractfile(member).read()
            data = gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw
            if not allow_bytes.intersection(FLIGHT_BYTES_RE.findall(data)):
                continue
            trace = json.loads(data)
            if trace.get("dbFlags", 0) & 1:  # military
                counts["military"] += 1
                continue
            counts["airline planes"] += 1
            hex_id = trace["icao"]
            samples = samples_of(trace)
            if not samples:
                continue
            legs = split_legs(samples)
            carried = open_before.get(hex_id)
            for index, (points, departure, landing, bridged) in enumerate(legs):
                airliner = airliner_of(points)
                names = leg_callsign(points)
                origin, takeoff = None, None
                if index == 0 and carried and departure is None:
                    first = points[0]
                    gap = first[T] - carried["t"]
                    reach = max(carried.get("gs") or 250, 250) * gap / 3600 * 1.5 + 30
                    if 0 <= gap < 1800 and dist_nm(carried["lat"], carried["lon"], first[LAT], first[LON]) < reach:
                        origin, takeoff = carried["origin"], carried["takeoff"]
                        bridged = bridged or carried.get("bridged", False)
                        names.update(carried["callsigns"])
                        counts["carried over midnight"] += 1
                if origin is None:
                    origin = origin_of(points, departure, airports, airliner)
                    takeoff = int(points[0][T]) if origin else None
                if not names:
                    counts["no callsign"] += 1
                    continue
                callsign = standing_data_callsign(names.most_common(1)[0][0])
                if not CALLSIGN_RE.match(callsign) or callsign[:3] not in allow:
                    counts["not an airline"] += 1
                    continue
                last = points[-1]
                if landing is None and last[T] > end_of_day - 900 and origin:
                    still_open[hex_id] = {"origin": origin, "takeoff": takeoff, "callsigns": dict(names), "bridged": bridged,
                                          "t": last[T], "lat": last[LAT], "lon": last[LON], "gs": last[GS]}
                    counts["open at midnight"] += 1
                    continue
                if not origin:
                    counts["takeoff unseen"] += 1
                    continue
                destination = destination_of(points, landing, airports, airliner)
                if not destination:
                    counts["landing unseen"] += 1
                    continue
                if destination == origin:
                    counts["same field"] += 1
                    continue
                landed = int(landing[T]) if landing else int(last[T])
                rows.append((hex_id, callsign, origin, destination, takeoff, landed, int(bridged)))
                counts["flights across an ocean gap"] += int(bridged)
                counts["flights"] += 1
    if process:
        process.wait()
        if process.returncode:
            raise SystemExit(f"download failed (curl exit {process.returncode}); nothing written")

    os.makedirs(os.path.join(WORK, "legs"), exist_ok=True)
    os.makedirs(os.path.join(WORK, "open"), exist_ok=True)
    temp = day_path("legs", day, "csv.gz.tmp")
    with gzip.open(temp, "wt", newline="") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(["hex", "callsign", "origin", "destination", "takeoff", "landing", "bridged"])
        writer.writerows(rows)
    os.replace(temp, day_path("legs", day, "csv.gz"))
    with gzip.open(day_path("open", day, "json.gz"), "wt") as f:
        json.dump(still_open, f)
    elapsed = time.time() - started
    summary = {"day": day.isoformat(), "seconds": round(elapsed), "peak_mb": round(peak_mb()), **counts}
    with open(day_path("legs", day, "json"), "w") as f:
        json.dump(summary, f, indent=1)
    log(json.dumps(summary, indent=1))


# ---------------------------------------------------------------- the table

def read_legs(day):
    path = day_path("legs", day, "csv.gz")
    if not os.path.exists(path):
        return None
    with gzip.open(path, "rt", newline="") as f:
        return list(csv.DictReader(f))


def median_minute(minutes):
    """Median time of day that copes with a schedule straddling midnight UTC."""
    minutes = sorted(minutes)
    if minutes[-1] - minutes[0] > 720:
        minutes = sorted(m + 1440 if m < 720 else m for m in minutes)
    return minutes[len(minutes) // 2] % 1440


def build_table(days):
    """(callsign, origin) → {destination: {days, minutes, last}} from the legs of `days`."""
    table = defaultdict(dict)
    missing = []
    for day in days:
        legs = read_legs(day)
        if legs is None:
            missing.append(day.isoformat())
            continue
        for leg in legs:
            entry = table[(leg["callsign"], leg["origin"])].setdefault(
                leg["destination"], {"days": set(), "minutes": [], "last": day, "only_bridged": True})
            entry["only_bridged"] = entry["only_bridged"] and leg.get("bridged") == "1"
            entry["days"].add(day)
            entry["last"] = max(entry["last"], day)
            t = datetime.fromtimestamp(int(leg["takeoff"]), timezone.utc)
            entry["minutes"].append(t.hour * 60 + t.minute)
    if missing:
        log(f"no flights for {', '.join(missing)}")
    return table


def write_table(day, out=ROOT, caution=None):
    table = build_table([day - timedelta(days=n) for n in range(WINDOW)])
    by_airline = defaultdict(list)
    for (callsign, origin), destinations in table.items():
        for destination, e in destinations.items():
            minute = median_minute(e["minutes"])
            by_airline[callsign[:3]].append((callsign, origin, destination, len(e["days"]), e["last"].isoformat(),
                                             f"{minute // 60:02d}:{minute % 60:02d}"))
    routes = os.path.join(out, "routes")
    for dirpath, _, files in os.walk(routes):
        for name in files:
            os.remove(os.path.join(dirpath, name))
    for airline, rows in by_airline.items():
        rows.sort(key=lambda r: (r[0], r[1], -r[3], r[2]))
        folder = os.path.join(routes, airline[0])
        os.makedirs(folder, exist_ok=True)
        with open(os.path.join(folder, f"{airline}.csv"), "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f, lineterminator="\n")
            writer.writerow(["callsign", "origin", "destination", "days_seen", "last_seen", "takeoff_utc"])
            writer.writerows(rows)
    total = sum(len(r) for r in by_airline.values())
    write_json(os.path.join(out, "latest.json"), {"date": day.isoformat(), "commit": None, "caution": caution})
    log(f"{total:,} rows for {len(by_airline):,} airlines")


def last_sunday(year, month):
    d = date(year, month + 1, 1) - timedelta(days=1)
    return d - timedelta(days=(d.weekday() + 1) % 7)


def caution_for(day):
    """Airlines switch schedule seasons on the last Sunday of March and October; old rows age out over a week.
    `day` is the table's date: phones use it the day after."""
    for change in (last_sunday(day.year, 3), last_sunday(day.year, 10)):
        if change <= day + timedelta(days=1) < change + timedelta(days=WINDOW + 1):
            return {"reason": "schedule-change", "since": change.isoformat(),
                    "until": (change + timedelta(days=WINDOW)).isoformat()}
    return None


def accuracy_drop(day):
    """Last night's wrong answers well above their recent run: an airline reshuffled its numbers outside the season
    dates (Southwest does). Needs five earlier grades to compare against."""
    def wrong_rate(d):
        g = read_json(day_path("grades", d, "json"))
        return g["wrong (most days)"] / g["answered"] if g and g.get("answered") else None
    now = wrong_rate(day)
    history = sorted(r for r in (wrong_rate(day - timedelta(days=n)) for n in range(1, 15)) if r is not None)
    if now is None or len(history) < 5:
        return None
    typical = history[len(history) // 2]
    if now <= max(2 * typical, typical + 0.015):
        return None
    return {"reason": "accuracy-drop", "since": (day + timedelta(days=1)).isoformat(),
            "until": (day + timedelta(days=2)).isoformat()}


# ---------------------------------------------------------------- grading

def grade(day):
    """How the table of the 7 days before `day` did on `day`'s real flights, picking each key's most-seen row."""
    days = [day - timedelta(days=n) for n in range(1, WINDOW + 1)]
    legs = read_legs(day)
    if legs is None or sum(os.path.exists(day_path("legs", d, "csv.gz")) for d in days) < 3:
        log(f"too few days to grade {day}")
        return None
    table = build_table(days)
    tally = Counter()
    per_airline = defaultdict(Counter)
    wrong_examples = []
    for leg in legs:
        key = (leg["callsign"], leg["origin"])
        airline = per_airline[leg["callsign"][:3]]
        tally["flights"] += 1
        airline["flights"] += 1
        crossing = leg.get("bridged") == "1"
        if crossing:
            tally["ocean crossings"] += 1
        destinations = table.get(key)
        if not destinations:
            tally["missing"] += 1
            tally["ocean crossings: missing"] += crossing
            airline["missing"] += 1
            continue
        truth = leg["destination"]
        most = max(destinations.items(), key=lambda kv: (len(kv[1]["days"]), kv[1]["last"]))
        newest = max(destinations.items(), key=lambda kv: (kv[1]["last"], len(kv[1]["days"])))
        sure = len(most[1]["days"]) >= 2
        tally["answered"] += 1
        tally["one row" if len(destinations) == 1 else "several rows"] += 1
        tally["right (most days)" if most[0] == truth else "wrong (most days)"] += 1
        tally["right (newest)" if newest[0] == truth else "wrong (newest)"] += 1
        tally["right row among them" if truth in destinations else "right row absent"] += 1
        tally[("seen 2+ days: " if sure else "seen once: ") + ("right" if most[0] == truth else "wrong")] += 1
        if crossing:
            tally["ocean crossings: " + ("right" if most[0] == truth else "wrong")] += 1
        airline["answered"] += 1
        airline["right" if most[0] == truth else "wrong"] += 1
        if most[0] != truth and sure and len(wrong_examples) < 25:
            wrong_examples.append(f"{leg['callsign']} {leg['origin']}→{truth}, table: " + ", ".join(
                f"{d} ({len(e['days'])}d, last {e['last']:%m-%d})" for d, e in destinations.items()))
    top = sorted(per_airline.items(), key=lambda kv: -kv[1]["flights"])[:15]
    tally["table rows"] = sum(len(d) for d in table.values())
    tally["table rows only from ocean crossings"] = sum(
        1 for d in table.values() for e in d.values() if e["only_bridged"])
    result = {"day": day.isoformat(), **tally,
              "airlines": {code: dict(c) for code, c in top}, "wrong_examples": wrong_examples}
    os.makedirs(os.path.join(WORK, "grades"), exist_ok=True)
    write_json(day_path("grades", day, "json"), result)
    return result


# ---------------------------------------------------------------- publishing (the Pi, nightly)

REPO = "GumDropSoftware/overhead-routes"


def read_json(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def write_json(path, value):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(value, f, indent=1)
        f.write("\n")


def git(*args):
    return subprocess.run(["git", "-C", ROOT, *args], check=True, capture_output=True, text=True).stdout.strip()


def push():
    git("push", "-q", "origin", "HEAD:main")
    # latest.json is the one file phones read at @main; everything else they read at the commit it names.
    try:
        purge = urllib.request.Request(f"https://purge.jsdelivr.net/gh/{REPO}@main/latest.json", headers=UA)
        urllib.request.urlopen(purge, timeout=30).read()
    except Exception as error:  # noqa: BLE001 — jsDelivr refreshes @main within 12 h by itself
        log(f"jsDelivr purge failed: {error}")


def publish(day):
    """Two commits: the night's routes, then latest.json naming that commit."""
    git("add", "-A", "routes", "stats.json", "airlines-allow.txt")
    if git("diff", "--cached", "--name-only"):
        git("commit", "-q", "-m", f"Routes for {day}")
    path = os.path.join(ROOT, "latest.json")
    latest = read_json(path)
    latest["commit"] = git("rev-parse", "HEAD")
    write_json(path, latest)
    git("add", "latest.json")
    git("commit", "-q", "-m", f"latest.json: {day}")
    push()
    log(f"published {day} at {latest['commit'][:7]}")


def prune(day, keep_days=30):
    for kind in ("legs", "open"):
        folder = os.path.join(WORK, kind)
        for name in os.listdir(folder) if os.path.isdir(folder) else ():
            try:
                if date.fromisoformat(name[:10]) < day - timedelta(days=keep_days):
                    os.remove(os.path.join(folder, name))
            except ValueError:
                continue


def nightly(day=None):
    """What the Pi runs: publish yesterday's table (adsb.lol posts a day's archive around 03:30 UTC the next morning).
    Catches up on days of the last week it missed, and is safe to run again: a published day is left alone."""
    day = day or datetime.now(timezone.utc).date() - timedelta(days=1)
    git("fetch", "-q", "origin")
    if int(git("rev-list", "--count", "origin/main..HEAD") or 0):
        push()  # last run committed but couldn't push
    latest = read_json(os.path.join(ROOT, "latest.json")) or {}
    if latest.get("date") == day.isoformat() and latest.get("commit"):
        log(f"{day} is already published")
        return
    for back in range(WINDOW, -1, -1):
        d = day - timedelta(days=back)
        if os.path.exists(day_path("legs", d, "csv.gz")):
            continue
        try:
            process_day(d)
        except NoArchive:
            if d == day:
                raise
            log(f"{d}: no archive, skipped")
    if time.time() - os.path.getmtime(os.path.join(ROOT, "airlines-allow.txt")) > 7 * DAY:
        refresh_airlines()
    result = grade(day)
    if result:
        write_json(os.path.join(ROOT, "stats.json"), {
            "date": day.isoformat(), "flights": result.get("flights", 0), "answered": result.get("answered", 0),
            "right": result.get("right (most days)", 0), "wrong": result.get("wrong (most days)", 0),
            "missing": result.get("missing", 0)})
    write_table(day, caution=caution_for(day) or accuracy_drop(day))
    publish(day)
    prune(day)


# ---------------------------------------------------------------- airlines

def refresh_airlines():
    """Scheduled airlines and cargo: the operators VRS's standing data lists routes for. Leaves out business-jet
    operators (NetJets EJA, Flexjet LXJ, VistaJet VJT) and military (RCH), so no private trips get published."""
    request = urllib.request.Request(
        "https://data.jsdelivr.com/v1/packages/gh/vradarserver/standing-data@main?structure=flat", headers=UA)
    with urllib.request.urlopen(request, timeout=60) as response:
        files = json.load(response)["files"]
    codes = sorted({m.group(1) for f in files
                    if (m := re.match(r"/routes/schema-01/./([A-Z0-9]{3})-", f["name"]))})
    with open(os.path.join(ROOT, "airlines-allow.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(codes) + "\n")
    log(f"{len(codes)} airlines")


def save_traces(day, out):
    """The day's trace_full files (gzipped, as adsb.lol serves them) for every plane in that day's flights, from the
    GitHub archive: lab replays read these instead of asking adsb.lol for each plane."""
    wanted = {leg["hex"] for leg in read_legs(day) or []}
    process = subprocess.Popen(["curl", "-sfL", *release_urls(day)], stdout=subprocess.PIPE, bufsize=1 << 20)
    saved = 0
    with tarfile.open(fileobj=process.stdout, mode="r|") as tar:
        for member in tar:
            if not member.isfile() or "/traces/" not in member.name:
                continue
            hex_id = member.name.rsplit("trace_full_", 1)[-1].split(".")[0]
            if hex_id not in wanted:
                continue
            raw = tar.extractfile(member).read()
            folder = os.path.join(out, hex_id[-2:])
            os.makedirs(folder, exist_ok=True)
            with open(os.path.join(folder, f"trace_full_{hex_id}.json"), "wb") as f:
                f.write(raw if raw[:2] == b"\x1f\x8b" else gzip.compress(raw))
            saved += 1
    process.wait()
    if process.returncode:
        raise SystemExit(f"download failed (curl exit {process.returncode})")
    log(f"{saved:,} of {len(wanted):,} planes saved to {out}")


def main(argv):
    if len(argv) >= 2 and argv[1] == "airlines":
        return refresh_airlines()
    if len(argv) >= 2 and argv[1] == "nightly":
        return nightly(date.fromisoformat(argv[2]) if len(argv) > 2 else None)
    if len(argv) < 3:
        raise SystemExit(__doc__)
    command, day = argv[1], date.fromisoformat(argv[2])
    if command == "day":
        process_day(day, source=argv[3] if len(argv) > 3 else None)
    elif command == "table":
        write_table(day, out=argv[3] if len(argv) > 3 else ROOT, caution=caution_for(day))
    elif command == "traces" and len(argv) > 3:
        save_traces(day, argv[3])
    elif command == "grade":
        print(json.dumps(grade(day), indent=1))
    else:
        raise SystemExit(__doc__)


if __name__ == "__main__":
    main(sys.argv)
