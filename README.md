# Overhead route list

Where airline flights actually went, rebuilt every night from public flight history. The
[Overhead](https://ota-drop.vercel.app/apps/overhead/) app uses it to say where a flight is headed.

Each night a small job reads the previous day's full flight history that [adsb.lol](https://adsb.lol) publishes on
GitHub, finds every airline flight whose takeoff and landing were both heard, and keeps the last 7 days. For a flight
number leaving a given airport, the table says where it went.

## Files

- **`latest.json`**: `{"date", "commit", "caution"}`. Read it at `@main`, then read everything else at `@<commit>`,
  so every airline comes from the same night.
- **`routes/{first letter}/{ICAO}.csv`**: one file per airline, for example `routes/D/DAL.csv`:

  ```
  callsign,origin,destination,days_seen,last_seen,takeoff_utc
  DAL2735,KATL,KBDL,7,2026-10-05,19:55
  ```

  - `callsign`: in VRS standing data's form, with the number's leading zeros dropped (KAL081 → KAL81).
  - `origin`, `destination`: codes from `airports.tsv` (ICAO where an airport has one).
  - `days_seen`: how many of the last 7 UTC days it flew this. `1` means seen once.
  - `last_seen`: `YYYY-MM-DD`, UTC.
  - `takeoff_utc`: median takeoff time, `HH:MM` UTC.
  - One row per destination. A number that went to two places this week has two rows.
- **`stats.json`**: how the previous night's table did against the day's real flights (`flights`, `answered`, `right`,
  `wrong`, `missing`).
- **`caution`** in `latest.json`: `null`, or a reason the table may be less reliable for a few days.
  - `schedule-change`: from the last Sunday of March or October (airlines' season changes), for a week.
  - `accuracy-drop`: the nightly grade came out well below normal.

Through jsDelivr:

```
https://cdn.jsdelivr.net/gh/GumDropSoftware/overhead-routes@main/latest.json
https://cdn.jsdelivr.net/gh/GumDropSoftware/overhead-routes@<commit>/routes/D/DAL.csv
```

## What's in it, and what isn't

- **Airlines only.** Scheduled airlines and cargo carriers, meaning the operators
  [VRS standing data](https://github.com/vradarserver/standing-data) lists routes for. No business-jet operators, no
  private flights, no military.
- **Only flights with both ends heard.** Coverage is good across North America and Europe and thinner elsewhere.
  Ocean crossings are included when both ends are heard.
- **It learns a day late.** A flight number flown somewhere new today isn't in it until tomorrow, and a diversion
  shows up as a one-day row.
- **Measured:** we graded the table of Sept 28–Oct 4, 2026 against Oct 5's 52,584 airline flights. It had a row for
  90.6% of them, and its most-seen row was right for 98.5% of those (99.0% for rows seen on 2+ days).

## Building it

```
python3 overhead_routes.py nightly
```

It needs only the Python standard library and `curl`. It streams the day's ~4 GB archive from GitHub without saving
it, uses about 150 MB of memory, and takes a few minutes. Other commands are `day`, `table`, `grade`, `airlines` and
`traces`; run it with no arguments to list them. One machine publishes, and only that one.

## Licenses

- **Data** (`routes/`, `latest.json`, `stats.json`): [Open Database License 1.0](LICENSE-ODbL.txt). Contains information
  from [adsb.lol](https://adsb.lol), which is made available under the ODbL. It's built from adsb.lol's daily history
  ([globe_history_2026](https://github.com/adsblol/globe_history_2026)), which includes data from adsb.lol's feeders
  and partners.
- **`airports.tsv`**: from [OurAirports](https://ourairports.com/data/) (public domain).
- **`airlines-allow.txt`**: operator codes from VRS standing data (CC0).
- **Code**: [MIT](LICENSE).

Questions: support@gumdrop.space
