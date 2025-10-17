import hashlib
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import recurring_ical_events
import requests
from dateutil import parser as dtparser
from dateutil import tz
from icalendar import Calendar, Event, Timezone

DEFAULT_TZID = "Europe/Paris"


def load_ics(source: str, timeout: int = 20) -> bytes:
    if source.lower().startswith(("http://", "https://")):
        r = requests.get(source, timeout=timeout)
        r.raise_for_status()
        return r.content
    with open(source, "rb") as f:
        return f.read()


def ensure_timezone(dt, default_tz):
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=default_tz)
        return dt
    if isinstance(dt, date):
        return datetime(dt.year, dt.month, dt.day, tzinfo=default_tz)
    return dt


def normalize_to_tz(dt: datetime, target_tz):
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(target_tz)


def expand_events(cal: Calendar, start: datetime, end: datetime):
    return recurring_ical_events.of(cal).between(start, end)


def serialize_event(evt: Event, target_tz, default_tz) -> Dict[str, Any]:
    def g(name):
        return evt.get(name)

    uid = str(g("UID") or "")
    summary = str(g("SUMMARY") or "")
    description = str(g("DESCRIPTION") or "")
    location = str(g("LOCATION") or "")
    organizer = str(g("ORGANIZER") or "")

    dtstart = g("DTSTART").dt if g("DTSTART") else None
    dtend = g("DTEND").dt if g("DTEND") else None
    all_day = False

    if dtstart is not None:
        if not isinstance(dtstart, datetime):
            all_day = True
            dtstart = ensure_timezone(dtstart, target_tz)
        else:
            dtstart = ensure_timezone(dtstart, default_tz)
        dtstart = normalize_to_tz(dtstart, target_tz)

    if dtend is not None:
        if not isinstance(dtend, datetime):
            all_day = True
            dtend = ensure_timezone(dtend, target_tz)
        else:
            dtend = ensure_timezone(dtend, default_tz)
        dtend = normalize_to_tz(dtend, target_tz)

    if dtstart and not dtend:
        duration = g("DURATION")
        if duration:
            dtend = dtstart + duration.dt
        else:
            dtend = dtstart + (timedelta(days=1) if all_day else timedelta(minutes=0))

    transparency = str(g("TRANSP") or "")
    status = str(g("STATUS") or "")
    categories = g("CATEGORIES")
    if isinstance(categories, list):
        categories = [str(c) for c in categories]
    elif categories is not None:
        categories = [str(categories)]

    return {
        "uid": uid,
        "summary": summary,
        "description": description,
        "location": location,
        "organizer": organizer,
        "start": dtstart.isoformat() if dtstart else None,
        "end": dtend.isoformat() if dtend else None,
        "all_day": all_day,
        "status": status,
        "transparency": transparency,
        "categories": categories,
        "raw_class": str(g("CLASS") or ""),
    }


def _build_vtimezone(tzid_str: str) -> Timezone:
    tz_comp = Timezone()
    tz_comp.add("tzid", tzid_str)
    return tz_comp


def _dt_for_strategy(dt: datetime, all_day: bool, strategy: str):
    if all_day:
        return date(dt.year, dt.month, dt.day)
    if strategy == "utc":
        return dt.astimezone(timezone.utc)
    elif strategy == "floating":
        return dt.replace(tzinfo=None)
    else:  # tzid
        return dt


def events_to_ics(
    events: List[Dict[str, Any]],
    calendar_name: str,
    tzid_str: str,
    tz_strategy: str = "tzid",
) -> bytes:
    # Deterministic ordering
    def parse_dt(iso):
        try:
            return dtparser.isoparse(iso) if iso else None
        except Exception:
            return None

    events_sorted = sorted(
        events,
        key=lambda e: (
            parse_dt(e.get("start")) or datetime.min.replace(tzinfo=timezone.utc),
            e.get("uid") or "",
        ),
    )

    cal = Calendar()
    cal.add("prodid", "-//ICS Normalizer//v1//EN")
    cal.add("version", "2.0")
    cal.add("calscale", "GREGORIAN")
    cal.add("x-wr-calname", calendar_name)
    cal.add("x-wr-timezone", tzid_str)

    if tz_strategy == "tzid":
        cal.add_component(_build_vtimezone(tzid_str))

    target_tz = tz.gettz(tzid_str)

    for e in events_sorted:
        ve = Event()
        # Fixed property add order for determinism
        ve.add("uid", e.get("uid") or None)
        ve.add("summary", e.get("summary") or "")

        if e.get("location"):
            ve.add("location", e["location"])
        if e.get("description"):
            ve.add("description", e["description"])

        start_iso = e.get("start")
        end_iso = e.get("end")
        if not start_iso:
            continue
        start_dt = dtparser.isoparse(start_iso)
        end_dt = dtparser.isoparse(end_iso) if end_iso else None

        if start_dt.tzinfo is None:
            start_dt = start_dt.replace(tzinfo=target_tz)
        else:
            start_dt = start_dt.astimezone(target_tz)
        if end_dt:
            if end_dt.tzinfo is None:
                end_dt = end_dt.replace(tzinfo=target_tz)
            else:
                end_dt = end_dt.astimezone(target_tz)

        all_day = bool(e.get("all_day"))
        start_val = _dt_for_strategy(start_dt, all_day, tz_strategy)
        end_val = _dt_for_strategy(end_dt or start_dt, all_day, tz_strategy)

        if tz_strategy == "tzid" and not all_day:
            ve.add("dtstart", start_val, parameters={"TZID": tzid_str})
            ve.add("dtend", end_val, parameters={"TZID": tzid_str})
        else:
            ve.add("dtstart", start_val)
            ve.add("dtend", end_val)

        if e.get("status"):
            ve.add("status", e["status"])
        if e.get("transparency"):
            ve.add("transp", e["transparency"])
        if e.get("categories"):
            ve.add("categories", e["categories"])

        cal.add_component(ve)

    return cal.to_ical()


def normalize_upstream_to_ics(
    source_url: str,
    start: Optional[datetime],
    end: Optional[datetime],
    tzid: str = DEFAULT_TZID,
    default_tzid: str = "UTC",
    tz_strategy: str = "tzid",
) -> bytes:
    ics_bytes = load_ics(source_url)
    cal = Calendar.from_ical(ics_bytes)

    now = datetime.now(timezone.utc)
    if start is None:
        start = now - timedelta(days=7)
    if end is None:
        end = now + timedelta(days=90)

    target_tz = tz.gettz(tzid)
    default_tz = tz.gettz(default_tzid)

    instances = expand_events(cal, start, end)
    rows = [serialize_event(e, target_tz, default_tz) for e in instances]

    return events_to_ics(rows, "Normalized Calendar", tzid, tz_strategy)


def compute_etag(data: bytes) -> str:
    # Strong ETag = SHA-256 of bytes
    h = hashlib.sha256(data).hexdigest()
    return f'"{h}"'
