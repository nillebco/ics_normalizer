import os
from datetime import timezone
from typing import Optional

from dateutil import parser as dtparser
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import PlainTextResponse

from ics_normalizer import compute_etag, normalize_upstream_to_ics

load_dotenv()


class CalendarResponse(PlainTextResponse):
    media_type = "text/calendar; charset=UTF-8"


# Disable OpenAPI docs unless environment is explicitly "dev"
docs_url = "/docs" if os.getenv("ENVIRONMENT") == "dev" else None
redoc_url = "/redoc" if os.getenv("ENVIRONMENT") == "dev" else None
openapi_url = "/openapi.json" if os.getenv("ENVIRONMENT") == "dev" else None

app = FastAPI(
    title="ICS Normalizer Service",
    docs_url=docs_url,
    redoc_url=redoc_url,
    openapi_url=openapi_url,
)


@app.get("/calendar.ics", response_class=CalendarResponse)
async def calendar_ics(
    request: Request,
    source: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    tzid: str = "Europe/Paris",
    default_tzid: str = "UTC",
    tz_strategy: str = "tzid",  # tzid | utc | floating
):
    """Generate a normalized ICS from an upstream feed on-the-fly.

    Query params:
      - source: upstream ICS URL (https/http)
      - start/end: ISO datetimes (optional); default: now-7d .. now+90d (UTC)
      - tzid: target timezone TZID for output (default Europe/Paris)
      - default_tzid: assumed tz for naive source times
      - tz_strategy: tzid | utc | floating
    """
    try:
        start_dt = dtparser.isoparse(start).astimezone(timezone.utc) if start else None
        end_dt = dtparser.isoparse(end).astimezone(timezone.utc) if end else None
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid start/end: {e}")

    try:
        ical_bytes = normalize_upstream_to_ics(
            source_url=source,
            start=start_dt,
            end=end_dt,
            tzid=tzid,
            default_tzid=default_tzid,
            tz_strategy=tz_strategy,
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Upstream/normalize error: {e}")

    etag = compute_etag(ical_bytes)
    inm = request.headers.get("If-None-Match")
    if inm and inm.strip() == etag:
        return Response(status_code=304)

    headers = {
        "ETag": etag,
        "Cache-Control": "public, max-age=3600",
    }
    return CalendarResponse(content=ical_bytes, headers=headers)


@app.get("/")
def root():
    return {"ok": True, "msg": "Use /calendar.ics?source=<url>"}
