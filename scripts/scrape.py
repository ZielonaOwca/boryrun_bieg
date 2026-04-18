"""
Scraper danych BoryRun 2026 -> data.json

Źródła:
  - dorośli:  https://www.zmierzymyczas.pl/2404/boryrun-2026.html
              paginacja ?start=N (100 rekordów/stronę), SSR tabela
  - dzieci:   https://www.boryrun.pl/rejestracja
              Next.js Server Action getAvailableSpots
              (ID akcji zmienia się przy redeployu - fallback parsuje chunki JS)

Uruchamiane z GitHub Actions co 15 min. Nadpisuje data.json w repo.
"""

from __future__ import annotations

import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

ADULTS_URL = "https://www.zmierzymyczas.pl/2404/boryrun-2026.html"
ADULTS_SUMMARY_URL = "https://www.zmierzymyczas.pl/"
ADULTS_DETAIL_PATH = "/2404/boryrun-2026.html"
KIDS_URL = "https://www.boryrun.pl/rejestracja"

ADULTS_LIMIT = 700
KIDS_LIMIT = 300
PAGE_SIZE = 100
MAX_PAGES = 30  # twardy bezpiecznik (3000 zapisanych)

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

OUTPUT_PATH = Path(__file__).resolve().parent.parent / "data.json"


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept-Language": "pl,en;q=0.8"})
    return s


def _get_with_retry(session: requests.Session, url: str, **kwargs: Any) -> requests.Response:
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            r = session.get(url, timeout=20, **kwargs)
            if r.status_code == 429 or 500 <= r.status_code < 600:
                raise requests.HTTPError(f"HTTP {r.status_code}")
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            last_exc = e
            time.sleep(2 ** attempt)
    raise RuntimeError(f"GET {url} failed: {last_exc}")


_PAID_TOKENS = re.compile(r"\b(yes|paid|ok)\b")


def _parse_paid(td) -> bool:
    """Wiersz 'Zapłacono' ma img z alt='Zapłacono' lub src typu yes.png."""
    if td is None:
        return False
    img = td.find("img")
    if img is None:
        return "zap" in td.get_text(strip=True).lower()
    alt = (img.get("alt") or "").strip().lower()
    src = (img.get("src") or "").lower()
    filename = src.rsplit("/", 1)[-1]
    if "zap" in alt or _PAID_TOKENS.search(filename):
        return True
    return False


def _cell(tr, class_name: str) -> str:
    td = tr.find("td", class_=class_name)
    return td.get_text(strip=True) if td else ""


def _scrape_adults_summary(session: requests.Session) -> dict[str, int]:
    """Pobiera total/paid/limit z tabeli zawodów na zmierzymyczas.pl.

    Podstrona imprezy listuje TYLKO opłaconych, więc prawdziwe liczby
    (w tym nieopłaceni) są tylko tutaj.
    """
    resp = _get_with_retry(session, ADULTS_SUMMARY_URL)
    soup = BeautifulSoup(resp.text, "html.parser")
    link = soup.select_one(f'a[href*="{ADULTS_DETAIL_PATH}"]')
    if link is None:
        raise RuntimeError("Nie znaleziono linku BoryRun 2026 na stronie głównej")
    tr = link.find_parent("tr")
    if tr is None:
        raise RuntimeError("Link BoryRun 2026 nie jest w wierszu tabeli")
    tds = tr.find_all("td")
    if len(tds) < 3:
        raise RuntimeError(f"Wiersz BoryRun 2026 ma za mało kolumn ({len(tds)})")

    def _int(td) -> int:
        digits = re.sub(r"\D", "", td.get_text())
        return int(digits) if digits else 0

    # Ostatnie 3 kolumny: zapisanych | opłaconych | limit
    total = _int(tds[-3])
    paid = _int(tds[-2])
    limit = _int(tds[-1]) or ADULTS_LIMIT
    return {"total": total, "paid": paid, "limit": limit}


def _scrape_adults_participants(session: requests.Session) -> list[dict[str, Any]]:
    participants: list[dict[str, Any]] = []

    for page in range(MAX_PAGES):
        start = page * PAGE_SIZE
        url = f"{ADULTS_URL}?start={start}"
        resp = _get_with_retry(session, url)
        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.select_one("table#zapisy_impreza_tabela")
        if table is None:
            if page == 0:
                raise RuntimeError("Brak tabeli #zapisy_impreza_tabela na stronie dorosłych")
            break

        rows = table.select("tbody > tr")
        if not rows:
            break

        for tr in rows:
            p = {
                "imie": _cell(tr, "zapisy_impreza_imie"),
                "nazwisko": _cell(tr, "zapisy_impreza_nazwisko"),
                "plec": _cell(tr, "zapisy_impreza_plec"),
                "kategoria": _cell(tr, "zapisy_impreza_kat"),
                "miejscowosc": _cell(tr, "zapisy_impreza_miejscowosc"),
                "klub": _cell(tr, "zapisy_impreza_klub"),
                "oplacone": _parse_paid(tr.find("td", class_="zapisy_impreza_platn")),
            }
            if not (p["imie"] or p["nazwisko"]):
                continue
            participants.append(p)

        if len(rows) < PAGE_SIZE:
            break

        time.sleep(1.2)

    return participants


def scrape_adults(session: requests.Session) -> dict[str, Any]:
    participants = _scrape_adults_participants(session)
    try:
        summary = _scrape_adults_summary(session)
    except Exception as e:
        # Awaryjnie: policz z listy (pokaże 0 nieopłaconych, ale nie wywali całości).
        print(f"[adults] WARN: brak sumarycznej -> licznik z listy. {e}", file=sys.stderr)
        summary = {
            "total": len(participants),
            "paid": sum(1 for p in participants if p["oplacone"]),
            "limit": ADULTS_LIMIT,
        }
    return {
        "total": summary["total"],
        "paid": summary["paid"],
        "limit": summary["limit"],
        "participants": participants,
    }


def _find_action_id(session: requests.Session) -> str:
    """Fallback: pobierz /rejestracja, przeparsuj chunki JS, znajdź aktualne
    createServerReference('<hex>'...'getAvailableSpots'...).
    """
    page = _get_with_retry(session, KIDS_URL).text
    chunk_refs = re.findall(r'/_next/static/chunks/[^"\']+\.js', page)
    seen: set[str] = set()
    for ref in chunk_refs[:20]:
        if ref in seen:
            continue
        seen.add(ref)
        chunk_url = urljoin(KIDS_URL, ref)
        try:
            js = _get_with_retry(session, chunk_url).text
        except Exception:
            continue
        if "getAvailableSpots" not in js:
            continue
        # createServerReference("<hex>",...,"getAvailableSpots")
        m = re.search(
            r'createServerReference\("([a-f0-9]{32,})"[^)]*?"getAvailableSpots"',
            js,
        )
        if m:
            return m.group(1)
        # fallback - hex w pobliżu nazwy akcji
        m = re.search(r'"([a-f0-9]{32,})"[^"]{0,200}?"getAvailableSpots"', js)
        if m:
            return m.group(1)
        m = re.search(r'"getAvailableSpots"[^"]{0,200}?"([a-f0-9]{32,})"', js)
        if m:
            return m.group(1)
    raise RuntimeError("Nie udało się znaleźć Next-Action ID (getAvailableSpots)")


def scrape_kids(session: requests.Session) -> dict[str, Any]:
    action_id = _find_action_id(session)
    resp = session.post(
        KIDS_URL,
        data="[]",
        headers={
            "Next-Action": action_id,
            "Content-Type": "text/plain;charset=UTF-8",
            "Accept": "text/x-component",
            "Origin": "https://www.boryrun.pl",
            "Referer": KIDS_URL,
        },
        timeout=20,
    )
    resp.raise_for_status()

    available: int | None = None
    for line in resp.text.splitlines():
        m = re.match(r"^\d+:(\{.*\})\s*$", line)
        if not m:
            continue
        try:
            payload = json.loads(m.group(1))
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict) and "availableSpots" in payload:
            available = payload.get("availableSpots")
            break

    if available is None:
        return {"registered": None, "limit": KIDS_LIMIT}

    registered = max(0, KIDS_LIMIT - int(available))
    return {"registered": registered, "limit": KIDS_LIMIT}


def main() -> int:
    session = _session()
    adults_ok = True
    kids_ok = True

    try:
        adults = scrape_adults(session)
    except Exception as e:
        print(f"[adults] ERROR: {e}", file=sys.stderr)
        adults_ok = False
        adults = _load_fallback("adults") or {
            "total": None, "paid": None, "limit": ADULTS_LIMIT, "participants": [],
        }

    try:
        kids = scrape_kids(session)
    except Exception as e:
        print(f"[kids]   ERROR: {e}", file=sys.stderr)
        kids_ok = False
        kids = _load_fallback("kids") or {"registered": None, "limit": KIDS_LIMIT}

    out = {
        "adults": adults,
        "kids": kids,
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    OUTPUT_PATH.write_text(
        json.dumps(out, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(
        f"OK: adults.total={adults.get('total')} paid={adults.get('paid')} "
        f"kids.registered={kids.get('registered')}"
    )
    # Obie sekcje padły -> workflow ma świecić na czerwono.
    return 0 if (adults_ok or kids_ok) else 1


def _load_fallback(section: str) -> dict[str, Any] | None:
    try:
        prev = json.loads(OUTPUT_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None
    val = prev.get(section)
    return val if isinstance(val, dict) else None


if __name__ == "__main__":
    sys.exit(main())
