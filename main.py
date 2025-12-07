#!/usr/bin/env python3
"""
job_board_finder.py

Reads input.csv with columns:
  - companyName
  - applyUrl

For each row:
  - Normalizes and fetches applyUrl.
  - Detects job boards (ATS) via:
      * Direct match on applyUrl string
      * Full HTML of the page
      * Any <iframe> src URLs
      * Optionally the HTML content of those iframe src URLs
  - Currently knows about: BambooHR, Greenhouse, Workable, JazzHR, Paycor, TrineHire
    (easy to add more).
  - Detects "email to apply with PDFs" pattern.

Outputs output.csv with columns:
  - companyName
  - applyUrl                 (original)
  - job_board_url            (detected ATS URL, if any)
  - board_type               (e.g. "bamboohr", "greenhouse", "paycor", "trinethire")
  - update                   ("email to apply with pdfs" or "")

Requires:
  pip install requests beautifulsoup4
"""

import csv
import re
import sys
from pathlib import Path
from typing import List, Tuple, Optional, Dict
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

INPUT_FILE = "input.csv"
OUTPUT_FILE = "output.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/129.0.0.0 Safari/537.36"
    )
}
TIMEOUT = 20

# -------------------------------------------------------------------
# Job board patterns (extend this as you add more ATS)
# -------------------------------------------------------------------
JOB_BOARD_PATTERNS: Dict[str, re.Pattern] = {
    # BambooHR – e.g. https://organovo.bamboohr.com/jobs/?source=aWQ9MjM=
    "bamboohr": re.compile(
        r'https?://[a-zA-Z0-9.-]*bamboohr\.com[^\s"\'<>]*',
        re.IGNORECASE,
    ),

    # Greenhouse – handle boards, job-boards, company subdomains, etc.
    # e.g. https://job-boards.greenhouse.io/bioskryb
    #      https://boards.greenhouse.io/company
    "greenhouse": re.compile(
        r'https?://[a-zA-Z0-9.-]*greenhouse\.io[^\s"\'<>]*',
        re.IGNORECASE,
    ),

    # Workable – includes apply.workable.com and company subdomains
    "workable": re.compile(
        r'https?://[a-zA-Z0-9.-]*workable\.com[^\s"\'<>]*',
        re.IGNORECASE,
    ),

    # JazzHR – primary application links are served from applytojob.com
    "jazzhr": re.compile(
        r'https?://[a-zA-Z0-9.-]*applytojob\.com[^\s"\'<>]*',
        re.IGNORECASE,
    ),

    # Paycor – rough pattern for embedded Paycor job pages
    # (You can tighten this when you see more real-world URLs.)
    "paycor": re.compile(
        r'https?://[a-zA-Z0-9.-]*paycor\.com[^\s"\'<>]*',
        re.IGNORECASE,
    ),

    # TrineHire – example ATS
    "trinethire": re.compile(
        r'https?://[a-zA-Z0-9.-]*trinethire\.com[^\s"\'<>]*',
        re.IGNORECASE,
    ),

    # Indeed – company pages and viewjob links
    "indeed": re.compile(
        r'https?://[a-zA-Z0-9.-]*indeed\.com[^\s"\'<>]*',
        re.IGNORECASE,
    ),

    # Add more here as needed:
    # "workday": re.compile(r'https?://[a-zA-Z0-9.-]*\.myworkdayjobs\.com[^\s"\'<>]*', re.IGNORECASE),
    # "lever": re.compile(r'https?://jobs\.lever\.co[^\s"\'<>]*', re.IGNORECASE),
    # "icims": re.compile(r'https?://[a-zA-Z0-9.-]*\.icims\.com[^\s"\'<>]*', re.IGNORECASE),
}


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------
def normalize_url(url: str) -> str:
    """Ensure URL has a scheme (https://) so requests can handle it."""
    url = (url or "").strip()
    if not url:
        return ""
    if not url.lower().startswith(("http://", "https://")):
        url = "https://" + url
    return url


def fetch_html(url: str) -> Optional[str]:
    """Fetch HTML for a URL, returning None on error."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        print(f"[ERROR] Fetch failed for {url}: {e}", file=sys.stderr)
        return None


# -------------------------------------------------------------------
# Job board detection
# -------------------------------------------------------------------
def find_job_boards_in_text(text: str) -> List[Tuple[str, str]]:
    """
    Search an arbitrary text (URL, script, etc.) for known ATS patterns.
    Returns a list of (vendor, url) tuples.
    """
    results: List[Tuple[str, str]] = []
    if not text:
        return results

    for vendor, pattern in JOB_BOARD_PATTERNS.items():
        for match in pattern.findall(text):
            cleaned = match.strip().rstrip(')"\'.,;>')
            results.append((vendor, cleaned))
    return results


def find_job_boards_in_html(html: str) -> List[Tuple[str, str]]:
    """
    Search the entire HTML for job board URLs using `JOB_BOARD_PATTERNS`.
    Returns a list of (vendor, url) tuples.
    """
    return find_job_boards_in_text(html or "")


def pick_best_job_board(candidates: List[Tuple[str, str]]) -> Optional[Tuple[str, str]]:
    """
    Given a list of (vendor, url) candidates, pick one "best" candidate.

    Strategy:
      - Prefer URLs containing "/jobs" or "/careers".
      - Among those, just pick the first.
      - If none match that pattern, return the first candidate overall.
    """
    if not candidates:
        return None

    with_jobs_or_careers = [
        (v, u) for (v, u) in candidates
        if "/jobs" in u.lower() or "/careers" in u.lower()
    ]
    if with_jobs_or_careers:
        return with_jobs_or_careers[0]

    return candidates[0]


# -------------------------------------------------------------------
# Email + PDF pattern detection
# -------------------------------------------------------------------
def detect_email_with_pdfs(soup: BeautifulSoup) -> bool:
    """
    Detect pattern:
      - Jobs are listed as PDF links, and
      - There's an email address (mailto:) used to apply.

    Heuristic:
      - At least one <a> href with ".pdf"
      - At least one <a> href with "mailto:" whose nearby text mentions
        'apply', 'resume', or 'cv'.
    """
    # Find PDF links
    job_pdf_links = [
        a for a in soup.find_all("a", href=True)
        if ".pdf" in a["href"].lower()
    ]
    if not job_pdf_links:
        return False

    # Find mailto links
    mailto_links = [
        a for a in soup.find_all("a", href=True)
        if a["href"].lower().startswith("mailto:")
    ]
    if not mailto_links:
        return False

    KEYWORDS = ("apply", "resume", "cv")

    def has_apply_context(anchor) -> bool:
        # Text on the anchor itself
        text = (anchor.get_text(strip=True) or "").lower()
        if any(k in text for k in KEYWORDS):
            return True

        # Text in the immediate parent (if available)
        parent = anchor.parent
        if parent is not None:
            parent_text = " ".join(parent.stripped_strings).lower()
            if any(k in parent_text for k in KEYWORDS):
                return True

        return False

    has_apply_mailto = any(has_apply_context(a) for a in mailto_links)
    return bool(job_pdf_links and has_apply_mailto)


# -------------------------------------------------------------------
# Per-URL analysis (now with iframe support)
# -------------------------------------------------------------------
def analyze_url(url: str) -> Tuple[str, str, str]:
    """
    Analyze a careers URL and return:
      (job_board_url, board_type, update)

    job_board_url: detected ATS/job board URL (or "")
    board_type   : one of JOB_BOARD_PATTERNS keys (or "")
    update       : "email to apply with pdfs" or ""
    """
    normalized = normalize_url(url)
    if not normalized:
        return "", "", ""

    # ---- 1) Start with direct match on the URL itself ----
    candidates: List[Tuple[str, str]] = []
    candidates.extend(find_job_boards_in_text(normalized))

    # ---- 2) Fetch the main page HTML ----
    html = fetch_html(normalized)
    if html is None:
        # If we at least had a direct match, we can still use that
        if candidates:
            # Deduplicate & pick best from what's available
            seen = set()
            uniq = []
            for vendor, u in candidates:
                if (vendor, u) not in seen:
                    seen.add((vendor, u))
                    uniq.append((vendor, u))
            best = pick_best_job_board(uniq)
            if best:
                vendor, jb_url = best
                return jb_url, vendor, ""
        return "", "", ""

    soup = BeautifulSoup(html, "html.parser")

    # ---- 3) Scan full HTML for job board URLs ----
    candidates.extend(find_job_boards_in_html(html))

    # ---- 4) Scan iframes: src and optional iframe HTML ----
    iframes = soup.find_all("iframe", src=True)
    for iframe in iframes:
        raw_src = iframe.get("src", "").strip()
        if not raw_src:
            continue

        iframe_src = urljoin(normalized, raw_src)
        # 4a) Inspect the iframe src URL itself
        candidates.extend(find_job_boards_in_text(iframe_src))

        # 4b) Optionally fetch iframe HTML and scan that too
        iframe_html = fetch_html(iframe_src)
        if iframe_html:
            candidates.extend(find_job_boards_in_html(iframe_html))

    # Deduplicate all candidates while preserving order
    seen = set()
    unique_candidates: List[Tuple[str, str]] = []
    for vendor, u in candidates:
        key = (vendor, u)
        if key not in seen:
            seen.add(key)
            unique_candidates.append((vendor, u))

    job_board_url = ""
    board_type = ""
    if unique_candidates:
        best = pick_best_job_board(unique_candidates)
        if best:
            board_type, job_board_url = best[0], best[1]

    # ---- 5) Email + PDF pattern on the main page ----
    update = ""
    if detect_email_with_pdfs(soup):
        update = "email to apply with pdfs"

    return job_board_url, board_type, update


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------
def main():
    in_path = Path(INPUT_FILE)
    if not in_path.exists():
        print(f"Input file '{INPUT_FILE}' not found.", file=sys.stderr)
        sys.exit(1)

    with in_path.open("r", newline="", encoding="utf-8-sig") as f_in, \
            open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f_out:

        reader = csv.DictReader(f_in)
        fieldnames = ["companyName", "applyUrl", "job_board_url", "board_type", "update"]
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()

        for idx, row in enumerate(reader, start=2):
            company = (row.get("companyName") or "").strip()
            url = (row.get("applyUrl") or "").strip()

            if not url:
                print(f"[SKIP] Row {idx}: no applyUrl for company '{company}'")
                continue

            print(f"[INFO] Row {idx}: processing {company} -> {url}")
            job_board_url, board_type, update = analyze_url(url)

            if job_board_url:
                print(f"  [FOUND ATS] {board_type} -> {job_board_url}")
            if update:
                print(f"  [PATTERN] {update}")

            writer.writerow({
                "companyName": company,
                "applyUrl": url,
                "job_board_url": job_board_url,
                "board_type": board_type,
                "update": update,
            })

    print(f"\nDone. Wrote results to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
