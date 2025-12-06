#!/usr/bin/env python3
"""
job_board_finder.py

Reads input.csv with columns:
    companyName, applyUrl

For each row:
  - Checks if applyUrl itself is already a job board URL.
  - If not, requests the page and searches the HTML for job board URLs.
  - Currently supports BambooHR, but is structured to easily add more.

Outputs output_job_boards.csv with columns:
    companyName, source_applyUrl, job_board_vendor, job_board_url, notes

Requirements:
    pip install requests beautifulsoup4
"""

import csv
import time
import re
from pathlib import Path
from typing import Optional, Tuple, Dict, List

import requests
from bs4 import BeautifulSoup

# ====================== CONFIG ====================== #

INPUT_CSV = "input.csv"
OUTPUT_CSV = "output_job_boards.csv"

REQUEST_TIMEOUT = 20         # seconds
SLEEP_BETWEEN_REQUESTS = 1   # seconds delay between rows (politeness)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}

# Dictionary of job board patterns.
# Key: vendor name
# Value: compiled regex matching job-board URLs in raw HTML.
JOB_BOARD_PATTERNS: Dict[str, re.Pattern] = {
    # BambooHR – matches things like:
    # https://organovo.bamboohr.com/jobs/?source=aWQ9MjM=
    # https://company.bamboohr.com/careers
    "bamboohr": re.compile(
        r'https?://[a-zA-Z0-9.-]*bamboohr\.com[^\s"\'<]*',
        re.IGNORECASE
    ),

    # Example placeholders to extend later:
    # "greenhouse": re.compile(r'https?://boards\.greenhouse\.io[^\s"\'<]*', re.I),
    # "workday": re.compile(r'https?://[a-zA-Z0-9.-]*\.myworkdayjobs\.com[^\s"\'<]*', re.I),
    # etc...
}


# ====================== CORE LOGIC ====================== #

def normalize_url(url: str) -> str:
    """Ensure URL has a scheme."""
    url = (url or "").strip()
    if not url:
        return ""
    if not url.lower().startswith(("http://", "https://")):
        url = "https://" + url
    return url


def find_job_board_in_string(url: str) -> Optional[Tuple[str, str]]:
    """
    Check if a given URL string itself matches any known job board pattern.
    Returns (vendor, matched_url) or None.
    """
    text = url or ""
    for vendor, pattern in JOB_BOARD_PATTERNS.items():
        m = pattern.search(text)
        if m:
            return vendor, m.group(0)
    return None


def find_job_board_in_html(html: str) -> List[Tuple[str, str]]:
    """
    Search the entire HTML for job board URLs using JOB_BOARD_PATTERNS.
    Returns a list of (vendor, url) tuples (may contain duplicates).
    """
    results: List[Tuple[str, str]] = []
    if not html:
        return results

    for vendor, pattern in JOB_BOARD_PATTERNS.items():
        matches = pattern.findall(html)
        for match in matches:
            # Basic cleanup
            cleaned = match.strip()
            # Remove trailing punctuation that regex might have grabbed
            cleaned = cleaned.rstrip(')"\'.,;>')
            results.append((vendor, cleaned))
    return results


def pick_best_job_board(candidates: List[Tuple[str, str]]) -> Optional[Tuple[str, str]]:
    """
    Given a list of (vendor, url) candidates, pick one "best" candidate.

    Strategy:
      - Prefer URLs containing "/jobs" or "/careers".
      - Otherwise, return the first candidate.
    """
    if not candidates:
        return None

    # Separate best-by-path vs fallback
    with_jobs_or_careers = [
        (v, u) for (v, u) in candidates
        if "/jobs" in u.lower() or "/careers" in u.lower()
    ]
    if with_jobs_or_careers:
        return with_jobs_or_careers[0]

    return candidates[0]


def fetch_html(url: str) -> Optional[str]:
    """Fetch page HTML, returning None on error."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        print(f"[ERROR] Request failed for {url}: {e}")
        return None


def process_row(company: str, source_url: str) -> Tuple[str, str, str, str]:
    """
    Process a single (companyName, applyUrl) row.
    Returns (companyName, source_applyUrl, job_board_vendor, job_board_url, notes).
    """
    source_url_norm = normalize_url(source_url)
    if not source_url_norm:
        print(f"[WARN] No applyUrl for company '{company}', skipping.")
        return company, source_url, "", "", "no_apply_url"

    print(f"\n[INFO] Processing: {company} | {source_url_norm}")

    # 1) Check if applyUrl is already a job board URL
    direct_match = find_job_board_in_string(source_url_norm)
    if direct_match:
        vendor, jb_url = direct_match
        print(f"[OK] Direct job board URL detected ({vendor}): {jb_url}")
        return company, source_url_norm, vendor, jb_url, "direct_match"

    # 2) Fetch HTML and search within the page
    html = fetch_html(source_url_norm)
    if html is None:
        return company, source_url_norm, "", "", "fetch_error"

    # (Optional) You can still parse with BeautifulSoup if later you want more structured searching
    # For now, we're using raw regex which also covers inline scripts/JSON.
    _soup = BeautifulSoup(html, "html.parser")

    candidates = find_job_board_in_html(html)
    if not candidates:
        print(f"[WARN] No known job board URLs found in page for {company}")
        return company, source_url_norm, "", "", "no_match_in_html"

    # Deduplicate (vendor, url) pairs while preserving order
    seen = set()
    unique_candidates: List[Tuple[str, str]] = []
    for vendor, url in candidates:
        if (vendor, url) not in seen:
            seen.add((vendor, url))
            unique_candidates.append((vendor, url))

    best = pick_best_job_board(unique_candidates)
    if best:
        vendor, jb_url = best
        print(f"[OK] Found job board ({vendor}): {jb_url}")
        return company, source_url_norm, vendor, jb_url, "html_match"

    # Should rarely get here if candidates is non-empty
    print(f"[WARN] Candidates present but no best candidate selected for {company}")
    return company, source_url_norm, "", "", "candidate_selection_failed"


# ====================== MAIN ====================== #

def main():
    input_path = Path(INPUT_CSV)
    if not input_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {INPUT_CSV}")

    with input_path.open("r", newline="", encoding="utf-8-sig") as f_in, \
            open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f_out:

        reader = csv.DictReader(f_in)
        fieldnames = [
            "companyName",
            "source_applyUrl",
            "job_board_vendor",
            "job_board_url",
            "notes",
        ]
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()

        row_count = 0
        for row in reader:
            row_count += 1
            company = (row.get("companyName") or "").strip()
            apply_url = (row.get("applyUrl") or "").strip()

            result = process_row(company, apply_url)
            out_row = {
                "companyName": result[0],
                "source_applyUrl": result[1],
                "job_board_vendor": result[2],
                "job_board_url": result[3],
                "notes": result[4],
            }
            writer.writerow(out_row)

            time.sleep(SLEEP_BETWEEN_REQUESTS)

        print(f"\n[INFO] Done. Processed {row_count} rows.")
        print(f"[INFO] Output written to: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
