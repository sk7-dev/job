import json
import os
import sys
import time
from typing import Dict, List, Optional

import requests


CONFIG_PATH = "config.json"
STATE_PATH = "state_seen.json"
REQUEST_TIMEOUT = 30


def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, data: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")


def normalize_text(value: Optional[str]) -> str:
    return (value or "").strip().lower()


def matches_filters(job: dict, filters: dict) -> bool:
    title = normalize_text(job.get("title"))
    location = normalize_text(job.get("location"))
    department = normalize_text(job.get("department"))
    combined = " | ".join([title, location, department])

    title_keywords_any = [normalize_text(x) for x in filters.get("title_keywords_any", []) if x.strip()]
    locations_any = [normalize_text(x) for x in filters.get("locations_any", []) if x.strip()]

    title_ok = True
    if title_keywords_any:
        title_ok = any(k in title for k in title_keywords_any)

    location_ok = True
    if locations_any:
        location_ok = any(k in combined for k in locations_any)

    return title_ok and location_ok


def safe_get(url: str, params: Optional[dict] = None, headers: Optional[dict] = None) -> dict:
    resp = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def safe_post(url: str, json_body: dict, headers: Optional[dict] = None) -> dict:
    resp = requests.post(url, json=json_body, headers=headers, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def fetch_greenhouse(source: dict) -> List[dict]:
    token = source["board_token"]
    # Greenhouse Job Board API
    url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs"
    data = safe_get(url)

    jobs = []
    for item in data.get("jobs", []):
        location = ""
        if isinstance(item.get("location"), dict):
            location = item["location"].get("name", "")

        departments = item.get("departments") or []
        department = ", ".join(d.get("name", "") for d in departments if d.get("name"))

        offices = item.get("offices") or []
        office = ", ".join(o.get("name", "") for o in offices if o.get("name"))

        jobs.append({
            "source_name": source["name"],
            "source_type": "greenhouse",
            "external_id": str(item.get("id")),
            "title": item.get("title", ""),
            "location": location or office,
            "department": department,
            "url": item.get("absolute_url", ""),
            "posted_at": "",
        })
    return jobs


def fetch_lever(source: dict) -> List[dict]:
    company = source["company"]
    # Lever public postings endpoint
    url = f"https://api.lever.co/v0/postings/{company}"
    params = {"mode": "json"}
    data = safe_get(url, params=params)

    jobs = []
    for item in data:
        categories = item.get("categories") or {}
        location = categories.get("location", "") or item.get("categories", {}).get("team", "")
        department = categories.get("team", "")

        hosted_url = item.get("hostedUrl") or item.get("applyUrl") or ""
        jobs.append({
            "source_name": source["name"],
            "source_type": "lever",
            "external_id": str(item.get("id")),
            "title": item.get("text", ""),
            "location": location,
            "department": department,
            "url": hosted_url,
            "posted_at": item.get("createdAt", ""),
        })
    return jobs


def fetch_ashby(source: dict) -> List[dict]:
    # Ashby public job postings API
    # Many public career pages use this endpoint pattern.
    url = source.get("api_url", "https://api.ashbyhq.com/jobPosting.list")
    body = {
        "organizationHostedJobsPageName": source["organization_key"],
        "listedOnly": True
    }
    data = safe_post(url, body)

    jobs = []
    for item in data.get("results", []):
        location_parts = []

        location_obj = item.get("location")
        if isinstance(location_obj, dict):
            for key in ("locationSummary", "city", "region", "country"):
                val = location_obj.get(key)
                if val:
                    location_parts.append(str(val))

        location = " / ".join(dict.fromkeys(location_parts))

        department = ""
        departments = item.get("department") or item.get("departments") or []
        if isinstance(departments, list):
            if departments and isinstance(departments[0], dict):
                department = ", ".join(d.get("name", "") for d in departments if d.get("name"))
            else:
                department = ", ".join(str(x) for x in departments if x)
        elif isinstance(departments, dict):
            department = departments.get("name", "")

        jobs.append({
            "source_name": source["name"],
            "source_type": "ashby",
            "external_id": str(item.get("id") or item.get("jobPostingId") or item.get("title", "")),
            "title": item.get("title", ""),
            "location": location,
            "department": department,
            "url": item.get("jobUrl") or item.get("applicationUrl") or "",
            "posted_at": item.get("publishedAt", ""),
        })
    return jobs


def fetch_jobs_for_source(source: dict) -> List[dict]:
    stype = source["type"].lower()
    if stype == "greenhouse":
        return fetch_greenhouse(source)
    if stype == "lever":
        return fetch_lever(source)
    if stype == "ashby":
        return fetch_ashby(source)
    raise ValueError(f"Unsupported source type: {stype}")


def stable_job_key(job: dict) -> str:
    return "||".join([
        job.get("source_type", ""),
        job.get("source_name", ""),
        job.get("external_id", ""),
        job.get("url", ""),
    ])


def format_slack_text(new_jobs: List[dict]) -> str:
    lines = [f"*{len(new_jobs)} new matching job(s) found:*"]
    for job in new_jobs[:25]:
        line = (
            f"• *{job['title']}* — {job['source_name']} | "
            f"{job.get('location','')} | {job.get('department','')}\n"
            f"  {job['url']}"
        )
        lines.append(line)

    if len(new_jobs) > 25:
        lines.append(f"...and {len(new_jobs) - 25} more.")

    return "\n".join(lines)


def send_slack(webhook_url: str, text: str) -> None:
    resp = requests.post(
        webhook_url,
        json={"text": text},
        timeout=REQUEST_TIMEOUT,
        headers={"Content-Type": "application/json"},
    )
    resp.raise_for_status()


def main() -> int:
    config = load_json(CONFIG_PATH)
    state = load_json(STATE_PATH)

    filters = config.get("filters", {})
    sources = config.get("sources", [])
    seen_keys = set(state.get("seen_keys", []))

    all_jobs = []
    errors = []

    for source in sources:
        try:
            jobs = fetch_jobs_for_source(source)
            all_jobs.extend(jobs)
            time.sleep(0.5)
        except Exception as e:
            errors.append(f"{source.get('name', 'unknown source')}: {e}")

    matching_jobs = [job for job in all_jobs if matches_filters(job, filters)]

    new_jobs = []
    for job in matching_jobs:
        key = stable_job_key(job)
        if key not in seen_keys:
            new_jobs.append(job)
            seen_keys.add(key)

    state["seen_keys"] = sorted(seen_keys)
    save_json(STATE_PATH, state)

    print(f"Fetched total jobs: {len(all_jobs)}")
    print(f"Matching jobs: {len(matching_jobs)}")
    print(f"New jobs: {len(new_jobs)}")

    if errors:
        print("Errors:")
        for err in errors:
            print(f" - {err}", file=sys.stderr)

    slack_webhook = os.getenv("SLACK_WEBHOOK_URL")
    if new_jobs and slack_webhook:
        text = format_slack_text(new_jobs)
        send_slack(slack_webhook, text)
        print("Slack alert sent.")
    elif new_jobs:
        print("New jobs found, but SLACK_WEBHOOK_URL is not configured.")
        for job in new_jobs:
            print(job["title"], "-", job["url"])

    # Fail only if every source failed
    if errors and not all_jobs:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())