import json
import os
import sys
import time
from typing import Optional, List

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


def safe_get(url: str, params: Optional[dict] = None) -> dict:
    resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def fetch_greenhouse(source: dict) -> List[dict]:
    token = source["board_token"]
    url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs"
    data = safe_get(url)

    jobs = []
    for item in data.get("jobs", []):
        location = ""
        if isinstance(item.get("location"), dict):
            location = item["location"].get("name", "")

        departments = item.get("departments") or []
        department = ", ".join(d.get("name", "") for d in departments if d.get("name"))

        jobs.append({
            "source_name": source["name"],
            "source_type": "greenhouse",
            "external_id": str(item.get("id")),
            "title": item.get("title", ""),
            "location": location,
            "department": department,
            "url": item.get("absolute_url", "")
        })
    return jobs


def stable_job_key(job: dict) -> str:
    return "||".join([
        job.get("source_type", ""),
        job.get("source_name", ""),
        job.get("external_id", ""),
        job.get("url", ""),
    ])


def format_discord_text(new_jobs: List[dict]) -> str:
    lines = [f"{len(new_jobs)} new matching job(s) found:"]
    for job in new_jobs[:10]:
        lines.append(
            f"- {job['title']} | {job['source_name']} | {job.get('location','')} | {job['url']}"
        )
    if len(new_jobs) > 10:
        lines.append(f"...and {len(new_jobs) - 10} more.")
    return "\n".join(lines)


def send_discord(webhook_url: str, text: str) -> None:
    resp = requests.post(
        webhook_url,
        json={"content": text},
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
            jobs = fetch_greenhouse(source)
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
        print("Errors:", file=sys.stderr)
        for err in errors:
            print(f" - {err}", file=sys.stderr)

    webhook = os.getenv("DISCORD_WEBHOOK_URL")
    if new_jobs and webhook:
        send_discord(webhook, format_discord_text(new_jobs))
        print("Discord alert sent.")
    elif new_jobs:
        print("New jobs found, but DISCORD_WEBHOOK_URL is not configured.")

    if errors and not all_jobs:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())