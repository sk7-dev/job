import json
import os
import sys
import time
from typing import List, Optional
from urllib.parse import urlparse, quote

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

    title_keywords_any = [
        normalize_text(x) for x in filters.get("title_keywords_any", []) if str(x).strip()
    ]
    locations_any = [
        normalize_text(x) for x in filters.get("locations_any", []) if str(x).strip()
    ]
    excluded_keywords_any = [
        normalize_text(x) for x in filters.get("excluded_keywords_any", []) if str(x).strip()
    ]

    title_ok = True
    if title_keywords_any:
        title_ok = any(k in title for k in title_keywords_any)

    location_ok = True
    if locations_any:
        location_ok = any(k in combined for k in locations_any)

    excluded_ok = True
    if excluded_keywords_any:
        excluded_ok = not any(k in combined for k in excluded_keywords_any)

    return title_ok and location_ok and excluded_ok


def safe_get(url: str, params: Optional[dict] = None, headers: Optional[dict] = None):
    resp = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp


def safe_get_json(url: str, params: Optional[dict] = None, headers: Optional[dict] = None):
    return safe_get(url, params=params, headers=headers).json()


def safe_post_json(url: str, json_body: dict, headers: Optional[dict] = None):
    resp = requests.post(url, json=json_body, headers=headers, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def fetch_greenhouse(source: dict) -> List[dict]:
    token = source["board_token"]
    url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs"
    data = safe_get_json(url)

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
    url = f"https://api.lever.co/v0/postings/{company}"
    data = safe_get_json(url, params={"mode": "json"})

    jobs = []
    for item in data:
        categories = item.get("categories") or {}
        jobs.append({
            "source_name": source["name"],
            "source_type": "lever",
            "external_id": str(item.get("id")),
            "title": item.get("text", ""),
            "location": categories.get("location", ""),
            "department": categories.get("team", ""),
            "url": item.get("hostedUrl") or item.get("applyUrl") or "",
            "posted_at": str(item.get("createdAt", "")),
        })
    return jobs


def fetch_ashby(source: dict) -> List[dict]:
    url = source.get("api_url", "https://api.ashbyhq.com/jobPosting.list")
    body = {
        "organizationHostedJobsPageName": source["organization_key"],
        "listedOnly": True,
    }
    data = safe_post_json(url, body)

    jobs = []
    for item in data.get("results", []):
        location = ""
        loc = item.get("location")
        if isinstance(loc, dict):
            location = (
                loc.get("locationSummary")
                or loc.get("city")
                or loc.get("region")
                or loc.get("country")
                or ""
            )

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


def extract_json_object(text: str, marker: str) -> Optional[dict]:
    start = text.find(marker)
    if start == -1:
        return None

    start = text.find("{", start)
    if start == -1:
        return None

    depth = 0
    in_string = False
    escape = False

    for i in range(start, len(text)):
        ch = text[i]

        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                candidate = text[start:i + 1]
                try:
                    return json.loads(candidate)
                except json.JSONDecodeError:
                    return None
    return None


def fetch_phenom_embedded(source: dict) -> List[dict]:
    url = source["url"]
    resp = safe_get(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36"
        },
    )

    ddo = extract_json_object(resp.text, "phApp.ddo =")
    if not ddo:
        raise ValueError("Could not locate phApp.ddo JSON in page source")

    jobs = (
        ddo.get("eagerLoadRefineSearch", {})
           .get("data", {})
           .get("jobs", [])
    )

    out = []
    for item in jobs:
        title = item.get("title", "")
        location = (
            item.get("location")
            or item.get("cityStateCountry")
            or item.get("locationName")
            or ""
        )
        department = item.get("category", "")
        external_id = item.get("jobId") or item.get("jobSeqNo") or title

        apply_url = item.get("applyUrl", "")
        if source.get("strip_apply_suffix", True) and apply_url.endswith("/apply"):
            job_url = apply_url[:-6]
        else:
            job_url = apply_url

        out.append({
            "source_name": source["name"],
            "source_type": "phenom_embedded",
            "external_id": str(external_id),
            "title": title,
            "location": location,
            "department": department,
            "url": job_url,
            "posted_at": item.get("postedDate", "") or item.get("dateCreated", ""),
        })

    return out


def parse_workday_source(source: dict) -> tuple[str, str, str]:
    if source.get("tenant") and source.get("site") and source.get("base_url"):
        return source["base_url"].rstrip("/"), source["tenant"], source["site"]

    url = source["url"]
    parsed = urlparse(url)
    host = parsed.netloc
    path_parts = [p for p in parsed.path.split("/") if p]

    if not path_parts:
        raise ValueError("Could not determine Workday site from URL")

    if len(path_parts) >= 2 and "-" in path_parts[0]:
        site = path_parts[1]
    else:
        site = path_parts[0]

    tenant = host.split(".")[0]
    base_url = f"{parsed.scheme}://{host}"
    return base_url, tenant, site


def workday_extract_location(item: dict) -> str:
    locations = item.get("locationsText")
    if locations:
        return locations

    bullet_fields = item.get("bulletFields") or []
    if bullet_fields:
        return " | ".join(str(x) for x in bullet_fields if x)

    locations = item.get("locations") or []
    if isinstance(locations, list) and locations:
        vals = []
        for loc in locations:
            if isinstance(loc, dict):
                text = loc.get("displayName") or loc.get("name")
                if text:
                    vals.append(text)
            elif loc:
                vals.append(str(loc))
        if vals:
            return " | ".join(vals)

    return ""


def workday_extract_posted(item: dict) -> str:
    return (
        item.get("postedOn")
        or item.get("postedDate")
        or item.get("startDate")
        or ""
    )


def fetch_workday(source: dict) -> List[dict]:
    base_url, tenant, site = parse_workday_source(source)
    endpoint = f"{base_url}/wday/cxs/{tenant}/{site}/jobs"

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0",
    }

    limit = int(source.get("limit", 20))
    offset = 0
    jobs = []

    while True:
        body = {
            "limit": limit,
            "offset": offset,
            "searchText": source.get("search_text", ""),
        }

        data = safe_post_json(endpoint, body, headers=headers)

        postings = (
            data.get("jobPostings")
            or data.get("job_postings")
            or data.get("jobs")
            or []
        )

        if not postings:
            break

        for item in postings:
            title = item.get("title", "")
            external_path = item.get("externalPath", "")
            if external_path:
                job_url = f"{base_url}/{site}/job/{external_path.lstrip('/')}"
            else:
                job_url = source.get("url", "")

            department = ""
            if item.get("jobFamily"):
                department = str(item.get("jobFamily"))
            elif item.get("jobFamilyGroup"):
                department = str(item.get("jobFamilyGroup"))

            external_id = (
                item.get("bulletFields", [None, None])[-1]
                or item.get("jobReqId")
                or item.get("id")
                or item.get("title")
            )

            jobs.append({
                "source_name": source["name"],
                "source_type": "workday",
                "external_id": str(external_id),
                "title": title,
                "location": workday_extract_location(item),
                "department": department,
                "url": job_url,
                "posted_at": workday_extract_posted(item),
            })

        total = data.get("total")
        offset += len(postings)

        if total is not None and offset >= total:
            break
        if len(postings) < limit:
            break

        time.sleep(0.2)

    return jobs


def entertime_extract_list(data: dict) -> List[dict]:
    for key in ("job_requisitions", "items", "data", "results", "jobs", "requisitions", "jobRequisitions"):
        value = data.get(key)
        if isinstance(value, list):
            return value
    if isinstance(data, list):
        return data
    return []


def entertime_pick(item: dict, keys: List[str]) -> str:
    for key in keys:
        value = item.get(key)
        if value:
            return str(value)
    return ""


def entertime_location(item: dict) -> str:
    location = item.get("location")
    if isinstance(location, dict):
        parts = [
            location.get("city"),
            location.get("state"),
            location.get("country"),
        ]
        parts = [str(x).strip() for x in parts if x]
        if parts:
            return ", ".join(parts)

        line_parts = [
            location.get("address_line_1"),
            location.get("city"),
            location.get("state"),
            location.get("zip"),
            location.get("country"),
        ]
        line_parts = [str(x).strip() for x in line_parts if x]
        if line_parts:
            return ", ".join(line_parts)

    return entertime_pick(item, ["locationName", "jobLocation", "cityState", "location"])


def fetch_entertime(source: dict) -> List[dict]:
    base_url = source["base_url"].rstrip("/")
    company_id = source["company_id"]
    lang = source.get("lang", "en-US")
    size = int(source.get("size", 20))
    sort = source.get("sort", "desc")

    endpoint = f"{base_url}/ta/rest/ui/recruitment/companies/%7C{company_id}/job-requisitions"

    headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0",
        "Referer": f"{base_url}/ta/{company_id}.careers?CareersSearch=&lang={lang}",
    }

    offset = 0
    jobs = []

    while True:
        params = {
            "_": str(int(time.time() * 1000)),
            "offset": offset,
            "size": size,
            "lang": lang,
        }

        ein_id = source.get("ein_id")
        if ein_id is not None:
            params["ein_id"] = ein_id

        if sort:
            params["sort"] = sort

        data = safe_get_json(endpoint, params=params, headers=headers)
        items = entertime_extract_list(data)

        if not items:
            break

        for item in items:
            title = entertime_pick(item, ["job_title", "title", "jobTitle", "requisitionTitle", "name"])
            location = entertime_location(item)
            department = ""

            employee_type = item.get("employee_type")
            if isinstance(employee_type, dict):
                department = employee_type.get("name", "")

            req_id = entertime_pick(item, ["id", "jobId", "requisitionId", "jobReqId", "reqId"])
            external_id = req_id or title

            # Common detail URL pattern on this platform
            if req_id:
                detail_url = f"{base_url}/ta/rest/ui/recruitment/companies/%7C{company_id}/job-requisitions/{req_id}"
            else:
                detail_url = f"{base_url}/ta/{company_id}.careers?CareersSearch=&lang={quote(lang)}"

            posted_at = entertime_pick(item, ["postedDate", "datePosted", "createdDate", "updateDate"])

            jobs.append({
                "source_name": source["name"],
                "source_type": "entertime",
                "external_id": str(external_id),
                "title": title,
                "location": location,
                "department": department,
                "url": detail_url,
                "posted_at": posted_at,
            })

        if len(items) < size:
            break

        offset += size
        time.sleep(0.2)

    return jobs

def fetch_jobs_for_source(source: dict) -> List[dict]:
    stype = source["type"].lower()
    if stype == "greenhouse":
        return fetch_greenhouse(source)
    if stype == "lever":
        return fetch_lever(source)
    if stype == "ashby":
        return fetch_ashby(source)
    if stype == "phenom_embedded":
        return fetch_phenom_embedded(source)
    if stype == "workday":
        return fetch_workday(source)
    if stype == "entertime":
        return fetch_entertime(source)
    raise ValueError(f"Unsupported source type: {stype}")


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
            f"- {job['title']} | {job['source_name']} | {job.get('location', '')} | {job['url']}"
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
            jobs = fetch_jobs_for_source(source)
            all_jobs.extend(jobs)
            print(f"{source.get('name', 'unknown source')}: fetched {len(jobs)} job(s)")
            time.sleep(0.5)
        except Exception as e:
            err = f"{source.get('name', 'unknown source')}: {e}"
            errors.append(err)
            print(f"ERROR - {err}", file=sys.stderr)

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

    webhook = os.getenv("DISCORD_WEBHOOK_URL")
    if new_jobs and webhook:
        send_discord(webhook, format_discord_text(new_jobs))
        print("Discord alert sent.")
    elif new_jobs:
        print("New jobs found, but DISCORD_WEBHOOK_URL is not configured.")
    else:
        print("No new matching jobs to send.")

    if errors:
        print("Completed with source errors:", file=sys.stderr)
        for err in errors:
            print(f" - {err}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())