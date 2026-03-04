import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.request import Request, urlopen
from urllib.error import HTTPError

"""
Transcript downloader script.
Fetches transcripts for episodes within a lookback window for specified series.
Exports data to a local folder structure.
"""


API_URL = 'https://api.serafis.dev/public/v1'
# API_URL = 'http://localhost:8080/public/v1'

# Place your API keys here or use environment variables
API_KEY = os.getenv('SERAFIS_KEY')
API_SECRET = os.getenv('SERAFIS_SECRET')

EXPORT_DIR = "samples/data"
SLEEP_INTERVAL_S = 1


R_HEADERS = {
  'X-API-Key': API_KEY,
  'X-API-Secret': API_SECRET,
}


def api_request(url: str, method: str = "GET", body: Optional[dict] = None) -> tuple[int, Optional[Any]]:
  """Make an HTTP request using urllib. Returns (status_code, parsed_json)."""
  data = json.dumps(body).encode('utf-8') if body else None
  headers = {k: v for k, v in R_HEADERS.items() if v is not None}
  if data:
    headers['Content-Type'] = 'application/json'

  req = Request(url, data=data, headers=headers, method=method)
  try:
    with urlopen(req) as res:
      return res.status, json.loads(res.read())
  except HTTPError as e:
    return e.code, None


def dtn():
  return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def parse_ts(ts: int) -> datetime:
  return datetime.fromtimestamp(ts, tz=timezone.utc)


def calc_days_since_pub(timestamp: int) -> int:
  """Calculate days since publication timestamp."""
  now = datetime.now(timezone.utc)
  target_date = parse_ts(timestamp)
  time_diff = now - target_date
  return time_diff.days


def fetch_episodes_page(series_uuid: str, page: int) -> tuple[int, Optional[Any]]:
  """Fetch a single page of episodes for a series."""
  url = f"{API_URL}/universe/series/episodes"
  body = { "series_uuid": series_uuid, "page": page }
  return api_request(url, method="POST", body=body)


def query_episodes(series_uuid: str, lookback_days: int) -> tuple[dict, list[dict]]:
  """
  Query episodes for a series, filtering by lookback window.
  Returns series data and filtered episode list.
  """
  page_n, total = 1, 0
  series_data, episodes = None, []
  log_pfx = f"[query_episodes] {series_uuid}"

  while True:
    print(f"[{dtn()}] {log_pfx} page={page_n}")
    status, res_json = fetch_episodes_page(series_uuid, page_n)
    if status != 200 or not res_json:
      print(f"[{dtn()}] {log_pfx} ERROR: {status}")
      break

    if series_data is None:
      series_data = {"uuid": res_json['series_uuid'], "name": res_json['name']}
      total = int(res_json.get("total_episodes", 0) or 0)

    ep_batch = res_json.get('episodes', [])
    n_episodes_res = len(ep_batch)
    print(f"[{dtn()}] {log_pfx} page={page_n} received={n_episodes_res}")

    if n_episodes_res == 0:
      break

    # Filter episodes within lookback window
    for ep in ep_batch:
      ep_pub_ts = ep.get('date_published', 0)
      days_since = calc_days_since_pub(ep_pub_ts)
      if days_since <= lookback_days:
        episodes.append(ep)
      elif days_since > lookback_days and len(episodes) > 0:
        break

    # Check if we should continue pagination
    if len(episodes) >= total:
      break
    if ep_batch and calc_days_since_pub(ep_batch[-1].get('date_published', 0)) > lookback_days:
      break

    page_n += 1

  return series_data or {"uuid": series_uuid, "name": "Unknown"}, episodes


def fetch_transcript(episode_uuid: str) -> Optional[list[dict]]:
  """Fetch transcript for an episode via API."""
  if not episode_uuid:
    return None
  transcript_url = f"{API_URL}/universe/episode/transcript"
  body = { "episode_uuid": episode_uuid }
  print(f"[{dtn()}] [fetch_transcript] {episode_uuid}")
  status, data = api_request(transcript_url, method="POST", body=body)
  if status != 200:
    print(f"[{dtn()}] [fetch_transcript] ERROR {status} for {episode_uuid}")
    return None
  return data


def setup_export_dir(series_id: str) -> str:
  """Create export directory structure."""
  series_dir = os.path.join(EXPORT_DIR, series_id)
  os.makedirs(series_dir, exist_ok=True)
  return series_dir


def save_json(path: str, data: dict | list):
  """Save data as JSON file."""
  with open(path, 'w') as f:
    json.dump(data, f, indent=2)


def process_episode(ep: dict, series_dir: str) -> dict:
  """Fetch transcript for an episode, save to disk, and return an index entry."""
  ep_uuid = ep.get('episode_uuid', '')
  ep_name = ep.get('name', 'Untitled')
  ep_pub_ts = ep.get('date_published', 0)
  ep_pub_date = parse_ts(ep_pub_ts).strftime("%Y-%m-%d") if ep_pub_ts else "Unknown"

  print(f"[{dtn()}] Fetching transcript for: {ep_name[:40]}...")
  transcript = fetch_transcript(ep_uuid)
  time.sleep(SLEEP_INTERVAL_S)

  episode_data = {
    "uuid": ep_uuid,
    "title": ep_name,
    "publish_date": ep_pub_date,
    "duration": ep.get('duration'),
    "description": ep.get('description'),
    "transcript": transcript or []
  }
  episode_path = os.path.join(series_dir, f"{ep_uuid}.json")
  save_json(episode_path, episode_data)

  return {"title": ep_name, "uuid": ep_uuid, "publish_date": ep_pub_date}


def process_series(series_id: str, lookback_days: int) -> dict:
  """
  Process a single series: fetch episodes and transcripts.
  Returns index entry for this series.
  """
  print(f"\n{'=' * 50}")
  print(f"[{dtn()}] Processing series: {series_id}")
  print(f"{'=' * 50}\n")

  series_data, episodes = query_episodes(series_id, lookback_days)
  series_name = series_data.get('name', 'Unknown')
  print(f"[{dtn()}] Found {len(episodes)} episodes within {lookback_days} days for {series_name}")

  if not episodes:
    return {
      "series_id": series_id,
      "series_name": series_name,
      "episodes_exported": 0,
      "episodes": []
    }

  series_dir = setup_export_dir(series_id)
  episode_index = [process_episode(ep, series_dir) for ep in episodes]

  return {
    "series_id": series_id,
    "series_name": series_name,
    "episodes_exported": len(episode_index),
    "episodes": episode_index
  }


def run_transcript_export(series_ids: list[str], lookback_days: int):
  """
  Main export function.
  Processes all series and creates index file.
  """
  print(f"\n[{dtn()}] Starting bulk transcript export")
  print(f"[{dtn()}] Series count: {len(series_ids)}")
  print(f"[{dtn()}] Lookback window: {lookback_days} days\n")

  # Create export directory
  os.makedirs(EXPORT_DIR, exist_ok=True)

  # Process each series
  index_data = []
  for series_id in series_ids:
    series_index = process_series(series_id, lookback_days)
    index_data.append(series_index)

  # Save index file
  index_path = os.path.join(EXPORT_DIR, "index.json")
  save_json(index_path, index_data)

  # Print summary
  total_episodes = sum(s["episodes_exported"] for s in index_data)
  print(f"\n{'=' * 50}")
  print(f"[{dtn()}] Export complete!")
  print(f"[{dtn()}] Total series: {len(index_data)}")
  print(f"[{dtn()}] Total episodes: {total_episodes}")
  print(f"[{dtn()}] Export directory: {EXPORT_DIR}")
  print(f"{'=' * 50}\n")


# Example usage
EXAMPLE_SERIES_IDS = [
  "912340a6-71e8-434b-87d5-ba5834bbcbcf",  # Breaking beauty podcast
  "127c2924-d276-4ffc-9fef-1b4ae16d0888", # gloss angeles podcast
  "564286a5-17cc-4b0a-88ec-8ebbd8555caf", # Forever35 podcast
  "b67ff02c-1fff-4764-87bd-5ba61a1fd3f2", # smell ya later podcast
]
EXAMPLE_LOOKBACK_DAYS = 90


if __name__ == "__main__":
  run_transcript_export(EXAMPLE_SERIES_IDS, EXAMPLE_LOOKBACK_DAYS)
