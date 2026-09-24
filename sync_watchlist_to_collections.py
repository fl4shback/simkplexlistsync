#!/usr/bin/env python3
"""Sync Plex Discover watchlist to local Plex collections.

Optional SIMKL-rank sorting uses one metadata TTL (30 days by default) and a
persisted Plex collection-order cache. Plex reorder writes are skipped whenever
the desired ratingKey sequence is unchanged.

SIMKL uses separate databases. Resolution is exclusive and sequential per item:
- Plex movie: SIMKL movies (TMDB, then IMDb), then SIMKL anime (IMDb).
- Plex show:  SIMKL TV (TVDB, then IMDb), then SIMKL anime (IMDb).

The anime endpoint is a fallback only: a successful movie/TV resolution stops
further lookup for that item. Separate items may resolve in parallel.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import requests
from plexapi.myplex import MyPlexAccount
from plexapi.server import PlexServer
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

DEFAULT_CACHE_PATH = os.getenv("CACHE_PATH", "/data/plex-watchlist-cache.json")
DEFAULT_MOVIE_COLLECTION_NAME = os.getenv("MOVIE_COLLECTION_NAME", "Films à voir")
DEFAULT_SHOW_COLLECTION_NAME = os.getenv("SHOW_COLLECTION_NAME", "Séries à voir")
DEFAULT_PLEX_BASE_URL = os.getenv("PLEX_BASE_URL", "https://plex.sample")
DEFAULT_PLEX_TOKEN = os.getenv("PLEX_TOKEN", "")
DEFAULT_MOVIES_SECTION = os.getenv("MOVIES_SECTION", "Films")
DEFAULT_SHOWS_SECTION = os.getenv("SHOWS_SECTION", "Séries TV")

DEFAULT_COLLECTION_SORT = os.getenv("COLLECTION_SORT", "alpha").strip().lower()
DEFAULT_SIMKL_CLIENT_ID = os.getenv("SIMKL_CLIENT_ID", "")
DEFAULT_SIMKL_APP_NAME = os.getenv("SIMKL_APP_NAME", "Plex Discover Watchlist Sync")
DEFAULT_SIMKL_APP_VERSION = os.getenv("SIMKL_APP_VERSION", "v0.5.0")
DEFAULT_SIMKL_CACHE_TTL = int(os.getenv("SIMKL_CACHE_TTL", "2592000"))
DEFAULT_SIMKL_MAX_WORKERS = int(os.getenv("SIMKL_MAX_WORKERS", "6"))

CACHE_VERSION = 7
SIMKL_BASE_URL = "https://api.simkl.com"
SIMKL_URL_RE = re.compile(r"/(movies|tv|anime)/(\d+)(?:/|$)")
SIMKL_ENDPOINTS = {"movies", "tv", "anime"}


@dataclass(frozen=True)
class SyncStats:
    watchlist_items: int = 0
    additions: int = 0
    removals: int = 0
    unresolved: int = 0
    simkl_resolved: int = 0
    simkl_refreshed: int = 0
    collections_reordered: int = 0
    collections_skipped: int = 0


# -----------------------------------------------------------------------------
# Cache
# -----------------------------------------------------------------------------

def load_cache(path: Path) -> dict[str, Any]:
    cache: dict[str, Any] = {}

    if path.exists():
        try:
            with path.open("r", encoding="utf-8") as file:
                loaded = json.load(file)
        except (OSError, json.JSONDecodeError) as exc:
            logging.warning("Unable to read cache %s: %s", path, exc)
            loaded = {}

        if isinstance(loaded, dict):
            cache = loaded
        else:
            logging.warning("Invalid cache root; rebuilding cache")
            cache = {}

    cache.setdefault("version", CACHE_VERSION)
    cache.setdefault("last_run", None)
    cache.setdefault("items", {})
    cache.setdefault("collections", {})

    if not isinstance(cache["items"], dict):
        logging.warning("Invalid item cache; rebuilding it")
        cache["items"] = {}

    if not isinstance(cache["collections"], dict):
        logging.warning("Invalid collection cache; rebuilding it")
        cache["collections"] = {}

    cache["collections"].setdefault("last_sort_mode", None)

    # Schema migration (if needed)
    if cache.get("version") != CACHE_VERSION:
        for entry in cache["items"].values():
            if isinstance(entry, dict):
                entry.pop("simkl", None)
        cache["collections"] = {"last_sort_mode": None}
        logging.info("Cache schema changed; SIMKL metadata and collection order will rebuild")

    cache["version"] = CACHE_VERSION
    return cache


def save_cache(path: Path, cache: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cache["last_run"] = datetime.now(timezone.utc).isoformat()
    temp_path = path.with_suffix(path.suffix + ".tmp")

    with temp_path.open("w", encoding="utf-8") as file:
        json.dump(cache, file, ensure_ascii=False, indent=2, sort_keys=True)

    temp_path.replace(path)


def utc_now() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def is_fresh(record: dict[str, Any] | None, ttl: int, now: int) -> bool:
    if not isinstance(record, dict):
        return False

    try:
        fetched_at = int(record.get("fetched_at"))
    except (TypeError, ValueError):
        return False

    return now - fetched_at < ttl


# -----------------------------------------------------------------------------
# Logging and HTTP
# -----------------------------------------------------------------------------


def setup_logging(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def make_simkl_session(args: argparse.Namespace) -> requests.Session:
    """Build a per-worker SIMKL session with retries for transient failures."""
    session = requests.Session()
    session.params.update(
        {
            "client_id": args.simkl_client_id,
            "app-name": args.simkl_app_name,
            "app-version": args.simkl_app_version,
        }
    )
    session.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": f"{args.simkl_app_name}/{args.simkl_app_version}",
        }
    )

    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=8, pool_maxsize=8)
    session.mount("https://", adapter)
    return session


# -----------------------------------------------------------------------------
# Plex watchlist and collections
# -----------------------------------------------------------------------------


def connect_plex(base_url: str, token: str) -> tuple[MyPlexAccount, PlexServer]:
    if not token:
        raise RuntimeError("PLEX_TOKEN is required")

    return MyPlexAccount(token=token), PlexServer(base_url, token)


def fetch_watchlist(account: MyPlexAccount) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}

    movies = account.watchlist(libtype="movie")
    shows = account.watchlist(libtype="show")

    for item in movies + shows:
        guid = getattr(item, "guid", None)
        item_type = getattr(item, "type", None)

        if not guid or item_type not in {"movie", "show"}:
            continue

        result[guid] = {
            "guid": guid,
            "type": item_type,
            "title": getattr(item, "title", ""),
            "year": getattr(item, "year", None),
        }

    logging.info("Fetched %d watchlist items", len(result))
    return result


def get_or_create_collection(plex: PlexServer, section_name: str, name: str):
    section = plex.library.section(section_name)

    for collection in section.collections():
        if collection.title == name:
            return collection

    placeholder_items = section.all(maxresults=1)
    if not placeholder_items:
        raise RuntimeError(
            f"Cannot create collection {name!r}: library section {section_name!r} is empty"
        )

    collection = plex.createCollection(name, section, items=placeholder_items)
    collection.removeItems(placeholder_items)
    logging.info("Created Plex collection %s in %s", name, section_name)
    return collection


def item_rating_key(item: Any) -> int | None:
    try:
        value = getattr(item, "ratingKey", None)
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def resolve_local_items(
    plex: PlexServer,
    current: dict[str, dict[str, Any]],
    cache_items: dict[str, dict[str, Any]],
) -> dict[str, Any | None]:
    """Search Plex only for new/unresolved GUIDs.

    A cached ratingKey is retained without a per-run metadata fetch, which is
    the main reduction in local PMS calls on no-change runs.
    """
    resolved: dict[str, Any | None] = {}

    for guid, meta in current.items():
        cached = cache_items.get(guid, {})
        if cached.get("ratingKey"):
            resolved[guid] = None
            continue

        try:
            matches = plex.library.search(guid=guid, libtype=meta["type"])
        except Exception as exc:
            logging.warning("Plex GUID search failed for %s: %s", guid, exc)
            resolved[guid] = None
            continue

        resolved[guid] = matches[0] if matches else None

    return resolved


def update_collections(
    plex: PlexServer,
    movie_collection: Any,
    show_collection: Any,
    cache: dict[str, Any],
    current: dict[str, dict[str, Any]],
    dry_run: bool,
) -> tuple[dict[str, Any], dict[str, bool], SyncStats]:
    cache_items = cache["items"]
    current_guids = set(current)
    changed = {"movie": False, "show": False}

    removals: dict[str, list[Any]] = {"movie": [], "show": []}
    for guid in set(cache_items) - current_guids:
        entry = cache_items.pop(guid)
        item_type = entry.get("type")
        rating_key = entry.get("ratingKey")

        if item_type not in removals or not rating_key:
            continue

        try:
            removals[item_type].append(plex.fetchItem(int(rating_key)))
        except Exception as exc:
            logging.warning("Unable to fetch removed item %s: %s", guid, exc)

    if not dry_run:
        if removals["movie"]:
            movie_collection.removeItems(removals["movie"])
            changed["movie"] = True
        if removals["show"]:
            show_collection.removeItems(removals["show"])
            changed["show"] = True

    resolved = resolve_local_items(plex, current, cache_items)
    additions: dict[str, list[Any]] = {"movie": [], "show": []}
    unresolved_count = 0

    for guid, meta in current.items():
        old_entry = cache_items.get(guid, {})
        local_item = resolved[guid]

        if old_entry.get("ratingKey"):
            cache_items[guid] = {
                **old_entry,
                **meta,
                "unresolved": False,
            }
            continue

        if local_item is None:
            cache_items[guid] = {
                **old_entry,
                **meta,
                "ratingKey": None,
                "unresolved": True,
            }
            unresolved_count += 1
            continue

        rating_key = item_rating_key(local_item)
        cache_items[guid] = {
            **old_entry,
            **meta,
            "ratingKey": rating_key,
            "unresolved": False,
        }

        if rating_key is not None:
            additions[meta["type"]].append(local_item)

    if not dry_run:
        if additions["movie"]:
            movie_collection.addItems(additions["movie"])
            changed["movie"] = True
        if additions["show"]:
            show_collection.addItems(additions["show"])
            changed["show"] = True

    stats = SyncStats(
        additions=len(additions["movie"]) + len(additions["show"]),
        removals=len(removals["movie"]) + len(removals["show"]),
        unresolved=unresolved_count,
    )
    return cache, changed, stats


def get_guid_ids(media_item: Any) -> dict[str, str]:
    """Extract local Plex provider IDs usable by SIMKL redirect lookups."""
    ids: dict[str, str] = {}

    for guid in getattr(media_item, "guids", []) or []:
        raw = getattr(guid, "id", "")
        if "://" not in raw:
            continue

        provider, value = raw.split("://", 1)
        provider = provider.lower()
        if provider in {"tmdb", "tvdb", "imdb"} and value:
            ids[provider] = value

    return ids


# -----------------------------------------------------------------------------
# SIMKL
# -----------------------------------------------------------------------------


def simkl_candidates(
    plex_type: str,
    provider_ids: dict[str, str],
) -> tuple[tuple[str, str, str, str], ...]:
    """Return exclusive SIMKL resolver candidates in priority order.

    The anime candidates are fallback-only. They are reached only when no movie
    or TV candidate returned a valid SIMKL redirect for the current item.
    """
    if plex_type == "movie":
        candidates = (
            ("movie", "movies", "tmdb", provider_ids.get("tmdb")),
            ("movie", "movies", "imdb", provider_ids.get("imdb")),
            ("anime", "anime", "imdb", provider_ids.get("imdb")),
        )
    else:
        candidates = (
            ("tv", "tv", "tvdb", provider_ids.get("tvdb")),
            ("tv", "tv", "imdb", provider_ids.get("imdb")),
            ("anime", "anime", "imdb", provider_ids.get("imdb")),
        )

    return tuple(candidate for candidate in candidates if candidate[3])


def resolve_simkl(
    session: requests.Session,
    plex_type: str,
    provider_ids: dict[str, str],
) -> dict[str, Any] | None:
    """Resolve exactly one SIMKL record using the ordered candidate list."""
    for requested_type, expected_endpoint, provider, external_id in simkl_candidates(
        plex_type,
        provider_ids,
    ):
        try:
            response = session.get(
                f"{SIMKL_BASE_URL}/redirect",
                params={
                    "to": "simkl",
                    "type": requested_type,
                    provider: external_id,
                },
                timeout=20,
                allow_redirects=False,
            )
        except requests.RequestException as exc:
            logging.warning("SIMKL redirect failed for %s=%s (%s): %s", provider, external_id, requested_type, exc)
            continue

        if response.status_code != 301:
            logging.debug("SIMKL did not resolve %s=%s as %s (HTTP %s)", provider, external_id, requested_type, response.status_code)
            continue

        location = response.headers.get("Location", "")
        match = SIMKL_URL_RE.search(location)
        if not match:
            logging.warning("SIMKL redirect had an unparsable Location: %s", location)
            continue

        endpoint, simkl_id = match.group(1), int(match.group(2))
        if endpoint != expected_endpoint:
            logging.warning("SIMKL returned %s while resolving expected %s; ignoring result", endpoint, expected_endpoint)
            continue

        logging.debug("SIMKL resolved %s=%s as %s/%s", provider, external_id, endpoint, simkl_id)
        return {
            "id": simkl_id,
            "endpoint": endpoint,
            "source": provider,
        }

    return None


def fetch_simkl_metadata(
    session: requests.Session,
    simkl_id: int,
    endpoint: str,
) -> dict[str, Any] | None:
    if endpoint not in SIMKL_ENDPOINTS:
        raise ValueError(f"Unsupported SIMKL endpoint: {endpoint}")

    url = f"{SIMKL_BASE_URL}/{endpoint}/{simkl_id}"
    try:
        response = session.get(url, timeout=20)
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        logging.warning("SIMKL metadata fetch failed for %s: %s", url, exc)
        return None

    if not isinstance(payload, dict):
        logging.warning("Unexpected SIMKL response for %s", url)
        return None

    try:
        rank = int(payload["rank"]) if payload.get("rank") is not None else None
    except (TypeError, ValueError):
        rank = None

    metadata: dict[str, Any] = {"rank": rank}
    if endpoint == "anime":
        metadata.update(
            {
                "anime_type": payload.get("anime_type"),
                "total_episodes": payload.get("total_episodes"),
                "runtime": payload.get("runtime"),
            }
        )

    return metadata


def populate_simkl_metadata(
    plex: PlexServer,
    cache: dict[str, Any],
    args: argparse.Namespace,
) -> SyncStats:
    """Refresh stale SIMKL records with one 30-day TTL for all SIMKL data."""
    if not args.simkl_client_id:
        return SyncStats()

    now = utc_now()
    stale = [
        (guid, entry)
        for guid, entry in cache["items"].items()
        if not entry.get("unresolved")
        and entry.get("ratingKey")
        and not is_fresh(entry.get("simkl"), args.simkl_cache_ttl, now)
    ]

    if not stale:
        logging.debug("SIMKL metadata fully fresh; no API calls needed")
        return SyncStats()

    logging.info("SIMKL metadata: %d items need refresh", len(stale))

    def refresh_one(task: tuple[str, dict[str, Any]]) -> tuple[str, dict[str, Any] | None]:
        guid, entry = task

        try:
            local_item = plex.fetchItem(int(entry["ratingKey"]))
        except Exception as exc:
            logging.warning("Unable to fetch %s for SIMKL resolution: %s", guid, exc)
            return guid, None

        # Session instances are intentionally not shared across worker threads.
        session = make_simkl_session(args)
        try:
            resolved = resolve_simkl(
                session,
                entry["type"],
                get_guid_ids(local_item),
            )
            if resolved is None:
                return guid, None

            metadata = fetch_simkl_metadata(
                session,
                resolved["id"],
                resolved["endpoint"],
            )
            if metadata is None:
                return guid, None

            return guid, {
                **resolved,
                **metadata,
                "fetched_at": now,
            }
        finally:
            session.close()

    refreshed = 0
    resolved_count = 0

    with ThreadPoolExecutor(max_workers=args.simkl_max_workers) as executor:
        futures = {executor.submit(refresh_one, task): task[0] for task in stale}

        for future in as_completed(futures):
            fallback_guid = futures[future]
            try:
                guid, record = future.result()
            except Exception as exc:
                logging.warning("SIMKL worker failed for %s: %s", fallback_guid, exc)
                continue

            if record is None:
                continue

            old_record = cache["items"][guid].get("simkl")
            if not isinstance(old_record, dict) or old_record.get("id") != record["id"]:
                resolved_count += 1

            cache["items"][guid]["simkl"] = record
            refreshed += 1

            logging.debug(
                "Cached SIMKL %s/%s for %s (rank=%s)",
                record["endpoint"],
                record["id"],
                guid,
                record["rank"],
            )

    return SyncStats(
        simkl_resolved=resolved_count,
        simkl_refreshed=refreshed,
    )


# -----------------------------------------------------------------------------
# Collection order cache
# -----------------------------------------------------------------------------


def collection_cache_key(collection: Any) -> str:
    section_id = getattr(collection, "librarySectionID", "unknown")
    return f"{section_id}:{collection.title}"


def order_signature(items: Iterable[Any]) -> list[int]:
    return [
        rating_key
        for item in items
        if (rating_key := item_rating_key(item)) is not None
    ]


def get_cached_order(cache: dict[str, Any], collection: Any) -> list[int] | None:
    record = cache["collections"].get(collection_cache_key(collection))
    if not isinstance(record, dict):
        return None

    rating_keys = record.get("rating_keys")
    if not isinstance(rating_keys, list):
        return None

    try:
        return [int(value) for value in rating_keys]
    except (TypeError, ValueError):
        return None

def store_order(cache: dict[str, Any], collection: Any, rating_keys: list[int]) -> None:
    cache["collections"][collection_cache_key(collection)] = {
        "rating_keys": rating_keys,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def invalidate_order(cache: dict[str, Any], collection: Any) -> None:
    cache["collections"].pop(collection_cache_key(collection), None)


def simkl_sort_key(item: Any, cache: dict[str, Any]) -> tuple[bool, int, str]:
    guid = getattr(item, "guid", None)
    simkl = cache["items"].get(guid, {}).get("simkl", {})
    rank = simkl.get("rank") if isinstance(simkl, dict) else None

    return (
        rank is None,
        rank if isinstance(rank, int) else 2**31 - 1,
        getattr(item, "title", "").casefold(),
    )

def get_last_sort_mode(cache: dict[str, Any]) -> str | None:
    return cache["collections"].get("last_sort_mode")


def set_last_sort_mode(cache: dict[str, Any], mode: str) -> None:
    cache["collections"]["last_sort_mode"] = mode

def sort_collection(
    collection: Any,
    mode: str,
    cache: dict[str, Any],
    dry_run: bool,
    force_custom_mode: bool,
) -> tuple[bool, bool]:
    if mode == "alpha":
        logging.info("Setting %s to alphabetical collection ordering", collection.title)
        if not dry_run:
            collection.sortUpdate("alpha")
        return True, False

    if mode == "release":
        logging.info("Setting %s to release-date collection ordering", collection.title)
        if not dry_run:
            collection.sortUpdate("release")
        return True, False

    if mode != "simkl":
        raise ValueError("COLLECTION_SORT must be 'alpha', 'release' or 'simkl'")

    collection_items = collection.items()
    desired_items = sorted(
        collection_items,
        key=lambda item: simkl_sort_key(item, cache),
    )
    desired_order = order_signature(desired_items)
    cached_order = get_cached_order(cache, collection)

    if cached_order == desired_order:
        if force_custom_mode and not dry_run:
            logging.info("%s SIMKL order unchanged (%d items), switching sort mode back to custom", collection.title, len(desired_order))
            collection.sortUpdate("custom")
        elif force_custom_mode:
            logging.info("%s SIMKL order unchanged (%d items), would switch sort mode to custom (dry-run)", collection.title, len(desired_order))
        else:
            logging.info("%s SIMKL order unchanged (%d items); skipping Plex reorder", collection.title, len(desired_order))

        return False, True

    # Order changed: re-apply moves as before
    logging.info(
        "Applying SIMKL custom order to %s (%d items)",
        collection.title,
        len(desired_order),
    )

    if dry_run:
        return False, False

    collection.sortUpdate("custom")
    after = None
    for item in desired_items:
        if item_rating_key(item) is None:
            continue
        collection.moveItem(item, after=after)
        after = item

    store_order(cache, collection, desired_order)
    return True, False


# -----------------------------------------------------------------------------
# CLI and orchestration
# -----------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync Plex Discover watchlist to local collections with optional SIMKL sorting"
    )
    parser.add_argument("--plex-base-url", default=DEFAULT_PLEX_BASE_URL)
    parser.add_argument("--plex-token", default=DEFAULT_PLEX_TOKEN)
    parser.add_argument("--cache-path", default=DEFAULT_CACHE_PATH)
    parser.add_argument("--movie-collection", default=DEFAULT_MOVIE_COLLECTION_NAME)
    parser.add_argument("--show-collection", default=DEFAULT_SHOW_COLLECTION_NAME)
    parser.add_argument("--movies-section", default=DEFAULT_MOVIES_SECTION)
    parser.add_argument("--shows-section", default=DEFAULT_SHOWS_SECTION)
    parser.add_argument("--sort", dest="sort_mode", default=DEFAULT_COLLECTION_SORT, choices=("alpha", "release", "simkl"))
    parser.add_argument("--simkl-client-id", default=DEFAULT_SIMKL_CLIENT_ID)
    parser.add_argument("--simkl-app-name", default=DEFAULT_SIMKL_APP_NAME)
    parser.add_argument("--simkl-app-version", default=DEFAULT_SIMKL_APP_VERSION)
    parser.add_argument("--simkl-cache-ttl", type=int, default=DEFAULT_SIMKL_CACHE_TTL, help="SIMKL metadata TTL in seconds (default: 2592000 / 30 days)")
    parser.add_argument("--simkl-max-workers", type=int, default=DEFAULT_SIMKL_MAX_WORKERS)
    parser.add_argument("--force-reorder", action="store_true", help="Discard saved SIMKL order and apply the desired order again")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def merge_stats(*stats: SyncStats) -> SyncStats:
    return SyncStats(
        watchlist_items=sum(stat.watchlist_items for stat in stats),
        additions=sum(stat.additions for stat in stats),
        removals=sum(stat.removals for stat in stats),
        unresolved=sum(stat.unresolved for stat in stats),
        simkl_resolved=sum(stat.simkl_resolved for stat in stats),
        simkl_refreshed=sum(stat.simkl_refreshed for stat in stats),
        collections_reordered=sum(stat.collections_reordered for stat in stats),
        collections_skipped=sum(stat.collections_skipped for stat in stats),
    )


def main() -> None:
    args = parse_args()
    setup_logging(args.verbose)

    cache_path = Path(args.cache_path)
    cache = load_cache(cache_path)
    account, plex = connect_plex(args.plex_base_url, args.plex_token)
    watchlist = fetch_watchlist(account)

    movie_collection = get_or_create_collection(
        plex,
        args.movies_section,
        args.movie_collection,
    )
    show_collection = get_or_create_collection(
        plex,
        args.shows_section,
        args.show_collection,
    )

    cache, membership_changed, collection_stats = update_collections(
        plex,
        movie_collection,
        show_collection,
        cache,
        watchlist,
        args.dry_run,
    )

    if membership_changed["movie"]:
        invalidate_order(cache, movie_collection)
    if membership_changed["show"]:
        invalidate_order(cache, show_collection)

    if args.force_reorder:
        invalidate_order(cache, movie_collection)
        invalidate_order(cache, show_collection)

    simkl_stats = SyncStats()
    if args.sort_mode == "simkl" and args.simkl_client_id:
        simkl_stats = populate_simkl_metadata(plex, cache, args)

    last_mode = get_last_sort_mode(cache)
    force_custom_mode = (
        args.sort_mode == "simkl"
        and last_mode in {"alpha", "release"}
    )

    movie_reordered, movie_skipped = sort_collection(
        movie_collection,
        args.sort_mode,
        cache,
        args.dry_run,
        force_custom_mode,
    )
    show_reordered, show_skipped = sort_collection(
        show_collection,
        args.sort_mode,
        cache,
        args.dry_run,
        force_custom_mode,
    )

    set_last_sort_mode(cache, args.sort_mode)

    stats = merge_stats(
        SyncStats(watchlist_items=len(watchlist)),
        collection_stats,
        simkl_stats,
        SyncStats(
            collections_reordered=int(movie_reordered) + int(show_reordered),
            collections_skipped=int(movie_skipped) + int(show_skipped),
        ),
    )

    logging.info(
        "Summary: watchlist=%d additions=%d removals=%d unresolved=%d "
        "simkl_resolved=%d simkl_refreshed=%d collections_reordered=%d collections_skipped=%d",
        stats.watchlist_items,
        stats.additions,
        stats.removals,
        stats.unresolved,
        stats.simkl_resolved,
        stats.simkl_refreshed,
        stats.collections_reordered,
        stats.collections_skipped,
    )

    if not args.dry_run:
        save_cache(cache_path, cache)


if __name__ == "__main__":
    main()
