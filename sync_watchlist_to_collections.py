#!/usr/bin/env python3
"""Sync Plex Discover watchlist to local collections, with optional SIMKL rank sorting."""

import argparse
import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from plexapi.myplex import MyPlexAccount
from plexapi.server import PlexServer

# Plex configuration
DEFAULT_CACHE_PATH = os.getenv("CACHE_PATH", "./plex-watchlist-cache.json")
DEFAULT_MOVIE_COLLECTION_NAME = os.getenv("MOVIE_COLLECTION_NAME", "Films à voir")
DEFAULT_SHOW_COLLECTION_NAME = os.getenv("SHOW_COLLECTION_NAME", "Séries à voir")
DEFAULT_PLEX_BASE_URL = os.getenv("PLEX_BASE_URL", "https://plex.sample")
DEFAULT_PLEX_TOKEN = os.getenv("PLEX_TOKEN", "")
DEFAULT_MOVIES_SECTION = os.getenv("MOVIES_SECTION", "Films")
DEFAULT_SHOWS_SECTION = os.getenv("SHOWS_SECTION", "Séries TV")

# Sorting and SIMKL configuration
DEFAULT_COLLECTION_SORT = os.getenv("COLLECTION_SORT", "alpha").strip().lower()
DEFAULT_SIMKL_CLIENT_ID = os.getenv("SIMKL_CLIENT_ID", "")
DEFAULT_SIMKL_APP_NAME = os.getenv("SIMKL_APP_NAME", "plex-watchlist-sync")
DEFAULT_SIMKL_APP_VERSION = os.getenv("SIMKL_APP_VERSION", "1.0")
DEFAULT_SIMKL_CACHE_TTL = int(os.getenv("SIMKL_CACHE_TTL", "2592000"))
DEFAULT_SIMKL_ID_CACHE_TTL = int(os.getenv("SIMKL_ID_CACHE_TTL", "31536000"))
DEFAULT_SIMKL_MAX_WORKERS = int(os.getenv("SIMKL_MAX_WORKERS", "8"))

SIMKL_BASE_URL = "https://api.simkl.com"
SIMKL_ID_RE = re.compile(r"/(?:movies|tv|anime)/(\d+)(?:/|$)")


def empty_cache() -> dict:
    return {"version": 2, "last_run": None, "items": {}}


def setup_logging(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def load_cache(path: Path) -> dict:
    if not path.exists():
        return empty_cache()
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        data.setdefault("version", 2)
        data.setdefault("items", {})
        return data
    except (OSError, json.JSONDecodeError) as exc:
        logging.warning("Unable to read cache %s: %s", path, exc)
        return empty_cache()


def save_cache(path: Path, cache: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cache["last_run"] = datetime.now(timezone.utc).isoformat()
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)
    temp_path.replace(path)


def connect_plex(base_url: str, token: str) -> tuple[MyPlexAccount, PlexServer]:
    if not token:
        raise RuntimeError("PLEX_TOKEN is required")
    account = MyPlexAccount(token=token)
    plex = PlexServer(base_url, token)
    return account, plex


def fetch_watchlist(account: MyPlexAccount) -> dict[str, dict]:
    movies = account.watchlist(libtype="movie")
    shows = account.watchlist(libtype="show")
    result = {}
    for item in movies + shows:
        guid = getattr(item, "guid", None)
        if not guid:
            continue
        item_type = getattr(item, "type", None) or "movie"
        result[guid] = {
            "guid": guid,
            "type": item_type,
            "title": getattr(item, "title", ""),
            "year": getattr(item, "year", None),
        }
    logging.info("Fetched %d watchlist items", len(result))
    return result


def resolve_local_item(plex: PlexServer, guid: str, item_type: str):
    try:
        results = plex.library.search(guid=guid, libtype=item_type)
    except Exception as exc:
        logging.warning("Search failed for %s: %s", guid, exc)
        return None
    return results[0] if results else None


def get_or_create_collection(plex: PlexServer, section_name: str, name: str):
    section = plex.library.section(section_name)
    for collection in section.collections():
        if collection.title == name:
            return collection
    placeholder = section.all()[0]
    collection = plex.createCollection(name, section, items=[placeholder])
    collection.removeItems([placeholder])
    return collection


def update_collections(plex, movie_collection, show_collection, cache, current, dry_run):
    cached_items = cache["items"]
    current_guids = set(current)

    # Removals
    to_remove = {"movie": [], "show": []}
    for guid in set(cached_items) - current_guids:
        entry = cached_items.pop(guid)
        if entry.get("ratingKey"):
            try:
                item = plex.fetchItem(int(entry["ratingKey"]))
                collection = movie_collection if entry["type"] == "movie" else show_collection
                to_remove[entry["type"]].append(item)
            except Exception as exc:
                logging.warning("Unable to remove %s: %s", guid, exc)

    if not dry_run:
        if to_remove["movie"]:
            movie_collection.removeItems(to_remove["movie"])
        if to_remove["show"]:
            show_collection.removeItems(to_remove["show"])

    # Additions / updates
    to_add = {"movie": [], "show": []}
    for guid, meta in current.items():
        entry = cached_items.get(guid, {})
        item = None
        if entry.get("ratingKey"):
            try:
                item = plex.fetchItem(int(entry["ratingKey"]))
            except Exception:
                pass
        if item is None:
            item = resolve_local_item(plex, guid, meta["type"])

        if item is not None:
            if not entry.get("ratingKey") and not dry_run:
                to_add[meta["type"]].append(item)
            cached_items[guid] = {
                **entry,
                **meta,
                "ratingKey": getattr(item, "ratingKey", None),
                "unresolved": False,
            }
        else:
            cached_items[guid] = {
                **entry,
                **meta,
                "ratingKey": None,
                "unresolved": True,
            }

    if not dry_run:
        if to_add["movie"]:
            movie_collection.addItems(to_add["movie"])
        if to_add["show"]:
            show_collection.addItems(to_add["show"])

    return cache


def utc_now() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def is_cache_fresh(entry: dict[str, Any] | None, ttl: int, now: int) -> bool:
    if not entry:
        return False
    fetched_at = entry.get("fetched_at")
    if fetched_at is None:
        return False
    try:
        fetched_at = int(fetched_at)
    except (TypeError, ValueError):
        return False
    return now - fetched_at < ttl


def get_guid_ids(media_item) -> dict[str, str]:
    """Return Plex provider IDs: tmdb, tvdb and imdb where present."""
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


def resolve_simkl_id(
    provider_ids: dict[str, str],
    media_type: str,
    args: argparse.Namespace,
) -> tuple[int | None, str | None]:
    """Resolve movie: TMDB -> IMDb. Resolve show: TVDB -> IMDb.

    SIMKL /redirect responds with a 301. The Location header contains the
    canonical SIMKL URL, including its numeric ID. Never follow the redirect.
    """
    if media_type == "movie":
        candidates = (("tmdb", provider_ids.get("tmdb")), ("imdb", provider_ids.get("imdb")))
        simkl_type = "movie"
    else:
        candidates = (("tvdb", provider_ids.get("tvdb")), ("imdb", provider_ids.get("imdb")))
        simkl_type = "tv"

    for provider, external_id in candidates:
        if not external_id:
            continue
        params = {
            "client_id": args.simkl_client_id,
            "app-name": args.simkl_app_name,
            "app-version": args.simkl_app_version,
            "to": "simkl",
            "type": simkl_type,
            provider: external_id,
        }
        try:
            response = requests.get(
                f"{SIMKL_BASE_URL}/redirect",
                params=params,
                headers={"Accept": "application/json", "User-Agent": f"{args.simkl_app_name}/{args.simkl_app_version}"},
                timeout=20,
                allow_redirects=False,
            )
        except requests.RequestException as exc:
            logging.warning("SIMKL redirect lookup failed for %s=%s: %s", provider, external_id, exc)
            continue

        if response.status_code != 301:
            logging.info("SIMKL could not resolve %s=%s (HTTP %s)", provider, external_id, response.status_code)
            continue

        location = response.headers.get("Location", "")
        match = SIMKL_ID_RE.search(location)
        if match:
            return int(match.group(1)), provider
        logging.warning("SIMKL redirect had no parsable media ID: %s", location)
    return None, None


def fetch_simkl_rank(simkl_id: int, media_type: str, args: argparse.Namespace) -> int | None:
    """Fetch only the top-level rank field from SIMKL detail endpoint."""
    endpoint = "movies" if media_type == "movie" else "tv"
    url = f"{SIMKL_BASE_URL}/{endpoint}/{simkl_id}"
    try:
        response = requests.get(
            url,
            params={
                "client_id": args.simkl_client_id,
                "app-name": args.simkl_app_name,
                "app-version": args.simkl_app_version,
            },
            headers={"Accept": "application/json", "User-Agent": f"{args.simkl_app_name}/{args.simkl_app_version}"},
            timeout=20,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        logging.warning("SIMKL rank HTTP error for %s: %s", url, exc)
        return None

    payload = response.json()
    rank = payload.get("rank")
    logging.debug(
        "SIMKL %s/%s → rank=%r (full keys: %s)",
        endpoint, simkl_id, rank, list(payload.keys()),
    )
    return int(rank) if rank is not None else None


def populate_simkl_metadata(plex, cache: dict, args: argparse.Namespace) -> None:
    """Resolve SIMKL IDs and fetch ranks only for items that actually need it."""
    if not args.simkl_client_id:
        return

    now = utc_now()
    items = cache.get("items", {})

    # --- Pass 1: ID resolution ---
    to_resolve = []
    for guid, entry in items.items():
        if entry.get("unresolved") or not entry.get("ratingKey"):
            continue
        simkl = entry.setdefault("simkl", {})
        id_entry = simkl.get("id", {})
        if not is_cache_fresh(id_entry, args.simkl_id_cache_ttl, now):
            to_resolve.append((guid, entry))

    if to_resolve:
        logging.info("SIMKL metadata: %d items need ID resolution", len(to_resolve))

        def resolve_work(guid_entry):
            guid, entry = guid_entry
            try:
                local_item = plex.fetchItem(int(entry["ratingKey"]))
            except Exception as exc:
                logging.warning("Could not fetch %s for SIMKL ID resolution: %s", guid, exc)
                return guid, None, None
            simkl_id, source = resolve_simkl_id(get_guid_ids(local_item), entry["type"], args)
            return guid, simkl_id, source

        with ThreadPoolExecutor(max_workers=args.simkl_max_workers) as ex:
            futures = {ex.submit(resolve_work, item): item for item in to_resolve}
            for future in as_completed(futures):
                guid, entry = futures[future]
                try:
                    guid, simkl_id, source = future.result()
                except Exception as exc:
                    logging.warning("SIMKL ID resolution failed: %s", exc)
                    continue
                if simkl_id is None:
                    continue
                entry.setdefault("simkl", {})["id"] = {
                    "value": simkl_id,
                    "source": source,
                    "fetched_at": now,
                }

    # --- Pass 2: Rank fetch (after IDs are resolved) ---
    to_fetch_rank = []
    for guid, entry in items.items():
        if entry.get("unresolved") or not entry.get("ratingKey"):
            continue
        simkl = entry.get("simkl", {})
        id_entry = simkl.get("id", {})
        simkl_id = id_entry.get("value") if id_entry else None
        if not simkl_id:
            continue
        rank_entry = simkl.get("rank")
        if not is_cache_fresh(rank_entry, args.simkl_cache_ttl, now):
            to_fetch_rank.append((guid, entry, int(simkl_id), entry["type"]))

    if to_fetch_rank:
        logging.info("SIMKL metadata: %d items need rank refresh", len(to_fetch_rank))

        def rank_work(task):
            guid, entry, simkl_id, media_type = task
            rank = fetch_simkl_rank(simkl_id, media_type, args)
            return guid, simkl_id, rank

        with ThreadPoolExecutor(max_workers=args.simkl_max_workers) as ex:
            futures = {ex.submit(rank_work, t): t for t in to_fetch_rank}
            for future in as_completed(futures):
                guid, entry, simkl_id, _ = futures[future]
                try:
                    guid, simkl_id, rank = future.result()
                except Exception as exc:
                    logging.warning("SIMKL rank fetch failed: %s", exc)
                    continue
                entry = cache["items"].get(guid)
                if not entry:
                    logging.warning("Cache entry missing for %s when writing rank", guid)
                    continue
                entry.setdefault("simkl", {})["rank"] = {
                    "value": rank,
                    "fetched_at": now,
                }
                logging.debug(
                    "Cached rank for %s: simkl_id=%s, rank=%s",
                    guid, simkl_id, rank,
                )

    if not to_resolve and not to_fetch_rank:
        logging.debug("SIMKL metadata fully fresh; no API calls needed")


def simkl_sort_key(item, cache: dict) -> tuple[int, int, str]:
    guid = getattr(item, "guid", None)
    value = cache.get("items", {}).get(guid, {}).get("simkl", {}).get("rank", {}).get("value")
    return (value is None, value if value is not None else 2**31 - 1, item.title.casefold())


def sort_collection(collection, mode: str, cache: dict, dry_run: bool) -> None:
    if mode == "alpha":
        logging.info("Setting %s to alphabetical collection ordering", collection.title)
        if not dry_run:
            collection.sortUpdate("alpha")
        return
    if mode != "simkl":
        raise ValueError("COLLECTION_SORT must be either 'alpha' or 'simkl'")

    ordered_items = sorted(collection.items(), key=lambda item: simkl_sort_key(item, cache))
    logging.info("Setting %s to custom SIMKL-rank ordering (%d items)", collection.title, len(ordered_items))
    if dry_run:
        return
    after = None
    for item in ordered_items:
        collection.moveItem(item, after=after)
        after = item
    collection.sortUpdate("custom")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Sync Plex Discover watchlist to local collections, with optional SIMKL rank sorting"
    )
    parser.add_argument("--plex-base-url", default=DEFAULT_PLEX_BASE_URL)
    parser.add_argument("--plex-token", default=DEFAULT_PLEX_TOKEN)
    parser.add_argument("--cache-path", default=DEFAULT_CACHE_PATH)
    parser.add_argument("--movie-collection", default=DEFAULT_MOVIE_COLLECTION_NAME)
    parser.add_argument("--show-collection", default=DEFAULT_SHOW_COLLECTION_NAME)
    parser.add_argument("--movies-section", default=DEFAULT_MOVIES_SECTION)
    parser.add_argument("--shows-section", default=DEFAULT_SHOWS_SECTION)
    parser.add_argument("--sort", dest="sort_mode", default=DEFAULT_COLLECTION_SORT, choices=("alpha", "simkl"))
    parser.add_argument("--simkl-client-id", default=DEFAULT_SIMKL_CLIENT_ID)
    parser.add_argument("--simkl-app-name", default=DEFAULT_SIMKL_APP_NAME)
    parser.add_argument("--simkl-app-version", default=DEFAULT_SIMKL_APP_VERSION)
    parser.add_argument("--simkl-cache-ttl", type=int, default=DEFAULT_SIMKL_CACHE_TTL)
    parser.add_argument("--simkl-id-cache-ttl", type=int, default=DEFAULT_SIMKL_ID_CACHE_TTL)
    parser.add_argument("--simkl-max-workers", type=int, default=DEFAULT_SIMKL_MAX_WORKERS)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    setup_logging(args.verbose)

    cache_path = Path(args.cache_path)
    cache = load_cache(cache_path)
    account, plex = connect_plex(args.plex_base_url, args.plex_token)
    current = fetch_watchlist(account)

    movie_collection = get_or_create_collection(plex, args.movies_section, args.movie_collection)
    show_collection = get_or_create_collection(plex, args.shows_section, args.show_collection)
    cache = update_collections(plex, movie_collection, show_collection, cache, current, args.dry_run)

    # Always populate SIMKL metadata if a client ID is configured, even for alpha sorting.
    if args.simkl_client_id:
        populate_simkl_metadata(plex, cache, args)

    sort_collection(movie_collection, args.sort_mode, cache, args.dry_run)
    sort_collection(show_collection, args.sort_mode, cache, args.dry_run)
    save_cache(cache_path, cache)


if __name__ == "__main__":
    main()