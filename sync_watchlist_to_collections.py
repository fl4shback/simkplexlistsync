#!/usr/bin/env python3
"""Sync Plex Discover watchlist to local collections, with optional SIMKL rank sorting.

When sorting by SIMKL rank, the desired Plex ratingKey order is persisted in the
cache. Plex reorder writes are skipped when that desired order has not changed.
"""

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

CACHE_VERSION = 3
SIMKL_BASE_URL = "https://api.simkl.com"
SIMKL_ID_RE = re.compile(r"/(?:movies|tv|anime)/(\d+)(?:/|$)")


def empty_cache() -> dict:
    return {
        "version": CACHE_VERSION,
        "last_run": None,
        "items": {},
        "collections": {},
    }


def setup_logging(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def load_cache(path: Path) -> dict:
    if not path.exists():
        return empty_cache()

    try:
        with path.open("r", encoding="utf-8") as file:
            data = json.load(file)

        data.setdefault("version", CACHE_VERSION)
        data.setdefault("last_run", None)
        data.setdefault("items", {})
        data.setdefault("collections", {})

        if not isinstance(data["items"], dict):
            logging.warning("Invalid items cache; rebuilding it")
            data["items"] = {}

        if not isinstance(data["collections"], dict):
            logging.warning("Invalid collections cache; rebuilding it")
            data["collections"] = {}

        data["version"] = CACHE_VERSION
        return data

    except (OSError, json.JSONDecodeError) as exc:
        logging.warning("Unable to read cache %s: %s", path, exc)
        return empty_cache()


def save_cache(path: Path, cache: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cache["last_run"] = datetime.now(timezone.utc).isoformat()
    temp_path = path.with_suffix(path.suffix + ".tmp")

    with temp_path.open("w", encoding="utf-8") as file:
        json.dump(cache, file, ensure_ascii=False, indent=2)

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
    result: dict[str, dict] = {}

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

    library_items = section.all()
    if not library_items:
        raise RuntimeError(
            f"Cannot create collection {name!r}: library section {section_name!r} is empty"
        )

    placeholder = library_items[0]
    collection = plex.createCollection(name, section, items=[placeholder])
    collection.removeItems([placeholder])
    return collection


def update_collections(
    plex: PlexServer,
    movie_collection,
    show_collection,
    cache: dict,
    current: dict[str, dict],
    dry_run: bool,
) -> tuple[dict, dict[str, bool]]:
    cached_items = cache["items"]
    current_guids = set(current)
    collection_changed = {"movie": False, "show": False}

    # Removals
    to_remove = {"movie": [], "show": []}

    for guid in set(cached_items) - current_guids:
        entry = cached_items.pop(guid)
        item_type = entry.get("type")

        if item_type not in to_remove or not entry.get("ratingKey"):
            continue

        try:
            item = plex.fetchItem(int(entry["ratingKey"]))
            to_remove[item_type].append(item)
        except Exception as exc:
            logging.warning("Unable to remove %s: %s", guid, exc)

    if not dry_run:
        if to_remove["movie"]:
            movie_collection.removeItems(to_remove["movie"])
            collection_changed["movie"] = True

        if to_remove["show"]:
            show_collection.removeItems(to_remove["show"])
            collection_changed["show"] = True

    # Additions / updates
    to_add = {"movie": [], "show": []}

    for guid, meta in current.items():
        entry = cached_items.get(guid, {})
        item = None

        if entry.get("ratingKey"):
            try:
                item = plex.fetchItem(int(entry["ratingKey"]))
            except Exception:
                item = None

        if item is None:
            item = resolve_local_item(plex, guid, meta["type"])

        if item is not None:
            is_new_collection_item = not entry.get("ratingKey")

            if is_new_collection_item and not dry_run:
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
            collection_changed["movie"] = True

        if to_add["show"]:
            show_collection.addItems(to_add["show"])
            collection_changed["show"] = True

    return cache, collection_changed


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
    """Return Plex provider IDs: TMDB, TVDB and IMDb where present."""
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
    """Resolve movie: TMDB then IMDb; show: TVDB then IMDb.

    SIMKL /redirect responds with a 301. The Location header contains the
    canonical SIMKL URL, including its numeric ID. Redirects are not followed.
    """
    if media_type == "movie":
        candidates = (
            ("tmdb", provider_ids.get("tmdb")),
            ("imdb", provider_ids.get("imdb")),
        )
        simkl_type = "movie"
    else:
        candidates = (
            ("tvdb", provider_ids.get("tvdb")),
            ("imdb", provider_ids.get("imdb")),
        )
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
                headers={
                    "Accept": "application/json",
                    "User-Agent": (
                        f"{args.simkl_app_name}/{args.simkl_app_version}"
                    ),
                },
                timeout=20,
                allow_redirects=False,
            )
        except requests.RequestException as exc:
            logging.warning(
                "SIMKL redirect lookup failed for %s=%s: %s",
                provider,
                external_id,
                exc,
            )
            continue

        if response.status_code != 301:
            logging.info(
                "SIMKL could not resolve %s=%s (HTTP %s)",
                provider,
                external_id,
                response.status_code,
            )
            continue

        location = response.headers.get("Location", "")
        match = SIMKL_ID_RE.search(location)

        if match:
            return int(match.group(1)), provider

        logging.warning("SIMKL redirect had no parsable media ID: %s", location)

    return None, None


def fetch_simkl_rank(
    simkl_id: int,
    media_type: str,
    args: argparse.Namespace,
) -> int | None:
    """Fetch the top-level rank field from the SIMKL detail endpoint."""
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
            headers={
                "Accept": "application/json",
                "User-Agent": f"{args.simkl_app_name}/{args.simkl_app_version}",
            },
            timeout=20,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        logging.warning("SIMKL rank HTTP error for %s: %s", url, exc)
        return None

    payload = response.json()
    rank = payload.get("rank")

    logging.debug(
        "SIMKL %s/%s -> rank=%r (full keys: %s)",
        endpoint,
        simkl_id,
        rank,
        list(payload.keys()),
    )

    return int(rank) if rank is not None else None


def populate_simkl_metadata(
    plex: PlexServer,
    cache: dict,
    args: argparse.Namespace,
) -> None:
    """Resolve SIMKL IDs and fetch ranks only for entries with stale metadata."""
    if not args.simkl_client_id:
        return

    now = utc_now()
    items = cache.get("items", {})

    # Pass 1: SIMKL ID resolution.
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
                logging.warning(
                    "Could not fetch %s for SIMKL ID resolution: %s",
                    guid,
                    exc,
                )
                return guid, None, None

            simkl_id, source = resolve_simkl_id(
                get_guid_ids(local_item),
                entry["type"],
                args,
            )
            return guid, simkl_id, source

        with ThreadPoolExecutor(max_workers=args.simkl_max_workers) as executor:
            futures = {
                executor.submit(resolve_work, task): task
                for task in to_resolve
            }

            for future in as_completed(futures):
                guid, _ = futures[future]

                try:
                    guid, simkl_id, source = future.result()
                except Exception as exc:
                    logging.warning("SIMKL ID resolution failed: %s", exc)
                    continue

                if simkl_id is None:
                    continue

                entry = items.get(guid)
                if not entry:
                    continue

                entry.setdefault("simkl", {})["id"] = {
                    "value": simkl_id,
                    "source": source,
                    "fetched_at": now,
                }

    # Pass 2: SIMKL rank refresh.
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
            to_fetch_rank.append((guid, int(simkl_id), entry["type"]))

    if to_fetch_rank:
        logging.info("SIMKL metadata: %d items need rank refresh", len(to_fetch_rank))

        def rank_work(task):
            guid, simkl_id, media_type = task
            rank = fetch_simkl_rank(simkl_id, media_type, args)
            return guid, simkl_id, rank

        with ThreadPoolExecutor(max_workers=args.simkl_max_workers) as executor:
            futures = {
                executor.submit(rank_work, task): task
                for task in to_fetch_rank
            }

            for future in as_completed(futures):
                try:
                    guid, simkl_id, rank = future.result()
                except Exception as exc:
                    logging.warning("SIMKL rank fetch failed: %s", exc)
                    continue

                entry = items.get(guid)
                if not entry:
                    logging.warning(
                        "Cache entry missing for %s when writing SIMKL rank",
                        guid,
                    )
                    continue

                entry.setdefault("simkl", {})["rank"] = {
                    "value": rank,
                    "fetched_at": now,
                }

                logging.debug(
                    "Cached rank for %s: simkl_id=%s, rank=%s",
                    guid,
                    simkl_id,
                    rank,
                )

    if not to_resolve and not to_fetch_rank:
        logging.debug("SIMKL metadata fully fresh; no API calls needed")


def simkl_sort_key(item, cache: dict) -> tuple[bool, int, str]:
    guid = getattr(item, "guid", None)
    value = (
        cache.get("items", {})
        .get(guid, {})
        .get("simkl", {})
        .get("rank", {})
        .get("value")
    )

    return (
        value is None,
        value if value is not None else 2**31 - 1,
        getattr(item, "title", "").casefold(),
    )


def collection_cache_key(collection) -> str:
    """Return a cache key unique to a Plex library section and collection title."""
    section_key = getattr(collection, "librarySectionID", None)

    if section_key is None:
        section = getattr(collection, "section", None)
        section_key = getattr(section, "key", "unknown")

    return f"{section_key}:{collection.title}"


def item_rating_key(item) -> int | None:
    """Return an item's Plex ratingKey as an int, if available."""
    rating_key = getattr(item, "ratingKey", None)

    try:
        return int(rating_key) if rating_key is not None else None
    except (TypeError, ValueError):
        return None


def item_order_signature(items) -> list[int]:
    """Return the persisted custom-order signature for a sequence of Plex items."""
    signature = []

    for item in items:
        rating_key = item_rating_key(item)

        if rating_key is None:
            logging.warning(
                "Skipping %r in collection-order cache: missing ratingKey",
                getattr(item, "title", "<unknown>"),
            )
            continue

        signature.append(rating_key)

    return signature


def cached_collection_order(
    cache: dict,
    collection,
    mode: str,
) -> list[int] | None:
    entry = cache.get("collections", {}).get(collection_cache_key(collection))

    if not entry or entry.get("sort_mode") != mode:
        return None

    rating_keys = entry.get("rating_keys")
    if not isinstance(rating_keys, list):
        return None

    try:
        return [int(rating_key) for rating_key in rating_keys]
    except (TypeError, ValueError):
        logging.warning(
            "Invalid cached collection order for %s; rebuilding",
            collection.title,
        )
        return None


def save_collection_order(
    cache: dict,
    collection,
    mode: str,
    rating_keys: list[int],
) -> None:
    cache.setdefault("collections", {})[collection_cache_key(collection)] = {
        "sort_mode": mode,
        "rating_keys": rating_keys,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def invalidate_collection_order(cache: dict, collection) -> None:
    cache.setdefault("collections", {}).pop(collection_cache_key(collection), None)


def sort_collection(
    collection,
    mode: str,
    cache: dict,
    dry_run: bool,
) -> bool:
    """Set collection ordering, skipping no-op SIMKL reorder writes.

    The cached order is the previously applied desired ratingKey sequence.  It is
    intentionally authoritative: manual Plex reorders are not detected. Remove
    the corresponding cache entry or pass --force-reorder to repair manually
    changed order.
    """
    if mode == "alpha":
        logging.info("Setting %s to alphabetical collection ordering", collection.title)

        if dry_run:
            return False

        collection.sortUpdate("alpha")
        save_collection_order(cache, collection, "alpha", [])
        return True

    if mode != "simkl":
        raise ValueError("COLLECTION_SORT must be either 'alpha' or 'simkl'")

    collection_items = collection.items()
    ordered_items = sorted(
        collection_items,
        key=lambda item: simkl_sort_key(item, cache),
    )
    desired_order = item_order_signature(ordered_items)
    previous_order = cached_collection_order(cache, collection, "simkl")

    if previous_order == desired_order:
        logging.info(
            "%s SIMKL order unchanged (%d items); skipping Plex reorder",
            collection.title,
            len(desired_order),
        )
        return False

    logging.info(
        "%s SIMKL order changed; applying custom order for %d items",
        collection.title,
        len(desired_order),
    )

    if dry_run:
        logging.info(
            "Would update %s order: old=%s new=%s",
            collection.title,
            previous_order,
            desired_order,
        )
        return False

    collection.sortUpdate("custom")

    after = None
    for item in ordered_items:
        if item_rating_key(item) is None:
            continue

        collection.moveItem(item, after=after)
        after = item

    save_collection_order(cache, collection, "simkl", desired_order)
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Sync Plex Discover watchlist to local collections, "
            "with optional SIMKL rank sorting"
        )
    )

    parser.add_argument("--plex-base-url", default=DEFAULT_PLEX_BASE_URL)
    parser.add_argument("--plex-token", default=DEFAULT_PLEX_TOKEN)
    parser.add_argument("--cache-path", default=DEFAULT_CACHE_PATH)
    parser.add_argument("--movie-collection", default=DEFAULT_MOVIE_COLLECTION_NAME)
    parser.add_argument("--show-collection", default=DEFAULT_SHOW_COLLECTION_NAME)
    parser.add_argument("--movies-section", default=DEFAULT_MOVIES_SECTION)
    parser.add_argument("--shows-section", default=DEFAULT_SHOWS_SECTION)
    parser.add_argument(
        "--sort",
        dest="sort_mode",
        default=DEFAULT_COLLECTION_SORT,
        choices=("alpha", "simkl"),
    )
    parser.add_argument("--simkl-client-id", default=DEFAULT_SIMKL_CLIENT_ID)
    parser.add_argument("--simkl-app-name", default=DEFAULT_SIMKL_APP_NAME)
    parser.add_argument("--simkl-app-version", default=DEFAULT_SIMKL_APP_VERSION)
    parser.add_argument(
        "--simkl-cache-ttl",
        type=int,
        default=DEFAULT_SIMKL_CACHE_TTL,
    )
    parser.add_argument(
        "--simkl-id-cache-ttl",
        type=int,
        default=DEFAULT_SIMKL_ID_CACHE_TTL,
    )
    parser.add_argument(
        "--simkl-max-workers",
        type=int,
        default=DEFAULT_SIMKL_MAX_WORKERS,
    )
    parser.add_argument(
        "--force-reorder",
        action="store_true",
        help="Ignore cached SIMKL collection order and apply it again",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    setup_logging(args.verbose)

    cache_path = Path(args.cache_path)
    cache = load_cache(cache_path)
    account, plex = connect_plex(args.plex_base_url, args.plex_token)
    current = fetch_watchlist(account)

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

    cache, collection_changed = update_collections(
        plex,
        movie_collection,
        show_collection,
        cache,
        current,
        args.dry_run,
    )

    if collection_changed["movie"]:
        invalidate_collection_order(cache, movie_collection)

    if collection_changed["show"]:
        invalidate_collection_order(cache, show_collection)

    if args.force_reorder:
        invalidate_collection_order(cache, movie_collection)
        invalidate_collection_order(cache, show_collection)

    if args.sort_mode == "simkl" and args.simkl_client_id:
        populate_simkl_metadata(plex, cache, args)

    sort_collection(
        movie_collection,
        args.sort_mode,
        cache,
        args.dry_run,
    )
    sort_collection(
        show_collection,
        args.sort_mode,
        cache,
        args.dry_run,
    )

    if not args.dry_run:
        save_cache(cache_path, cache)


if __name__ == "__main__":
    main()
