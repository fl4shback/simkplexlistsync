
#!/usr/bin/env python3
"""Sync Plex Discover watchlist to local Plex collections.

- Reads Plex account watchlist (Discover / Universal Watchlist).
- Diffs against a cached snapshot from the previous run.
- Applies incremental updates to two local collections in your Plex libraries:
  * Movies collection
  * Shows collection
- Tracks items that exist in Discover but not yet in the local server,
  and adds them once they become available.

Intended to be run inside Docker on a schedule (e.g. Swarm cron).
"""

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from plexapi.myplex import MyPlexAccount
from plexapi.server import PlexServer


DEFAULT_CACHE_PATH = os.getenv("CACHE_PATH", "/data/plex-watchlist-cache.json")
DEFAULT_MOVIE_COLLECTION_NAME = os.getenv("MOVIE_COLLECTION_NAME", "Plan to Watch (Movies)")
DEFAULT_SHOW_COLLECTION_NAME = os.getenv("SHOW_COLLECTION_NAME", "Plan to Watch (Shows)")
DEFAULT_PLEX_BASE_URL = os.getenv("PLEX_BASE_URL", "http://plex:32400")
DEFAULT_PLEX_TOKEN = os.getenv("PLEX_TOKEN", "")
DEFAULT_MOVIES_SECTION = os.getenv("MOVIES_SECTION", "Movies")
DEFAULT_SHOWS_SECTION = os.getenv("SHOWS_SECTION", "TV Shows")


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def load_cache(path: Path) -> dict:
    if not path.exists():
        logging.info("No existing cache at %s, starting fresh.", path)
        return {"version": 1, "last_run": None, "items": {}}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if "items" not in data:
            logging.warning("Cache missing 'items' key, resetting.")
            return {"version": 1, "last_run": None, "items": {}}
        return data
    except Exception as e:
        logging.error("Failed to load cache from %s: %s", path, e)
        return {"version": 1, "last_run": None, "items": {}}


def save_cache(path: Path, cache: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cache["last_run"] = datetime.now(timezone.utc).isoformat()
    with path.open("w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)
    logging.info("Updated cache at %s", path)


def connect_plex(base_url: str, token: str) -> tuple[MyPlexAccount, PlexServer]:
    if not token:
        raise RuntimeError("PLEX_TOKEN is required")
    account = MyPlexAccount(token=token)
    plex = PlexServer(base_url, token)
    logging.info("Connected to Plex server at %s", base_url)
    return account, plex


def fetch_watchlist(account: MyPlexAccount) -> dict:
    """Return dict of current watchlist items keyed by guid."""
    movies = account.watchlist(libtype="movie")
    shows = account.watchlist(libtype="show")

    current_items: dict[str, dict] = {}

    for item in movies + shows:
        guid = getattr(item, "guid", None)
        if not guid:
            logging.debug("Skipping item without guid: %s", item)
            continue
        item_type = getattr(item, "type", None) or (
            "movie" if getattr(item, "TYPE", "movie") == "movie" else "show"
        )
        current_items[guid] = {
            "guid": guid,
            "type": item_type,
            "title": getattr(item, "title", ""),
        }

    logging.info(
        "Fetched %d watchlist items (%d movies, %d shows)",
        len(current_items), len(movies), len(shows),
    )
    return current_items


def resolve_local_item(plex: PlexServer, guid: str, item_type: str):
    """Try to resolve a cloud watchlist item to a local library item via guid.

    Returns a Plex media object or None.
    """
    try:
        results = plex.library.search(guid=guid, libtype=item_type)
    except Exception as e:
        logging.error("Search failed for guid %s (%s): %s", guid, item_type, e)
        return None
    if not results:
        logging.debug("No local match for guid %s (%s)", guid, item_type)
        return None
    if len(results) > 1:
        logging.debug(
            "Multiple local matches for guid %s (%s), taking first.", guid, item_type
        )
    item = results[0]
    logging.debug(
        "Resolved guid %s (%s) to local item %s (ratingKey=%s)",
        guid,
        item_type,
        getattr(item, "title", ""),
        getattr(item, "ratingKey", None),
    )
    return item


def get_or_create_collection(plex: PlexServer, section_name: str, name: str):
    """Get or create a regular collection in the given library section.

    Collections are per-library; we look in the specified section.
    """
    try:
        section = plex.library.section(section_name)
    except Exception as e:
        raise RuntimeError(f"Library section '{section_name}' not found: {e}")

    try:
        collections = section.collections()
    except Exception as e:
        logging.error("Failed to list collections in section %s: %s", section_name, e)
        collections = []

    for coll in collections:
        if getattr(coll, "title", "") == name:
            logging.info("Using existing collection '%s' in section '%s'", name, section_name)
            return coll

    logging.info("Creating new collection '%s' in section '%s'", name, section_name)
    try:
        collection = plex.createCollection(name, section, items=[])
    except Exception as e:
        raise RuntimeError(f"Failed to create collection '{name}' in section '{section_name}': {e}")
    return collection


def update_collections(
    plex: PlexServer,
    movie_collection,
    show_collection,
    cached: dict,
    current_items: dict,
    dry_run: bool,
) -> dict:
    """Apply incremental updates and return updated cache dict."""
    cached_items = cached.get("items", {})
    cached_guids = set(cached_items.keys())
    current_guids = set(current_items.keys())

    added_guids = current_guids - cached_guids
    removed_guids = cached_guids - current_guids

    logging.info("Added %d items, removed %d items", len(added_guids), len(removed_guids))

    # Handle removals
    for guid in removed_guids:
        entry = cached_items.get(guid)
        if not entry:
            continue
        rating_key = entry.get("ratingKey")
        if not rating_key:
            logging.debug(
                "Removed guid %s had no ratingKey (likely never resolved), skipping.",
                guid,
            )
        else:
            try:
                item = plex.fetchItem(int(rating_key))
            except Exception as e:
                logging.warning(
                    "Failed to fetch item for ratingKey %s (guid %s): %s",
                    rating_key,
                    guid,
                    e,
                )
                item = None
            if item is not None:
                collection = (
                    movie_collection if entry.get("type") == "movie" else show_collection
                )
                logging.info(
                    "Removing item %s (guid=%s) from collection %s",
                    getattr(item, "title", ""),
                    guid,
                    collection.title,
                )
                if not dry_run:
                    try:
                        collection.removeItems([item])
                    except Exception as e:
                        logging.error(
                            "Failed to remove item %s from collection %s: %s",
                            getattr(item, "title", ""),
                            collection.title,
                            e,
                        )
        # Remove from cache
        cached_items.pop(guid, None)

    # Handle additions
    for guid in added_guids:
        meta = current_items[guid]
        item_type = meta["type"]
        item = resolve_local_item(plex, guid, item_type)
        if item is not None:
            rating_key = getattr(item, "ratingKey", None)
            collection = movie_collection if item_type == "movie" else show_collection
            logging.info(
                "Adding item %s (guid=%s) to collection %s",
                getattr(item, "title", ""),
                guid,
                collection.title,
            )
            if not dry_run:
                try:
                    collection.addItems([item])
                except Exception as e:
                    logging.error(
                        "Failed to add item %s to collection %s: %s",
                        getattr(item, "title", ""),
                        collection.title,
                        e,
                    )
            cached_items[guid] = {
                "guid": guid,
                "type": item_type,
                "title": meta.get("title", ""),
                "ratingKey": rating_key,
                "unresolved": False,
            }
        else:
            logging.info(
                "Watchlist item guid=%s (%s, title=%s) not yet on server, tracking as unresolved.",
                guid,
                item_type,
                meta.get("title", ""),
            )
            cached_items[guid] = {
                "guid": guid,
                "type": item_type,
                "title": meta.get("title", ""),
                "ratingKey": None,
                "unresolved": True,
            }

    # Try to resolve previously unresolved items
    for guid, entry in list(cached_items.items()):
        if not entry.get("unresolved"):
            continue
        if guid not in current_items:
            # No longer in watchlist; we should have removed it above, but guard anyway.
            cached_items.pop(guid, None)
            continue
        item_type = entry.get("type")
        item = resolve_local_item(plex, guid, item_type)
        if item is None:
            continue
        rating_key = getattr(item, "ratingKey", None)
        collection = movie_collection if item_type == "movie" else show_collection
        logging.info(
            "Previously unresolved item %s (guid=%s) now resolvable, adding to %s",
            getattr(item, "title", ""),
            guid,
            collection.title,
        )
        if not dry_run:
            try:
                collection.addItems([item])
            except Exception as e:
                logging.error(
                    "Failed to add previously unresolved item %s to collection %s: %s",
                    getattr(item, "title", ""),
                    collection.title,
                    e,
                )
        entry["ratingKey"] = rating_key
        entry["unresolved"] = False

    # Update titles/types from current snapshot for remaining items
    for guid in current_items.keys():
        if guid in cached_items:
            cached_items[guid]["title"] = current_items[guid].get("title", "")
            cached_items[guid]["type"] = current_items[guid].get("type")

    cached["items"] = cached_items
    return cached


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync Plex Discover watchlist to local collections",
    )
    parser.add_argument(
        "--plex-base-url",
        default=DEFAULT_PLEX_BASE_URL,
        help=f"Plex server base URL (default: {DEFAULT_PLEX_BASE_URL})",
    )
    parser.add_argument(
        "--plex-token",
        default=DEFAULT_PLEX_TOKEN,
        help="Plex token (defaults to PLEX_TOKEN env)",
    )
    parser.add_argument(
        "--cache-path",
        default=DEFAULT_CACHE_PATH,
        help=f"Path to cache file (default: {DEFAULT_CACHE_PATH})",
    )
    parser.add_argument(
        "--movie-collection",
        default=DEFAULT_MOVIE_COLLECTION_NAME,
        help=f"Movies collection name (default: {DEFAULT_MOVIE_COLLECTION_NAME})",
    )
    parser.add_argument(
        "--show-collection",
        default=DEFAULT_SHOW_COLLECTION_NAME,
        help=f"Shows collection name (default: {DEFAULT_SHOW_COLLECTION_NAME})",
    )
    parser.add_argument(
        "--movies-section",
        default=DEFAULT_MOVIES_SECTION,
        help=f"Name of Movies library section (default: {DEFAULT_MOVIES_SECTION})",
    )
    parser.add_argument(
        "--shows-section",
        default=DEFAULT_SHOWS_SECTION,
        help=f"Name of TV Shows library section (default: {DEFAULT_SHOWS_SECTION})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute diffs and log actions but do not modify collections",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    setup_logging(verbose=args.verbose)

    cache_path = Path(args.cache_path)
    cache = load_cache(cache_path)

    account, plex = connect_plex(args.plex_base_url, args.plex_token)

    current_items = fetch_watchlist(account)

    movie_collection = get_or_create_collection(
        plex, args.movies_section, args.movie_collection
    )
    show_collection = get_or_create_collection(
        plex, args.shows_section, args.show_collection
    )

    updated_cache = update_collections(
        plex,
        movie_collection,
        show_collection,
        cache,
        current_items,
        dry_run=args.dry_run,
    )

    save_cache(cache_path, updated_cache)
    logging.info("Sync complete (dry_run=%s)", args.dry_run)


if __name__ == "__main__":
    main()
