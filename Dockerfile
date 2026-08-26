
FROM python:3.13-alpine

# Install runtime deps (ca-certificates for HTTPS to plex.tv)
RUN apk add --no-cache ca-certificates && python -m pip install --no-cache-dir plexapi

# Workdir
WORKDIR /app

# Copy script into image
COPY sync_watchlist_to_playlists.py /app/sync_watchlist_to_playlists.py

# Default env placeholders (override in Swarm)
ENV PLEX_BASE_URL="http://plex:32400" \
    CACHE_PATH="/data/plex-watchlist-cache.json" \
    MOVIE_PLAYLIST_NAME="Movies Watchlist" \
    SHOW_PLAYLIST_NAME="TV Watchlist"

# Data volume for cache JSON
VOLUME ["/data"]

# Default command – can be overridden by Swarm cron
CMD ["python", "/app/sync_watchlist_to_playlists.py"]
