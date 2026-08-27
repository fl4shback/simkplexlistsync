
FROM python:3.13-alpine

# Install runtime deps (ca-certificates for HTTPS to plex.tv)
RUN apk add --no-cache ca-certificates && python -m pip install --no-cache-dir plexapi

# Workdir
WORKDIR /app

# Copy script into image
COPY sync_watchlist_to_collections.py /app/sync_watchlist_to_collections.py

# Data volume for cache JSON
VOLUME ["/data"]

# Default command – can be overridden by Swarm cron
CMD ["python", "/app/sync_watchlist_to_collections.py"]
