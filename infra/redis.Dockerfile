# Registry-independent fallback: official Debian Redis package on cached Python base.
FROM python:3.12.10-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends redis-server && rm -rf /var/lib/apt/lists/*
USER redis
EXPOSE 6379
CMD ["redis-server", "--bind", "0.0.0.0", "--protected-mode", "no", "--save", "", "--appendonly", "no", "--maxmemory", "128mb", "--maxmemory-policy", "allkeys-lru"]
