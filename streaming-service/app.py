import os
import re
import logging
from pathlib import Path
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response

from shared.models import HealthResponse, ViewCountResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("streaming-service")

PROCESSED_STORAGE: str = "/storage/processed"
REDIS_HOST: str = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT: int = int(os.environ.get("REDIS_PORT", "6379"))

redis_client: aioredis.Redis | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    redis_client = aioredis.from_url(
        f"redis://{REDIS_HOST}:{REDIS_PORT}", decode_responses=True
    )
    logger.info("Streaming service started.")
    yield
    await redis_client.aclose()


app = FastAPI(
    title="Streaming Service",
    description=(
        "Serves HLS manifests and video segments."
        " Tracks view counts via Redis."
    ),
    version="2.0.0",
    root_path="/api/stream",
    redirect_slashes=False,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def resolve_path(
    video_id: str, *parts: str,
) -> Path:
    """Resolve a path under PROCESSED_STORAGE.

    Guards against directory traversal.
    """
    base = Path(PROCESSED_STORAGE).resolve()
    target = (base / video_id / Path(*parts)).resolve()
    if not str(target).startswith(str(base)):
        raise HTTPException(
            status_code=400, detail="Invalid path",
        )
    return target


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health() -> HealthResponse:
    return HealthResponse(status="ok", service="streaming-service")


# Ensure CDN URLs in manifests have https:// protocol
_BARE_CDN_RE = re.compile(
    r"^([a-z0-9.-]+\.cloudfront\.net/)",
    re.MULTILINE,
)


@app.get(
    "/{video_id}/manifest.m3u8",
    tags=["Streaming"],
)
async def get_manifest(video_id: str) -> Response:
    """
    Serve the HLS master manifest for a video.
    Ensures any CDN URLs have the https:// scheme so hls.js treats
    them as absolute URLs and fetches playlists/segments from CloudFront.
    Increments the Redis view counter on each request.
    """
    path = resolve_path(video_id, "manifest.m3u8")
    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Video '{video_id}' not found or not ready yet.",
        )

    if redis_client:
        views: int = await redis_client.incr(  # type: ignore[misc]
            f"views:{video_id}",
        )
        logger.info(
            "Served manifest for %s | total views: %d",
            video_id, views,
        )

    # Fix bare CDN hostnames → add https://
    content = path.read_text()
    content = _BARE_CDN_RE.sub(r"https://\1", content)

    return Response(
        content=content,
        media_type="application/vnd.apple.mpegurl",
    )


@app.get(
    "/{video_id}/{resolution}/playlist.m3u8",
    response_class=FileResponse,
    tags=["Streaming"],
)
async def get_resolution_playlist(
    video_id: str, resolution: str,
) -> FileResponse:
    """Serve a per-resolution HLS playlist (e.g 720p/playlist.m3u8)."""
    path = resolve_path(
        video_id, resolution, "playlist.m3u8",
    )
    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail="Resolution playlist not found",
        )
    return FileResponse(
        path=str(path),
        media_type="application/vnd.apple.mpegurl",
    )


@app.get(
    "/{video_id}/{resolution}/{segment}",
    response_class=FileResponse,
    tags=["Streaming"],
)
async def get_segment(
    video_id: str, resolution: str, segment: str,
) -> FileResponse:
    """Serve a .ts video segment"""
    if not segment.endswith(".ts"):
        raise HTTPException(
            status_code=400,
            detail="Only .ts segments are served here.",
        )
    path = resolve_path(video_id, resolution, segment)
    if not path.exists():
        raise HTTPException(
            status_code=404, detail="Segment not found.",
        )
    return FileResponse(
        path=str(path), media_type="video/MP2T",
    )


@app.get(
    "/views/{video_id}",
    response_model=ViewCountResponse,
    tags=["Analytics"],
)
async def get_views(video_id: str) -> ViewCountResponse:
    """Return the current view count for a video from Redis."""
    if not redis_client:
        raise HTTPException(
            status_code=503, detail="Redis unavailable",
        )
    raw: str | None = await redis_client.get(
        f"views:{video_id}",
    )
    return ViewCountResponse(
        video_id=video_id, views=int(raw or 0),
    )
