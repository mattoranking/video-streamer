# pyright: reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownArgumentType=false
import os
import logging
from contextlib import asynccontextmanager
from typing import Any

import asyncpg  # type: ignore[reportMissingTypeStubs]
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from shared.models import (
    VideoCreateRequest,
    VideoUpdateRequest,
    VideoResponse,
    PaginatedVideoResponse,
    HealthResponse,
    VideoStatus,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("metadata-service")

DB_HOST: str = os.environ.get("DB_HOST", "postgres")
DB_NAME: str = os.environ.get("DB_NAME", "videodb")
DB_USER: str = os.environ.get("DB_USER", "postgres")
DB_PASS: str = os.environ.get("DB_PASS", "postgres")
DB_PORT: int = int(os.environ.get("DB_PORT", "5432"))

pool: asyncpg.Pool | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    pool = await asyncpg.create_pool(
        host=DB_HOST, port=DB_PORT, database=DB_NAME,
        user=DB_USER, password=DB_PASS, min_size=2, max_size=10
    )
    await init_db()
    logger.info("Metadata service started, DB pool ready.")
    yield
    if pool:
        await pool.close()


async def init_db() -> None:
    """Create the videos table if it doesn't exist."""
    if not pool:
        return
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS videos (
                video_id            TEXT PRIMARY KEY,
                title               TEXT NOT NULL,
                uploader_id         TEXT NOT NULL,
                filename            TEXT NOT NULL,
                status              TEXT NOT NULL DEFAULT 'uploaded',
                manifest_url        TEXT,
                resolutions         TEXT[],
                view_count          BIGINT NOT NULL DEFAULT 0,
                cdn_enabled         BOOLEAN NOT NULL DEFAULT FALSE,
                storage_location    TEXT,
                created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)

    logger.info("Database schema initialised.")


def _ensure_pool() -> "asyncpg.Pool":
    """Return the pool, raising 503 if unavailable."""
    if pool is None:
        raise HTTPException(
            status_code=503,
            detail="Database pool unavailable",
        )
    return pool


app = FastAPI(
    title="Metadata Service",
    description="CRUD for video metadata backed by PostgreSQL.",
    version="1.0.0",
    root_path="/api/videos",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def row_to_response(
    row: asyncpg.Record,
) -> VideoResponse:
    return VideoResponse(
        video_id=row["video_id"],
        title=row["title"],
        uploader_id=row["uploader_id"],
        filename=row["filename"],
        status=VideoStatus(row["status"]),
        manifest_url=row["manifest_url"],
        resolutions=list(row["resolutions"]) if row["resolutions"] else None,
        view_count=row["view_count"],
        cdn_enabled=row["cdn_enabled"],
        storage_location=row["storage_location"],
        created_at=row["created_at"],
    )


@app.get(
    "/health",
    response_model=HealthResponse,
    tags=["Health"]
)
async def health() -> HealthResponse:
    return HealthResponse(status="ok", service="metadata-service")


@app.post(
    "/",
    response_model=VideoResponse,
    status_code=201,
    tags=["Videos"],
)
async def create_video(
    payload: VideoCreateRequest,
) -> VideoResponse:
    """Register a new video record after upload."""
    db = _ensure_pool()
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO videos (
                video_id, title, uploader_id,
                filename, status, storage_location
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING *
            """,
            payload.video_id,
            payload.title,
            payload.uploader_id,
            payload.filename,
            payload.status.value,
            payload.storage_location,
        )

    logger.info("Created video record: %s", payload.video_id)
    return row_to_response(row)


@app.get(
    "/",
    response_model=PaginatedVideoResponse,
    tags=["Videos"]
)
async def list_videos(
    page: int = Query(default=1, ge=1, description="Page number"),
    per_page: int = Query(default=20, ge=1, le=100, description="Items per page"),
) -> PaginatedVideoResponse:
    """List all videos, newest first (paginated)."""
    db = _ensure_pool()
    offset = (page - 1) * per_page
    async with db.acquire() as conn:
        total: int = await conn.fetchval("SELECT COUNT(*) FROM videos")
        rows = await conn.fetch(
            "SELECT * FROM videos ORDER BY created_at DESC LIMIT $1 OFFSET $2",
            per_page, offset,
        )

    pages = max(1, -(-total // per_page))  # ceil division
    return PaginatedVideoResponse(
        items=[row_to_response(r) for r in rows],
        total=total,
        page=page,
        per_page=per_page,
        pages=pages,
    )


@app.get(
    "/search",
    response_model=list[VideoResponse],
    tags=["Videos"],
)
async def search_videos(
    q: str = Query(..., min_length=1, description="Search term"),
    limit: int = Query(default=20, ge=1, le=100),
) -> list[VideoResponse]:
    """Full-text search on title (case-insensitive)."""
    db = _ensure_pool()
    async with db.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT * FROM videos
            WHERE title ILIKE $1 AND status = 'ready'
            ORDER BY created_at DESC LIMIT $2
            """,
            f"%{q}%", limit,
        )
    return [row_to_response(r) for r in rows]


@app.get(
    "/{video_id}",
    response_model=VideoResponse,
    tags=["Videos"],
)
async def get_video(
    video_id: str,
) -> VideoResponse:
    """Fetch a single video by ID."""
    db = _ensure_pool()
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM videos WHERE video_id = $1",
            video_id,
        )
    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"Video '{video_id}' not found",
        )
    return row_to_response(row)


@app.patch(
    "/{video_id}",
    response_model=VideoResponse,
    tags=["Videos"],
)
async def update_video(
    video_id: str, payload: VideoUpdateRequest,
) -> VideoResponse:
    """Partial update used by the transcoding worker."""
    updates: dict[str, Any] = payload.model_dump(
        exclude_none=True,
    )
    if not updates:
        raise HTTPException(
            status_code=422,
            detail="No fields provided for update",
        )

    # Convert enum values to their string equivalents for DB
    if "status" in updates:
        updates["status"] = updates["status"].value

    if "resolutions" in updates:
        updates["resolutions"] = [
            r.value if hasattr(r, "value") else r
            for r in updates["resolutions"]
        ]

    cols = ", ".join(
        f"{k} = ${i + 2}" for i, k in enumerate(updates)
    )
    values = list(updates.values())

    db = _ensure_pool()
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE videos SET {cols}"
            f" WHERE video_id = $1 RETURNING *",
            video_id, *values,
        )

    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"Video '{video_id}' not found",
        )

    logger.info(
        "Updated video %s: %s", video_id, updates,
    )
    return row_to_response(row)


@app.delete(
    "/{video_id}",
    status_code=204,
    tags=["Videos"],
)
async def delete_video(video_id: str) -> None:
    """Delete a video record."""
    db = _ensure_pool()
    async with db.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM videos WHERE video_id = $1",
            video_id,
        )

    if result == "DELETE 0":
        raise HTTPException(
            status_code=404,
            detail=f"Video '{video_id}' not found",
        )

    logger.info("Deleted video %s", video_id)
