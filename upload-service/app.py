import json
import os
import uuid
import logging
from typing import Annotated

import redis.asyncio as aioredis
import httpx
from fastapi import (
    FastAPI, File, Form, UploadFile,
    HTTPException, Depends,
)
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from shared.s3_utils import (
    upload_to_s3, S3_RAW_BUCKET, USE_S3,
)
from shared.models import (
    HealthResponse, UploadResponse,
    VideoCreateRequest, VideoStatus,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("upload-service")

UPLOAD_FOLDER: str = "/storage/raw"
ALLOWED_EXTENSIONS: frozenset[str] = frozenset(
    {"mp4", "avi", "mov", "mkv", "webm"}
)
REDIS_HOST: str = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT: int = int(os.environ.get("REDIS_PORT", "6379"))
METADATA_SERVICE: str = os.environ.get(
    "METADATA_SERVICE", "http://metadata-service:5003"
)

redis_client: aioredis.Redis | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    redis_client = aioredis.from_url(
        f"redis://{REDIS_HOST}:{REDIS_PORT}", decode_responses=True
    )
    logger.info("Upload service started. S3 enabled: %s", USE_S3)
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    yield
    await redis_client.aclose()


app = FastAPI(
    title="Upload Service",
    description=(
        "Handles video uploads, stores to disk/S3,"
        " and queues transcoding jobs."
    ),
    version="1.0.0",
    root_path="/api/upload",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_extension(filename: str) -> str | None:
    parts = filename.rsplit(".", 1)
    return parts[1].lower() if len(parts) == 2 else None


async def get_redis() -> aioredis.Redis:
    if redis_client is None:
        raise HTTPException(status_code=503, detail="Redis not available")
    return redis_client


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health() -> HealthResponse:
    return HealthResponse(
        status="ok",
        service="upload-service",
        s3_enabled=USE_S3,
    )


@app.post(
    "/",
    response_model=UploadResponse,
    status_code=201,
    tags=["Upload"],
)
async def upload_video(
    file: Annotated[UploadFile, File(description="Video file to upload")],
    title: Annotated[str, Form(description="Video title")] = "Untitled",
    uploader_id: Annotated[
        str, Form(description="Uploader user ID")
    ] = "anonymous",
    r: aioredis.Redis = Depends(get_redis),
) -> UploadResponse:
    """Accept a video upload, persist it, and enqueue a transcoding job."""
    if not file.filename:
        raise HTTPException(status_code=400, detail="Filename is required")

    ext = get_extension(file.filename)
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=415,
            detail=(
                f"Unsupported type '.{ext}'."
                f" Allowed: {sorted(ALLOWED_EXTENSIONS)}"
            ),
        )

    video_id: str = str(uuid.uuid4())
    filename: str = f"{video_id}.{ext}"
    local_path: str = os.path.join(UPLOAD_FOLDER, filename)

    # Stream upload to disk to avoid loading the whole file into memory
    total_bytes: int = 0
    chunk_size: int = 64 * 1024  # 64 KiB
    with open(local_path, "wb") as out_f:
        while True:
            chunk = await file.read(chunk_size)
            if not chunk:
                break
            out_f.write(chunk)
            total_bytes += len(chunk)
    logger.info("Saved %s locally (%d bytes)", filename, total_bytes)

    s3_raw_key: str = f"raw/{filename}"
    storage_location: str = local_path
    if USE_S3:
        ok = upload_to_s3(
            local_path, s3_raw_key,
            S3_RAW_BUCKET,
            content_type="video/mp4",
        )
        if ok:
            storage_location = f"s3://{S3_RAW_BUCKET}/{s3_raw_key}"

    metadata_payload = VideoCreateRequest(
        video_id=video_id,
        title=title,
        uploader_id=uploader_id,
        filename=filename,
        status=VideoStatus.uploaded,
        storage_location=storage_location
    )

    async with httpx.AsyncClient() as client:
        try:
            await client.post(
                f"{METADATA_SERVICE}/",
                json=metadata_payload.model_dump(mode="json"),
                timeout=5.0
            )
        except httpx.RequestError as exc:
            logger.warning("Metadata service unreachable: %s", exc)

    job: dict[str, str | None] = {
        "video_id": video_id,
        "filename": filename,
        "title": title,
        "local_path": local_path,
        "s3_raw_key": s3_raw_key if USE_S3 else None
    }

    await r.lpush(  # type: ignore[misc]
        "transcoding_queue", json.dumps(job),
    )
    logger.info("Queued transcoding job for %s", video_id)

    return UploadResponse(
        video_id=video_id,
        status=VideoStatus.uploaded,
        s3_enabled=USE_S3,
        message="Video uploaded. Transcoding in progress.",
    )


@app.get("/status/{video_id}", tags=["Upload"])
async def get_upload_status(video_id: str) -> dict[str, object]:
    """Proxy to metadata service for video status."""
    async with httpx.AsyncClient() as client:
        try:
            res = await client.get(
                f"{METADATA_SERVICE}/{video_id}",
                timeout=5.0,
            )
            return res.json()
        except httpx.RequestError as exc:
            raise HTTPException(status_code=503, detail=str(exc))
