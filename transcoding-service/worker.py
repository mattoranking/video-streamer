import os
import json
import time
import shutil
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import redis
import httpx

from shared.s3_utils import (
    upload_to_s3,
    S3_PROCESSED_BUCKET,
    USE_S3,
    get_cdn_url,
)
from shared.models import VideoStatus, Resolution

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("transcoding-worker")

REDIS_HOST: str = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT: int = int(os.environ.get("REDIS_PORT", "6379"))
RAW_STORAGE: str = "/storage/raw"
PROCESSED_STORAGE: str = "/storage/processed"
METADATA_SERVICE: str = os.environ.get(
    "METADATA_SERVICE", "http://metadata-service:5003"
)

RESOLUTIONS: list[Resolution] = [
    Resolution.p360, Resolution.p720, Resolution.p1080,
]
RES_SCALE_MAP: dict[Resolution, str] = {
    Resolution.p360: "640x360",
    Resolution.p720: "1280x720",
    Resolution.p1080: "1920x1080",
}
BANDWIDTH_MAP: dict[Resolution, int] = {
    Resolution.p360: 800_000,
    Resolution.p720: 2_800_000,
    Resolution.p1080: 5_000_000,
}


@dataclass
class TranscodeJob:
    video_id: str
    filename: str
    title: str
    local_path: str | None = None
    s3_raw_key: str | None = None

    @classmethod
    def from_json(cls, raw: str) -> "TranscodeJob":
        data: dict[str, Any] = json.loads(raw)
        return cls(
            video_id=data["video_id"],
            filename=data["filename"],
            title=data.get("title", "Untitled"),
            local_path=data.get("local_path"),
            s3_raw_key=data.get("s3_raw_key"),
        )


def update_metadata(
    video_id: str, update: dict[str, Any],
) -> None:
    try:
        httpx.patch(
            f"{METADATA_SERVICE}/{video_id}",
            json=update,
            timeout=5.0,
        )
    except httpx.RequestError as exc:
        logger.warning("Metadata update failed for %s: %s", video_id, exc)


def write_resolution_playlist(res_dir: Path, resolution: Resolution) -> None:
    """Write a minimal HLS playlist referencing 3 fake segments."""
    content = "\n".join([
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        "#EXT-X-TARGETDURATION:6",
        "#EXT-X-MEDIA-SEQUENCE:0",
        "",
        "#EXTINF:6.0,", "segment_000.ts",
        "#EXTINF:6.0,", "segment_001.ts",
        "#EXTINF:6.0,", "segment_002.ts",
        "#EXT-X-ENDLIST",
    ])
    playlist_path = res_dir / "playlist.m3u8"
    playlist_path.write_text(content)

    if USE_S3:
        upload_to_s3(
            str(playlist_path),
            f"{res_dir.parent.name}/{resolution.value}/playlist.m3u8",
            S3_PROCESSED_BUCKET,
            content_type="application/vnd.apple.mpegurl",
        )


def write_master_manifest(
    video_id: str,
    video_out_dir: Path,
    use_cdn: bool,
) -> str:
    """Write (and optionally upload) the HLS master manifest.
    Returns the public URL."""
    lines: list[str] = ["#EXTM3U", "#EXT-X-VERSION:3", ""]
    for res in RESOLUTIONS:
        playlist_url = (
            get_cdn_url(f"{video_id}/{res.value}/playlist.m3u8")
            if use_cdn
            else f"{res.value}/playlist.m3u8"
        )
        lines += [
            f"#EXT-X-STREAM-INF:BANDWIDTH="
            f"{BANDWIDTH_MAP[res]},"
            f" RESOLUTION={res.value}",
            playlist_url,
            "",
        ]

    manifest_path = video_out_dir / "manifest.m3u8"
    manifest_path.write_text("\n".join(lines))

    if USE_S3:
        upload_to_s3(
            str(manifest_path),
            f"{video_id}/manifest.m3u8",
            S3_PROCESSED_BUCKET,
            content_type="application/vnd.apple.mpegurl",
        )

        return get_cdn_url(f"{video_id}/manifest.m3u8")

    return f"/stream/{video_id}/manifest.m3u8"


def simulate_resolution(
    video_id: str,
    res_dir: Path,
    resolution: Resolution,
) -> None:
    """Create fake .ts segments and a playlist (no FFmpeg required)."""
    logger.info("Simulating %s for %s", resolution.value, video_id)
    for i in range(3):
        seg = res_dir / f"segment_{i:03d}.ts"
        seg.write_bytes(b"\x00" * 1024)
        if USE_S3:
            upload_to_s3(
                str(seg),
                f"{video_id}/{resolution.value}/segment_{i:03d}.ts",
                S3_PROCESSED_BUCKET,
                content_type="video/MP2T",
            )

    write_resolution_playlist(res_dir, resolution)


def ffmpeg_transcode(
    video_id: str,
    raw_path: str,
    res_dir: Path,
    resolution: Resolution,
) -> bool:
    """Run FFmpeg HLS segmentation. Returns True on success."""
    seg_pattern = str(res_dir / "segment_%03d.ts")
    playlist = str(res_dir / "playlist.m3u8")
    scale = RES_SCALE_MAP[resolution]

    ret = os.system(
        f'ffmpeg -i "{raw_path}" -vf scale={scale} '
        f'-c:v libx264 -hls_time 6 -hls_playlist_type vod '
        f'-hls_segment_filename "{seg_pattern}" '
        f'"{playlist}" -y -loglevel error'
    )

    if ret != 0:
        return False

    if USE_S3:
        for seg_file in res_dir.iterdir():
            ct = (
                "video/MP2T"
                if seg_file.suffix == ".ts"
                else "application/vnd.apple.mpegurl"
            )
            upload_to_s3(
                str(seg_file),
                f"{video_id}/{resolution.value}/{seg_file.name}",
                S3_PROCESSED_BUCKET,
                content_type=ct,
            )

    return True


def transcode(job: TranscodeJob) -> None:
    raw_path: str = (
        job.local_path
        or os.path.join(RAW_STORAGE, job.filename)
    )
    video_out_dir = Path(PROCESSED_STORAGE) / job.video_id
    video_out_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Starting transcode for %s", job.video_id)
    update_metadata(job.video_id, {"status": VideoStatus.transcoding.value})

    ffmpeg_available = shutil.which("ffmpeg") is not None

    for resolution in RESOLUTIONS:
        res_dir = video_out_dir / resolution.value
        res_dir.mkdir(parents=True, exist_ok=True)
        time.sleep(0.2)     # small delay for realism in simulation mode

        if ffmpeg_available and Path(raw_path).exists():
            ok = ffmpeg_transcode(job.video_id, raw_path, res_dir, resolution)

            if not ok:
                logger.warning(
                    "FFmpeg failed for %s %s,"
                    " falling back to simulation",
                    job.video_id, resolution.value,
                )
                simulate_resolution(job.video_id, res_dir, resolution)
        else:
            simulate_resolution(job.video_id, res_dir, resolution)

        logger.info(
            "Done %s for %s",
            resolution.value, job.video_id,
        )

    manifest_url: str = write_master_manifest(
        job.video_id, video_out_dir, use_cdn=USE_S3,
    )

    update_metadata(job.video_id, {
        "status": VideoStatus.ready.value,
        "manifest_url": manifest_url,
        "resolutions": [r.value for r in RESOLUTIONS],
        "cdn_enabled": USE_S3,
    })
    logger.info(
        "Video %s ready. Manifest: %s",
        job.video_id, manifest_url,
    )


def run_worker() -> None:
    r = redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT,
        decode_responses=True,
    )
    logger.info(
        "Transcoding worker ready - polling Redis queue...",
    )

    while True:
        result = r.brpop(  # type: ignore[arg-type]
            ["transcoding_queue"], timeout=5,
        )
        if result is None:  # type: ignore[reportUnnecessaryComparison]
            continue
        _, raw = result  # type: ignore[misc]
        raw_str: str = str(raw)  # type: ignore[reportUnknownArgumentType]
        try:
            job = TranscodeJob.from_json(raw_str)
            logger.info("Picked up job: %s", job.video_id)
            transcode(job)
        except Exception as ex:
            logger.error(
                "Job failed: %s", ex, exc_info=True,
            )
            try:
                data = json.loads(raw_str)
                update_metadata(
                    data.get("video_id", ""),
                    {"status": VideoStatus.failed.value},
                )
            except Exception:
                pass


if __name__ == "__main__":
    time.sleep(3)      # let Redis settle on startup
    run_worker()
