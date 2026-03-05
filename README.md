# 🎬 Video Streaming Platform — Docker Simulation

A simplified video streaming platform built with Docker to simulate real-world architecture components like YouTube or Netflix.

## Architecture

```
                    ┌─────────────────────┐
                    │   nginx (port 8080)  │  ← API Gateway
                    └─────────┬───────────┘
                              │
          ┌───────────────────┼───────────────────┐
          │                   │                   │
   ┌──────▼──────┐   ┌────────▼───────┐  ┌───────▼──────┐
   │   upload-   │   │   streaming-   │  │  metadata-   │
   │   service   │   │   service      │  │  service     │
   └──────┬──────┘   └────────┬───────┘  └───────┬──────┘
          │                   │                   │
          │ queue job         │ serve HLS         │ CRUD
          ▼                   ▼                   ▼
       Redis              processed/           PostgreSQL
    (job queue +          (S3 sim)
     view counts)
          │
   ┌──────▼──────────┐
   │ transcoding-    │  ← Worker (polls Redis queue)
   │ service         │
   └─────────────────┘
```

## Services

| Service | Port | Responsibility |
|---|---|---|
| nginx | 8080 | API Gateway / reverse proxy |
| upload-service | 5001 | Handle video uploads, queue transcoding jobs |
| transcoding-service | — | Worker: converts video → HLS segments |
| streaming-service | 5002 | Serve HLS manifest & video segments |
| metadata-service | 5003 | Store/query video metadata in PostgreSQL |
| redis | 6379 | Job queue + view count buffer |
| postgres | 5432 | Video metadata database |

## Quick Start

```bash
# Clone and enter the repo
git clone https://github.com/mattoranking/sysdesign-sk
cd sysdesign-sk

# Start everything
docker compose up --build

# In another terminal, upload a test video
curl -X POST http://localhost:8080/upload \
  -F "file=@/path/to/your/video.mp4" \
  -F "title=My First Video" \
  -F "uploader_id=user123"

# Response includes a video_id:
# { "video_id": "abc-123", "status": "uploaded" }

# Check transcoding status
curl http://localhost:8080/videos/abc-123

# Once status is "ready", stream it
curl http://localhost:8080/stream/abc-123/manifest.m3u8

# Check view count
curl http://localhost:8080/views/abc-123

# Search videos
curl http://localhost:8080/videos/search?q=first

# List all videos
curl http://localhost:8080/videos
```

## How the Pipeline Works

1. **Upload**: You POST a video file → `upload-service` saves it to shared volume and pushes a job to Redis queue
2. **Transcode**: `transcoding-service` worker picks up the job → runs FFmpeg (or simulates if not available) → produces HLS segments at 360p / 720p / 1080p
3. **Ready**: Metadata is updated to `status: ready` with the manifest URL
4. **Stream**: Client fetches `/stream/{id}/manifest.m3u8` → gets HLS playlist → streams segments adaptively
5. **Views**: Each manifest request increments a Redis counter

## Simulated vs Real Components

| Real World | This Simulation |
|---|---|
| AWS S3 | Docker named volumes (`raw_storage`, `processed_storage`) |
| Kafka | Redis List (LPUSH/BRPOP) |
| CDN (CloudFront) | nginx reverse proxy |
| Multiple transcoding workers | Single transcoding container |
| Cloud PostgreSQL | Local postgres container |

## Health Checks

```bash
curl http://localhost:8080/health/upload
curl http://localhost:8080/health/streaming
curl http://localhost:8080/health/metadata
```
