from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class VideoStatus(str, Enum):
    uploaded = "uploaded"
    transcoding = "transcoding"
    ready = "ready"
    failed = "failed"


class Resolution(str, Enum):
    p360 = "360p"
    p720 = "720p"
    p1080 = "1080p"


class VideoCreateRequest(BaseModel):
    video_id: str = Field(..., description="UUID of the video")
    title: str = Field(..., description="Display title")
    uploader_id: str = Field(..., description="ID of the uploader")
    filename: str = Field(..., description="Stored filename on disk/S3")
    status: VideoStatus = VideoStatus.uploaded
    storage_location: Optional[str] = Field(None, description="Local path or S3 URI")


class VideoUpdateRequest(BaseModel):
    status: Optional[VideoStatus] = None
    manifest_url: Optional[str] = None
    resolutions: Optional[list[Resolution]] = None
    view_count: Optional[int] = None
    cdn_enabled: Optional[bool] = None


class VideoResponse(BaseModel):
    video_id: str
    title: str
    uploader_id: str
    filename: str
    status: VideoStatus
    manifest_url: Optional[str] = None
    resolutions: Optional[list[str]] = None
    view_count: int = 0
    cdn_enabled: bool = False
    storage_location: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True


class UploadResponse(BaseModel):
    video_id: str
    status: VideoStatus
    s3_enabled: bool
    message: str


class ViewCountResponse(BaseModel):
    video_id: str
    views: int


class PaginatedVideoResponse(BaseModel):
    items: list[VideoResponse]
    total: int
    page: int
    per_page: int
    pages: int


class HealthResponse(BaseModel):
    status: str
    service: str
    s3_enabled: Optional[bool] = None
