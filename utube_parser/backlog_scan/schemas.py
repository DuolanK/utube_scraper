from pydantic import BaseModel


class ChannelDetails(BaseModel):
    id: str
    title: str
    video_title: str
    custom_url: str
    tags: str
    scan_date: str
    subs: int
    published_at: str
    avg_views: float
    er: float
    contacts: str


class DiscoveredDomains(BaseModel):
    valid_links: str
    is_url: bool
