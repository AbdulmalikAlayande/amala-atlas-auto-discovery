"""
Source Management Models - Track crawl sources and state

Uses SQLAlchemy for database abstraction.
Supports PostgreSQL (production) and SQLite (development).

Key Entities:
- Source: A seed URL or domain to crawl
- CrawlRun: A single execution of the crawler
- CrawlResult: Individual pages crawled in a run
"""

from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime,
    Boolean, Text, Float, ForeignKey, JSON, Enum as SQLEnum, ColumnElement
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
import enum

Base = declarative_base()


class SourceType(str, enum.Enum):
    """Types of crawl sources."""
    BLOG = "blog"
    INSTAGRAM = "instagram"
    FACEBOOK = "facebook"
    YOUTUBE = "youtube"
    DIRECTORY = "directory"
    SITEMAP = "sitemap"
    RSS_FEED = "rss_feed"
    MANUAL = "manual"


class DiscoveryMethod(str, enum.Enum):
    """How the source was discovered."""
    SEED = "seed"  # Manually added seed
    LINK_EXPANSION = "link"  # Found via link following
    SEARCH = "search"  # Found via search
    USER_SUBMIT = "user"  # User-submitted


class CrawlStatus(str, enum.Enum):
    """Status of a crawl run."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class Source(Base):
    """
    A crawl source - represents a seed URL or domain.

    Examples:
    - https://foodieinlagos.com (blog)
    - https://instagram.com/foodieinlagos (social)
    - https://foodieinlagos.com/sitemap.xml (sitemap)
    """
    __tablename__ = 'sources'

    id = Column(Integer, primary_key=True)

    # Core fields
    source_type = Column(SQLEnum(SourceType), nullable=False)
    url = Column(String(2048), nullable=False, unique=True)
    domain = Column(String(255), nullable=False, index=True)

    # Discovery metadata
    discovery_method = Column(SQLEnum(DiscoveryMethod), default=DiscoveryMethod.SEED)
    discovered_from_id = Column(Integer, ForeignKey('sources.id'), nullable=True)

    # Geography
    region_focus = Column(String(100), nullable=True)  # Lagos, Ibadan, etc.

    # Trust & quality
    trust_level = Column(Float, default=0.5)  # 0.0 to 1.0
    is_active = Column(Boolean, default=True)

    # Crawl scheduling
    crawl_frequency_days = Column(Integer, default=7)  # How often to crawl
    last_crawled_at = Column(DateTime(timezone=True), nullable=True)
    next_crawl_at = Column(DateTime(timezone=True), nullable=True)

    # Statistics
    total_pages_crawled = Column(Integer, default=0)
    total_candidates_found = Column(Integer, default=0)
    avg_candidate_score = Column(Float, nullable=True)

    # Metadata
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

    # Relationships
    crawl_runs = relationship("CrawlRun", back_populates="source")
    discovered_sources = relationship("Source", remote_side=[id])

    def __repr__(self):
        return f"<Source(id={self.id}, type={self.source_type}, domain={self.domain})>"

    def should_crawl_now(self) -> bool | ColumnElement[bool]:
        """Check if this source is due for crawling."""
        if not self.is_active:
            return False

        if not self.next_crawl_at:
            return True

        return datetime.now(timezone.utc) >= self.next_crawl_at


class CrawlRun(Base):
    """
    A single execution of the crawler.

    Tracks when crawls start, finish, and their results.
    """
    __tablename__ = 'crawl_runs'

    id = Column(Integer, primary_key=True)

    # Source being crawled
    source_id = Column(Integer, ForeignKey('sources.id'), nullable=False)

    # Run metadata
    status = Column(SQLEnum(CrawlStatus), default=CrawlStatus.PENDING)
    spider_name = Column(String(100), nullable=False)  # 'generic', 'sitemap', etc.

    # Timing
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    duration_seconds = Column(Integer, nullable=True)

    # Results
    pages_crawled = Column(Integer, default=0)
    pages_passed_gate = Column(Integer, default=0)
    candidates_found = Column(Integer, default=0)
    candidates_published = Column(Integer, default=0)

    # Error tracking
    error_message = Column(Text, nullable=True)

    # Configuration used
    config = Column(JSON, nullable=True)  # Crawl parameters

    # Metadata
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    # Relationships
    source = relationship("Source", back_populates="crawl_runs")
    results = relationship("CrawlResult", back_populates="crawl_run")

    def __repr__(self):
        return f"<CrawlRun(id={self.id}, source_id={self.source_id}, status={self.status})>"

    def mark_completed(self, success: bool = True, error: Optional[str] = None):
        """Mark the crawl run as completed."""
        self.completed_at = datetime.now(timezone.utc)

        if self.started_at:
            duration = (self.completed_at - self.started_at).total_seconds()
            self.duration_seconds = int(duration)

        if success:
            self.status = CrawlStatus.COMPLETED
        else:
            self.status = CrawlStatus.FAILED
            self.error_message = error


class CrawlResult(Base):
    """
    Individual page result from a crawl.

    Tracks what happened to each URL crawled.
    """
    __tablename__ = 'crawl_results'

    id = Column(Integer, primary_key=True)

    # Which crawl run
    crawl_run_id = Column(Integer, ForeignKey('crawl_runs.id'), nullable=False)

    # URL crawled
    url = Column(String(2048), nullable=False)
    final_url = Column(String(2048), nullable=True)  # After redirects

    # HTTP status
    status_code = Column(Integer, nullable=True)

    # Processing results
    passed_nlp_gate = Column(Boolean, default=False)
    candidate_score = Column(Float, nullable=True)
    was_published = Column(Boolean, default=False)
    was_duplicate = Column(Boolean, default=False)

    # Drop reason if not published
    drop_reason = Column(String(255), nullable=True)

    # Signals for debugging
    signals = Column(JSON, nullable=True)

    # Timing
    fetched_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    # Relationships
    crawl_run = relationship("CrawlRun", back_populates="results")

    def __repr__(self):
        return f"<CrawlResult(id={self.id}, url={self.url[:50]}, score={self.candidate_score})>"


# Database connection management
class DatabaseManager:
    """
    Manages database connections and sessions.

    Supports both PostgreSQL and SQLite.
    """

    def __init__(self, database_url: str):
        """
        Args:
            database_url: SQLAlchemy database URL
                - PostgreSQL: postgresql://user:pass@host:port/dbname
                - SQLite: sqlite:///path/to/db.sqlite
        """
        self.engine = create_engine(database_url, echo=False)
        self.SessionLocal = sessionmaker(bind=self.engine)

    def create_tables(self):
        """Create all tables if they don't exist."""
        Base.metadata.create_all(self.engine)

    def get_session(self):
        """Get a new database session."""
        return self.SessionLocal()

    def drop_tables(self):
        """Drop all tables (use with caution!)."""
        Base.metadata.drop_all(self.engine)


# Convenience functions
def get_db_manager(database_url: Optional[str] = None) -> DatabaseManager:
    """
    Get database manager with connection URL from env or parameter.

    Defaults to SQLite for development.
    """
    import os

    if not database_url:
        database_url = os.getenv(
            'DATABASE_URL',
            'sqlite:///./auto_discovery.db'
        )

    return DatabaseManager(database_url)


def init_database(database_url: Optional[str] = None):
    """Initialize the database with all tables."""
    db = get_db_manager(database_url)
    db.create_tables()
    return db


# Example usage
if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)

    # Initialize database (SQLite for testing)
    db_ = init_database('sqlite:///./test_discovery.db')

    # Create a session
    session = db_.get_session()

    # Add a test source
    source = Source(
        source_type=SourceType.BLOG,
        url="https://foodieinlagos.com",
        domain="foodieinlagos.com",
        discovery_method=DiscoveryMethod.SEED,
        region_focus="Lagos",
        trust_level=0.8,
        crawl_frequency_days=7,
    )

    session.add(source)
    session.commit()

    print(f"Created source: {source}")

    # Create a crawl run
    crawl_run = CrawlRun(
        source_id=source.id,
        spider_name="generic",
        status=CrawlStatus.RUNNING,
        started_at=datetime.now(timezone.utc),
    )

    session.add(crawl_run)
    session.commit()

    print(f"Created crawl run: {crawl_run}")

    # Add a result
    result = CrawlResult(
        crawl_run_id=crawl_run.id,
        url="https://foodieinlagos.com/review-amala-spot/",
        status_code=200,
        passed_nlp_gate=True,
        candidate_score=0.75,
        was_published=True,
    )

    session.add(result)
    session.commit()

    print(f"Created result: {result}")

    # Query example
    print("\n=== All Active Sources ===")
    sources = session.query(Source).filter_by(is_active=True).all()
    for s in sources:
        print(f"  {s.domain} ({s.source_type}) - Last crawled: {s.last_crawled_at}")

    session.close()
