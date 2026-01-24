"""
Source Manager - High-level operations for source management

Provides clean API for:
- Adding sources
- Listing sources due for crawling
- Updating crawl statistics
- Managing source trust levels
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from urllib.parse import urlparse

from apps.sources.models import (
    Source, SourceType, DiscoveryMethod, CrawlRun,
    CrawlStatus, CrawlResult, get_db_manager
)

log = logging.getLogger(__name__)


class SourceManager:
    """High-level source management operations."""

    def __init__(self, database_url: Optional[str] = None):
        self.db = get_db_manager(database_url)
        self.session = self.db.get_session()

    def add_source(self,
                   url: str,
                   source_type: SourceType,
                   discovery_method: DiscoveryMethod = DiscoveryMethod.SEED,
                   region_focus: Optional[str] = None,
                   trust_level: float = 0.5,
                   crawl_frequency_days: int = 7) -> Source:
        """
        Add a new source to track.

        Args:
            url: Source URL
            source_type: Type of source (blog, instagram, etc.)
            discovery_method: How it was discovered
            region_focus: Geographic focus (Lagos, Ibadan, etc.)
            trust_level: Initial trust (0.0 to 1.0)
            crawl_frequency_days: Days between crawls

        Returns:
            Created Source object
        """
        # Extract domain
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path.split('/')[0]

        # Check if already exists
        existing = self.session.query(Source).filter_by(url=url).first()
        if existing:
            log.info(f"Source already exists: {url}")
            return existing

        # Create new source
        source = Source(
            url=url,
            domain=domain,
            source_type=source_type,
            discovery_method=discovery_method,
            region_focus=region_focus,
            trust_level=trust_level,
            crawl_frequency_days=crawl_frequency_days,
            next_crawl_at=datetime.now(timezone.utc),  # Crawl immediately
        )

        self.session.add(source)
        self.session.commit()

        log.info(f"Added new source: {domain} ({source_type})")
        return source

    def get_sources_due_for_crawl(self, limit: Optional[int] = None) -> List[Source]:
        """
        Get sources that are due for crawling.

        Args:
            limit: Maximum number of sources to return

        Returns:
            List of Source objects ready to crawl
        """
        query = self.session.query(Source).filter(
            Source.is_active == True,
            Source.next_crawl_at <= datetime.now(timezone.utc)
        ).order_by(Source.next_crawl_at)

        if limit:
            query = query.limit(limit)

        sources = query.all()
        log.info(f"Found {len(sources)} sources due for crawling")

        return sources

    def start_crawl_run(self, source_id: int, spider_name: str) -> CrawlRun:
        """
        Start a new crawl run for a source.

        Args:
            source_id: Source to crawl
            spider_name: Name of spider to use

        Returns:
            Created CrawlRun object
        """
        crawl_run = CrawlRun(
            source_id=source_id,
            spider_name=spider_name,
            status=CrawlStatus.RUNNING,
            started_at=datetime.now(timezone.utc),
        )

        self.session.add(crawl_run)
        self.session.commit()

        log.info(f"Started crawl run {crawl_run.id} for source {source_id}")
        return crawl_run

    def complete_crawl_run(self,
                           crawl_run_id: int,
                           success: bool = True,
                           error: Optional[str] = None):
        """
        Mark a crawl run as completed and update source statistics.

        Args:
            crawl_run_id: ID of crawl run
            success: Whether crawl succeeded
            error: Error message if failed
        """
        crawl_run = self.session.query(CrawlRun).get(crawl_run_id)
        if not crawl_run:
            log.error(f"Crawl run {crawl_run_id} not found")
            return

        # Mark crawl run complete
        crawl_run.mark_completed(success, error)

        # Update source statistics
        source = crawl_run.source_
        source.last_crawled_at = datetime.now(timezone.utc)
        source.total_pages_crawled += crawl_run.pages_crawled
        source.total_candidates_found += crawl_run.candidates_found

        # Calculate next crawl time
        source.next_crawl_at = (
                datetime.now(timezone.utc) +
                timedelta(days=source.crawl_frequency_days)
        )

        # Update average score
        if crawl_run.candidates_found > 0:
            # Get all results with scores
            results = self.session.query(CrawlResult).filter(
                CrawlResult.crawl_run_id == crawl_run_id,
                CrawlResult.candidate_score.isnot(None)
            ).all()

            if results:
                avg_score = sum(r.candidate_score for r in results) / len(results)

                # Exponential moving average
                if source.avg_candidate_score:
                    alpha = 0.3  # Weight for new data
                    source.avg_candidate_score = (
                            alpha * avg_score +
                            (1 - alpha) * source.avg_candidate_score
                    )
                else:
                    source.avg_candidate_score = avg_score

        self.session.commit()

        log.info(f"Completed crawl run {crawl_run_id}: {crawl_run.candidates_published} published")

    def record_page_result(self,
                           crawl_run_id: int,
                           url: str,
                           status_code: int,
                           passed_gate: bool,
                           score: Optional[float] = None,
                           published: bool = False,
                           duplicate: bool = False,
                           drop_reason: Optional[str] = None,
                           signals: Optional[dict] = None):
        """
        Record the result of processing a single page.

        Args:
            crawl_run_id: ID of crawl run
            url: URL processed
            status_code: HTTP status
            passed_gate: Whether passed NLP gate
            score: Candidate score if applicable
            published: Whether published to backend
            duplicate: Whether detected as duplicate
            drop_reason: Why it was dropped (if applicable)
            signals: Extraction signals
        """
        result = CrawlResult(
            crawl_run_id=crawl_run_id,
            url=url,
            status_code=status_code,
            passed_nlp_gate=passed_gate,
            candidate_score=score,
            was_published=published,
            was_duplicate=duplicate,
            drop_reason=drop_reason,
            signals=signals,
        )

        self.session.add(result)

        # Update crawl run stats
        crawl_run = self.session.query(CrawlRun).get(crawl_run_id)
        if crawl_run:
            crawl_run.pages_crawled += 1
            if passed_gate:
                crawl_run.pages_passed_gate += 1
            if score is not None:
                crawl_run.candidates_found += 1
            if published:
                crawl_run.candidates_published += 1

        self.session.commit()

    def adjust_trust_level(self, source_id: int, delta: float):
        """
        Adjust source trust level based on quality.

        Args:
            source_id: Source to adjust
            delta: Change in trust (-1.0 to 1.0)
        """
        source = self.session.query(Source).get(source_id)
        if not source:
            return

        # Adjust trust
        new_trust = max(0.0, min(1.0, source.trust_level + delta))
        source.trust_level = new_trust

        # Adjust crawl frequency based on trust
        # High trust = crawl more often
        if new_trust > 0.8:
            source.crawl_frequency_days = 3
        elif new_trust > 0.6:
            source.crawl_frequency_days = 7
        elif new_trust > 0.4:
            source.crawl_frequency_days = 14
        else:
            source.crawl_frequency_days = 30

        self.session.commit()

        log.info(f"Adjusted trust for {source.domain}: {new_trust:.2f}, crawl every {source.crawl_frequency_days}d")

    def deactivate_source(self, source_id: int, reason: Optional[str] = None):
        """
        Deactivate a source (stop crawling it).

        Args:
            source_id: Source to deactivate
            reason: Why it's being deactivated
        """
        source = self.session.query(Source).get(source_id)
        if not source:
            return

        source.is_active = False
        if reason:
            source.notes = f"{source.notes or ''}\nDeactivated: {reason}"

        self.session.commit()

        log.info(f"Deactivated source {source.domain}: {reason}")

    def get_source_stats(self, source_id: int) -> dict:
        """
        Get statistics for a source.

        Returns:
            dict with crawl stats, quality metrics, etc.
        """
        source = self.session.query(Source).get(source_id)
        if not source:
            return {}

        # Get crawl run stats
        crawl_runs = self.session.query(CrawlRun).filter_by(
            source_id=source_id
        ).all()

        total_runs = len(crawl_runs)
        successful_runs = sum(1 for r in crawl_runs if r.status == CrawlStatus.COMPLETED)

        return {
            'source_id': source.id,
            'domain': source.domain,
            'is_active': source.is_active,
            'trust_level': source.trust_level,
            'total_pages_crawled': source.total_pages_crawled,
            'total_candidates_found': source.total_candidates_found,
            'avg_candidate_score': source.avg_candidate_score,
            'total_crawl_runs': total_runs,
            'successful_runs': successful_runs,
            'last_crawled_at': source.last_crawled_at,
            'next_crawl_at': source.next_crawl_at,
        }

    def close(self):
        """Close database session."""
        self.session.close()


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Initialize manager
    manager_ = SourceManager('sqlite:///./test_discovery.db')

    # Add some test sources
    print("\n=== Adding Sources ===")

    sources_to_add = [
        ("https://foodieinlagos.com", SourceType.BLOG, "Lagos"),
        ("https://www.eatdrinklagos.com", SourceType.BLOG, "Lagos"),
        ("https://blog.fusion.ng", SourceType.BLOG, "Lagos"),
    ]

    for url_, stype, region in sources_to_add:
        source_ = manager_.add_source(url_, stype, region_focus=region)
        print(f"  Added: {source_.domain}")

    # Get sources due for crawling
    print("\n=== Sources Due for Crawl ===")
    due_sources_ = manager_.get_sources_due_for_crawl()

    for source_ in due_sources_:
        print(f"  {source_.domain} - Last crawled: {source_.last_crawled_at}")

    # Simulate a crawl run
    if due_sources_:
        print("\n=== Simulating Crawl Run ===")
        source_ = due_sources_[0]

        # Start run
        run = manager_.start_crawl_run(source_.id, "generic")
        print(f"  Started run {run.id}")

        # Record some results
        manager_.record_page_result(
            run.id,
            f"{source_.url}/page1",
            200,
            passed_gate=True,
            score=0.75,
            published=True
        )

        manager_.record_page_result(
            run.id,
            f"{source_.url}/page2",
            200,
            passed_gate=False,
            drop_reason="recipe_content"
        )

        # Complete run
        manager_.complete_crawl_run(run.id, success=True)
        print(f"  Completed run {run.id}")

        # Get stats
        stats = manager_.get_source_stats(source_.id)
        print(f"\n=== Source Stats ===")
        for key, value in stats.items():
            print(f"  {key}: {value}")

    manager_.close()
