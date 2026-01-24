"""
Crawl Scheduler - Orchestrates crawl execution

Responsibilities:
- Find sources due for crawling
- Launch Scrapy spiders with correct config
- Track crawl progress
- Handle failures and retries

Can run as:
- CLI command (manual trigger)
- Cron job (periodic execution)
- Background service (continuous)
"""

import logging
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from apps.sources.models import Source, SourceType
from apps.sources.source_manager import SourceManager

log = logging.getLogger(__name__)


class CrawlScheduler:
    """
    Orchestrates crawl execution.

    Finds sources due for crawling and launches appropriate spiders.
    """

    # Map source types to spider names
    SPIDER_MAP = {
        SourceType.BLOG: 'generic',
        SourceType.DIRECTORY: 'generic',
        SourceType.SITEMAP: 'sitemap',
        SourceType.RSS_FEED: 'feed',
        # Instagram/Facebook would need dedicated spiders
    }

    def __init__(self,
                 database_url: Optional[str] = None,
                 max_concurrent_crawls: int = 1,
                 scrapy_project_path: str = "apps/crawler"):
        """
        Args:
            database_url: Database connection string
            max_concurrent_crawls: How many crawls to run simultaneously
            scrapy_project_path: Path to Scrapy project directory
        """
        self.manager = SourceManager(database_url)
        self.max_concurrent = max_concurrent_crawls
        self.scrapy_path = Path(scrapy_project_path)
        self.active_processes = []

    def run_once(self, limit: Optional[int] = None) -> int:
        """
        Run one cycle of scheduling.

        Args:
            limit: Max number of sources to crawl

        Returns:
            Number of crawls started
        """
        log.info("Starting scheduler cycle")

        # Get sources due for crawling
        sources = self.manager.get_sources_due_for_crawl(limit=limit)

        if not sources:
            log.info("No sources due for crawling")
            return 0

        crawls_started = 0

        for source in sources:
            # Check concurrency limit
            self._cleanup_finished_processes()
            if len(self.active_processes) >= self.max_concurrent:
                log.info(f"Concurrency limit reached ({self.max_concurrent}), waiting...")
                break

            # Start crawl
            if self._start_crawl(source):
                crawls_started += 1

        log.info(f"Scheduler cycle complete: {crawls_started} crawls started")
        return crawls_started

    def run_continuous(self, check_interval: int = 300):
        """
        Run scheduler continuously.

        Args:
            check_interval: Seconds between checks
        """
        log.info(f"Starting continuous scheduler (check every {check_interval}s)")

        try:
            while True:
                self.run_once()

                # Wait for interval
                log.debug(f"Sleeping for {check_interval}s...")
                time.sleep(check_interval)

        except KeyboardInterrupt:
            log.info("Scheduler interrupted by user")
            self._cleanup_finished_processes()

    def _start_crawl(self, source: Source) -> bool:
        """
        Start a crawl for a source.

        Args:
            source: Source to crawl

        Returns:
            True if crawl started successfully
        """
        # Determine spider to use
        spider_name = self.SPIDER_MAP.get(source.source_type, 'generic')

        # Create crawl run record
        crawl_run = self.manager.start_crawl_run(source.id, spider_name)

        # Build scrapy command
        cmd = [
            'scrapy', 'crawl', spider_name,
            '-a', f'start_urls={source.url}',
            '-a', f'source_id={source.id}',
            '-a', f'crawl_run_id={crawl_run.id}',
            '-a', 'discovery_type=scheduled',
        ]

        # Set environment variables
        env = os.environ.copy()
        env['CRAWL_RUN_ID'] = str(crawl_run.id)
        env['SOURCE_ID'] = str(source.id)

        log.info(f"Starting crawl: {source.domain} (run_id={crawl_run.id})")
        log.debug(f"Command: {' '.join(cmd)}")

        try:
            # Start subprocess
            process = subprocess.Popen(
                cmd,
                cwd=self.scrapy_path,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            # Track process
            self.active_processes.append({
                'process': process,
                'crawl_run_id': crawl_run.id,
                'source_id': source.id,
                'started_at': datetime.now(timezone.utc),
            })

            return True

        except Exception as e:
            log.error(f"Failed to start crawl for {source.domain}: {e}")
            self.manager.complete_crawl_run(crawl_run.id, success=False, error=str(e))
            return False

    def _cleanup_finished_processes(self):
        """Check for finished processes and clean them up."""
        still_running = []

        for proc_info in self.active_processes:
            process = proc_info['process']
            crawl_run_id = proc_info['crawl_run_id']

            # Check if process finished
            return_code = process.poll()

            if return_code is not None:
                # Process finished
                success = (return_code == 0)

                # Get stderr for errors
                error = None
                if not success:
                    _, stderr = process.communicate()
                    error = stderr[-500:] if stderr else "Unknown error"

                # Mark crawl complete
                self.manager.complete_crawl_run(crawl_run_id, success, error)

                log.info(f"Crawl run {crawl_run_id} finished: {'success' if success else 'failed'}")
            else:
                # Still running
                still_running.append(proc_info)

        self.active_processes = still_running

    def wait_for_all_crawls(self):
        """Wait for all active crawls to finish."""
        log.info(f"Waiting for {len(self.active_processes)} active crawls to finish...")

        while self.active_processes:
            self._cleanup_finished_processes()
            time.sleep(5)

        log.info("All crawls finished")

    def close(self):
        """Clean up resources."""
        self.wait_for_all_crawls()
        self.manager.close()


def seed_sources(database_url: Optional[str] = None):
    """
    Seed the database with initial sources.

    This is a one-time setup to add known good sources.
    """
    manager = SourceManager(database_url)

    # Initial seed sources
    seeds = [
        ("https://foodieinlagos.com", SourceType.BLOG, "Lagos", 0.9),
        ("https://www.eatdrinklagos.com", SourceType.BLOG, "Lagos", 0.8),
        ("https://blog.fusion.ng", SourceType.BLOG, "Lagos", 0.7),
        ("https://meiza.ng", SourceType.BLOG, "Lagos", 0.6),
        ("https://ofadaa.com/lagos/restaurants", SourceType.DIRECTORY, "Lagos", 0.7),
        ("https://ofadaa.com/ibadan/restaurants", SourceType.DIRECTORY, "Ibadan", 0.7),
    ]

    log.info("Seeding database with initial sources...")

    for url, stype, region, trust in seeds:
        try:
            source = manager.add_source(
                url=url,
                source_type=stype,
                region_focus=region,
                trust_level=trust,
            )
            log.info(f"  ✓ Added: {source.domain}")
        except Exception as e:
            log.error(f"  ✗ Failed to add {url}: {e}")

    manager.close()
    log.info("Seeding complete")


# CLI interface
def main():
    """CLI entry point for scheduler."""
    import argparse

    parser = argparse.ArgumentParser(description="Auto-Discovery Crawl Scheduler")
    parser.add_argument(
        'mode',
        choices=['once', 'continuous', 'seed'],
        help='Scheduler mode'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Max sources to crawl (once mode only)'
    )
    parser.add_argument(
        '--interval',
        type=int,
        default=300,
        help='Check interval in seconds (continuous mode)'
    )
    parser.add_argument(
        '--database-url',
        help='Database connection URL'
    )
    parser.add_argument(
        '--max-concurrent',
        type=int,
        default=1,
        help='Max concurrent crawls'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )

    args = parser.parse_args()

    # Setup logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Handle modes
    if args.mode == 'seed':
        seed_sources(args.database_url)
        return

    # Create scheduler
    scheduler = CrawlScheduler(
        database_url=args.database_url,
        max_concurrent_crawls=args.max_concurrent,
    )

    try:
        if args.mode == 'once':
            count = scheduler.run_once(limit=args.limit)
            log.info(f"Started {count} crawls")
            scheduler.wait_for_all_crawls()

        elif args.mode == 'continuous':
            scheduler.run_continuous(check_interval=args.interval)

    finally:
        scheduler.close()


if __name__ == "__main__":
    main()
