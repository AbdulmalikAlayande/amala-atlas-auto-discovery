#!/usr/bin/env python3
"""
Auto-Discovery CLI - Command line tool for managing sources and crawls

Usage:
    python cli.py sources list
    python cli.py sources add https://example.com blog
    python cli.py sources stats 1
    python cli.py crawl run --source-id=1
    python cli.py crawl schedule
"""

import logging
import sys

import click

from apps.sources.models import SourceType, init_database, Source
from apps.sources.scheduler import CrawlScheduler, seed_sources
from apps.sources.source_manager import SourceManager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)


@click.group()
@click.option('--database-url', envvar='DATABASE_URL',
              help='Database connection URL')
@click.pass_context
def cli(ctx, database_url):
    """Auto-Discovery management CLI."""
    ctx.ensure_object(dict)
    ctx.obj['database_url'] = database_url or 'sqlite:///./auto_discovery.db'


@cli.group()
def sources():
    """Manage crawl sources."""
    pass


@sources.command('list')
@click.option('--active-only/--all', default=True, help='Show only active sources')
@click.pass_context
def list_sources(ctx, active_only):
    """List all sources."""
    manager = SourceManager(ctx.obj['database_url'])

    query = manager.session.query(manager.session.query(Source).all().__class__)
    if active_only:
        sources = [s for s in manager.session.query(Source).all() if s.is_active]
    else:
        sources = manager.session.query(Source).all()

    if not sources:
        click.echo("No sources found")
        manager.close()
        return

    click.echo(f"\n{'ID':<5} {'Domain':<30} {'Type':<12} {'Trust':<7} {'Last Crawl':<20}")
    click.echo("-" * 80)

    for source in sources:
        last_crawl = source.last_crawled_at.strftime('%Y-%m-%d %H:%M') if source.last_crawled_at else 'Never'
        click.echo(
            f"{source.id:<5} {source.domain:<30} {source.source_type.value:<12} "
            f"{source.trust_level:<7.2f} {last_crawl:<20}"
        )

    click.echo(f"\nTotal: {len(sources)} sources\n")
    manager.close()


@sources.command('add')
@click.argument('url')
@click.argument('source_type', type=click.Choice([t.value for t in SourceType]))
@click.option('--region', help='Region focus (e.g., Lagos, Ibadan)')
@click.option('--trust', type=float, default=0.5, help='Trust level (0.0-1.0)')
@click.option('--frequency', type=int, default=7, help='Crawl frequency in days')
@click.pass_context
def add_source(ctx, url, source_type, region, trust, frequency):
    """Add a new source."""
    manager = SourceManager(ctx.obj['database_url'])

    try:
        source = manager.add_source(
            url=url,
            source_type=SourceType(source_type),
            region_focus=region,
            trust_level=trust,
            crawl_frequency_days=frequency,
        )
        click.echo(f"✓ Added source: {source.domain} (ID: {source.id})")
    except Exception as e:
        click.echo(f"✗ Failed to add source: {e}", err=True)
        sys.exit(1)
    finally:
        manager.close()


@sources.command('stats')
@click.argument('source_id', type=int)
@click.pass_context
def source_stats(ctx, source_id):
    """Show detailed statistics for a source."""
    manager = SourceManager(ctx.obj['database_url'])

    stats = manager.get_source_stats(source_id)

    if not stats:
        click.echo(f"Source {source_id} not found", err=True)
        manager.close()
        sys.exit(1)

    click.echo(f"\n=== Source Stats: {stats['domain']} ===\n")
    click.echo(f"Source ID:          {stats['source_id']}")
    click.echo(f"Active:             {'Yes' if stats['is_active'] else 'No'}")
    click.echo(f"Trust Level:        {stats['trust_level']:.2f}")
    click.echo(f"Pages Crawled:      {stats['total_pages_crawled']}")
    click.echo(f"Candidates Found:   {stats['total_candidates_found']}")
    if stats['avg_candidate_score']:
        click.echo(f"Avg Score:          {stats['avg_candidate_score']:.2f}")
    click.echo(f"Total Crawl Runs:   {stats['total_crawl_runs']}")
    click.echo(f"Successful Runs:    {stats['successful_runs']}")

    if stats['last_crawled_at']:
        click.echo(f"Last Crawled:       {stats['last_crawled_at'].strftime('%Y-%m-%d %H:%M')}")
    if stats['next_crawl_at']:
        click.echo(f"Next Crawl:         {stats['next_crawl_at'].strftime('%Y-%m-%d %H:%M')}")

    click.echo()
    manager.close()


@sources.command('deactivate')
@click.argument('source_id', type=int)
@click.option('--reason', help='Reason for deactivation')
@click.pass_context
def deactivate_source(ctx, source_id, reason):
    """Deactivate a source (stop crawling)."""
    manager = SourceManager(ctx.obj['database_url'])

    manager.deactivate_source(source_id, reason)
    click.echo(f"✓ Deactivated source {source_id}")

    manager.close()


@cli.group()
def crawl():
    """Manage crawl runs."""
    pass


@crawl.command('schedule')
@click.option('--limit', type=int, help='Max sources to crawl')
@click.option('--continuous', is_flag=True, help='Run continuously')
@click.option('--interval', type=int, default=300, help='Check interval (seconds)')
@click.option('--max-concurrent', type=int, default=1, help='Max concurrent crawls')
@click.pass_context
def schedule_crawls(ctx, limit, continuous, interval, max_concurrent):
    """Schedule and run crawls."""
    scheduler = CrawlScheduler(
        database_url=ctx.obj['database_url'],
        max_concurrent_crawls=max_concurrent,
    )

    try:
        if continuous:
            click.echo(f"Starting continuous scheduler (interval: {interval}s)...")
            scheduler.run_continuous(check_interval=interval)
        else:
            click.echo("Running one-time crawl schedule...")
            count = scheduler.run_once(limit=limit)
            click.echo(f"Started {count} crawls")

            if count > 0:
                click.echo("Waiting for crawls to finish...")
                scheduler.wait_for_all_crawls()
                click.echo("All crawls complete")
    except KeyboardInterrupt:
        click.echo("\nInterrupted by user")
    finally:
        scheduler.close()


@crawl.command('history')
@click.option('--source-id', type=int, help='Filter by source ID')
@click.option('--limit', type=int, default=10, help='Max results to show')
@click.pass_context
def crawl_history(ctx, source_id, limit):
    """Show crawl run history."""
    from apps.sources.models import CrawlRun

    manager = SourceManager(ctx.obj['database_url'])

    query = manager.session.query(CrawlRun)
    if source_id:
        query = query.filter_by(source_id=source_id)

    runs = query.order_by(CrawlRun.started_at.desc()).limit(limit).all()

    if not runs:
        click.echo("No crawl runs found")
        manager.close()
        return

    click.echo(f"\n{'ID':<6} {'Source':<25} {'Status':<12} {'Pages':<8} {'Published':<10} {'Started':<20}")
    click.echo("-" * 90)

    for run in runs:
        started = run.started_at.strftime('%Y-%m-%d %H:%M') if run.started_at else 'N/A'
        source_domain = run.source.domain if run.source else 'Unknown'

        click.echo(
            f"{run.id:<6} {source_domain:<25} {run.status.value:<12} "
            f"{run.pages_crawled:<8} {run.candidates_published:<10} {started:<20}"
        )

    click.echo()
    manager.close()


@cli.group()
def db():
    """Database management."""
    pass


@db.command('init')
@click.pass_context
def init_db(ctx):
    """Initialize database (create tables)."""
    click.echo("Initializing database...")
    init_database(ctx.obj['database_url'])
    click.echo("✓ Database initialized")


@db.command('seed')
@click.pass_context
def seed_db(ctx):
    """Seed database with initial sources."""
    click.echo("Seeding database with initial sources...")
    seed_sources(ctx.obj['database_url'])
    click.echo("✓ Database seeded")


@db.command('reset')
@click.confirmation_option(prompt='This will DELETE ALL DATA. Continue?')
@click.pass_context
def reset_db(ctx):
    """Reset database (DELETE ALL DATA)."""
    from apps.sources.models import get_db_manager

    click.echo("Resetting database...")
    db_manager = get_db_manager(ctx.obj['database_url'])
    db_manager.drop_tables()
    db_manager.create_tables()
    click.echo("✓ Database reset")


if __name__ == '__main__':
    cli(obj={})
