import os
import subprocess
import logging
from typing import List

log = logging.getLogger(__name__)

def run_discovery_crawl(urls: List[str], discovery_type: str = "automated"):
    """
    Triggers a Scrapy crawl for the given URLs.
    In a real system, this might be a Celery task or a standalone process.
    """
    if not urls:
        log.warning("No URLs provided for discovery crawl")
        return

    urls_str = ",".join(urls)
    command = [
        "scrapy", "crawl", "generic",
        "-a", f"start_urls={urls_str}",
        "-a", f"discovery_type={discovery_type}"
    ]
    
    log.info(f"Starting discovery crawl for {len(urls)} URLs")
    
    try:
        # We run this in the apps/crawler directory
        result = subprocess.run(
            command,
            cwd="apps/crawler",
            capture_output=True,
            text=True,
            env=os.environ
        )
        
        if result.returncode == 0:
            log.info("Discovery crawl completed successfully")
        else:
            log.error(f"Discovery crawl failed with return code {result.returncode}")
            log.error(f"Stdout: {result.stdout}")
            log.error(f"Stderr: {result.stderr}")
            
    except Exception as e:
        log.exception(f"Error running discovery crawl: {e}")

if __name__ == "__main__":
    # Example usage:
    logging.basicConfig(level=logging.INFO)
    seeds = [
        "https://www.eatdrinklagos.com/roll-call/2021/7/5/the-best-amala-spots-in-lagos-part-i",
        "https://foodieinlagos.com/review-amala-labule-lagos/"
    ]
    run_discovery_crawl(seeds)
