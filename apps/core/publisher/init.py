#!/usr/bin/env python3
"""
Solid Backend Client for Publishing Candidate Data

A robust, production-ready client that integrates with your Django backend
to publish candidate data with proper authentication, error handling, and retry logic.
"""

import asyncio
import hashlib
import logging
import time
from dataclasses import dataclass
from typing import Dict, Optional, Any
from urllib.parse import urljoin

import aiohttp
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class ClientConfig:

    base_url: str
    token: str
    timeout: int = 30
    max_retries: int = 3
    backoff_factor: float = 1.0
    verify_ssl: bool = True


@dataclass
class PublishResponse:

    success: bool
    status_code: int
    data: Optional[Dict] = None
    error: Optional[str] = None
    idempotency_key: Optional[str] = None


class BackendClientError(Exception):
    """Base exception for backend client errors."""
    pass


class AuthenticationError(BackendClientError):
    """Raised when authentication fails."""
    pass


class ValidationError(BackendClientError):
    """Raised when payload validation fails."""
    pass


class ServerError(BackendClientError):
    """Raised when server returns 5xx errors."""
    pass


class BackendClient:
    """
    Solid backend client for publishing candidate data to Django backend.

    Features:
    - Automatic authentication handling
    - Idempotency key generation
    - Retry logic with exponential backoff
    - Comprehensive error handling
    - Request/response logging
    - Payload validation
    """

    def __init__(self, config: ClientConfig):
        self.config = config
        self.session = None
        self._headers = {
            'Authorization': f'Bearer {self.config.token}',
            'Content-Type': 'application/json',
            'User-Agent': 'BackendClient/1.0',
        }

    async def __aenter__(self):
        """Async context manager entry."""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.config.timeout),
            connector=aiohttp.TCPConnector(verify_ssl=self.config.verify_ssl),
            headers=self._headers
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()

    def _generate_idempotency_key(self, payload: Dict) -> str:
        """
        Generate a deterministic idempotency key from payload content.
        Uses payload hash to ensure the same data generates the same key.
        """
        # Create a stable string representation
        payload_str = str(sorted(payload.items()))
        hash_obj = hashlib.sha256(payload_str.encode('utf-8'))
        return f"client_{hash_obj.hexdigest()[:16]}_{int(time.time())}"

    def _validate_payload(self, payload: Dict) -> None:
        """Validate payload structure before sending."""
        if not isinstance(payload, dict):
            raise ValidationError("Payload must be a dictionary")

        if not payload:
            raise ValidationError("Payload cannot be empty")

        # Auto-discovery candidates have 'candidate_key' or 'fields'
        # We don't want to block publishing if some fields are missing,
        # but we need at least a name or a reference.
        pass

    def _handle_response_error(self, status_code: int, response_text: str) -> None:
        """Handle HTTP error responses with appropriate exceptions."""
        if status_code == 401:
            raise AuthenticationError("Authentication failed - check your token")
        elif status_code == 400:
            raise ValidationError(f"Validation error: {response_text}")
        elif status_code == 422:
            raise ValidationError(f"Payload validation failed: {response_text}")
        elif 500 <= status_code < 600:
            raise ServerError(f"Server error ({status_code}): {response_text}")
        else:
            raise BackendClientError(f"HTTP {status_code}: {response_text}")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((ServerError, aiohttp.ClientError))
    )
    async def publish_candidate(
            self,
            payload: Dict[str, Any],
            idempotency_key: Optional[str] = None
    ) -> PublishResponse | None:
        """
        Publish candidate data to the backend with idempotency support.

        Args:
            payload: Candidate data to publish
            idempotency_key: Optional custom idempotency key

        Returns:
            PublishResponse: Response object with success status and data

        Raises:
            AuthenticationError: When authentication fails
            ValidationError: When payload validation fails
            ServerError: When server returns 5xx errors
            BackendClientError: For other HTTP errors
        """
        if not self.session:
            raise BackendClientError("Client not initialized. Use 'async with' context manager.")

        # Validate payload
        self._validate_payload(payload)

        # Generate an idempotency key if not provided
        if not idempotency_key:
            idempotency_key = self._generate_idempotency_key(payload)

        # Prepare request
        url = urljoin(self.config.base_url.rstrip('/') + '/', 'api/ingest/candidate')
        headers = {
            **self._headers,
            'Idempotency-Key': idempotency_key,
        }

        logger.info(f"Publishing candidate to {url} with key: {idempotency_key}")
        logger.debug(f"Payload: {payload}")

        try:
            async with self.session.post(url, json=payload, headers=headers) as response:
                response_text = await response.text()

                # Handle successful responses
                if response.status == 200:
                    try:
                        data = await response.json()
                        logger.info(f"Successfully published candidate: {idempotency_key}")
                        return PublishResponse(
                            success=True,
                            status_code=response.status,
                            data=data,
                            idempotency_key=idempotency_key
                        )
                    except ValueError:
                        # Handle non-JSON response
                        return PublishResponse(
                            success=True,
                            status_code=response.status,
                            data={'message': response_text},
                            idempotency_key=idempotency_key
                        )

                # Handle idempotency (already processed)
                elif response.status == 409:
                    logger.info(f"Candidate already processed (idempotent): {idempotency_key}")
                    return PublishResponse(
                        success=True,
                        status_code=response.status,
                        data={'message': 'Already processed'},
                        idempotency_key=idempotency_key
                    )

                # Handle errors
                else:
                    logger.error(f"Failed to publish candidate: {response.status} - {response_text}")
                    self._handle_response_error(response.status, response_text)

        except aiohttp.ClientError as e:
            logger.error(f"Network error publishing candidate: {e}")
            raise ServerError(f"Network error: {e}")
        except Exception as e:
            logger.error(f"Unexpected error publishing candidate: {e}")
            return PublishResponse(
                success=False,
                status_code=0,
                error=str(e),
                idempotency_key=idempotency_key
            )

    async def publish_batch(
            self,
            candidates: list[Dict[str, Any]],
            max_concurrent: int = 5
    ) -> list[PublishResponse]:
        """
        Publish multiple candidates concurrently with rate limiting.

        Args:
            candidates: List of candidate payloads
            max_concurrent: Maximum concurrent requests

        Returns:
            List of PublishResponse objects
        """
        semaphore = asyncio.Semaphore(max_concurrent)

        async def publish_single(candidate):
            async with semaphore:
                return await self.publish_candidate(candidate)

        logger.info(f"Publishing {len(candidates)} candidates with max {max_concurrent} concurrent requests")

        tasks = [publish_single(candidate) for candidate in candidates]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Convert exceptions to failed responses
        responses = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                responses.append(PublishResponse(
                    success=False,
                    status_code=0,
                    error=str(result),
                    idempotency_key=f"batch_{i}_{int(time.time())}"
                ))
            else:
                responses.append(result)

        success_count = sum(1 for r in responses if r.success)
        logger.info(f"Batch complete: {success_count}/{len(candidates)} successful")

        return responses

    async def health_check(self) -> bool:
        """
        Check if the backend is healthy and authentication works.

        Returns:
            bool: True if backend is healthy and authenticated
        """
        if not self.session:
            return False

        try:
            # Try a simple endpoint (adjust based on your API)
            url = urljoin(self.config.base_url.rstrip('/') + '/', 'api/health')

            async with self.session.get(url) as response:
                is_healthy = response.status == 200
                logger.info(f"Health check: {'✓' if is_healthy else '✗'}")
                return is_healthy

        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False


# Synchronous wrapper for simpler usage
class SyncBackendClient:
    """Synchronous wrapper around the async BackendClient."""

    def __init__(self, config: ClientConfig):
        self.config = config
        self._session = requests.Session()
        self._session.headers.update({
            'Authorization': f'Bearer {self.config.token}',
            'Content-Type': 'application/json',
            'User-Agent': 'BackendClient/1.0',
        })
        self._session.verify = self.config.verify_ssl

    def publish_candidate(
            self,
            payload: Dict[str, Any],
            idempotency_key: Optional[str] = None
    ) -> PublishResponse:
        """Synchronous version of publish_candidate."""

        # Basic validation
        if not isinstance(payload, dict) or not payload:
            return PublishResponse(
                success=False,
                status_code=400,
                error="Invalid payload"
            )

        # Generate idempotency key
        if not idempotency_key:
            payload_str = str(sorted(payload.items()))
            hash_obj = hashlib.sha256(payload_str.encode('utf-8'))
            idempotency_key = f"sync_{hash_obj.hexdigest()[:16]}_{int(time.time())}"

        url = urljoin(self.config.base_url.rstrip('/') + '/', 'api/ingest/candidate')
        headers = {'Idempotency-Key': idempotency_key}

        logger.info(f"Publishing candidate (sync) to {url} with key: {idempotency_key}")

        try:
            response = self._session.post(
                url,
                json=payload,
                headers=headers,
                timeout=self.config.timeout
            )

            if response.status_code in (200, 201, 409):
                try:
                    data = response.json()
                except ValueError:
                    data = {'message': response.text}

                return PublishResponse(
                    success=True,
                    status_code=response.status_code,
                    data=data,
                    idempotency_key=idempotency_key
                )
            else:
                logger.error(f"Failed to publish candidate: {response.status_code} - {response.text}")
                return PublishResponse(
                    success=False,
                    status_code=response.status_code,
                    error=response.text,
                    idempotency_key=idempotency_key
                )

        except requests.RequestException as e:
            logger.error(f"Network error publishing candidate: {e}")
            return PublishResponse(
                success=False,
                status_code=0,
                error=str(e),
                idempotency_key=idempotency_key
            )


# Example usage and testing
async def main():
    """Example usage of the BackendClient."""

    config = ClientConfig(
        base_url="https://your-backend.com",
        token="your-auth-token-here",
        timeout=30,
        max_retries=3
    )

    # Example candidate data
    sample_candidate = {
        "name": "Test Restaurant",
        "phone": "07036174617",
        "address": "123 Test Street, Lagos",
        "city": "Lagos",
        "extracted_content": {
            "title": "Best Jollof Rice in Lagos",
            "text": "We serve the best jollof rice...",
            "publish_date": "2024-12-01",
            "is_recent": True
        },
        "social_links": {
            "instagram": "https://instagram.com/testrestaurant"
        },
        "score": 0.85
    }

    # Async usage
    async with BackendClient(config) as client:
        # Health check
        if await client.health_check():
            # Publish a single candidate
            response = await client.publish_candidate(payload=sample_candidate)
            print(f"Publish result: {response}")

            # Publish batch
            candidates = [sample_candidate] * 3  # Example batch
            batch_results = await client.publish_batch(candidates)
            print(f"Batch results: {len([r for r in batch_results if r.success])} successful")

    # Sync usage
    sync_client = SyncBackendClient(config)
    sync_response = sync_client.publish_candidate(sample_candidate)
    print(f"Sync publish result: {sync_response}")


if __name__ == "__main__":
    asyncio.run(main())
