from typing import Optional
import time
from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode
import redis.asyncio as redis

from infrahub import config
from infrahub.message_bus.types import KVTTL
from infrahub.services import InfrahubServices
from infrahub.services.adapters.cache import InfrahubCache


class RedisCache(InfrahubCache):
    def __init__(self) -> None:
        self.connection = redis.Redis(
            host=config.SETTINGS.cache.address,
            port=config.SETTINGS.cache.service_port,
            db=config.SETTINGS.cache.database,
            ssl=config.SETTINGS.cache.tls_enabled,
            ssl_cert_reqs="optional" if not config.SETTINGS.cache.tls_insecure else "none",
            ssl_check_hostname=not config.SETTINGS.cache.tls_insecure,
            ssl_ca_certs=config.SETTINGS.cache.tls_ca_file,
        )

    async def initialize(self, service: InfrahubServices) -> None:
        pass

    async def delete(self, key: str) -> None:
        with trace.get_tracer(__name__).start_as_current_span(
            "cache.delete",
            kind=SpanKind.CLIENT,
            attributes={
                "db.system": "redis",
                "db.name": "cache",
                "db.operation": "delete",
                "db.redis.database_index": config.SETTINGS.cache.database,
                "db.key": key,
                "net.peer.name": config.SETTINGS.cache.address,
                "net.peer.port": config.SETTINGS.cache.service_port,
                "peer.service": "cache",
                "service.name": "infrahub"
            }
        ) as span:
            try:
                start_time = time.time()
                await self.connection.delete(key)
                span.set_attribute("duration_ms", (time.time() - start_time) * 1000)
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

    async def get(self, key: str) -> Optional[str]:
        with trace.get_tracer(__name__).start_as_current_span(
            "cache.get",
            kind=SpanKind.CLIENT,
            attributes={
                "db.system": "redis",
                "db.name": "cache",
                "db.operation": "get",
                "db.redis.database_index": config.SETTINGS.cache.database,
                "db.key": key,
                "net.peer.name": config.SETTINGS.cache.address,
                "net.peer.port": config.SETTINGS.cache.service_port,
                "peer.service": "cache",
                "service.name": "infrahub"
            }
        ) as span:
            try:
                start_time = time.time()
                value = await self.connection.get(name=key)
                span.set_attribute("duration_ms", (time.time() - start_time) * 1000)
                span.set_attribute("cache.hit", value is not None)
                if value is not None:
                    return value.decode()
                return None
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

    async def get_values(self, keys: list[str]) -> list[Optional[str]]:
        with trace.get_tracer(__name__).start_as_current_span(
            "cache.mget",
            kind=SpanKind.CLIENT,
            attributes={
                "db.system": "redis",
                "db.name": "cache",
                "db.operation": "mget",
                "db.redis.database_index": config.SETTINGS.cache.database,
                "db.keys": str(keys),
                "db.keys.count": len(keys),
                "net.peer.name": config.SETTINGS.cache.address,
                "net.peer.port": config.SETTINGS.cache.service_port,
                "peer.service": "cache",
                "service.name": "infrahub"
            }
        ) as span:
            try:
                start_time = time.time()
                values = await self.connection.mget(keys=keys)
                hits = sum(1 for v in values if v is not None)
                span.set_attribute("duration_ms", (time.time() - start_time) * 1000)
                span.set_attribute("cache.hits", hits)
                span.set_attribute("cache.misses", len(keys) - hits)
                return [value.decode() if value is not None else value for value in values]
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

    async def list_keys(self, filter_pattern: str) -> list[str]:
        with trace.get_tracer(__name__).start_as_current_span(
            "cache.scan",
            kind=SpanKind.CLIENT,
            attributes={
                "db.system": "redis",
                "db.name": "cache",
                "db.operation": "scan",
                "db.redis.database_index": config.SETTINGS.cache.database,
                "db.scan.pattern": filter_pattern,
                "net.peer.name": config.SETTINGS.cache.address,
                "net.peer.port": config.SETTINGS.cache.service_port,
                "peer.service": "cache",
                "service.name": "infrahub"
            }
        ) as span:
            try:
                start_time = time.time()
                cursor = 0
                has_remaining_keys = True
                keys = []
                scan_count = 0
                
                while has_remaining_keys:
                    cursor, scanned_keys = await self.connection.scan(cursor=cursor, match=filter_pattern, count=100)
                    keys.extend(scanned_keys)
                    scan_count += 1
                    if cursor == 0:
                        has_remaining_keys = False

                span.set_attribute("duration_ms", (time.time() - start_time) * 1000)
                span.set_attribute("scan.iterations", scan_count)
                span.set_attribute("keys.found", len(keys))
                
                return [key.decode() for key in keys]
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

    async def set(
        self, key: str, value: str, expires: Optional[KVTTL] = None, not_exists: bool = False
    ) -> Optional[bool]:
        with trace.get_tracer(__name__).start_as_current_span(
            "cache.set",
            kind=SpanKind.CLIENT,
            attributes={
                "db.system": "redis",
                "db.name": "cache",
                "db.operation": "set",
                "db.redis.database_index": config.SETTINGS.cache.database,
                "db.key": key,
                "db.ttl": expires.value if expires else None,
                "db.set.nx": not_exists,
                "net.peer.name": config.SETTINGS.cache.address,
                "net.peer.port": config.SETTINGS.cache.service_port,
                "peer.service": "cache",
                "service.name": "infrahub"
            }
        ) as span:
            try:
                start_time = time.time()
                result = await self.connection.set(
                    name=key, 
                    value=value, 
                    ex=expires.value if expires else None, 
                    nx=not_exists
                )
                span.set_attribute("duration_ms", (time.time() - start_time) * 1000)
                return result
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise
