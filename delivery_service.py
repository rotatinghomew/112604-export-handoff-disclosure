"""兼容入口：完整实现见 :mod:`service`。"""

from service import *  # noqa: F401,F403
import service as _service  # noqa: F401

from service import (  # noqa: F401
    Actor,
    DeliveryService,
    FieldScope,
    PeerClient,
    PeerResponse,
    PermissionDenied,
    Service,
    ServiceError,
    Store,
    evaluate_proof,
    format_time,
    parse_time,
    proof_is_valid,
    resolve_allowed_fields,
)

__all__ = getattr(_service, "__all__", [])
