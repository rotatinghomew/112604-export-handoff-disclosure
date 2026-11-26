"""兼容入口：交付领域服务，完整实现见 :mod:`service`。"""

from service import *  # noqa: F401,F403
from service import (  # noqa: F401
    Actor,
    DeliveryService,
    FieldScope,
    PeerClient,
    PermissionDenied,
    Service,
    Store,
    create_service,
    evaluate_proof,
    parse_time,
    proof_is_valid,
    read_jsonl,
    resolve_allowed_fields,
)
