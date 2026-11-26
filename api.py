"""兼容入口：HTTP 路由与工厂见 :mod:`app`，领域服务见 :mod:`service`。"""

from service import *  # noqa: F401,F403
from app import (  # noqa: F401
    create_app,
    create_server,
    create_service,
    json_default,
    main,
    make_handler,
    route,
)
