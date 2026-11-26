"""兼容入口：HTTP 服务实现见 :mod:`app`。"""

from app import *  # noqa: F401,F403
import app as _app  # noqa: F401
from app import create_app, create_server, create_service, main, make_handler, route  # noqa: F401

__all__ = ["create_app", "create_server", "create_service", "make_handler", "route", "main"]
