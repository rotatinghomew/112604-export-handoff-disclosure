"""通用入口：``python3 main.py`` 启动 HTTP 交付服务。

业务实现见 :mod:`service`，HTTP 实现见 :mod:`app`。
"""

from service import *  # noqa: F401,F403
from service import DeliveryService, Service, create_service  # noqa: F401
from app import create_app, create_server, main, route  # noqa: F401

if __name__ == "__main__":
    main()
