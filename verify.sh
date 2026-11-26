#!/usr/bin/env sh
# 一键自检：资料约定测试 + 交付服务测试 + 端到端联调剧本。
set -eu
cd "$(dirname "$0")"

echo "== python3 -m unittest discover -s tests =="
python3 -m unittest discover -s tests -v

echo
echo "== python3 demo_e2e.py =="
python3 demo_e2e.py
