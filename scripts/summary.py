#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
把 dist/status.json 里的体检结果打印成人类可读的摘要。
CI 里用来在日志中一眼看清这周建成了什么样。
"""

import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
STATUS = os.path.join(HERE, "..", "dist", "status.json")


def main() -> int:
    try:
        with open(STATUS, encoding="utf-8") as f:
            st = json.load(f)
    except Exception as e:
        print(f"读不到 status.json：{e}")
        return 0  # 不让 CI 因为这个红掉

    print("=" * 60)
    print(f"构建时间   : {st.get('generated_at')}")
    print(f"体检站点   : {st.get('total_probed')}")
    print(f"存活       : {st['alive']}")
    print(f"失效       : {st['dead']}")
    print("=" * 60)

    print("\n存活站点（按体检得分排序）：")
    for r in st.get("alive_list", []):
        flag = "可搜" if r.get("search") else "  - "
        det = "详情OK" if r.get("detail") else "  -   "
        print(f"  {r['score']:>6}  {flag} {det}  {r['latency_ms']:>5}ms  {r['api']}")

    dead = st.get("dead_list", [])
    if dead:
        print(f"\n失效站点（前 {min(len(dead), 15)} 个，共 {len(dead)}）：")
        for r in dead[:15]:
            print(f"  {r['api'][:70]:<70} {r.get('reason', '')}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
