#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TVBox 个人点播单源生成器
============================================================
做三件事：

  1. 采集   —— 从 config/aggregators.txt 里的公共聚合配置中抽取
               type=1（JSON CMS 直连，免 spider.jar）的站点，
               再并入 config/seeds.txt 里手工维护的种子。
  2. 校验   —— 每个候选真实请求一次 ?ac=videolist，同时多线程测
               分类列表和搜索接口，确认它活着、确实吐 JSON、
               而且能搜到东西。任何一项不过就直接丢掉。
  3. 合成   —— 输出一份纯 CMS 的 TVBox 单仓配置（不需要 jar），
               外加一个 status.json 记录本次体检结果。

核心思路：不依赖任何一个"单源"，而是每次从一大堆源里现场捞活的。
         所以别人的源挂了，你的源还能自己长回来。
============================================================
用法：
    python build.py                 # 正常构建
    python build.py --dry-run       # 只采集和报告，不写文件
    python build.py --limit 20      # 只保留体检得分最高的 20 个站
"""

from __future__ import annotations

import argparse
import concurrent.futures as futures
import hashlib
import json
import os
import random
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
from typing import Any, Iterable

# ---------------------------------------------------------------- 常量

ROOT = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(ROOT, "config")
OUTPUT_DIR = os.path.join(ROOT, "dist")

CST = timezone(timedelta(hours=8))

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# 有些采集站会挑 UA，这里备用几个
UA_POOL = [
    UA,
    "okhttp/3.12.13",
    "Dalvik/2.1.0 (Linux; U; Android 11; SM-G975F Build/RP1A.200720.012)",
    "Mozilla/5.0 (Linux; Android 10; TV) AppleWebKit/537.36 Chrome/92.0.4515.159 Safari/537.36",
]

# 超时（秒）。CI 上网络抖，给宽一点。
TIMEOUT_DISCOVERY = 20      # 抓聚合配置
TIMEOUT_LIST = 12           # 站点分类列表
TIMEOUT_SEARCH = 15         # 站点搜索
TIMEOUT_DETAIL = 15         # 站点详情/播放链
TIMEOUT_PLAY = 15           # 实测拉 m3u8（视频 CDN 比 API 慢，给足）

# 是否实测播放。开启后会真正拉一次 m3u8 验证能不能播，
# 代价是体检变慢，收益是直接把"死链站"挡在门外。
PLAY_TEST = True

MAX_BYTES = 8 * 1024 * 1024  # 单个响应最多读 8MB，防止被巨型响应拖死

# 排序用的分类前缀
CATEGORY_PREFIX = "点播"

# 判定"这是个 CMS 采集接口"的特征
CMS_HINTS = ("/api.php/provide/vod", "/provide/vod", "apijson.php", "provide/vod")

# 站名里常见的纯分类词，清洗时直接丢掉
CATEGORY_WORDS = {
    "影视", "点播", "采集", "直连", "资源", "片库", "高速", "备用",
    "推荐", "精选", "热门", "切", "线路", "官方", "永久", "免费",
}


# ---------------------------------------------------------------- 工具

class Log:
    def __init__(self) -> None:
        self.t0 = time.time()

    def __call__(self, msg: str) -> None:
        el = time.time() - self.t0
        print(f"[{el:7.1f}s] {msg}", flush=True)


log = Log()


def idna_url(url: str) -> str:
    """
    把中文域名和中文路径转成可直接请求的形式。

    这是之前最大的隐藏 bug：肥猫/饭太硬/盒子迷这些源的域名和路径大量是中文，
    而 urllib 的底层只接受 ASCII，会直接抛 UnicodeEncodeError，
    导致这些源在探测阶段就被判"拉取失败"，实际上根本没发出请求。
    """
    try:
        p = urllib.parse.urlsplit(url)
        host = p.hostname
        if not host:
            return url
        try:
            ascii_host = host.encode("idna").decode("ascii")
        except Exception:
            ascii_host = host
        netloc = ascii_host
        if p.port:
            netloc += ":" + str(p.port)
        path = urllib.parse.quote(p.path or "/", safe="/%")
        query = urllib.parse.quote(p.query, safe="=&%")
        return urllib.parse.urlunsplit((p.scheme or "http", netloc, path, query, ""))
    except Exception:
        return url


def http_get(
    url: str,
    timeout: int = TIMEOUT_LIST,
    ua: str | None = None,
    headers: dict[str, str] | None = None,
) -> bytes | None:
    """GET 一个 URL，返回原始字节；任何失败都返回 None，绝不抛异常。"""
    h = {
        "User-Agent": ua or UA,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Connection": "close",
    }
    if headers:
        h.update(headers)
    req = urllib.request.Request(idna_url(url), headers=h)
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE  # 采集站证书经常有问题，别因为这个误杀
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            if getattr(resp, "status", 200) >= 400:
                return None
            data = resp.read(MAX_BYTES)
            return data
    except Exception:
        return None


_RAW_GH = re.compile(
    r"^https?://raw\.githubusercontent\.com/([^/]+)/([^/]+)/([^/]+)/(.+)$", re.I
)


def mirror_urls(url: str) -> list[str]:
    """
    给 GitHub raw 地址生成一组镜像，按"通常最快最稳"的顺序排列。
    国内网络下 raw.githubusercontent.com 时通时不通，这是候选池
    命中率最大的瓶颈，所以这里必须做多级回退。
    """
    m = _RAW_GH.match(url)
    if not m:
        return [url]
    owner, repo, br, path = m.groups()
    return [
        url,                                                     # 直连
        f"https://ghproxy.net/https://raw.githubusercontent.com/{owner}/{repo}/{br}/{path}",
        f"https://gcore.jsdelivr.net/gh/{owner}/{repo}@{br}/{path}",
        f"https://cdn.jsdelivr.net/gh/{owner}/{repo}@{br}/{path}",
        f"https://raw.gitmirror.com/{owner}/{repo}/{br}/{path}",
        f"https://gh-proxy.com/https://raw.githubusercontent.com/{owner}/{repo}/{br}/{path}",
    ]


def fetch_config(url: str, timeout: int = TIMEOUT_DISCOVERY) -> tuple[Any, str]:
    """
    抓一个配置，自动尝试所有镜像。
    返回 (解析后的对象 或 None, 实际成功使用的地址)。
    """
    for u in mirror_urls(url):
        raw = http_get(u, timeout=timeout)
        if not raw:
            continue
        obj = json_maybe(raw)
        if obj is not None:
            return obj, u
    return None, ""


def json_maybe(raw: bytes | None) -> Any:
    """
    尽量把响应解析成 JSON。

    TVBox 生态里大量配置是"非严格 JSON"，必须容错：
      * 开头有 // 整行注释（老刘备、盒子迷这类很常见）
      * 字段里混入未转义的控制字符
      * 前后有 BOM / <script> / 空白杂质
    解析失败会直接导致整份配置被丢弃，候选人头白丢，所以这里要尽量宽。
    """
    if not raw:
        return None
    for enc in ("utf-8-sig", "utf-8", "gb18030"):
        try:
            text = raw.decode(enc)
        except Exception:
            continue
        text = text.strip()
        if not text:
            return None

        # 尝试多种候选文本形态，从"原样"到"清洗过的"
        candidates = [text]

        # 1) 去掉 // 整行注释（只去掉行首就是 // 的，避免破坏 URL 里的 //）
        if "//" in text:
            stripped = "\n".join(
                ln for ln in text.splitlines() if not ln.lstrip().startswith("//")
            )
            if stripped != text:
                candidates.append(stripped)

        # 2) 定位 JSON 起点（跳过注释和杂质）
        for base in list(candidates):
            if not base.lstrip().startswith(("{", "[")):
                m = re.search(r"[\[{]", base)
                if m:
                    candidates.append(base[m.start():])

        for cand in candidates:
            cand = cand.strip()
            if not cand:
                continue
            # 先严格，再宽松（strict=False 允许字符串里有裸控制字符）
            for strict in (True, False):
                try:
                    return json.loads(cand, strict=strict)
                except Exception:
                    pass
            # 3) 兜底：截到最后一个 } 或 ]
            for close in ("}", "]"):
                idx = cand.rfind(close)
                if idx > 0:
                    try:
                        return json.loads(cand[: idx + 1], strict=False)
                    except Exception:
                        pass
    return None


def normalize_api(url: str) -> str:
    """把接口地址清理成 TVBox 期望的干净形态（去掉查询串和结尾斜杠）。"""
    url = url.strip()
    if not url:
        return ""
    # 有的配置写成 "xxx?ac=list" / "xxx&ac=detail"
    url = re.split(r"[?#]", url)[0]
    return url.rstrip("/")


def site_signature(api: str) -> str:
    """用于去重的稳定指纹：主机名 + 路径，忽略协议和末尾差异。"""
    p = urllib.parse.urlsplit(api if "://" in api else "http://" + api)
    host = (p.hostname or "").lower()
    path = (p.path or "").rstrip("/").lower()
    # 同一台机器上的不同片库路径要保留，所以 path 参与指纹
    return f"{host}{path}"


def safe_key(host: str) -> str:
    h = re.sub(r"[^a-z0-9]+", "", host.lower())
    return h[:24] or "s"


# ---------------------------------------------------------------- 采集

def read_config_lines(path: str) -> list[str]:
    if not os.path.exists(path):
        return []
    out = []
    with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            out.append(s)
    return out


def load_blocklist() -> tuple[str, ...]:
    """读 config/blocklist.txt，返回小写关键词元组。"""
    words = []
    for line in read_config_lines(os.path.join(CONFIG_DIR, "blocklist.txt")):
        w = line.strip().lower()
        if w:
            words.append(w)
    return tuple(words)


BLOCKLIST: tuple[str, ...] = ()


def is_blocked(name: str, api: str) -> bool:
    """域名或站名命中黑名单就丢弃。"""
    if not BLOCKLIST:
        return False
    hay = f"{name} {api}".lower()
    return any(w in hay for w in BLOCKLIST)


def load_seeds() -> list[dict[str, str]]:
    """读 config/seeds.txt：名称, API"""
    seeds = []
    for line in read_config_lines(os.path.join(CONFIG_DIR, "seeds.txt")):
        parts = [p.strip() for p in re.split(r"[,，]", line, maxsplit=1)]
        if len(parts) < 2:
            continue
        name, api = parts[0], normalize_api(parts[1])
        if api and "://" in api:
            seeds.append({"name": name, "api": api, "origin": "seed"})
    return seeds


def load_aggregators() -> list[list[str]]:
    """读 config/aggregators.txt：分组名 | URL | URL ...  → 返回 URL 组列表"""
    groups = []
    for line in read_config_lines(os.path.join(CONFIG_DIR, "aggregators.txt")):
        parts = [p.strip() for p in line.split("|")]
        urls = [p for p in parts[1:] if p.startswith(("http://", "https://"))]
        if urls:
            groups.append(urls)
    return groups


def harvest_api_strings(raw: bytes) -> list[str]:
    """
    从任意配置文本里正则捞 CMS 接口地址。

    这是候选池最重要的来源：绝大多数公共配置以 type=3（spider）站为主，
    但它们的 parses / ext / 说明里经常散落着大量可直连的 CMS 接口。
    我们不用 spider，但这些直连接口正是我们要的。
    """
    if not raw:
        return []
    text = raw.decode("utf-8", "replace")
    found: list[str] = []

    # 1) 带 CMS 特征的完整 URL
    for m in re.finditer(r"https?://[^\s\"'<>,;\\]+", text):
        u = m.group(0)
        low = u.lower()
        if any(hint in low for hint in CMS_HINTS):
            found.append(u)

    # 2) JSON 里被转义过的（\/）
    unescaped = text.replace("\\/", "/")
    if unescaped != text:
        for m in re.finditer(r"https?://[^\s\"'<>,;\\]+", unescaped):
            u = m.group(0)
            if any(hint in u.lower() for hint in CMS_HINTS):
                found.append(u)

    out = []
    for u in found:
        api = normalize_api(u)
        if api and api.lower().startswith(("http://", "https://")):
            out.append(api)
    return out


def harvest_text_api_lists(raw: bytes) -> list[dict[str, str]]:
    """
    解析 "名称|地址" 或 "名称, 地址" 或纯地址列表格式的接口清单。
    典型来源：lubin776/tvbox-api-backup 的 cjlist.txt / apilinks.txt。
    """
    if not raw:
        return []
    text = raw.decode("utf-8-sig", "replace")
    out: list[dict[str, str]] = []
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        # 找这一行里所有 http(s) 地址
        urls = re.findall(r"https?://[^\s,，|｜]+", s)
        if not urls:
            continue
        # 名称：取第一个分隔符前的内容
        name = re.split(r"[,，|｜]", s, maxsplit=1)[0].strip()
        name = re.sub(r"^[-\s]+", "", name)
        for u in urls:
            api = normalize_api(u)
            if api and any(h in api.lower() for h in CMS_HINTS):
                out.append({"name": name, "api": api, "origin": "apilist"})
    return out


def extract_sites_from_config(obj: Any) -> list[dict[str, Any]]:
    """从任意 TVBox 配置里挖出所有 type=1 的站点（兼容多仓/单仓）。"""
    found: list[dict[str, Any]] = []

    def walk(node: Any, depth: int = 0) -> None:
        if depth > 6 or node is None:
            return
        if isinstance(node, list):
            for item in node:
                walk(item, depth + 1)
            return
        if not isinstance(node, dict):
            return
        sites = node.get("sites")
        if isinstance(sites, list):
            for s in sites:
                if not isinstance(s, dict):
                    continue
                # 只收 type=1（JSON CMS 直连）。type 可能是 "1" 或 1。
                if str(s.get("type", "")).strip() != "1":
                    continue
                api = s.get("api") or s.get("url") or ""
                if not isinstance(api, str):
                    continue
                api = normalize_api(api)
                if not api.lower().startswith(("http://", "https://")):
                    continue
                found.append(
                    {
                        "name": str(s.get("name") or "").strip(),
                        "api": api,
                        "origin": "aggregator",
                        "ext": str(s.get("ext") or "").strip(),
                    }
                )
        # 多仓：urls 里是子配置地址，这里不递归下载，交给下一轮
        for k in ("sites", "parses", "lives", "config", "urls"):
            if k in node and k != "sites":
                v = node[k]
                if isinstance(v, (dict, list)):
                    walk(v, depth + 1)

    walk(obj)
    return found


def collect_candidates(include_github_search: bool = True) -> list[dict[str, Any]]:
    """汇合种子 + 所有聚合配置里的 CMS 站点。"""
    cands: list[dict[str, Any]] = []

    # 1) 种子
    seeds = load_seeds()
    cands.extend(seeds)
    log(f"种子接口 {len(seeds)} 个")

    # 2) 聚合配置
    groups = load_aggregators()
    log(f"聚合配置 {len(groups)} 组，开始抓取 …")

    fetched_ok = 0
    fetched_fail = 0
    failed_urls: list[str] = []

    def grab(group: list[str]) -> list[dict[str, Any]]:
        for url in group:
            obj, used = fetch_config(url)
            if obj is None:
                continue
            got = extract_sites_from_config(obj)
            # 即使没有 type=1 站点，也从整份配置里正则捞 CMS 接口
            got.extend(
                {"name": "", "api": a, "origin": "harvest"}
                for a in harvest_api_strings(
                    json.dumps(obj, ensure_ascii=False).encode("utf-8")
                )
            )
            if got:
                for g in got:
                    g["source_url"] = used or url
                return got
        return []

    with futures.ThreadPoolExecutor(max_workers=16) as ex:
        for group, res in zip(groups, ex.map(grab, groups)):
            if res:
                fetched_ok += 1
                cands.extend(res)
            else:
                fetched_fail += 1
                failed_urls.append(group[0])

    log(f"聚合配置抓取成功 {fetched_ok} / 失败 {fetched_fail}，"
        f"累计候选 {len(cands)} 个")
    if failed_urls:
        log(f"  （失败的配置：{', '.join(failed_urls[:8])}"
            f"{' …' if len(failed_urls) > 8 else ''}）")

    # 3) 专门的 CMS 接口清单（这批最"干净"，几乎全是可直连的采集站）
    try:
        extra_lists = harvest_apilist_repos()
        if extra_lists:
            cands.extend(extra_lists)
            log(f"接口清单补充候选 {len(extra_lists)} 个")
    except Exception as e:
        log(f"接口清单跳过：{e}")

    # 4) 可选：GitHub 上搜索更多聚合配置（给候选池续命）
    if include_github_search:
        try:
            extra = github_search_configs()
            if extra:
                cands.extend(extra)
                log(f"GitHub 检索补充候选 {len(extra)} 个")
        except Exception as e:
            log(f"GitHub 检索跳过：{e}")

    # 4) 去重（同时做黑名单过滤）
    seen: dict[str, dict[str, Any]] = {}
    blocked = 0
    for c in cands:
        api = normalize_api(c.get("api", ""))
        if not api:
            continue
        if is_blocked(str(c.get("name") or ""), api):
            blocked += 1
            continue
        sig = site_signature(api)
        if sig in seen:
            continue
        c["api"] = api
        seen[sig] = c

    uniq = list(seen.values())
    if blocked:
        log(f"黑名单过滤掉 {blocked} 个站点")
    log(f"去重后候选 {len(uniq)} 个")
    return uniq


def harvest_apilist_repos() -> list[dict[str, Any]]:
    """
    抓取专门维护"CMS 接口清单"的仓库。
    这类清单比聚合配置干净得多——里面基本全是可直接用的采集站。
    """
    sources = [
        ("lubin776/tvbox-api-backup", "main", "cjlist.txt"),
        ("lubin776/tvbox-api-backup", "main", "api_list.json"),
        ("lubin776/tvbox-api-backup", "main", "list.txt"),
        ("lubin776/tvbox-api-backup", "main", "config.json"),
    ]
    out: list[dict[str, Any]] = []
    for repo, br, path in sources:
        url = f"https://raw.githubusercontent.com/{repo}/{br}/{path}"
        for u in mirror_urls(url):
            raw = http_get(u, timeout=TIMEOUT_DISCOVERY)
            if not raw:
                continue
            got = harvest_text_api_lists(raw)
            if not got:
                obj = json_maybe(raw)
                if obj is not None:
                    got = [
                        {"name": s.get("name", ""), "api": a, "origin": "apilist"}
                        for s in extract_sites_from_config(obj)
                        for a in [s["api"]]
                    ]
                    got += [
                        {"name": "", "api": a, "origin": "harvest"}
                        for a in harvest_api_strings(
                            json.dumps(obj, ensure_ascii=False).encode("utf-8")
                        )
                    ]
            if got:
                for g in got:
                    g["source_url"] = f"{repo}/{path}"
                out.extend(got)
                break
    return out


def github_search_configs() -> list[dict[str, Any]]:
    """
    在 GitHub 上搜 TVBox 配置仓库，抓它们根目录的 json 配置。
    没有 token 时限速 10 次/分钟，所以只做很小的量；有 token 会好些。
    这一步纯粹是给候选池续命用的，失败完全不影响主流程。
    """
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "tvbox-selfbuild"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    def api(url: str) -> Any:
        raw = http_get(url, timeout=20, headers=headers)
        return json_maybe(raw)

    q = urllib.parse.quote("tvbox config in:name,description pushed:>2026-01-01")
    res = api(f"https://api.github.com/search/repositories?q={q}&sort=stars&order=desc&per_page=12")
    if not isinstance(res, dict):
        return []

    repos = []
    for item in (res.get("items") or [])[:12]:
        full = item.get("full_name")
        br = item.get("default_branch") or "main"
        if full:
            repos.append((full, br))

    out: list[dict[str, Any]] = []
    for full, br in repos:
        tree = api(f"https://api.github.com/repos/{full}/git/trees/{br}?recursive=1")
        if not isinstance(tree, dict):
            continue
        paths = [
            e["path"]
            for e in (tree.get("tree") or [])
            if e.get("type") == "blob"
            and e["path"].lower().endswith(".json")
            and e["path"].count("/") <= 1
            and e.get("size", 0) < 1_500_000
        ]
        # 只挑名字像配置的，避免抓一堆 package.json
        paths = [p for p in paths if not re.search(r"(package|tsconfig|launch|settings|lock)", p, re.I)]
        for p in paths[:4]:
            raw = http_get(
                f"https://raw.githubusercontent.com/{full}/{br}/{urllib.parse.quote(p)}",
                timeout=20,
            )
            obj = json_maybe(raw)
            if obj is None:
                continue
            got = extract_sites_from_config(obj)
            for g in got:
                g["source_url"] = f"github:{full}/{p}"
            if got:
                out.extend(got)
    return out


# ---------------------------------------------------------------- 校验

class Probe:
    """一个站点的完整体检报告。"""

    __slots__ = (
        "api", "name", "origin", "ok", "score", "reason",
        "latency_ms", "categories", "sample", "has_search", "detail_ok", "episodes", "play_ok",
        "fingerprint",
    )

    def __init__(self, api: str, name: str, origin: str) -> None:
        self.api = api
        self.name = name
        self.origin = origin
        self.ok = False
        self.score = 0.0
        self.reason = ""
        self.latency_ms = 0
        self.categories: list[str] = []
        self.sample = ""
        self.has_search = False
        self.detail_ok = False
        self.episodes = 0
        self.play_ok = False
        self.fingerprint = ""


def _vod_list(obj: Any) -> list[dict[str, Any]]:
    """从 CMS 响应里取 list 数组，兼容各种字段名。"""
    if not isinstance(obj, dict):
        return []
    for key in ("list", "data", "result", "videos"):
        v = obj.get(key)
        if isinstance(v, list):
            return [x for x in v if isinstance(x, dict)]
        if isinstance(v, dict):
            for k2 in ("list", "data"):
                v2 = v.get(k2)
                if isinstance(v2, list):
                    return [x for x in v2 if isinstance(x, dict)]
    return []


def http_get_retry(
    url: str, timeout: int = TIMEOUT_LIST, attempts: int = 2
) -> bytes | None:
    """采集站抖动很频繁，关键请求重试一次能显著降低误杀。"""
    for i in range(attempts):
        raw = http_get(url, timeout=timeout)
        if raw:
            return raw
        if i + 1 < attempts:
            time.sleep(0.6 + random.random() * 0.6)
    return None


def verify_playable(urls: list[str], attempts: int = 3) -> bool:
    """
    真正拉一次播放地址，确认它返回的是 m3u8 内容。

    为什么必须做这件事：CMS 站点返回的 vod_play_url 经常是
      * 已经下线的死链
      * 返回 HTML 错误页 / 广告页
      * 需要 Referer 才给内容
    只看"有没有地址"会严重高估可用性，用户点进去就是黑屏。
    """
    ua_variants = [None, "okhttp/3.12.13", "Lavf/58.29.100"]
    checked = 0
    for u in urls:
        if checked >= attempts:
            break
        # 只测看起来像视频清单的地址，图片/字幕跳过
        if not re.search(r"\.(m3u8|mp4|m3u)(\?|$)", u, re.I):
            continue
        checked += 1
        for ua in ua_variants:
            raw = http_get(u, timeout=TIMEOUT_PLAY, ua=ua)
            if not raw:
                continue
            # 合法的 HLS 清单一定以 #EXTM3U 开头
            if raw.lstrip()[:7] == b"#EXTM3U":
                return True
            head = raw[:200].decode("utf-8", "replace")
            # 有些是直链 MP4：能拿到二进制且不是 HTML 也算可播
            if ".mp4" in u.lower() and "<html" not in head.lower():
                return True
    return False


def probe_site(api: str, name: str, origin: str) -> Probe:
    p = Probe(api, name, origin)
    t0 = time.time()

    # ---- 第一步：分类列表 ----
    raw = http_get_retry(f"{api}?ac=list", timeout=TIMEOUT_LIST)
    if raw is None:
        p.reason = "列表请求失败"
        return p
    p.latency_ms = int((time.time() - t0) * 1000)

    obj = json_maybe(raw)
    if obj is None:
        head = raw[:60].decode("utf-8", "replace").replace("\n", " ")
        p.reason = f"返回非 JSON（{head[:40]}）"
        return p
    if not isinstance(obj, dict):
        p.reason = "JSON 结构异常"
        return p

    # 有的站 ac=list 只给分类，有的直接给影片；两种都算活
    classes = obj.get("class")
    lst = _vod_list(obj)
    if not classes and not lst:
        p.reason = f"响应无 class/list（msg={str(obj.get('msg'))[:30]}）"
        return p

    if isinstance(classes, list):
        for c in classes:
            if isinstance(c, dict):
                tn = str(c.get("type_name") or "").strip()
                if tn:
                    p.categories.append(tn)

    # ---- 第二步：取一页真实影片，确认片库非空 ----
    raw2 = http_get_retry(f"{api}?ac=videolist&pg=1", timeout=TIMEOUT_LIST)
    obj2 = json_maybe(raw2)
    lst2 = _vod_list(obj2)
    if not lst2:
        raw3 = http_get_retry(f"{api}?ac=detail&pg=1", timeout=TIMEOUT_LIST)
        lst2 = _vod_list(json_maybe(raw3))
    if not lst2:
        p.reason = "列表有分类但片库为空"
        return p

    first = lst2[0]
    p.sample = str(first.get("vod_name") or first.get("name") or "")[:40]
    total = 0
    for k in ("total", "pagecount"):
        try:
            total = max(total, int(str((obj2 or {}).get(k, "0")).strip() or 0))
        except Exception:
            pass

    # ---- 第三步：搜索 ----
    keyword = p.sample or "电影"
    keyword = re.sub(r"[^\w\u4e00-\u9fa5]", "", keyword)[:8] or "电影"
    q = urllib.parse.urlencode({"wd": keyword})
    raw4 = http_get_retry(f"{api}?ac=detail&{q}", timeout=TIMEOUT_SEARCH)
    if raw4 is None:
        raw4 = http_get(f"{api}?wd={urllib.parse.quote(keyword)}", timeout=TIMEOUT_SEARCH)
    lst4 = _vod_list(json_maybe(raw4))
    p.has_search = bool(lst4)

    # ---- 第四步：详情/播放链，并实测能否真的拉到视频 ----
    vid = first.get("vod_id") or first.get("id")
    if vid:
        raw5 = http_get_retry(f"{api}?ac=detail&ids={vid}", timeout=TIMEOUT_DETAIL)
        detail = _vod_list(json_maybe(raw5))
        if detail:
            p.detail_ok = True
            v = detail[0].get("vod_play_url")
            if isinstance(v, str) and v.strip():
                urls = re.findall(r"(https?://[^\s#$]+)", v)
                p.episodes = len(urls)

                # 关键一步：真正拉一次 m3u8。
                # 以前只统计"有没有地址"，但很多站给的地址是死链、
                # 或者返回 HTML 错误页——这才是用户看到"不能用"的根源。
                if urls and PLAY_TEST:
                    p.play_ok = verify_playable(urls)

    # ---- 打分 ----
    score = 0.0
    score += 40 if lst2 else 0                      # 有片库
    score += min(len(p.categories), 20) * 0.5       # 分类丰富度
    score += 15 if p.has_search else 0              # 能搜
    score += 15 if p.detail_ok else 0               # 详情可用
    score += 10 if p.episodes > 0 else 0            # 有播放链
    score += 25 if p.play_ok else 0                 # 实测能播（权重最高）
    score += max(0, 10 - p.latency_ms / 1000)       # 响应快
    if total > 1000:
        score += 5
    p.score = round(score, 1)

    # 片库指纹：用于识别"同一片库换了域名"的镜像站
    p.fingerprint = "|".join(sorted(p.categories))[:200]

    # ---- 通过标准 ----
    # 只有"能搜到"是不够的：用户点进去放不出来等于不能用。
    # 所以只要开启了播放实测，就必须真的拉到过 m3u8 才算通过。
    # 注意：如果连列表都拿不到（网络层就失败），那是"未验证"而非"已死"，
    # 交给上层决定是否保留上次的结论，不能直接当死链丢掉。
    if PLAY_TEST:
        p.ok = bool(lst2) and p.play_ok
        if not p.ok:
            if not lst2:
                p.reason = "片库为空"
            elif not p.detail_ok:
                p.reason = "拿不到详情"
            elif p.episodes == 0:
                p.reason = "没有播放地址"
            else:
                p.reason = "播放地址实测拉不到 m3u8（死链）"
    else:
        p.ok = bool(lst2) and (p.has_search or p.episodes > 0)
        if not p.ok:
            if not p.has_search and p.episodes == 0:
                p.reason = "仅能返回列表，搜不到也拿不到播放链"
            else:
                p.reason = "片库可用但搜索/详情均失败"
    return p


def run_probes(cands: list[dict[str, Any]], workers: int = 24) -> list[Probe]:
    log(f"开始体检 {len(cands)} 个站点（{workers} 并发）…")
    results: list[Probe] = []

    def work(c: dict[str, Any]) -> Probe:
        try:
            return probe_site(c["api"], c.get("name", ""), c.get("origin", ""))
        except Exception as e:  # 单个站点崩溃不能带崩整轮
            p = Probe(c["api"], c.get("name", ""), c.get("origin", ""))
            p.reason = f"体检异常: {e}"
            return p

    done = 0
    with futures.ThreadPoolExecutor(max_workers=workers) as ex:
        for p in ex.map(work, cands):
            results.append(p)
            done += 1
            if done % 20 == 0:
                alive = sum(1 for r in results if r.ok)
                log(f"  … 已完成 {done}/{len(cands)}，存活 {alive}")

    alive = [r for r in results if r.ok]
    log(f"体检完成：存活 {len(alive)} / {len(results)}")
    return results


# ---------------------------------------------------------------- 合成

def build_config(
    alive: list[Probe],
    seeds_apis: set[str],
    limit: int | None,
    now: datetime,
    args_max_per_host: int = 2,
) -> dict[str, Any]:
    # 按分数排序，同分时种子优先（你自己挑的更可信）
    alive.sort(
        key=lambda p: (-p.score, 0 if p.api in seeds_apis else 1, p.latency_ms)
    )
    if limit:
        alive = alive[:limit]

    sites: list[dict[str, Any]] = []
    used_keys: set[str] = set()
    used_names: dict[str, int] = {}
    per_host: dict[str, int] = {}
    used_fingerprints: set[str] = set()

    def clean_name(raw: str, host: str) -> str:
        """把各路人马五花八门的站名洗成干净、统一的样子。"""
        # 1) 按竖线切段，丢掉纯分类词段，取剩下的第一个
        segs = [s.strip() for s in re.split(r"[|｜┃丨]", raw or "") if s.strip()]
        if segs:
            keep = [s for s in segs if s not in CATEGORY_WORDS]
            nm = keep[0] if keep else segs[0]
        else:
            nm = host

        # 2) 去掉 "影视 • " / "点播·" 之类前缀
        nm = re.sub(r"^(影视|点播)\s*[•·・]\s*", "", nm)
        # 3) 去掉 [直连] 【xx】 (xx) 之类注记
        nm = re.sub(r"[\[【(（][^\]】)）]*[\]】)）]", "", nm)
        # 4) 去掉 emoji / 装饰符号
        nm = re.sub(
            r"[\U0001F000-\U0001FAFF\u2190-\u21FF\u2300-\u27BF\u2B00-\u2BFF"
            r"\uFE0F\u2022\u00b7\u25A0-\u25FF]",
            "", nm,
        )
        # 5) 去掉纯分类词 + 代码化前缀（csp_xml_ 之类）
        for w in CATEGORY_WORDS:
            nm = nm.replace(w, " ")
        nm = re.sub(r"^(csp[_-]?(xml|json)?[_-]?|xml[_-]|json[_-]|jx[_-])", "", nm, flags=re.I)
        nm = re.sub(r"[┃|｜丨•·・]+", " ", nm)
        nm = re.sub(r"\s+", " ", nm).strip(" -_·•")
        nm = re.sub(r"^(csp[_-]?(xml|json)?[_-]?|xml[_-]|json[_-])", "", nm, flags=re.I)
        return nm.strip() or host

    for p in alive:
        host = urllib.parse.urlsplit(p.api).hostname or "site"

        # 同一台机器最多保留 N 个片库变体。
        # 注意：这里刻意保留镜像——采集站几乎天天换域名，
        # 同一片库的多个域名本身就是最好的抗失效冗余。
        if per_host.get(host, 0) >= args_max_per_host:
            continue

        # 片库内容指纹去重：分类完全一致的视为同一片库，只留最好的
        fp = p.fingerprint or safe_key(host)
        if fp in used_fingerprints:
            continue

        per_host[host] = per_host.get(host, 0) + 1
        used_fingerprints.add(fp)

        base = safe_key(host)
        key = base
        n = 1
        while key in used_keys:
            n += 1
            key = f"{base}{n}"
        used_keys.add(key)

        nm = clean_name(p.name, host)
        used_names[nm] = used_names.get(nm, 0) + 1
        if used_names[nm] > 1:
            nm = f"{nm}{used_names[nm]}"
        display = f"{CATEGORY_PREFIX} · {nm}"

        site: dict[str, Any] = {
            "key": key,
            "name": display,
            "type": 1,
            "api": p.api,
            "searchable": 1 if p.has_search else 0,
            "quickSearch": 1 if p.has_search else 0,
            "filterable": 1 if p.categories else 0,
        }
        sites.append(site)

    ts = now.astimezone(CST).strftime("%Y-%m-%d %H:%M")

    config: dict[str, Any] = {
        # 刻意不写 spider 字段：纯 type=1 直连 CMS，
        # 省掉 spider.jar 这个最容易失效、也最容易导致白屏的环节。
        "sites": sites,
        # 刻意留空：本配置全部是直连 CMS，播放不需要解析。
        # 之前放的两个解析地址一个已失效、一个是占位符 "Demo"，
        # 反而会在需要解析时误导 TVBox，造成"能搜到但点开黑屏"。
        "parses": [],
        "flags": [
            "电影", "电视剧", "综艺", "动漫", "纪录片", "短剧",
            "国产剧", "香港剧", "台湾剧", "日韩剧", "欧美剧", "海外剧",
        ],
        "ijk": {
            "mediaCodec": 1,
            "mediaCodecAutoRotate": 1,
            "mediaCodecHandleResolutionChange": 1,
            "enableMediaCodecBuffer": 1,
            "opensles": 0,
            "soundTouch": 1,
        },
        "ads": [
            "mimg.0c1q0l.cn",
            "www.googletagmanager.com",
            "www.google-analytics.com",
            "cnzz.com",
            "doubleclick.net",
        ],
        "rules": [],
        "headers": [],
        # ---- 以下为个人标记，TVBox 会忽略未知字段，保留便于自查 ----
        "_meta": {
            "generator": "tvbox-selfbuild",
            "built_at": now.astimezone(CST).isoformat(timespec="seconds"),
            "built_at_week": now.astimezone(CST).strftime("%G-W%V"),
            "site_count": len(sites),
            "note": f"免spider点播单源 · 自动构建于 {ts} (北京时间)",
        },
    }
    return config


def build_status(probes: list[Probe], alive: list[Probe], now: datetime) -> dict[str, Any]:
    def row(p: Probe) -> dict[str, Any]:
        return {
            "api": p.api,
            "name": p.name,
            "ok": p.ok,
            "score": p.score,
            "latency_ms": p.latency_ms,
            "categories": len(p.categories),
            "search": p.has_search,
            "detail": p.detail_ok,
            "reason": p.reason,
        }

    dead = [p for p in probes if not p.ok]
    return {
        "generated_at": now.astimezone(CST).isoformat(timespec="seconds"),
        "total_probed": len(probes),
        "alive": len(alive),
        "dead": len(dead),
        "alive_list": sorted((row(p) for p in alive), key=lambda r: -r["score"]),
        "dead_list": sorted((row(p) for p in dead), key=lambda r: r["api"])[:200],
    }


def write_json(path: str, obj: Any, pretty: bool = False) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    text = json.dumps(
        obj,
        ensure_ascii=False,
        indent=1 if pretty else None,
        separators=None if pretty else (",", ":"),
    )
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)
    os.replace(tmp, path)


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------- 完整版（带 jar）

# 每个提供方 -> (配置地址, jar 覆盖地址或空串)
# 铁律：type=3 站点的 api 是 jar 内的类名（csp_Xxx），
#       必须用该提供方自己的 jar，混用会让站点引用不存在的类而全部失效。
# jar 覆盖地址留空表示"用配置自带的 spider 字段"。
FULL_GROUPS: dict[str, tuple[str, str]] = {
    # 肥猫：官方域名已停放/引流，改用社区镜像（jar md5 与配置内一致）
    "肥猫": (
        "https://raw.githubusercontent.com/Lightconer/tvbox-ysc-config/main/output/feimao.json",
        "",
    ),
    # 老刘备：配置里写的是相对路径 ./fty.jar，必须解析成绝对地址，
    # 否则 TVBox 会把它解析到本仓库域名下，必然 404。
    "老刘备": (
        "https://raw.liucn.cc/box/m.json",
        "",
    ),
    "盒子迷xhz": (
        "http://xhztv.top/xhz",
        "",
    ),
    "盒子迷4K": (
        "http://xhztv.top/4k.json",
        "",
    ),
}


def resolve_jar(cfg_url: str, raw: str) -> tuple[str, str]:
    """
    把配置里的 spider 值拆成 (可用的绝对地址, md5)。
    处理三种情况：
      * 绝对 URL，可能带 ;md5;xxx
      * 相对路径 ./fty.jar（相对配置文件所在目录）
      * 空值
    """
    raw = (raw or "").strip()
    if not raw:
        return "", ""
    url, md5 = raw, ""
    if ";md5;" in raw:
        url, md5 = raw.split(";md5;", 1)
        url, md5 = url.strip(), md5.strip().lower()
    # 相对路径解析为绝对地址
    if not url.startswith(("http://", "https://")):
        url = urllib.parse.urljoin(cfg_url, url)
    return url, md5


def build_full_configs(now: datetime) -> list[tuple[str, dict[str, Any] | None, list[str]]]:
    """为每个提供方单独生成一份带 jar 的完整版配置。"""
    out: list[tuple[str, dict[str, Any] | None, list[str]]] = []

    for prov, (cfg_url, jar_override) in FULL_GROUPS.items():
        notes: list[str] = []
        obj, real = fetch_config(cfg_url)
        if obj is None:
            out.append((prov, None, [f"配置拉取失败：{cfg_url}"]))
            continue

        sites = obj.get("sites") or []
        t3 = [s for s in sites if isinstance(s, dict) and str(s.get("type")) == "3"]
        if not t3:
            out.append((prov, None, [f"无 type=3 站点（sites={len(sites)}）"]))
            continue

        jar_url, jar_md5 = resolve_jar(cfg_url, jar_override or str(obj.get("spider") or ""))
        if not jar_url:
            out.append((prov, None, [f"配置未声明 jar，跳过其 {len(t3)} 个站点"]))
            continue

        used_keys: set[str] = set()
        built: list[dict[str, Any]] = []
        skipped = 0
        for s in t3:
            api = str(s.get("api") or "").strip()
            name = str(s.get("name") or "").strip()
            if not api or not name:
                skipped += 1
                continue
            key = safe_key(f"{prov}{name}") or "s"
            while key in used_keys:
                key += "x"
            used_keys.add(key)
            site: dict[str, Any] = {
                "key": key,
                "name": f"{CATEGORY_PREFIX} · {name}",
                "type": 3,
                "api": api,
                "searchable": 1,
                "quickSearch": 1,
                "filterable": 1,
            }
            if s.get("ext"):
                site["ext"] = s["ext"]
            built.append(site)

        if not built:
            out.append((prov, None, ["type=3 站点字段不全"]))
            continue

        spider_value = f"{jar_url};md5;{jar_md5}" if jar_md5 else jar_url
        cfg = {
            "spider": spider_value,
            "sites": built,
            "parses": [],
            "flags": [],
            "ijk": {"mediaCodec": 0, "opensles": 0, "soundTouch": 1},
            "ads": [],
            "rules": [],
            "headers": [],
            "_meta": {
                "generator": "tvbox-selfbuild-full",
                "source_provider": prov,
                "source_config": cfg_url,
                "jar": jar_url,
                "jar_md5": jar_md5 or "未声明",
                "built_at": now.astimezone(CST).isoformat(timespec="seconds"),
                "site_count": len(built),
                "note": f"完整版（带 jar）· 来源 {prov} · {len(built)} 个 type=3 站点",
                "warning": ("本文件挂载了 spider.jar。jar 地址一旦失效，"
                            "整份配置所有站点都会不可用（白屏）。"
                            "日常请优先使用 dist/tvbox.json（免 jar 版）。"),
            },
        }
        notes.append(f"{len(built)} 个 type=3 站点，jar={jar_url}")
        if skipped:
            notes.append(f"跳过 {skipped} 个字段不全的站点")
        out.append((prov, cfg, notes))

    return out


# ---------------------------------------------------------------- 主流程

def main() -> int:
    ap = argparse.ArgumentParser(description="构建个人 TVBox 免spider点播单源")
    ap.add_argument("--dry-run", action="store_true", help="只体检，不写文件")
    ap.add_argument("--limit", type=int, default=0, help="最多保留多少个站点")
    ap.add_argument("--workers", type=int, default=24, help="体检并发数")
    ap.add_argument("--no-github-search", action="store_true", help="跳过 GitHub 检索")
    ap.add_argument("--min-alive", type=int, default=5,
                    help="存活站点少于此数则认为构建失败，不覆盖旧文件")
    ap.add_argument("--max-per-host", type=int, default=2,
                    help="同一个域名下最多保留几个片库，防止列表同源刷屏")
    args = ap.parse_args()

    now = datetime.now(timezone.utc)

    global BLOCKLIST
    BLOCKLIST = load_blocklist()

    log("=" * 60)
    log("TVBox 个人点播单源 · 构建开始")
    log(f"黑名单关键词 {len(BLOCKLIST)} 条")
    log("=" * 60)

    # 1. 采集
    cands = collect_candidates(include_github_search=not args.no_github_search)
    if not cands:
        log("!! 一个候选都没拿到，检查网络或 config/aggregators.txt")
        return 2

    seeds_apis = {normalize_api(s["api"]) for s in load_seeds()}

    # 2. 体检
    probes = run_probes(cands, workers=args.workers)
    alive = [p for p in probes if p.ok]

    if len(alive) < args.min_alive:
        log(f"!! 存活站点只有 {len(alive)} 个（阈值 {args.min_alive}），"
            f"为免把可用配置覆盖掉，本次不写文件。")
        # 仍然把体检报告落盘，方便排查
        if not args.dry_run:
            write_json(
                os.path.join(OUTPUT_DIR, "status.json"),
                build_status(probes, alive, now),
                pretty=True,
            )
        return 3

    # 3. 合成
    cfg = build_config(alive, seeds_apis, args.limit or None, now, args.max_per_host)
    log(f"合成完成：{len(cfg['sites'])} 个站点")

    if args.dry_run:
        log("--dry-run：不写文件。前 15 个站点：")
        for s in cfg["sites"][:15]:
            log(f"    {s['key']:22s} {s['name']:26s} {s['api']}")
        return 0

    # 4. 落盘
    out_main = os.path.join(OUTPUT_DIR, "tvbox.json")
    write_json(out_main, cfg)

    # 附带一份极简版：只留站点，给加载慢的老盒子用
    slim = {
        "sites": cfg["sites"],
        "parses": cfg["parses"],
        "flags": cfg["flags"],
        "_meta": cfg["_meta"],
    }
    out_slim = os.path.join(OUTPUT_DIR, "tvbox-slim.json")
    write_json(out_slim, slim)

    status = build_status(probes, alive, now)
    write_json(os.path.join(OUTPUT_DIR, "status.json"), status, pretty=True)

    # ---- 完整版（带 jar）----
    # 每个提供方单独一个文件：jar 与站点必须配套，
    # 混在一起挂错 jar 会导致整份配置的站点全部失效。
    for prov, full_cfg, notes in build_full_configs(now):
        if not full_cfg:
            log(f"完整版[{prov}] 未生成：{'；'.join(notes)}")
            continue
        fname = f"tvbox-full-{prov}.json"
        out_full = os.path.join(OUTPUT_DIR, fname)
        write_json(out_full, full_cfg)
        log(f"已写出完整版：{fname}（{full_cfg['_meta']['site_count']} 个 jar 站点）")
        for n in notes:
            log(f"    {n}")

    log("构建完成 ✔")
    return 0


if __name__ == "__main__":
    sys.exit(main())
