# 个人 TVBox 点播单源（自愈版）

> 每周自动重建的 TVBox 配置，只收录**免 spider 的直连 CMS 站点**，
> 每个站点都实测过能搜到片、能拿到 m3u8 播放链才会被收录。

---

## 为什么你的"单源"总是失效

你把 `肥猫`、`饭太硬`、`王二小`、`潇洒` 这类地址填进 TVBox，它们本质上是
**别人维护的一份 JSON**。失效原因有三层，全都不是你的问题：

| 失效原因 | 说明 |
|---|---|
| **域名被封** | 这类配置多挂在 `kstore.vip` / `.top` / 短域名上，被投诉就换域名，旧地址直接 404 |
| **挂载的 spider.jar 没了** | 配置里 `type: 3` 的站点全靠一个 jar 干活，jar 地址一挂，整份配置白屏 |
| **里面的 CMS 站烂掉** | 采集站生命周期很短，平均几周就换库或关站 |

**所以"找一个稳定的单源"这条路本身是走不通的。**
正确做法是：**不要依赖任何单个源，而是让你的源每次自动重建。**

这个仓库就是这么干的：

```
每周一 06:00 (北京时间)
        ↓
GitHub Actions 自动触发
        ↓
① 采集  从 28 个公共聚合配置 + CMS 接口清单里捞出 100+ 个候选
② 过滤  黑名单挡掉成人/赌博站，按域名和片库指纹去重
③ 体检  每个候选实测 分类列表 → 片库 → 搜索 → 详情 → m3u8 播放链
④ 合成  只留下真正能播的，输出一份全新的 tvbox.json
        ↓
提交回仓库（TVBox 里的地址始终不变，内容每周自己换血）
```

**关键点：你填的地址永远不用改，但里面的站点每周都是重新挑过的。**

---

## 使用方法

### 1. 填订阅地址

部署到你自己的 GitHub 后，在 TVBox 里填（二选一）：

```
https://raw.githubusercontent.com/<你的用户名>/<仓库名>/main/dist/tvbox.json
https://cdn.jsdelivr.net/gh/<你的用户名>/<仓库名>@main/dist/tvbox.json
```

> 国内网络下 raw 地址时通时不通，**建议先试 jsDelivr 那个**。
> 两个地址内容完全一样。

### 2. 兼容性

输出刻意取了字段交集，以下客户端都能正常加载：

- 影视仓 / FongMi 影迷
- TVBox Takagen99 及各类二次开发版
- 影视仓 TV 版

### 3. 少了哪些功能（有意为之）

| 没有的东西 | 原因 |
|---|---|
| `spider` 字段 | jar 是最主要的失效点，直接不挂。代价：没有需要 jar 的站点 |
| 直播 `lives` | 只做点播，直播源失效更快，且和你需求无关 |
| 弹幕、壁纸 | 可选项，加了只会增加加载失败概率 |

---

## 两种版本，按需选择

| 文件 | 是否挂 jar | 站点数 | 适用场景 |
|---|---|---|---|
| `dist/tvbox.json` | ❌ 无 | 约 19-40 | **日常首选**，全 type=1 直连，jar 挂了也不受影响 |
| `dist/tvbox-slim.json` | ❌ 无 | 同上 | 极简版，给加载慢的老盒子 |
| `dist/tvbox-full-盒子迷xhz.json` | ✅ 有 | 54 | 需要 jar 站的完整体验，**jar 一挂整份白屏** |
| `dist/tvbox-full-盒子迷4K.json` | ✅ 有 | 44 | 同上 |

### 为什么完整版要分成两个文件

`type=3` 站点的 `api` 是 jar 里的**类名**（如 `csp_XBPQ`）。不同提供方的 jar 内含的类完全不同：

| jar | spider 类数量 |
|---|---|
| `qist/spider.jar` | 123 |
| `Yoursmile.jar` | 63 |

**把 A 家的站点挂在 B 家的 jar 上，站点引用的类不存在，点开必然失败。**
所以每个提供方单独出一个文件，jar 与站点严格配套。

## 目录结构

```
.
├── build.py                    # 构建脚本（只用 Python 标准库，零依赖）
├── config/
│   ├── seeds.txt               # 你手动维护的"底仓"接口
│   ├── aggregators.txt         # 候选池来源（公共聚合配置）
│   └── blocklist.txt           # 黑名单（成人/赌博站）
├── dist/                       # 构建产物（自动生成，别手改）
│   ├── tvbox.json              # 主配置，给 TVBox 用
│   ├── tvbox-slim.json         # 极简版，给加载慢的老盒子
│   └── status.json             # 本次体检报告
└── .github/workflows/build.yml # 每周定时任务
```

---

## 日常维护

### 加一个你发现的能用的接口

编辑 `config/seeds.txt`，加一行：

```
我的源, https://xxx.com/api.php/provide/vod
```

提交后 Actions 会立刻重建。**接口挂了也不用删**，体检会自动剔除它。

### 屏蔽某个站点

编辑 `config/blocklist.txt`，加一行关键词（域名片段或站名片段）。命中的会被直接丢弃。

### 想让站更多 / 更少

```bash
python build.py --limit 30          # 最多保留 30 个（按体检得分排序）
python build.py --max-per-host 3    # 同一域名最多 3 个镜像
```

### 本地手动重建

```bash
python build.py                 # 正式构建，写入 dist/
python build.py --dry-run       # 只体检不写文件，先看看结果
python build.py --no-github-search   # 跳过 GitHub 检索，跑得更快
```

脚本只用标准库，**不需要 pip install 任何东西**，Python 3.10+ 即可。

---

## 怎么判断这次更新靠不靠谱

打开 `dist/status.json`，或者仓库的提交记录。提交信息里会写存活数量：

```
自动更新: 2026-11-09 存活 17 个站点
```

看 Actions 日志里这一步的输出：

```
体检 96 个，存活 26 个，失效 70 个
  OK   104.8  建安      http://154.219.117.232:9981/jacloudapi.php/provide/vod
  OK   104.6  电影天堂   http://caiji.dyttzyapi.com/api.php/provide/vod
```

**存活数长期低于 5 时，脚本会拒绝写文件**（退出码 3），
宁可让 CI 失败，也不会把仓库里能用的旧配置覆盖成空壳。

---

## 免责声明

本项目只是一个**配置生成工具**，仓库内不存储、不分发任何音视频内容。
`config/seeds.txt` 与 `aggregators.txt` 里的地址均为互联网公开信息，
站点内容与本项目无关。请在所在地法律允许的范围内使用。
