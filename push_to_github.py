# -*- coding: utf-8 -*-
"""
把当前目录一次性推送到你自己的 GitHub 仓库。

用法（先装好 git，并且本机已能登录 GitHub）：

    python push_to_github.py

脚本会问你仓库地址，然后把除临时文件外的所有内容提交并推上去。
推完之后，去仓库的 Actions 页面确认 "每周更新点播单源" 这个工作流
在列表里，点一次 Run workflow 手动跑一遍，成功后 dist/ 里就有东西了。
"""

import os
import subprocess
import sys

SKIP = {".git", "__pycache__", "candidates", "run_full.log", ".venv"}
SKIP_FILE_SUFFIX = (".pyc", ".log", ".tmp")
SKIP_FILES = {"tmp_e2e.py", "tmp_check.py", "tmp_diag.py", "tmp_explore.py",
              "tmp_trees.py", "tmp_cj.py"}


def run(cmd, check=True):
    print(f"  $ {' '.join(cmd)}")
    r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if r.stdout.strip():
        print("    " + r.stdout.strip().replace("\n", "\n    "))
    if r.returncode != 0:
        if r.stderr.strip():
            print("    !! " + r.stderr.strip().replace("\n", "\n    "))
        if check:
            raise SystemExit(f"命令失败: {' '.join(cmd)}")
    return r


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    os.chdir(here)

    print("=" * 60)
    print(" 推送到 GitHub")
    print("=" * 60)

    if not os.path.isdir(".git"):
        print("\n[1/5] 初始化 git 仓库")
        run(["git", "init"])
        run(["git", "branch", "-M", "main"])
    else:
        print("\n[1/5] git 仓库已存在，跳过初始化")

    print("\n[2/5] 检查 git 身份")
    r = run(["git", "config", "user.name"], check=False)
    if not r.stdout.strip():
        name = input("  你的名字: ").strip() or "tvbox"
        email = input("  你的邮箱: ").strip() or "tvbox@users.noreply.github.com"
        run(["git", "config", "user.name", name])
        run(["git", "config", "user.email", email])
    else:
        print(f"    已配置: {r.stdout.strip()}")

    print("\n[3/5] 添加文件")
    run(["git", "add", "-A"])

    # 把不该提交的从暂存区拿掉
    for skip in SKIP | SKIP_FILES:
        run(["git", "rm", "--cached", "-r", "--ignore-unmatch", skip], check=False)

    print("\n[4/5] 提交")
    r = run(["git", "diff", "--cached", "--quiet"], check=False)
    if r.returncode == 0:
        print("    没有变化，跳过提交")
    else:
        run(["git", "commit", "-m", "个人 TVBox 点播单源：自动构建 + 每周更新"])

    print("\n[5/5] 推送")
    remotes = run(["git", "remote"], check=False).stdout.split()
    if "origin" not in remotes:
        print("  还没配置远程仓库。")
        print("  先在 GitHub 上新建一个空仓库（不要勾选 README），然后填地址：")
        url = input("  仓库地址（如 https://github.com/你/你的仓库.git）: ").strip()
        if not url:
            print("  已跳过推送。之后可以手动执行：")
            print("    git remote add origin <地址>")
            print("    git push -u origin main")
            return
        run(["git", "remote", "add", "origin", url])

    run(["git", "push", "-u", "origin", "main"], check=False)

    print("\n" + "=" * 60)
    print(" 完成。接下来：")
    print("  1. 打开你仓库的 Actions 标签页")
    print("  2. 确认「每周更新点播单源」在列表里")
    print("  3. 点 Run workflow 手动跑一次（约 2-4 分钟）")
    print("  4. 成功后把下面地址填进 TVBox：")
    print()
    print("     https://cdn.jsdelivr.net/gh/<用户名>/<仓库名>@main/dist/tvbox.json")
    print("=" * 60)


if __name__ == "__main__":
    main()
