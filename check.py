#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
whatslink.info ed2k 链接批量验证器
- 读取 data/ 目录下所有 txt（每行一条 ed2k 链接）
- JSONL 进度保存，支持断点续传
- 被限流的 URL 自动重试一次
- 全部完成后汇总所有成功链接到 work/all_success_ed2k.txt（A-Z 排序）

环境变量：
  TXT_FILE   : 指定 data/ 下某个文件名，或 all（默认 all）
  DELAY_SECS : 每次请求间隔秒数（默认 2）
"""

import argparse
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Tuple

import requests

# ── 目录 ──────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR   = SCRIPT_DIR / "data"
WORK_DIR   = SCRIPT_DIR / "work"

DATA_DIR.mkdir(exist_ok=True)
WORK_DIR.mkdir(exist_ok=True)

COMPLETED_FILE = WORK_DIR / ".completed"   # 已全部处理完的文件 stem

# ── API ───────────────────────────────────────────────────────────────
API          = "https://whatslink.info/api/v1/link"
DELAY        = float(os.environ.get("DELAY_SECS", "2"))
RETRY_WAIT   = 90   # 限流后等待秒数再重试


# ═══════════════════════════════════════════════════════════════════════
#  工具
# ═══════════════════════════════════════════════════════════════════════

def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# ── 已完成文件标记（同 pikpak 模式，防止重复执行）─────────────────────

def load_completed() -> Set[str]:
    if not COMPLETED_FILE.exists():
        return set()
    return {l.strip() for l in COMPLETED_FILE.read_text("utf-8").splitlines() if l.strip()}


def mark_completed(stem: str):
    done = load_completed()
    if stem not in done:
        done.add(stem)
        COMPLETED_FILE.write_text("\n".join(sorted(done)) + "\n", "utf-8")
        log(f"  [{stem}] 已写入完成标记")


# ── JSONL 进度（每条 URL 一行，重复 URL 以最后一条为准）──────────────

def load_progress(stem: str) -> Dict[str, dict]:
    """返回 {url: record}，重复 url 保留最后写入的记录"""
    p = WORK_DIR / f"{stem}.jsonl"
    records: Dict[str, dict] = {}
    if p.exists():
        for line in p.read_text("utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                records[rec["url"]] = rec   # 后写覆盖前写
            except Exception:
                pass
    return records


def append_record(stem: str, rec: dict):
    """追加一条记录到 JSONL（幂等，重试时再追加一条即可）"""
    p = WORK_DIR / f"{stem}.jsonl"
    with open(p, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


# ── API 调用 ──────────────────────────────────────────────────────────

def check_url(url: str, idx: int, total: int) -> Tuple[str, dict]:
    """
    返回 (status, data)
    status: "success" | "failed" | "quota_limited"
    """
    log(f"  [{idx:>4}/{total}] {url[:90]}")
    try:
        resp = requests.get(API, params={"url": url}, timeout=30)
        data = resp.json()
        if data.get("error") == "quota_limited":
            log("          ⏳ 被限流")
            return "quota_limited", data
        if data.get("screenshots"):
            log("          ✅ 有效")
            return "success", data
        log("          ❌ 无效")
        return "failed", data
    except Exception as e:
        log(f"          ❌ 请求异常: {e}")
        return "failed", {"error": str(e)}


# ═══════════════════════════════════════════════════════════════════════
#  单文件处理
# ═══════════════════════════════════════════════════════════════════════

def process_file(txt_path: Path) -> List[str]:
    """
    处理一个 txt 文件，返回本文件所有成功的 ed2k URL 列表。
    支持断点续传：已完成的 URL 直接跳过，quota_limited 会重试一轮。
    """
    stem = txt_path.stem
    log(f"\n{'━'*60}")
    log(f"▶  {txt_path.name}")
    log(f"{'━'*60}")

    all_urls = [u.strip() for u in txt_path.read_text("utf-8").splitlines()
                if u.strip() and not u.startswith("#")]
    done_map = load_progress(stem)

    # 分类：需要首次处理 / 需要重试限流 / 已完成跳过
    pending       = [u for u in all_urls if u not in done_map]
    quota_retry   = [u for u, r in done_map.items() if r.get("status") == "quota_limited"]

    log(f"  总计 {len(all_urls)} 条 | 已完成 {len(done_map) - len(quota_retry)} | "
        f"待处理 {len(pending)} | 限流重试 {len(quota_retry)}")

    if not pending and not quota_retry:
        log(f"  [{stem}] 全部已处理，跳过")
    else:
        # ── 第一轮：处理 pending ──────────────────────────────────────
        new_quota: List[str] = []
        for i, url in enumerate(pending, 1):
            status, data = check_url(url, len(done_map) - len(quota_retry) + i, len(all_urls))
            rec = {"url": url, "status": status, "ts": datetime.now().isoformat()}
            done_map[url] = rec
            append_record(stem, rec)
            if status == "quota_limited":
                new_quota.append(url)
            time.sleep(DELAY)

        # ── 第二轮：重试限流（旧的 + 本轮新产生的）────────────────────
        retry_list = quota_retry + new_quota
        if retry_list:
            log(f"\n  重试 {len(retry_list)} 条被限流 URL，等待 {RETRY_WAIT}s...")
            time.sleep(RETRY_WAIT)
            for i, url in enumerate(retry_list, 1):
                status, data = check_url(url, i, len(retry_list))
                rec = {"url": url, "status": status, "ts": datetime.now().isoformat()}
                done_map[url] = rec
                append_record(stem, rec)   # 最新状态追加覆盖，load 时取最后一条
                time.sleep(DELAY)

    # 统计
    success_urls = [u for u, r in done_map.items() if r.get("status") == "success"]
    failed_count  = sum(1 for r in done_map.values() if r.get("status") == "failed")
    limited_count = sum(1 for r in done_map.values() if r.get("status") == "quota_limited")
    log(f"  ✅ 有效 {len(success_urls)} | ❌ 无效 {failed_count} | ⏳ 仍限流 {limited_count}")

    # 如果没有剩余待处理项，标记完成
    if limited_count == 0:
        mark_completed(stem)

    return success_urls


# ═══════════════════════════════════════════════════════════════════════
#  主程序
# ═══════════════════════════════════════════════════════════════════════

def collect_txt_files(txt_file: str) -> List[Path]:
    if txt_file.lower() != "all":
        p = DATA_DIR / txt_file
        if not p.exists():
            raise FileNotFoundError(f"文件不存在: {p}")
        return [p]
    files = sorted(DATA_DIR.glob("*.txt"))
    if not files:
        raise FileNotFoundError("data/ 目录下没有 txt 文件")
    return files


def print_status(txt_files: List[Path]):
    completed = load_completed()
    print("=" * 64)
    print(f"  {'文件':<35} {'状态':<12} {'成功':>6} {'失败':>6} {'限流':>6}")
    print(f"  {'─'*35} {'─'*12} {'─'*6} {'─'*6} {'─'*6}")
    for f in txt_files:
        stem = f.stem
        if stem in completed:
            # 读取 JSONL 获取数量
            done_map = load_progress(stem)
            ok  = sum(1 for r in done_map.values() if r.get("status") == "success")
            bad = sum(1 for r in done_map.values() if r.get("status") == "failed")
            print(f"  {'✅ '+stem[:33]:<35} {'已完成':<12} {ok:>6} {bad:>6} {'0':>6}")
        else:
            jsonl = WORK_DIR / f"{stem}.jsonl"
            if not jsonl.exists():
                total = sum(1 for l in f.read_text("utf-8").splitlines() if l.strip())
                print(f"  {'🆕 '+stem[:33]:<35} {'未开始':<12} {'—':>6} {'—':>6} {total:>6}")
            else:
                done_map = load_progress(stem)
                ok  = sum(1 for r in done_map.values() if r.get("status") == "success")
                bad = sum(1 for r in done_map.values() if r.get("status") == "failed")
                lim = sum(1 for r in done_map.values() if r.get("status") == "quota_limited")
                total = sum(1 for l in f.read_text("utf-8").splitlines() if l.strip())
                pct   = int(len(done_map) / total * 100) if total else 0
                mark  = "✅" if lim == 0 and len(done_map) >= total else "⏳"
                print(f"  {mark+' '+stem[:33]:<35} {f'{pct}%':<12} {ok:>6} {bad:>6} {lim:>6}")
    print("=" * 64)


def build_final_output(txt_files: List[Path]):
    """汇总所有文件的成功 ed2k 链接，去重，A-Z 排序，写入统一 txt"""
    all_success: List[str] = []
    for f in txt_files:
        done_map = load_progress(f.stem)
        all_success.extend(u for u, r in done_map.items() if r.get("status") == "success")

    deduped = sorted(set(all_success))   # A-Z 排序（按字母序）
    out = WORK_DIR / "all_success_ed2k.txt"
    out.write_text("\n".join(deduped) + ("\n" if deduped else ""), "utf-8")
    log(f"\n{'='*60}")
    log(f"汇总完成：共 {len(deduped)} 条有效 ed2k 链接（已去重）")
    log(f"输出文件: {out.name}")
    return out


def main():
    parser = argparse.ArgumentParser(description="ed2k 链接批量验证器")
    parser.add_argument("--txt-file", "-t",
                        default=os.environ.get("TXT_FILE", "all"),
                        help="data/ 下的文件名，或 all（默认）")
    parser.add_argument("--status", "-s", action="store_true",
                        help="只显示进度状态，不执行")
    args = parser.parse_args()

    try:
        txt_files = collect_txt_files(args.txt_file)
    except FileNotFoundError as e:
        log(f"❌ {e}")
        return

    print_status(txt_files)

    if args.status:
        return

    completed = load_completed()
    to_run = [f for f in txt_files if f.stem not in completed]

    if not to_run:
        log("所有文件均已完成，直接生成汇总文件...")
    else:
        log(f"共 {len(txt_files)} 个文件，其中 {len(to_run)} 个待处理")
        for txt_path in to_run:
            try:
                process_file(txt_path)
            except KeyboardInterrupt:
                log("\n用户中断，进度已保存，下次运行可续接")
                build_final_output(txt_files)
                raise SystemExit(0)
            except Exception as e:
                import traceback
                log(f"❌ [{txt_path.stem}] 出错: {e}")
                traceback.print_exc()

    # 无论是否全部完成，都输出当前已知的成功链接
    build_final_output(txt_files)
    print_status(txt_files)


if __name__ == "__main__":
    main()
