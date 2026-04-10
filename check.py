#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
whatslink.info ed2k 链接批量验证器
- 读取 data/ 目录下所有 txt（每行一条 ed2k 链接）
- JSONL 进度保存，支持断点续传
- 被限流的 URL 多轮重试，直到全部解决或达到最大轮数
- 全部完成后汇总所有成功链接到 work/all_success_ed2k.txt（A-Z 排序）
- 每个文件另存 work/{stem}_success.txt

环境变量：
  TXT_FILE    : 指定 data/ 下某个文件名，或 all（默认 all）
  DELAY_SECS  : 正常请求间隔秒数（默认 2）
  RETRY_DELAY : 重试请求间隔秒数（默认 4，比正常慢）
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
API         = "https://whatslink.info/api/v1/link"
DELAY       = float(os.environ.get("DELAY_SECS", "2"))

# 限流后重试 3 轮，每轮等待 90s，请求间隔 2s（同正常）
RETRY_WAITS = [90, 90, 90]


# ═══════════════════════════════════════════════════════════════════════
#  工具
# ═══════════════════════════════════════════════════════════════════════

def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# ── 已完成文件标记 ─────────────────────────────────────────────────────

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
    """返回 {url: record}，重复 url 保留最后写入的记录（支持多轮重试覆盖）"""
    p = WORK_DIR / f"{stem}.jsonl"
    records: Dict[str, dict] = {}
    if p.exists():
        for line in p.read_text("utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                records[rec["url"]] = rec
            except Exception:
                pass
    return records


def append_record(stem: str, rec: dict):
    """追加一条记录到 JSONL（重试时再追加，load 时取最后一条）"""
    p = WORK_DIR / f"{stem}.jsonl"
    with open(p, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


# ── API 调用 ──────────────────────────────────────────────────────────

def check_url(url: str, label: str) -> Tuple[str, dict]:
    """
    返回 (status, data)
    status: "success" | "failed" | "quota_limited"
    label: 显示用的进度文字，如 "[  3/75]"
    """
    log(f"  {label} {url[:90]}")
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


# ─── 保存单文件成功 txt ────────────────────────────────────────────────

def save_file_success_txt(stem: str, done_map: Dict[str, dict]):
    success = sorted(u for u, r in done_map.items() if r.get("status") == "success")
    out = WORK_DIR / f"{stem}_success.txt"
    out.write_text("\n".join(success) + ("\n" if success else ""), "utf-8")
    return success


# ═══════════════════════════════════════════════════════════════════════
#  单文件处理
# ═══════════════════════════════════════════════════════════════════════

def process_file(txt_path: Path) -> List[str]:
    """
    处理一个 txt 文件，返回本文件所有成功的 ed2k URL 列表。
    - 已完成的 URL 直接跳过（断点续传）
    - 被限流的 URL 多轮重试，直到全部解决或达到最大轮数
    """
    stem = txt_path.stem
    log(f"\n{'━'*60}")
    log(f"▶  {txt_path.name}")
    log(f"{'━'*60}")

    all_urls = [u.strip() for u in txt_path.read_text("utf-8").splitlines()
                if u.strip() and not u.startswith("#")]
    done_map = load_progress(stem)

    pending     = [u for u in all_urls if u not in done_map]
    quota_retry = [u for u, r in done_map.items() if r.get("status") == "quota_limited"]

    log(f"  总计 {len(all_urls)} 条 | 已完成(非限流) {len(done_map) - len(quota_retry)} | "
        f"待处理 {len(pending)} | 限流待重试 {len(quota_retry)}")

    # ── 第一轮：处理真正未处理过的 pending ─────────────────────────────
    if pending:
        log(f"\n  ── 首次处理 {len(pending)} 条 ──")
        new_quota: List[str] = []
        for i, url in enumerate(pending, 1):
            label = f"[{i:>4}/{len(pending)}]"
            status, data = check_url(url, label)
            rec = {"url": url, "status": status, "data": data, "ts": datetime.now().isoformat()}
            done_map[url] = rec
            append_record(stem, rec)          # JSONL 逐条追加，实时保存
            if status == "quota_limited":
                new_quota.append(url)
            time.sleep(DELAY)
        quota_retry = quota_retry + new_quota
        save_file_success_txt(stem, done_map) # 首轮结束后立即保存 TXT

    # ── 3 轮重试：每轮等 90s，间隔 2s ────────────────────────────────────
    if quota_retry:
        log(f"\n  共有 {len(quota_retry)} 条限流，开始重试（共 {len(RETRY_WAITS)} 轮）")

    for rnd, wait in enumerate(RETRY_WAITS, 1):
        if not quota_retry:
            break
        log(f"\n  ── 第 {rnd}/{len(RETRY_WAITS)} 轮重试 {len(quota_retry)} 条，"
            f"等待 {wait}s... ──")
        time.sleep(wait)

        still_limited: List[str] = []
        for i, url in enumerate(quota_retry, 1):
            label = f"[重试{rnd} {i:>3}/{len(quota_retry)}]"
            status, data = check_url(url, label)
            rec = {"url": url, "status": status, "data": data, "ts": datetime.now().isoformat()}
            done_map[url] = rec
            append_record(stem, rec)          # JSONL 追加（load 时取最后一条）
            if status == "quota_limited":
                still_limited.append(url)
            time.sleep(DELAY)

        resolved = len(quota_retry) - len(still_limited)
        log(f"  第 {rnd} 轮完成：解决 {resolved} 条，仍限流 {len(still_limited)} 条")
        quota_retry = still_limited
        save_file_success_txt(stem, done_map) # 每轮结束立即更新 TXT

    if quota_retry:
        log(f"  ⚠️  3 轮后仍有 {len(quota_retry)} 条限流，留待下次运行续接")

    # 统计
    ok    = sum(1 for r in done_map.values() if r.get("status") == "success")
    bad   = sum(1 for r in done_map.values() if r.get("status") == "failed")
    lim   = sum(1 for r in done_map.values() if r.get("status") == "quota_limited")
    log(f"\n  [{stem}] 结果：✅ 有效 {ok} | ❌ 无效 {bad} | ⏳ 仍限流 {lim}")

    # 保存本文件的成功 txt
    success_urls = save_file_success_txt(stem, done_map)
    log(f"  [{stem}] 已保存 → work/{stem}_success.txt（{len(success_urls)} 条）")

    # 无限流剩余则标记完成
    if lim == 0:
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
    print("\n" + "=" * 68)
    print(f"  {'文件':<36} {'状态':<10} {'✅成功':>6} {'❌无效':>6} {'⏳限流':>6}")
    print(f"  {'─'*36} {'─'*10} {'─'*6} {'─'*6} {'─'*6}")
    for f in txt_files:
        stem    = f.stem
        total   = sum(1 for l in f.read_text("utf-8").splitlines() if l.strip())
        jsonl   = WORK_DIR / f"{stem}.jsonl"
        if not jsonl.exists():
            mark = "🆕"
            stat = "未开始"
            print(f"  {mark+' '+stem[:34]:<36} {stat:<10} {'—':>6} {'—':>6} {total:>6}")
            continue
        dm  = load_progress(stem)
        ok  = sum(1 for r in dm.values() if r.get("status") == "success")
        bad = sum(1 for r in dm.values() if r.get("status") == "failed")
        lim = sum(1 for r in dm.values() if r.get("status") == "quota_limited")
        pct = int(len(dm) / total * 100) if total else 0
        if stem in completed:
            mark, stat = "✅", "已完成"
        elif lim == 0 and len(dm) >= total:
            mark, stat = "✅", "已完成"
        else:
            mark, stat = "⏳", f"{pct}%进行中"
        print(f"  {mark+' '+stem[:34]:<36} {stat:<10} {ok:>6} {bad:>6} {lim:>6}")
    print("=" * 68)


def build_final_output(txt_files: List[Path]):
    """汇总所有文件的成功 ed2k 链接，去重，A-Z 排序"""
    all_success: List[str] = []
    for f in txt_files:
        dm = load_progress(f.stem)
        all_success.extend(u for u, r in dm.items() if r.get("status") == "success")

    deduped = sorted(set(all_success))
    out = WORK_DIR / "all_success_ed2k.txt"
    out.write_text("\n".join(deduped) + ("\n" if deduped else ""), "utf-8")
    log(f"\n{'='*60}")
    log(f"汇总完成：{len(deduped)} 条有效 ed2k（去重后，A-Z 排序）")
    log(f"输出文件: work/all_success_ed2k.txt")
    return out


def main():
    parser = argparse.ArgumentParser(description="ed2k 链接批量验证器")
    parser.add_argument("--txt-file", "-t",
                        default=os.environ.get("TXT_FILE", "all"),
                        help="data/ 下的文件名，或 all（默认）")
    parser.add_argument("--status", "-s", action="store_true",
                        help="只显示进度，不执行")
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
        log(f"共 {len(txt_files)} 个文件，其中 {len(to_run)} 个需要处理")
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

    build_final_output(txt_files)
    print_status(txt_files)


if __name__ == "__main__":
    main()
