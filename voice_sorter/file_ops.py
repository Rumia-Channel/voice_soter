# -*- coding: utf-8 -*-
"""ファイル移動に関する純粋な操作。GUI 非依存。"""

from __future__ import annotations

import shutil
import time
from pathlib import Path
from typing import Any, Callable


LogError = Callable[[str, dict[str, Any]], None] | None


def try_move_with_retry(
    src: Path,
    dest: Path,
    tries: int = 10,
    wait_sec: float = 0.05,
) -> tuple[bool, Exception | None]:
    last_err: Exception | None = None
    for _ in range(tries):
        try:
            shutil.move(str(src), str(dest))
            return True, None
        except Exception as e:
            last_err = e
            time.sleep(wait_sec)
    return False, last_err


def finalize_dest(dest: Path) -> Path:
    if not dest.exists():
        return dest
    stem, suf = dest.stem, dest.suffix
    i = 1
    while True:
        cand = dest.parent / f"{stem} ({i}){suf}"
        if not cand.exists():
            return cand
        i += 1


def collect_extra_siblings(src: Path, extra_move_exts: list[str]) -> list[Path]:
    res = []
    parent = src.parent
    stem = src.stem
    for ext in extra_move_exts:
        cand = parent / f"{stem}{ext}"
        if cand.exists() and cand.is_file() and cand != src:
            res.append(cand)
    return res


def move_main_and_extras(
    src: Path,
    dest_main: Path,
    extra_move_exts: list[str],
    log_error: LogError = None,
) -> tuple[bool, str | None, list[dict[str, str]]]:
    """
    メインの src を dest_main に動かし、同じフォルダにある extra も一緒に動かす。

    戻り値: (成功/失敗, エラー文字列, extras_moved)
    extras_moved は [{"from":..., "to":...}, ...]
    """
    extras = collect_extra_siblings(src, extra_move_exts)
    moved_extras: list[dict[str, str]] = []

    ok, err = try_move_with_retry(src, dest_main)
    if not ok:
        return False, f"メインファイルの移動に失敗: {err}", []

    for ex in extras:
        target = finalize_dest(dest_main.parent / ex.name)
        ok2, err2 = try_move_with_retry(ex, target)
        if ok2:
            moved_extras.append({"from": str(ex), "to": str(target)})
        elif log_error:
            log_error("extra_move_failed", {
                "src": str(ex), "dest": str(target), "error": str(err2)})

    return True, None, moved_extras
