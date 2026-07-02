# -*- coding: utf-8 -*-
"""汎用ユーティリティ。"""

from __future__ import annotations

import re
from pathlib import Path

from voice_sorter.constants import APP_DIR_NAME, PROJECTS_DIR


def app_data_dir() -> Path:
    base = Path.home() / APP_DIR_NAME
    (base / PROJECTS_DIR).mkdir(parents=True, exist_ok=True)
    return base


def safe_key(name: str) -> str:
    s = re.sub(r"\s+", "_", (name or "").strip())
    s = s.strip("._") or "Unnamed"
    return re.sub(r"[\\/:*?\"<>|]", "_", s)


def normalize_exts(raw: list[str]) -> list[str]:
    out = []
    for r in raw:
        r = r.strip()
        if not r:
            continue
        if not r.startswith("."):
            r = "." + r
        out.append(r.lower())
    # 重複除去
    seen = set()
    uniq = []
    for e in out:
        if e not in seen:
            seen.add(e)
            uniq.append(e)
    return uniq
