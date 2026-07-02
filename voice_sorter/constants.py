# -*- coding: utf-8 -*-
"""アプリケーション全体で使う定数。"""

from __future__ import annotations

# ---------- defaults ----------
DEFAULT_AUDIO_EXTS = [".wav", ".mp3", ".flac", ".ogg", ".m4a", ".aac"]
DEFAULT_EXTRA_MOVE_EXTS = [".lab"]

# ---------- app identity ----------
ORG_NAME = "VoiceSorter"
APP_NAME = "VoiceSorterGUI"
APP_DIR_NAME = ".voice_sorter"
PROJECTS_DIR = "projects"
DB_NAME = "voice_sorter.sqlite3"

# ---------- special dir names ----------
EXCLUDE_DIR_NAME = "_excluded_by_voice_sorter"
DEFER_DIR_NAME = "_deferred_by_voice_sorter"

# ---------- settings keys ----------
SETTINGS_KEY_AUDIO_EXTS = "audio_exts"
SETTINGS_KEY_EXTRA_EXTS = "extra_move_exts"
