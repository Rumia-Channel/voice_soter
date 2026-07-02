# -*- coding: utf-8 -*-
"""プロジェクト単位の SQLite ストア。"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any


class Store:
    def __init__(self, db_path: Path):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(db_path))
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self._init_schema()

    def _init_schema(self):
        c = self.conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS settings(
            key TEXT PRIMARY KEY, value TEXT
        );""")
        c.execute("""CREATE TABLE IF NOT EXISTS names(
            id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE
        );""")
        # 内部用（Undo/Redo の再構築に使う）
        c.execute("""CREATE TABLE IF NOT EXISTS history(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            action TEXT NOT NULL,
            payload TEXT
        );""")
        # 監査ログ（確定操作のみ）
        c.execute("""CREATE TABLE IF NOT EXISTS audit(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            op TEXT NOT NULL,   -- 'move' | 'exclude' | 'defer'
            src TEXT NOT NULL,
            dst TEXT NOT NULL,
            character TEXT,     -- move のみ（任意）
            folder TEXT         -- move のみ（任意）
        );""")
        # 入力フォルダ
        c.execute("""CREATE TABLE IF NOT EXISTS inputs(
            path TEXT PRIMARY KEY,
            enabled INTEGER NOT NULL DEFAULT 1,
            done INTEGER NOT NULL DEFAULT 0
        );""")
        self.conn.commit()

    # settings
    def get_setting(self, key: str, default: str | None = None) -> str | None:
        c = self.conn.cursor()
        c.execute("SELECT value FROM settings WHERE key=?", (key,))
        r = c.fetchone()
        return r[0] if r else default

    def set_setting(self, key: str, value: str):
        c = self.conn.cursor()
        c.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        self.conn.commit()

    # names
    def get_names(self) -> list[str]:
        c = self.conn.cursor()
        c.execute("SELECT name FROM names ORDER BY name COLLATE NOCASE")
        return [r[0] for r in c.fetchall()]

    def set_names(self, names: list[str]):
        c = self.conn.cursor()
        c.execute("DELETE FROM names")
        for n in names:
            if n:
                c.execute("INSERT OR IGNORE INTO names(name) VALUES(?)", (n,))
        self.conn.commit()

    # inputs
    def list_inputs(self) -> list[tuple[str, int, int]]:
        c = self.conn.cursor()
        c.execute("SELECT path, enabled, done FROM inputs ORDER BY path")
        return list(c.fetchall())

    def upsert_input(self, path: Path, enabled: bool = True, done: bool = False):
        c = self.conn.cursor()
        c.execute(
            "INSERT INTO inputs(path,enabled,done) VALUES(?,?,?) "
            "ON CONFLICT(path) DO UPDATE SET enabled=excluded.enabled, done=excluded.done",
            (str(path), 1 if enabled else 0, 1 if done else 0),
        )
        self.conn.commit()

    def set_enabled(self, path: Path, enabled: bool):
        c = self.conn.cursor()
        c.execute("UPDATE inputs SET enabled=? WHERE path=?",
                  (1 if enabled else 0, str(path)))
        self.conn.commit()

    def set_done(self, path: Path, done: bool):
        c = self.conn.cursor()
        c.execute("UPDATE inputs SET done=? WHERE path=?",
                  (1 if done else 0, str(path)))
        self.conn.commit()

    def remove_input(self, path: Path):
        c = self.conn.cursor()
        c.execute("DELETE FROM inputs WHERE path=?", (str(path),))
        self.conn.commit()

    # internal history
    def log(self, action: str, payload: dict[str, Any]):
        ts = datetime.now().isoformat(timespec="seconds")
        self.conn.execute(
            "INSERT INTO history(ts,action,payload) VALUES(?,?,?)",
            (ts, action, json.dumps(payload, ensure_ascii=False))
        )
        self.conn.commit()

    def fetch_history(self) -> list[dict[str, Any]]:
        c = self.conn.cursor()
        c.execute("SELECT id, ts, action, payload FROM history ORDER BY id ASC")
        rows = []
        for _id, ts, action, payload in c.fetchall():
            try:
                data = json.loads(payload) if payload else {}
            except Exception:
                data = {}
            rows.append(
                {"id": _id, "ts": ts, "action": action, "payload": data})
        return rows

    # audit log (confirmed ops only)
    def audit(self, op: str, *, src: str, dst: str, character: str | None = None, folder: str | None = None):
        ts = datetime.now().isoformat(timespec="seconds")
        self.conn.execute(
            "INSERT INTO audit(ts,op,src,dst,character,folder) VALUES(?,?,?,?,?,?)",
            (ts, op, src, dst, character, folder)
        )
        self.conn.commit()
