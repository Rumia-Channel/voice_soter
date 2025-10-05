#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
音声ファイル分類補助ソフト（PySide6）
プロジェクト対応／複数入力／有効/無効／完了タグ／再帰探索／除外（Del）／後回し（Shift+Space）／履歴保存

ポイント:
- 起動時に必ず「プロジェクト選択/新規作成」ダイアログ
- 画面上部に「プロジェクトを選択…」ボタン（いつでも切替）
- 永続化は **プロジェクト単位**：
    ~/.voice_sorter/projects/<project_key>/voice_sorter.sqlite3
  * tables: settings, names, history, inputs
- 入力フォルダを複数登録・有効/無効切替・完了タグ付与（done）
  * 無効 or 完了はスキャン対象外
- 再帰探索 ON/OFF
- **Del**: 「処理中のフォルダ」直下に `_excluded_by_voice_sorter` へ移動（以後対象外）
- **Shift+Space**: 「処理中のフォルダ」直下に `_deferred_by_voice_sorter` へ移動（いったん後回し）
  * 通常の音声がすべて無くなった時点で、これらを**自動で元の親フォルダへ戻し**、再度処理を続行
- **Space** は常に再生/一時停止だけに使用（他の動作はしない）。**Ctrl+Space** は入力欄に空白を挿入
- キャラ名オートコンプリート: 一意確定で即補完＆以降の入力をロック（Backspace/Delete で解除）
- 移動・除外・後回しの前に **QMediaPlayer をアンロード（stop→setSource(空)→processEvents）**
  + Windows のロック対策で短いリトライあり

依存:
  pip install PySide6
"""

from __future__ import annotations
import sys, json, re, sqlite3, shutil, time
from pathlib import Path
from typing import List, Optional, Tuple
from datetime import datetime

from PySide6.QtCore import Qt, QUrl, QSettings, Slot, QStringListModel, QEvent
from PySide6.QtGui import QKeySequence, QAction
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QFileDialog, QVBoxLayout, QHBoxLayout,
    QPushButton, QLabel, QLineEdit, QMessageBox, QCompleter, QDialog, QTextEdit,
    QDialogButtonBox, QFrame, QListWidget, QListWidgetItem, QCheckBox
)
from PySide6.QtMultimedia import QMediaPlayer, QAudioOutput

# ---------- constants ----------
AUDIO_EXTS = {".wav", ".mp3", ".flac", ".ogg", ".m4a", ".aac"}
ORG_NAME = "VoiceSorter"
APP_NAME = "VoiceSorterGUI"
APP_DIR_NAME = ".voice_sorter"
PROJECTS_DIR = "projects"
DB_NAME = "voice_sorter.sqlite3"
EXCLUDE_DIR_NAME = "_excluded_by_voice_sorter"
DEFER_DIR_NAME = "_deferred_by_voice_sorter"

# ---------- utils ----------
def app_data_dir() -> Path:
    base = Path.home() / APP_DIR_NAME
    (base / PROJECTS_DIR).mkdir(parents=True, exist_ok=True)
    return base

def safe_key(name: str) -> str:
    s = re.sub(r"\s+", "_", (name or "").strip())
    s = s.strip("._") or "Unnamed"
    return re.sub(r"[\\/:*?\"<>|]", "_", s)

# ---------- per-project store ----------
class Store:
    def __init__(self, db_path: Path):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(db_path))
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self._init_schema()

    def _init_schema(self):
        c = self.conn.cursor()
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS settings(
              key TEXT PRIMARY KEY, value TEXT
            );
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS names(
              id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE
            );
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS history(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts TEXT NOT NULL, action TEXT NOT NULL, payload TEXT
            );
            """
        )
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS inputs(
              path TEXT PRIMARY KEY, enabled INTEGER NOT NULL DEFAULT 1, done INTEGER NOT NULL DEFAULT 0
            );
            """
        )
        self.conn.commit()

    # settings
    def get_setting(self, key: str, default: Optional[str] = None) -> Optional[str]:
        c = self.conn.cursor(); c.execute("SELECT value FROM settings WHERE key=?", (key,))
        r = c.fetchone(); return r[0] if r else default

    def set_setting(self, key: str, value: str):
        c = self.conn.cursor()
        c.execute(
            """
            INSERT INTO settings(key,value) VALUES(?,?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (key, value),
        )
        self.conn.commit(); self.log("set_setting", {"key": key, "value": value})

    # names
    def get_names(self) -> List[str]:
        c = self.conn.cursor(); c.execute("SELECT name FROM names ORDER BY name COLLATE NOCASE")
        return [r[0] for r in c.fetchall()]

    def set_names(self, names: List[str]):
        c = self.conn.cursor(); c.execute("DELETE FROM names")
        for n in names:
            if n: c.execute("INSERT OR IGNORE INTO names(name) VALUES(?)", (n,))
        self.conn.commit(); self.log("set_names", {"names": names})

    # inputs
    def list_inputs(self) -> List[Tuple[str,int,int]]:
        c = self.conn.cursor(); c.execute("SELECT path, enabled, done FROM inputs ORDER BY path")
        return list(c.fetchall())

    def upsert_input(self, path: Path, enabled: bool=True, done: bool=False):
        c = self.conn.cursor()
        c.execute(
            """
            INSERT INTO inputs(path,enabled,done) VALUES(?,?,?)
            ON CONFLICT(path) DO UPDATE SET enabled=excluded.enabled, done=excluded.done
            """,
            (str(path), 1 if enabled else 0, 1 if done else 0),
        )
        self.conn.commit(); self.log("upsert_input", {"path": str(path), "enabled": enabled, "done": done})

    def set_enabled(self, path: Path, enabled: bool):
        c = self.conn.cursor(); c.execute("UPDATE inputs SET enabled=? WHERE path=?", (1 if enabled else 0, str(path)))
        self.conn.commit(); self.log("set_enabled", {"path": str(path), "enabled": enabled})

    def set_done(self, path: Path, done: bool):
        c = self.conn.cursor(); c.execute("UPDATE inputs SET done=? WHERE path=?", (1 if done else 0, str(path)))
        self.conn.commit(); self.log("set_done", {"path": str(path), "done": done})

    def remove_input(self, path: Path):
        c = self.conn.cursor(); c.execute("DELETE FROM inputs WHERE path=?", (str(path),))
        self.conn.commit(); self.log("remove_input", {"path": str(path)})

    # history
    def log(self, action: str, payload: dict):
        ts = datetime.now().isoformat(timespec="seconds")
        self.conn.execute(
            "INSERT INTO history(ts,action,payload) VALUES(?,?,?)",
            (ts, action, json.dumps(payload, ensure_ascii=False)),
        )
        self.conn.commit()

# ---------- dialogs ----------
class NamesEditor(QDialog):
    def __init__(self, names: List[str], parent=None):
        super().__init__(parent)
        self.setWindowTitle("キャラクター名を編集")
        self.setMinimumSize(480, 360)
        self.text = QTextEdit(self)
        self.text.setPlaceholderText("1行に1つ、またはカンマ区切りで入力\n例)\nArlan\nAsta\nDan Heng")
        self.text.setText("\n".join(names))
        lay = QVBoxLayout(self)
        btns = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept); btns.rejected.connect(self.reject)
        lay.addWidget(self.text); lay.addWidget(btns)

    def get_names(self) -> List[str]:
        raw = self.text.toPlainText(); parts: List[str] = []
        for line in raw.splitlines():
            if "," in line: parts.extend(p.strip() for p in line.split(","))
            else: parts.append(line.strip())
        seen=set(); out: List[str]=[]
        for p in parts:
            if p and p not in seen: seen.add(p); out.append(p)
        return out

class ProjectDialog(QDialog):
    def __init__(self, projects_dir: Path, parent=None):
        super().__init__(parent)
        self.setWindowTitle("プロジェクトを選択/作成")
        self.setMinimumSize(460, 360)
        self.projects_dir = projects_dir
        lay = QVBoxLayout(self)
        lay.addWidget(QLabel("既存プロジェクト"))
        self.listw = QListWidget(self)
        for d in sorted([p.name for p in projects_dir.iterdir() if p.is_dir()]):
            self.listw.addItem(QListWidgetItem(d))
        lay.addWidget(self.listw)
        lay.addWidget(QLabel("新規プロジェクト名（任意）"))
        self.new_edit = QLineEdit(self); self.new_edit.setPlaceholderText("例: star_rail_labeling")
        lay.addWidget(self.new_edit)
        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept); btns.rejected.connect(self.reject)
        lay.addWidget(btns)

    def get_selection(self) -> Tuple[str, bool]:
        name = self.new_edit.text().strip()
        if name: return safe_key(name), True
        cur = self.listw.currentItem()
        if cur: return cur.text(), False
        return "", False

# ---------- main window ----------
class VoiceSorter(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("音声ファイル分類補助ツール")
        self.setMinimumSize(960, 580)
        self.base_dir = app_data_dir()
        self.projects_dir = self.base_dir / PROJECTS_DIR
        self.qsettings = QSettings(ORG_NAME, APP_NAME)

        # 起動時に必ずプロジェクト選択
        self.project_key = self.ensure_project(force_prompt=True)
        self.project_dir = self.projects_dir / self.project_key
        self.store = Store(self.project_dir / DB_NAME)
        self.store.set_setting("project_key", self.project_key)

        # state
        self.recursive = (self.store.get_setting("recursive", "false") == "true")
        out = self.store.get_setting("last_output") or ""
        self.output_dir: Optional[Path] = Path(out) if out else None
        if self.output_dir and not self.output_dir.exists(): self.output_dir = None
        self.names = self.store.get_names()
        self.files: List[Path] = []; self.index = -1
        self.name_locked: bool = False  # 一意確定後のロック
        self.prev_name_text: str = ""   # 直前の入力値（削除検知用）
        self.restored_deferred_once: bool = False  # 無限復帰ループ防止

        # ---- UI ----
        central = QWidget(); self.setCentralWidget(central); root = QVBoxLayout(central)

        # top bar
        top = QHBoxLayout(); root.addLayout(top)
        self.btn_project = QPushButton(f"プロジェクトを選択…（現在: {self.project_key}）"); top.addWidget(self.btn_project)
        self.btn_out = QPushButton("出力フォルダ…"); top.addWidget(self.btn_out)
        self.btn_names = QPushButton("キャラ名を編集…"); top.addWidget(self.btn_names)
        self.btn_project.clicked.connect(self.change_project)
        self.btn_out.clicked.connect(self.choose_output)
        self.btn_names.clicked.connect(self.edit_names)

        # inputs area
        root.addWidget(self._sep())
        row = QHBoxLayout(); root.addLayout(row)
        col_left = QVBoxLayout(); row.addLayout(col_left, 3)
        col_right = QVBoxLayout(); row.addLayout(col_right, 1)

        self.list_inputs = QListWidget(); self.list_inputs.setSelectionMode(QListWidget.ExtendedSelection)
        col_left.addWidget(QLabel("入力フォルダ（チェック=有効 / グレー=完了は対象外）"))
        col_left.addWidget(self.list_inputs)
        self.btn_add_in = QPushButton("追加…")
        self.btn_rm_in = QPushButton("選択削除")
        self.btn_done = QPushButton("選択に完了タグを付ける/外す")
        self.chk_recursive = QCheckBox("再帰的に探索"); self.chk_recursive.setChecked(self.recursive)
        for w in (self.btn_add_in, self.btn_rm_in, self.btn_done, self.chk_recursive): col_right.addWidget(w)
        col_right.addStretch(1)
        self.btn_add_in.clicked.connect(self.add_input)
        self.btn_rm_in.clicked.connect(self.remove_inputs)
        self.btn_done.clicked.connect(self.toggle_done)
        self.chk_recursive.stateChanged.connect(self.set_recursive)

        # status + current file
        root.addWidget(self._sep())
        self.lbl_status = QLabel("入力/出力フォルダを選択してください。")
        self.lbl_file = QLabel("-")
        self.lbl_status.setStyleSheet("font-weight:600"); self.lbl_file.setStyleSheet("color:#555")
        root.addWidget(self.lbl_status); root.addWidget(self.lbl_file)

        # name input + completer (+ space handling)
        self.name_edit = QLineEdit()
        self.name_edit.setPlaceholderText("キャラクター名（Space:再生 / Ctrl+Space:空白 / Enter:振り分け / Del:除外 / Shift+Space:後回し）")
        root.addWidget(self.name_edit)
        self.name_edit.setReadOnly(False)
        self.model = QStringListModel(self.names)
        self.completer = QCompleter(self.model, self)
        self.completer.setCaseSensitivity(Qt.CaseInsensitive)
        self.completer.setFilterMode(Qt.MatchContains)
        self.completer.setCompletionMode(QCompleter.PopupCompletion)
        self.name_edit.setCompleter(self.completer)
        self.name_edit.textChanged.connect(self.on_name_changed)
        self.name_edit.installEventFilter(self)
        try: self.completer.popup().installEventFilter(self)
        except Exception: pass

        # player (guard)
        self.player = None
        try:
            self.audio = QAudioOutput(self)
            self.player = QMediaPlayer(self)
            self.player.setAudioOutput(self.audio)
        except Exception as e:
            self.store.log("player_init_failed", {"error": str(e)})
            self.player = None

        # shortcuts (application scope)
        self.act_play = QAction(self); self.act_play.setShortcut(QKeySequence(Qt.Key_Space))
        self.act_play.setShortcutContext(Qt.ApplicationShortcut); self.act_play.triggered.connect(self.toggle_play); self.addAction(self.act_play)
        self.act_enter1 = QAction(self); self.act_enter1.setShortcut(QKeySequence(Qt.Key_Return))
        self.act_enter1.setShortcutContext(Qt.ApplicationShortcut); self.act_enter1.triggered.connect(self.confirm_and_move); self.addAction(self.act_enter1)
        self.act_enter2 = QAction(self); self.act_enter2.setShortcut(QKeySequence(Qt.Key_Enter))
        self.act_enter2.setShortcutContext(Qt.ApplicationShortcut); self.act_enter2.triggered.connect(self.confirm_and_move); self.addAction(self.act_enter2)
        self.act_del = QAction(self); self.act_del.setShortcut(QKeySequence(Qt.Key_Delete))
        self.act_del.setShortcutContext(Qt.ApplicationShortcut); self.act_del.triggered.connect(self.exclude_current); self.addAction(self.act_del)

        # load
        self.refresh_inputs_view()
        if self.output_dir:
            self.load_files()
        self.ensure_focus()

    # ---------- helpers ----------
    def _sep(self):
        line = QFrame(); line.setFrameShape(QFrame.HLine); line.setFrameShadow(QFrame.Sunken); return line

    def ensure_focus(self):
        self.name_edit.setFocus(); self.name_edit.setCursorPosition(len(self.name_edit.text()))

    def update_completer(self):
        self.model.setStringList(self.names)

    def update_status(self):
        total = len(self.files); pos = self.index + 1 if self.index >= 0 else 0
        enabled_cnt = sum(1 for _,e,d in self.store.list_inputs() if e and not d)
        base = f"{pos}/{total} 件 | プロジェクト:{self.project_key} | 有効入力:{enabled_cnt}"
        base += " | 再帰:ON" if self.recursive else " | 再帰:OFF"
        if self.output_dir: base += f" | 出力:{self.output_dir}"
        self.lbl_status.setText(base)

    # ---------- project ----------
    def ensure_project(self, force_prompt: bool=False) -> str:
        last = self.qsettings.value("last_project", "", str)
        if (not force_prompt) and last and (self.projects_dir / last).exists():
            return last
        dlg = ProjectDialog(self.projects_dir, self)
        if dlg.exec() == QDialog.Accepted:
            key,_ = dlg.get_selection()
            if not key:
                QMessageBox.warning(self, "未選択", "プロジェクトを選ぶか、新規名称を入力してください。")
                return self.ensure_project(force_prompt=True)
            (self.projects_dir / key).mkdir(parents=True, exist_ok=True)
            self.qsettings.setValue("last_project", key)
            return key
        # cancel -> default
        key = "default"; (self.projects_dir / key).mkdir(parents=True, exist_ok=True)
        self.qsettings.setValue("last_project", key); return key

    @Slot()
    def edit_names(self):
        dlg = NamesEditor(self.names, self)
        if dlg.exec() == QDialog.Accepted:
            new_names = dlg.get_names()
            self.names = new_names
            # プロジェクトDBへ保存し、補完を更新
            self.store.set_names(self.names)
            self.update_completer()
            self.store.log("edit_names", {"names": self.names})
        self.ensure_focus()

    @Slot()
    def change_project(self):
        key = self.ensure_project(force_prompt=True)
        if key == self.project_key: return
        self.project_key = key; self.project_dir = self.projects_dir / key
        self.store = Store(self.project_dir / DB_NAME); self.store.set_setting("project_key", self.project_key)
        self.recursive = (self.store.get_setting("recursive", "false") == "true")
        self.chk_recursive.setChecked(self.recursive)
        out = self.store.get_setting("last_output") or ""
        self.output_dir = Path(out) if out else None
        if self.output_dir and not self.output_dir.exists(): self.output_dir = None
        self.names = self.store.get_names(); self.update_completer()
        self.btn_project.setText(f"プロジェクトを選択…（現在: {self.project_key}）")
        self.restored_deferred_once = False
        self.refresh_inputs_view(); self.load_files(); self.ensure_focus()

    # ---------- inputs CRUD ----------
    def refresh_inputs_view(self):
        self.list_inputs.clear()
        # 再接続のためのガード
        try:
            self.list_inputs.itemChanged.disconnect()
        except Exception:
            pass
        for path, enabled, done in self.store.list_inputs():
            it = QListWidgetItem(path)
            it.setFlags(it.flags() | Qt.ItemIsUserCheckable | Qt.ItemIsSelectable | Qt.ItemIsEnabled)
            it.setCheckState(Qt.Checked if enabled else Qt.Unchecked)
            if done: it.setForeground(Qt.gray)
            self.list_inputs.addItem(it)
        try:
            self.list_inputs.itemChanged.connect(self._on_input_item_changed)
        except Exception:
            pass
        self.update_status()

    def _on_input_item_changed(self, item: QListWidgetItem):
        p = Path(item.text()); enabled = (item.checkState() == Qt.Checked)
        self.store.set_enabled(p, enabled); self.load_files()

    @Slot()
    def add_input(self):
        d = QFileDialog.getExistingDirectory(self, "入力フォルダを追加")
        if d:
            self.store.upsert_input(Path(d), enabled=True, done=False)
            self.refresh_inputs_view(); self.load_files(); self.ensure_focus()

    @Slot()
    def remove_inputs(self):
        rows = sorted({i.row() for i in self.list_inputs.selectedIndexes()}, reverse=True)
        for r in rows:
            it = self.list_inputs.item(r)
            if it: self.store.remove_input(Path(it.text()))
        self.refresh_inputs_view(); self.load_files(); self.ensure_focus()

    @Slot()
    def toggle_done(self):
        for i in self.list_inputs.selectedIndexes():
            it = self.list_inputs.item(i.row()); p = Path(it.text())
            cur = next(((Path(path),e,d) for path,e,d in self.store.list_inputs() if path==it.text()), None)
            if cur:
                _,e,d = cur; self.store.set_done(p, not bool(d))
        self.refresh_inputs_view(); self.load_files(); self.ensure_focus()

    @Slot(int)
    def set_recursive(self, state: int):
        self.recursive = (state == Qt.Checked)
        self.store.set_setting("recursive", "true" if self.recursive else "false")
        self.store.log("set_recursive", {"recursive": self.recursive})
        self.load_files(); self.ensure_focus()

    # ---------- output ----------
    @Slot()
    def choose_output(self):
        d = QFileDialog.getExistingDirectory(self, "出力フォルダを選択")
        if d:
            self.output_dir = Path(d)
            self.store.set_setting("last_output", str(self.output_dir))
            self.store.log("choose_output", {"dir": str(self.output_dir)})
            self.update_status(); self.ensure_focus()

    # ---------- player handle helpers ----------
    def _unload_player_current(self):
        try:
            if self.player: self.player.stop()
        except Exception:
            pass
        try:
            if self.player: self.player.setSource(QUrl())
        except Exception:
            pass
        QApplication.processEvents()

    def _try_move_with_retry(self, src: Path, dest: Path, tries: int = 10, wait_sec: float = 0.05):
        last_err = None
        for _ in range(tries):
            try:
                shutil.move(str(src), str(dest))
                return True, None
            except Exception as e:
                last_err = e; time.sleep(wait_sec)
        return False, last_err

    # ---------- scanning ----------
    def load_files(self):
        inputs = [(Path(p), bool(e), bool(d)) for p,e,d in self.store.list_inputs()]
        files: List[Path] = []

        def add_from_dir(d: Path):
            if not d.exists(): return
            if d.name in (EXCLUDE_DIR_NAME, DEFER_DIR_NAME): return
            if self.recursive:
                for p in sorted(d.rglob("*")):
                    if p.is_dir() and p.name in (EXCLUDE_DIR_NAME, DEFER_DIR_NAME):
                        continue
                    if p.is_file() and p.suffix.lower() in AUDIO_EXTS:
                        files.append(p)
            else:
                for p in sorted(d.iterdir()):
                    if p.is_dir() and p.name in (EXCLUDE_DIR_NAME, DEFER_DIR_NAME):
                        continue
                    if p.is_file() and p.suffix.lower() in AUDIO_EXTS:
                        files.append(p)

        for d, enabled, done in inputs:
            if enabled and not done: add_from_dir(Path(d))

        # de-dup
        seen=set(); uniq=[]
        for p in files:
            sp=str(p)
            if sp not in seen: seen.add(sp); uniq.append(p)
        self.files = uniq; self.index = 0 if self.files else -1
        self.update_status(); self.store.log("load_files", {"count": len(self.files)})

        if not self.files:
            # 通常ファイルがなく、後回しが残っていれば元に戻してもう一度ロード
            if self.restore_deferred_if_any():
                self.restored_deferred_once = True
                return self.load_files()
        self.show_current_file()

    def restore_deferred_if_any(self) -> bool:
        """各入力フォルダ配下の _deferred_by_voice_sorter の中身を親フォルダへ戻す。
        戻した場合 True を返す。"""
        restored = False
        for path, enabled, done in self.store.list_inputs():
            if not enabled or done: continue
            base = Path(path)
            if not base.exists(): continue
            # 再帰でも非再帰でも、とりあえず全ての DEFER_DIR_NAME を探す
            for d in base.rglob(DEFER_DIR_NAME) if self.recursive else [base / DEFER_DIR_NAME]:
                if not d.exists() or not d.is_dir(): continue
                for f in sorted(d.iterdir()):
                    if f.is_file():
                        dest = d.parent / f.name
                        if dest.exists():
                            stem, suf = dest.stem, dest.suffix; i = 1
                            while True:
                                cand = d.parent / f"{stem} ({i}){suf}"
                                if not cand.exists(): dest = cand; break
                                i += 1
                        ok, err = self._try_move_with_retry(f, dest)
                        if ok:
                            restored = True
                            self.store.log("restore_deferred", {"src": str(f), "dst": str(dest)})
                        else:
                            self.store.log("restore_deferred_error", {"src": str(f), "error": str(err)})
                # 空になった defer フォルダは残しても問題ないのでそのまま
        return restored

    # ---------- playback ----------
    @Slot()
    def toggle_play(self):
        if not self.player:
            QMessageBox.information(self, "再生不可", "再生バックエンドが利用できません。"); return
        if self.player.source().isEmpty(): self.show_current_file()
        if self.player.playbackState() == QMediaPlayer.PlaybackState.PlayingState:
            self.player.pause(); self.store.log("pause", {"file": self.player.source().toString()})
        else:
            self.player.play(); self.store.log("play", {"file": self.player.source().toString()})
        self.ensure_focus()

    # ---------- classify / exclude / defer ----------
    def _safe_folder_name(self, name: str) -> str:
        return safe_key(name)

    @Slot()
    def confirm_and_move(self):
         # Enter 実行時に補完ポップアップが残らないよう明示的に閉じる
        try:
            if self.completer and self.completer.popup().isVisible():
                self.completer.popup().hide()
        except Exception:
            pass
        name = self.name_edit.text().strip()
        if not name:
            QMessageBox.warning(self, "未入力", "キャラクター名を入力してください。"); self.store.log("move_failed", {"reason":"empty_name"}); self.ensure_focus(); return
        if not self.output_dir:
            QMessageBox.warning(self, "出力未指定", "出力フォルダを選択してください。"); self.store.log("move_failed", {"reason":"no_output_dir"}); self.ensure_focus(); return
        if not (0 <= self.index < len(self.files)): self.ensure_focus(); return

        src = self.files[self.index]
        safe = self._safe_folder_name(name)
        dest_dir = self.output_dir / safe; dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / src.name
        if dest.exists():
            stem,suf = dest.stem,dest.suffix; i=1
            while True:
                cand = dest_dir / f"{stem} ({i}){suf}"
                if not cand.exists(): dest=cand; break
                i+=1

        self._unload_player_current()
        ok, err = self._try_move_with_retry(src, dest)
        if not ok:
            QMessageBox.critical(self, "移動エラー", f"ファイルを移動できませんでした:\n{err}")
            self.store.log("move_error", {"src": str(src), "error": str(err)}); self.ensure_focus(); return

        self.store.log("move", {"character": name, "folder": safe, "src": str(src), "dst": str(dest)})

        del self.files[self.index]
        if self.index >= len(self.files): self.index = len(self.files) - 1
        self.name_locked = False
        self.name_edit.setReadOnly(False)
        self.name_edit.clear()
        self.prev_name_text = ""  # 送出後にリセット
        self.update_status();
        if self.index < 0 and not self.files:
            if self.restore_deferred_if_any():
                return self.load_files()
        self.show_current_file(); self.ensure_focus()

    @Slot()
    def exclude_current(self):
        if not (0 <= self.index < len(self.files)): self.ensure_focus(); return
        src = self.files[self.index]
        excl_dir = src.parent / EXCLUDE_DIR_NAME
        excl_dir.mkdir(exist_ok=True)
        dest = excl_dir / src.name
        if dest.exists():
            stem,suf = dest.stem,dest.suffix; i=1
            while True:
                cand = excl_dir / f"{stem} ({i}){suf}"
                if not cand.exists(): dest=cand; break
                i+=1
        self._unload_player_current()
        ok, err = self._try_move_with_retry(src, dest)
        if not ok:
            QMessageBox.critical(self, "除外エラー", f"ファイルを除外できませんでした:\n{err}")
            self.store.log("exclude_error", {"src": str(src), "error": str(err)}); self.ensure_focus(); return
        self.store.log("exclude", {"src": str(src), "dst": str(dest)})
        del self.files[self.index]
        if self.index >= len(self.files): self.index = len(self.files) - 1
        self.update_status();
        if self.index < 0 and not self.files:
            if self.restore_deferred_if_any():
                return self.load_files()
        self.show_current_file(); self.ensure_focus()

    def defer_current(self):
        """Shift+Space: 後回し。処理中フォルダ直下の DEFER_DIR_NAME へ移動"""
        if not (0 <= self.index < len(self.files)): self.ensure_focus(); return
        src = self.files[self.index]
        dfr_dir = src.parent / DEFER_DIR_NAME
        dfr_dir.mkdir(exist_ok=True)
        dest = dfr_dir / src.name
        if dest.exists():
            stem,suf = dest.stem,dest.suffix; i=1
            while True:
                cand = dfr_dir / f"{stem} ({i}){suf}"
                if not cand.exists(): dest=cand; break
                i+=1
        self._unload_player_current()
        ok, err = self._try_move_with_retry(src, dest)
        if not ok:
            QMessageBox.critical(self, "後回しエラー", f"ファイルを後回しにできませんでした:\n{err}")
            self.store.log("defer_error", {"src": str(src), "error": str(err)}); self.ensure_focus(); return
        self.store.log("defer", {"src": str(src), "dst": str(dest)})
        del self.files[self.index]
        if self.index >= len(self.files): self.index = len(self.files) - 1
        self.update_status();
        if self.index < 0 and not self.files:
            if self.restore_deferred_if_any():
                return self.load_files()
        self.show_current_file(); self.ensure_focus()

    # ---------- display ----------
    def show_current_file(self):
        if 0 <= self.index < len(self.files):
            f = self.files[self.index]
            self.lbl_file.setText("現在: " + f.name)
            try:
                if self.player:
                    self.player.stop(); self.player.setSource(QUrl.fromLocalFile(str(f)))
            except Exception as e:
                self.store.log("player_set_source_failed", {"file": str(f), "error": str(e)})
            self.store.log("show_file", {"file": str(f), "index": self.index, "total": len(self.files)})
        else:
            self.lbl_file.setText("完了！ファイルはありません。")
            try:
                if self.player: self.player.stop(); self.player.setSource(QUrl())
            except Exception:
                pass
            self.store.log("show_file_none", {})

    # ---------- keyboard / autocomplete ----------
    def eventFilter(self, obj, event):
        # --- グローバル: Del は常に「除外」動作（かつロック解除） ---
        if event.type() == QEvent.KeyPress and event.key() == Qt.Key_Delete and event.modifiers() == Qt.NoModifier:
            self.name_locked = False  # 仕様どおり Delete で解除。ただし挙動は「除外」が優先
            try: self.name_edit.setReadOnly(False)
            except Exception: pass
            self.exclude_current()
            return True

        if event.type() == QEvent.KeyPress:
            # Space 系はグローバルに捕捉: Space→再生, Ctrl+Space→空白, Shift+Space→後回し
            if event.key() == Qt.Key_Space:
                if event.modifiers() == Qt.ControlModifier:
                    if obj is self.name_edit:
                        self.name_edit.insert(" ")
                    return True
                if event.modifiers() == Qt.ShiftModifier:
                    self.defer_current(); return True
                if event.modifiers() == Qt.NoModifier:
                    self.toggle_play(); return True
            if obj is self.name_edit:
                # 確定ロック中は一部以外の入力をブロック
                if self.name_locked:
                    allowed = {Qt.Key_Backspace, Qt.Key_Return, Qt.Key_Enter,
                               Qt.Key_Left, Qt.Key_Right, Qt.Key_Home, Qt.Key_End,
                               Qt.Key_Tab, Qt.Key_Backtab}
                    # Ctrl+V / Shift+Insert（貼り付け）も禁止
                    if (event.modifiers() & Qt.ControlModifier and event.key() == Qt.Key_V) or \
                       (event.modifiers() & Qt.ShiftModifier and event.key() == Qt.Key_Insert):
                        return True
                    if event.key() not in allowed:
                        return True
                    # Backspace でロック解除（Delete はグローバル除外を既に処理済み）
                    # 1) ロック解除＆編集可能化
                    self.name_locked = False
                    try:
                        self.name_edit.setReadOnly(False)
                        self.name_edit.setContextMenuPolicy(Qt.DefaultContextMenu)
                    except Exception:
                        pass
                    # 2) 読み取り専用だと元のイベントは効かないので、ここで手動削除してイベントを消費
                    txt = self.name_edit.text()
                    start = self.name_edit.selectionStart()
                    if start != -1:
                        # 選択範囲を削除
                        sel = self.name_edit.selectedText()
                        new_txt = txt[:start] + txt[start + len(sel):]
                        self.name_edit.blockSignals(True)
                        self.name_edit.setText(new_txt)
                        self.name_edit.blockSignals(False)
                        self.name_edit.setCursorPosition(start)
                    else:
                        # カーソル直前の1文字を削除
                        cur = self.name_edit.cursorPosition()
                        if cur > 0:
                            new_txt = txt[:cur-1] + txt[cur:]
                            self.name_edit.blockSignals(True)
                            self.name_edit.setText(new_txt)
                            self.name_edit.blockSignals(False)
                            self.name_edit.setCursorPosition(cur-1)
                    return True  # ここで完結する
        # IME からの確定（かな入力など）をロック中は無効化
        if obj is self.name_edit and self.name_locked and event.type() == QEvent.InputMethod:
            return True
        # ドロップによる貼り付け防止（ロック中）
        if obj is self.name_edit and self.name_locked and event.type() in (QEvent.DragEnter, QEvent.Drop):
            return True
        # 右クリックメニュー自体は on_name_changed で無効化しているが、保険で ContextMenu もブロック
        if obj is self.name_edit and self.name_locked and event.type() == QEvent.ContextMenu:
            return True
        return super().eventFilter(obj, event)

    @Slot(str)
    def on_name_changed(self, text: str):
        t = (text or "").strip()
        if not t:
            self.name_locked = False
            try: self.name_edit.setReadOnly(False)
            except Exception: pass
            try: self.name_edit.setContextMenuPolicy(Qt.DefaultContextMenu)
            except Exception: pass
            return
        if self.name_locked:
            return
        matches = [n for n in self.names if t.lower() in n.lower()]
        if len(matches) == 1:
            m = matches[0]
            if m.lower().startswith(t.lower()):
                # まだ完全一致でないなら確定文字列に合わせる
                if m != text:
                    self.name_edit.blockSignals(True)
                    self.name_edit.setText(m)
                    self.name_edit.blockSignals(False)
                    self.name_edit.setCursorPosition(len(m))
                # 文字列が同じでも「ユニークに確定」したのでロックを必ず掛ける
                self.name_locked = True
                # 候補ポップアップを消す
                try:
                    if self.completer and self.completer.popup().isVisible():
                        self.completer.popup().hide()
                except Exception:
                    pass
                # ロック中は右クリック貼り付け禁止 + 入力を不可
                try: self.name_edit.setContextMenuPolicy(Qt.NoContextMenu)
                except Exception: pass
                try: self.name_edit.setReadOnly(True)
                except Exception: pass
        self.ensure_focus()

# ---------- entry ----------
def main():
    app = QApplication(sys.argv)
    w = VoiceSorter(); w.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
