# -*- coding: utf-8 -*-
"""
音声ファイル分類補助ソフト（PySide6）
プロジェクト対応／複数入力／有効/無効／完了タグ／再帰探索／除外（Del）／後回し（Shift+Space）
履歴保存／永続 Undo/Redo（Ctrl+Z / Ctrl+Shift+Z or Ctrl+Y）

挙動要点：
- 起動時に必ず「プロジェクト選択/新規作成/削除/リネーム」ダイアログ。
- 永続化は**プロジェクト単位**（~/.voice_sorter/projects/<project_key>/voice_sorter.sqlite3）。
- 仕分け操作（move/exclude/defer）は内部 history に `op_id`・from/to を記録（Undo/Redo 用）。
- 監査ログ（audit）は**確定操作のみ**記録（move/exclude/defer）。undo/redo は記録しない。
- **Undo** は「キャラ名入力前（仕分け前）」に戻す：ファイルを元位置へ戻し、該当ファイルをカレント、入力欄は**シグナル停止で完全クリア**＆ロック解除。
- **Redo** は「より分け済み」まで進める：再度仕分け先へ移動（採番で衝突回避）。入力欄は触らない。
- Redo ショートカットは標準（Ctrl+Shift+Z / ⌘⇧Z）と Windows 流儀（Ctrl+Y）の両対応。
- QLineEdit のテキストUndo/Redoを**無効化**し、Ctrl+Z / Ctrl+Shift+Z / Ctrl+Y は **eventFilter で先取り**してアプリの Undo/Redo を発火。
- ★ ロック（name_locked=True）時は IME 確定含む全入力をブロック。Backspace は**ロック解除しつつ**一文字削除が即時に効く。
"""

from __future__ import annotations
import sys, json, re, sqlite3, shutil, time, uuid
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Any
from datetime import datetime

from PySide6.QtCore import Qt, QUrl, QSettings, Slot, QStringListModel, QEvent, QTimer
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
DEFER_DIR_NAME   = "_deferred_by_voice_sorter"

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
    def get_setting(self, key: str, default: Optional[str] = None) -> Optional[str]:
        c = self.conn.cursor(); c.execute("SELECT value FROM settings WHERE key=?", (key,))
        r = c.fetchone(); return r[0] if r else default

    def set_setting(self, key: str, value: str):
        c = self.conn.cursor()
        c.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        self.conn.commit()

    # names
    def get_names(self) -> List[str]:
        c = self.conn.cursor(); c.execute("SELECT name FROM names ORDER BY name COLLATE NOCASE")
        return [r[0] for r in c.fetchall()]

    def set_names(self, names: List[str]):
        c = self.conn.cursor(); c.execute("DELETE FROM names")
        for n in names:
            if n: c.execute("INSERT OR IGNORE INTO names(name) VALUES(?)", (n,))
        self.conn.commit()

    # inputs
    def list_inputs(self) -> List[Tuple[str,int,int]]:
        c = self.conn.cursor(); c.execute("SELECT path, enabled, done FROM inputs ORDER BY path")
        return list(c.fetchall())

    def upsert_input(self, path: Path, enabled: bool=True, done: bool=False):
        c = self.conn.cursor()
        c.execute(
            "INSERT INTO inputs(path,enabled,done) VALUES(?,?,?) "
            "ON CONFLICT(path) DO UPDATE SET enabled=excluded.enabled, done=excluded.done",
            (str(path), 1 if enabled else 0, 1 if done else 0),
        )
        self.conn.commit()

    def set_enabled(self, path: Path, enabled: bool):
        c = self.conn.cursor(); c.execute("UPDATE inputs SET enabled=? WHERE path=?", (1 if enabled else 0, str(path)))
        self.conn.commit()

    def set_done(self, path: Path, done: bool):
        c = self.conn.cursor(); c.execute("UPDATE inputs SET done=? WHERE path=?", (1 if done else 0, str(path)))
        self.conn.commit()

    def remove_input(self, path: Path):
        c = self.conn.cursor(); c.execute("DELETE FROM inputs WHERE path=?", (str(path),))
        self.conn.commit()

    # internal history
    def log(self, action: str, payload: Dict[str, Any]):
        ts = datetime.now().isoformat(timespec="seconds")
        self.conn.execute(
            "INSERT INTO history(ts,action,payload) VALUES(?,?,?)",
            (ts, action, json.dumps(payload, ensure_ascii=False))
        )
        self.conn.commit()

    def fetch_history(self) -> List[Dict[str, Any]]:
        c = self.conn.cursor()
        c.execute("SELECT id, ts, action, payload FROM history ORDER BY id ASC")
        rows = []
        for _id, ts, action, payload in c.fetchall():
            try:
                data = json.loads(payload) if payload else {}
            except Exception:
                data = {}
            rows.append({"id": _id, "ts": ts, "action": action, "payload": data})
        return rows

    # audit log (confirmed ops only)
    def audit(self, op: str, *, src: str, dst: str, character: Optional[str]=None, folder: Optional[str]=None):
        ts = datetime.now().isoformat(timespec="seconds")
        self.conn.execute(
            "INSERT INTO audit(ts,op,src,dst,character,folder) VALUES(?,?,?,?,?,?)",
            (ts, op, src, dst, character, folder)
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
            if "," in line:
                parts.extend(p.strip() for p in line.split(","))
            else:
                parts.append(line.strip())
        seen=set(); out: List[str]=[]
        for p in parts:
            if p and p not in seen:
                seen.add(p); out.append(p)
        return out

class ProjectDialog(QDialog):
    """プロジェクト選択/作成 + リネーム/削除"""
    def __init__(self, projects_dir: Path, parent=None):
        super().__init__(parent)
        self.setWindowTitle("プロジェクトを選択/作成")
        self.setMinimumSize(460, 420)
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

        btn_row = QHBoxLayout()
        self.btn_rename = QPushButton("リネーム")
        self.btn_delete = QPushButton("削除")
        btn_row.addWidget(self.btn_rename); btn_row.addWidget(self.btn_delete)
        lay.addLayout(btn_row)

        self.btn_rename.clicked.connect(self._rename_selected)
        self.btn_delete.clicked.connect(self._delete_selected)

        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept); btns.rejected.connect(self.reject)
        lay.addWidget(btns)

    def _refresh_list(self):
        self.listw.clear()
        for d in sorted([p.name for p in self.projects_dir.iterdir() if p.is_dir()]):
            self.listw.addItem(QListWidgetItem(d))

    def _rename_selected(self):
        cur = self.listw.currentItem()
        if not cur:
            QMessageBox.warning(self, "未選択", "リネームするプロジェクトを選択してください。"); return
        old = cur.text()
        new_raw = self.new_edit.text().strip()
        new = safe_key(new_raw or "")
        if not new:
            QMessageBox.warning(self, "名称未入力", "新しいプロジェクト名を入力してください。"); return
        if new == old:
            QMessageBox.information(self, "同一名", "同じ名前です。"); return
        if (self.projects_dir / new).exists():
            QMessageBox.warning(self, "重複", f"既に存在します: {new}"); return
        try:
            (self.projects_dir / old).rename(self.projects_dir / new)
            QMessageBox.information(self, "成功", f"{old} → {new} に変更しました。")
            self._refresh_list()
            items = self.listw.findItems(new, Qt.MatchExactly)
            if items: self.listw.setCurrentItem(items[0])
        except Exception as e:
            QMessageBox.critical(self, "エラー", f"リネームに失敗しました:\n{e}")

    def _delete_selected(self):
        cur = self.listw.currentItem()
        if not cur:
            QMessageBox.warning(self, "未選択", "削除するプロジェクトを選択してください。"); return
        name = cur.text()
        ret = QMessageBox.question(
            self, "確認",
            f"プロジェクト「{name}」を**完全に削除**します。フォルダごと消えます。よろしいですか？",
            QMessageBox.Yes | QMessageBox.No, QMessageBox.No
        )
        if ret != QMessageBox.Yes: return
        try:
            shutil.rmtree(self.projects_dir / name, ignore_errors=False)
            QMessageBox.information(self, "削除完了", f"{name} を削除しました。")
            self._refresh_list()
        except Exception as e:
            QMessageBox.critical(self, "エラー", f"削除に失敗しました:\n{e}")

    def get_selection(self) -> Tuple[str, bool]:
        name = self.new_edit.text().strip()
        if name:
            return safe_key(name), True
        cur = self.listw.currentItem()
        if cur:
            return cur.text(), False
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
        if self.output_dir and not self.output_dir.exists():
            self.output_dir = None

        self.names = self.store.get_names()
        self.files: List[Path] = []; self.index = -1

        self.name_locked: bool = False         # 一意確定ロック
        self.prev_name_text: str = ""
        self.is_deleting: bool = False

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
        self.chk_recursive = QCheckBox("再帰的に探索")
        for w in (self.btn_add_in, self.btn_rm_in, self.btn_done, self.chk_recursive):
            col_right.addWidget(w)
        col_right.addStretch(1)

        self.btn_add_in.clicked.connect(self.add_input)
        self.btn_rm_in.clicked.connect(self.remove_inputs)
        self.btn_done.clicked.connect(self.toggle_done)
        self.chk_recursive.stateChanged.connect(self.set_recursive)

        # status + current file
        root.addWidget(self._sep())
        self.lbl_status = QLabel("入力/出力フォルダを選択してください。")
        self.lbl_file = QLabel("-")
        self.lbl_playback_time = QLabel("再生時間: 0:00.00 / 0:00.00")
        self.lbl_status.setStyleSheet("font-weight:600")
        self.lbl_file.setStyleSheet("font-size:11pt")
        self.lbl_playback_time.setStyleSheet("font-size:11pt; font-weight:600")
        root.addWidget(self.lbl_status)
        root.addWidget(self.lbl_file)
        root.addWidget(self.lbl_playback_time)

        # name input + completer
        self.name_edit = QLineEdit()
        self.name_edit.setPlaceholderText(
            "キャラクター名（Space:再生 / Ctrl+Space:空白 / Enter:振り分け / Del:除外 / "
            "Shift+Space:後回し / Ctrl+Z:取り消し / Ctrl+Shift+Z or Ctrl+Y:やり直し）"
        )
        root.addWidget(self.name_edit)
        self.name_edit.setReadOnly(False)

        # QLineEdit のテキストUndo/Redoを無効化
        try:
            self.name_edit.setUndoRedoEnabled(False)
        except Exception:
            pass

        self.model = QStringListModel(self.names)
        self.completer = QCompleter(self.model, self)
        self.completer.setCaseSensitivity(Qt.CaseInsensitive)
        self.completer.setFilterMode(Qt.MatchContains)
        self.completer.setCompletionMode(QCompleter.PopupCompletion)
        self.name_edit.setCompleter(self.completer)
        self.name_edit.textChanged.connect(self.on_name_changed)
        self.name_edit.installEventFilter(self)
        try:
            self.completer.popup().installEventFilter(self)
        except Exception:
            pass

        # player (guard)
        self.player = None
        try:
            self.audio = QAudioOutput(self)
            self.player = QMediaPlayer(self)
            self.player.setAudioOutput(self.audio)
            self.player.positionChanged.connect(self.update_playback_time)
            self.player.durationChanged.connect(self.update_playback_time)
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

        # Undo / Redo（永続）
        self.act_undo = QAction(self)
        self.act_undo.setShortcut(QKeySequence.Undo)  # Ctrl+Z / ⌘Z
        self.act_undo.setShortcutContext(Qt.ApplicationShortcut)
        self.act_undo.triggered.connect(self.undo_last_persistent)
        self.addAction(self.act_undo)

        self.act_redo = QAction(self)
        self.act_redo.setShortcuts([QKeySequence.Redo, QKeySequence("Ctrl+Y")])  # Ctrl+Shift+Z / ⌘⇧Z / Ctrl+Y
        self.act_redo.setShortcutContext(Qt.ApplicationShortcut)
        self.act_redo.triggered.connect(self.redo_last_persistent)
        self.addAction(self.act_redo)

        # load
        self.refresh_inputs_view()
        # チェックボックスの初期値を設定（全UIが作成された後）
        self.chk_recursive.setChecked(self.recursive)
        if self.output_dir:
            self.load_files()
        self.ensure_focus()

    # ---------- helpers ----------
    def _sep(self):
        line = QFrame(); line.setFrameShape(QFrame.HLine); line.setFrameShadow(QFrame.Sunken)
        return line

    def ensure_focus(self):
        # フォーカスだけ当てる（ロック・ReadOnlyは一切いじらない）
        QTimer.singleShot(0, lambda: (self.name_edit.setFocus(),
                                      self.name_edit.setCursorPosition(len(self.name_edit.text()))))

    def update_completer(self):
        self.model.setStringList(self.names)

    def update_status(self):
        total = len(self.files); pos = self.index + 1 if self.index >= 0 else 0
        enabled_cnt = sum(1 for _,e,d in self.store.list_inputs() if e and not d)
        
        base = f"{pos}/{total} 件 | プロジェクト:{self.project_key} | 有効入力:{enabled_cnt}"
        base += " | 再帰:ON" if self.recursive else " | 再帰:OFF"
        if self.output_dir: base += f" | 出力:{self.output_dir}"
        self.lbl_status.setText(base)

    def _goto_file(self, target: Path) -> bool:
        try:
            sp = str(target)
            for i, p in enumerate(self.files):
                if str(p) == sp:
                    self.index = i
                    self.show_current_file()
                    self.ensure_focus()
                    return True
        except Exception:
            pass
        return False

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
        key = "default"; (self.projects_dir / key).mkdir(parents=True, exist_ok=True)
        self.qsettings.setValue("last_project", key); return key

    @Slot()
    def edit_names(self):
        dlg = NamesEditor(self.names, self)
        if dlg.exec() == QDialog.Accepted:
            new_names = dlg.get_names()
            self.names = new_names
            self.store.set_names(self.names)
            self.update_completer()
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

        self.refresh_inputs_view(); self.load_files(); self.ensure_focus()

    # ---------- inputs CRUD ----------
    def refresh_inputs_view(self):
        self.list_inputs.clear()
        try: 
            self.list_inputs.itemChanged.disconnect()
        except (RuntimeError, TypeError): 
            pass

        for path, enabled, done in self.store.list_inputs():
            it = QListWidgetItem(path)
            it.setFlags(it.flags() | Qt.ItemIsUserCheckable | Qt.ItemIsSelectable | Qt.ItemIsEnabled)
            it.setCheckState(Qt.Checked if enabled else Qt.Unchecked)
            if done: it.setForeground(Qt.gray)
            self.list_inputs.addItem(it)

        try: self.list_inputs.itemChanged.connect(self._on_input_item_changed)
        except Exception: pass
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
            if it:
                self.store.remove_input(Path(it.text()))
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
        from PySide6.QtCore import Qt
        self.recursive = (state == Qt.CheckState.Checked.value) or (state == 2)
        self.store.set_setting("recursive", "true" if self.recursive else "false")
        self.load_files(); self.ensure_focus()

    # ---------- output ----------
    @Slot()
    def choose_output(self):
        d = QFileDialog.getExistingDirectory(self, "出力フォルダを選択")
        if d:
            self.output_dir = Path(d)
            self.store.set_setting("last_output", str(self.output_dir))
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
            if not d.exists():
                return
            if d.name in (EXCLUDE_DIR_NAME, DEFER_DIR_NAME): return
            if self.recursive:
                for p in sorted(d.rglob("*")):
                    # 除外ディレクトリ内のファイルをスキップ
                    if any(part in (EXCLUDE_DIR_NAME, DEFER_DIR_NAME) for part in p.parts):
                        continue
                    if p.is_file() and p.suffix.lower() in AUDIO_EXTS: files.append(p)
            else:
                for p in sorted(d.iterdir()):
                    if p.is_dir() and p.name in (EXCLUDE_DIR_NAME, DEFER_DIR_NAME): continue
                    if p.is_file() and p.suffix.lower() in AUDIO_EXTS: files.append(p)

        for d, enabled, done in inputs:
            if enabled and not done: add_from_dir(Path(d))

        seen=set(); uniq=[]
        for p in files:
            sp=str(p)
            if sp not in seen:
                seen.add(sp); uniq.append(p)

        self.files = uniq; self.index = 0 if self.files else -1
        self.update_status()

        if not self.files:
            if self.restore_deferred_if_any():
                return self.load_files()
        self.show_current_file()

    def restore_deferred_if_any(self) -> bool:
        restored = False
        for path, enabled, done in self.store.list_inputs():
            if not enabled or done: continue
            base = Path(path)
            if not base.exists(): continue
            targets = list(base.rglob(DEFER_DIR_NAME)) if self.recursive else [base / DEFER_DIR_NAME]
            for d in targets:
                if not d.exists() or not d.is_dir(): continue
                for f in sorted(d.iterdir()):
                    if f.is_file():
                        dest = d.parent / f.name
                        if dest.exists():
                            stem, suf = dest.stem, dest.suffix; i = 1
                            while True:
                                cand = d.parent / f"{stem} ({i}){suf}"
                                if not cand.exists():
                                    dest = cand; break
                                i += 1
                        ok, err = self._try_move_with_retry(f, dest)
                        if ok:
                            restored = True
                        else:
                            self.store.log("restore_deferred_error", {"src": str(f), "error": str(err)})
        return restored

    # ---------- playback ----------
    def _format_time(self, ms: int) -> str:
        total_seconds = ms / 1000.0
        m = int(total_seconds // 60)
        s = total_seconds % 60
        return f"{m}:{s:05.2f}"

    @Slot()
    def update_playback_time(self):
        if not self.player:
            return
        # ソースが空の場合は 0:00.00/0:00.00 にリセット
        if self.player.source().isEmpty():
            self.lbl_playback_time.setText("再生時間: 0:00.00 / 0:00.00")
            return
        pos = self.player.position()
        dur = self.player.duration()
        # デバッグ: 実際の値を確認
        # print(f"[DEBUG] pos={pos}ms, dur={dur}ms")
        pos_str = self._format_time(pos) if pos >= 0 else "0:00.00"
        dur_str = self._format_time(dur) if dur > 0 else "0:00.00"
        self.lbl_playback_time.setText(f"再生時間: {pos_str} / {dur_str}")

    @Slot()
    def toggle_play(self):
        if not self.player:
            QMessageBox.information(self, "再生不可", "再生バックエンドが利用できません。"); return
        if self.player.source().isEmpty():
            self.show_current_file()
        if self.player.playbackState() == QMediaPlayer.PlaybackState.PlayingState:
            self.player.pause()
        else:
            self.player.play()
        self.ensure_focus()

    # ---------- classify / exclude / defer ----------
    def _safe_folder_name(self, name: str) -> str:
        return safe_key(name)

    def _log_op(self, action: str, op_id: str, src: Path, dst: Path, typ: str):
        # 内部履歴（Undo/Redo 用）
        self.store.log(action, {"op_id": op_id, "type": typ, "from": str(src), "to": str(dst)})

    def _new_op_id(self) -> str:
        return uuid.uuid4().hex

    def _finalize_dest(self, dest: Path) -> Path:
        if not dest.exists(): return dest
        stem, suf = dest.stem, dest.suffix; i = 1
        while True:
            cand = dest.parent / f"{stem} ({i}){suf}"
            if not cand.exists(): return cand
            i += 1

    @Slot()
    def confirm_and_move(self):
        try:
            if self.completer and self.completer.popup().isVisible():
                self.completer.popup().hide()
        except Exception:
            pass

        name = self.name_edit.text().strip()
        if not name:
            QMessageBox.warning(self, "未入力", "キャラクター名を入力してください。")
            self.ensure_focus(); return
        if not self.output_dir:
            QMessageBox.warning(self, "出力未指定", "出力フォルダを選択してください。")
            self.ensure_focus(); return
        if not (0 <= self.index < len(self.files)):
            self.ensure_focus(); return

        src = self.files[self.index]
        safe = self._safe_folder_name(name)
        dest_dir = self.output_dir / safe; dest_dir.mkdir(parents=True, exist_ok=True)
        dest = self._finalize_dest(dest_dir / src.name)

        self._unload_player_current()
        ok, err = self._try_move_with_retry(src, dest)
        if not ok:
            QMessageBox.critical(self, "移動エラー", f"ファイルを移動できませんでした:\n{err}")
            self.ensure_focus(); return

        op_id = self._new_op_id()
        self._log_op("move", op_id, src, dest, "move")  # 内部用
        self.store.audit("move", src=str(src), dst=str(dest), character=name, folder=safe)  # 監査ログ

        # 次のファイルへ進む前に UI 状態を完全リセット（明示的に解除）
        del self.files[self.index]
        if self.index >= len(self.files):
            self.index = len(self.files) - 1

        self.name_locked = False
        self.is_deleting = False
        self.name_edit.blockSignals(True)
        self.name_edit.clear()
        self.name_edit.blockSignals(False)
        self.prev_name_text = ""
        try:
            self.name_edit.setReadOnly(False)
            self.name_edit.setContextMenuPolicy(Qt.DefaultContextMenu)
        except Exception:
            pass
        try:
            if self.completer and self.completer.popup().isVisible():
                self.completer.popup().hide()
        except Exception:
            pass

        self.update_status()
        if self.index < 0 and not self.files:
            if self.restore_deferred_if_any():
                return self.load_files()

        self.show_current_file()
        self.ensure_focus()

    @Slot()
    def exclude_current(self):
        if not (0 <= self.index < len(self.files)):
            self.ensure_focus(); return
        src = self.files[self.index]
        excl_dir = src.parent / EXCLUDE_DIR_NAME; excl_dir.mkdir(exist_ok=True)
        dest = self._finalize_dest(excl_dir / src.name)

        self._unload_player_current()
        ok, err = self._try_move_with_retry(src, dest)
        if not ok:
            QMessageBox.critical(self, "除外エラー", f"ファイルを除外できませんでした:\n{err}")
            self.ensure_focus(); return

        op_id = self._new_op_id()
        self._log_op("exclude", op_id, src, dest, "exclude")  # 内部用
        self.store.audit("exclude", src=str(src), dst=str(dest))  # 監査ログ

        del self.files[self.index]
        if self.index >= len(self.files): self.index = len(self.files) - 1
        self.update_status()
        if self.index < 0 and not self.files:
            if self.restore_deferred_if_any(): return self.load_files()
        self.show_current_file(); self.ensure_focus()

    def defer_current(self):
        if not (0 <= self.index < len(self.files)):
            self.ensure_focus(); return
        src = self.files[self.index]
        dfr_dir = src.parent / DEFER_DIR_NAME; dfr_dir.mkdir(exist_ok=True)
        dest = self._finalize_dest(dfr_dir / src.name)

        self._unload_player_current()
        ok, err = self._try_move_with_retry(src, dest)
        if not ok:
            QMessageBox.critical(self, "後回しエラー", f"ファイルを後回しにできませんでした:\n{err}")
            self.ensure_focus(); return

        op_id = self._new_op_id()
        self._log_op("defer", op_id, src, dest, "defer")   # 内部用
        self.store.audit("defer", src=str(src), dst=str(dest))  # 監査ログ

        del self.files[self.index]
        if self.index >= len(self.files): self.index = len(self.files) - 1
        self.update_status()
        if self.index < 0 and not self.files:
            if self.restore_deferred_if_any(): return self.load_files()
        self.show_current_file(); self.ensure_focus()

    # ---------- persistent undo/redo helpers ----------
    def _build_op_state(self) -> Dict[str, Dict[str, Any]]:
        ops: Dict[str, Dict[str, Any]] = {}
        rows = self.store.fetch_history()
        for row in rows:
            act = row["action"]; p = row["payload"] or {}
            op_id = p.get("op_id")
            if not op_id:
                continue
            if act in ("move", "exclude", "defer"):
                ops.setdefault(op_id, {
                    "type": p.get("type", act),
                    "origin_src": Path(p.get("from", "")),
                    "origin_dst": Path(p.get("to", "")),
                    "state": "applied",
                    "current_path": Path(p.get("to", "")),
                    "last_event_id": row["id"],
                })
                ops[op_id]["state"] = "applied"
                ops[op_id]["current_path"] = Path(p.get("to", ""))
                ops[op_id]["last_event_id"] = row["id"]
            elif act == "undo":
                if op_id in ops:
                    ops[op_id]["state"] = "undone"
                    ops[op_id]["current_path"] = Path(p.get("to", p.get("from", "")))
                    ops[op_id]["last_event_id"] = row["id"]
            elif act == "redo":
                if op_id in ops:
                    ops[op_id]["state"] = "applied"
                    ops[op_id]["current_path"] = Path(p.get("to", p.get("from", "")))
                    ops[op_id]["last_event_id"] = row["id"]
        return ops

    # ---------- undo / redo (persistent) ----------
    @Slot()
    def undo_last_persistent(self):
        ops = self._build_op_state()
        # 最後に適用された（applied）操作を一つ選ぶ
        cand = None
        for op_id, st in ops.items():
            if st["state"] == "applied":
                if (cand is None) or (st["last_event_id"] > cand["last_event_id"]):
                    cand = {"op_id": op_id, **st}
        if not cand:
            QMessageBox.information(self, "取り消しなし", "取り消せる操作がありません。")
            return

        current = cand["current_path"]
        origin_src = Path(cand["origin_src"])
        if not current.exists():
            QMessageBox.critical(self, "取り消しエラー", f"現在の位置にファイルが見つかりません:\n{current}")
            return

        self._unload_player_current()
        target = self._finalize_dest(origin_src)

        ok, err = self._try_move_with_retry(current, target)
        if not ok:
            QMessageBox.critical(self, "取り消しエラー", f"操作を元に戻せませんでした:\n{err}")
            self.store.log("undo_error", {"op_id": cand["op_id"], "error": str(err)})
            return

        self._log_op("undo", cand["op_id"], current, target, cand["type"])

        # 再スキャンして該当ファイルにジャンプ
        self.load_files()
        self._goto_file(target)

        # 「キャラ名入力前」に明示的に戻す
        self.name_locked = False
        self.is_deleting = False
        self.name_edit.blockSignals(True)
        self.name_edit.clear()
        self.name_edit.blockSignals(False)
        self.prev_name_text = ""
        try:
            self.name_edit.setReadOnly(False)
            self.name_edit.setContextMenuPolicy(Qt.DefaultContextMenu)
        except Exception:
            pass
        self.ensure_focus()

    @Slot()
    def redo_last_persistent(self):
        ops = self._build_op_state()
        # 最後に取り消された（undone）操作を一つ選ぶ
        cand = None
        for op_id, st in ops.items():
            if st["state"] == "undone":
                if (cand is None) or (st["last_event_id"] > cand["last_event_id"]):
                    cand = {"op_id": op_id, **st}
        if not cand:
            QMessageBox.information(self, "やり直しなし", "やり直せる操作がありません。")
            return

        current = cand["current_path"]
        origin_dst = Path(cand["origin_dst"])
        if not current.exists():
            QMessageBox.critical(self, "やり直しエラー", f"現在の位置にファイルが見つかりません:\n{current}")
            return

        self._unload_player_current()
        target = self._finalize_dest(origin_dst)

        ok, err = self._try_move_with_retry(current, target)
        if not ok:
            QMessageBox.critical(self, "やり直しエラー", f"操作をやり直せませんでした:\n{err}")
            self.store.log("redo_error", {"op_id": cand["op_id"], "error": str(err)})
            return

        self._log_op("redo", cand["op_id"], current, target, cand["type"])

        # 再スキャンして該当ファイルにジャンプ（入力欄はいじらない＝ロック維持のまま）
        self.load_files()
        self._goto_file(target)

    # ---------- display ----------
    def show_current_file(self):
        # ロックや ReadOnly には触れない（各操作で明示的に管理）
        if 0 <= self.index < len(self.files):
            f = self.files[self.index]
            self.lbl_file.setText("現在: " + f.name)
            try:
                if self.player:
                    self.player.stop()
                    self.player.setSource(QUrl.fromLocalFile(str(f)))
            except Exception as e:
                self.store.log("player_set_source_failed", {"file": str(f), "error": str(e)})
        else:
            self.lbl_file.setText("完了！ファイルはありません。")
            try:
                if self.player:
                    self.player.stop()
                    self.player.setSource(QUrl())
            except Exception:
                pass
            # 明示的にリセット（シグナル発火を待たない）
            self.lbl_playback_time.setText("再生時間: 0:00.00 / 0:00.00")

    # ---------- keyboard / autocomplete ----------
    def eventFilter(self, obj, event):
        # QLineEdit の Ctrl+Z / Ctrl+Shift+Z / Ctrl+Y は先取りしてアプリの Undo/Redo
        if obj is self.name_edit and event.type() == QEvent.KeyPress:
            mods = event.modifiers()
            key  = event.key()
            if key == Qt.Key_Z and (mods & Qt.ControlModifier) and not (mods & Qt.ShiftModifier):
                self.undo_last_persistent(); return True
            if (key == Qt.Key_Z and (mods & Qt.ControlModifier) and (mods & Qt.ShiftModifier)) or \
               (key == Qt.Key_Y and (mods & Qt.ControlModifier)):
                self.redo_last_persistent(); return True

        # Del は常に「除外」
        if event.type() == QEvent.KeyPress and event.key() == Qt.Key_Delete and event.modifiers() == Qt.NoModifier:
            self.name_locked = False
            try: self.name_edit.setReadOnly(False)
            except Exception: pass
            self.exclude_current()
            return True

        if event.type() == QEvent.KeyPress:
            # Space 系: Space→再生, Ctrl+Space→空白, Shift+Space→後回し
            if event.key() == Qt.Key_Space:
                if event.modifiers() == Qt.ControlModifier:
                    if obj is self.name_edit: self.name_edit.insert(" "); return True
                if event.modifiers() == Qt.ShiftModifier:
                    self.defer_current(); return True
                if event.modifiers() == Qt.NoModifier:
                    self.toggle_play(); return True

            if obj is self.name_edit:
                # ロック中は入力を一律ブロック（Backspace だけ特別扱い）
                if self.name_locked:
                    # Backspace は「ロック解除＋一文字削除」を即時実行
                    if event.key() == Qt.Key_Backspace:
                        self.name_locked = False
                        self.is_deleting = True
                        try:
                            self.name_edit.setReadOnly(False)
                            self.name_edit.setContextMenuPolicy(Qt.DefaultContextMenu)
                        except Exception:
                            pass
                        txt = self.name_edit.text()
                        start = self.name_edit.selectionStart()
                        if start != -1:
                            sel = self.name_edit.selectedText()
                            new_txt = txt[:start] + txt[start + len(sel):]
                            self.name_edit.blockSignals(True); self.name_edit.setText(new_txt); self.name_edit.blockSignals(False)
                            self.name_edit.setCursorPosition(start); self.prev_name_text = new_txt
                        else:
                            cur = self.name_edit.cursorPosition()
                            if cur > 0:
                                new_txt = txt[:cur-1] + txt[cur:]
                                self.name_edit.blockSignals(True); self.name_edit.setText(new_txt); self.name_edit.blockSignals(False)
                                self.name_edit.setCursorPosition(cur-1); self.prev_name_text = new_txt
                        try:
                            if self.completer and self.completer.popup().isVisible(): self.completer.popup().hide()
                        except Exception:
                            pass
                        return True
                    # それ以外は消費（入力させない）
                    return True

                # ロックしていないときの貼り付けなどは通常通り

        # IME の確定もロック中は無効化
        if obj is self.name_edit and self.name_locked and event.type() == QEvent.InputMethod:
            return True

        # ドロップ/コンテキストメニューもロック中は無効化
        if obj is self.name_edit and self.name_locked and event.type() in (QEvent.DragEnter, QEvent.Drop, QEvent.ContextMenu):
            return True

        return super().eventFilter(obj, event)

    @Slot(str)
    def on_name_changed(self, text: str):
        t = (text or "").strip()
        is_deleting = (len(t) < len(self.prev_name_text)) or self.is_deleting

        if not t:
            self.name_locked = False
            try: self.name_edit.setReadOnly(False)
            except Exception: pass
            try: self.name_edit.setContextMenuPolicy(Qt.DefaultContextMenu)
            except Exception: pass
            self.prev_name_text = t; return

        # 既にロックされているときは（Backspace処理以外では）ここに来ない想定
        if self.name_locked:
            self.prev_name_text = t; return

        if is_deleting:
            try:
                if self.completer and self.completer.popup().isVisible():
                    self.completer.popup().hide()
            except Exception:
                pass
            self.prev_name_text = t; self.is_deleting = False; return

        matches = [n for n in self.names if t.lower() in n.lower()]
        if len(matches) == 1:
            m = matches[0]
            if m.lower().startswith(t.lower()):
                if m != text:
                    self.name_edit.blockSignals(True); self.name_edit.setText(m); self.name_edit.blockSignals(False)
                    self.name_edit.setCursorPosition(len(m))
                # ここでロック
                self.name_locked = True
                try:
                    if self.completer and self.completer.popup().isVisible():
                        self.completer.popup().hide()
                except Exception:
                    pass
                try: self.name_edit.setContextMenuPolicy(Qt.NoContextMenu)
                except Exception: pass
                try: self.name_edit.setReadOnly(True)
                except Exception: pass
                self.prev_name_text = t; self.ensure_focus(); return

        self.prev_name_text = t

# ---------- entry ----------
def main():
    app = QApplication(sys.argv)
    w = VoiceSorter(); w.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
