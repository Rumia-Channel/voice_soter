# -*- coding: utf-8 -*-
"""メインウィンドウとエントリポイント。"""

from __future__ import annotations

import re
import sys
import uuid
from pathlib import Path
from typing import Any

from PySide6.QtCore import QEvent, QSettings, QTimer, QUrl, Qt, Slot
from PySide6.QtGui import QAction, QKeySequence
from PySide6.QtMultimedia import QAudioOutput, QMediaPlayer
from PySide6.QtWidgets import (
    QApplication, QCheckBox, QDialog, QFileDialog, QFrame, QHBoxLayout,
    QLabel, QLineEdit, QListWidget, QListWidgetItem, QMainWindow, QMessageBox,
    QPushButton, QVBoxLayout, QWidget, QCompleter,
)

from voice_sorter import file_ops
from voice_sorter.constants import (
    APP_NAME, DB_NAME, DEFAULT_AUDIO_EXTS, DEFAULT_EXTRA_MOVE_EXTS,
    DEFER_DIR_NAME, EXCLUDE_DIR_NAME, ORG_NAME, PROJECTS_DIR,
    SETTINGS_KEY_AUDIO_EXTS, SETTINGS_KEY_EXTRA_EXTS,
)
from voice_sorter.dialogs import ExtsEditor, NamesEditor, ProjectDialog
from voice_sorter.store import Store
from voice_sorter.utils import app_data_dir, normalize_exts, safe_key


class VoiceSorter(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("音声ファイル分類補助ツール")
        self.setMinimumSize(960, 580)

        self._init_project()
        self._init_state()
        self._init_ui()
        self._init_player()
        self._init_shortcuts()
        self._load_persistent_settings()

    # ---------- initialization helpers ----------
    def _init_project(self):
        self.base_dir = app_data_dir()
        self.projects_dir = self.base_dir / PROJECTS_DIR
        self.qsettings = QSettings(ORG_NAME, APP_NAME)

        # 起動時に必ずプロジェクト選択
        self.project_key = self.ensure_project(force_prompt=True)
        self.project_dir = self.projects_dir / self.project_key
        self.store = Store(self.project_dir / DB_NAME)
        self.store.set_setting("project_key", self.project_key)

    def _init_state(self):
        self.recursive = (self.store.get_setting("recursive", "false") == "true")
        out = self.store.get_setting("last_output") or ""
        self.output_dir: Path | None = Path(out) if out else None
        if self.output_dir and not self.output_dir.exists():
            self.output_dir = None

        self.names = self.store.get_names()
        self.files: list[Path] = []
        self.index = -1

        self.audio_exts = self._load_audio_exts_from_store()
        self.extra_move_exts = self._load_extra_exts_from_store()

        self.name_locked = False
        self.prev_name_text = ""
        self.is_deleting = False

    def _init_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)

        # top bar
        top = QHBoxLayout()
        root.addLayout(top)
        self.btn_project = QPushButton(f"プロジェクトを選択…（現在: {self.project_key}）")
        top.addWidget(self.btn_project)
        self.btn_out = QPushButton("出力フォルダ…")
        top.addWidget(self.btn_out)
        self.btn_names = QPushButton("キャラ名を編集…")
        top.addWidget(self.btn_names)
        self.btn_exts = QPushButton("拡張子を編集…")
        top.addWidget(self.btn_exts)
        self.btn_project.clicked.connect(self.change_project)
        self.btn_out.clicked.connect(self.choose_output)
        self.btn_names.clicked.connect(self.edit_names)
        self.btn_exts.clicked.connect(self.edit_exts)

        # inputs area
        root.addWidget(self._sep())
        row = QHBoxLayout()
        root.addLayout(row)
        col_left = QVBoxLayout()
        row.addLayout(col_left, 3)
        col_right = QVBoxLayout()
        row.addLayout(col_right, 1)

        self.list_inputs = QListWidget()
        self.list_inputs.setSelectionMode(QListWidget.ExtendedSelection)
        self._inputs_itemchanged_connected = False
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
        # Ctrl+Space で必ず空白を挿入するためのショートカット
        self.act_insert_space = QAction(self)
        self.act_insert_space.setShortcut(QKeySequence("Ctrl+Space"))
        # この行が大事。子ウィジェットにも効く
        self.act_insert_space.setShortcutContext(Qt.WidgetWithChildrenShortcut)
        self.act_insert_space.triggered.connect(self.insert_space_into_name)
        # name_edit にぶら下げると、他のWidgetより優先して取れる
        self.name_edit.addAction(self.act_insert_space)
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

    def _init_player(self):
        self.player: QMediaPlayer | None = None
        try:
            self.audio = QAudioOutput(self)
            self.player = QMediaPlayer(self)
            self.player.setAudioOutput(self.audio)
            self.player.positionChanged.connect(self.update_playback_time)
            self.player.durationChanged.connect(self.update_playback_time)
        except Exception as e:
            self.store.log("player_init_failed", {"error": str(e)})
            self.player = None

    def _init_shortcuts(self):
        # shortcuts (application scope)
        self.act_play = QAction(self)
        self.act_play.setShortcut(QKeySequence(Qt.Key_Space))
        self.act_play.setShortcutContext(Qt.ApplicationShortcut)
        self.act_play.triggered.connect(self.toggle_play)
        self.addAction(self.act_play)

        self.act_enter1 = QAction(self)
        self.act_enter1.setShortcut(QKeySequence(Qt.Key_Return))
        self.act_enter1.setShortcutContext(Qt.ApplicationShortcut)
        self.act_enter1.triggered.connect(self.confirm_and_move)
        self.addAction(self.act_enter1)

        self.act_enter2 = QAction(self)
        self.act_enter2.setShortcut(QKeySequence(Qt.Key_Enter))
        self.act_enter2.setShortcutContext(Qt.ApplicationShortcut)
        self.act_enter2.triggered.connect(self.confirm_and_move)
        self.addAction(self.act_enter2)

        self.act_del = QAction(self)
        self.act_del.setShortcut(QKeySequence(Qt.Key_Delete))
        self.act_del.setShortcutContext(Qt.ApplicationShortcut)
        self.act_del.triggered.connect(self.exclude_current)
        self.addAction(self.act_del)

        # Undo / Redo（永続）
        self.act_undo = QAction(self)
        self.act_undo.setShortcut(QKeySequence.Undo)  # Ctrl+Z / ⌘Z
        self.act_undo.setShortcutContext(Qt.ApplicationShortcut)
        self.act_undo.triggered.connect(self.undo_last_persistent)
        self.addAction(self.act_undo)

        self.act_redo = QAction(self)
        # Ctrl+Shift+Z / ⌘⇧Z / Ctrl+Y
        self.act_redo.setShortcuts([QKeySequence.Redo, QKeySequence("Ctrl+Y")])
        self.act_redo.setShortcutContext(Qt.ApplicationShortcut)
        self.act_redo.triggered.connect(self.redo_last_persistent)
        self.addAction(self.act_redo)

    def _load_persistent_settings(self):
        self.refresh_inputs_view()
        # チェックボックスの初期値を設定（全UIが作成された後）
        self.chk_recursive.setChecked(self.recursive)
        if self.output_dir:
            self.load_files()
        self.ensure_focus()

    # ---------- helpers ----------
    def _sep(self):
        line = QFrame()
        line.setFrameShape(QFrame.HLine)
        line.setFrameShadow(QFrame.Sunken)
        return line

    def ensure_focus(self):
        # フォーカスだけ当てる（ロック・ReadOnlyは一切いじらない）
        QTimer.singleShot(0, lambda: (self.name_edit.setFocus(),
                                      self.name_edit.setCursorPosition(len(self.name_edit.text()))))

    def update_completer(self):
        self.model.setStringList(self.names)

    def _split_ext_text(self, raw: str) -> list[str]:
        """
        古い設定で「.wav, .mp3」みたいに1行カンマ区切りで保存されてても
        ここで改行扱いに直す。改行 / カンマ / セミコロンを区切りにする。
        """
        if not raw:
            return []
        parts = re.split(r"[,;\n\r]+", raw)
        return [p.strip() for p in parts if p.strip()]

    def _load_audio_exts_from_store(self) -> list[str]:
        """
        プロジェクト設定から「音声として扱う拡張子」を読む。
        旧フォーマットでもここで正規化するのでDBは触らなくていい。
        """
        raw = self.store.get_setting(SETTINGS_KEY_AUDIO_EXTS, "")
        if raw:
            items = self._split_ext_text(raw)
            norm = normalize_exts(items)
            return norm or normalize_exts(DEFAULT_AUDIO_EXTS)
        return normalize_exts(DEFAULT_AUDIO_EXTS)

    def _load_extra_exts_from_store(self) -> list[str]:
        """
        一緒に動かす拡張子を読む。
        空ならデフォルト(.lab)だけにしておく。
        """
        raw = self.store.get_setting(SETTINGS_KEY_EXTRA_EXTS, "")
        if raw:
            items = self._split_ext_text(raw)
            return normalize_exts(items)
        return normalize_exts(DEFAULT_EXTRA_MOVE_EXTS)

    def _save_exts_to_store(self):
        self.store.set_setting(SETTINGS_KEY_AUDIO_EXTS,
                               "\n".join(self.audio_exts))
        self.store.set_setting(SETTINGS_KEY_EXTRA_EXTS,
                               "\n".join(self.extra_move_exts))

    def update_status(self):
        total = len(self.files)
        pos = self.index + 1 if self.index >= 0 else 0
        enabled_cnt = sum(
            1 for _, e, d in self.store.list_inputs() if e and not d)

        base = f"{pos}/{total} 件 | プロジェクト:{self.project_key} | 有効入力:{enabled_cnt}"
        base += " | 再帰:ON" if self.recursive else " | 再帰:OFF"
        if self.output_dir:
            base += f" | 出力:{self.output_dir}"
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
    def ensure_project(self, force_prompt: bool = False) -> str:
        last = self.qsettings.value("last_project", "", str)
        if (not force_prompt) and last and (self.projects_dir / last).exists():
            return last
        dlg = ProjectDialog(self.projects_dir, self)
        if dlg.exec() == QDialog.Accepted:
            key, _ = dlg.get_selection()
            if not key:
                QMessageBox.warning(self, "未選択", "プロジェクトを選ぶか、新規名称を入力してください。")
                return self.ensure_project(force_prompt=True)
            (self.projects_dir / key).mkdir(parents=True, exist_ok=True)
            self.qsettings.setValue("last_project", key)
            return key
        key = "default"
        (self.projects_dir / key).mkdir(parents=True, exist_ok=True)
        self.qsettings.setValue("last_project", key)
        return key

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
    def edit_exts(self):
        dlg = ExtsEditor(self.audio_exts, self.extra_move_exts, self)
        if dlg.exec() == QDialog.Accepted:
            audio, extra = dlg.get_exts()
            if not audio:
                QMessageBox.warning(self, "無効な設定", "音声として扱う拡張子が空です。")
                return
            self.audio_exts = audio
            self.extra_move_exts = extra
            self._save_exts_to_store()
            self.load_files()
            self.ensure_focus()

    @Slot()
    def insert_space_into_name(self):
        """
        Ctrl+Space でここに来る。
        name_locked のときは元仕様どおり入力ブロックなので何もしない。
        """
        if self.name_locked:
            return
        self.name_edit.insert(" ")
        self.ensure_focus()

    @Slot()
    def change_project(self):
        key = self.ensure_project(force_prompt=True)
        if key == self.project_key:
            return
        self.project_key = key
        self.project_dir = self.projects_dir / key
        self.store = Store(self.project_dir / DB_NAME)
        self.store.set_setting("project_key", self.project_key)

        self.recursive = (self.store.get_setting("recursive", "false") == "true")
        self.chk_recursive.setChecked(self.recursive)

        out = self.store.get_setting("last_output") or ""
        self.output_dir = Path(out) if out else None
        if self.output_dir and not self.output_dir.exists():
            self.output_dir = None

        self.names = self.store.get_names()
        self.update_completer()
        self.audio_exts = self._load_audio_exts_from_store()
        self.extra_move_exts = self._load_extra_exts_from_store()
        self.btn_project.setText(f"プロジェクトを選択…（現在: {self.project_key}）")

        self.refresh_inputs_view()
        self.load_files()
        self.ensure_focus()

    # ---------- inputs CRUD ----------
    def refresh_inputs_view(self):
        """
        入力フォルダ一覧を描き直す。
        シグナルを止めてからアイテムを全部入れ直す。
        itemChanged の接続は初回だけにするので disconnect はしない。
        """
        self.list_inputs.blockSignals(True)
        self.list_inputs.clear()

        for path, enabled, done in self.store.list_inputs():
            it = QListWidgetItem(path)
            it.setFlags(it.flags() | Qt.ItemIsUserCheckable | Qt.ItemIsSelectable | Qt.ItemIsEnabled)
            it.setCheckState(Qt.Checked if enabled else Qt.Unchecked)
            if done:
                it.setForeground(Qt.gray)
            self.list_inputs.addItem(it)

        self.list_inputs.blockSignals(False)

        if not self._inputs_itemchanged_connected:
            self.list_inputs.itemChanged.connect(self._on_input_item_changed)
            self._inputs_itemchanged_connected = True

        self.update_status()

    def _on_input_item_changed(self, item: QListWidgetItem):
        p = Path(item.text())
        enabled = (item.checkState() == Qt.Checked)
        self.store.set_enabled(p, enabled)
        self.load_files()

    @Slot()
    def add_input(self):
        d = QFileDialog.getExistingDirectory(self, "入力フォルダを追加")
        if d:
            self.store.upsert_input(Path(d), enabled=True, done=False)
            self.refresh_inputs_view()
            self.load_files()
            self.ensure_focus()

    @Slot()
    def remove_inputs(self):
        rows = sorted({i.row()
                      for i in self.list_inputs.selectedIndexes()}, reverse=True)
        for r in rows:
            it = self.list_inputs.item(r)
            if it:
                self.store.remove_input(Path(it.text()))
        self.refresh_inputs_view()
        self.load_files()
        self.ensure_focus()

    @Slot()
    def toggle_done(self):
        for i in self.list_inputs.selectedIndexes():
            it = self.list_inputs.item(i.row())
            p = Path(it.text())
            cur = next(((Path(path), e, d) for path, e,
                       d in self.store.list_inputs() if path == it.text()), None)
            if cur:
                _, e, d = cur
                self.store.set_done(p, not bool(d))
        self.refresh_inputs_view()
        self.load_files()
        self.ensure_focus()

    @Slot(int)
    def set_recursive(self, state: int):
        self.recursive = (state == Qt.CheckState.Checked.value) or (state == 2)
        self.store.set_setting(
            "recursive", "true" if self.recursive else "false")
        self.load_files()
        self.ensure_focus()

    # ---------- output ----------
    @Slot()
    def choose_output(self):
        d = QFileDialog.getExistingDirectory(self, "出力フォルダを選択")
        if d:
            self.output_dir = Path(d)
            self.store.set_setting("last_output", str(self.output_dir))
            self.update_status()
            self.ensure_focus()

    # ---------- player handle helpers ----------
    def _unload_player_current(self):
        try:
            if self.player:
                self.player.stop()
        except Exception:
            pass
        try:
            if self.player:
                self.player.setSource(QUrl())
        except Exception:
            pass
        QApplication.processEvents()

    # ---------- scanning ----------
    def load_files(self):
        inputs = [(Path(p), bool(e), bool(d))
                  for p, e, d in self.store.list_inputs()]
        files: list[Path] = []

        def add_from_dir(d: Path):
            if not d.exists():
                return
            if d.name in (EXCLUDE_DIR_NAME, DEFER_DIR_NAME):
                return
            if self.recursive:
                for p in sorted(d.rglob("*")):
                    if any(part in (EXCLUDE_DIR_NAME, DEFER_DIR_NAME) for part in p.parts):
                        continue
                    if p.is_file() and p.suffix.lower() in self.audio_exts:
                        files.append(p)
            else:
                for p in sorted(d.iterdir()):
                    if p.is_dir() and p.name in (EXCLUDE_DIR_NAME, DEFER_DIR_NAME):
                        continue
                    if p.is_file() and p.suffix.lower() in self.audio_exts:
                        files.append(p)

        for d, enabled, done in inputs:
            if enabled and not done:
                add_from_dir(Path(d))

        seen = set()
        uniq = []
        for p in files:
            sp = str(p)
            if sp not in seen:
                seen.add(sp)
                uniq.append(p)

        self.files = uniq
        self.index = 0 if self.files else -1
        self.update_status()

        if not self.files:
            if self.restore_deferred_if_any():
                return self.load_files()
        self.show_current_file()

    def restore_deferred_if_any(self) -> bool:
        restored = False
        for path, enabled, done in self.store.list_inputs():
            if not enabled or done:
                continue
            base = Path(path)
            if not base.exists():
                continue
            targets = list(base.rglob(DEFER_DIR_NAME)) if self.recursive else [
                base / DEFER_DIR_NAME]
            for d in targets:
                if not d.exists() or not d.is_dir():
                    continue
                for f in sorted(d.iterdir()):
                    if f.is_file():
                        dest = d.parent / f.name
                        if dest.exists():
                            dest = file_ops.finalize_dest(dest)
                        ok, err = file_ops.try_move_with_retry(f, dest)
                        if ok:
                            restored = True
                        else:
                            self.store.log("restore_deferred_error", {
                                           "src": str(f), "error": str(err)})
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
        if self.player.source().isEmpty():
            self.lbl_playback_time.setText("再生時間: 0:00.00 / 0:00.00")
            return
        pos = self.player.position()
        dur = self.player.duration()
        pos_str = self._format_time(pos) if pos >= 0 else "0:00.00"
        dur_str = self._format_time(dur) if dur > 0 else "0:00.00"
        self.lbl_playback_time.setText(f"再生時間: {pos_str} / {dur_str}")

    @Slot()
    def toggle_play(self):
        if not self.player:
            QMessageBox.information(self, "再生不可", "再生バックエンドが利用できません。")
            return
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

    def _log_op(self, action: str, op_id: str, src: Path, dst: Path, typ: str, extras: list[dict[str, str]] | None = None):
        self.store.log(action, {
            "op_id": op_id,
            "type": typ,
            "from": str(src),
            "to": str(dst),
            "extras": extras or []
        })

    def _new_op_id(self) -> str:
        return uuid.uuid4().hex

    def _log_error(self, tag: str, data: dict[str, Any]):
        self.store.log(tag, data)

    def _reset_name_input(self):
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

    def _after_classify_step(self, reset_name: bool = False) -> None:
        """move/exclude/defer 後の共通後処理。"""
        del self.files[self.index]
        if self.index >= len(self.files):
            self.index = len(self.files) - 1
        if reset_name:
            self._reset_name_input()
        self.update_status()
        if self.index < 0 and not self.files:
            if self.restore_deferred_if_any():
                return self.load_files()
        self.show_current_file()
        self.ensure_focus()

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
            self.ensure_focus()
            return
        if not self.output_dir:
            QMessageBox.warning(self, "出力未指定", "出力フォルダを選択してください。")
            self.ensure_focus()
            return
        if not (0 <= self.index < len(self.files)):
            self.ensure_focus()
            return

        src = self.files[self.index]
        safe = self._safe_folder_name(name)
        dest_dir = self.output_dir / safe
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = file_ops.finalize_dest(dest_dir / src.name)

        self._unload_player_current()
        ok, err, extras_moved = file_ops.move_main_and_extras(
            src, dest, self.extra_move_exts, log_error=self._log_error)
        if not ok:
            QMessageBox.critical(self, "移動エラー", f"ファイルを移動できませんでした:\n{err}")
            self.ensure_focus()
            return

        op_id = self._new_op_id()
        self._log_op("move", op_id, src, dest, "move", extras=extras_moved)
        self.store.audit("move", src=str(src), dst=str(dest), character=name, folder=safe)

        self._after_classify_step(reset_name=True)

    @Slot()
    def exclude_current(self):
        if not (0 <= self.index < len(self.files)):
            self.ensure_focus()
            return
        src = self.files[self.index]
        excl_dir = src.parent / EXCLUDE_DIR_NAME
        excl_dir.mkdir(exist_ok=True)
        dest = file_ops.finalize_dest(excl_dir / src.name)

        self._unload_player_current()
        ok, err, extras_moved = file_ops.move_main_and_extras(
            src, dest, self.extra_move_exts, log_error=self._log_error)
        if not ok:
            QMessageBox.critical(self, "除外エラー", f"ファイルを除外できませんでした:\n{err}")
            self.ensure_focus()
            return

        op_id = self._new_op_id()
        self._log_op("exclude", op_id, src, dest, "exclude", extras=extras_moved)
        self.store.audit("exclude", src=str(src), dst=str(dest))

        self._after_classify_step()

    def defer_current(self):
        if not (0 <= self.index < len(self.files)):
            self.ensure_focus()
            return
        src = self.files[self.index]
        dfr_dir = src.parent / DEFER_DIR_NAME
        dfr_dir.mkdir(exist_ok=True)
        dest = file_ops.finalize_dest(dfr_dir / src.name)

        self._unload_player_current()
        ok, err, extras_moved = file_ops.move_main_and_extras(
            src, dest, self.extra_move_exts, log_error=self._log_error)
        if not ok:
            QMessageBox.critical(self, "後回しエラー", f"ファイルを後回しにできませんでした:\n{err}")
            self.ensure_focus()
            return

        op_id = self._new_op_id()
        self._log_op("defer", op_id, src, dest, "defer", extras=extras_moved)
        self.store.audit("defer", src=str(src), dst=str(dest))

        self._after_classify_step()

    # ---------- persistent undo/redo helpers ----------
    def _build_op_state(self) -> dict[str, dict[str, Any]]:
        ops: dict[str, dict[str, Any]] = {}
        rows = self.store.fetch_history()
        for row in rows:
            act = row["action"]
            p = row["payload"] or {}
            op_id = p.get("op_id")
            if not op_id:
                continue

            if act in ("move", "exclude", "defer"):
                extras = p.get("extras", [])
                ops.setdefault(op_id, {
                    "type": p.get("type", act),
                    "origin_src": Path(p.get("from", "")),
                    "origin_dst": Path(p.get("to", "")),
                    "state": "applied",
                    "current_path": Path(p.get("to", "")),
                    "extras": extras,
                    "current_extras": [Path(e.get("to", "")) for e in extras],
                    "last_event_id": row["id"],
                })
                ops[op_id]["state"] = "applied"
                ops[op_id]["current_path"] = Path(p.get("to", ""))
                ops[op_id]["current_extras"] = [
                    Path(e.get("to", "")) for e in extras]
                ops[op_id]["last_event_id"] = row["id"]

            elif act == "undo":
                extras = p.get("extras", [])
                if op_id in ops:
                    ops[op_id]["state"] = "undone"
                    ops[op_id]["current_path"] = Path(
                        p.get("to", p.get("from", "")))
                    if extras:
                        ops[op_id]["current_extras"] = [
                            Path(e.get("to", "")) for e in extras]
                    else:
                        ops[op_id]["current_extras"] = [
                            Path(e.get("from", "")) for e in ops[op_id].get("extras", [])]
                    ops[op_id]["last_event_id"] = row["id"]

            elif act == "redo":
                extras = p.get("extras", [])
                if op_id in ops:
                    ops[op_id]["state"] = "applied"
                    ops[op_id]["current_path"] = Path(
                        p.get("to", p.get("from", "")))
                    if extras:
                        ops[op_id]["current_extras"] = [
                            Path(e.get("to", "")) for e in extras]
                    else:
                        ops[op_id]["current_extras"] = [
                            Path(e.get("to", "")) for e in ops[op_id].get("extras", [])]
                    ops[op_id]["last_event_id"] = row["id"]
        return ops

    # ---------- undo / redo (persistent) ----------
    @Slot()
    def undo_last_persistent(self):
        ops = self._build_op_state()
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
            QMessageBox.critical(
                self, "取り消しエラー", f"現在の位置にファイルが見つかりません:\n{current}")
            return

        self._unload_player_current()
        target = file_ops.finalize_dest(origin_src)

        ok, err = file_ops.try_move_with_retry(current, target)
        if not ok:
            QMessageBox.critical(self, "取り消しエラー", f"操作を元に戻せませんでした:\n{err}")
            self.store.log(
                "undo_error", {"op_id": cand["op_id"], "error": str(err)})
            return

        extras_log: list[dict[str, str]] = []
        for idx, e in enumerate(cand.get("extras", [])):
            if idx < len(cand.get("current_extras", [])):
                ex_current = cand["current_extras"][idx]
            else:
                ex_current = Path(e.get("to", ""))
            ex_origin = Path(e.get("from", ""))
            if not ex_current:
                continue
            if ex_current.exists():
                ex_target = file_ops.finalize_dest(ex_origin)
                ok2, err2 = file_ops.try_move_with_retry(ex_current, ex_target)
                if ok2:
                    extras_log.append(
                        {"from": str(ex_current), "to": str(ex_target)})
                else:
                    self.store.log("undo_extra_error", {
                                   "file": str(ex_current), "error": str(err2)})

        self._log_op("undo", cand["op_id"], current,
                     target, cand["type"], extras=extras_log)

        self.load_files()
        self._goto_file(target)
        self._reset_name_input()
        self.ensure_focus()

    @Slot()
    def redo_last_persistent(self):
        ops = self._build_op_state()
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
            QMessageBox.critical(
                self, "やり直しエラー", f"現在の位置にファイルが見つかりません:\n{current}")
            return

        self._unload_player_current()
        target = file_ops.finalize_dest(origin_dst)

        ok, err = file_ops.try_move_with_retry(current, target)
        if not ok:
            QMessageBox.critical(self, "やり直しエラー", f"操作をやり直せませんでした:\n{err}")
            self.store.log(
                "redo_error", {"op_id": cand["op_id"], "error": str(err)})
            return

        extras_log: list[dict[str, str]] = []
        for idx, e in enumerate(cand.get("extras", [])):
            if idx < len(cand.get("current_extras", [])):
                ex_current = cand["current_extras"][idx]
            else:
                ex_current = Path(e.get("from", ""))
            ex_dst = Path(e.get("to", ""))
            if not ex_current:
                continue
            if ex_current.exists():
                ex_target = file_ops.finalize_dest(target.parent / ex_dst.name)
                ok2, err2 = file_ops.try_move_with_retry(ex_current, ex_target)
                if ok2:
                    extras_log.append(
                        {"from": str(ex_current), "to": str(ex_target)})
                else:
                    self.store.log("redo_extra_error", {
                                   "file": str(ex_current), "error": str(err2)})

        self._log_op("redo", cand["op_id"], current,
                     target, cand["type"], extras=extras_log)

        self.load_files()
        self._goto_file(target)

    # ---------- display ----------
    def show_current_file(self):
        if 0 <= self.index < len(self.files):
            f = self.files[self.index]
            self.lbl_file.setText("現在: " + f.name)
            try:
                if self.player:
                    self.player.stop()
                    self.player.setSource(QUrl.fromLocalFile(str(f)))
            except Exception as e:
                self.store.log("player_set_source_failed", {
                               "file": str(f), "error": str(e)})
        else:
            self.lbl_file.setText("完了！ファイルはありません。")
            try:
                if self.player:
                    self.player.stop()
                    self.player.setSource(QUrl())
            except Exception:
                pass
            self.lbl_playback_time.setText("再生時間: 0:00.00 / 0:00.00")

    # ---------- keyboard / autocomplete ----------
    def eventFilter(self, obj, event):
        # QLineEdit の Ctrl+Z / Ctrl+Shift+Z / Ctrl+Y は先取りしてアプリの Undo/Redo
        if obj is self.name_edit and event.type() == QEvent.KeyPress:
            mods = event.modifiers()
            key = event.key()
            if key == Qt.Key_Z and (mods & Qt.ControlModifier) and not (mods & Qt.ShiftModifier):
                self.undo_last_persistent()
                return True
            if (key == Qt.Key_Z and (mods & Qt.ControlModifier) and (mods & Qt.ShiftModifier)) or \
               (key == Qt.Key_Y and (mods & Qt.ControlModifier)):
                self.redo_last_persistent()
                return True

        # Del は常に「除外」
        if event.type() == QEvent.KeyPress and event.key() == Qt.Key_Delete and event.modifiers() == Qt.NoModifier:
            self.name_locked = False
            try:
                self.name_edit.setReadOnly(False)
            except Exception:
                pass
            self.exclude_current()
            return True

        if event.type() == QEvent.KeyPress:
            # Space 系: Space→再生, Ctrl+Space→空白, Shift+Space→後回し
            if event.key() == Qt.Key_Space:
                if event.modifiers() == Qt.ControlModifier:
                    if obj is self.name_edit:
                        self.name_edit.insert(" ")
                        return True
                if event.modifiers() == Qt.ShiftModifier:
                    self.defer_current()
                    return True
                if event.modifiers() == Qt.NoModifier:
                    self.toggle_play()
                    return True

            if obj is self.name_edit:
                # ロック中は入力を一律ブロック（Backspace だけ特別扱い）
                if self.name_locked:
                    # Backspace は「ロック解除＋一文字削除」を即時実行
                    if event.key() == Qt.Key_Backspace:
                        self.name_locked = False
                        self.is_deleting = True
                        try:
                            self.name_edit.setReadOnly(False)
                            self.name_edit.setContextMenuPolicy(
                                Qt.DefaultContextMenu)
                        except Exception:
                            pass
                        txt = self.name_edit.text()
                        start = self.name_edit.selectionStart()
                        if start != -1:
                            sel = self.name_edit.selectedText()
                            new_txt = txt[:start] + txt[start + len(sel):]
                            self.name_edit.blockSignals(True)
                            self.name_edit.setText(new_txt)
                            self.name_edit.blockSignals(False)
                            self.name_edit.setCursorPosition(start)
                            self.prev_name_text = new_txt
                        else:
                            cur = self.name_edit.cursorPosition()
                            if cur > 0:
                                new_txt = txt[:cur-1] + txt[cur:]
                                self.name_edit.blockSignals(True)
                                self.name_edit.setText(new_txt)
                                self.name_edit.blockSignals(False)
                                self.name_edit.setCursorPosition(cur-1)
                                self.prev_name_text = new_txt
                        try:
                            if self.completer and self.completer.popup().isVisible():
                                self.completer.popup().hide()
                        except Exception:
                            pass
                        return True
                    # それ以外は消費（入力させない）
                    return True

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
            try:
                self.name_edit.setReadOnly(False)
            except Exception:
                pass
            try:
                self.name_edit.setContextMenuPolicy(Qt.DefaultContextMenu)
            except Exception:
                pass
            self.prev_name_text = t
            return

        if self.name_locked:
            self.prev_name_text = t
            return

        if is_deleting:
            try:
                if self.completer and self.completer.popup().isVisible():
                    self.completer.popup().hide()
            except Exception:
                pass
            self.prev_name_text = t
            self.is_deleting = False
            return

        matches = [n for n in self.names if t.lower() in n.lower()]
        if len(matches) == 1:
            m = matches[0]
            if m.lower().startswith(t.lower()):
                if m != text:
                    self.name_edit.blockSignals(True)
                    self.name_edit.setText(m)
                    self.name_edit.blockSignals(False)
                    self.name_edit.setCursorPosition(len(m))
                self.name_locked = True
                try:
                    if self.completer and self.completer.popup().isVisible():
                        self.completer.popup().hide()
                except Exception:
                    pass
                try:
                    self.name_edit.setContextMenuPolicy(Qt.NoContextMenu)
                except Exception:
                    pass
                try:
                    self.name_edit.setReadOnly(True)
                except Exception:
                    pass
                self.prev_name_text = t
                self.ensure_focus()
                return

        self.prev_name_text = t


def main():
    app = QApplication(sys.argv)
    w = VoiceSorter()
    w.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
