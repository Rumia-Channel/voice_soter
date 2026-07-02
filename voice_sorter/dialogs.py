# -*- coding: utf-8 -*-
"""各種設定・選択ダイアログ。"""

from __future__ import annotations

import shutil
from pathlib import Path

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QDialog, QDialogButtonBox, QHBoxLayout, QLabel, QLineEdit, QListWidget,
    QListWidgetItem, QMessageBox, QPushButton, QTextEdit, QVBoxLayout,
)

from voice_sorter.utils import normalize_exts, safe_key


class NamesEditor(QDialog):
    def __init__(self, names: list[str], parent=None):
        super().__init__(parent)
        self.setWindowTitle("キャラクター名を編集")
        self.setMinimumSize(480, 360)

        self.text = QTextEdit(self)
        self.text.setPlaceholderText(
            "1行に1つ、またはカンマ区切りで入力\n例)\nArlan\nAsta\nDan Heng")
        self.text.setText("\n".join(names))

        lay = QVBoxLayout(self)
        btns = QDialogButtonBox(QDialogButtonBox.Save |
                                QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        lay.addWidget(self.text)
        lay.addWidget(btns)

    def get_names(self) -> list[str]:
        raw = self.text.toPlainText()
        parts: list[str] = []
        for line in raw.splitlines():
            if "," in line:
                parts.extend(p.strip() for p in line.split(","))
            else:
                parts.append(line.strip())
        seen = set()
        out: list[str] = []
        for p in parts:
            if p and p not in seen:
                seen.add(p)
                out.append(p)
        return out


class ExtsEditor(QDialog):
    def __init__(self, audio_exts: list[str], extra_exts: list[str], parent=None):
        super().__init__(parent)
        self.setWindowTitle("拡張子を編集")
        self.setMinimumSize(480, 420)

        self.text_audio = QTextEdit(self)
        self.text_audio.setPlaceholderText(".wav\n.mp3\n.flac")
        self.text_audio.setText("\n".join(audio_exts))

        self.text_extra = QTextEdit(self)
        self.text_extra.setPlaceholderText(".lab\n.txt")
        self.text_extra.setText("\n".join(extra_exts))

        lay = QVBoxLayout(self)
        lay.addWidget(QLabel("音声として扱う拡張子（改行区切り）"))
        lay.addWidget(self.text_audio)
        lay.addWidget(QLabel("一緒に移動する拡張子（改行区切り）"))
        lay.addWidget(self.text_extra)

        btns = QDialogButtonBox(QDialogButtonBox.Save |
                                QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        lay.addWidget(btns)

    def get_exts(self) -> tuple[list[str], list[str]]:
        audio_raw = [l.strip()
                     for l in self.text_audio.toPlainText().splitlines()]
        extra_raw = [l.strip()
                     for l in self.text_extra.toPlainText().splitlines()]
        return normalize_exts(audio_raw), normalize_exts(extra_raw)


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
        self.new_edit = QLineEdit(self)
        self.new_edit.setPlaceholderText("例: star_rail_labeling")
        lay.addWidget(self.new_edit)

        btn_row = QHBoxLayout()
        self.btn_rename = QPushButton("リネーム")
        self.btn_delete = QPushButton("削除")
        btn_row.addWidget(self.btn_rename)
        btn_row.addWidget(self.btn_delete)
        lay.addLayout(btn_row)

        self.btn_rename.clicked.connect(self._rename_selected)
        self.btn_delete.clicked.connect(self._delete_selected)

        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        lay.addWidget(btns)

    def _refresh_list(self):
        self.listw.clear()
        for d in sorted([p.name for p in self.projects_dir.iterdir() if p.is_dir()]):
            self.listw.addItem(QListWidgetItem(d))

    def _rename_selected(self):
        cur = self.listw.currentItem()
        if not cur:
            QMessageBox.warning(self, "未選択", "リネームするプロジェクトを選択してください。")
            return
        old = cur.text()
        new_raw = self.new_edit.text().strip()
        new = safe_key(new_raw or "")
        if not new:
            QMessageBox.warning(self, "名称未入力", "新しいプロジェクト名を入力してください。")
            return
        if new == old:
            QMessageBox.information(self, "同一名", "同じ名前です。")
            return
        if (self.projects_dir / new).exists():
            QMessageBox.warning(self, "重複", f"既に存在します: {new}")
            return
        try:
            (self.projects_dir / old).rename(self.projects_dir / new)
            QMessageBox.information(self, "成功", f"{old} → {new} に変更しました。")
            self._refresh_list()
            items = self.listw.findItems(new, Qt.MatchExactly)
            if items:
                self.listw.setCurrentItem(items[0])
        except Exception as e:
            QMessageBox.critical(self, "エラー", f"リネームに失敗しました:\n{e}")

    def _delete_selected(self):
        cur = self.listw.currentItem()
        if not cur:
            QMessageBox.warning(self, "未選択", "削除するプロジェクトを選択してください。")
            return
        name = cur.text()
        ret = QMessageBox.question(
            self, "確認",
            f"プロジェクト「{name}」を**完全に削除**します。フォルダごと消えます。よろしいですか？",
            QMessageBox.Yes | QMessageBox.No, QMessageBox.No
        )
        if ret != QMessageBox.Yes:
            return
        try:
            shutil.rmtree(self.projects_dir / name, ignore_errors=False)
            QMessageBox.information(self, "削除完了", f"{name} を削除しました。")
            self._refresh_list()
        except Exception as e:
            QMessageBox.critical(self, "エラー", f"削除に失敗しました:\n{e}")

    def get_selection(self) -> tuple[str, bool]:
        name = self.new_edit.text().strip()
        if name:
            return safe_key(name), True
        cur = self.listw.currentItem()
        if cur:
            return cur.text(), False
        return "", False
