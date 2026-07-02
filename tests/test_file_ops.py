# -*- coding: utf-8 -*-
from pathlib import Path

import pytest

from voice_sorter import file_ops
from voice_sorter.utils import normalize_exts, safe_key


def test_normalize_exts():
    assert normalize_exts(["wav", ".MP3", " flac ", ".ogg", "wav"]) == [
        ".wav", ".mp3", ".flac", ".ogg"]
    assert normalize_exts(["", "  "]) == []


def test_safe_key():
    assert safe_key("  Hello World  ") == "Hello_World"
    assert safe_key('a/b:c*d?e"f<g>|') == "a_b_c_d_e_f_g__"
    assert safe_key("") == "Unnamed"
    assert safe_key(".") == "Unnamed"


def test_finalize_dest_collision(tmp_path):
    base = tmp_path / "file.txt"
    base.write_text("x")
    assert file_ops.finalize_dest(base) == tmp_path / "file (1).txt"
    (tmp_path / "file (1).txt").write_text("x")
    assert file_ops.finalize_dest(base) == tmp_path / "file (2).txt"


def test_finalize_dest_no_collision(tmp_path):
    dest = tmp_path / "new.txt"
    assert file_ops.finalize_dest(dest) == dest


def test_collect_extra_siblings(tmp_path):
    main = tmp_path / "001.wav"
    main.write_text("audio")
    (tmp_path / "001.lab").write_text("label")
    (tmp_path / "001.txt").write_text("text")
    (tmp_path / "002.lab").write_text("other")

    extras = file_ops.collect_extra_siblings(main, [".lab", ".txt"])
    assert sorted(extras) == sorted([tmp_path / "001.lab", tmp_path / "001.txt"])


def test_move_main_and_extras(tmp_path):
    src_dir = tmp_path / "src"
    dst_dir = tmp_path / "dst"
    src_dir.mkdir()
    dst_dir.mkdir()

    main = src_dir / "001.wav"
    main.write_text("audio")
    (src_dir / "001.lab").write_text("label")
    (src_dir / "001.txt").write_text("text")

    ok, err, extras = file_ops.move_main_and_extras(
        main, dst_dir / "001.wav", [".lab"])

    assert ok is True
    assert err is None
    assert len(extras) == 1
    assert extras[0]["from"] == str(src_dir / "001.lab")
    assert extras[0]["to"] == str(dst_dir / "001.lab")
    assert (dst_dir / "001.wav").exists()
    assert (dst_dir / "001.lab").exists()
    assert not (src_dir / "001.wav").exists()
    assert not (src_dir / "001.lab").exists()
    # 設定外の extra は動かさない
    assert (src_dir / "001.txt").exists()


def test_move_main_and_extras_calls_error_logger(tmp_path, monkeypatch):
    src_dir = tmp_path / "src"
    dst_dir = tmp_path / "dst"
    src_dir.mkdir()
    dst_dir.mkdir()

    main = src_dir / "001.wav"
    main.write_text("audio")
    (src_dir / "001.lab").write_text("label")

    errors = []

    def log_error(tag, data):
        errors.append((tag, data))

    # 通常の try_move_with_retry を退避しつつ、.lab のみ失敗させる
    original_try_move = file_ops.try_move_with_retry

    def fake_try_move(src, dest, tries=10, wait_sec=0.05):
        if Path(src).suffix == ".lab":
            return False, Exception("locked")
        return original_try_move(src, dest, tries=tries, wait_sec=wait_sec)

    monkeypatch.setattr(file_ops, "try_move_with_retry", fake_try_move)

    ok, err, extras = file_ops.move_main_and_extras(
        main, dst_dir / "001.wav", [".lab"], log_error=log_error)

    assert ok is True
    assert err is None
    assert extras == []
    assert len(errors) == 1
    assert errors[0][0] == "extra_move_failed"
    assert errors[0][1]["src"] == str(src_dir / "001.lab")


@pytest.mark.parametrize("name,expected", [
    ("Alice", "Alice"),
    ("Bob Smith", "Bob_Smith"),
    ("a/b", "a_b"),
])
def test_safe_key_param(name, expected):
    assert safe_key(name) == expected
