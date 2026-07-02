# -*- coding: utf-8 -*-
from pathlib import Path

import pytest

from voice_sorter.store import Store


@pytest.fixture
def store(tmp_path):
    return Store(tmp_path / "test.sqlite3")


def test_setting_roundtrip(store):
    assert store.get_setting("foo") is None
    assert store.get_setting("foo", "default") == "default"
    store.set_setting("foo", "bar")
    assert store.get_setting("foo") == "bar"
    store.set_setting("foo", "baz")
    assert store.get_setting("foo") == "baz"


def test_names_deduplication_and_order(store):
    store.set_names(["Charlie", "alice", "Bob", "alice", ""])
    names = store.get_names()
    # COLLATE NOCASE ソート
    assert names == ["alice", "Bob", "Charlie"]


def test_input_crud(store):
    store.upsert_input(Path("a"), enabled=True, done=False)
    store.upsert_input(Path("b"), enabled=False, done=True)

    inputs = store.list_inputs()
    assert len(inputs) == 2

    a = next(i for i in inputs if i[0] == "a")
    assert a[1] == 1
    assert a[2] == 0

    store.set_enabled(Path("a"), False)
    store.set_done(Path("a"), True)
    a = next(i for i in store.list_inputs() if i[0] == "a")
    assert a[1] == 0
    assert a[2] == 1

    store.remove_input(Path("a"))
    assert len(store.list_inputs()) == 1


def test_history_and_audit(store):
    store.log("move", {"op_id": "x", "from": "/a", "to": "/b"})
    history = store.fetch_history()
    assert len(history) == 1
    assert history[0]["action"] == "move"
    assert history[0]["payload"]["op_id"] == "x"

    store.audit("move", src="/a", dst="/b", character="A", folder="A")
    rows = store.conn.execute("SELECT op, src, dst, character, folder FROM audit").fetchall()
    assert rows == [("move", "/a", "/b", "A", "A")]
