from __future__ import annotations

import pytest

from gkr_trading.persistence.db import open_sqlite


@pytest.fixture
def sqlite_conn(tmp_path):
    p = tmp_path / "t.db"
    conn = open_sqlite(str(p))
    yield conn
    conn.close()
