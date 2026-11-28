import os
from pathlib import Path
from unittest.mock import patch

import pytest
from gigamoney.api_trading import ApiTrading, OrderJournal, OrderUncertain


def test_guard_rechecks_replaced_added_and_corrupt_files(tmp_path):
    journal = OrderJournal(tmp_path, "A")
    api = ApiTrading.__new__(ApiTrading)
    api.journal = journal
    original = {"account":"A","command_id":"first","sent_at":1,"phase":"finished",
                "request":{"client_request_id":"1234"}}
    journal.save("first", original)
    assert api._guard_submission(allocate_reference=True) == 1234
    with patch("builtins.open", side_effect=AssertionError("unchanged record reopened")):
        assert api._guard_submission(allocate_reference=True) == 1234
    path = journal.path("first")
    mtime = path.stat().st_mtime_ns
    other = OrderJournal(tmp_path, "A")
    other.save("first",dict(original,phase="dispatched"))
    os.utime(path,ns=(mtime,mtime))
    with pytest.raises(OrderUncertain,match="unresolved"):
        api._guard_submission()
    other.save("first",dict(original,phase="finished"))
    other.save("second",dict(original,command_id="second",request={"client_request_id":"9234"}))
    assert api._guard_submission(allocate_reference=True) == 9234
    path.write_text("{")
    with pytest.raises(OrderUncertain,match="unreadable"):
        api._guard_submission()


def test_other_account_and_preview_keep_original_scope(tmp_path):
    j=OrderJournal(tmp_path,"A")
    api=ApiTrading.__new__(ApiTrading);api.journal=j
    OrderJournal(tmp_path,"B").save("foreign",{"account":"B","command_id":"foreign","phase":"dispatched","sent_at":1})
    j.save("preview",{"account":"A","command_id":"preview","phase":"prepared"})
    assert api._guard_submission(allocate_reference=True)==0
