from gigamoney.runner import command_plan


def test_ioc_and_legacy_alias_select_the_remainder_cancel_path():
    for tif in ("IOC", "FOK"):
        plan = command_plan({"type": "LIMIT_ORDER_" + tif, "time_in_force": tif, "cancel_unfilled": True,
                             "symbol": "AAPL", "side": "SELL", "qty_shares": 100, "limit_price": 10.0})
        assert plan.arguments["kill"] is True
