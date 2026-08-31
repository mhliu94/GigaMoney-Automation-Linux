import unittest

from gigamoney.holdings import cash_data, portfolio_bottom_from_xml, positions_from_xml
from gigamoney.ui import parse_xml


class HoldingsTests(unittest.TestCase):
    def test_position_resource_mapping_ignores_value_volatility(self):
        xml = parse_xml("""<hierarchy>
          <node resource-id="p:id/wealth_stock_hold_header" text="" bounds="[0,400][1080,500]">
            <node resource-id="p:id/tv_market_title" text="US" bounds="[0,400][200,500]" />
          </node>
          <node resource-id="p:id/wealth_stock_hold_item_view" text="" bounds="[0,500][1080,700]">
            <node resource-id="p:id/tv_stock_code" text="BILI" bounds="[20,550][200,590]" />
            <node resource-id="p:id/tv_stock_name" text="Bilibili" bounds="[20,510][300,550]" />
            <node resource-id="p:id/tv_column_00" text="$231.25" bounds="[390,510][625,550]" />
            <node resource-id="p:id/tv_column_01" text="12.5" bounds="[390,550][625,590]" />
            <node resource-id="p:id/tv_column_10" text="$18.50" bounds="[640,510][840,550]" />
            <node resource-id="p:id/tv_column_11" text="$17.25" bounds="[640,550][840,590]" />
            <node resource-id="p:id/tv_column_20" text="-$15.00" bounds="[840,510][1075,550]" />
            <node resource-id="p:id/tv_column_21" text="-6.94%" bounds="[840,550][1075,590]" />
          </node>
          <node resource-id="p:id/wealth_ut_item_view" text="" bounds="[0,1800][1080,2050]" />
        </hierarchy>""")
        result = positions_from_xml(xml)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["market"], "US")
        self.assertEqual(result[0]["symbol"], "BILI")
        self.assertEqual(result[0]["quantity"], "12.5")
        self.assertEqual(result[0]["cost"], "$17.25")
        self.assertEqual(result[0]["dailyPL"], "-$15.00")
        self.assertEqual(result[0]["dailyPLPercent"], "-6.94%")
        self.assertTrue(portfolio_bottom_from_xml(xml))

    def test_cash_xml(self):
        xml = parse_xml("""<hierarchy>
          <node resource-id="p:id/tv_summary_value" text="$1,234.50" bounds="[0,0][1,1]" />
          <node resource-id="p:id/rootContainer" text="" bounds="[0,0][1,1]">
            <node resource-id="p:id/tv_currency" text="USD" bounds="[0,0][1,1]" />
            <node resource-id="p:id/tv_balance" text="$1,234.50" bounds="[0,0][1,1]" />
          </node>
        </hierarchy>""")
        result = cash_data(xml)
        self.assertEqual(result["totalUsd"], "$1,234.50")
        self.assertEqual(result["balances"], [{"currency": "USD", "balance": "$1,234.50"}])


if __name__ == "__main__":
    unittest.main()
