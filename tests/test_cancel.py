import unittest

from gigamoney.cancel_orders import in_progress_rows, order_summary
from gigamoney.ui import parse_xml


class CancelTests(unittest.TestCase):
    def test_filters_terminal_and_offscreen_rows(self):
        xml = parse_xml("""<hierarchy>
          <node resource-id="p:id/cl_order" text="" bounds="[0,600][1080,900]">
            <node resource-id="p:id/wealth_item_tv_code" text="BILI" bounds="[0,0][1,1]" />
            <node resource-id="p:id/wealth_item_order_normal_tv_status" text="Submitted" bounds="[0,0][1,1]" />
            <node resource-id="p:id/wealth_item_order_normal_tv_action" text="Buy" bounds="[0,0][1,1]" />
          </node>
          <node resource-id="p:id/cl_order" text="" bounds="[0,1000][1080,1300]">
            <node resource-id="p:id/wealth_item_tv_code" text="AAPL" bounds="[0,0][1,1]" />
            <node resource-id="p:id/wealth_item_order_normal_tv_status" text="Filled" bounds="[0,0][1,1]" />
          </node>
        </hierarchy>""")
        rows = in_progress_rows(xml)
        self.assertEqual(len(rows), 1)
        self.assertEqual(order_summary(rows[0])["symbol"], "BILI")


if __name__ == "__main__":
    unittest.main()
