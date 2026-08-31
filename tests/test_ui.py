import unittest

from gigamoney.automation import Automation
from gigamoney.ui import bounds, child_text, find_by_resource, find_by_text, parse_bounds, parse_xml


XML = """<?xml version='1.0' encoding='UTF-8' standalone='yes' ?>
<hierarchy rotation="0">
  <node text="" resource-id="pkg:id/deal_quick_price" bounds="[0,100][500,300]">
    <node text="17.20" resource-id="pkg:id/et_input" bounds="[100,150][400,250]" />
  </node>
  <node text="" resource-id="pkg:id/deal_quick_qty" bounds="[0,300][500,500]">
    <node text="2" resource-id="pkg:id/et_input" bounds="[100,350][400,450]" />
  </node>
  <node text="Submit" resource-id="pkg:id/btn_place_order" bounds="[600,2000][1000,2200]" />
  <node text="Watchlist" resource-id="pkg:id/main_tab_tv" bounds="[0,2200][200,2400]" />
</hierarchy>"""


class UiTests(unittest.TestCase):
    def test_bounds_and_lookup(self):
        root = parse_xml(XML)
        self.assertEqual(parse_bounds("[1,2][11,22]").center, (6, 12))
        submit = find_by_resource(root, "btn_place_order")
        self.assertIsNotNone(submit)
        self.assertEqual(bounds(submit).center, (800, 2100))
        self.assertEqual(find_by_text(root, "watch", exact=False).attrib["text"], "Watchlist")

    def test_ticket_descendant_fields_are_distinct(self):
        ticket = Automation.ticket_nodes(parse_xml(XML))
        self.assertEqual(ticket.price.attrib["text"], "17.20")
        self.assertEqual(ticket.quantity.attrib["text"], "2")
        self.assertEqual(ticket.submit.attrib["text"], "Submit")


if __name__ == "__main__":
    unittest.main()
