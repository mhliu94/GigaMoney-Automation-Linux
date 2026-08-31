import unittest

from gigamoney.vision import RegionStats, ScreenProfile, classify_profile, parse_tsv


TSV = """level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\tleft\ttop\twidth\theight\tconf\ttext
5\t1\t1\t1\t1\t1\t30\t60\t90\t30\t96.5\tBILI
5\t1\t1\t1\t1\t2\t150\t60\t80\t30\t92.0\t6.94%
"""


class VisionTests(unittest.TestCase):
    def test_tsv_mapping(self):
        result = parse_tsv(TSV, x_offset=20, y_offset=100, scale=3)
        self.assertEqual(result.text, "BILI 6.94%")
        self.assertEqual(result.words[0].box.left, 30)
        self.assertEqual(result.words[0].box.top, 120)
        self.assertEqual(result.words[0].box.right, 60)

    def test_ad_classifier_parity_cases(self):
        confirmation = ScreenProfile(
            RegionStats(130, .2, .22), RegionStats(80, .7, .05),
            RegionStats(120, .2, .20), RegionStats(115, .2, .18),
        )
        advertisement = ScreenProfile(
            RegionStats(115, .3, .22), RegionStats(70, .8, .04),
            RegionStats(65, .85, .03), RegionStats(68, .82, .03),
        )
        ambiguous = ScreenProfile(
            RegionStats(115, .3, .22), RegionStats(70, .8, .04),
            RegionStats(100, .4, .05), RegionStats(100, .4, .05),
        )
        none = ScreenProfile(
            RegionStats(90, .5, .05), RegionStats(90, .5, .05),
            RegionStats(90, .5, .05), RegionStats(90, .5, .05),
        )
        self.assertEqual(classify_profile(confirmation), "ConfirmationLike")
        self.assertEqual(classify_profile(advertisement), "Ad")
        self.assertEqual(classify_profile(ambiguous), "Ambiguous")
        self.assertEqual(classify_profile(none), "None")


if __name__ == "__main__":
    unittest.main()
