import os
import unittest

from config import Config

from extract_meta_data import MetaData, CONFIG_FILE_ENVVAR


class TestMetaData(unittest.TestCase):
    def setUp(self):
        config_file = os.environ.get(CONFIG_FILE_ENVVAR, 'config/config-uat.json')
        config = Config(config_file)
        self.meta_data = MetaData(config)

    def test_delins_regex(self):
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.1000_1001delGGinsCT'}))
        self.assertFalse(self.meta_data.is_del_ins_variant({'hgvs': 'c.1000_1001GGinsCT'}))
        self.assertFalse(self.meta_data.is_del_ins_variant({'hgvs': 'c.1000_1001delGGins'}))
        self.assertFalse(self.meta_data.is_del_ins_variant({'hgvs': 'delGGins'}))
        self.assertFalse(self.meta_data.is_del_ins_variant({'hgvs': 'delGGinsF'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'delGGinsN'}))

        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'NM_001127500.1:c.3055_3082+2delinsAAGGG'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.1000_1001delGGinsCT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.1005_1006delACinsCA'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.10095_10095delCinsGAATTATATCT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.100_101delGCinsTA'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.1023_1024delCCinsTT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.103_109delATCCATTinsCTCCATG'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.1065_1066delCCinsAT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.1117_1118delCAinsTG'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.1147_1148delTCinsCCCCGGG'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.1170_1170delAinsCAC'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.1226_1231delAGCACCinsCAGCA'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.1405_1406delGGinsAA'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.1405_1406delGGinsTT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.1503_1503delAinsGAG'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.1607_1608delTCinsAA'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.1608_1609delCTinsTA'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.1654_1655delTAinsCTAC'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.1679_1680delTTinsAG'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.1704_1712delAGACCTTATinsCCAGACCCCTTATC'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.171_172delCCinsTT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.1798_1799delGTinsAA'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.1799_1800delTGinsAA'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.180_181delTCinsAA'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.180_181delTCinsAA'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.182_183delAAinsGG'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.182_183delAAinsTC'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.182_183delAAinsTG'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.1847_1852delCTGTTCinsTGTT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.1902_1903delCCinsTT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.1980_1981delCCinsTT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.2015_2016delCAinsG'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.2015_2025delCATCTGGCATAinsGTCTGGCAT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.2017_2020delTCTGinsGATCT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.205_206delGAinsAT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.2114_2116delTTGinsGTT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.21_34delinsC'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.2226_2228delTTTinsCTTTC'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.2239_2240delTTinsCC'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.2239_2248delTTAAGAGAAGinsC'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.2239_2248delTTAAGAGAAGinsC'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.2252_2260delCATCTCCGAinsTATCTCCGG'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.2263_2264delTTinsCC'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.2289_2290delCCinsTT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.2415_2426delTCCTGCACCTGGinsCTCCTGCACCTGA'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.2563_2563delAinsGAG'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.2645_2647delTTGinsCTTGC'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.2842_2846delTATGAinsGTATGAG'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.2919_2920delTGinsAT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.298_299delACinsTA'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.3019_3022delCATGinsAATGA'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.3021_3026delCGTGGCinsGTGG'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.309_310delCCinsTT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.309_318delTCCAGCTGTAinsCTCCAGCTGTAC'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.3132_3133delTGinsAA'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.313_314delGGinsTT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.3237_3245delCATGGAAGAinsATAGAAG'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.32_38delCTGGTGGinsTTGCTGA'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.34_35delGGinsCA'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.34_35delGGinsTT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.35_36delGCinsAT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.35_36delGTinsAA'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.3866_3867delGTinsAG'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.3868_3873delCACACGinsAACACGCC'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.387_388delGGinsTT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.387_390delCCTCinsAA'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.394_395delCGinsAT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.395_396delAGinsGA'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.4134_4135delGGinsTT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.4157_4161delAGACCinsCAGA'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.4197_4198delCCinsTT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.4293_4296delACCAinsCACCAC'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.432_433delGGinsTT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.4364_4370delGCCTCCGinsCCTCC'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.451_452delCCinsA'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.470_471delTCinsAA'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.474_475delCGinsAC'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.4750_4761delCAGGGGGAACTCinsAGGGGGGAACT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.4751_4751delAinsCAG'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.480_481delGGinsAT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.5008_5010delAGAinsTTTT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.51_54delCGGCinsTGGCG'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.527_527delGinsAC'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.527_527delGinsTC'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.527_527delGinsTC'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.538_540delCTGinsTAA'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.5445_5451delGATCGTAinsATGGTG'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.553_556delGAGTinsCAGTC'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.5589_5590delCCinsTT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.573_574delTCinsCT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.5756_5759delAAGAinsGAAGAG'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.5940_5948delAAGCCAGAAinsGAAGCCAGAG'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.5970_5970delAinsCAC'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.609_610delGGinsAT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.6228_6229delCGinsA'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.6275_6278delTCAGinsCTCAGC'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.633_633delAinsCAC'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.654_655delCAinsG'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.660_661delTGinsAT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.667_669delCGCinsG'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.711_718delGCAGACTGinsCAGACT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.7335_7336delGGinsTT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.734_735delGCinsTT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.737_738delCGinsGT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.7387_7387delTinsCTC'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.741_742delCCinsTT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.742_743delCGinsGA'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.743_744delGGinsAA'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.743_745delGGAinsTAC'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.747_748delGCinsTT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.748_749delCCinsTT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.756_757delCAinsGC'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.767_768delCCinsTT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.771_772delGGinsTT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.788_790delATCinsGGAACTCTGAGTACTGGGTACTCAGGGTAGGAACCCTGAG'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.795_796delGGinsTT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.809_809delTinsGTG'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.819_820delTCinsGT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.832_833delCCinsAG'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.832_833delCCinsGT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.873_874delGAinsCT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.888_889delGGinsCT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.915_919delGGGACinsACG'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.974_975delCCinsAA'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.978_980delGTCinsAT'}))
        self.assertTrue(self.meta_data.is_del_ins_variant({'hgvs': 'c.997_1008delGCCAACCGATACinsTTTTAATAAAATAAAA'}))

    def test_cipher(self):
        self.assertEqual('123', self.meta_data._cipher('012', 1))
        self.assertEqual('000', self.meta_data._cipher('999', 1))
        self.assertEqual('222', self.meta_data._cipher('999', 3))
        self.assertEqual('120', self.meta_data._cipher('019', 1))
        self.assertEqual('231', self.meta_data._cipher('019', 2))
        self.assertEqual('cde', self.meta_data._cipher('abc', 2))
        self.assertEqual('zab', self.meta_data._cipher('xyz', 2))
        self.assertEqual('CDE', self.meta_data._cipher('ABC', 2))
        self.assertEqual('ZAB', self.meta_data._cipher('XYZ', 2))


if __name__ == '__main__':

    unittest.main()