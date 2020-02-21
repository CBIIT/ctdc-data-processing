import unittest
from treatmentarm import ArmAPI

class test_arm_api(unittest.TestCase):
    def test_get_ctdc_arm_id(self):
        self.assertEqual(ArmAPI.get_ctdc_arm_id('EAY131-Q'), 'Q')
        self.assertEqual(ArmAPI.get_ctdc_arm_id('EAY131-Z1D'), 'Z1D')
