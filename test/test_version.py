import unittest

from decc import version


class KPITest(unittest.TestCase):
    def test_students_enrolled_in_time_one(self):
        self.assertTrue(version.__version__ == '0.0.1', "Check the version")
