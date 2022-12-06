import logging
import unittest
from datetime import date

from door2door.etl.process import read_data
from door2door.spark import get_spark_session


def quiet_py4j():
    """Suppress spark logging for the test context."""
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


class PySparkKPITest(unittest.TestCase):
    spark = get_spark_session()
    quiet_py4j()

    def __del__(self):
        self.spark.stop()

    def test_number_of_lines_read(self):
        """There should be 35351 lines
        """
        df_read = read_data(self.spark, day=date(2019, 6, 1))
        self.assertEqual(df_read.count(), 35351)


if __name__ == '__main__':
    unittest.main()
