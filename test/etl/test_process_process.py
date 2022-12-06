import logging
import unittest

from door2door.etl.process import process, read_data
from door2door.spark import get_spark_session


def quiet_py4j():
    """Suppress spark logging for the test context."""
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


class PySparkKPITest(unittest.TestCase):
    spark = get_spark_session()
    quiet_py4j()
    df_read = spark.read.json('./test/testdata.json')

    def __del__(self):
        self.spark.stop()

    def test_number_of_lines_read(self):
        """There should be 35351 lines
        """
        df_read = read_data(self.spark, day=date(2019, 6, 1))
        self.assertEqual(df_read.count(), 35351)

    def test_number_of_lines_processed(self):
        """There should be 2, 1 and 2 lines
        """
        df_vehicle, df_op, df_join = process(self.df_read)
        self.assertEqual(df_vehicle.count(), 2)
        self.assertEqual(df_op.count(), 1)
        self.assertEqual(df_join.count(), 2)

    def test_columns(self):
        """There should be the following columns
        """
        df_vehicle, df_op, df_join = process(self.df_read)
        self.assertEqual(set(df_vehicle.columns), {'equal', 'event', 'lat', 'lng', 'location_at',
                                                   'on', 'organization_id', 'vehicle_at', 'vehicle_id'})
        self.assertEqual(set(df_op.columns), {'at', 'event', 'finish', 'on', 'op_id', 'organization_id', 'start'})
        self.assertEqual(set(df_join.columns), {'at', 'equal', 'event', 'finish', 'lat', 'lng', 'location_at', 'on',
                                                'op_id', 'organization_id', 'start', 'vehicle_at', 'vehicle_id'})


if __name__ == '__main__':
    unittest.main()
