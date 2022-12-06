import logging
import unittest
from datetime import date

from pyspark.sql import functions as F

from door2door.etl.process import process, read_data
from door2door.spark import get_spark_session, write_partitioned


def quiet_py4j():
    """Suppress spark logging for the test context."""
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


class PySparkKPITest(unittest.TestCase):
    spark = get_spark_session()
    quiet_py4j()
    df_read = spark.read.json('./test/testdata.json')

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
        self.assertEqual(set(df_vehicle.columns), {'event', 'lat', 'lng', 'location_at',
                                                   'organization_id', 'vehicle_at', 'vehicle_id'})
        self.assertEqual(set(df_op.columns), {'at', 'event', 'finish', 'op_id', 'organization_id', 'start'})
        self.assertEqual(set(df_join.columns), {'at', 'vehicle_event', 'finish', 'lat', 'lng', 'location_at', 'op_id',
                                                'op_event', 'organization_id', 'start', 'vehicle_at', 'vehicle_id'})

    def test_write_csv(self):
        write_partitioned(self.df_read.select(F.col('at'), F.col('event')), './test/out/test_data_output1',
                          file_format='csv')

    def test_write_json_partitioned(self):
        write_partitioned(self.df_read.select(F.col('at'), F.col('event'), F.col('on')), './test/out/test_data_output2',
                          file_format='json', partition_columns=['on'], compression=None)

    def test_write_avro(self):
        write_partitioned(self.df_read.select(F.col('at'), F.col('event'), F.col('on')), './test/out/test_data_output3',
                          file_format='avro')


if __name__ == '__main__':
    unittest.main()
