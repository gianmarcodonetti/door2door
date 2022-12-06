import logging
import unittest

from door2door.etl.process import process
from door2door.spark import get_spark_session


def quiet_py4j():
    """Suppress spark logging for the test context."""
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


class PySparkKPITest(unittest.TestCase):
    spark = get_spark_session()
    quiet_py4j()
    """
    root
     |-- at: timestamp (nullable = true)
     |-- data: struct (nullable = true)
     |    |-- finish: timestamp (nullable = true)
     |    |-- id: string (nullable = true)
     |    |-- location: struct (nullable = true)
     |    |    |-- at: timestamp (nullable = true)
     |    |    |-- lat: double (nullable = true)
     |    |    |-- lng: double (nullable = true)
     |    |-- start: timestamp (nullable = true)
     |-- event: string (nullable = true)
     |-- on: string (nullable = true)
     |-- organization_id: string (nullable = true)
    """
    df_read = spark.createDataFrame(
        data=[
            {
                "event": "update",
                "on": "vehicle",
                "at": "2019-06-01T18:17:10.101Z",
                "data": {
                    "id": "bac5188f-67c6-4965-81dc-4ef49622e280",
                    "location": {
                        "lat": 52.45133,
                        "lng": 13.46045,
                        "at": "2019-06-01T18:17:10.101Z"
                    }
                },
                "organization_id": "org-id"
            },
            {
                "event": "update",
                "on": "vehicle",
                "at": "2019-06-01T18:17:10.123Z",
                "data": {
                    "id": "e6463c23-5ef5-4ec3-94c6-e2ab8580108d",
                    "location": {
                        "lat": 52.47383,
                        "lng": 13.2422,
                        "at": "2019-06-01T18:17:10.123Z"
                    }
                },
                "organization_id": "org-id"
            },
            {
                "event": "create",
                "on": "operating_period",
                "at": "2019-06-01T18:17:04.086Z",
                "data": {
                    "id": "op_1",
                    "start": "2016-12-30T18:17:04.079Z",
                    "finish": "2016-12-31T18:22:04.079Z"
                },
                "organization_id": "org-id"
            }
        ]
    )

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
