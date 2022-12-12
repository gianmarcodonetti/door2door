#!/usr/bin/env python3

import argparse

from door2door.etl.process import process_step
from door2door.spark import get_spark_session


def main(day):
    spark = get_spark_session()
    process_step(spark, day, bucket='de-tech-assessment-2022', prefix_in='data', prefix_out='processed_data')
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", help="execution date")
    args = parser.parse_args()
    day = args.day

    main(day=day)
