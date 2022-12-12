#!/usr/bin/env python3

from door2door.etl.process import process_step
from door2door.spark import get_spark_session


def main():
    spark = get_spark_session()
    process_step(spark, day, bucket='de-tech-assessment-2022', prefix_in='data', prefix_out='processed_data')
    spark.stop()


if __name__ == "__main__":
    main()
