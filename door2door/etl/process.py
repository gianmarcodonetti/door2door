from pyspark.sql import functions as F

from door2door import constants as c
from door2door.spark import write_partitioned


def process_step(spark_session, day, bucket='de-tech-assessment-2022', prefix_in='data', prefix_out='processed_data'):
    file_to_read = f"s3a://{bucket}/{prefix_in}/{day}-*.json"

    # 1. Read all the data and persist after read
    df = spark_session.read.json(file_to_read)
    df.persist()

    # 2. Create different dataframes for vehicle events, operation periods and the join between the two
    df_vehicle = (df
                  .filter(F.col('on') == 'vehicle')
                  .withColumn(c.VEHICLE_ID, F.col('data.id'))
                  .withColumn(c.LAT, F.col('data.location.lat'))
                  .withColumn(c.LNG, F.col('data.location.lng'))
                  .withColumn('location_at', F.col('data.location.at'))
                  # .withColumn('equal', F.col('at') == F.col('location_at'))
                  .withColumnRenamed("at", "vehicle_at")
                  .drop(F.col('data'))
                  )

    df_op = (df
             .filter(F.col('on') == 'operating_period')
             .withColumn('op_id', F.col('data.id'))
             .withColumn(c.START, F.col('data.start'))
             .withColumn(c.FINISH, F.col('data.finish'))
             .drop(F.col('data'))
             )

    # Maybe this could be done in the sql environment
    df_join = (df_vehicle
               .join(F.broadcast(df_op), on=c.ORGANIZATION_ID, how='left')  # test the broadcast, if useful or not
               .filter(F.col(c.START) <= F.col('vehicle_at'))
               .filter(F.col('vehicle_at') <= F.col(c.FINISH))
               )

    # 3. Save the results
    write_partitioned(df_vehicle, path_to_write=f"s3a://{bucket}/{prefix_out}/vehicle", partition_columns=None,
                      mode='errorifexists', compression=None, file_format='parquet')

    write_partitioned(df_op, path_to_write=f"s3a://{bucket}/{prefix_out}/operating_period", partition_columns=None,
                      mode='errorifexists', compression=None, file_format='csv')

    write_partitioned(df_join, path_to_write=f"s3a://{bucket}/{prefix_out}/joined", partition_columns=None,
                      mode='errorifexists', compression=None, file_format='parquet')

    return
