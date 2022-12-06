from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


def get_spark_session(executor_memory='8g', driver_memory='8g', master='local[*]', app_name='spark-door2door',
                      shuffle_partitions='40'):
    conf = (SparkConf()
            .set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
            .set('spark.hadoop.fs.s3a.aws.credentials.provider',
                 'org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider')
            .set('spark.executor.memory', executor_memory)
            .set('spark.driver.memory', driver_memory)
            # .set('spark.driver.extraClassPath', driver)
            # .set('spark.executor.extraClassPath', driver)
            .set('spark.sql.shuffle.partitions', shuffle_partitions)
            .setMaster(master)
            .setAppName(app_name)
            )

    sc = SparkContext(conf=conf)
    sc.setSystemProperty('com.amazonaws.services.s3.enableV4', 'true')
    sc._jsc.hadoopConfiguration().set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')

    spark = SparkSession(sc)
    return spark


def write_partitioned(df_to_write, path_to_write, partition_columns=None, mode='errorifexists', compression='gzip',
                      file_format='parquet', **kwargs):
    # Partition data and prepare for writing
    if partition_columns:
        df_writer = (df_to_write
                     .repartition(*partition_columns)
                     .write
                     .partitionBy(partition_columns)
                     )
    else:
        df_writer = df_to_write.write
    # Write data accordingly to the requested file format
    if file_format == 'csv':
        kwargs['header'] = 'true'
        kwargs['quote'] = ''
    getattr(df_writer, file_format)(path_to_write, mode=mode, compression=compression, **kwargs)
