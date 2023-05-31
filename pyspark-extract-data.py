from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, col
from functools import reduce
import os


class CouncilsJob:
    def __init__(self):
        self.spark_session = (SparkSession.builder
                              .master("local[*]")
                              .appName("EnglandCouncilsJob")
                              .getOrCreate())
        self.input_directory = "data"

    def extract_councils(self):
        self.district = self.spark_session.read.csv(
            f'{os.getcwd()}/data/england_councils/district_councils.csv', header=True).withColumn(
            'council_type', lit('District Council'))
        self.london = self.spark_session.read.csv(
            f'{os.getcwd()}/data/england_councils/london_boroughs.csv', header=True).withColumn(
            'council_type', lit('London Borough'))
        self.metropolitan = self.spark_session.read.csv(
            f'{os.getcwd()}/data/england_councils/metropolitan_districts.csv', header=True).withColumn(
            'council_type', lit('Metropolitan District'))
        self.unitary = self.spark_session.read.csv(
            f'{os.getcwd()}/data/england_councils/unitary_authorities.csv', header=True).withColumn(
            'council_type', lit('Unitary Authority'))
        dfs = [self.district, self.london, self.metropolitan, self.unitary]
        self.councils_df = reduce(DataFrame.unionAll, dfs)
        return self.councils_df

    def extract_avg_price(self):
        self.avg_price_df = self.spark_session.read.csv(f'{os.getcwd()}/data/property_avg_price.csv', header=True).withColumnRenamed(
            'local_authority',
            'council').select(
            col('council'), col('avg_price_nov_2019'))
        return self.avg_price_df

    def extract_sales_volume(self):
        self.sales_volume_df = self.spark_session.read.csv(
            f'{os.getcwd()}/data/property_sales_volume.csv', header=True).withColumnRenamed(
            'local_authority', 'council')\
            .select(col('council'), col('sales_volume_sep_2019'))
        return self.sales_volume_df

    def transform(self, councils_df, avg_price_df, sales_volume_df):
        return self.councils_df.join(self.avg_price_df, on='council', how='left').join(self.sales_volume_df,
                                                                                       on='council', how='left')

    def run(self):
        return self.transform(self.extract_councils(),
                              self.extract_avg_price(),
                              self.extract_sales_volume())


test = CouncilsJob()
print(test.run().show())
print(test.run().count(), ' Row Number')
print(len(test.run().columns), ' Column Number')
