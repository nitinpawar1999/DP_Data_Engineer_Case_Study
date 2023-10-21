from pyspark.sql.functions import sum as f_sum, col, asc, desc, lit
from pyspark.sql.dataframe import DataFrame


class Question_2:

    def ingest(self, spark):

        customer_df = spark.read.options(header='True', inferSchema='True').csv(
            'input/customers_dataset.csv')
        orders_df = spark.read.options(
            header='True', inferSchema='True').csv('input/orders_dataset.csv')

        return {'customer_df': customer_df,
                'orders_df': orders_df}

    def process(self, df_dict):

        df = df_dict['customer_df'].join(df_dict['orders_df'], on='customer_id', how='inner')\
            .select('customer_unique_id', 'order_status')\
            .withColumn('amount', lit(1))\
            .groupBy('customer_unique_id').pivot('order_status').sum('amount')\
            .fillna(0)

        return df

    def export(self, final_df):

        final_df.printSchema()
        final_df.write.mode('overwrite').parquet('output/Question2')
