from pyspark.sql.functions import sum as f_sum, col, asc, desc, lit, datediff, when
from pyspark.sql.dataframe import DataFrame


class Question_4:

    def ingest(self, spark):

        customer_df = spark.read.options(header='True', inferSchema='True').csv(
            'input/customers_dataset.csv')

        orders_df = spark.read.options(
            header='True', inferSchema='True').csv('input/orders_dataset.csv')

        order_reviews_df = spark.read.options(
            header='True', inferSchema='True', multiLine='True').csv('input/order_reviews_dataset.csv')

        return {'customer_df': customer_df,
                'orders_df': orders_df,
                'order_reviews_df': order_reviews_df}

    def process(self, df_dict):

        df = df_dict['customer_df'].join(df_dict['orders_df'], 'customer_id')\
            .select('customer_unique_id', 'order_id')

        df = df.join(df_dict['order_reviews_df'], 'order_id')\
            .select('customer_unique_id', 'order_id', 'review_creation_date', 'review_answer_timestamp',
                    datediff(col('review_answer_timestamp'), col('review_creation_date')).alias("datediff"),
                    lit(1).alias('row_count'))

        df = df.groupBy('customer_unique_id')\
            .agg(f_sum('datediff').alias('days'), f_sum('row_count').alias('row_count'))\
            .select('customer_unique_id', (col('days')/col('row_count')).alias('Avg_Time_To_Complete'))\
            .withColumn('Time_period', when(col('Avg_Time_To_Complete') < 1, 'Within 1 day')\
                                      .when((col('Avg_Time_To_Complete') >= 1) & (col('Avg_Time_To_Complete') <= 5), '2 to 5 days')\
                                      .when(col('Avg_Time_To_Complete') > 5, 'More than 5 days'))

        return df

    def export(self, final_df):

        final_df.printSchema()
        final_df.write.mode('overwrite').parquet('output/Question4')
