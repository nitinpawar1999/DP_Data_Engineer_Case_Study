from pyspark.sql.functions import sum as f_sum

class Question_2:

    def ingest(self, spark):
        
        customer_df = spark.read.options(header='True', inferSchema='True').csv('input/customers_dataset.csv')
        orders_df = spark.read.options(header='True', inferSchema='True').csv('input/orders_dataset.csv')
        order_payments_df = spark.read.options(header='True', inferSchema='True').csv('input/order_payments_dataset.csv')

        return {'customer_df': customer_df,
                'orders_df': orders_df,
                'order_payments_df': order_payments_df}

    def process(self, df_dict):
        
        df = df_dict['customer_df'].join(df_dict['order_payments_df']\
                                        .join(df_dict['orders_df'], on='order_id',how='inner'), on='customer_id', how='inner')
        df = df.groupBy('customer_unique_id')\
                    .agg(f_sum('payment_value').alias('sum_payment_value'))

        df.show()

        return df

    def export(self, final_df):
        
        final_df.write.mode('overwrite').parquet('output/Question1')

