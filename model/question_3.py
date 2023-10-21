from pyspark.sql.functions import sum as f_sum, col, asc, desc, lit
from pyspark.sql.dataframe import DataFrame


class Question_3:

    def ingest(self, spark):

        customer_df = spark.read.options(header='True', inferSchema='True').csv(
            'input/customers_dataset.csv')

        orders_df = spark.read.options(
            header='True', inferSchema='True').csv('input/orders_dataset.csv')\
            .filter(col('order_status') == 'delivered')

        order_items_df = spark.read.options(
            header='True', inferSchema='True').csv('input/order_items_dataset.csv')\
            .filter(col('order_item_id') == 1)

        products_df = spark.read.options(
            header='True', inferSchema='True').csv('input/products_dataset.csv')

        product_category_name_translation_df = spark.read.options(
            header='True', inferSchema='True').csv('input/product_category_name_translation.csv')

        return {'customer_df': customer_df,
                'orders_df': orders_df,
                'order_items_df': order_items_df,
                'products_df': products_df,
                'product_category_name_translation_df': product_category_name_translation_df}

    def process(self, df_dict):

        df = df_dict['customer_df'].join(df_dict['orders_df'], 'customer_id')\
            .select('customer_unique_id', 'order_id')

        df = df.join(df_dict['order_items_df'], 'order_id')\
            .select('customer_unique_id', 'product_id')

        df = df.join(df_dict['products_df'], 'product_id')\
               .join(df_dict['product_category_name_translation_df'], 'product_category_name')\
               .select('customer_unique_id', 'product_category_name', 'product_category_name_english')

        return df

    def export(self, final_df):

        final_df.printSchema()
        final_df.write.mode('overwrite').parquet('output/Question3')
