import json
import os

from transform_load_abs import TransformLoad, read_table_from_staging

class NomalizeTable(TransformLoad):
    def __init__(self, env, bucket_name, action, zone, app_name) -> None:
        super().__init__(env, bucket_name, action, zone, app_name)

    def transform(self, **kwargs):

        bucket_name = kwargs['input_bucket']
        schema = kwargs['schema']
        db_name = kwargs['db_name']

        customer_table = read_table_from_staging(self.spark, bucket_name, schema, db_name, 'customers')
        orders_table = read_table_from_staging(self.spark, bucket_name, schema, db_name, 'orders')
        product_table = read_table_from_staging(self.spark, bucket_name, schema, db_name, 'products')

        orders_customer = orders_table.join(customer_table, customer_table.id == orders_table.purchaser, 'inner') \
                                        .select(
                                                orders_table.id, 
                                                orders_table.order_date,
                                                orders_table.quantity,
                                                orders_table.product_id,
                                                customer_table.first_name,
                                                customer_table.last_name,
                                                customer_table.email,
                                                ) 

        orders_customer = orders_customer.join(product_table, product_table.id == orders_customer.product_id, 'inner') \
                                        .drop(
                                                product_table.id,
                                                product_table.ROW_NUMBER,
                                                product_table.UPDATED_AT,
                                            )
        
        orders_customer.show()

        return orders_customer, 'normalize_table'

if __name__ == '__main__':
    env= 'hieu_env'
    db_name = 'hieuut'
    schema = 'inventory'
    action = 'transform'
    zone='warehouse'

    os.environ['ENV_CONFIG'] = json.dumps(
        {
            'SQL_URI': 'postgresql://hieuut:hieuut@10.99.193.236:5432/hieuut'
        }
    )

    transform = NomalizeTable(env,
                              bucket_name= 'warehouse',
                              action= action,
                              zone= zone,
                              app_name= "Normalize table"
                              )
    
    kwargs = {
        'db_name': db_name,
        'schema': schema,
        'input_bucket': 'staging-data',
    }

    transform.load(**kwargs)