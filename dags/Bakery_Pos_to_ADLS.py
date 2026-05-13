from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import xmlrpc.client
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

PASSWORD = os.getenv('api_key', 'f149a9907ae3f85487be2e384b382e244add8da9')
USERNAME = os.getenv('username', 'admin')
URL = os.getenv('url', 'http://localhost:8069/').rstrip('/')
DB = os.getenv('db', 'panaderia')
OUTPUT_DIR = "/opt/airflow/logs/pos_data"

def get_orders(**kwargs):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    password = PASSWORD
    username = USERNAME
    url = URL
    db = DB
    now = datetime.today() + timedelta(hours=6)
    start = now - timedelta(hours=12)
    common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
    uid = common.authenticate(db, username, password, {})
    models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")

    pos_order_ids = models.execute_kw(
        db, uid, password,
        'pos.order', 'search',
        [["&",
          ["create_date", ">=", start.strftime('%Y-%m-%d %H:%M:%S')],
          ["create_date", "<=", now.strftime('%Y-%m-%d %H:%M:%S')]
          ]],
        {'limit': 500000})
    
    pos_order_records = models.execute_kw(
        db, uid, password,
        'pos.order', 'read',
        [pos_order_ids],
        {'fields': ['id', 'name', 'date_order', 'session_id', 'user_id',
                    'partner_id', 'account_move', 'state', 'pos_reference',
                    'company_id', 'amount_tax', 'amount_total', 'create_date',
                    'write_date']}
    )

    orders_path = os.path.join(OUTPUT_DIR, "pos_order_test.json")
    
    with open(orders_path, "w", encoding="utf-8") as file:
        json.dump(pos_order_records, file, indent=4, ensure_ascii=False)
    
    pos_order_line_ids = models.execute_kw(
        db, uid, password,
        'pos.order.line', 'search',
        [[('order_id', 'in', pos_order_ids)]],
        {'limit': 500000}
    )
    
    pos_order_line_records = models.execute_kw(
        db, uid, password,
        'pos.order.line', 'read',
        [pos_order_line_ids],
        {'fields': ['id', 'product_id', 'qty', 'price_unit', 'discount',
                    'tax_ids_after_fiscal_position', 'price_subtotal',
                    'price_subtotal_incl', 'total_cost']}
    )

    lines_path = os.path.join(OUTPUT_DIR, "pos_order_line_test.json")
    with open(lines_path, "w", encoding="utf-8") as file:
        json.dump(pos_order_line_records, file, indent=4, ensure_ascii=False)
    
    return {
        "orders_path": orders_path,
        "lines_path": lines_path,
        "order_count": len(pos_order_ids),
        "line_count": len(pos_order_line_ids),
    }
with DAG(
    dag_id='bakery_pos_to_adls',
    default_args=default_args,
    description='Extract POS orders and lines from Odoo and store as JSON',
    schedule=timedelta(hours=12),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bakery', 'pos', 'odoo'],
) as dag:
    extract_pos_data = PythonOperator(
        task_id='extract_pos_data',
        python_callable=get_orders,
    )
    extract_pos_data