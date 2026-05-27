from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import os
import xmlrpc.client
import json
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

PASSWORD = os.getenv('api_key')
USERNAME = os.getenv('username', 'admin')
URL = os.getenv('url', 'http://172.23.0.1:8069/').rstrip('/')
DB = os.getenv('db', 'panaderia')
OUTPUT_DIR = "/opt/airflow/logs/pos_data"
ADLS_FILESYSTEM_NAME = os.getenv('ADLS_FILESYSTEM_NAME')


def _get_odoo_models():
    common = xmlrpc.client.ServerProxy(f"{URL}/xmlrpc/2/common")
    uid = common.authenticate(DB, USERNAME, PASSWORD, {})
    models = xmlrpc.client.ServerProxy(f"{URL}/xmlrpc/2/object")
    return DB, uid, PASSWORD, models


def extract_pos_orders(**kwargs):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    db, uid, password, models = _get_odoo_models()

    now = datetime.today() + timedelta(hours=6)
    start = now - timedelta(hours=12)

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

    orders_path = os.path.join(OUTPUT_DIR, "pos_order_records.json")
    with open(orders_path, "w", encoding="utf-8") as file:
        json.dump(pos_order_records, file, indent=4, ensure_ascii=False)

    return {
        "orders_path": orders_path,
        "order_count": len(pos_order_ids),
        "pos_order_ids": pos_order_ids
    }


def extract_pos_order_lines(**kwargs):
    ti = kwargs['ti']
    orders_result = ti.xcom_pull(task_ids='extract_pos_orders')
    if not orders_result:
        raise ValueError("No data found from extract_pos_orders task")

    pos_order_ids = orders_result['pos_order_ids']
    db, uid, password, models = _get_odoo_models()

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

    lines_path = os.path.join(OUTPUT_DIR, "pos_order_line_records.json")
    with open(lines_path, "w", encoding="utf-8") as file:
        json.dump(pos_order_line_records, file, indent=4, ensure_ascii=False)

    return {
        "lines_path": lines_path,
        "line_count": len(pos_order_line_ids)
    }


def extract_pos_payments(**kwargs):
    ti = kwargs['ti']
    orders_result = ti.xcom_pull(task_ids='extract_pos_orders')
    if not orders_result:
        raise ValueError("No data found from extract_pos_orders task")

    pos_order_ids = orders_result['pos_order_ids']
    db, uid, password, models = _get_odoo_models()

    pos_order_payment_ids = models.execute_kw(
        db, uid, password,
        'pos.payment', 'search',
        [[('pos_order_id', 'in', pos_order_ids)]],
        {'limit': 500000}
    )

    pos_order_payment_records = models.execute_kw(
        db, uid, password,
        'pos.payment', 'read',
        [pos_order_payment_ids],
        {'fields': ['id', 'pos_order_id', 'payment_method_id', 'account_move_id', 'create_uid',
                    'write_uid', 'name', 'payment_ref_no', 'payment_status']}
    )

    payments_path = os.path.join(OUTPUT_DIR, "pos_order_payment_records.json")
    with open(payments_path, "w", encoding="utf-8") as file:
        json.dump(pos_order_payment_records, file, indent=4, ensure_ascii=False)

    return {
        "payments_path": payments_path,
        "payment_count": len(pos_order_payment_ids)
    }


def upload_raw_to_adls(**kwargs):
    ti = kwargs['ti']
    orders_result = ti.xcom_pull(task_ids='extract_pos_orders')
    lines_result = ti.xcom_pull(task_ids='extract_pos_order_lines')
    payments_result = ti.xcom_pull(task_ids='extract_pos_payments')

    if not orders_result or not lines_result or not payments_result:
        raise ValueError("Missing data from one or more extraction tasks")

    azure_conexion = BaseHook.get_connection('azure_adls_conn')

    client_id = azure_conexion.login
    client_secret = azure_conexion.password
    tenant_id = azure_conexion.extra_dejson.get('tenant_id')
    account_name = azure_conexion.extra_dejson.get('account_name', 'bakeryposadlsgen2')
    filesystem_name = ADLS_FILESYSTEM_NAME

    credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret
    )

    service_client = DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=credential
    )

    file_system_client = service_client.get_file_system_client(filesystem_name)

    now = datetime.today() + timedelta(hours=6)
    directory_path = f"bakery_pos/{now.strftime('%Y/%m/%d')}"

    uploaded_files = []
    for filename, local_path in [
        ('pos_order_records.json', orders_result['orders_path']),
        ('pos_order_line_records.json', lines_result['lines_path']),
        ('pos_order_payment_records.json', payments_result['payments_path'])
    ]:
        file_client = file_system_client.get_file_client(f"{directory_path}/{filename}")

        with open(local_path, "rb") as data:
            file_client.upload_data(data, overwrite=True)

        uploaded_files.append(f"{directory_path}/{filename}")

    return {
        "uploaded_files": uploaded_files,
        "filesystem": filesystem_name,
        "account": account_name
    }


with DAG(
    dag_id='bakery_pos_to_adls',
    default_args=default_args,
    description='Extract POS orders, lines and payments from Odoo and store as JSON',
    schedule=timedelta(hours=12),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bakery', 'pos', 'odoo'],
) as dag:
    extract_pos_orders_task = PythonOperator(
        task_id='extract_pos_orders',
        python_callable=extract_pos_orders,
    )

    extract_pos_order_lines_task = PythonOperator(
        task_id='extract_pos_order_lines',
        python_callable=extract_pos_order_lines,
    )

    extract_pos_payments_task = PythonOperator(
        task_id='extract_pos_payments',
        python_callable=extract_pos_payments,
    )

    upload_to_adls = PythonOperator(
        task_id='upload_raw_to_adls',
        python_callable=upload_raw_to_adls,
    )

    extract_pos_orders_task >> [extract_pos_order_lines_task, extract_pos_payments_task]
    [extract_pos_order_lines_task, extract_pos_payments_task] >> upload_to_adls