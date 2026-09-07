from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import os
import xmlrpc.client
import json
import boto3

BOGOTA_TZ = ZoneInfo("America/Bogota")
ODOO_TZ = ZoneInfo(os.getenv('ODOO_TZ', 'UTC'))


def _get_bogota_now(**kwargs):
    logical_date = kwargs.get("data_interval_end") or kwargs.get("logical_date")
    if not logical_date:
        logical_date = datetime.now(timezone.utc)
    elif logical_date.tzinfo is None:
        logical_date = logical_date.replace(tzinfo=timezone.utc)
    return logical_date.astimezone(BOGOTA_TZ)


def _get_run_timestamp(**kwargs):
    bogota_now = _get_bogota_now(**kwargs)
    return bogota_now.strftime("%Y%m%d%H%M%S")


def _get_query_time_window(**kwargs):
    bogota_now = _get_bogota_now(**kwargs)
    bogota_start = bogota_now - timedelta(hours=4)
    odoo_now = bogota_now.astimezone(ODOO_TZ)
    odoo_start = bogota_start.astimezone(ODOO_TZ)
    return odoo_start, odoo_now


def _build_time_window_domain(field, **kwargs):
    start, now = _get_query_time_window(**kwargs)
    start_str = start.strftime('%Y-%m-%d %H:%M:%S')
    now_str = now.strftime('%Y-%m-%d %H:%M:%S')
    return ["&",
            [field, ">=", start_str],
            [field, "<=", now_str]]


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
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
S3_PREFIX = os.getenv('S3_PREFIX', 'bakery-pos')


def _get_odoo_models():
    common = xmlrpc.client.ServerProxy(f"{URL}/xmlrpc/2/common")
    uid = common.authenticate(DB, USERNAME, PASSWORD, {})
    models = xmlrpc.client.ServerProxy(f"{URL}/xmlrpc/2/object")
    return DB, uid, PASSWORD, models


def extract_pos_orders(**kwargs):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    db, uid, password, models = _get_odoo_models()

    domain_create = _build_time_window_domain('create_date', **kwargs)
    domain_write = _build_time_window_domain('write_date', **kwargs)

    pos_order_ids_create = models.execute_kw(
        db, uid, password,
        'pos.order', 'search',
        [domain_create],
        {'limit': 500000})

    pos_order_ids_write = models.execute_kw(
        db, uid, password,
        'pos.order', 'search',
        [domain_write],
        {'limit': 500000})

    pos_order_ids = list(set(pos_order_ids_create + pos_order_ids_write))

    pos_order_records = models.execute_kw(
        db, uid, password,
        'pos.order', 'read',
        [pos_order_ids],
        {'fields': ['id', 'name', 'date_order', 'session_id', 'user_id',
                    'partner_id', 'account_move', 'state', 'pos_reference',
                    'company_id', 'amount_tax', 'amount_total', 'create_date',
                    'write_date', 'write_uid']}
    )
    
    run_ts = _get_run_timestamp(**kwargs)
    orders_path = os.path.join(OUTPUT_DIR, f"pos_order_records_{run_ts}.json")
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
        {'fields': ['id', 'product_id','order_id' 'qty', 'price_unit', 'discount',
                    'tax_ids_after_fiscal_position', 'price_subtotal',
                    'price_subtotal_incl', 'total_cost', 'create_date',
                    'write_date', 'write_uid']}
    )

    run_ts = _get_run_timestamp(**kwargs)
    lines_path = os.path.join(OUTPUT_DIR, f"pos_order_line_records_{run_ts}.json")
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
                    'write_uid', 'name', 'payment_ref_no', 'payment_status',
                    'create_date', 'write_date']}
    )

    run_ts = _get_run_timestamp(**kwargs)
    payments_path = os.path.join(OUTPUT_DIR, f"pos_order_payment_records_{run_ts}.json")
    with open(payments_path, "w", encoding="utf-8") as file:
        json.dump(pos_order_payment_records, file, indent=4, ensure_ascii=False)

    return {
        "payments_path": payments_path,
        "payment_count": len(pos_order_payment_ids)
    }

def extract_account_move(**kwargs):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    db, uid, password, models = _get_odoo_models()

    domain_create = _build_time_window_domain('create_date', **kwargs)
    domain_write = _build_time_window_domain('write_date', **kwargs)

    account_move_ids_create = models.execute_kw(
        db, uid, password,
        'account.move', 'search',
        [domain_create],
        {'limit': 500000})

    account_move_ids_write = models.execute_kw(
        db, uid, password,
        'account.move', 'search',
        [domain_write],
        {'limit': 500000})

    account_move_ids = list(set(account_move_ids_create + account_move_ids_write))

    account_move_records = models.execute_kw(
        db, uid, password,
        'account.move', 'read',
        [account_move_ids],
        {'fields': ['id', 'name', 'ref', 'state', 'move_type',
                    'date', 'journal_id', 'company_id', 'partner_id',
                    'payment_state', 'create_date',
                    'write_date', 'write_uid']}
    )

    run_ts = _get_run_timestamp(**kwargs)
    account_move_path = os.path.join(OUTPUT_DIR, f"account_move_records_{run_ts}.json")
    with open(account_move_path, "w", encoding="utf-8") as file:
        json.dump(account_move_records, file, indent=4, ensure_ascii=False)

    return {
        "account_move_path": account_move_path,
        "account_move_count": len(account_move_records),
        "account_move_ids": account_move_ids
    }


def extract_account_move_line(**kwargs):
    ti = kwargs['ti']
    account_move_result = ti.xcom_pull(task_ids='extract_account_move')
    if not account_move_result:
        raise ValueError("No data found from extract_account_move task")

    account_move_ids = account_move_result['account_move_ids']
    db, uid, password, models = _get_odoo_models()

    account_move_line_ids = models.execute_kw(
        db, uid, password,
        'account.move.line', 'search',
        [[('move_id', 'in', account_move_ids)]],
        {'limit': 500000}
    )

    account_move_line_records = models.execute_kw(
        db, uid, password,
        'account.move.line', 'read',
        [account_move_line_ids],
        {'fields': ['id', 'move_id', 'account_id', 'payment_id', 'product_id',
                    'move_name', 'parent_state', 'ref', 'name', 'date', 'invoice_date', 
                    'analytic_distribution', 'debit', 'credit', 'balance', 'amount_currency',
                    'tax_base_amount', 'amount_residual', 'quantity', 'price_unit', 'price_subtotal',
                    'price_total', 'create_date', 'write_date', 'write_uid']}
    )

    run_ts = _get_run_timestamp(**kwargs)
    account_move_lines_path = os.path.join(OUTPUT_DIR, f"account_move_line_records_{run_ts}.json")
    with open(account_move_lines_path, "w", encoding="utf-8") as file:
        json.dump(account_move_line_records, file, indent=4, ensure_ascii=False)

    return {
        "account_move_lines_path": account_move_lines_path,
        "account_move_lines_count": len(account_move_line_records)
    }


def extract_stock_picking(**kwargs):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    db, uid, password, models = _get_odoo_models()

    domain_create = _build_time_window_domain('create_date', **kwargs)
    domain_write = _build_time_window_domain('write_date', **kwargs)

    stock_picking_ids_create = models.execute_kw(
        db, uid, password,
        'stock.picking', 'search',
        [domain_create],
        {'limit': 500000})

    stock_picking_ids_write = models.execute_kw(
        db, uid, password,
        'stock.picking', 'search',
        [domain_write],
        {'limit': 500000})

    stock_picking_ids = list(set(stock_picking_ids_create + stock_picking_ids_write))

    stock_picking_records = models.execute_kw(
        db, uid, password,
        'stock.picking', 'read',
        [stock_picking_ids],
        {'fields': ['id', 'name', 'partner_id', 'picking_type_id',
                    'scheduled_date', 'date_done', 'origin', 'state',
                    'write_date', 'create_date', 'write_uid']}
    )

    run_ts = _get_run_timestamp(**kwargs)
    stock_picking_path = os.path.join(OUTPUT_DIR, f"stock_picking_records_{run_ts}.json")
    with open(stock_picking_path, "w", encoding="utf-8") as file:
        json.dump(stock_picking_records, file, indent=4, ensure_ascii=False)

    return {
        "stock_picking_path": stock_picking_path,
        "stock_picking_count": len(stock_picking_records),
        "stock_picking_ids": stock_picking_ids
    }


def extract_stock_move(**kwargs):
    ti = kwargs['ti']
    stock_picking_result = ti.xcom_pull(task_ids='extract_stock_picking')
    if not stock_picking_result:
        raise ValueError("No data found from extract_stock_picking task")

    stock_picking_ids = stock_picking_result['stock_picking_ids']
    db, uid, password, models = _get_odoo_models()

    stock_move_ids = models.execute_kw(
        db, uid, password,
        'stock.move', 'search',
        [[('picking_id', 'in', stock_picking_ids)]],
        {'limit': 500000}
    )

    stock_move_records = models.execute_kw(
        db, uid, password,
        'stock.move', 'read',
        [stock_move_ids],
        {'fields': ['id', 'product_id', 'product_uom', 'location_id',
                    'location_dest_id', 'location_final_id', 'picking_id',
                    'name', 'priority', 'state', 'origin', 'quantity',
                    'price_unit', 'to_refund', 'create_date', 'write_date',
                    'write_uid']}
    )

    run_ts = _get_run_timestamp(**kwargs)
    stock_move_path = os.path.join(OUTPUT_DIR, f"stock_move_records_{run_ts}.json")
    with open(stock_move_path, "w", encoding="utf-8") as file:
        json.dump(stock_move_records, file, indent=4, ensure_ascii=False)

    return {
        "stock_move_path": stock_move_path,
        "stock_move_count": len(stock_move_records)
    }


def upload_raw_to_s3(**kwargs):
    ti = kwargs["ti"]
    run_ts = _get_run_timestamp(**kwargs)

    # 1. Consolidamos las rutas locales que vinieron de XCom
    files_to_upload = {
        f"pos_order_records_{run_ts}.json": ti.xcom_pull(task_ids="extract_pos_orders")[
            "orders_path"
        ],
        f"pos_order_line_records_{run_ts}.json": ti.xcom_pull(
            task_ids="extract_pos_order_lines"
        )["lines_path"],
        f"pos_order_payment_records_{run_ts}.json": ti.xcom_pull(
            task_ids="extract_pos_payments"
        )["payments_path"],
        f"account_move_records_{run_ts}.json": ti.xcom_pull(
            task_ids="extract_account_move"
        )["account_move_path"],
        f"account_move_line_records_{run_ts}.json": ti.xcom_pull(
            task_ids="extract_account_move_line"
        )["account_move_lines_path"],
        f"stock_picking_records_{run_ts}.json": ti.xcom_pull(
            task_ids="extract_stock_picking"
        )["stock_picking_path"],
        f"stock_move_records_{run_ts}.json": ti.xcom_pull(task_ids="extract_stock_move")[
            "stock_move_path"
        ],
    }

    # 2. Inicializamos el S3Hook usando el ID de la conexión
    s3_hook = S3Hook(aws_conn_id="aws_s3_conn")

    # 3. Preparamos el prefijo de fecha para S3 en hora Bogotá
    bogota_now = _get_bogota_now(**kwargs)
    directory_path = f"{S3_PREFIX}/{bogota_now.strftime('%Y/%m/%d')}"

    uploaded_files = []

    # 4. Iteramos y subimos cada archivo usando load_file
    for filename, local_path in files_to_upload.items():
        if not local_path:
            raise ValueError(f"No se encontró el archivo local para {filename}")

        # Definimos la ruta destino dentro del bucket
        folder = filename.split("_")[:-2]
        directory = "_".join(folder)
        s3_key = f"{directory_path}/{directory}/{filename}"

        # El Hook se encarga de autenticar y subir el archivo
        s3_hook.load_file(
            filename=local_path,
            key=s3_key,
            bucket_name=S3_BUCKET_NAME,
            replace=True,  # Si el archivo ya existe hoy, lo sobrescribe (Idempotencia)
        )
        uploaded_files.append(s3_key)

    return {
        "uploaded_files": uploaded_files,
        "bucket": S3_BUCKET_NAME,
        "prefix": directory_path,
    }


def cleanup_pos_data(**kwargs):
    cleaned_files = []
    if os.path.isdir(OUTPUT_DIR):
        for filename in os.listdir(OUTPUT_DIR):
            file_path = os.path.join(OUTPUT_DIR, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
                cleaned_files.append(filename)
    return {
        "cleaned_dir": OUTPUT_DIR,
        "cleaned_files": cleaned_files,
    }


with DAG(
    dag_id='bakery_pos_to_s3',
    default_args=default_args,
    description='Extract POS orders, lines and payments from Odoo and store as JSON in S3',
    schedule=timedelta(hours=4),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bakery', 'pos', 'odoo', 's3'],
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

    extract_account_move_task = PythonOperator(
        task_id='extract_account_move',
        python_callable=extract_account_move,
    )

    extract_account_move_line_task = PythonOperator(
        task_id='extract_account_move_line',
        python_callable=extract_account_move_line,
    )

    extract_stock_picking_task = PythonOperator(
        task_id='extract_stock_picking',
        python_callable=extract_stock_picking,
    )

    extract_stock_move_task = PythonOperator(
        task_id='extract_stock_move',
        python_callable=extract_stock_move,
    )

    upload_to_s3 = PythonOperator(
        task_id='upload_raw_to_s3',
        python_callable=upload_raw_to_s3,
    )

    cleanup_pos_data_task = PythonOperator(
        task_id='cleanup_pos_data',
        python_callable=cleanup_pos_data,
        trigger_rule='all_done',
    )

    extract_pos_orders_task >> [extract_pos_order_lines_task, extract_pos_payments_task]
    extract_account_move_task >> extract_account_move_line_task
    extract_stock_picking_task >> extract_stock_move_task
    [extract_pos_order_lines_task, extract_pos_payments_task, extract_account_move_line_task, extract_stock_move_task] >> upload_to_s3 >> cleanup_pos_data_task
