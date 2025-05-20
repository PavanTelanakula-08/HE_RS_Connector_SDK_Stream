import json
from datetime import datetime, date
import redshift_connector
import snowflake.connector
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op
from schema import table_list

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

def schema(configuration: dict):
    return [{"table": t["table"], "primary_key": t["primary_key"]} for t in table_list]

def dt2str(incoming: datetime) -> str:
    if isinstance(incoming, (datetime, date)):
        return incoming.strftime(TIMESTAMP_FORMAT)
    return incoming

def connect_to_redshift(configuration):
    return redshift_connector.connect(
        host=configuration['host'],
        database=configuration['database'],
        port=configuration['port'],
        user=configuration['user'],
        password=configuration['password']
    )

def connect_to_snowflake(configuration):
    return snowflake.connector.connect(
        user=configuration["sf_user"],
        password=configuration["sf_password"],
        account=configuration["sf_account"],
        warehouse=configuration["sf_warehouse"],
        database=configuration["sf_database"],
        schema=configuration["sf_schema"],
        role=configuration["sf_role"]
    )

def safe_log_info(message):
    try:
        if log.LOG_LEVEL is None or log.Level.INFO is None:
            print(message)
        elif log.LOG_LEVEL <= log.Level.INFO:
            log.info(message)
    except:
        print(message)

def create_schema_if_not_exists(snowflake_cursor, database_name, schema_name):
    safe_log_info(f"Ensuring schema '{database_name}.{schema_name}' exists.")
    snowflake_cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{database_name}"."{schema_name}"')

def create_table_if_not_exists(snowflake_cursor, database_name, schema_name, table_name, column_names, column_types):
    type_mapping = {
        'bigint': 'NUMBER', 'integer': 'NUMBER', 'int8': 'NUMBER', 'int2': 'NUMBER', 'smallint': 'NUMBER',
        'double precision': 'DOUBLE', 'real': 'NUMBER', 'float': 'NUMBER(38,2)',
        'timestamp with time zone': 'TIMESTAMP_TZ', 'timestamp without time zone': 'TIMESTAMP_TZ',
        'timestamp': 'TIMESTAMP_TZ', 'timestamptz': 'TIMESTAMP_TZ',
        'boolean': 'BOOLEAN', 'bool': 'BOOLEAN',
        'character varying': 'VARCHAR', 'varchar': 'VARCHAR', 'char': 'VARCHAR', 'nvarchar': 'TEXT'
    }

    snowflake_cursor.execute(f'SHOW TABLES IN SCHEMA "{database_name}"."{schema_name}"')
    existing_tables = [row[1].upper() for row in snowflake_cursor.fetchall()]

    if table_name.upper() in existing_tables:
        safe_log_info(f"Table {database_name}.{schema_name}.{table_name} already exists.")
        return

    columns_def = []
    for col_name, col_type in zip(column_names, column_types):
        redshift_type = str(col_type).lower()
        snowflake_type = type_mapping.get(redshift_type, 'VARCHAR')
        columns_def.append(f'"{col_name.upper()}" {snowflake_type}')

    create_stmt = f'CREATE TABLE "{database_name}"."{schema_name}"."{table_name.upper()}" ({", ".join(columns_def)})'
    safe_log_info(f"Creating Snowflake table: {create_stmt}")
    snowflake_cursor.execute(create_stmt)

def get_query(current_cursor, table_name, replication_key, batch_size):
    if not current_cursor:
        return f"SELECT * FROM {table_name} ORDER BY {replication_key} ASC LIMIT {batch_size}"
    return f"SELECT * FROM {table_name} WHERE {replication_key} >= '{current_cursor}' ORDER BY {replication_key} ASC LIMIT {batch_size}"

def update(configuration: dict, state: dict, cancel_flag=lambda: False):
    batch_process_size = int(configuration["batch_process_size"])
    batch_query_size = int(configuration["batch_query_size"])

    redshift_conn = connect_to_redshift(configuration)
    redshift_cursor = redshift_conn.cursor()

    snowflake_conn = connect_to_snowflake(configuration)
    snowflake_cursor = snowflake_conn.cursor()

    database_name = configuration["sf_database"]
    schema_name = configuration["sf_schema"]
    create_schema_if_not_exists(snowflake_cursor, database_name, schema_name)

    table_cursors = {}

    for table in table_list:
        if cancel_flag():
            safe_log_info("Sync cancelled at table level.")
            return

        table_name = table["table"]
        if "only_tables" in configuration and table_name not in configuration["only_tables"]:
            continue

        table_cursor_key = f"{table_name}_cursor"
        current_cursor = state.get(table_cursor_key)
        replication_key = table["replication_key"].lower()

        while True:
            if cancel_flag():
                safe_log_info("Sync cancelled in while loop (query loop).")
                return

            query = get_query(current_cursor, table_name, replication_key, batch_query_size)
            safe_log_info(f"Running query: {query}")
            redshift_cursor.execute(query)

            column_names = [desc[0] for desc in redshift_cursor.description]
            type_code_map = {
                16: 'boolean', 20: 'bigint', 21: 'int2', 23: 'integer', 25: 'text',
                700: 'real', 701: 'double precision', 1042: 'char', 1043: 'varchar',
                1082: 'date', 1114: 'timestamp without time zone', 1184: 'timestamp with time zone',
                1700: 'float'
            }
            column_types = [type_code_map.get(desc[1], 'varchar') for desc in redshift_cursor.description]

            create_table_if_not_exists(snowflake_cursor, database_name, schema_name, table_name, column_names, column_types)

            total_processed = 0
            max_cursor_value = current_cursor

            while True:
                if cancel_flag():
                    safe_log_info("Sync cancelled during record loop.")
                    return

                result = redshift_cursor.fetchmany(batch_process_size)
                if not result:
                    break

                records = [{col: dt2str(val) for col, val in zip(column_names, row)} for row in result]
                total_processed += len(records)

                for record in records:
                    yield op.upsert(table_name.replace('.', '_'), record)

                cols = ', '.join([f'"{col.upper()}"' for col in column_names])
                placeholders = ', '.join(['%s'] * len(column_names))
                insert_stmt = f'INSERT INTO "{database_name}"."{schema_name}"."{table_name.upper()}" ({cols}) VALUES ({placeholders})'
                snowflake_cursor.executemany(insert_stmt, [[dt2str(v) for v in row] for row in result])

                max_cursor_value = records[-1][replication_key]
                if len(records) < batch_process_size:
                    break

            if total_processed % batch_query_size != 0:
                break

            table_cursors[table_cursor_key] = max_cursor_value
            current_cursor = max_cursor_value

        yield op.checkpoint(table_cursors)

    redshift_conn.close()
    snowflake_cursor.close()
    snowflake_conn.close()

def redshift_to_snowflake_for_ui(configuration, batch_size, only_tables=None):
    state = {}
    batch_counter = 0
    total_records = 0

    for result in update(configuration, state):
        if isinstance(result, dict) and result.get("type") == "UPSERT":
            total_records += 1

        elif isinstance(result, dict) and result.get("type") == "CHECKPOINT":
            batch_counter += 1
            yield {
                "type": "progress",
                "timestamp": str(datetime.utcnow()),
                "total_records": total_records,
                "batch": batch_counter,
                "checkpoint": result.get("state", {})
            }

        else:
            yield {
                "type": "log",
                "message": str(result)
            }


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)
