from datawarehouse.utils import (
    get_postgres_connection,
    close_postgres_connection,
    create_schema_if_not_exists,
    create_table_if_not_exists,
    get_video_ids_from_db
)

from datawarehouse.data_loading import load_data
from datawarehouse.dml import (
    insert_rows,
    update_rows,
    delete_rows
)
from datawarehouse.data_transformation import transform_data

import logging
from airflow.decorators import task


logger = logging.getLogger(__name__)
table = "yt_api"

@task
def populate_staging_table():
    schema = "staging"
    connection, cursor = None, None

    try:
        connection, cursor = get_postgres_connection()
        YT_data = load_data()

        create_schema_if_not_exists(schema)
        create_table_if_not_exists(schema)

        table_ids = get_video_ids_from_db(cursor, schema)

        for row in YT_data:
            if len(table_ids) == 0:
                insert_rows(cursor, connection, schema, row)

            else:
                if row["video_id"] in table_ids:
                    update_rows(cursor, connection, schema, row)
                else:
                    insert_rows(cursor, connection, schema, row)
        
        ids_in_json = {row['video_id'] for row in YT_data}
        ids_to_delete = set(table_ids) - ids_in_json

        if ids_to_delete:
            delete_rows(cursor, connection, schema, ids_to_delete)
        
        logger.info("Staging table population completed successfully.")

    except Exception as e:
        logger.error(f"Error populating staging table: {e}")
        raise e

    finally:
        if connection and cursor:
            close_postgres_connection(connection, cursor)


@task
def populate_core_table():
    schema = "core"
    connection, cursor = None, None

    try:
        connection, cursor = get_postgres_connection()
        create_schema_if_not_exists(schema)
        create_table_if_not_exists(schema)

        table_ids = get_video_ids_from_db(cursor, schema)

        # current video ids in the staging schema yt_api table
        current_ids = set()
        cursor.execute(f"SELECT * FROM staging.{table};")
        staging_rows = cursor.fetchall()

        for row in staging_rows:
            current_ids.add(row['Video_ID'])

            if len(table_ids) == 0:
                transformed_row = transform_data(row)
                insert_rows(cursor, connection, schema, transformed_row)

            else:
                transformed_row = transform_data(row)

                if transformed_row["Video_ID"] in table_ids:
                    update_rows(cursor, connection, schema, transformed_row)
                else:
                    insert_rows(cursor, connection, schema, transformed_row)
        
        ids_to_delete = set(table_ids) - current_ids

        if ids_to_delete:
            delete_rows(cursor, connection, schema, ids_to_delete)
            
        logger.info(f"{schema} table population completed successfully.")

    except Exception as e:
        logger.error(f"Error populating {schema} table: {e}")
        raise e
    finally:
        if connection and cursor:
            close_postgres_connection(connection, cursor)