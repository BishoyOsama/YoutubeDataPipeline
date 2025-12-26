from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor


table = "yt_api"

def get_postgres_connection():
    """Establishes and returns a connection to the Postgres database using Airflow's PostgresHook."""
    pg_hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")
    connection = pg_hook.get_conn()
    cursor = connection.cursor(cursor_factory=RealDictCursor)
    return connection, cursor


def close_postgres_connection(connection, cursor):
    """Closes the given Postgres database connection and cursor."""
    cursor.close()
    connection.close()


def create_schema_if_not_exists(schema):
    """Creates the specified schema in the Postgres database if it does not already exist."""
    connection, cursor = get_postgres_connection()
    create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {schema};"
    cursor.execute(create_schema_query)
    connection.commit()
    close_postgres_connection(connection, cursor)


def create_table_if_not_exists(schema):
    """Creates the yt_api table in the Postgres database if it does not already exist.
    for either staging or core schema.
    """
    connection, cursor = get_postgres_connection()

    if schema == "staging":
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                "Video_Title" TEXT NOT NULL,
                "Upload_Date" TIMESTAMP NOT NULL,
                "Duration" VARCHAR(20) NOT NULL,
                "Video_Views" INT,
                "Likes_Count" INT,
                "Comments_Count" INT
                );
            """
    else:
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                "Video_Title" TEXT NOT NULL,
                "Upload_Date" TIMESTAMP NOT NULL,
                "Duration" TIME NOT NULL,
                "Video_Type" VARCHAR(10) NOT NULL,
                "Video_Views" INT,
                "Likes_Count" INT,
                "Comments_Count" INT
                );
            """
        
    cursor.execute(create_table_query)
    connection.commit()
    close_postgres_connection(connection, cursor)



def get_video_ids_from_db(cursor, schema):
    """Fetches and returns all Video_IDs from the yt_api table in the specified schema."""
    connection, cursor = get_postgres_connection()
    fetch_query = f'SELECT "Video_ID" FROM {schema}.{table};'
    cursor.execute(fetch_query)
    results = cursor.fetchall()
    video_ids = [row["Video_ID"] for row in results]
    close_postgres_connection(connection, cursor)
    return video_ids
        