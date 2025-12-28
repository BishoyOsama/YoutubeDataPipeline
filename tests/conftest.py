import os
import pytest
import psycopg2
from unittest import mock
from airflow.models import Variable, Connection, DagBag

#mock test
@pytest.fixture
def api_key():
    with mock.patch.dict("os.environ", AIRFLOW_VAR_API_KEY="MOCK_KEY1234"):
        yield Variable.get("API_KEY")

#mock test
@pytest.fixture
def channel_handle():
    with mock.patch.dict("os.environ", AIRFLOW_VAR_CHANNEL_HANDLE="MOCK_CHANNEL"):
        yield Variable.get("CHANNEL_HANDLE")

#mock test
@pytest.fixture
def mock_postgres_connection():

    conn = Connection(
        login="mock_username",
        password="mock_password",
        host="mock_host",
        port=1234,
        schema="mock_db_name"
    )

    conn_uri = conn.get_uri()

    with mock.patch.dict("os.environ", AIRFLOW_CONN_POSTGRES_DB_YT_ELT=conn_uri):
        yield Connection.get_connection_from_secrets(conn_id="POSTGRES_DB_YT_ELT")

#real test fixtures
@pytest.fixture
def dagbag():
    yield DagBag()


@pytest.fixture
def get_airflow_variable():
    return lambda variable_name: os.getenv(f"AIRFLOW_VAR_{variable_name.upper()}")

@pytest.fixture
def postgres_connection():
    dbname = os.getenv("ELT_DATABASE_NAME")
    user = os.getenv("ELT_DATABASE_USERNAME")
    password = os.getenv("ELT_DATABASE_PASSWORD")
    host = os.getenv("POSTGRES_CONN_HOST")
    port = os.getenv("POSTGRES_CONN_PORT")

    conn = None

    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )

        yield conn

    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL database: {e}")
    
    finally:
        if conn:
            conn.close()