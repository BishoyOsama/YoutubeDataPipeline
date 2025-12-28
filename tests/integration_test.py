import requests
import pytest
import psycopg2



def test_api_response(get_airflow_variable):
    api_key = get_airflow_variable("api_key")
    channel_handle = get_airflow_variable("channel_handle")

    url = f"https://www.googleapis.com/youtube/v3/channels?part=contentDetails&forUsername={channel_handle}&key={api_key}"

    try:
        response = requests.get(url)
        assert response.status_code == 200

    except requests.RequestException as e:
        pytest.fail(f"API request failed: {e}")


def test_postgres_connection(postgres_connection):
    cursor = None

    try:
        cursor = postgres_connection.cursor()
        cursor.execute("SELECT 1;")
        result = cursor.fetchone()
        assert result[0] == 1
    
    except psycopg2.Error as e:
        pytest.fail(f"Postgres connection test failed: {e}")
    finally:
        if cursor:
            cursor.close()
        if postgres_connection:
            postgres_connection.close()