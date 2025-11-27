import mysql.connector
import yaml
from mysql.connector import pooling


def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)


config = load_config("../conf/config.yml")
mysql_conf = config["mysql"]

def get_db_connection():
    try:
        conn = mysql.connector.connect(
            host=mysql_conf["host"],
            user=mysql_conf["user"],
            password=mysql_conf["password"],
            database=mysql_conf["database"]
        )
        return conn
    except Exception as e:
        raise

