import mysql.connector

class OffsetManager:
    def __init__(self, mysql_conf,spark):
        self.spark = spark
        self.mysql_conf = mysql_conf

    def get_last_offset(self, table):
        query = f"(SELECT last_offset FROM etl_offsets WHERE table_name = '{table}' ORDER BY inserted_ts DESC LIMIT 1) as t"
        url = f"jdbc:mysql://{self.mysql_conf['host']}:{self.mysql_conf['port']}/{self.mysql_conf['database']}"
        df = (
            self.spark.read.format("jdbc")
            .option("url", url)
            .option("dbtable", query)
            .option("user", self.mysql_conf["user"])
            .option("password", self.mysql_conf["password"])
            .option("driver", self.mysql_conf["driver"])
            .load()
        )

        if df.count() == 0:
            return 0

        return df.first()["last_offset"]


    def update_offset(self, table, pk, df):
        if df.count() == 0:
            return
        max_pk = df.agg({pk: "max"}).first()[0]
        conn = mysql.connector.connect(
            host=self.mysql_conf["host"],
            port=self.mysql_conf["port"],
            user=self.mysql_conf["user"],
            password=self.mysql_conf["password"],
            database=self.mysql_conf["database"]
        )
        cursor = conn.cursor()

