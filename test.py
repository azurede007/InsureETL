from stages import ingest
from utils import offsets, spark_session
from utils.logger import get_logger
import utils.spark_session
import yaml
import stages.ingest

def load_config(path):
    with open(path) as f:
        return yaml.safe_load(f)

logger = get_logger("InsuranceETL")
config = load_config("conf/config.yml")
mysql_conf = config["mysql"]
spark = spark_session.get_spark("InsureETL")
offsetmgr = offsets.OffsetManager(mysql_conf,spark)
read_data = ingest.IngestStage(spark, mysql_conf, offsetmgr, logger)







