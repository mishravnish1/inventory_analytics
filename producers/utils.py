from pyspark import SparkConf
from pyspark.sql import SparkSession
from confluent_kafka import Producer
import logging
from config import BOOTSTRAP_SERVERS

def get_logger(name=__name__):
    logging.basicConfig(
    filename="ims.log",
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(name)s: %(message)s"
    )
    return logging.getLogger(name)
logger = get_logger()

def sparkConf(app_name="spark app"):
    conf = SparkConf()
    conf.setAll([
        ("spark.app.name", app_name),
        ("spark.master", "local[*]")
    ])
    return conf
    
def createSparkSession(app_name):
    try:
        logger.info("creating spark session...")
        return SparkSession.builder.config(conf=sparkConf(app_name)).getOrCreate()
    except Exception as e:
        logger.error("In createSparkSession() method -> ", e)
        raise
        
def producerConf(name="producer-1", BOOTSTRAP_SERVERS=BOOTSTRAP_SERVERS):
    producer_conf = {
        "bootstrap.servers":BOOTSTRAP_SERVERS,
        "client.id":name,
        "acks":"all",
        "linger.ms":1000,
        "batch.num.messages":1000,
        "compression.type":"snappy",
    }
    return producer_conf
def createProducer(name):
    if name:
        return Producer(producerConf(name))
    else:
        return Producer(producerConf())


