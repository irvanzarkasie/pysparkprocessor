# Prepare configparser lib
import configparser
import os

# Prepare logger lib
import LoggerWrapper
logger = LoggerWrapper.init_logger(__name__)

# Get current script directory
main_dir = os.path.dirname(os.path.abspath(__file__))

# Load config from config file using configparser lib
logger.debug("Loading configuration from config file")
config = configparser.ConfigParser()
config.read(os.path.join(main_dir, "../config/pyspark_wrapper.ini"))
logger.debug("Configuration loaded from " + os.path.join(main_dir, "../config/pyspark_wrapper.ini"))

def load_pyspark():
  # Prepare os lib to be used to retrieve SPARK_HOME
  import os

  """
  # Prepare findspark lib
  logger.debug("Importing findspark")
  import findspark
  logger.debug("findspark imported")
  """

  # Prepare py4j lib
  logger.debug("Importing py4j")
  import py4j
  logger.debug("py4j imported")

  """
  # Initialize findspark using SPARK_HOME
  logger.debug("Initializing spark")
  findspark.init(os.environ['SPARK_HOME'])
  logger.debug("Spark initialization done")
  """

  # Prepare pyspark lib
  logger.debug("Importing pyspark")
  import pyspark
  logger.debug("pyspark imported")

# end def

def create_session(app_name):
  # Import SparkSession lib required to create a spark session
  logger.debug("Importing SparkSession")
  from pyspark.sql import SparkSession
  logger.debug("SparkSession imported")
  
  # Return a spark session to the function caller
  logger.debug("Creating spark session....")
  return SparkSession.builder.appName(app_name).getOrCreate()

# end def

def create_hive_session(app_name):
  # Import SparkConf & SparkContext lib
  from pyspark import SparkConf, SparkContext  

  # Import SparkSession lib required to create hive context
  logger.debug("Importing pyspark.sql.HiveContext")
  from pyspark.sql import HiveContext
  logger.debug("pyspark.sql.HiveContext imported")

  # Create a spark context
  logger.debug("Creating hive context....")
  conf = SparkConf().setAppName(app_name)
  sc = SparkContext(conf=conf)
  
  # Return a hive context to the function caller
  hc = HiveContext(sc)
  hc.setConf("hive.metastore.uris", config["HIVE"]["hive.metastore.uris"])
  return hc

# end def

def init_pyspark():
  # Initialize pyspark module
  logger.debug("Invoke PysparkWrapper to load pyspark lib")
  load_pyspark()
  logger.debug("PysparkWrapper successfully loaded pyspark lib")

# end def

def get_hivecontext(app_name):
  # Initialize HiveContext
  logger.debug("Invoke PysparkWrapper to initialize hive context")
  hive_context = create_hive_session(app_name)
  logger.debug("Hive context initialized")
  logger.debug("PysparkWrapper successfully create HiveContext")
  return hive_context

# end def
