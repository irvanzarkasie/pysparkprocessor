from pysparkprocessor.wrappers import LoggerWrapper

# Initialize logger
logger = LoggerWrapper.init_logger(__name__)

def read_from_table(**kwargs):
  logger.debug("Read data from Hive with table name: " + kwargs["input_table"])
  #print(kwargs)
  kwargs["output"] = kwargs["sqlcontext"].sql("select * from " + kwargs["input_table"])
  return kwargs
# end def

def insert_to_table(**kwargs):
  logger.debug("Write data into Hive with table name: " + kwargs["output_table"])
  print(kwargs)
  kwargs["input"].write.format("hive").mode("append").saveAsTable(kwargs["output_table"])
  logger.debug("Successfully write data into " + kwargs["output_table"] + " table")
  kwargs["output"] = kwargs["sqlcontext"].sql("select * from " + kwargs["output_table"])
  return kwargs
# end def
