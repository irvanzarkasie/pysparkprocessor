import LoggerWrapper from wrappers

# Initialize logger
logger = LoggerWrapper.init_logger(__name__)

def map_read_output_to_write_input(**kwargs):
  logger.debug("Map read output data into write input data")
  print(kwargs)
  kwargs["input"] = kwargs["output"]
  logger.debug("Finish mapping read output data into write input data")
  return kwargs
# end def
