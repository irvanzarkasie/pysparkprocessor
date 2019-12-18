# Prepare logging lib
import logging

# Prepare sys lib
import sys

def init_logger(app_name):
  # Get logger from logging lib
  logger = logging.getLogger(app_name)
  logger.setLevel(logging.DEBUG)

  # Add handler to redirect log to stdout
  handler = logging.StreamHandler(sys.stdout)
  #handler.setLevel(logging.DEBUG)

  # Define universal log format
  formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

  # Append log format to log handler
  handler.setFormatter(formatter)

  # Append handler to logger
  logger.addHandler(handler)

  return logger

# end def

# This function is a getter function to retrieve the generic logger
def get_logger(app_name):
  # Directly return a logger object for generic logging purpose
  return logging.getLogger(app_name)
# end def
