import logging
import sys

def setup_logging():
    """
    Configures a structured logger for the application.
    """
    # Get the root logger
    logger = logging.getLogger()
    
    # Set the lowest-level to be processed. 
    # DEBUG is the most verbose.
    logger.setLevel(logging.DEBUG)

    # Create a handler to write logs to the console (standard output)
    # This is essential for seeing logs in Docker and cloud platforms
    handler = logging.StreamHandler(sys.stdout)

    # Define the format for the log messages
    # This format includes timestamp, log level, module name, and the message
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(module)s] - %(message)s'
    )
    
    # Set the formatter for the handler
    handler.setFormatter(formatter)

    # Add the handler to the root logger
    # This check prevents adding duplicate handlers if the function is called again
    if not logger.handlers:
        logger.addHandler(handler)

    return logger

# Create a logger instance to be imported by other modules
log = setup_logging()