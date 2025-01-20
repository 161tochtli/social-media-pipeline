import logging
import sys
import datetime
from logging.handlers import TimedRotatingFileHandler

loggers = {}
FORMATTER = logging.Formatter('%(asctime)s-%(name)s-%(levelname)s-%(message)s')


def get_console_handler():
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(FORMATTER)
    return console_handler

def get_file_handler(file_name):
    file_handler = TimedRotatingFileHandler(file_name, when='midnight')
    file_handler.setFormatter(FORMATTER)
    return file_handler

def get_logger(work_dir,logger_name):
    global loggers

    if loggers.get(logger_name):
        return loggers.get(logger_name)
    else:
        current_date = datetime.date.today()
        lfile_name = f"{work_dir}/logging/{logger_name}_{str(current_date).replace('-', '')}.log"
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)
        logger.addHandler(get_console_handler())
        logger.addHandler(get_file_handler(lfile_name))
        logger.propagate = False
        loggers[logger_name] = logger
        return logger
