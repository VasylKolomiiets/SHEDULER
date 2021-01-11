# -*- coding: utf-8 -*-
"""
Created on Sun Jun 14 20:32:08 2020

@author: Vasil,  https://python-scripts.com/decorators,
  https://python-scripts.com/logging-python

"""
import logging
import logging.config
from functools import wraps


def log(func):
    """Логируем какая функция вызывается."""
    @wraps(func)
    def wrap_log(*args, **kwargs):
        logger = logging.getLogger(f"Sheduler.{func.__name__}")
        logger.info(f"Вызов с args={args}, kwargs={kwargs}")
        result = func(*args, **kwargs)
        logger.debug(f"Результат: {result}")
        return result

    return wrap_log


def logging_init(logging_config="logging.conf", logger_name="Sheduler"):
    logging.config.fileConfig(logging_config)
    return logging.getLogger(logger_name)


@log
def double_function(a):
    return a*2


if __name__ == "__main__":
    logger = logging_init()
    value = double_function(2)
    print(double_function("a"))
