# -*- coding: utf-8 -*-
"""
日志工具
"""
import logging


def setup_logger():
    """设置日志记录器"""
    logger = logging.getLogger('rquote')
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        file_handler = logging.FileHandler('/tmp/rquote.log')
        
        formatter = logging.Formatter('%(asctime)-15s:%(lineno)s %(message)s')
        file_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(logging.StreamHandler())
    
    return logger


logger = setup_logger()

