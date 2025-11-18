# -*- coding: utf-8 -*-
"""
日志工具
"""
import logging
import os


def setup_logger():
    """设置日志记录器"""
    logger = logging.getLogger('rquote')
    if not logger.handlers:
        # 默认关闭日志，通过环境变量 RQUOTE_LOG_LEVEL 控制
        log_level = os.getenv('RQUOTE_LOG_LEVEL', '').upper()
        log_file = os.getenv('RQUOTE_LOG_FILE', '/tmp/rquote.log')
        
        # 如果设置了有效的日志级别，则启用日志
        if log_level in ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'):
            # 将字符串级别转换为logging级别
            level_map = {
                'DEBUG': logging.DEBUG,
                'INFO': logging.INFO,
                'WARNING': logging.WARNING,
                'ERROR': logging.ERROR,
                'CRITICAL': logging.CRITICAL,
            }
            logger.setLevel(level_map[log_level])
            
            # 添加文件handler
            file_handler = logging.FileHandler(log_file)
            formatter = logging.Formatter('%(asctime)-15s:%(lineno)s %(message)s')
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            
            # 添加控制台handler
            logger.addHandler(logging.StreamHandler())
        else:
            # 默认关闭日志：设置为CRITICAL级别，不添加handler
            logger.setLevel(logging.CRITICAL)
    
    return logger


logger = setup_logger()

