import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from datetime import datetime
import tqdm
 
logger = logging.getLogger(__name__)
current_log_path = ''

def setup_logging():
    global logger, current_log_path
    from config import ConfigManager  # 延迟导入避免循环依赖
    
    log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    
    log_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    log_filename = datetime.now().strftime('monitor_%Y-%m-%d.log')
    current_log_path = os.path.join(log_dir, log_filename)
    
    file_handler = RotatingFileHandler(
        current_log_path,
        maxBytes=1*1024*1024,
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setFormatter(log_formatter)

    class TqdmLoggingHandler(logging.Handler):
        def emit(self, record):
            try:
                msg = self.format(record)
                tqdm.write(msg, end='')
            except Exception:
                pass

    class InfoFilter(logging.Filter):
        def filter(self, record):
            config = ConfigManager().config
            debug_mode = config['logging'].get('debug', False)
            log_api_requests = config['logging'].get('log_api_requests', False)
            
            # API请求日志过滤
            if 'GET' in record.getMessage() and 'HTTP/1.1' in record.getMessage():
                return log_api_requests
                
            # 调试模式显示所有日志
            if debug_mode:
                return True
                
            # 生产模式过滤调试日志
            if record.levelno == logging.DEBUG:
                return False
                
            return True
    console_handler = TqdmLoggingHandler()
    console_handler.setFormatter(log_formatter)
    console_handler.addFilter(InfoFilter())
    file_handler.addFilter(InfoFilter())

    config = ConfigManager().config
    log_level = logging.DEBUG if bool(config.get('debug', False)) else logging.INFO
    logger.setLevel(log_level)
    logger.handlers = [file_handler, console_handler]

    logging.getLogger('ccxt.base.exchange').setLevel(logging.DEBUG)
    logging.getLogger('ccxt.base.pool').setLevel(logging.DEBUG)