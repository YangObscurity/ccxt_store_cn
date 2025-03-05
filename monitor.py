import sys
import time
from logger import setup_logging, logger
from config import ConfigManager
from trading_monitor import TradingMonitor
import logging

def setup_temp_logging():
    """初始化临时日志用于捕获配置加载阶段的错误"""
    temp_logger = logging.getLogger()
    temp_logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    temp_logger.addHandler(handler)
    return temp_logger, handler

if __name__ == '__main__':
    # 第一阶段：临时日志配置
    temp_logger, temp_handler = setup_temp_logging()
    
    try:
        # 第二阶段：配置加载（使用临时日志）
        temp_logger.info("正在加载配置文件...")
        config = ConfigManager()
        
        # 第三阶段：正式日志配置
        setup_logging()
        logger.info("日志系统初始化完成")
        
        # 移除临时日志处理器
        temp_logger.removeHandler(temp_handler)

        # 第四阶段：主程序逻辑
        exit_status = 'abnormal'
        monitor = None
        try:
            logger.info("启动量化监控系统")
            monitor = TradingMonitor()
            
            logger.info("初始化交易所连接...")
            monitor.setup_exchange()
            
            logger.info("初始化数据管理器...")
            from data_manager import DataManager
            monitor.data_manager = DataManager(monitor.store)
            
            logger.info("启动历史数据同步...")
            monitor._run_historical_fill()
            
            logger.info("添加策略...")
            monitor.add_strategy()
            
            logger.info("启动可视化模块...")
            monitor.start_visualization()
            
            logger.info("进入主监控循环")
            # 主循环
            while True:
                try:
                    monitor.run_monitor_cycle()
                except KeyboardInterrupt:
                    logger.info("用户终止监控")
                    exit_status = 'normal'
                    break
                except Exception as e:
                    logger.error(f"监控周期异常: {str(e)}", exc_info=True)
                    time.sleep(30)
                    
        except Exception as e:
            logger.critical(f"启动失败: {str(e)}", exc_info=True)
        finally:
            if monitor:
                monitor.shutdown()
            logger.info(f"系统退出，状态: {exit_status}")
            sys.exit(0 if exit_status == 'normal' else 1)

    except Exception as e:
        temp_logger.error(f"初始化失败: {str(e)}", exc_info=True)
        sys.exit(1)