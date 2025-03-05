import sqlite3
from datetime import datetime, timedelta
from logger import logger
import time

def _delete_old_data(conn, table: str, days: int):
    """分块删除旧数据"""
    cutoff = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
    while True:
        deleted = conn.execute(f'''
            WITH to_delete AS (
                SELECT symbol, timestamp 
                FROM {table} 
                WHERE timestamp < ? 
                ORDER BY timestamp 
                LIMIT 10000
            )
            DELETE FROM {table} 
            WHERE (symbol, timestamp) IN (SELECT symbol, timestamp FROM to_delete)
        ''', (cutoff,)).rowcount
        conn.commit()
        if deleted == 0:
            break
        logger.debug(f"已删除{table}旧数据 {deleted}条")
        time.sleep(1)

def database_maintenance(db_path: str):
    """执行数据库维护任务"""
    try:
        logger.info("执行数据库维护任务")
        config = ConfigManager().config  # 新增配置读取
        retention_config = config.get('data_retention', {
            '1m': 30,
            '1h': 365
        })
        with sqlite3.connect(db_path, timeout=60) as conn:
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')
            
            # 优化索引维护
            conn.execute('REINDEX ohlcv_1m')
            conn.execute('REINDEX ohlcv_1h')
            
            # 分阶段删除旧数据
            _delete_old_data(conn, 'ohlcv_1m', days=retention_config['1m'])
            _delete_old_data(conn, 'ohlcv_1h', days=retention_config['1h'])
            # 合并 WAL 文件
            conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
            conn.commit()
            # 更新统计信息
            conn.execute('ANALYZE')
            logger.debug(f"数据库维护完成，清理时间点: 1m<{cutoff_1m}, 1h<{cutoff_1h}")
            # 创建数据质量表
            conn.execute('''
                CREATE TABLE IF NOT EXISTS data_quality (
                    symbol TEXT,
                    timeframe TEXT,
                    date DATE,
                    completeness FLOAT,
                    gaps_count INTEGER,
                    PRIMARY KEY(symbol, timeframe, date)
                )
            ''')

            # 每日统计数据质量（在删除前统计以确保数据完整）
            conn.execute('''
                INSERT OR REPLACE INTO data_quality
                SELECT 
                    symbol,
                    '1m' as timeframe,
                    DATE(timestamp/1000, 'unixepoch') as date,
                    COUNT(*)/(1440.0) as completeness,
                    (SELECT COUNT(*) FROM data_gaps 
                     WHERE symbol = t.symbol AND timeframe='1m') as gaps_count
                FROM ohlcv_1m t
                GROUP BY symbol, date
            ''')

            # 删除30天前的1分钟数据
            cutoff_1m = int((datetime.now() - timedelta(days=retention_config['1m'])).timestamp() * 1000)
            conn.execute('DELETE FROM ohlcv_1m WHERE timestamp < ?', (cutoff_1m,))
            
            cutoff_1h = int((datetime.now() - timedelta(days=retention_config['1h'])).timestamp() * 1000)
            conn.execute('DELETE FROM ohlcv_1h WHERE timestamp < ?', (cutoff_1h,))
            
            # 清理过期绘图数据
            cutoff_plot = datetime.now() - timedelta(days=3)
            conn.execute('DELETE FROM plot_data WHERE time < ?', (cutoff_plot,))
            
            conn.commit()
            logger.debug(f"数据库维护完成，清理时间点: 1m<{cutoff_1m}, 1h<{cutoff_1h}")
    except sqlite3.OperationalError as e:
        logger.error(f"数据库维护失败（锁定）: {str(e)}")
        time.sleep(30)
        database_maintenance(db_path)  # 重试