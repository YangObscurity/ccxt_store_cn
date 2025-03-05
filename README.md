# BT-CCXT-Store-CN 量化交易框架

[English Version Below](#english-version)

## 概述

本框架是基于 **CCXT** 和 **Backtrader** 构建的加密货币量化交易系统，专为中文开发者优化。支持从数据采集到策略执行的全流程管理，针对中国网络环境深度优化，提供完整的量化解决方案。

---

## ✨ 核心特性

### 数据管理
- 🕒 **智能数据同步**  
  支持1分钟/1小时K线数据自动补全，自动检测时间戳连续性
- 🧩 **缺口修复系统**  
  多层级缺口检测算法，支持失败重试和进度保存
- 🛡️ **数据完整性保障**  
  自动清理过期数据，支持30天分钟线/365天小时线保留策略

### 交易引擎
- ⚡ **多交易所统一接口**  
  深度适配OKX/Binance等交易所API，优化中国网络连接
- 📈 **实时监控系统**  
  异步数据更新，每秒处理1000+行情事件
- 🔄 **策略热加载**  
  支持运行时动态更新策略参数

### 策略开发
- 📊 **内置指标库**  
  集成RSI/MACD/布林带等20+常用技术指标
- 🧠 **智能信号引擎**  
  支持三重过滤机制（趋势线+波动率+季节性因子）
- 📉 **参数优化工具**  
  提供遗传算法和蒙特卡洛优化模块

### 可视化
- 📊 **交互式仪表板**  
  实时展示资金曲线/持仓分布/风险指标
- 🚨 **风险预警系统**  
  动态回撤监控与自动熔断机制
- 📈 **多维度分析**  
  支持策略收益归因和参数敏感性分析

---

## 🚀 快速入门

### 环境要求
- Python 3.8+
- SQLite 3.35+
- CCXT 3.0+

### 安装步骤
```bash
# 克隆仓库
git clone https://github.com/phonegapX/bt-ccxt-store-cn.git
cd bt-ccxt-store-cn

# 安装依赖
pip install -r requirements.txt

# 初始化数据库
mkdir -p data && sqlite3 data/tick.db ".read init.sql"
```
### 配置说明 (params.json)
```
{
    "exchange": {  // 交易所配置
        "name": "okx",                // 交易所名称 (支持okx/binance等)
        "apikey": "",     // API访问密钥（需从交易所获取）
        "secret": "",      // API密钥签名（敏感信息，需加密存储）
        "password": "" // 交易密码（部分交易所需要）
    },
    "spot_symbols": [                 // 监控的现货交易对列表
        "BTC/USDT",                   // 比特币/泰达币
        "SOL/USDT",                   // Solana/泰达币
        "TRX/USDT",                   // TRON/泰达币 
        "SUI/USDT",                   // Sui/泰达币
        "DOGE/USDT"                   // 狗狗币/泰达币
    ],
    "gap_fill": {                     // 数据缺口修复配置
        "enabled": true,              // 启用缺口自动修复（true/false）
        "max_retries": 5,             // 单次缺口最大重试次数（3-10）
        "retry_delay": 10             // 重试间隔时间（秒，建议10-60）
    },
    "historical_fill": {              // 历史数据补全配置
        "enabled": false,             // 启用历史数据同步（true/false）
        "initial_days": 365,          // 初始化获取天数（最大3年）
        "page_size": 2000             // 分页大小（500-5000）
    },
    "proxy": {                        // 网络代理配置（中国用户必填）
        "https": "http://你的地址:端口",  // HTTPS代理地址
        "http": "http://你的地址:端口"    // HTTP代理地址
    },
    "monitor": {                      // 策略监控参数
        "rsi_period": 14,             // RSI计算周期（7-21）
        "ma_period": 200,             // 移动平均周期（50-500）
        "atr_period": 14,             // ATR波动率周期（7-28）
        "min_history_bars": XX,      // 最小历史数据量（建议≥策略最大周期）
        "oversold": XX,               // RSI超卖阈值（0-30）
        "overbought": XX              // RSI超买阈值（70-100）
    },
    "logging": {                      // 日志配置
        "debug": true,                // 调试模式（详细日志，生产环境建议false）
        "log_api_requests": true      // 记录完整API请求（可能包含敏感信息）
    },
    "data_retention": {               // 数据保留策略
        "1m": XX,                     // 分钟级数据保留天数（7-90）
        "1h": XX                     // 小时级数据保留天数（90-730）
    },
    "database_maintenance": {         // 数据库维护
        "interval_hours": XX           // 维护间隔（小时，建议4-24）
    },
    "sandbox": false,                 // 沙盒模式（测试环境）
    "enable_visualization": false     // 启用可视化仪表板（http://localhost:8050）
}
```

## 代码示例
```python
from monitor import TradingMonitor
from strategies.enhanced_strategy import TrendFollowingStrategy

# 初始化交易监控器
monitor = TradingMonitor(
    strategy_class=TrendFollowingStrategy,
    config_path='params.json'
)

# 启动实时交易循环
monitor.run(
    enable_historical_fill=True,  # 自动补全历史数据
    enable_risk_control=True       # 启用风控模块
)
```
## 📊 可视化功能
### 访问 ` http://localhost:8050 ` 查看实时仪表板：

- 实时行情：多品种价格走势对比

- 仓位管理：动态展示资产分布

- 风险指标：夏普比率/最大回撤实时计算

- 信号追踪：买卖信号标记与历史回放

## 🔧 高级功能
- 回测引擎
```python
from backtest_engine import Optimizer
# 多参数空间优化
optimizer = Optimizer(
    strategy=TrendFollowingStrategy,
    params_space={
        'ma_period': range(50, 301, 50),
        'rsi_period': [14, 21, 28]
    }
)
best_params = optimizer.run(
    initial_capital=100000,
    commission=0.001
)
```
### 风险管理
```python
from risk_manager import PortfolioOptimizer

# 组合优化
optimizer = PortfolioOptimizer()
weights = optimizer.calculate(
    symbols=["BTC/USDT", "ETH/USDT"],
    method='max_sharpe'  # 最大夏普比率优化
)
```
## 📚 开发指南
### 扩展策略
```python
class MyCustomStrategy(bt.Strategy):
    params = (
        ('custom_param', 30),
        ('volatility_window', 14)
    )

    def __init__(self):
        # 使用内置指标计算器
        self.volatility = self.indicators.ATR(period=self.p.volatility_window)
        
    def next(self):
        if self.volatility[0] > 0.1:
            self.order_target_percent(target=0.9)
```
### 数据接入
```python
from data_feeds import CustomDataFeed

# 接入自定义数据源
feed = CustomDataFeed(
    symbol="BTC/USDT",
    timeframe="1h",
    start_date="2023-01-01"
)
```
---
<a id="english-version"></a>

# BT-CCXT-Store-CN Quantitative Trading Framework

## Overview

A professional-grade cryptocurrency quantitative trading framework built on **CCXT** and **Backtrader**, optimized for global developers with special enhancements for Chinese network environments. Provides end-to-end solutions from data acquisition to strategy execution.

---

## ✨ Core Features

### Data Management
- 🕒 **Smart Data Synchronization**  
  Auto-completion of 1m/1h candlestick data with timestamp continuity validation
- 🧩 **Gap Repair System**  
  Multi-layer gap detection algorithm with retry logic and progress persistence
- 🛡️ **Data Integrity Assurance**  
  Automatic data retention policies (30 days for 1m, 365 days for 1h data)

### Trading Engine
- ⚡ **Unified Exchange Interface**  
  Deep integration with OKX/Binance APIs, optimized for China network latency
- 📈 **Real-time Monitoring**  
  Asynchronous data processing (1000+ events/sec)
- 🔄 **Hot-reload Strategies**  
  Dynamic parameter updates during runtime

### Strategy Development
- 📊 **Built-in Indicator Library**  
  20+ technical indicators including RSI, MACD, Bollinger Bands
- 🧠 **Smart Signal Engine**  
  Triple-layer filters (Trend + Volatility + Seasonality)
- 📉 **Parameter Optimization**  
  Genetic Algorithm and Monte Carlo optimization modules

### Visualization
- 📊 **Interactive Dashboard**  
  Real-time equity curve/position distribution/risk metrics
- 🚨 **Risk Alert System**  
  Dynamic drawdown monitoring with auto-circuit-breaker
- 📈 **Multi-dimensional Analysis**  
  Strategy attribution and parameter sensitivity analysis

---

## 🚀 Quick Start

### Requirements
- Python 3.8+
- SQLite 3.35+
- CCXT 3.0+

### Installation
```bash
# Clone repository
git clone https://github.com/phonegapX/bt-ccxt-store-cn.git
cd bt-ccxt-store-cn

# Install dependencies
pip install -r requirements.txt

# Initialize database
mkdir -p data && sqlite3 data/tick.db ".read init.sql"
```
## Configuration (params.json)
```
{
    "exchange": {  // Exchange Configuration
        "name": "okx",                // Exchange name (okx/binance supported)
        "apikey": "",     // API access key (from exchange)
        "secret": "",      // API secret (encrypt in production)
        "password": "" // Trading password (if required)
    },
    "spot_symbols": [                 // Monitoring spot pairs
        "BTC/USDT",                   // Bitcoin/Tether
        "SOL/USDT",                   // Solana/Tether
        "TRX/USDT",                   // TRON/Tether
        "SUI/USDT",                   // Sui/Tether
        "DOGE/USDT"                   // Dogecoin/Tether
    ],
    "gap_fill": {                     // Data gap repair settings
        "enabled": true,              // Enable auto gap filling (true/false)
        "max_retries": 5,             // Max retries per gap (3-10)
        "retry_delay": 10             // Retry interval in seconds (10-60)
    },
    "historical_fill": {              // Historical data sync
        "enabled": false,             // Enable historical sync (true/false)
        "initial_days": 365,          // Days to fetch (max 3 years)
        "page_size": 2000             // Pagination size (500-5000)
    },
    "proxy": {                        // Network proxy (required in China)
        "https": "http://YOURS:PORT",  // HTTPS proxy endpoint
        "http": "http://YOURS:PORT"    // HTTP proxy endpoint
    },
    "monitor": {                      // Strategy parameters
        "rsi_period": 14,             // RSI period (7-21)
        "ma_period": 200,             // Moving average period (50-500)
        "atr_period": 14,             // ATR volatility period (7-28)
        "min_history_bars": XX,      // Minimum historical bars (≥ strategy period)
        "oversold": XX,               // RSI oversold threshold (0-30)
        "overbought": XX              // RSI overbought threshold (70-100)
    },
    "logging": {                      // Logging settings
        "debug": true,                // Debug mode (verbose logging)
        "log_api_requests": true      // Log full API requests (sensitive)
    },
    "data_retention": {               // Data retention policy
        "1m": XX,                     // Minute-level data (7-90 days)
        "1h": XX                     // Hour-level data (90-730 days)
    },
    "database_maintenance": {         // DB maintenance
        "interval_hours": XX           // Maintenance interval (4-24 hours)
    },
    "sandbox": false,                 // Sandbox mode (test environment)
    "enable_visualization": false     // Enable dashboard (http://localhost:8050)
}
```
## Code Example
```python
from monitor import TradingMonitor
from strategies.enhanced_strategy import TrendFollowingStrategy

# Initialize trading monitor
monitor = TradingMonitor(
    strategy_class=TrendFollowingStrategy,
    config_path='params.json'
)

# Start trading loop
monitor.run(
    enable_historical_fill=True,  # Auto-complete historical data
    enable_risk_control=True       # Enable risk management
)
```
## 📊 Visualization
### Access real-time dashboard at `http://localhost:8050`:

- Market Monitor: Multi-asset price comparison

- Position Management: Dynamic asset allocation display

- Risk Metrics: Sharpe Ratio/Max Drawdown calculators

- Signal Tracking: Historical trade signal replay

## 🔧 Advanced Features
### Backtesting Engine
```python
from backtest_engine import Optimizer

# Multi-parameter optimization
optimizer = Optimizer(
    strategy=TrendFollowingStrategy,
    params_space={
        'ma_period': range(50, 301, 50),
        'rsi_period': [14, 21, 28]
    }
)
best_params = optimizer.run(
    initial_capital=100000,
    commission=0.001
)
```
### Risk Management
```python
from risk_manager import PortfolioOptimizer

# Portfolio optimization
optimizer = PortfolioOptimizer()
weights = optimizer.calculate(
    symbols=["BTC/USDT", "ETH/USDT"],
    method='max_sharpe'  # Sharpe Ratio optimization
)
```
## 📚 Development Guide
### Custom Strategy
```python
class MyCustomStrategy(bt.Strategy):
    params = (
        ('custom_param', 30),
        ('volatility_window', 14)
    )

    def __init__(self):
        # Use built-in indicator calculator
        self.volatility = self.indicators.ATR(period=self.p.volatility_window)
        
    def next(self):
        if self.volatility[0] > 0.1:
            self.order_target_percent(target=0.9)
```
### Data Integration
```python
from data_feeds import CustomDataFeed

# Connect custom data source
feed = CustomDataFeed(
    symbol="BTC/USDT",
    timeframe="1h",
    start_date="2023-01-01"
)
```
License
MIT License
