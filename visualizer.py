import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import sqlite3
import pandas as pd
import queue
from logger import logger
import threading
from data_manager import DataManager

class StrategyVisualizer:
    def __init__(self, data_manager, db_path: str, enabled: bool = True):
        self.enabled = enabled
        self.data_manager = data_manager
        self.db_path = db_path
        self.progress_queue = queue.Queue()
        self.shutdown_flag = threading.Event()
        data_manager.set_progress_queue(self.progress_queue)
        logger.debug(f"DataManager实例验证: {hasattr(data_manager, 'progress_bars')}")
        if self.enabled:
            self.app = dash.Dash(__name__)
            self._setup_layout()
            self._register_callbacks()
        else:
            logger.info("可视化模块已禁用")

    def _setup_layout(self):
        """布局"""
        self.app.layout = html.Div([
        html.H1("实时策略监控仪表板", style={'textAlign': 'center'}),
        html.Div([
            dcc.Dropdown(
                id='symbol-selector',
                placeholder="选择交易品种",
                multi=True,
                style={'width': '60%', 'margin': '10px'}
            ),
            dcc.RadioItems(
                id='timeframe-selector',
                options=[
                    {'label': ' 1分钟', 'value': '1m'},
                    {'label': ' 1小时', 'value': '1h'}
                ],
                value='1h',
                inline=True,
                style={'margin': '10px'}
            )
        ], style={'display': 'flex', 'justifyContent': 'center'}),
        dcc.Graph(id='live-indicators', style={'height': '80vh'}),
        dcc.Interval(id='refresh', interval=60*1000),
        html.Div(id='status-bar', style={
            'padding': '10px',
            'borderTop': '1px solid #eee',
            'height': '10vh',
            'overflowY': 'auto'
        })
    ])
    
    def _register_callbacks(self):
        """统一注册所有回调"""
        
        @self.app.callback(
            Output('symbol-selector', 'options'),
            [Input('refresh', 'n_intervals')]
        )
        def update_symbols(_):
            with sqlite3.connect(
                self.db_path,
                timeout=30,
                check_same_thread=False
            ) as conn:
                conn.execute('PRAGMA busy_timeout=5000')
                symbols = pd.read_sql(
                    'SELECT DISTINCT symbol FROM plot_data', conn
                )['symbol'].tolist()
            return [{'label': s, 'value': s} for s in sorted(symbols)]

        @self.app.callback(
        Output('live-indicators', 'figure'),
        [Input('symbol-selector', 'value'),
        Input('timeframe-selector', 'value')]
        )
        def update_graph(selected_symbols, timeframe):
            fig = make_subplots(
                rows=4, cols=1,
                shared_x=True,
                vertical_spacing=0.05,
                subplot_titles=('价格与MA', 'RSI', 'ATR', '交易信号')
            )

            with sqlite3.connect(self.db_path) as conn:
                for symbol in selected_symbols or []:
                    df = pd.read_sql('''
                        SELECT time, price, ma, rsi, atr, signals 
                        FROM plot_data 
                        WHERE symbol=? AND timeframe=?
                        ORDER BY time DESC 
                        LIMIT 1000
                    ''', conn, params=(symbol, timeframe))

                    # 价格和MA
                    fig.add_trace(go.Scatter(
                        x=df['time'], y=df['price'],
                        name=f'{symbol} Price', line=dict(width=2)),
                        row=1, col=1
                    )
                    fig.add_trace(go.Scatter(
                        x=df['time'], y=df['ma'],
                        name=f'{symbol} MA', line=dict(dash='dot')),
                        row=1, col=1
                    )

                    # RSI
                    fig.add_trace(go.Scatter(
                        x=df['time'], y=df['rsi'],
                        name=f'{symbol} RSI', line=dict(color='green')),
                        row=2, col=1
                    )

                    # ATR
                    fig.add_trace(go.Scatter(
                        x=df['time'], y=df['atr'],
                        name=f'{symbol} ATR', line=dict(color='red')),
                        row=3, col=1
                    )

                    # 交易信号
                    signals = df[df['signals'].notnull()]
                    fig.add_trace(go.Scatter(
                        x=signals['time'],
                        y=signals['price'],
                        mode='markers',
                        marker=dict(size=10, color='cyan'),
                        name=f'{symbol} Signals'
                    ), row=4, col=1)

            fig.update_layout(
                height=800,
                template='plotly_dark',
                hoverdistance=100
            )
            return fig
        # 3. 新增：更新进度条
        @self.app.callback(
            Output('status-bar', 'children'),
            [Input('refresh', 'n_intervals')]
        )
        def update_progress(_):
            status = []
            try:
                while True:
                    item = self.progress_queue.get_nowait()
                    symbol, tf, current, total = item
                    status.append(
                        html.Div(
                            f"{symbol} {tf}: {current}/{total}",
                            style={'color': '#666', 'padding': '2px'}
                        )
                    )
            except queue.Empty:
                pass
            return html.Div(status)
    
    def run_server(self, port=8050):
        """启动可视化服务器（仅在启用时运行）"""
        if self.enabled and not self.shutdown_flag.is_set():
            self.app.run_server(port=port, debug=False, use_reloader=False)
        else:
            logger.info("可视化模块未启用，跳过启动")