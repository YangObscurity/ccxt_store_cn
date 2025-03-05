import json
from logger import logger

class ConfigManager:
    _instance = None
    
    def __new__(cls, config_path='./params.json'):
        if not cls._instance:
            cls._instance = super().__new__(cls)
            cls._instance.load_config(config_path)
        return cls._instance
    
    def load_config(self, path: str):
        try:
            with open(path, 'r') as f:
                self.config = json.load(f)
            self._validate_config()
        except Exception as e:
            logger.error(f"配置文件加载失败: {str(e)}")
            raise
    
    def _validate_config(self):
        """增强配置验证逻辑"""
        required_fields = [
            'exchange.name', 
            'exchange.apikey',
            'exchange.secret',
            'spot_symbols'
        ]
        
        # 使用嵌套结构验证
        section_validators = {
            'gap_fill': ['enabled', 'max_retries'],
            'historical_fill': ['enabled', 'initial_days'],
            'logging': ['debug', 'log_api_requests'],
            'monitor': ['rsi_period', 'ma_period', 'atr_period']
        }

        # 验证必需字段
        for field in required_fields:
            keys = field.split('.')
            temp = self.config
            for key in keys:
                if key not in temp:
                    raise ValueError(f"缺少必要配置项: {field}")
                temp = temp[key]

        # 验证各配置段
        for section, fields in section_validators.items():
            if section in self.config:
                for field in fields:
                    if field not in self.config[section]:
                        raise ValueError(f"{section} 缺少 {field} 配置项")
                        
        # 类型验证
        if not isinstance(self.config.get('sandbox', False), bool):
            raise ValueError("sandbox 必须是布尔值")