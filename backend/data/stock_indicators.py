import pandas as pd
import numpy as np
import logging
from typing import List, Dict, Any, Optional

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TechnicalIndicators:
    """技术指标计算类"""
    
    @staticmethod
    def calculate_ma(df: pd.DataFrame, column: str = 'close', periods: list = [5, 10, 20, 60]) -> pd.DataFrame:
        """计算移动平均线
        
        Args:
            df: 包含价格数据的DataFrame
            column: 要计算均线的列名
            periods: MA周期列表
            
        Returns:
            添加了MA列的DataFrame
        """
        result = df.copy()
        for period in periods:
            result[f'MA{period}'] = result[column].rolling(window=period).mean()
        return result
    
    @staticmethod
    def calculate_ema(df: pd.DataFrame, column: str = 'close', periods: list = [5, 10, 20, 60]) -> pd.DataFrame:
        """计算指数移动平均线
        
        Args:
            df: 包含价格数据的DataFrame
            column: 要计算均线的列名
            periods: EMA周期列表
            
        Returns:
            添加了EMA列的DataFrame
        """
        result = df.copy()
        for period in periods:
            result[f'EMA{period}'] = result[column].ewm(span=period, adjust=False).mean()
        return result
    
    @staticmethod
    def calculate_macd(df: pd.DataFrame, column: str = 'close', fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
        """计算MACD指标
        
        Args:
            df: 包含价格数据的DataFrame
            column: 要计算MACD的列名
            fast: 快线周期
            slow: 慢线周期
            signal: 信号线周期
            
        Returns:
            添加了MACD相关列的DataFrame
        """
        result = df.copy()
        # 计算快线和慢线的EMA
        ema_fast = result[column].ewm(span=fast, adjust=False).mean()
        ema_slow = result[column].ewm(span=slow, adjust=False).mean()
        
        # 计算DIF、DEA和MACD
        result['MACD_DIF'] = ema_fast - ema_slow
        result['MACD_DEA'] = result['MACD_DIF'].ewm(span=signal, adjust=False).mean()
        result['MACD_HIST'] = 2 * (result['MACD_DIF'] - result['MACD_DEA'])
        
        return result
    
    @staticmethod
    def calculate_rsi(df: pd.DataFrame, column: str = 'close', periods: list = [6, 12, 24]) -> pd.DataFrame:
        """计算RSI指标
        
        Args:
            df: 包含价格数据的DataFrame
            column: 要计算RSI的列名
            periods: RSI周期列表
            
        Returns:
            添加了RSI列的DataFrame
        """
        result = df.copy()
        delta = result[column].diff()
        
        for period in periods:
            # 计算上涨和下跌绝对值
            up = delta.copy()
            up[up < 0] = 0
            down = -delta.copy()
            down[down < 0] = 0
            
            # 计算平均上涨和下跌
            avg_up = up.rolling(window=period).mean()
            avg_down = down.rolling(window=period).mean()
            
            # 计算RS和RSI
            rs = avg_up / avg_down
            result[f'RSI{period}'] = 100 - (100 / (1 + rs))
            
        return result
    
    @staticmethod
    def calculate_kdj(df: pd.DataFrame, high_column: str = 'high', low_column: str = 'low', 
                      close_column: str = 'close', n: int = 9, m1: int = 3, m2: int = 3) -> pd.DataFrame:
        """计算KDJ指标
        
        Args:
            df: 包含价格数据的DataFrame
            high_column: 最高价列名
            low_column: 最低价列名
            close_column: 收盘价列名
            n: 计算周期
            m1: K值平滑因子
            m2: D值平滑因子
            
        Returns:
            添加了KDJ列的DataFrame
        """
        result = df.copy()
        
        # 计算N日内的最高价和最低价
        result['low_n'] = result[low_column].rolling(window=n).min()
        result['high_n'] = result[high_column].rolling(window=n).max()
        
        # 计算RSV值
        result['RSV'] = 100 * (result[close_column] - result['low_n']) / (result['high_n'] - result['low_n'] + 1e-9)
        
        # 计算K值、D值、J值
        result['K'] = result['RSV'].ewm(alpha=1/m1, adjust=False).mean()
        result['D'] = result['K'].ewm(alpha=1/m2, adjust=False).mean()
        result['J'] = 3 * result['K'] - 2 * result['D']
        
        # 删除中间计算列
        result.drop(['low_n', 'high_n', 'RSV'], axis=1, inplace=True)
        
        return result
    
    @staticmethod
    def calculate_bollinger_bands(df: pd.DataFrame, column: str = 'close', window: int = 20, num_std: float = 2.0) -> pd.DataFrame:
        """计算布林带指标
        
        Args:
            df: 包含价格数据的DataFrame
            column: 要计算布林带的列名
            window: 移动平均的窗口大小
            num_std: 标准差的倍数
            
        Returns:
            添加了布林带相关列的DataFrame
        """
        result = df.copy()
        
        # 计算中轨（移动平均线）
        result['BB_MA'] = result[column].rolling(window=window).mean()
        
        # 计算标准差
        result['BB_STD'] = result[column].rolling(window=window).std()
        
        # 计算上轨和下轨
        result['BB_Upper'] = result['BB_MA'] + (result['BB_STD'] * num_std)
        result['BB_Lower'] = result['BB_MA'] - (result['BB_STD'] * num_std)
        
        # 计算带宽
        result['BB_Width'] = (result['BB_Upper'] - result['BB_Lower']) / result['BB_MA']
        
        return result
    
    @staticmethod
    def calculate_volume_indicators(df: pd.DataFrame, volume_column: str = 'volume', price_column: str = 'close') -> pd.DataFrame:
        """计算成交量相关指标
        
        Args:
            df: 包含价格和成交量数据的DataFrame
            volume_column: 成交量列名
            price_column: 价格列名
            
        Returns:
            添加了成交量指标的DataFrame
        """
        result = df.copy()
        
        # 计算成交量移动平均
        result['Volume_MA5'] = result[volume_column].rolling(window=5).mean()
        result['Volume_MA10'] = result[volume_column].rolling(window=10).mean()
        
        # 计算成交量变化率
        result['Volume_Change'] = result[volume_column].pct_change() * 100
        
        # 计算价格成交量相关性（OBV - On-Balance Volume）
        result['OBV'] = np.where(
            result[price_column] > result[price_column].shift(1),
            result[volume_column],
            np.where(
                result[price_column] < result[price_column].shift(1),
                -result[volume_column],
                0
            )
        ).cumsum()
        
        return result
    
    @staticmethod
    def calculate_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
        """计算所有技术指标
        
        Args:
            df: 原始价格数据DataFrame
            
        Returns:
            添加了所有技术指标的DataFrame
        """
        logger.info("开始计算所有技术指标...")
        
        result = df.copy()
        
        # 确保DataFrame包含所需的列
        required_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in required_columns:
            if col not in result.columns:
                logger.error(f"缺少必要的列: {col}")
                return df
        
        # 计算各类指标
        result = TechnicalIndicators.calculate_ma(result)
        result = TechnicalIndicators.calculate_ema(result)
        result = TechnicalIndicators.calculate_macd(result)
        result = TechnicalIndicators.calculate_rsi(result)
        result = TechnicalIndicators.calculate_kdj(result)
        result = TechnicalIndicators.calculate_bollinger_bands(result)
        result = TechnicalIndicators.calculate_volume_indicators(result)
        
        logger.info("所有技术指标计算完成")
        return result

# 测试代码
if __name__ == "__main__":
    # 创建一个示例数据集进行测试
    import numpy as np
    
    # 生成示例数据
    dates = pd.date_range(start='2023-01-01', periods=100, freq='D')
    close = np.random.normal(100, 5, 100).cumsum() + 1000
    high = close + np.random.uniform(0, 10, 100)
    low = close - np.random.uniform(0, 10, 100)
    open_price = close.copy()
    np.random.shuffle(open_price)
    volume = np.random.uniform(1000000, 10000000, 100)
    
    # 创建DataFrame
    df = pd.DataFrame({
        'date': dates,
        'open': open_price,
        'high': high,
        'low': low,
        'close': close,
        'volume': volume
    })
    
    # 计算所有指标
    result = TechnicalIndicators.calculate_all_indicators(df)
    
    # 打印结果
    print(result.head())
    print("\n指标列:", [col for col in result.columns if col not in ['date', 'open', 'high', 'low', 'close', 'volume']])
