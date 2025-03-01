import akshare as ak
import pandas as pd
import time
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StockDataFetcher:
    """股票数据获取类"""
    
    def __init__(self):
        """初始化数据获取器"""
        logger.info("初始化股票数据获取器")
        
    def get_stock_list(self) -> pd.DataFrame:
        """获取A股上市公司列表"""
        try:
            # 使用akshare获取A股上市公司基本信息
            logger.info("开始获取股票列表...")
            stock_info = ak.stock_info_a_code_name()
            logger.info(f"成功获取 {len(stock_info)} 只股票的基本信息")
            return stock_info
        except Exception as e:
            logger.error(f"获取股票列表时出错: {e}")
            return pd.DataFrame()
    
    def get_daily_data(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取指定股票的日线数据
        
        Args:
            symbol: 股票代码，如'600000'，不含市场前缀
            start_date: 开始日期，如'20220101'
            end_date: 结束日期，如'20220131'
            
        Returns:
            包含日线数据的DataFrame
        """
        try:
            # 添加适当的休眠以避免请求过于频繁
            time.sleep(0.5)
            
            logger.info(f"获取股票 {symbol} 从 {start_date} 到 {end_date} 的日线数据")
            
            # 使用akshare获取股票日线数据
            df = ak.stock_zh_a_hist(
                symbol=symbol, 
                period="daily", 
                start_date=start_date, 
                end_date=end_date, 
                adjust="qfq"  # 前复权
            )
            
            # 重命名列以便于后续处理
            df.rename(columns={
                '日期': 'date',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'amount',
                '振幅': 'amplitude',
                '涨跌幅': 'pct_change',
                '涨跌额': 'change',
                '换手率': 'turnover'
            }, inplace=True)
            
            logger.info(f"成功获取 {len(df)} 条日线数据记录")
            return df
        except Exception as e:
            logger.error(f"获取日线数据时出错: {e}")
            return pd.DataFrame()
    
    def get_financial_data(self, symbol: str) -> pd.DataFrame:
        """获取财务数据
        
        Args:
            symbol: 股票代码
            
        Returns:
            包含财务指标的DataFrame
        """
        try:
            logger.info(f"获取股票 {symbol} 的财务数据")
            
            # 获取最新财务指标数据
            financial_data = ak.stock_financial_abstract(symbol=symbol)
            
            logger.info(f"成功获取财务数据，包含 {len(financial_data)} 条记录")
            return financial_data
        except Exception as e:
            logger.error(f"获取财务数据时出错: {e}")
            return pd.DataFrame()
    
    def get_industry_classification(self) -> pd.DataFrame:
        """获取行业分类数据"""
        try:
            logger.info("获取股票行业分类数据")
            
            # 使用akshare获取申万行业分类
            industry_data = ak.stock_sector_spot()
            
            logger.info(f"成功获取行业分类数据，包含 {len(industry_data)} 条记录")
            return industry_data
        except Exception as e:
            logger.error(f"获取行业分类数据时出错: {e}")
            return pd.DataFrame()
    
    def get_latest_market_data(self) -> pd.DataFrame:
        """获取最新市场行情数据"""
        try:
            logger.info("获取最新市场行情数据")
            
            # 使用akshare获取A股实时行情
            market_data = ak.stock_zh_a_spot_em()
            
            logger.info(f"成功获取最新市场行情数据，包含 {len(market_data)} 条记录")
            return market_data
        except Exception as e:
            logger.error(f"获取最新市场行情数据时出错: {e}")
            return pd.DataFrame()
    
    def update_stock_data(self, symbol: str, days: int = 30) -> pd.DataFrame:
        """更新指定股票的最近N天数据
        
        Args:
            symbol: 股票代码
            days: 要获取的天数
            
        Returns:
            更新的数据
        """
        # 计算日期范围
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
        
        return self.get_daily_data(symbol, start_date, end_date)

# 测试代码
if __name__ == "__main__":
    fetcher = StockDataFetcher()
    
    # 测试获取股票列表
    stock_list = fetcher.get_stock_list()
    print(f"获取到 {len(stock_list)} 只股票")
    if not stock_list.empty:
        print(stock_list.head())
    
    # 测试获取日线数据
    if not stock_list.empty:
        test_stock = stock_list.iloc[0]['code']
        print(f"\n获取 {test_stock} 的日线数据")
        daily_data = fetcher.get_daily_data(
            symbol=test_stock, 
            start_date="20240101", 
            end_date="20240201"
        )
        if not daily_data.empty:
            print(daily_data.head())
