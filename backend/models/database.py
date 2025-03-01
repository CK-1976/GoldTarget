import sqlite3
import pandas as pd
import os
import logging
from pathlib import Path
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Database:
    """数据库管理类"""
    
    def __init__(self, db_path: str = "data/stocks.db"):
        """初始化数据库连接
        
        Args:
            db_path: 数据库文件路径
        """
        # 确保数据库目录存在
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        
        logger.info(f"数据库初始化于: {os.path.abspath(db_path)}")
    
    def connect(self):
        """连接到数据库"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            logger.info("数据库连接成功")
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            raise
    
    def disconnect(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None
            logger.info("数据库连接已关闭")
    
    def execute(self, query: str, params=()):
        """执行SQL语句
        
        Args:
            query: SQL查询语句
            params: 查询参数
        
        Returns:
            查询结果
        """
        if not self.conn:
            self.connect()
        try:
            self.cursor.execute(query, params)
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"SQL执行错误: {e}")
            logger.error(f"SQL语句: {query}")
            logger.error(f"参数: {params}")
            return None
    
    def execute_many(self, query: str, params_list):
        """执行多个SQL语句
        
        Args:
            query: SQL模板
            params_list: 参数列表
        """
        if not self.conn:
            self.connect()
        try:
            self.cursor.executemany(query, params_list)
            self.conn.commit()
            logger.info(f"批量执行SQL成功，影响 {len(params_list)} 条记录")
        except Exception as e:
            logger.error(f"SQL批量执行错误: {e}")
            logger.error(f"SQL语句: {query}")
            self.conn.rollback()
    
    def commit(self):
        """提交事务"""
        if self.conn:
            self.conn.commit()
            logger.debug("提交事务成功")
    
    def rollback(self):
        """回滚事务"""
        if self.conn:
            self.conn.rollback()
            logger.warning("事务已回滚")
    
    def create_tables(self):
        """创建数据库表结构"""
        logger.info("开始创建数据库表结构...")
        
        # 股票基本信息表
        self.execute('''
        CREATE TABLE IF NOT EXISTS stocks (
            code TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            market TEXT,
            industry TEXT,
            list_date TEXT,
            last_update TEXT
        )
        ''')
        
        # 日线数据表
        self.execute('''
        CREATE TABLE IF NOT EXISTS daily_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            amount REAL,
            pct_change REAL,
            turnover REAL,
            UNIQUE(code, date)
        )
        ''')
        
        # 技术指标表
        self.execute('''
        CREATE TABLE IF NOT EXISTS technical_indicators (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            date TEXT NOT NULL,
            ma5 REAL,
            ma10 REAL,
            ma20 REAL,
            ma60 REAL,
            macd_dif REAL,
            macd_dea REAL,
            macd_hist REAL,
            rsi6 REAL,
            rsi12 REAL,
            rsi24 REAL,
            k REAL,
            d REAL,
            j REAL,
            bb_upper REAL,
            bb_middle REAL,
            bb_lower REAL,
            UNIQUE(code, date)
        )
        ''')
        
        # 用户筛选条件表
        self.execute('''
        CREATE TABLE IF NOT EXISTS saved_filters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            filter_json TEXT NOT NULL,
            create_time TEXT NOT NULL,
            update_time TEXT
        )
        ''')
        
        self.commit()
        logger.info("数据库表结构创建完成")
    
    def save_stock_list(self, df: pd.DataFrame):
        """保存股票列表到数据库
        
        Args:
            df: 包含股票信息的DataFrame
        """
        if df.empty:
            logger.warning("股票列表为空，未保存任何数据")
            return
        
        logger.info(f"开始保存 {len(df)} 条股票信息")
        
        # 准备数据
        data = []
        for _, row in df.iterrows():
            # 根据实际DataFrame结构调整列名
            data.append((
                row.get('code', ''),  # 股票代码
                row.get('name', ''),   # 股票名称
                '',                    # 市场
                '',                    # 行业
                '',                    # 上市日期
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # 更新时间
            ))
        
        # 执行批量插入
        self.execute_many('''
        INSERT OR REPLACE INTO stocks (code, name, market, industry, list_date, last_update)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', data)
        
        self.commit()
        logger.info(f"股票列表保存完成，共 {len(data)} 条记录")
    
    def save_daily_data(self, code: str, df: pd.DataFrame):
        """保存日线数据到数据库
        
        Args:
            code: 股票代码
            df: 包含日线数据的DataFrame
        """
        if df.empty:
            logger.warning(f"股票 {code} 的日线数据为空，未保存任何数据")
            return
        
        logger.info(f"开始保存股票 {code} 的 {len(df)} 条日线数据")
        
        # 准备数据
        data = []
        for _, row in df.iterrows():
            # 确保日期是字符串格式
            date_str = row['date']
            if isinstance(date_str, pd.Timestamp):
                date_str = date_str.strftime('%Y-%m-%d')
                
            data.append((
                code,
                date_str,
                row.get('open', None),
                row.get('high', None),
                row.get('low', None),
                row.get('close', None),
                row.get('volume', None),
                row.get('amount', None),
                row.get('pct_change', None),
                row.get('turnover', None)
            ))
        
        # 执行批量插入
        self.execute_many('''
        INSERT OR REPLACE INTO daily_data 
        (code, date, open, high, low, close, volume, amount, pct_change, turnover)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', data)
        
        self.commit()
        logger.info(f"股票 {code} 的日线数据保存完成，共 {len(data)} 条记录")
    
    def save_technical_indicators(self, code: str, df: pd.DataFrame):
        """保存技术指标数据到数据库
        
        Args:
            code: 股票代码
            df: 包含技术指标的DataFrame
        """
        if df.empty:
            logger.warning(f"股票 {code} 的技术指标数据为空，未保存任何数据")
            return
        
        logger.info(f"开始保存股票 {code} 的 {len(df)} 条技术指标数据")
        
        # 准备数据
        data = []
        for _, row in df.iterrows():
            # 确保日期是字符串格式
            date_str = row['date']
            if isinstance(date_str, pd.Timestamp):
                date_str = date_str.strftime('%Y-%m-%d')
                
            data.append((
                code,
                date_str,
                row.get('MA5', None),
                row.get('MA10', None),
                row.get('MA20', None),
                row.get('MA60', None),
                row.get('MACD_DIF', None),
                row.get('MACD_DEA', None),
                row.get('MACD_HIST', None),
                row.get('RSI6', None),
                row.get('RSI12', None),
                row.get('RSI24', None),
                row.get('K', None),
                row.get('D', None),
                row.get('J', None),
                row.get('BB_Upper', None),
                row.get('BB_MA', None),
                row.get('BB_Lower', None)
            ))
        
        # 执行批量插入
        self.execute_many('''
        INSERT OR REPLACE INTO technical_indicators
        (code, date, ma5, ma10, ma20, ma60, macd_dif, macd_dea, macd_hist, 
         rsi6, rsi12, rsi24, k, d, j, bb_upper, bb_middle, bb_lower)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', data)
        
        self.commit()
        logger.info(f"股票 {code} 的技术指标数据保存完成，共 {len(data)} 条记录")
    
    def save_filter(self, name: str, filter_json: str):
        """保存筛选条件
        
        Args:
            name: 筛选条件名称
            filter_json: 筛选条件的JSON字符串
        
        Returns:
            新增筛选条件的ID
        """
        try:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            self.execute('''
            INSERT INTO saved_filters (name, filter_json, create_time, update_time)
            VALUES (?, ?, ?, ?)
            ''', (name, filter_json, now, now))
            
            self.commit()
            
            # 获取新增ID
            filter_id = self.cursor.lastrowid
            logger.info(f"筛选条件 '{name}' 保存成功，ID: {filter_id}")
            
            return filter_id
        except Exception as e:
            logger.error(f"保存筛选条件失败: {e}")
            self.rollback()
            return None
    
    def get_stock_by_code(self, code: str):
        """根据股票代码获取股票信息
        
        Args:
            code: 股票代码
            
        Returns:
            股票信息字典，如果不存在则返回None
        """
        result = self.execute('''
        SELECT code, name, market, industry, list_date, last_update
        FROM stocks
        WHERE code = ?
        ''', (code,))
        
        if result and len(result) > 0:
            return {
                'code': result[0][0],
                'name': result[0][1],
                'market': result[0][2],
                'industry': result[0][3],
                'list_date': result[0][4],
                'last_update': result[0][5]
            }
        else:
            return None
    
    def get_daily_data(self, code: str, start_date: str = None, end_date: str = None):
        """获取指定股票的日线数据
        
        Args:
            code: 股票代码
            start_date: 开始日期，格式为 'YYYY-MM-DD'
            end_date: 结束日期，格式为 'YYYY-MM-DD'
            
        Returns:
            包含日线数据的DataFrame
        """
        query = '''
        SELECT code, date, open, high, low, close, volume, amount, pct_change, turnover
        FROM daily_data
        WHERE code = ?
        '''
        
        params = [code]
        
        if start_date:
            query += " AND date >= ?"
            params.append(start_date)
        
        if end_date:
            query += " AND date <= ?"
            params.append(end_date)
        
        query += " ORDER BY date"
        
        result = self.execute(query, tuple(params))
        
        if result:
            df = pd.DataFrame(result, columns=[
                'code', 'date', 'open', 'high', 'low', 'close', 'volume', 
                'amount', 'pct_change', 'turnover'
            ])
            return df
        else:
            return pd.DataFrame()
    
    def get_technical_indicators(self, code: str, start_date: str = None, end_date: str = None):
        """获取指定股票的技术指标数据
        
        Args:
            code: 股票代码
            start_date: 开始日期，格式为 'YYYY-MM-DD'
            end_date: 结束日期，格式为 'YYYY-MM-DD'
            
        Returns:
            包含技术指标的DataFrame
        """
        query = '''
        SELECT code, date, ma5, ma10, ma20, ma60, macd_dif, macd_dea, macd_hist,
               rsi6, rsi12, rsi24, k, d, j, bb_upper, bb_middle, bb_lower
        FROM technical_indicators
        WHERE code = ?
        '''
        
        params = [code]
        
        if start_date:
            query += " AND date >= ?"
            params.append(start_date)
        
        if end_date:
            query += " AND date <= ?"
            params.append(end_date)
        
        query += " ORDER BY date"
        
        result = self.execute(query, tuple(params))
        
        if result:
            df = pd.DataFrame(result, columns=[
                'code', 'date', 'MA5', 'MA10', 'MA20', 'MA60', 'MACD_DIF', 'MACD_DEA', 'MACD_HIST',
                'RSI6', 'RSI12', 'RSI24', 'K', 'D', 'J', 'BB_Upper', 'BB_Middle', 'BB_Lower'
            ])
            return df
        else:
            return pd.DataFrame()
    
    def get_saved_filters(self):
        """获取所有保存的筛选条件
        
        Returns:
            筛选条件列表
        """
        result = self.execute('''
        SELECT id, name, filter_json, create_time, update_time
        FROM saved_filters
        ORDER BY update_time DESC
        ''')
        
        if result:
            filters = []
            for row in result:
                filters.append({
                    'id': row[0],
                    'name': row[1],
                    'filter_json': row[2],
                    'create_time': row[3],
                    'update_time': row[4]
                })
            return filters
        else:
            return []

# 测试代码
if __name__ == "__main__":
    # 测试数据库操作
    db = Database("test_stocks.db")
    
    # 创建表结构
    db.create_tables()
    
    # 测试插入一些示例数据
    import numpy as np
    
    # 生成示例股票列表
    stock_list = pd.DataFrame({
        'code': ['600000', '600001', '600002'],
        'name': ['浦发银行', '邯郸钢铁', '齐鲁石化']
    })
    
    db.save_stock_list(stock_list)
    
    # 生成示例日线数据
    dates = pd.date_range(start='2023-01-01', periods=10, freq='D')
    for code in stock_list['code']:
        # 生成随机价格数据
        close = np.random.normal(100, 5, 10).cumsum() + 1000
        high = close + np.random.uniform(0, 10, 10)
        low = close - np.random.uniform(0, 10, 10)
        open_price = close.copy()
        np.random.shuffle(open_price)
        volume = np.random.uniform(1000000, 10000000, 10)
        
        # 创建DataFrame
        daily_data = pd.DataFrame({
            'date': dates,
            'open': open_price,
            'high': high,
            'low': low,
            'close': close,
            'volume': volume,
            'amount': volume * close,
            'pct_change': np.random.uniform(-5, 5, 10),
            'turnover': np.random.uniform(0, 5, 10)
        })
        
        # 保存日线数据
        db.save_daily_data(code, daily_data)
        
        # 计算技术指标
        from data.stock_indicators import TechnicalIndicators
        indicators = TechnicalIndicators.calculate_all_indicators(daily_data)
        
        # 保存技术指标
        db.save_technical_indicators(code, indicators)
    
    # 测试查询
    print("\n获取股票信息:")
    stock = db.get_stock_by_code('600000')
    print(stock)
    
    print("\n获取日线数据:")
    daily_data = db.get_daily_data('600000')
    print(daily_data.head())
    
    print("\n获取技术指标:")
    indicators = db.get_technical_indicators('600000')
    print(indicators.head())
    
    # 关闭数据库连接
    db.disconnect()
