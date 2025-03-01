import pandas as pd
import numpy as np
import logging
from typing import List, Dict, Any, Optional, Union

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StockFilter:
    """股票筛选类"""
    
    def __init__(self, db_connection):
        """初始化筛选器
        
        Args:
            db_connection: 数据库连接对象
        """
        self.db = db_connection
        logger.info("股票筛选器初始化完成")
    
    def filter_by_price(self, min_price: Optional[float] = None, max_price: Optional[float] = None) -> List[str]:
        """根据价格范围筛选股票
        
        Args:
            min_price: 最低价格，如果为None则不设下限
            max_price: 最高价格，如果为None则不设上限
            
        Returns:
            符合条件的股票代码列表
        """
        logger.info(f"根据价格范围筛选: {min_price} - {max_price}")
        
        query = """
        SELECT DISTINCT code FROM daily_data 
        WHERE date = (SELECT MAX(date) FROM daily_data)
        """
        
        params = []
        if min_price is not None:
            query += " AND close >= ?"
            params.append(min_price)
        
        if max_price is not None:
            query += " AND close <= ?"
            params.append(max_price)
            
        result = self.db.execute(query, tuple(params))
        stock_codes = [row[0] for row in result] if result else []
        
        logger.info(f"价格范围筛选结果: {len(stock_codes)} 只股票")
        return stock_codes
    
    def filter_by_ma(self, period: int = 20, relation: str = 'above', price_column: str = 'close') -> List[str]:
        """根据均线关系筛选股票
        
        Args:
            period: 均线周期
            relation: 关系类型，可选: 'above'(价格在均线上方), 'below'(价格在均线下方), 
                     'cross_above'(价格上穿均线), 'cross_below'(价格下穿均线)
            price_column: 价格列名
            
        Returns:
            符合条件的股票代码列表
        """
        logger.info(f"根据均线关系筛选: MA{period}, 关系={relation}")
        
        # 获取所有股票的最近数据
        ma_column = f'ma{period}'
        
        if relation in ['above', 'below']:
            # 静态关系筛选
            query = f"""
            SELECT DISTINCT d.code 
            FROM daily_data d
            JOIN technical_indicators t ON d.code = t.code AND d.date = t.date
            WHERE d.date = (SELECT MAX(date) FROM daily_data)
            """
            
            if relation == 'above':
                query += f" AND d.{price_column} > t.{ma_column}"
            else:  # below
                query += f" AND d.{price_column} < t.{ma_column}"
        
        elif relation in ['cross_above', 'cross_below']:
            # 动态关系筛选（需要两天数据）
            # 获取最近两天的日期
            dates = self.db.execute("""
            SELECT DISTINCT date FROM daily_data
            ORDER BY date DESC
            LIMIT 2
            """)
            
            if not dates or len(dates) < 2:
                logger.warning("数据不足，无法进行均线穿越筛选")
                return []
            
            latest_date, prev_date = dates[0][0], dates[1][0]
            
            query = f"""
            SELECT DISTINCT d1.code
            FROM daily_data d1
            JOIN technical_indicators t1 ON d1.code = t1.code AND d1.date = t1.date
            JOIN daily_data d2 ON d1.code = d2.code AND d2.date = ?
            JOIN technical_indicators t2 ON d2.code = t2.code AND d2.date = t2.date
            WHERE d1.date = ?
            """
            
            params = []
            if relation == 'cross_above':
                # 昨天价格低于均线，今天价格高于均线
                query += f" AND d2.{price_column} < t2.{ma_column} AND d1.{price_column} > t1.{ma_column}"
            else:  # cross_below
                # 昨天价格高于均线，今天价格低于均线
                query += f" AND d2.{price_column} > t2.{ma_column} AND d1.{price_column} < t1.{ma_column}"
            
            params = [prev_date, latest_date]
        else:
            logger.error(f"不支持的均线关系类型: {relation}")
            return []
            
        result = self.db.execute(query, tuple(params) if 'params' in locals() else ())
        stock_codes = [row[0] for row in result] if result else []
        
        logger.info(f"均线关系筛选结果: {len(stock_codes)} 只股票")
        return stock_codes
    
    def filter_by_rsi(self, period: int = 14, min_value: Optional[float] = None, 
                    max_value: Optional[float] = None) -> List[str]:
        """根据RSI值筛选股票
        
        Args:
            period: RSI周期
            min_value: 最小RSI值
            max_value: 最大RSI值
            
        Returns:
            符合条件的股票代码列表
        """
        logger.info(f"根据RSI{period}筛选: {min_value} - {max_value}")
        
        rsi_column = f'rsi{period}'
        
        query = f"""
        SELECT DISTINCT code 
        FROM technical_indicators
        WHERE date = (SELECT MAX(date) FROM technical_indicators)
        """
        
        params = []
        if min_value is not None:
            query += f" AND {rsi_column} >= ?"
            params.append(min_value)
        
        if max_value is not None:
            query += f" AND {rsi_column} <= ?"
            params.append(max_value)
            
        result = self.db.execute(query, tuple(params))
        stock_codes = [row[0] for row in result] if result else []
        
        logger.info(f"RSI筛选结果: {len(stock_codes)} 只股票")
        return stock_codes
    
    def filter_by_macd(self, condition: str) -> List[str]:
        """根据MACD条件筛选股票
        
        Args:
            condition: MACD条件, 可选: 
                      'golden_cross'（金叉，DIFF上穿DEA）, 
                      'dead_cross'（死叉，DIFF下穿DEA）,
                      'positive'（DIFF和MACD柱同为正）,
                      'negative'（DIFF和MACD柱同为负）
            
        Returns:
            符合条件的股票代码列表
        """
        logger.info(f"根据MACD条件筛选: {condition}")
        
        # 获取最近两天的日期（用于判断金叉死叉）
        dates = self.db.execute("""
        SELECT DISTINCT date FROM technical_indicators
        ORDER BY date DESC
        LIMIT 2
        """)
        
        if not dates or len(dates) < 2:
            logger.warning("数据不足，无法进行MACD筛选")
            return []
        
        latest_date, prev_date = dates[0][0], dates[1][0]
        
        if condition == 'golden_cross':
            # 金叉：前一天DIFF < DEA，当天DIFF > DEA
            query = """
            SELECT DISTINCT t1.code
            FROM technical_indicators t1
            JOIN technical_indicators t2 ON t1.code = t2.code
            WHERE t1.date = ? AND t2.date = ?
            AND t2.macd_dif < t2.macd_dea
            AND t1.macd_dif > t1.macd_dea
            """
            params = (latest_date, prev_date)
            
        elif condition == 'dead_cross':
            # 死叉：前一天DIFF > DEA，当天DIFF < DEA
            query = """
            SELECT DISTINCT t1.code
            FROM technical_indicators t1
            JOIN technical_indicators t2 ON t1.code = t2.code
            WHERE t1.date = ? AND t2.date = ?
            AND t2.macd_dif > t2.macd_dea
            AND t1.macd_dif < t1.macd_dea
            """
            params = (latest_date, prev_date)
            
        elif condition == 'positive':
            # DIFF和MACD柱同为正
            query = """
            SELECT DISTINCT code
            FROM technical_indicators
            WHERE date = ?
            AND macd_dif > 0 AND macd_hist > 0
            """
            params = (latest_date,)
            
        elif condition == 'negative':
            # DIFF和MACD柱同为负
            query = """
            SELECT DISTINCT code
            FROM technical_indicators
            WHERE date = ?
            AND macd_dif < 0 AND macd_hist < 0
            """
            params = (latest_date,)
            
        else:
            logger.error(f"不支持的MACD条件: {condition}")
            return []
            
        result = self.db.execute(query, params)
        stock_codes = [row[0] for row in result] if result else []
        
        logger.info(f"MACD条件筛选结果: {len(stock_codes)} 只股票")
        return stock_codes
    
    def filter_by_kdj(self, condition: str) -> List[str]:
        """根据KDJ条件筛选股票
        
        Args:
            condition: KDJ条件, 可选:
                      'golden_cross'（金叉，K上穿D）,
                      'dead_cross'（死叉，K下穿D）,
                      'oversold'（超卖，K和D都小于20）,
                      'overbought'（超买，K和D都大于80）
            
        Returns:
            符合条件的股票代码列表
        """
        logger.info(f"根据KDJ条件筛选: {condition}")
        
        # 获取最近两天的日期（用于判断金叉死叉）
        dates = self.db.execute("""
        SELECT DISTINCT date FROM technical_indicators
        ORDER BY date DESC
        LIMIT 2
        """)
        
        if not dates or len(dates) < 2:
            logger.warning("数据不足，无法进行KDJ筛选")
            return []
        
        latest_date, prev_date = dates[0][0], dates[1][0]
        
        if condition == 'golden_cross':
            # 金叉：前一天K < D，当天K > D
            query = """
            SELECT DISTINCT t1.code
            FROM technical_indicators t1
            JOIN technical_indicators t2 ON t1.code = t2.code
            WHERE t1.date = ? AND t2.date = ?
            AND t2.k < t2.d
            AND t1.k > t1.d
            """
            params = (latest_date, prev_date)
            
        elif condition == 'dead_cross':
            # 死叉：前一天K > D，当天K < D
            query = """
            SELECT DISTINCT t1.code
            FROM technical_indicators t1
            JOIN technical_indicators t2 ON t1.code = t2.code
            WHERE t1.date = ? AND t2.date = ?
            AND t2.k > t2.d
            AND t1.k < t1.d
            """
            params = (latest_date, prev_date)
            
        elif condition == 'oversold':
            # 超卖：K和D都小于20
            query = """
            SELECT DISTINCT code
            FROM technical_indicators
            WHERE date = ?
            AND k < 20 AND d < 20
            """
            params = (latest_date,)
            
        elif condition == 'overbought':
            # 超买：K和D都大于80
            query = """
            SELECT DISTINCT code
            FROM technical_indicators
            WHERE date = ?
            AND k > 80 AND d > 80
            """
            params = (latest_date,)
            
        else:
            logger.error(f"不支持的KDJ条件: {condition}")
            return []
            
        result = self.db.execute(query, params)
        stock_codes = [row[0] for row in result] if result else []
        
        logger.info(f"KDJ条件筛选结果: {len(stock_codes)} 只股票")
        return stock_codes
    
    def filter_by_volume(self, condition: str, threshold: Optional[float] = None) -> List[str]:
        """根据成交量条件筛选股票
        
        Args:
            condition: 成交量条件, 可选:
                      'increase'（成交量增加，相比昨日增加threshold%）,
                      'decrease'（成交量减少，相比昨日减少threshold%）,
                      'above_ma'（成交量高于N日均量线）,
                      'below_ma'（成交量低于N日均量线）
            threshold: 阈值，对于'increase'/'decrease'是百分比，对于'above_ma'/'below_ma'是天数
            
        Returns:
            符合条件的股票代码列表
        """
        logger.info(f"根据成交量条件筛选: {condition}, 阈值={threshold}")
        
        # 获取最近两天的日期
        dates = self.db.execute("""
        SELECT DISTINCT date FROM daily_data
        ORDER BY date DESC
        LIMIT 2
        """)
        
        if not dates or len(dates) < 2:
            logger.warning("数据不足，无法进行成交量筛选")
            return []
        
        latest_date, prev_date = dates[0][0], dates[1][0]
        
        if condition in ['increase', 'decrease']:
            # 成交量增加/减少
            query = """
            SELECT DISTINCT d1.code
            FROM daily_data d1
            JOIN daily_data d2 ON d1.code = d2.code
            WHERE d1.date = ? AND d2.date = ?
            """
            
            if threshold is None:
                threshold = 20  # 默认阈值20%
            
            if condition == 'increase':
                # 成交量增加threshold%
                query += " AND d1.volume > d2.volume * (1 + ?/100)"
            else:  # decrease
                # 成交量减少threshold%
                query += " AND d1.volume < d2.volume * (1 - ?/100)"
            
            params = (latest_date, prev_date, threshold)
            
        elif condition in ['above_ma', 'below_ma']:
            # 成交量高于/低于N日均量线
            if threshold is None or threshold <= 0:
                threshold = 5  # 默认5日均量线
            
            # 计算N日均量
            avg_query = f"""
            WITH volume_data AS (
                SELECT code, date, volume,
                       AVG(volume) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN {int(threshold)-1} PRECEDING AND CURRENT ROW) as avg_volume
                FROM daily_data
            )
            SELECT DISTINCT code
            FROM volume_data
            WHERE date = ?
            """
            
            if condition == 'above_ma':
                avg_query += " AND volume > avg_volume"
            else:  # below_ma
                avg_query += " AND volume < avg_volume"
            
            params = (latest_date,)
            query = avg_query
            
        else:
            logger.error(f"不支持的成交量条件: {condition}")
            return []
            
        result = self.db.execute(query, params)
        stock_codes = [row[0] for row in result] if result else []
        
        logger.info(f"成交量条件筛选结果: {len(stock_codes)} 只股票")
        return stock_codes
    
    def filter_by_bollinger_bands(self, condition: str) -> List[str]:
        """根据布林带条件筛选股票
        
        Args:
            condition: 布林带条件, 可选:
                      'upper_break'（突破上轨）,
                      'lower_break'（突破下轨）,
                      'middle_cross_above'（向上穿越中轨）,
                      'middle_cross_below'（向下穿越中轨）,
                      'squeeze'（带宽收窄，振幅变小）
            
        Returns:
            符合条件的股票代码列表
        """
        logger.info(f"根据布林带条件筛选: {condition}")
        
        # 获取最近两天的日期
        dates = self.db.execute("""
        SELECT DISTINCT date FROM technical_indicators
        ORDER BY date DESC
        LIMIT 2
        """)
        
        if not dates or len(dates) < 2:
            logger.warning("数据不足，无法进行布林带筛选")
            return []
        
        latest_date, prev_date = dates[0][0], dates[1][0]
        
        if condition == 'upper_break':
            # 突破上轨：前一天收盘价低于上轨，当天收盘价高于上轨
            query = """
            SELECT DISTINCT t1.code
            FROM technical_indicators t1
            JOIN technical_indicators t2 ON t1.code = t2.code
            JOIN daily_data d1 ON t1.code = d1.code AND t1.date = d1.date
            JOIN daily_data d2 ON t2.code = d2.code AND t2.date = d2.date
            WHERE t1.date = ? AND t2.date = ?
            AND d2.close < t2.bb_upper
            AND d1.close > t1.bb_upper
            """
            params = (latest_date, prev_date)
            
        elif condition == 'lower_break':
            # 突破下轨：前一天收盘价高于下轨，当天收盘价低于下轨
            query = """
            SELECT DISTINCT t1.code
            FROM technical_indicators t1
            JOIN technical_indicators t2 ON t1.code = t2.code
            JOIN daily_data d1 ON t1.code = d1.code AND t1.date = d1.date
            JOIN daily_data d2 ON t2.code = d2.code AND t2.date = d2.date
            WHERE t1.date = ? AND t2.date = ?
            AND d2.close > t2.bb_lower
            AND d1.close < t1.bb_lower
            """
            params = (latest_date, prev_date)
            
        elif condition == 'middle_cross_above':
            # 向上穿越中轨：前一天收盘价低于中轨，当天收盘价高于中轨
            query = """
            SELECT DISTINCT t1.code
            FROM technical_indicators t1
            JOIN technical_indicators t2 ON t1.code = t2.code
            JOIN daily_data d1 ON t1.code = d1.code AND t1.date = d1.date
            JOIN daily_data d2 ON t2.code = d2.code AND t2.date = d2.date
            WHERE t1.date = ? AND t2.date = ?
            AND d2.close < t2.bb_middle
            AND d1.close > t1.bb_middle
            """
            params = (latest_date, prev_date)
            
        elif condition == 'middle_cross_below':
            # 向下穿越中轨：前一天收盘价高于中轨，当天收盘价低于中轨
            query = """
            SELECT DISTINCT t1.code
            FROM technical_indicators t1
            JOIN technical_indicators t2 ON t1.code = t2.code
            JOIN daily_data d1 ON t1.code = d1.code AND t1.date = d1.date
            JOIN daily_data d2 ON t2.code = d2.code AND t2.date = d2.date
            WHERE t1.date = ? AND t2.date = ?
            AND d2.close > t2.bb_middle
            AND d1.close < t1.bb_middle
            """
            params = (latest_date, prev_date)
            
        elif condition == 'squeeze':
            # 带宽收窄：当天带宽小于前一天带宽的90%
            query = """
            SELECT DISTINCT t1.code
            FROM technical_indicators t1
            JOIN technical_indicators t2 ON t1.code = t2.code
            WHERE t1.date = ? AND t2.date = ?
            AND (t1.bb_upper - t1.bb_lower) < (t2.bb_upper - t2.bb_lower) * 0.9
            """
            params = (latest_date, prev_date)
            
        else:
            logger.error(f"不支持的布林带条件: {condition}")
            return []
            
        result = self.db.execute(query, params)
        stock_codes = [row[0] for row in result] if result else []
        
        logger.info(f"布林带条件筛选结果: {len(stock_codes)} 只股票")
        return stock_codes
    
    def combine_filters(self, filter_results: List[List[str]], logic: str = 'and') -> List[str]:
        """组合多个筛选结果
        
        Args:
            filter_results: 多个筛选结果列表
            logic: 逻辑关系，'and'表示交集，'or'表示并集
            
        Returns:
            组合后的股票代码列表
        """
        if not filter_results:
            return []
        
        if len(filter_results) == 1:
            return filter_results[0]
        
        if logic == 'and':
            # 交集运算
            result = set(filter_results[0])
            for codes in filter_results[1:]:
                result = result.intersection(set(codes))
            return list(result)
        else:  # 'or'
            # 并集运算
            result = set()
            for codes in filter_results:
                result = result.union(set(codes))
            return list(result)
    
    def apply_filters(self, filter_config: Dict[str, Any]) -> List[str]:
        """应用筛选配置
        
        Args:
            filter_config: 筛选配置字典，包含多个筛选条件
            
        Returns:
            符合所有条件的股票代码列表
        """
        logger.info(f"应用筛选配置: {filter_config}")
        
        filter_results = []
        
        # 价格筛选
        if 'price' in filter_config:
            price_min = filter_config['price'].get('min')
            price_max = filter_config['price'].get('max')
            if price_min is not None or price_max is not None:
                result = self.filter_by_price(price_min, price_max)
                filter_results.append(result)
        
        # 均线筛选
        if 'ma' in filter_config:
            ma_period = filter_config['ma'].get('period', 20)
            ma_relation = filter_config['ma'].get('relation', 'above')
            result = self.filter_by_ma(ma_period, ma_relation)
            filter_results.append(result)
        
        # RSI筛选
        if 'rsi' in filter_config:
            rsi_period = filter_config['rsi'].get('period', 14)
            rsi_min = filter_config['rsi'].get('min')
            rsi_max = filter_config['rsi'].get('max')
            if rsi_min is not None or rsi_max is not None:
                result = self.filter_by_rsi(rsi_period, rsi_min, rsi_max)
                filter_results.append(result)
        
        # MACD筛选
        if 'macd' in filter_config:
            macd_condition = filter_config['macd'].get('condition', 'golden_cross')
            result = self.filter_by_macd(macd_condition)
            filter_results.append(result)
        
        # KDJ筛选
        if 'kdj' in filter_config:
            kdj_condition = filter_config['kdj'].get('condition', 'golden_cross')
            result = self.filter_by_kdj(kdj_condition)
            filter_results.append(result)
        
        # 成交量筛选
        if 'volume' in filter_config:
            volume_condition = filter_config['volume'].get('condition', 'increase')
            volume_threshold = filter_config['volume'].get('threshold')
            result = self.filter_by_volume(volume_condition, volume_threshold)
            filter_results.append(result)
        
        # 布林带筛选
        if 'bollinger' in filter_config:
            bb_condition = filter_config['bollinger'].get('condition', 'upper_break')
            result = self.filter_by_bollinger_bands(bb_condition)
            filter_results.append(result)
        
        # 组合筛选结果
        logic = filter_config.get('logic', 'and')
        final_result = self.combine_filters(filter_results, logic)
        
        logger.info(f"最终筛选结果: {len(final_result)} 只股票")
        return final_result
    
    def get_filter_result_details(self, stock_codes: List[str]) -> pd.DataFrame:
        """获取筛选结果的详细信息
        
        Args:
            stock_codes: 股票代码列表
            
        Returns:
            包含股票详细信息的DataFrame
        """
        if not stock_codes:
            return pd.DataFrame()
        
        # 获取最近的交易日期
        latest_date = self.db.execute("""
        SELECT MAX(date) FROM daily_data
        """)[0][0]
        
        # 构建查询参数
        placeholders = ','.join(['?'] * len(stock_codes))
        
        # 查询股票的详细信息
        query = f"""
        SELECT s.code, s.name, s.industry, d.date, d.open, d.high, d.low, d.close, 
               d.volume, d.pct_change, t.ma5, t.ma10, t.ma20, t.rsi6, t.rsi12, t.rsi24
        FROM stocks s
        JOIN daily_data d ON s.code = d.code
        JOIN technical_indicators t ON d.code = t.code AND d.date = t.date
        WHERE s.code IN ({placeholders}) AND d.date = ?
        ORDER BY d.pct_change DESC
        """
        
        params = stock_codes + [latest_date]
        result = self.db.execute(query, tuple(params))
        
        if not result:
            return pd.DataFrame()
        
        # 构建DataFrame
        columns = ['code', 'name', 'industry', 'date', 'open', 'high', 'low', 'close', 
                   'volume', 'pct_change', 'ma5', 'ma10', 'ma20', 'rsi6', 'rsi12', 'rsi24']
        
        df = pd.DataFrame(result, columns=columns)
        
        return df

# 测试代码
if __name__ == "__main__":
    from database import Database
    
    # 创建数据库连接
    db = Database("test_stocks.db")
    db.connect()
    
    # 创建股票筛选器
    filter_engine = StockFilter(db)
    
    # 测试价格筛选
    price_result = filter_engine.filter_by_price(10, 50)
    print(f"价格筛选结果：{len(price_result)} 只股票")
    
    # 测试均线筛选
    ma_result = filter_engine.filter_by_ma(period=5, relation='above')
    print(f"均线筛选结果：{len(ma_result)} 只股票")
    
    # 测试组合筛选
    combined_result = filter_engine.combine_filters([price_result, ma_result], 'and')
    print(f"组合筛选结果：{len(combined_result)} 只股票")
    
    # 测试筛选配置
    filter_config = {
        'price': {'min': 10, 'max': 50},
        'ma': {'period': 5, 'relation': 'above'},
        'rsi': {'period': 6, 'min': 30, 'max': 70},
        'logic': 'and'
    }
    
    config_result = filter_engine.apply_filters(filter_config)
    print(f"应用筛选配置结果：{len(config_result)} 只股票")
    
    # 关闭数据库连接
    db.disconnect()
