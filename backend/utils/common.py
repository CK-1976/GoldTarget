import os
import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, date
import pandas as pd

logger = logging.getLogger(__name__)

class DateTimeEncoder(json.JSONEncoder):
    """用于JSON序列化DateTime对象的encoder"""
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)

def save_json(data: Dict[str, Any], filepath: str) -> bool:
    """保存数据到JSON文件
    
    Args:
        data: 要保存的数据字典
        filepath: 文件路径
        
    Returns:
        是否保存成功
    """
    try:
        # 确保目录存在
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2, cls=DateTimeEncoder)
        
        logger.info(f"数据成功保存到 {filepath}")
        return True
    except Exception as e:
        logger.error(f"保存JSON数据失败: {e}")
        return False

def load_json(filepath: str) -> Optional[Dict[str, Any]]:
    """从JSON文件加载数据
    
    Args:
        filepath: 文件路径
        
    Returns:
        加载的数据字典，失败则返回None
    """
    try:
        if not os.path.exists(filepath):
            logger.warning(f"文件不存在: {filepath}")
            return None
        
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        logger.info(f"数据成功从 {filepath} 加载")
        return data
    except Exception as e:
        logger.error(f"加载JSON数据失败: {e}")
        return None

def ensure_dir(directory: str) -> bool:
    """确保目录存在，如果不存在则创建
    
    Args:
        directory: 目录路径
        
    Returns:
        是否成功创建或确认目录存在
    """
    try:
        os.makedirs(directory, exist_ok=True)
        return True
    except Exception as e:
        logger.error(f"创建目录失败: {e}")
        return False

def dataframe_to_dict_list(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """将DataFrame转换为字典列表
    
    Args:
        df: 要转换的DataFrame
        
    Returns:
        转换后的字典列表
    """
    if df.empty:
        return []
    
    return json.loads(df.to_json(orient='records', date_format='iso'))

def round_price_data(df: pd.DataFrame) -> pd.DataFrame:
    """对价格数据进行四舍五入，保留小数点后两位
    
    Args:
        df: 价格数据DataFrame
        
    Returns:
        处理后的DataFrame
    """
    price_columns = ['open', 'high', 'low', 'close']
    other_columns = ['pct_change', 'turnover']
    
    result = df.copy()
    
    # 价格列保留两位小数
    for col in price_columns:
        if col in result.columns:
            result[col] = result[col].round(2)
    
    # 其他需要处理的列
    for col in other_columns:
        if col in result.columns:
            result[col] = result[col].round(2)
    
    return result

def get_date_range(start_date: Optional[str] = None, end_date: Optional[str] = None, days: int = 30) -> tuple:
    """获取日期范围
    
    Args:
        start_date: 开始日期，格式为'YYYYMMDD'或'YYYY-MM-DD'
        end_date: 结束日期，格式为'YYYYMMDD'或'YYYY-MM-DD'
        days: 如果没有指定开始日期，从结束日期往前推的天数
        
    Returns:
        (start_date, end_date) 格式化后的日期范围元组，格式为'YYYYMMDD'
    """
    today = datetime.now()
    
    # 处理结束日期
    if end_date is None:
        end_date = today.strftime('%Y%m%d')
    else:
        # 统一格式为'YYYYMMDD'
        if '-' in end_date:
            end_date = end_date.replace('-', '')
    
    # 处理开始日期
    if start_date is None:
        # 计算从结束日期往前推days天
        end_dt = datetime.strptime(end_date, '%Y%m%d')
        start_dt = end_dt - pd.Timedelta(days=days)
        start_date = start_dt.strftime('%Y%m%d')
    else:
        # 统一格式为'YYYYMMDD'
        if '-' in start_date:
            start_date = start_date.replace('-', '')
    
    return start_date, end_date
