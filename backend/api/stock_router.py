from fastapi import APIRouter, Query, HTTPException, Depends
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
import pandas as pd
import json
from datetime import datetime
import logging

# 导入自定义模块
from models.database import Database
from models.stock_filter import StockFilter
from data.stock_data import StockDataFetcher
from data.stock_indicators import TechnicalIndicators

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 创建路由
router = APIRouter(prefix="/api/stocks", tags=["stocks"])

# 创建数据库连接和筛选器的依赖
def get_db():
    db = Database()
    db.connect()
    try:
        yield db
    finally:
        db.disconnect()

def get_filter(db: Database = Depends(get_db)):
    return StockFilter(db)

def get_data_fetcher():
    return StockDataFetcher()

# 模型定义
class StockInfo(BaseModel):
    code: str
    name: str
    industry: Optional[str] = None
    market: Optional[str] = None
    list_date: Optional[str] = None

class StockDailyData(BaseModel):
    code: str
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    amount: Optional[float] = None
    pct_change: Optional[float] = None
    turnover: Optional[float] = None

class StockIndicator(BaseModel):
    code: str
    date: str
    ma5: Optional[float] = None
    ma10: Optional[float] = None
    ma20: Optional[float] = None
    ma60: Optional[float] = None
    rsi6: Optional[float] = None
    rsi12: Optional[float] = None
    rsi24: Optional[float] = None
    macd_dif: Optional[float] = None
    macd_dea: Optional[float] = None
    macd_hist: Optional[float] = None
    k: Optional[float] = None
    d: Optional[float] = None
    j: Optional[float] = None

class PriceFilter(BaseModel):
    min: Optional[float] = None
    max: Optional[float] = None

class MAFilter(BaseModel):
    period: int = Field(20, ge=5, le=120)
    relation: str = Field('above', pattern='^(above|below|cross_above|cross_below)$')

class RSIFilter(BaseModel):
    period: int = Field(14, ge=6, le=24)
    min: Optional[float] = Field(None, ge=0, le=100)
    max: Optional[float] = Field(None, ge=0, le=100)

class MACDFilter(BaseModel):
    condition: str = Field('golden_cross', pattern='^(golden_cross|dead_cross|positive|negative)$')

class KDJFilter(BaseModel):
    condition: str = Field('golden_cross', pattern='^(golden_cross|dead_cross|oversold|overbought)$')

class VolumeFilter(BaseModel):
    condition: str = Field('increase', pattern='^(increase|decrease|above_ma|below_ma)$')
    threshold: Optional[float] = None

class BollingerFilter(BaseModel):
    condition: str = Field('upper_break', pattern='^(upper_break|lower_break|middle_cross_above|middle_cross_below|squeeze)$')

class StockFilterParams(BaseModel):
    price: Optional[PriceFilter] = None
    ma: Optional[MAFilter] = None
    rsi: Optional[RSIFilter] = None
    macd: Optional[MACDFilter] = None
    kdj: Optional[KDJFilter] = None
    volume: Optional[VolumeFilter] = None
    bollinger: Optional[BollingerFilter] = None
    logic: str = Field('and', pattern='^(and|or)$')

class FilterResponse(BaseModel):
    count: int
    stocks: List[Dict[str, Any]]

class SavedFilter(BaseModel):
    name: str
    filter_json: str

class SavedFilterResponse(BaseModel):
    id: int
    name: str
    filter_json: str
    create_time: str
    update_time: Optional[str] = None

# 路由定义
@router.get("/list", response_model=List[StockInfo])
async def get_stock_list(
    industry: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: Database = Depends(get_db)
):
    """获取股票列表，支持分页和行业筛选"""
    try:
        # 构建查询
        query = "SELECT code, name, industry, market, list_date FROM stocks"
        params = []
        
        if industry:
            query += " WHERE industry = ?"
            params.append(industry)
        
        # 分页
        query += " ORDER BY code LIMIT ? OFFSET ?"
        params.append(page_size)
        params.append((page - 1) * page_size)
        
        # 执行查询
        result = db.execute(query, tuple(params))
        
        if not result:
            return []
        
        stocks = []
        for row in result:
            stocks.append({
                "code": row[0],
                "name": row[1],
                "industry": row[2],
                "market": row[3],
                "list_date": row[4]
            })
        
        return stocks
    except Exception as e:
        logger.error(f"获取股票列表失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取股票列表失败: {str(e)}")

@router.get("/info/{code}", response_model=StockInfo)
async def get_stock_info(
    code: str,
    db: Database = Depends(get_db)
):
    """获取单个股票的基本信息"""
    try:
        result = db.get_stock_by_code(code)
        
        if not result:
            raise HTTPException(status_code=404, detail=f"未找到股票代码: {code}")
        
        return result
    except Exception as e:
        logger.error(f"获取股票信息失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取股票信息失败: {str(e)}")

@router.get("/daily/{code}", response_model=List[StockDailyData])
async def get_stock_daily_data(
    code: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = Query(30, ge=1, le=365),
    db: Database = Depends(get_db)
):
    """获取股票的日线数据"""
    try:
        # 从数据库获取日线数据
        df = db.get_daily_data(code, start_date, end_date)
        
        if df.empty:
            # 如果数据库没有数据，尝试从API获取
            fetcher = get_data_fetcher()
            
            # 设置默认日期范围
            if not end_date:
                end_date = datetime.now().strftime('%Y%m%d')
            if not start_date:
                start_date = (datetime.strptime(end_date, '%Y%m%d') - pd.Timedelta(days=365)).strftime('%Y%m%d')
            
            df = fetcher.get_daily_data(code, start_date, end_date)
            
            if not df.empty:
                # 保存到数据库
                db.save_daily_data(code, df)
                
                # 计算技术指标
                indicators_df = TechnicalIndicators.calculate_all_indicators(df)
                db.save_technical_indicators(code, indicators_df)
        
        if df.empty:
            return []
        
        # 限制返回条数
        if len(df) > limit:
            df = df.iloc[-limit:]
        
        # 转换为API响应格式
        result = []
        for _, row in df.iterrows():
            result.append({
                "code": code,
                "date": row['date'] if isinstance(row['date'], str) else row['date'].strftime('%Y-%m-%d'),
                "open": row['open'],
                "high": row['high'],
                "low": row['low'],
                "close": row['close'],
                "volume": row['volume'],
                "amount": row.get('amount'),
                "pct_change": row.get('pct_change'),
                "turnover": row.get('turnover')
            })
        
        return result
    except Exception as e:
        logger.error(f"获取日线数据失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取日线数据失败: {str(e)}")

@router.get("/indicators/{code}", response_model=List[StockIndicator])
async def get_stock_indicators(
    code: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = Query(30, ge=1, le=365),
    db: Database = Depends(get_db)
):
    """获取股票的技术指标数据"""
    try:
        # 从数据库获取技术指标数据
        df = db.get_technical_indicators(code, start_date, end_date)
        
        if df.empty:
            # 如果技术指标为空，先尝试获取日线数据
            daily_df = db.get_daily_data(code, start_date, end_date)
            
            if daily_df.empty:
                # 如果日线数据也为空，从API获取
                fetcher = get_data_fetcher()
                
                # 设置默认日期范围
                if not end_date:
                    end_date = datetime.now().strftime('%Y%m%d')
                if not start_date:
                    start_date = (datetime.strptime(end_date, '%Y%m%d') - pd.Timedelta(days=365)).strftime('%Y%m%d')
                
                daily_df = fetcher.get_daily_data(code, start_date, end_date)
                
                if not daily_df.empty:
                    # 保存日线数据到数据库
                    db.save_daily_data(code, daily_df)
            
            if not daily_df.empty:
                # 计算技术指标
                df = TechnicalIndicators.calculate_all_indicators(daily_df)
                db.save_technical_indicators(code, df)
        
        if df.empty:
            return []
        
        # 限制返回条数
        if len(df) > limit:
            df = df.iloc[-limit:]
        
        # 转换为API响应格式
        result = []
        for _, row in df.iterrows():
            result.append({
                "code": code,
                "date": row['date'] if isinstance(row['date'], str) else row['date'].strftime('%Y-%m-%d'),
                "ma5": row.get('MA5'),
                "ma10": row.get('MA10'),
                "ma20": row.get('MA20'),
                "ma60": row.get('MA60'),
                "rsi6": row.get('RSI6'),
                "rsi12": row.get('RSI12'),
                "rsi24": row.get('RSI24'),
                "macd_dif": row.get('MACD_DIF'),
                "macd_dea": row.get('MACD_DEA'),
                "macd_hist": row.get('MACD_HIST'),
                "k": row.get('K'),
                "d": row.get('D'),
                "j": row.get('J')
            })
        
        return result
    except Exception as e:
        logger.error(f"获取技术指标数据失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取技术指标数据失败: {str(e)}")

@router.post("/filter", response_model=FilterResponse)
async def filter_stocks(
    filter_params: StockFilterParams,
    filter_engine: StockFilter = Depends(get_filter),
    db: Database = Depends(get_db)
):
    """根据多种条件筛选股票"""
    try:
        # 将筛选参数转换为字典
        filter_config = filter_params.dict(exclude_none=True)
        
        # 应用筛选
        stock_codes = filter_engine.apply_filters(filter_config)
        
        if not stock_codes:
            return {"count": 0, "stocks": []}
        
        # 获取筛选结果的详细信息
        details_df = filter_engine.get_filter_result_details(stock_codes)
        
        # 转换为API响应格式
        stocks = []
        for _, row in details_df.iterrows():
            stocks.append({
                "code": row['code'],
                "name": row['name'],
                "industry": row['industry'],
                "date": row['date'] if isinstance(row['date'], str) else row['date'].strftime('%Y-%m-%d'),
                "close": row['close'],
                "pct_change": row['pct_change'],
                "ma5": row['ma5'],
                "ma10": row['ma10'],
                "ma20": row['ma20'],
                "rsi6": row['rsi6'],
                "rsi12": row['rsi12'],
                "rsi24": row['rsi24']
            })
        
        return {"count": len(stocks), "stocks": stocks}
    except Exception as e:
        logger.error(f"筛选股票失败: {e}")
        raise HTTPException(status_code=500, detail=f"筛选股票失败: {str(e)}")

@router.post("/filters/save", response_model=dict)
async def save_filter(
    filter_data: SavedFilter,
    db: Database = Depends(get_db)
):
    """保存筛选条件"""
    try:
        # 验证JSON格式
        try:
            json.loads(filter_data.filter_json)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="无效的筛选条件JSON格式")
        
        # 保存到数据库
        filter_id = db.save_filter(filter_data.name, filter_data.filter_json)
        
        if filter_id is None:
            raise HTTPException(status_code=500, detail="保存筛选条件失败")
        
        return {"id": filter_id, "message": "筛选条件保存成功"}
    except Exception as e:
        logger.error(f"保存筛选条件失败: {e}")
        raise HTTPException(status_code=500, detail=f"保存筛选条件失败: {str(e)}")

@router.get("/filters", response_model=List[SavedFilterResponse])
async def get_saved_filters(
    db: Database = Depends(get_db)
):
    """获取所有保存的筛选条件"""
    try:
        filters = db.get_saved_filters()
        return filters
    except Exception as e:
        logger.error(f"获取筛选条件失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取筛选条件失败: {str(e)}")

@router.get("/industries", response_model=List[str])
async def get_industries(
    db: Database = Depends(get_db)
):
    """获取所有行业分类"""
    try:
        result = db.execute("SELECT DISTINCT industry FROM stocks WHERE industry IS NOT NULL AND industry != ''")
        
        if not result:
            return []
        
        return [row[0] for row in result]
    except Exception as e:
        logger.error(f"获取行业分类失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取行业分类失败: {str(e)}")

@router.get("/update/{code}")
async def update_stock_data(
    code: str,
    days: int = Query(30, ge=1, le=365),
    db: Database = Depends(get_db)
):
    """更新指定股票的数据"""
    try:
        fetcher = get_data_fetcher()
        
        # 更新日线数据
        daily_df = fetcher.update_stock_data(code, days)
        
        if daily_df.empty:
            return {"status": "error", "message": "获取股票数据失败"}
        
        # 保存日线数据
        db.save_daily_data(code, daily_df)
        
        # 计算并保存技术指标
        indicators_df = TechnicalIndicators.calculate_all_indicators(daily_df)
        db.save_technical_indicators(code, indicators_df)
        
        return {
            "status": "success", 
            "message": f"股票 {code} 数据更新成功",
            "data_count": len(daily_df)
        }
    except Exception as e:
        logger.error(f"更新股票数据失败: {e}")
        raise HTTPException(status_code=500, detail=f"更新股票数据失败: {str(e)}")

@router.get("/init")
async def initialize_database(
    db: Database = Depends(get_db)
):
    """初始化数据库并加载基础数据"""
    try:
        # 创建数据库表结构
        db.create_tables()
        
        # 获取股票列表
        fetcher = get_data_fetcher()
        stock_list = fetcher.get_stock_list()
        
        if stock_list.empty:
            return {"status": "error", "message": "获取股票列表失败"}
        
        # 保存股票列表
        db.save_stock_list(stock_list)
        
        return {
            "status": "success",
            "message": "数据库初始化成功",
            "stock_count": len(stock_list)
        }
    except Exception as e:
        logger.error(f"初始化数据库失败: {e}")
        raise HTTPException(status_code=500, detail=f"初始化数据库失败: {str(e)}")
