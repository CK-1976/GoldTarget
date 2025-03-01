from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 创建FastAPI实例
app = FastAPI(
    title="GoldTarget API",
    description="股票筛选应用后端API",
    version="0.1.0"
)

# 允许跨域请求
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 在生产环境中应该限制来源
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 导入路由
from api.stock_router import router as stock_router
app.include_router(stock_router)

@app.get("/")
async def root():
    return {"message": "GoldTarget 股票筛选应用API"}

@app.get("/health")
async def health_check():
    return {"status": "ok"}

if __name__ == "__main__":
    # 获取端口，默认为8000
    port = int(os.getenv("PORT", 8000))
    
    # 启动服务器
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)