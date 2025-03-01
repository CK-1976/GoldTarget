
# GoldTarget 股票筛选应用

基于 Python 和 ArkTS 开发的鸿蒙生态股票筛选应用，提供高效、直观的股票筛选功能。

## 项目介绍

GoldTarget 是一款为鸿蒙系统打造的股票筛选应用，旨在为投资者提供便捷的投资决策工具。项目采用前后端分离架构，使用 Python 处理后端数据分析，ArkTS 实现鸿蒙前端界面开发。

### 主要功能

- 基于多维度指标的股票筛选
- 技术分析指标自动计算与可视化
- 个性化筛选条件设置与保存
- 适配鸿蒙系统多设备协同

## 系统架构

项目采用前后端分离架构：

- **后端服务**(Python)：负责数据采集、处理、分析和筛选逻辑
- **前端应用**(ArkTS)：负责UI展示和用户交互
- **数据交互**：通过RESTful API实现前后端通信

## 目录结构

```
├── AppScope/              # 鸿蒙应用域
│   ├── app.json5          # 应用配置
│   └── resources/         # 应用资源
├── backend/               # Python后端
│   ├── api/               # API接口
│   ├── data/              # 数据获取与处理
│   ├── models/            # 数据模型
│   └── utils/             # 工具函数
├── entry/                 # 鸿蒙应用入口
│   └── src/               # 前端源代码
│       └── main/          # 主要代码
│           ├── ets/       # ArkTS代码
│           └── resources/ # 前端资源
└── oh_modules/           # 鸿蒙模块依赖
```

## 技术栈

### 后端技术栈

- **编程语言**：Python 3.10+
- **数据获取**：Akshare (金融数据API)
- **数据处理**：Pandas, NumPy
- **后端框架**：FastAPI
- **数据存储**：SQLite

### 前端技术栈

- **开发语言**：ArkTS
- **开发框架**：鸿蒙应用开发框架
- **UI组件**：鸿蒙UI组件库

## 安装与使用

### 后端环境准备

1. 进入后端目录
```bash
cd backend
```

2. 安装依赖
```bash
pip install -r requirements.txt
```

3. 运行后端服务
```bash
python main.py
```

### 前端环境准备

1. 安装DevEco Studio和HarmonyOS SDK
2. 在DevEco Studio中打开项目
3. 构建并运行应用

## API接口文档

启动后端服务后，可以访问 `http://localhost:8000/docs` 查看完整的API接口文档。

### 主要接口

- `GET /api/stocks/list` - 获取股票列表
- `GET /api/stocks/daily/{code}` - 获取指定股票的日线数据
- `GET /api/stocks/indicators/{code}` - 获取指定股票的技术指标
- `POST /api/stocks/filter` - 根据条件筛选股票
- `GET /api/stocks/init` - 初始化数据库和基础数据

## 数据来源

本项目使用开源金融数据库 AKShare 作为主要数据源。AKShare 是一个优秀的开源金融数据接口库，提供了丰富的金融市场数据。

## 筛选指标说明

### 技术指标

- **MA(移动平均线)**: 反映股价的平均趋势
- **MACD(指数平滑异同移动平均线)**: 判断股价趋势的强弱和变化
- **RSI(相对强弱指标)**: 判断市场的超买超卖情况
- **KDJ(随机指标)**: 分析市场的超买超卖情况
- **布林带**: 判断股价的波动区间和趋势

### 筛选条件

支持多种筛选条件组合，包括但不限于：

- 价格区间筛选
- 均线关系筛选(上穿、下穿、站上、跌破)
- RSI指标区间筛选
- MACD金叉死叉筛选
- KDJ超买超卖筛选
- 成交量变化筛选
- 布林带突破筛选

## 开发路线图

### 已完成

- 后端基础架构搭建
- 数据获取模块实现
- 技术指标计算模块实现
- 股票筛选引擎实现
- API接口设计与实现

### 进行中

- 前端UI设计与实现
- 数据可视化组件开发
- 用户配置保存功能

### 计划中

- 实时行情推送
- 自定义指标计算
- 多设备协同功能
- 云端数据同步

## 贡献指南

欢迎贡献代码、报告问题或提出新功能建议！

1. Fork本仓库
2. 创建特性分支 (`git checkout -b feature/amazing-feature`)
3. 提交更改 (`git commit -m 'Add some amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 创建Pull Request

## 许可证

本项目采用 MIT 许可证 - 详情请查看 [LICENSE](LICENSE) 文件

## 鸣谢

- [AKShare](https://github.com/akfamily/akshare) - 提供金融数据API
- [HarmonyOS](https://www.harmonyos.com/) - 提供鸿蒙系统开发环境
- 所有贡献者与支持者

## 联系方式

如有任何问题或建议，请通过GitHub Issues与我们联系。

---

**免责声明**: 本应用仅供学习和研究使用，不构成任何投资建议。投资有风险，入市需谨慎。
