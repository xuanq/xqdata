# xqdata

统一适配rqdata、火富牛API等数据源

## 简介

xqdata是一个统一的数据访问接口库，旨在提供一致的API来访问不同的金融数据源，包括RQData、火富牛等。

## 安装

```bash
pip install xqdata
```

## 使用方法

### 基本用法

```python
from xqdata import get_dataapi

# 获取数据API实例，默认使用mock数据
api = get_dataapi()

# 或者指定特定的数据源
api = get_dataapi("rq")  # RQData
api = get_dataapi("mock")  # Mock数据（默认）
```

### 认证

```python
# 对于需要认证的数据源（如RQData）
api.auth(username="your_username", password="your_password")
```

### 获取基础信息

```python
# 获取股票信息
stock_info = api.get_info("stock")

# 获取基金信息
fund_info = api.get_info("fund")

# 获取期货信息
futures_info = api.get_info("futures")

# 获取ETF信息
etf_info = api.get_info("etf")
```

### 获取因子数据

```python
# 获取单个因子数据
factor_data = api.get_factor("pe_ratio", "000001.XSHE")

# 获取多个因子数据
factors = ["pe_ratio", "pb_ratio"]
codes = ["000001.XSHE", "000002.XSHE"]
factor_data = api.get_factor(factors, codes)

# 指定时间范围和频率
factor_data = api.get_factor(
    factors, 
    codes, 
    start_time="2024-01-01", 
    end_time="2024-12-31", 
    frequency="D"
)
```

### 获取双键因子数据

```python
# 获取双键因子数据（例如持仓数据）
dualkey_data = api.get_dualkey_factor(
    "holding_ratio", 
    ["client_001", "client_002"],  # 客户代码
    ["product_A", "product_B"],    # 产品代码
)
```

## 支持的数据源

1. **Mock数据**：用于开发和测试的模拟数据
2. **RQData**：米筐RQData金融数据接口（需要安装rqdatac库并具有有效账户）

## RQData配置

要使用RQData数据源，需要：

1. 安装rqdatac库：
   ```bash
   pip install rqdatac
   ```

2. 具有有效的RQData账户

3. 在代码中进行认证：
   ```python
   api = get_dataapi("rq")
   api.auth(username="your_username", password="your_password")
   ```

## 扩展新的数据类型

RQData API支持通过配置来扩展新的数据类型查询。可以通过以下方式注册新的信息类型：

```python
import rqdatac as rq

# 注册新的信息类型
api.register_info_type(
    "my_custom_type",           # 类型名称
    rq.all_instruments,         # 调用的RQData函数
    {"type": "CustomType"},     # 传递给RQData函数的参数
    None,                       # 后处理函数（可选）
    {}                          # 传递给后处理函数的参数（可选）
)

# 使用新注册的类型
data = api.get_info("my_custom_type")
```

## 开发指南

### 运行测试

```bash
# 运行所有测试
pytest

# 运行特定测试文件
pytest test/test_mock.py
pytest test/test_rqdata.py
```

### 添加新的数据源

要添加新的数据源，需要：

1. 创建新的模块目录（如`xqdata/newsource`）
2. 实现`DataApi`抽象基类的所有方法
3. 在模块的`__init__.py`中创建单例实例
4. 更新工厂函数以支持新的数据源

## License

MIT