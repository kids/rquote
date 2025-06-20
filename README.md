# rquote

`rquote` 是一个提供 A股/港股/美股/ETF基金/期货 历史数据获取的Python库

## 主要功能

### 历史价格数据获取

#### `get_price(i, sdate='', edate='', freq='day', days=320, fq='qfq', dd=None)`

获取股票、基金、期货的历史价格数据

**参数:**
- `i`: 股票代码，使用新浪/腾讯的id形式
- `sdate`: 开始日期 (可选)
- `edate`: 结束日期 (可选) 
- `freq`: 频率，默认'day' (日线)
- `days`: 获取天数，默认320天
- `fq`: 复权方式，默认'qfq' (前复权)
- `dd`: 本地缓存字典 (可选)

**代码格式说明:**
- A股: `sh000001`表示上证指数，`sz000001`表示深市000001股票`平安银行`
- ETF: `sh510050`表示上证50指数ETF
- 港股: `hk00700`表示港股腾讯
- 期货: 需加`fu`前缀，如`fuAP2110`，`fuBTC`表示比特币
- 美股: 需加对应交易所后缀，如`usBABA.N`，`usC.N`，`usAAPL.OQ`等
- 比特币：使用`fuBTC`代码

**示例:**
```python
from rquote import get_price

# 获取上证指数数据
sid, nm, df = get_price('sh000001')
print(df.head())  # 数据为pandas dataframe

# 获取指定日期范围的数据
sid, nm, df = get_price('sz000001', sdate='2024-01-01', edate='2024-02-01')

# 获取比特币数据
sid, nm, df = get_price('fuBTC')
```

**返回数据格式:**
| date       | open    | close   | high    | low     | vol        |
|------------|---------|---------|---------|---------|------------|
| 2024-02-06 | 2680.48 | 2789.49 | 2802.93 | 2669.67 | 502849313  |
| 2024-02-07 | 2791.51 | 2829.70 | 2829.70 | 2770.53 | 547117439  |
| 2024-02-08 | 2832.49 | 2865.90 | 2867.47 | 2827.90 | 531108893  |
| 2024-02-19 | 2886.59 | 2910.54 | 2910.54 | 2867.71 | 458967704  |
| 2024-02-20 | 2902.88 | 2922.73 | 2927.31 | 2887.47 | 350138735  |

#### `get_price_longer(i, l=2, dd={})`

获取更长时间的历史数据，默认获取2年数据

```python
from rquote import get_price_longer

# 获取2年的历史数据
sid, nm, df = get_price_longer('sh000001', l=3)  # 获取3年数据
```

### 股票列表获取

#### `get_cn_stock_list(money_min=2e8)`

获取A股股票列表，按成交额排序，默认筛选成交额大于2亿的股票

```python
from rquote import get_cn_stock_list

# 获取成交额大于5亿的股票列表
stocks = get_cn_stock_list(money_min=5e8)
# 返回格式: [{code, name, pe_ttm, volume, turnover/亿, ...}, ...] 
# 如 {"code":"sh600519","hsl":"0.28","lb":"0.94","ltsz":"17946.80","name":"贵州茅台","pe_ttm":"20.16","pn":"6.95","speed":"0.02","state":"","stock_type":"GP-A","turnover":"499068","volume":"34816.00","zd":"2.66","zdf":"0.19","zdf_d10":"-5.16","zdf_d20":"-9.58","zdf_d5":"0.12","zdf_d60":"-9.22","zdf_w52":"-3.22","zdf_y":"-6.26","zf":"1.47","zljlr":"16268.84","zllc":"263511.99","zllc_d5":"1295957.54","zllr":"279780.83","zllr_d5":"1264966.39","zsz":"17946.80","zxj":"1428.66"}
```

#### `get_hk_stocks_500()`

获取港股前500只股票列表

```python
from rquote import get_hk_stocks_500

stocks = get_hk_stocks_500()
# 返回格式: [[code, name, price, turnover, ...], ...] 
# 如 ['00700', '腾讯控股', '505.50', '1.51', '7.50', '505.50', '505.50', '20622275.00', '10364144211.54', '504.50', '498.00', '505.50', '496.00', '0.22']
```

#### `get_hk_stocks_hsi()`

获取恒生指数成分股列表

```python
from rquote import get_hk_stocks_hsi

hsi_stocks = get_hk_stocks_hsi()
```

#### `get_hk_stocks_ggt()`

获取港股通股票列表

```python
from rquote import get_hk_stocks_ggt

ggt_stocks = get_hk_stocks_ggt()
```

#### `get_us_stocks(k=100)`

获取美股最大市值的k支股票列表

```python
from rquote import get_us_stocks_biggest

us_stocks = get_us_stocks_biggest(k=100)  # 获取前100只
# 返回格式: [{name, symbol, market, mktcap, pe, ...}, ...] 
# 如 {"name":"Microsoft Corp.","cname":"微软公司","category":"软件","symbol":"MSFT","price":"480.24","diff":"2.20","chg":"0.46","preclose":"478.04","open":"478.00","high":"481.00","low":"474.46","amplitude":"1.37%","volume":"17526452","mktcap":"3569404793144","pe":"36.94153771","market":"NASDAQ","category_id":"14"}
```

### 基金和期货

#### `get_cn_fund_list()`

获取A股ETF基金列表，按成交额排序

```python
from rquote import get_cn_fund_list

funds = get_cn_fund_list()
# 返回格式: [code, name, change, amount, price]
```

#### `get_cn_future_list()`

获取国内期货合约列表

```python
from rquote import get_cn_future_list

futures = get_cn_future_list()
# 返回格式: ['fuSC2109', 'fuRB2110', 'fuHC2110', ...]
```

### 板块和概念

#### `get_all_industries()`

获取所有行业板块列表

```python
from rquote import get_all_industries

industries = get_all_industries()
# 返回格式: [code, name, change, amount, price]
```

#### `get_all_concepts()`

获取所有概念板块列表

```python
from rquote import get_all_concepts

concepts = get_all_concepts()
# 返回格式: [code, name, change, amount, price]
```

#### `get_stock_concepts(i)`

获取指定股票所属的概念板块

```python
from rquote import get_stock_concepts

# 获取平安银行的概念板块
concepts = get_stock_concepts('sz000001')
# 返回概念代码列表，如 ['BK0420', 'BK0900', ...]
```

#### `get_concept_stocks(bkid, dc=None)`

获取指定概念板块的股票列表

```python
from rquote import get_concept_stocks

# 获取概念板块BK0420的股票
stocks = get_concept_stocks('BK0420')
# 返回格式: [code, name, change, amount, mktcap]
```

#### `get_bk_stocks(bkid)`

获取指定板块的股票列表

```python
from rquote import get_bk_stocks

# 获取板块股票
stocks = get_bk_stocks('BK0420')
```

#### `get_industry_stocks(bkid)`

获取指定行业板块的股票列表

```python
from rquote import get_industry_stocks

# 获取行业板块股票
stocks = get_industry_stocks('BK0420')
```

### 实时行情

#### `get_tick(tgts=[])`

获取实时行情数据

```python
from rquote import get_tick

# 获取美股实时行情
tick_data = get_tick(['AAPL', 'GOOGL'])
# 返回格式: [{'name': 'Apple Inc', 'price': '150.25', 'price_change_rate': '1.2%', ...}]
```

### 可视化工具

#### `PlotUtils.plot_candle(i, sdate='', edate='', dsh=False, vol=True)`

绘制K线图

```python
from rquote import PlotUtils

# 绘制平安银行的K线图
data, layout = PlotUtils.plot_candle('sz000001', sdate='2024-01-01', edate='2024-02-01')

# 使用plotly显示
import plotly.graph_objs as go
fig = go.Figure(data=data, layout=layout)
fig.show()
```

### 工具类

#### `WebUtils`

网络请求工具类

#### `BasicFactors`

基础因子计算工具类

## 安装

```bash
pip install rquote
```

## 注意事项

1. 数据来源于新浪财经、腾讯财经、东方财富等公开数据源
2. 建议合理控制请求频率，避免被限制访问
3. 期货代码需要加`fu`前缀，如`fuAP2110`
4. 美股代码需要加对应后缀，如`usAAPL.OQ` （OQ->NASDAQ, N->NYSE, AM->ETF）



