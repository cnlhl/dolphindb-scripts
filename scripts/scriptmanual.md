# 使用说明

一个简单的分析工具，用于计算给定股票组合在指定时间范围内的R平方值。以下是代码中各个函数的说明和使用方法。

## 导入模块

```python
import pandas as pd 
import dolphindb as ddb
import numpy as np
import argparse
```

代码使用了`pandas`、`dolphindb`、`numpy`和`argparse`模块，确保在运行代码之前安装了这些模块。

## 函数：R_square_of_targetstock(target_stock, target_date, horizen)

该函数用于计算给定目标股票在目标日期和时间范围内的R平方值。

### 参数：

- `target_stock`：目标股票的代码（字符串）。
- `target_date`：目标日期（字符串），格式为"yyyyMMdd"。
- `horizen`：时间范围（字符串），如"15m"表示15分钟。

### 返回值：

返回目标股票在目标日期和时间范围内的R平方值。

## 函数：R_square_of_file(filepath, start_date, end_date, horizen, method)

该函数用于计算给定文件中的股票组合在指定时间范围内的平均R平方值。

### 参数：

- `filepath`：输入文件的路径（字符串）。
- `start_date`：开始日期（整数），格式为yyyyMMdd。
- `end_date`：结束日期（整数），格式为yyyyMMdd。
- `horizen`：时间范围（字符串），如"15m"表示15分钟。
- `method`：计算方法（字符串），可选值为"ALL"、"UP"或"DOWN"。

### 返回值：

返回股票组合在指定时间范围内的平均R平方值。

## 函数：choose_comp(comp)

该函数用于选择指定的股票组合。

### 参数：

- `comp`：股票组合的名称（字符串）。

### 返回值：

返回选择的股票组合的数据。

## 函数：R_calculate(comp, start_date, end_date, horizen, filepath, method)

该函数用于计算给定股票组合在指定时间范围内的平均R平方值。

### 参数：

- `comp`：股票组合的名称（字符串）。
- `start_date`：开始日期（整数），格式为yyyyMMdd。
- `end_date`：结束日期（整数），格式为yyyyMMdd。
- `horizen`：时间范围（字符串），如"15m"表示15分钟。
- `filepath`：输入文件的路径（字符串），可选参数，默认为None，要求为csv格式。
- `method`：计算方法（字符串），可选值为"ALL"、"UP"或"DOWN"，默认为"ALL"。

### 返回值：

返回股票组合在指定时间范围内的平均R平方值。

## 输入文件格式要求

如果使用`R_square_of_file`函数并提供`filepath`参数，则要求输入文件为csv格式，具体要求如下：

- 第一列为

时间戳（Timestamp）。
- 第一行为股票代码。
- 其他单元格为相应的股票数据。

示例输入文件内容如下：

| Timestamp       | 00001  | 000002  |
|-----------------|--------|---------|
| 2022-9-1 9:30   | 71 | 44 |
| 2022-9-1 9:45   | 5  | 85 |
| 2022-9-1 10:00  | 77 | 29 |
| 2022-9-1 10:15  | 22 | 11 |


## 使用示例

你可以直接运行该代码，并提供以下命令行参数：

- `-c` 或 `--portfolio`：股票组合，默认为"399300.SZ"。
- `-s` 或 `--start_date`：开始日期，格式为yyyyMMdd，必需参数。
- `-e` 或 `--end_date`：结束日期，格式为yyyyMMdd，必需参数。
- `-f` 或 `--horizon`：时间范围，默认为"15m"。
- `-p` 或 `--filepath`：输入文件的路径，默认为None。
- `-m` 或 `--method`：计算方法，默认为"ALL"。

示例命令：

```bash
python3 filename.py -c 399300.SZ -s 20220101 -e 20220131 -f 15m -p data.csv -m ALL
```

注意：确保替换`filename.py`为实际的文件名，`data.csv`为实际的数据文件名，参数的值根据需要进行修改。

