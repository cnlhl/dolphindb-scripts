# dolphindb-scripts
一些简单的dolphindb脚本，用于将csv格式的股市交易数据导入dolphindb数据库，同时添加日期和股票代码，并进行一定的格式转换
## point to be finished:[tutor](https://gitee.com/dolphindb/Tutorials_CN/blob/master/LoadDataForPoc.md)
1. 将OLAP引擎改为TSDB引擎：*“查询某个 key （设备或股票）在某个时间点或时间段的数据”*不适合使用OLAP
2. 将TIME类型更改为TIMESTAMP类型：相对更加节省空间
3. 使用loadtextex：将增加列，数据类型变更等操作放到transform函数中，更加简洁
4. 并行导入可以优化为按照日期发送任务（每天）
5. 修改 workerNum 和 localExecutors 配置，提高导入效率