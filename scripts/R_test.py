import pandas as pd 
import dolphindb as ddb
import numpy as np
import argparse

s = ddb.session()
conn = s.connect("192.168.0.202", 8848, "admin", "123456")

def R_square_of_targetstock(target_stock, target_date, horizen):
  # define the script as a string
  script="""
def feature_gen(transtb,ordertb,depthtb) {
    interval_ms = 3 * 1000
    three_group = <(floor(TransactionTime$INT / interval_ms) * interval_ms)$TIME>
    afterAuction = <TransactionTime >= 09:30:00.000>
    limitOrderOnly = <OrderType = 0>
    buyOrderOnly = <BSFlag = "B">
    sellOrderOnly = <BSFlag = "S">
    t1 = sql(
      select=(sqlCol("*"), sqlColAlias(three_group, "three")),
      from=ordertb, where=afterAuction
    )
    eval(t1)
    t2 = sql(
      select=sqlCol(`Price`BSFlag`Volume`three`OrderType),
      from=t1,
      where=[limitOrderOnly, sellOrderOnly]
    )
    t2 = eval(t2)
    t3 = select t2.*, depthtb.Close, max((t2.Price / depthtb.Close - 1), -0.2) as diff from t2 inner join depthtb on t2.three=depthtb.DepthMarketTime where t2.Price < depthtb.Close
    t4 = select three, wavg(diff, Volume) as feature from t3 group by three
    result = select three as MarketTime, feature from t4
    return result
  }


def each_return(time0,time1){
    return (time1/time0-1)
}

def return_gen(transtb,target_date,target_stock,horizen){
    tb = select last(Price)as Price from transtb group by interval(TransactionTime,horizen,"prev") as MarketTime
    update tb set stock_return = eachPost(each_return,Price,Price[-1])
    return tb
}

def R_gen(return_tb,feature_tb, method){
    t = select * from feature_tb left join return_tb on feature_tb.MarketTime = return_tb.MarketTime 
    if(method == "UP"){
        ct = select * from t where stock_return>=0
    }else if(method == "DOWN"){
        ct = select * from t where stock_return<0
    }else{
        ct = t
    }
    res = ols(ct.stock_return,ct.feature,1,2)
    return res["RegressionStat"]["statistics"][0]
}

def R_square_gen(target_stock, string_date ,horizen, method = "ALL"){
    target_date = temporalParse(string_date,"yyyyMMdd")
    depthtb = loadTable("dfs://history","depthBook")
    ordertb = loadTable("dfs://history","orderBook")
    transtb = loadTable("dfs://history","transBook")
    depth_d = select * from depthtb where Date = target_date and StockCode = target_stock
    trans_d = select * from transtb where Date = target_date and StockCode = target_stock and CancelFlag = "F"
    order_d = select * from ordertb where Date = target_date and StockCode = target_stock
    return_tb = return_gen(trans_d,target_date,target_stock,duration(horizen))
    feature_tb = feature_gen(trans_d,order_d,depth_d)
    try{
        return R_gen(return_tb,feature_tb,method)
    }catch(ex){
        print("error when processing stock: "+target_stock)
        print("detail:",ex)
    }
}
  """
  # run the script
  s.run(script)
  # return the result of R_square_gen
  print(target_stock)
  return s.run("R_square_gen",target_stock,target_date,horizen)

# feature_gen 格式：
# def feature_gen(transtb,ordertb,depthtb){
#     ****
#     return result
# }
# result为两列表，第一列为时间（频率与horizen一致）：列名为MerketTime；第二列为feature：列名为feature；

def R_square_of_file(filepath, start_date, end_date, horizen, method):
   script="""

def each_return(time0,time1){
    return (time1/time0-1)
}

def get_target_stock(start_date,end_date,target_stock,horizen){
    trans = loadTable("dfs://history","transBook")
    trans = select * from trans where Date>=start_date and Date<=end_date and StockCode=target_stock and CancelFlag="F"
    update trans set Timestamp=concatDateTime(Date,TransactionTime)
    trans = select last(Price)as Price from trans group by interval(Timestamp,horizen,"prev") as Time
    update trans set stock_return = eachPost(each_return,Price)
    return trans
}

def R_gen(t, method="ALL"){
    if(method == "UP"){
        ct = select * from t where stock_return>=0
    }else if(method == "DOWN"){
        ct = select * from t where stock_return<0
    }else{
        ct = t
    }
    res = ols(ct.stock_return,ct.feature_value,1,2)
    return res["RegressionStat"]["statistics"][0]
}

def readdata(filepath,start_date,end_date,horizen,method="ALL"){
    file_in = loadText(filename=filepath,containHeader=true)
    names = columnNames(file_in)
    R = array(DOUBLE, 0)
    for(i in 1..(size(names)-1)){
        print(i)
        target_stock = names[i][1:]
        tmp_tb=table(file_in[names[0]],file_in[names[i]] as feature_value)
        tmp_trans=get_target_stock(start_date,end_date,target_stock,horizen)
        tb = select * from tmp_tb left join tmp_trans on tmp_tb.Timestamp=tmp_trans.Time
        R.append!(R_gen(tb,method))
    }
    return avg(R)
}
   """
   s.run(script)
   return s.run("readdata",filepath,str(start_date),str(end_date),horizen,method)

# 选取指定股票组合
def choose_comp(comp):
    path = "/home/ddb/Comp/"+comp
    data = pd.read_csv(path)
    return data

def R_calculate_by_comp(comp,start_date,end_date,horizen,filepath,method):
  if filepath is not None:
     return R_square_of_file(filepath,start_date,end_date,horizen,method)
  # 初始化一个空列表，用于存储每一行的R值
  R_list = []
  data = choose_comp(comp)
  # 选取第一列大于等于start_date并且小于等于end_date的行
  condition = (data.iloc[:,0] >= start_date) & (data.iloc[:,0] <= end_date)
  # 用data.loc来根据条件选取行，并赋值给rows
  rows = data.loc[condition]
  # 遍历每一行
  for index, row in rows.iterrows():
    # 初始化R为0
    R = 0
    # 遍历每一列
    for i in range(1, len(data.columns)):
      # 如果该列的值不为0且不为nan
      if row[i] != 0 and not np.isnan(row[i]):
        # 计算该列对应的股票的R_square值
        # print(data.columns[i],str(int(row[0])),horizen)
        R_temp = R_square_of_targetstock(data.columns[i],str(int(row[0])),horizen)
        if R_temp is None:
          print("missing data: date:",data.columns[i],"stock:",str(int(row[0])))
          R_temp = 0
        # 累加到R上，乘以权重
        R = R + R_temp*row[i]/100
    # 将该行的R值添加到列表中
    R_list.append(R)
  # 返回列表的均值
  return np.mean(R_list)

def R_calculate_by_setting(start_date,end_date,horizen,filepath,method,stocks):
   for stock in stocks:
      R_temp = R_square_of_targetstock(stock,horizen,horizen)

def R_calculate(comp,start_date,end_date,horizen,filepath,method,stocks):
    if len(stocks) == 0:
      return R_calculate_by_comp(comp,start_date,end_date,horizen,filepath,method)
    else:
      return R_calculate_by_setting(start_date,end_date,horizen,filepath,method,stocks)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--portfolio', type=str, default="399300.SZ", help='Stock portfolio')
    parser.add_argument('-s', '--start_date', type=int, default=None, help='Start date')
    parser.add_argument('-e', '--end_date', type=int, default=None, help='End date')
    parser.add_argument('-f', '--horizon', type=str, default="3s", help='Horizon')
    parser.add_argument('-p', '--filepath', type=str, default=None, help='Input file')
    parser.add_argument('-m', '--method', type=str, default="ALL", help='Calculate method')
    parser.add_argument('-h', '--manual_setting', nargs='+', type=str, default=[], help='manually set target stocks')

    args = parser.parse_args()

    if args.start_date is None or args.end_date is None:
        raise ValueError("Both start_date and end_date must be provided.")

    # 读取参数值
    portfolio = args.portfolio
    start_date = args.start_date
    end_date = args.end_date
    horizon = args.horizon
    filepath = args.filepath
    method = args.method
    stocks = args.manual_setting

    print(R_calculate(portfolio, start_date, end_date, horizon, filepath, method,stocks))
