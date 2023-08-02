import pandas as pd 
import dolphindb as ddb
import numpy as np
import argparse

s = ddb.session()
conn = s.connect("192.168.0.202", 8848, "admin", "123456")


# feature_gen 格式：
# def feature_gen(transtb,ordertb,depthtb,time_limit){
#     ****
#     return result
# }
# result为两列表，第一列为时间（频率与timelimit["horizen"]一致）：列名为MerketTime；第二列为feature：列名为feature；


def R_calculate_by_setting(start_date, end_date,start_time, end_time, frequency, method,stocks):
    script = '''
    def feature_gen(transtb,ordertb,depthtb,time_limit) {
        interval_ms = 3 * 1000
        three_group = <(floor(TransactionTime$INT / interval_ms) * interval_ms)$TIME>
        afterAuction = <TransactionTime >= 09:30:00.000>
        limitOrderOnly = <OrderType = "0">
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


    def each_return(time0,time1){ return (time1/time0-1)}

    def return_gen(transtb,time_limit){
        tb = select last(Price)as Price from transtb group by interval(TransactionTime,time_limit["frequency"],"prev",closed = "right",label = "right") as MarketTime
        tb = select * from tb where MarketTime >= time_limit["start_time"] and MarketTime <= time_limit["end_time"]
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

    def load_3tables(target_date,target_stock){
        depthtb = loadTable("dfs://history","depthBook")
        ordertb = loadTable("dfs://history","orderBook")
        transtb = loadTable("dfs://history","transBook")
        depth_d = select * from depthtb where Date = target_date and StockCode = target_stock
        trans_d = select * from transtb where Date = target_date and StockCode = target_stock and CancelFlag = "F"
        order_d = select * from ordertb where Date = target_date and StockCode = target_stock
        return depth_d,trans_d,order_d
    }

    def R_square(target_stock, target_date ,time_limit, method = "ALL"){
        depth_d,trans_d,order_d = load_3tables(target_date,target_stock)
        return_tb = return_gen(trans_d,time_limit)
        feature_tb = feature_gen(trans_d,order_d,depth_d,time_limit)
        feature_tb = select * from feature_tb where MarketTime <= 11:30:00 or MarketTime >= 13:00:00 
        try{
            return R_gen(return_tb,feature_tb,method),feature_tb
        }catch(ex){
            print("error when processing stock: "+target_stock)
            print("detail:",ex)
        }
    }

    def process_datetime(start_date_str, end_date_str, start_time_str, end_time_str,frequency){
        start_date = temporalParse(start_date_str,"yyyy.MM.dd")
        end_date = temporalParse(end_date_str,"yyyy.MM.dd")
        start_time = temporalParse(start_time_str,"HH:mm:ss")
        end_time = temporalParse(end_time_str,"HH:mm:ss")
        time_limitation = dict(["start_date","end_date","start_time","end_time","frequency"],[start_date,end_date,start_time,end_time,duration(frequency)])
        return time_limitation
    }

    def R_square_gen(stocks, start_date_str, end_date_str, start_time_str, end_time_str, frequency, method){
        time_limit = process_datetime(start_date_str, end_date_str, start_time_str, end_time_str,frequency)
        days = select * from loadTable("dfs://stock_info","calendar") where Date >= time_limit["start_date"] and Date <= time_limit["end_date"]
        trading_days = exec Date from days
        
        res = table(1:0,["Date","Stock","R"],[DATE,SYMBOL,DOUBLE])
        feature = table(1:0,["Date","Stock","MarketTime","Feature"],[DATE,SYMBOL,TIME,DOUBLE])

        for(current_day in trading_days){
            for(stock in stocks){
                temp_R,temp_feature = R_square(stock,current_day,time_limit,method)
                temp_feature.addColumn(["Date","Stockcode"],[DATE,SYMBOL])
                update temp_feature set Date = current_day
                update temp_feature set Stockcode = stock
                temp_feature.reorderColumns!(["Date","Stockcode","MarketTime","feature"])
                feature.append!(temp_feature)
                print(temp_R)
                insert into res values(current_day,stock,temp_R)
            }
        }
        return res,feature
    }
    '''
    s.run(script)
    return s.run("R_square_gen",stocks, start_date, end_date, start_time, end_time, frequency, method)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--start_date', type=str, default=None, help='Start date')
    parser.add_argument('-e', '--end_date', type=str, default=None, help='End date')
    parser.add_argument('-st', '--start_time', type=str, default="09:00:00", help='Start time')
    parser.add_argument('-et', '--end_time', type=str, default="15:00:00", help='End time')
    parser.add_argument('-f', '--frequency', type=str, default="3s", help='frequency')
    parser.add_argument('-m', '--method', type=str, default="ALL", help='Calculate method')
    parser.add_argument('-q', "--chosen_stocks",nargs='+', type=str, default=[], help='manually set target stocks')

    args = parser.parse_args()

    if args.start_date is None or args.end_date is None:
        raise ValueError("Both start_date and end_date must be provided.")

    start_date = args.start_date
    end_date = args.end_date
    start_time = args.start_time
    end_time = args.end_time
    frequency = args.frequency
    method = args.method
    stocks = args.chosen_stocks

    print(R_calculate_by_setting(start_date, end_date, start_time, end_time, frequency, method,stocks))