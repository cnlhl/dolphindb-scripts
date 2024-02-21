import os
import pandas as pd 
import dolphindb as ddb
import numpy as np
import argparse
import matplotlib.pyplot as plt

s = ddb.session()
conn = s.connect("localhost", 8848, "admin", "123456")


# feature_gen 格式：
# def feature_gen(transtb,ordertb,depthtb,time_limit){
#     ****
#     return result
# }
# result为两列表，第一列为时间（频率与timelimit["horizen"]一致）：列名为MerketTime；第二列为feature：列名为feature；

def R_caculate(args):
    script = '''
def feature_gen(transtb,ordertb,depthtb,limitations) {
    tb = select DepthMarketTime as MarketTime from depthtb
    update tb set feature = rand(100,size(tb.MarketTime))
    result = tb
    return result
}

def each_return(time0,time1){ return (ratio(time1,time0)-1)}

defg each_return_by_count(price,return_intervals){ return ratio(price[return_intervals],price[0])-1 }

def return_gen_by_depth(depthtb,limitations){
    if(limitations["frenquency"]==3s){
        tb = select DepthMarketTime as MarketTime,(AskPrice1+BidPrice1)/2 as Price from depthtb
    }else{
        tb = select (last(AskPrice1)+last(BidPrice1))/2 as Price from depthtb group by interval(DepthMarketTime,limitations["frequency"],"prev",closed = "right",label = "right") as MarketTime
    }
    tb = select * from tb where MarketTime >= limitations["start_time"] 
    // update tb set StockReturn = eachPost(each_return,Price,Price[-1])
    update tb set StockReturn = window(each_return_by_count,(Price,limitations["return_intervals"]),0:limitations["return_intervals"])
    return tb
}

def return_gen(transtb,limitations){
    tb = select * from transtb where CancelFlag = "F"
    tb = select last(Price)as Price from tb group by interval(TransactionTime,limitations["frequency"],"prev",closed = "right",label = "right") as MarketTime
    tb = select * from tb where MarketTime >= limitations["start_time"] and MarketTime <= limitations["end_time"]
    update tb set StockReturn = eachPost(each_return,Price,Price[-1])
    return tb
}

def R_gen(t, method){
    if(method == "UP"){
        ct = select * from t where StockReturn>=0
    }else if(method == "DOWN"){
        ct = select * from t where StockReturn<0
    }else{
        ct = t
    }
    res = ols(ct.StockReturn,ct.feature,1,2)
    ic = corr(ct.StockReturn,ct.feature)
    return res["RegressionStat"]["statistics"][0],ic,ct
}

def load_3tables(target_date,target_stock){
    depthtb = loadTable("dfs://stock_history","depthBook")
    ordertb = loadTable("dfs://stock_history","orderBook")
    transtb = loadTable("dfs://stock_history","transBook")
    depth_d = select * from depthtb where Date = target_date and StockCode = target_stock
    trans_d = select * from transtb where Date = target_date and StockCode = target_stock
    order_d = select * from ordertb where Date = target_date and StockCode = target_stock
    return depth_d,trans_d,order_d
}

def feature_selection(feature_file,target_stock, target_date ,limitations){
    if(limitations["file_flag"] == 1 ){
        depth_d,trans_d,order_d = load_3tables(target_date,target_stock[1:])
        feature_tb = select * from feature_file where date(Timestamp) = target_date
        feature_tb = table(time(feature_tb["Timestamp"]) as MarketTime,feature_tb[target_stock] as feature)
    }else{
        depth_d,trans_d,order_d = load_3tables(target_date,target_stock)
        feature_tb = feature_gen(trans_d,order_d,depth_d,limitations)
    }
    feature_tb = select * from feature_tb where MarketTime <= 11:30:00 or MarketTime >= 13:00:00 and feature is not null
    return feature_tb,depth_d,trans_d,order_d
}

def match_table_process(target_date,target_stock,feature_tb,return_tb,limitations){
    match_tb = select * from feature_tb left join return_tb on feature_tb.MarketTime = return_tb.MarketTime 
    match_tb = select concatDateTime(target_date,MarketTime) as Timestamp, feature, Price, StockReturn from match_tb where MarketTime >= limitations["start_time"] and MarketTime <= limitations["end_time"]
    update match_tb set StockCode = regexReplace(target_stock,"[a-z]","")
    match_tb.reorderColumns!(["Timestamp","StockCode","feature","Price","StockReturn"])
    return match_tb
}

def R_square(feature_file,target_stock, target_date ,limitations){
    feature_tb,depth_d,trans_d,order_d = feature_selection(feature_file,target_stock, target_date ,limitations)
    return_tb = return_gen_by_depth(depth_d,limitations)
    match_tb = match_table_process(target_date,target_stock,feature_tb,return_tb,limitations)

    try{
        return R_gen(match_tb,limitations["method"])
    }catch(ex){
        print("error when processing stock: "+target_stock)
        print("detail:",ex)
    }
}

def loop_caculate(feature_file,limitations,stocks){
    trading_days = getMarketCalendar('SSE', limitations["start_date"], limitations["end_date"])

    print(trading_days)
    res = table(1:0,["Date","Stock","R","Correlation"],[DATE,SYMBOL,DOUBLE,DOUBLE])
    feature = table(1:0,["Timestamp","StockCode","Feature","Price","StockReturn"],[TIMESTAMP,SYMBOL,DOUBLE,DOUBLE,DOUBLE])

    for(current_day in trading_days){
        for(stock in stocks){
            temp_res = R_square(feature_file,stock,current_day,limitations)
            feature.append!(temp_res[2])
            insert into res values(current_day,regexReplace(stock,"[a-z]",""),temp_res[0],temp_res[1])
        }
    }
    return res,feature
}

def process_datetime(start_date_str, end_date_str, start_time_str, end_time_str,frequency_str,return_intervals_str,method,file_flag){
    start_date = temporalParse(start_date_str,"yyyy.MM.dd")
    end_date = temporalParse(end_date_str,"yyyy.MM.dd")
    start_time = temporalParse(start_time_str,"HH:mm:ss")
    end_time = temporalParse(end_time_str,"HH:mm:ss")
    frequency = duration(frequency_str)
    return_intervals_time = duration(return_intervals_str)
    return_intervals_count = int(return_intervals_time)/int(frequency)
    limitations = dict(["start_date","end_date","start_time","end_time","return_intervals","frequency","file_flag","method"],[start_date,end_date,start_time,end_time,return_intervals_count,frequency,file_flag,method])
    return limitations
}

def args_test(limitations,timestamps){
    try{   
        start_time = first(timestamps)
        end_time = last(timestamps)
        if (date(start_time)>limitations["start_date"] or date(end_time)<limitations["end_date"])
            throw "error: Start/end date range is too large"
    }catch(ex){
        print(ex)
    }
}

def readfile_caculate(filepath,start_date_str, end_date_str, start_time_str, end_time_str,frequency,return_intervals,method){
    feature_file = loadText(filename = filepath, containHeader = true)
    file_header = columnNames(feature_file)
    stocks = file_header[1:]
    limitations = process_datetime(start_date_str, end_date_str, start_time_str, end_time_str,frequency, return_intervals,method,1)
    args_test(limitations,feature_file[file_header[0]])
    return loop_caculate(feature_file,limitations,stocks)
}

def setting_caculate(stocks, start_date_str, end_date_str, start_time_str, end_time_str, frequency,return_intervals,method){
    limitations = process_datetime(start_date_str, end_date_str, start_time_str, end_time_str,frequency,return_intervals, method,0)
    return loop_caculate(,limitations,stocks)
}
    '''
    s.run(script)
    if args.file_path is not None:
        return s.run("readfile_caculate",args.file_path,args.start_date,args.end_date,args.start_time,args.end_time,args.frequency,args.return_intervals,args.method)
    else:
        return s.run("setting_caculate",args.chosen_stocks,args.start_date,args.end_date,args.start_time,args.end_time,args.frequency,args.return_intervals,args.method)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--start_date', type=str, default=None, help='Start date')
    parser.add_argument('-e', '--end_date', type=str, default=None, help='End date')
    parser.add_argument('-st', '--start_time', type=str, default="09:00:00", help='Start time')
    parser.add_argument('-et', '--end_time', type=str, default="15:00:00", help='End time')
    parser.add_argument('-f', '--frequency', type=str, default="3s", help='Frenquency')
    parser.add_argument('-m', '--method', type=str, default="ALL", help='Calculate method')
    parser.add_argument('-q', "--chosen_stocks",nargs='+', type=str, default=[], help='manually set target stocks')
    parser.add_argument('-p', "--file_path", type=str, default=None, help='feature file path')
    parser.add_argument('-i',"--return_intervals",type = str,default="30s",help = "time intervals of return calculate")

    args = parser.parse_args()

    if args.start_date is None or args.end_date is None:
        raise ValueError("Both start_date and end_date must be provided.")

    res,feature = R_caculate(args)
    print(feature)
    
    save_directory = 'testfig'
    if not os.path.exists(save_directory):
        os.makedirs(save_directory)

    code_grouped = feature.groupby("StockCode")
    for stockcode,stockgroup in code_grouped:
        date_grouped = stockgroup.groupby(stockgroup['Timestamp'].dt.date)
        for date,stockdategroup in date_grouped:
            new_row = {
                'Timestamp': pd.to_datetime(f'{date} 11:30:01'),
                'StockCode': np.nan,
                'Feature': np.nan,
                'Price': np.nan,
                'StockReturn': np.nan
            }
            insert_index = stockdategroup['Timestamp'].searchsorted(new_row['Timestamp'])
            new_row_df = pd.DataFrame(new_row,index=[0])
            stockdategroup = pd.concat([stockdategroup.iloc[:insert_index],new_row_df,stockdategroup.iloc[insert_index:]]).reset_index(drop=True)
            plt.figure(figsize=(40,20))
            plt.subplot(3,2,1)
            plt.plot(stockdategroup["Timestamp"],stockdategroup["Feature"])
            plt.title("feature")
            plt.subplot(3,2,3)
            plt.plot(stockdategroup["Timestamp"],stockdategroup["StockReturn"])
            plt.title("return")
            plt.subplot(3,2,5)
            plt.plot(stockdategroup["Timestamp"],stockdategroup["Price"])
            plt.title("price")
            plt.subplot(1,2,2)
            plt.scatter(stockdategroup["Feature"],stockdategroup["StockReturn"],marker='.')
            plt.title("feature-return")
            plt.xlabel("feature")
            plt.ylabel("return")
            plt.tight_layout()
            plt.savefig(f"testfig/testfig_{stockcode}_{date}",dpi=600)
            plt.close()