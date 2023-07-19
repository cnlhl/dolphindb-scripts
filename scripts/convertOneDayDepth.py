import argparse
from collections import Counter
from datetime import datetime
import h5py
import numpy as np
import os
import pandas as pd
import sys

#=============================================
# Data Segment
#=============================================
def strip_keys(target_dict: dict):
    origin_keys = []
    for k, v in target_dict.items():
        origin_keys.append(k)

    for k in origin_keys:
        target_dict[k.strip()] = str(target_dict[k])
        if k.strip() != k:
            del target_dict[k]

trans_keys = dict()
trans_keys["SeqNo          "] = 0       # unique in one channel
trans_keys["ChannelNo      "] = 1
trans_keys["TransactionTime"] = 2
trans_keys["Price          "] = 3
trans_keys["Volume         "] = 4
trans_keys["Amount         "] = 5      # SH only
trans_keys["BSFlag         "] = 6      # SH only 'B' for active buy and 'S' for active sell, 'N' for unknown 
trans_keys["CancelFlag     "] = 7      # SZ only '4' for cancel, 'F' for trade
trans_keys["BidOrderSeqNo  "] = 8
trans_keys["AskOrderSeqNo  "] = 9
trans_keys["BizIndex       "] = 10
trans_keys["HostTime       "] = 11
strip_keys(trans_keys)

order_keys = dict()
order_keys["SeqNo           "] = 0 
order_keys["ChannelNo       "] = 1
order_keys["TransactionTime "] = 2
order_keys["Price           "] = 3
order_keys["Volume          "] = 4
order_keys["BSFlag          "] = 5
order_keys["OrderType       "] = 6
order_keys["OriginOrderSeqNo"] = 7
order_keys["BizIndex        "] = 8
order_keys["HostTime        "] = 9
strip_keys(order_keys)

depth_keys = dict()
depth_keys["DepthMarketTime     "] = 0 
depth_keys["Status              "] = 1 
depth_keys["PreClose            "] = 2 
depth_keys["Open                "] = 3 
depth_keys["High                "] = 4 
depth_keys["Low                 "] = 5 
depth_keys["Close               "] = 6 
depth_keys["AskPrice1           "] = 7 
depth_keys["AskPrice2           "] = 8 
depth_keys["AskPrice3           "] = 9 
depth_keys["AskPrice4           "] = 10
depth_keys["AskPrice5           "] = 11
depth_keys["AskPrice6           "] = 12
depth_keys["AskPrice7           "] = 13
depth_keys["AskPrice8           "] = 14
depth_keys["AskPrice9           "] = 15
depth_keys["AskPrice10          "] = 16
depth_keys["AskVol1             "] = 17
depth_keys["AskVol2             "] = 18
depth_keys["AskVol3             "] = 19
depth_keys["AskVol4             "] = 20
depth_keys["AskVol5             "] = 21
depth_keys["AskVol6             "] = 22
depth_keys["AskVol7             "] = 23
depth_keys["AskVol8             "] = 24
depth_keys["AskVol9             "] = 25
depth_keys["AskVol10            "] = 26
depth_keys["BidPrice1           "] = 27
depth_keys["BidPrice2           "] = 28
depth_keys["BidPrice3           "] = 29
depth_keys["BidPrice4           "] = 30
depth_keys["BidPrice5           "] = 31
depth_keys["BidPrice6           "] = 32
depth_keys["BidPrice7           "] = 33
depth_keys["BidPrice8           "] = 34
depth_keys["BidPrice9           "] = 35
depth_keys["BidPrice10          "] = 36
depth_keys["BidVol1             "] = 37
depth_keys["BidVol2             "] = 38
depth_keys["BidVol3             "] = 39
depth_keys["BidVol4             "] = 40
depth_keys["BidVol5             "] = 41
depth_keys["BidVol6             "] = 42
depth_keys["BidVol7             "] = 43
depth_keys["BidVol8             "] = 44
depth_keys["BidVol9             "] = 45
depth_keys["BidVol10            "] = 46
depth_keys["Trades              "] = 47
depth_keys["Volume              "] = 48
depth_keys["Turnover            "] = 49
depth_keys["TotalBidVol         "] = 50
depth_keys["TotalAskVol         "] = 51
depth_keys["WeightedAvgBidPrice "] = 52
depth_keys["WeightedAvgAskPrice "] = 53
depth_keys["HighLimit           "] = 54
depth_keys["LowLimit            "] = 55
depth_keys["HostTime            "] = 56
strip_keys(depth_keys)

#=============================================
# Readers
#=============================================
class H5Level2Reader:
    def __init__(self, filename: str, keys: dict):
        self._data = h5py.File(filename, 'r')
        self._keys = keys
    
    def get_stock_pool(self):
        return ['{:06d}'.format(s) for s in self._data['StockPool'][:]]
    
    def get_field_timeseries(self, stock_id, field_name):
        stock_group = self._data['FeaturesDB']['{:06}'.format(stock_id)]
        raw = stock_group[self._keys[field_name]][:]
        if ("Flag" in field_name) or ("OrderType" == field_name):
            processed = np.vectorize(chr)(raw)
        else:
            processed = raw
        return processed
    
    def get_stock_dataframe(self, stock_id):
        fields = dict()
        stock_group = self._data['FeaturesDB']['{:06}'.format(stock_id)]
        if not 'T' in stock_group.keys():
            #raise KeyError('Empty DB: {:06}'.format(stock_id))
            print('Empty DB: {:06}'.format(stock_id))
            return pd.DataFrame([])
        else:
            for k in self._keys:
                fields[k] = self.get_field_timeseries(stock_id, k)
            return pd.DataFrame(fields)
    
    def get_field_crosssection_by_index(self, field_name:str, index):
        cross_section = dict()
        if ("Flag" in field_name) or ("OrderType" == field_name):
            processor = chr
        else:
            processor = lambda x: x
        field_id = self._keys[field_name]
        for s in self.get_stock_pool():
            if(len(self._data['FeaturesDB'][s].keys()) == 0):
                continue
            raw = self._data['FeaturesDB'][s][field_id][index]
            cross_section[s] = processor(raw)
        return pd.Series(cross_section)
    
    def get_field_crosssection_by_timestamp(self, field_name:str, timestamp):
        pass
    
    def summary(self):
        return {
            'count': self.count()
        }
    
    def count(self):
        count_num = dict()
        for s in self.get_stock_pool():
            if(len(self._data['FeaturesDB'][s].keys()) == 0):
                count_num[s] = 0
                continue
            count_num[s] = len(self._data['FeaturesDB'][s]['T'])
        return count_num
    
    def data(self):
        return self._data


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract stock depth/trans/order data from hdf5 file.")
    #parser.add_argument("today", help="date in format YYYYMMDD")
    parser.add_argument("-p", "--path", default="/mnt/StockLevelII/V2/Tdf", help="Path to data file having date/{depth_market.transaction,order}.h5 files")
    parser.add_argument("-s", "--savepath", default="/mnt/StockLevelII/Level2CSV/depth")
    args = parser.parse_args()

    start_day = 20220103
    end_day = 20220104

    
    # target_days = [20210924,20211011,20211217]
    #today = args.today
    path = args.path
    savepath = args.savepath
    datedir = os.listdir(path)
    datedir.sort()
    for today in datedir:
        # if int(today) in target_days:
        if int(today) >=start_day and int(today) <=end_day:
            data_root = os.path.join(path, today)   
            depthtime_to_datetime = lambda x: datetime.strptime(today+str(int(x)), '%Y%m%d%H%M%S%f')
            string_to_datetime = lambda s: datetime.strptime(today+s, '%Y%m%d%H:%M:%S.%f')

    
            depth_filename = os.path.join(data_root, 'depth_market.h5')
            #trans_filename = os.path.join(data_root, 'transaction.h5' )
            #order_filename = os.path.join(data_root, 'order.h5'       )
   
            depth_reader = H5Level2Reader(depth_filename, depth_keys)
            #trans_reader = H5Level2Reader(trans_filename, trans_keys)
            #order_reader = H5Level2Reader(order_filename, order_keys)


            if not os.path.exists(savepath+'/'+str(today)):
                os.makedirs(savepath+'/'+str(today))

            for main_stock in depth_reader.get_stock_pool():
                print("Processing DepthMarket:",main_stock,' ',today)
                main_depth = depth_reader.get_stock_dataframe(int(main_stock))
                main_depth.to_csv(savepath+'/'+str(today)+'/{:06d}.csv'.format(int(main_stock)),index = False)
    
    

    

    #for main_stock in trans_reader.get_stock_pool():
    #    print("Processing TransMarket:",main_stock)
    #    main_trans = trans_reader.get_stock_dataframe(int(main_stock))
    #    main_trans.to_csv(savepath+'/'+str(today)+'/{:06d}.trans.csv'.format(int(main_stock)),index = False)

    #for main_stock in order_reader.get_stock_pool():
    #    print("Processing OrderMarket:",main_stock)
    #    main_order = order_reader.get_stock_dataframe(int(main_stock))
    #    main_order.to_csv(savepath+'/'+str(today)+'/{:06d}.depth.csv'.format(int(main_stock)),index = False)
         
    
