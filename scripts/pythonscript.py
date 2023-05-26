import dolphindb as ddb
s = ddb.session()
conn = s.connect("192.168.0.202", 8848, "admin", "123456")
print(conn)

# define a function that takes target_stock, target_date and horizen as parameters
def R_square(target_stock, target_date, horizen):
  # define the script as a string
  script="""

  def feature_gen(mutable t){
      update t set Price_diff = deltas(t.Price)
  }

  def trans_convert(target_date,target_stock,horizen){
      trans = loadTable("dfs://history","transBook")
      trans = select * from trans where Date = target_date and stockcode = target_stock and CancelFlag = "F"
      trans = select last(Price)as Price from trans group by interval(TransactionTime,horizen,"prev") as Time
      return trans
  }

  def profit_gen(mutable t){
      update t set stock_return = eachPre(ratio,Price)-1
  }

  def R_square_gen(target_stock, target_date ,horizen){
      string2date = temporalParse(target_date,"yyyyMMdd")
      rtb = trans_convert(string2date,target_stock,duration(horizen))
      feature_gen(rtb)
      profit_gen(rtb)
      sse =  sum(pow((rtb.Price_diff-rtb.stock_return),2))
      sst =  sum(pow((rtb.stock_return - avg(rtb.stock_return)),2))
      return 1-(sse/sst)
  }
  """
  # run the script
  s.run(script)
  # return the result of R_square_gen
  return s.run("R_square_gen",target_stock,target_date,horizen)

# call the function with some example values
print(R_square("000001", "20220901", "15m"))