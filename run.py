import pandas as pd
import math
import datetime



def getSourceURL():
  basic_path = "http://stat.taiwan.net.tw/system/cross_sheet_right_months_DESTINATIONAGESEXUAL_result.asp"
  from_year = 106
  from_month = '1'
  to_year = 106
  to_month = '4'
  data_type = 0 # 1 入境 0 出境
  other_parameter = '4&0&%d'%data_type 
  full_path = "%s?%d&%d&%s&%s&%s"%(basic_path,from_year,to_year,from_month,to_month,other_parameter)
  return full_path

def getSource(url,in_path):
  print("抓檔案中...")
  tables = pd.read_html(url)
  table = tables[0]
  table = table.drop(table.columns[116:],axis=1)[2:-1]
  table.to_csv(in_path,index=False,header=False)
  print("抓取完成")

def cleanSouce(in_path,out_path):
  table = pd.read_csv(in_path)
  columns = table.columns[2:]
  cols = len(table.columns)
  length = len(table)

  year = []
  month = []
  country = []
  gender = []
  age = []
  value = []

  age_labels = ['1-12','13-19','20-29','30-39','40-49','50-59','60-65','66+']
  for i in range(length):
      for j in range(len(columns)):
          if type(table.values[i,j+2]) == str:
              t = pd.Series(table.values[i,j+2].split(' ')).map(int)            
          else:
              t = pd.Series([0]*16)
          for k in t.keys():
              year.append(table.values[i,0])
              month.append(table.values[i,1])
              country.append(columns[j])
              gender.append(['男','女'][k%2])
              age.append(age_labels[int(k/2)])
              value.append(t[k])
  d = {'year':year, 'month':month,'country':country,'gender':gender,'age':age,'value':value}
  result = pd.DataFrame(data=d,columns=['year','month','country','gender','age','value'])  
  result.to_csv(out_path,index=False)
  print("處理完成")


def run():  
  source_path = 'raw_data.csv'
  out_path = 'source.csv'
  getSource(getSourceURL(),source_path)    
  cleanSouce(source_path,out_path)


if __name__ == "__main__":
  run()
