# -*- coding: utf-8 -*-
"""
Created on Fri Feb 16 20:25:12 2024

@author: Mohamed Ahmed
"""
#Importing nesessary packages
import requests
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt


#doing all data extraction through API calls
def Data_Extraction(endpoint):
    try:
        response=requests.get(endpoint)
        data=response.json() if response.status_code==200 else None
        data_normalized=pd.json_normalize(data)
    except:
        conn = sqlite3.connect('DataPipeline')
        pd.DataFrame({'Erorr':['Error featching data from API']}).to_sql(name='logs', con=conn,if_exists='append', index=True)
        conn.close()
    return data_normalized

#Doing all Data manipulation across multiple DFs
def Data_Manipulation(data):
    try:
        data['quarter'] =pd.to_datetime(data['order_date']).dt.quarter
        data['month'] =pd.to_datetime(data['order_date']).dt.month
        data['weather.main']=data['weather'].map(lambda v: v['main'])
        data['weather.descreption']=data['weather'].map(lambda v: v['description'])
        data=data.drop('weather', axis=1)
        Write_DB(data,'Data_Transformed')
    except:
        conn = sqlite3.connect('DataPipeline')
        pd.DataFrame({'Erorr':['Failed calculating manipulations']}).to_sql(name='logs', con=conn,if_exists='append', index=True)
        conn.close()
    return data
    
#Do all our aggregation in one place
def Data_Aggregation(data):
    try:
        df=pd.DataFrame()
        df['Sum_price_per_customer']=data.groupby('customer_id')['price'].sum()
        df['Mean_quantity_per_product']=data.groupby('product_id')['quantity'].mean()
        df['Max_product_quantity']=data.groupby('product_id')['quantity'].sum().idxmax()
        df['Max_product_price']=data.groupby('product_id')['price'].sum().idxmax()
        df['Max_month_per_quantity']=data.groupby('month')['quantity'].sum().idxmax()
        df['Max_month_per_price']=data.groupby('month')['price'].sum().idxmax()
        df['Max_quarter_per_quantity']=data.groupby('quarter')['quantity'].sum().idxmax()
        df['Max_quarter_per_price']=data.groupby('quarter')['price'].sum().idxmax()  
        Write_DB(df,'Data_Aggregation')
    except:
        conn = sqlite3.connect('DataPipeline')
        pd.DataFrame({'Erorr':['Error while calculating aggregations']}).to_sql(name='logs', con=conn,if_exists='append', index=True)
        conn.close()

#Write Into our sqlite DB
def Write_DB(df,table_name):
    try:
        conn = sqlite3.connect('DataPipeline')
        df.to_sql(name=table_name, con=conn,if_exists='append', index=True)
        
    except:
        conn = sqlite3.connect('DataPipeline')
        pd.DataFrame({'Erorr':['Failed to wirte to DB']}).to_sql(name='logs', con=conn,if_exists='append', index=True)
    
    finally:
        conn.close()
        
        
#Reading from sqlite DB       
def Read_DB(query):
    try:
        conn = sqlite3.connect('DataPipeline')
        df = pd.read_sql_query(query, conn)
        return df
    except:
        conn = sqlite3.connect('DataPipeline')
        pd.DataFrame({'Erorr':['Failed to wirte to DB']}).to_sql(name='logs', con=conn,if_exists='append', index=True)
    
    finally:
        conn.close()
        

#Getting some visualization and analysis        
def Visualize():
    try:
        df = Read_DB('select order_id,product_id,customer_id,price,quantity,month,quarter ,order_date from Data_Transformed')
        p1=plt.bar(df['order_date'], df['price'])
        p2=plt.bar(df['order_date'], df['quantity'])
        return p1,p2
    
    except:
        conn = sqlite3.connect('DataPipeline')
        pd.DataFrame({'Erorr':['Error while  visualization']}).to_sql(name='logs', con=conn,if_exists='append', index=True)
        conn.close()


        
def main():
    try:
        user_data=Data_Extraction('https://jsonplaceholder.typicode.com/users')
        user_data=user_data.rename(columns={'id':'customer_id'})
        user_data=user_data[['customer_id','name','username','email','phone','address.geo.lat','address.geo.lng','company.name']]
        Write_DB(user_data, 'Customers')
        sales_data=pd.read_csv('AIQ - Data Engineer Assignment - Sales data.csv')
        Write_DB(sales_data,'Products')
        weather={}
        df_weather=pd.DataFrame()
        for i in range (len(user_data)):
            if user_data['customer_id'][i] not in weather:                          
                weather[user_data['customer_id'][i]]=Data_Extraction('https://api.openweathermap.org/data/2.5/weather?appid=a8ce3805722343fe8d97a1cc8a693881&lat='+user_data['address.geo.lat'][i]+'&lon='+user_data['address.geo.lng'][i])
                df_weather=df_weather._append(pd.DataFrame({'customer_id':user_data['customer_id'][i],'temp':weather[user_data['customer_id'][i]]['main.temp'],'weather':weather[user_data['customer_id'][i]]['weather'][0]}))
        merged_data=pd.merge(sales_data,user_data ,on='customer_id')
        merged_data=pd.merge(merged_data,df_weather ,on='customer_id')
        Manipulated_data=Data_Manipulation(merged_data)
        Data_Aggregation(Manipulated_data)
        return True
    except:
        conn = sqlite3.connect('DataPipeline')
        pd.DataFrame({'Erorr':['Failure in the main logic']}).to_sql(name='logs', con=conn,if_exists='append', index=True)
        conn.close()
