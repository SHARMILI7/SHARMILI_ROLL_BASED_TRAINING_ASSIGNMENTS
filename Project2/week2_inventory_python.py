import pandas as pd
import numpy as np

df=pd.read_csv(r"D:\Project2\stock_movements.csv")

print("===== ORIGINAL STOCK DATA =====")
print(df)

df=df.dropna(subset=['product_name'])

df['movement_date']=pd.to_datetime(df['movement_date'],errors='coerce')

df['quantity']=pd.to_numeric(df['quantity'],errors='coerce').fillna(0)

df['quantity']=np.clip(df['quantity'],0,None)

df['net_quantity']=np.where(df['movement_type']=='IN',df['quantity'],-df['quantity'])

stock_summary=df.groupby(['product_id','product_name','reorder_level'])['net_quantity'].sum().reset_index()

stock_summary.rename(columns={'net_quantity':'current_stock'},inplace=True)

stock_summary['reorder_flag']=np.where(stock_summary['current_stock']<stock_summary['reorder_level'],'Reorder Needed','Stock OK')

print("\n===== CURRENT STOCK SUMMARY =====")
print(stock_summary)

low_stock=stock_summary[stock_summary['current_stock']<stock_summary['reorder_level']]

print("\n===== LOW STOCK PRODUCTS =====")
print(low_stock)

stock_summary.to_csv(r"D:\Project2\cleaned_stock.csv",index=False)

print("\ncleaned_stock.csv exported successfully")
