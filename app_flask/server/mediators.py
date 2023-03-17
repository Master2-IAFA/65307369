import datetime as dt 
import requests as rq
import json
import pandas as pd 
import time
import numpy as np
import psutil
import os
import csv
import re 
from sqlite3 import connect


from server.adapters import EnergyAdapter1,EnergyAdapter2,PopulationAdapter,LogementAdapter


# Mediator

class InformationMediator:
    def __init__(self):
        self.energy_adapter1 =  EnergyAdapter1()
        self.energy_adapter2 =  EnergyAdapter2()
        self.population_adapter = PopulationAdapter()
        self.Logement_adapter = LogementAdapter()
    
    def get_API1_data(self):
        return pd.DataFrame(self.energy_adapter1.get_energy_production())    
    def get_API2_data(self):
        return pd.DataFrame(self.energy_adapter2.get_energy_production())        
    def get_API3_data(self):
        return pd.DataFrame(self.population_adapter.get_population())
    def get_API4_data(self):
        return pd.DataFrame(self.Logement_adapter.API_Logement_sociaux_bailleurs())
    
    def restriction(self,Query,df1,df2,df3,df4):
        
        start, end = "WHERE",""
        conds=Query[Query.find(start)+len(start):Query.rfind(end)].strip().split(' and ')
        pattern = r'[<>=!]=?'
        matches=[]
        conditions=[]
        cond_api1, cond_api2, cond_api3,cond_api4 = [], [], [], []
        df1Cols=df1.columns
        df2Cols=df2.columns
        df3Cols=df3.columns
        df4Cols=df4.columns
        
        for i in range(len(conds)):
            matches.append(re.findall(pattern, conds[i])[0])

        for i in range(len(conds)):
            pattern=conds[i].split(f'{matches[i]}')[0]
            if ( pattern in df1Cols) and (pattern in df2Cols) and (pattern in df3Cols) and (pattern in df4Cols):
                cond_api1.append(conds[i]), cond_api2.append(conds[i]), 
                cond_api3.append(conds[i]), cond_api4.append(conds[i])
            
            elif (pattern in df1Cols) and (pattern in df2Cols)and (pattern in df3Cols):
                cond_api1.append(conds[i]), cond_api2.append(conds[i]),cond_api3.append(conds[i])
            elif (pattern in df1Cols) and (pattern in df2Cols)and (pattern in df4Cols):
                cond_api1.append(conds[i]), cond_api2.append(conds[i]),cond_api4.append(conds[i])
            elif (pattern in df1Cols) and (pattern in df3Cols)and (pattern in df4Cols):
                cond_api1.append(conds[i]), cond_api3.append(conds[i]),cond_api4.append(conds[i])
            elif (pattern in df2Cols) and (pattern in df3Cols)and (pattern in df4Cols):
                cond_api2.append(conds[i]), cond_api3.append(conds[i]),cond_api4.append(conds[i])
            
            elif (pattern in df1Cols) and (pattern in df2Cols):
                cond_api1.append(conds[i]), cond_api2.append(conds[i])
            elif (pattern in df1Cols) and (pattern in df3Cols):
                cond_api1.append(conds[i]), cond_api3.append(conds[i])
            elif (pattern in df1Cols) and (pattern in df4Cols):
                cond_api1.append(conds[i]), cond_api4.append(conds[i])
            elif (pattern in df2Cols) and (pattern in df3Cols):
                cond_api2.append(conds[i]), cond_api3.append(conds[i])
            elif (pattern in df2Cols) and (pattern in df4Cols):
                cond_api2.append(conds[i]), cond_api4.append(conds[i])
            elif (pattern in df3Cols) and (pattern in df4Cols):
                cond_api3.append(conds[i]), cond_api4.append(conds[i])
            
            elif (pattern in df1Cols):
                cond_api1.append(conds[i])
            elif (pattern in df2Cols):
                cond_api2.append(conds[i])
            elif (pattern in df3Cols):
                cond_api3.append(conds[i])
            else:
                cond_api4.append(conds[i])
                
            cond1=' and '.join(i for i in cond_api1)
            cond2=' and '.join(i for i in cond_api2)
            cond3=' and '.join(i for i in cond_api3)
            cond4=' and '.join(i for i in cond_api4)
        print("cond1: ", cond1, "cond2: ", cond2, "cond3: ", cond3)        
        
        Q1=f""" SELECT * FROM df1 WHERE {cond1} """
        Q2=f""" SELECT * FROM df2 WHERE {cond2} """
        Q3=f""" SELECT * FROM df3 WHERE {cond3} """
        Q4=f""" SELECT * FROM df4 WHERE {cond4} """
        
        conn = connect(':memory:')

        if len(cond1)>1:
            df1.to_sql('df1',conn)
            df1 = pd.read_sql(Q1,conn)

        if len(cond2)>1:
            df2.to_sql('df2',conn)
            df2 = pd.read_sql(Q2,conn)

        if len(cond3)>1:
            df3.to_sql('df3',conn)
            df3 = pd.read_sql(Q3,conn)

        if len(cond4)>1:
            df4.to_sql('df4',conn)
            df4 = pd.read_sql(Q4,conn)
        
        print("\n Shape apres la sélection")
        print(f"\n {df1.shape} = 𝜎_{cond1}_(API_1)")
        print(f"\n {df2.shape} = 𝜎_{cond2}_(API_2)")
        print(f"\n {df3.shape} = 𝜎_{cond3}_(API_3)")
        print(f"\n {df4.shape} = 𝜎_{cond4}_(API_4)")
            
        return df1,df2,df3,df4
    
    
    def projection(self,list_api_1, list_api_2, list_api_3, list_api_4,Query):
        start, end = 'SELECT', 'FROM'
        cols_in_query=Query[Query.find(start)+len(start):Query.rfind(end)].strip().split(', ')
        list_api1, list_api2, list_api3, list_api4 = [], [], [], []
        for word in cols_in_query:
            if (word in list_api_1) and (word in list_api_2) and (word in list_api_3) and (word in list_api_4):
                list_api1.append(word), list_api2.append(word)
                list_api3.append(word), list_api4.append(word)
            
            elif (word in list_api_1) and (word in list_api_2) and (word in list_api_3):
                list_api1.append(word), list_api2.append(word), list_api3.append(word)
            elif (word in list_api_1) and (word in list_api_2) and (word in list_api_4):
                list_api1.append(word), list_api2.append(word), list_api4.append(word)
            elif (word in list_api_1) and (word in list_api_3) and (word in list_api_4):
                list_api1.append(word), list_api3.append(word), list_api4.append(word)    
            elif (word in list_api_2) and (word in list_api_3) and (word in list_api_4):
                list_api2.append(word), list_api3.append(word), list_api4.append(word)

            elif (word in list_api_1) and (word in list_api_2):
                list_api1.append(word), list_api2.append(word)
            elif (word in list_api_1) and (word in list_api_3):
                list_api1.append(word), list_api3.append(word)
            elif (word in list_api_1) and (word in list_api_4):
                list_api1.append(word), list_api4.append(word)
                
            elif (word in list_api_2) and (word in list_api_3):
                list_api2.append(word), list_api3.append(word)
            elif (word in list_api_2) and (word in list_api_4):
                list_api2.append(word), list_api4.append(word)
                
            elif (word in list_api_3) and (word in list_api_4):
                list_api3.append(word), list_api4.append(word)

            elif (word in list_api_1):
                list_api1.append(word)
            elif (word in list_api_2) :
                list_api2.append(word)
            elif (word in list_api_3) :
                list_api3.append(word)
            else:
                list_api4.append(word) 
                
#         print(
#             '\n List Cols API_1',list_api1,
#             '\n List Cols API_2',list_api2,
#             '\n List Cols API_3',list_api3,
#             '\n List Cols API_4',list_api4)
        
        return list_api1, list_api2, list_api3, list_api4
    
    def jointure(self,df1,df2,df3,df4,list_api1,list_api2,list_api3,list_api4):
        
        if len(list_api1)>1 and len(list_api2)>1 and len(list_api3)>1 and len(list_api4)>1:
            df123=pd.merge(pd.merge(df1, df2, on = 'region'), df4, on = 'region')
            df=pd.DataFrame(pd.merge(df123, df3, on = 'region'))
            print(f"\n {df.shape} = (API_1 ⋈ API_2 ⋈ API_3 ⋈ API_4)")
            
        elif len(list_api1)>1 and len(list_api2)>1 and len(list_api3)>1:
            df=pd.merge(pd.merge(df1, df2,  on = 'region'),df3,on = 'region')
            print(f"\n {df.shape} = (API_1 ⋈ API_2 ⋈ API_3 )")
        elif len(list_api1)>1 and len(list_api2)>1 and len(list_api4)>1:
            df=pd.merge(pd.merge(df1, df2,  on = 'region'),df4,on = 'region')
            print(f"\n {df.shape} = (API_1 ⋈ API_2 ⋈ API_4)")
        elif len(list_api1)>1 and len(list_api3)>1 and len(list_api4)>1:
            df=pd.merge(pd.merge(df1, df4,  on = 'region'),df3,on = 'region')
            print(f"\n {df.shape} = (API_1 ⋈ API_3 ⋈ API_4)")
        elif len(list_api2)>1 and len(list_api3)>1 and len(list_api4)>1:
            df=pd.merge(pd.merge(df2, df4,  on = 'region'),df3,on = 'region')
            print(f"\n {df.shape} = (API_2 ⋈ API_3 ⋈ API_4)")
        
        elif len(list_api1)>1 and len(list_api2)>1 :
            df=pd.DataFrame(pd.merge(df1, df2,  on = 'region'))
            print(f"\n {df.shape} = (API_1 ⋈ API_2)")
        elif len(list_api1)>1 and len(list_api3)>1 :
            df=pd.DataFrame(pd.merge(df1, df3,  on = 'region'))
            print(f"\n {df.shape} = (API_1 ⋈ API_3)")
        elif len(list_api1)>1 and len(list_api4)>1 :
            df=pd.DataFrame(pd.merge(df1, df4,  on = 'region'))
            print(f"\n {df.shape} = (API_1 ⋈ API_4)")
            
        elif len(list_api2)>1 and len(list_api3)>1 :
            df=pd.DataFrame(pd.merge(df2, df3,  on = 'region'))
            print(f"\n {df.shape} = (API_2 ⋈ API_3)")
        elif len(list_api2)>1 and len(list_api4)>1 :
            df=pd.DataFrame(pd.merge(df2, df4,  on = 'region'))
            print(f"\n {df.shape} = (API_2 ⋈ API_4)")
            
        elif len(list_api3)>1 and len(list_api4)>1 :
            df=pd.DataFrame(pd.merge(df3, df4,  on = 'region'))
            print(f"\n{df.shape} = (API_3 ⋈ API_4)")
            
        elif len(list_api1)>1 :
            df=pd.DataFrame(df1)
            print(f"\n {df.shape} = (API_1)")
            
        elif len(list_api2)>1 :
            df=pd.DataFrame(df2)
            print(f"\n {df.shape} = (API_2)")
        
        elif len(list_api3)>1 :
            df=pd.DataFrame(df3)
            print(f"\n {df.shape} = (API_3)")
        
        else:
            df=pd.DataFrame(df4)
            print(f"\n {df.shape} = (API_4)")

        return df  
    
    
    
    def data(self,Query):      
        
        print('\n ---------------- Initial shapes start ----------------')
        informationMediator = InformationMediator()
        df1 = informationMediator.get_API1_data()  
        df2 = informationMediator.get_API2_data()  
        df3 = informationMediator.get_API3_data()
        df4 = informationMediator.get_API4_data()
        print(f" API_1 :{df1.shape}")
        print(f" API_2 :{df2.shape}")
        print(f" API_3 :{df3.shape}")
        print(f" API_4 :{df4.shape}")
        print('\n ---------------- Initial shapes end ----------------')
        
        # La sélection 𝜎
        print('\n ---------------- 𝜎 start  ----------------')
        if 'WHERE' in Query:
            print("\n Selection d'abord par conditions")
            df1,df2,df3,df4 = self.restriction(Query,df1,df2,df3,df4)
        else:
            print("\n Pas de conditions sur les APIs")
        print('\n ---------------- 𝜎 end ----------------') 
        
        # Π : La projection
        print('\n ---------------- Π start ----------------')
        list_api_1, list_api_2, list_api_3, list_api_4 = df1.columns, df2.columns, df3.columns, df4.columns
        list_api1,list_api2,list_api3,list_api4 = self.projection(list_api_1, list_api_2, list_api_3, list_api_4, Query)
        df1, df2, df3, df4 = df1[list_api1], df2[list_api2], df3[list_api3], df4[list_api4]
        print('\n Shapes apres projection:')
        
        if len(list_api1)>1 :
            print(f"\n {df1.shape} = Π_{list_api1}_(API_1)")
        if len(list_api2)>1 :
            print(f"\n {df2.shape} = Π_{list_api2}_(API_2)")
        if len(list_api3)>1 :
            print(f"\n {df3.shape} = Π_{list_api3}_(API_3)")
        if len(list_api4)>1 :
            print(f"\n {df4.shape} = Π_{list_api4}_(API_4)")
        print('\n ---------------- Π end ----------------') 


        # ⋈ : La jointure 
        print('\n ---------------- ⋈ start  ----------------')
        df = self.jointure(df1,df2,df3,df4,
                           list_api1,
                           list_api2,
                           list_api3,
                           list_api4)             
        print('\n ---------------- ⋈ end ----------------')
                
        
        return df