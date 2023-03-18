import datetime as dt 
import requests as rq
import matplotlib.pyplot as plt
import json 
from pandas import json_normalize
import time
import pandas as pd 
from pandasql import sqldf
import numpy as np
import psutil
import os
import csv
import re

## Adapters
# Energy adapters
class EnergyAdapter1:
    def __init__(self):
        """ The adapater for the first API Energy"""
        self.base_url = 'https://odre.opendatasoft.com/api/records/1.0/search/?dataset=consommation-quotidienne-brute-regionale&q=&sort=-consommation_brute_gaz_grtgaz&facet=date_heure&facet=code_insee_region&facet=region'
        self.list_of_cols = [ 'region', 'code_insee_region','date_heure', 'date', 'heure', 'consommation_brute_electricite_rte', 'consommation_brute_totale', 'consommation_brute_gaz_totale']

    def get_energy_production(self):
        response = rq.get(self.base_url)
        Repense = response.content
        parse_json = json.loads(Repense)
        n=parse_json['records'].__len__()
        list_data=[x for x in parse_json['records']]
        list_dic,df={},{}
        for i in range(n):
            list_dic[i] = {key: list_data[i]["fields"][key] for key in self.list_of_cols}
        data=pd.DataFrame.from_dict(list_dic[0], orient='index').T
        for i in range(1,n):
            df[i]=pd.DataFrame.from_dict(list_dic[i], orient='index').T
            data=pd.concat([data,df[i]], axis=0)
        return data
class EnergyAdapter2:
    def __init__(self):
        """ The adapater for the second API of Energy """
        self.base_url = "https://opendata.agenceore.fr/api/records/1.0/search/?dataset=production-demi-horaire-agregee-par-region&q=&sort=-horodate&facet=horodate&facet=region&facet=grd"
        self.list_of_cols = ['region','code','horodate','energie_injectee','nb_points_injection' ]
    def get_energy_production(self):
        return EnergyAdapter1.get_energy_production(self)
# population adapter
class PopulationAdapter:
    def __init__(self):
        """ The adapater for  Population API """
        self.base_url = "https://public.opendatasoft.com/api/records/1.0/search/?dataset=demographyref-france-pop-legale-commune-arrondissement-municipal-millesime&q=&rows=3536&facet=reg_code&facet=reg_name&facet=com_arm_code&facet=com_arm_name&facet=dep_code&facet=arrdep_code&facet=census_year&facet=start_year&facet=geo_year&facet=epci_name&facet=epci_code&facet=dep_name"
        self.list_of_cols = ['com_arm_pop_tot', 'reg_name', 'start_year', 'arrdep_code','geo_year', 'census_year',  'com_arm_code', 'com_arm_pop_mun', 'dep_code', 'com_arm_pop_cap', 'reg_code',  'com_arm_name'] 
        #'epci_name', 'epci_code', 'dep_name'

    def get_population(self):
        data=EnergyAdapter1.get_energy_production(self)
        data=data.rename(columns={"reg_name": "region"})
        return data
# Logement_sociaux_bailleurs adapter
class LogementAdapter:
    def __init__(self):
        """ The adapater for  Logement API """
        self.base_url = "https://opendata.caissedesdepots.fr/api/records/1.0/search/?dataset=bailleurs_sociaux_region&q=&rows=1000&facet=code_region&facet=libelle_region&facet=annee"
        self.list_of_cols = ['part_logement_sociaux_geres_sem', 'nbre_logements', 'code_region',
                             'nbre_bailleurs_consolide', 'part_logements_sociaux_geres_autres_bailleurs', 
                             'nbre_bailleurs','annee', 'libelle_region', 'part_logement_sociaux_geres_esh',
                             'part_logement_sociaux_geres_oph']
                              

    def API_Logement_sociaux_bailleurs(self):
        req = rq.get(self.base_url)
        data = json.loads(req.content)
        data = pd.DataFrame(data['records'])
        data.drop(columns=["datasetid", "recordid", "record_timestamp"], axis=1, inplace=True)
        data = json_normalize(data['fields'])
        data=data.rename(columns={"libelle_region": "region"})
        return pd.DataFrame(data)

## Mediator

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
        #print("cond1: ", cond1, "cond2: ", cond2, "cond3: ", cond3)        
        
        Q1=f""" SELECT * FROM df1 WHERE {cond1} """
        Q2=f""" SELECT * FROM df2 WHERE {cond2} """
        Q3=f""" SELECT * FROM df3 WHERE {cond3} """
        Q4=f""" SELECT * FROM df4 WHERE {cond4} """
        
        if len(cond1)>1:
            df1=sqldf(Q1)
        if len(cond2)>1:
            df2=sqldf(Q2)
        if len(cond3)>1:
            df3=sqldf(Q3)
        if len(cond4)>1:
            df4=sqldf(Q4)
        
        #print("\n Shape apres la sélection")
        if len(cond1)>1:
            print(f"\n {df1.shape} = 𝜎_{cond1}_(API_1)")
        if len(cond2)>1:
            print(f"\n {df2.shape} = 𝜎_{cond2}_(API_2)")
        if len(cond3)>1:
            print(f"\n {df3.shape} = 𝜎_{cond3}_(API_3)")
        if len(cond4)>1:
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
        #print( '\n\n cols of df:' , df.columns)
        return df  
    
    
    
    def data(self,Query):      
        
        print('\n ---------------- APIs shapes: ----------------')
        informationMediator = InformationMediator()
        df1 = informationMediator.get_API1_data()  
        df2 = informationMediator.get_API2_data()  
        df3 = informationMediator.get_API3_data()
        df4 = informationMediator.get_API4_data()
        print(f" API_1 :{df1.shape}")
        print(f" API_2 :{df2.shape}")
        print(f" API_3 :{df3.shape}")
        print(f" API_4 :{df4.shape}")
        print('\n ----------------------------------------------')
        
        # La sélection 𝜎
        print('\n -------- 𝜎 ')
        if 'WHERE' in Query:
            #print("\n Selection d'abord par conditions")
            df1,df2,df3,df4 = self.restriction(Query,df1,df2,df3,df4)
        else:
            print("\n pas de 𝜎")
        #print('\n -------------------------------------------') 
        
        # Π : La projection
        print('\n -------- Π ')
        list_api_1, list_api_2, list_api_3, list_api_4 = df1.columns, df2.columns, df3.columns, df4.columns
        list_api1,list_api2,list_api3,list_api4 = self.projection(list_api_1, list_api_2, list_api_3, list_api_4, Query)
        df1, df2, df3, df4 = df1[list_api1], df2[list_api2], df3[list_api3], df4[list_api4]
        #print('\n Shapes apres projection:')
        
        if len(list_api1)>1 :
            print(f"\n {df1.shape} = Π_{list_api1}_(API_1)")
        if len(list_api2)>1 :
            print(f"\n {df2.shape} = Π_{list_api2}_(API_2)")
        if len(list_api3)>1 :
            print(f"\n {df3.shape} = Π_{list_api3}_(API_3)")
        if len(list_api4)>1 :
            print(f"\n {df4.shape} = Π_{list_api4}_(API_4)")
        #print('\n -------------------------------------------') 


        # ⋈ : La jointure 
        print('\n -------- ⋈')
        df = self.jointure(df1,df2,df3,df4,
                           list_api1,
                           list_api2,
                           list_api3,
                           list_api4)             
        print('\n -------------------------------------------')
                
        
        return df
    

## Test

FirstQuery="""
SELECT region, heure, consommation_brute_electricite_rte, energie_injectee 
FROM df"""

pid = os.getpid()
py = psutil.Process(pid)
cpuStart = py.cpu_percent()
memoryStart = py.memory_info()[0] / 2. ** 30
start_time = time.time()

InformationMediator().data(FirstQuery)

elapsed_time =abs(time.time() - start_time)
cpuEnd = py.cpu_percent()
memoryEnd = py.memory_info()[0] / 2. ** 30
cpu=abs(cpuEnd - cpuStart)
memory=abs(memoryEnd - memoryStart)
print("Temps d'exécution : ", elapsed_time, " secondes.")
print("CPU utilization:", cpu)
print("Memory utilization:", memory)
