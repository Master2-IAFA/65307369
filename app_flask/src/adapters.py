import datetime as dt 
import requests as rq
import json 
from pandas import json_normalize
import time
import pandas as pd 
from pandasql import sqldf
import numpy as np


# Adapters

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