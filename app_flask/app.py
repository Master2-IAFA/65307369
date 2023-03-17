from flask import Flask, render_template, request, Markup

import datetime as dt 
import requests as rq
import json 
from pandas import json_normalize
import time
import pandas as pd 
import numpy as np
import psutil
import os


# import de la classe mediateur 
from server.mediators import InformationMediator

app = Flask(__name__)

@app.route('/')
def homepage():
    return render_template('formulaire.html')

@app.route('/infos')
def infos_page():
    return render_template('infos.html')

@app.route('/sqlQuery')
def sqlQuery():
    
    return render_template('sql_query_form.html')

@app.route('/result')
def results():
    return render_template('results.html')

@app.route('/post_submit_form',methods=['get','post'])
def post_submit_form():

    isSQL = False
    title_form = "La requete Choisie est  : "

    if request.method == 'POST':

        query = request.form["defined_query"]

    else:
        return 'Error'

    pid = os.getpid()
    py = psutil.Process(pid)
    res = InformationMediator().data(query)
    res = res.to_html()
    
    return render_template('results.html',query=query,res=Markup(res),title_form=title_form,isSQL=isSQL)




@app.route('/post_submit_sql',methods=['get','post'])
def post_submit_sql():

    res = None
    isSQL = True
    title_sql = "La Requête Saisie : "
    rows = []

    if request.method == 'POST':

        # get the input values 

        restriction = request.form['restriction']

        if request.form.get('region_row') == "region":
            region_row = request.form['region_row']
            rows.append(region_row)
    
        if request.form.get('date_heure') == "date_heure":
            date_heure = request.form['date_heure']
            rows.append(date_heure)
        
        if request.form.get('annee') == "annee":
            annee = request.form['annee']
            rows.append(annee)
        
        if request.form.get('dep_code') == "dep_code":
            dep_code = request.form['dep_code']
            rows.append(dep_code)
        
        if request.form.get('nbre_bailleurs') == "nbre_bailleurs":
            nbre_bailleurs = request.form['nbre_bailleurs']
            rows.append(nbre_bailleurs)
        
        if request.form.get('start_year') == "start_year":
            start_year = request.form['start_year']
            rows.append(start_year)
        
        if request.form.get('geo_year') == "geo_year":
            geo_year = request.form['geo_year']
            rows.append(geo_year)
        
        if request.form.get('census_year') == "census_year":
            census_year = request.form['census_year']
            rows.append(census_year)
     
        if request.form.get('date') == "date":
            date = request.form['date']
            rows.append(date)
        
        if request.form.get('heure') == "heure":
            heure = request.form['heure']
            rows.append(heure)

        if request.form.get('part_logement_sociaux_geres_sem') == "part_logement_sociaux_geres_sem":
            part_logement_sociaux_geres_sem = request.form['part_logement_sociaux_geres_sem']
            rows.append(part_logement_sociaux_geres_sem)

        if request.form.get('part_logement_sociaux_geres_sem') == "part_logement_sociaux_geres_sem":
            part_logement_sociaux_geres_sem = request.form['part_logement_sociaux_geres_sem']
            rows.append(part_logement_sociaux_geres_sem)
        
        if request.form.get('nbre_logements') == "nbre_logements":
            nbre_logements = request.form['nbre_logements']
            rows.append(nbre_logements)
        
        if request.form.get('nbre_bailleurs_consolide') == "nbre_bailleurs_consolide":
            nbre_bailleurs_consolide = request.form['nbre_bailleurs_consolide']
            rows.append(nbre_bailleurs_consolide)
        
        if request.form.get('com_arm_code') == "com_arm_code":
            com_arm_code = request.form['com_arm_code']
            rows.append(com_arm_code)
        
        # if request.form.get('com_arm_pop_cap') == "com_arm_pop_cap":
        #     com_arm_pop_cap = request.form['com_arm_pop_cap']
        #     rows.append(com_arm_pop_cap)
        
        if request.form.get('code_insee_region') == "code_insee_region":
            code_insee_region = request.form['code_insee_region']
            rows.append(code_insee_region)
        
        if request.form.get('consommation_brute_electricite_rte') == "consommation_brute_electricite_rte":
            consommation_brute_electricite_rte = request.form['consommation_brute_electricite_rte']
            rows.append(consommation_brute_electricite_rte)
        
        if request.form.get('consommation_brute_totale') == "consommation_brute_totale":
            consommation_brute_totale = request.form['consommation_brute_totale']
            rows.append(consommation_brute_totale)



        # construct a Query 

        n = len(rows)
        # projection
        Query = "SELECT "
        # if request.form.get('all-rows') == "all-rows":
        #      Query += "* "

        # else:
        for i in range(n-1):
            Query += rows[i] + ", "
        Query += rows[n-1]

        Query += " FROM df "

        # restriction
        Query += " WHERE " + restriction

        # envoi de la requete
        pid = os.getpid()
        py = psutil.Process(pid)
        res = InformationMediator().data(Query)
        res = res.to_html()

    else:
        res =  'Error'


    

    return render_template('results.html', Query = Query, res=Markup(res), restriction=restriction,rows=rows,title_sql=title_sql,isSQL=isSQL)

if __name__ == "__main__":
    app.run(debug=True)
