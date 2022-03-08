# -*- coding: utf-8 -*-
"""
Created on Wed Sep 29 12:22:27 2021

@author: lumini04
"""

import requests
import json
import pandas as pd
import numpy as np
import glob
import config 
url = 'http://localhost:5000/leads?apikey=031edd7d41651593c5fe5c006fa5752b37fddff7bc4e843aa6af0c950f4b9406'
arq_list = glob.glob(config.main_path +'/recebidos'+ "/*"+".csv")

for arq in arq_list:
    try:
        print("ok")
        df = pd.read_csv(arq_list)
    except:
        print('DataFrame Vazio')
    id_lead_cod_entrada = sum(df['ID_LEAD'])*len(df['ID_LEAD'])
    body_json = df.to_dict()
    body = {"leads":body_json, "flagDistribuicaoScore":False}
    
    response = requests.request('POST', url, headers={'Content-Type':'application/json'},zdata=json.dumps(body))
    r = df.from_dict(response.json()).T
    id_lead_cod_saida = sum(r['ID_LEAD'])*len(r['ID_LEAD'])
    if id_lead_cod_entrada == id_lead_cod_saida:
        print('Retorno OK')
    


