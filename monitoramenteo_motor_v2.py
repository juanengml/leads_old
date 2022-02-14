#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from datetime import datetime
import glob
import numpy as np


# In[2]:


#--------------- CORRETORES ATIVOS API ----------------------------------

import requests
import json
import urllib3
urllib3.disable_warnings()

url = "https://qualitech.qualicorp.com.br/focus-leads-integration/corretor"

payload={}
headers = {}
headers['X-Gravitee-Api-Key'] = '2e31146a-f2d5-42b4-a228-341b31db7652'

response = requests.request("POST", url, headers=headers, data=payload, verify = False)

dados_corretores_ativos = json.loads(response.content)

corretores_ativos = pd.DataFrame(dados_corretores_ativos)
corretores_ativos = corretores_ativos[["cpf"]].drop_duplicates(subset="cpf")

corr_ativos = list(corretores_ativos["cpf"].unique())

#--------------- CORRETORES INTERNOS ----------------------------------

corretores_interno = pd.read_excel('C:/Users/lumini10/Desktop/motor_cognitivo_leads/Leads_recomendacao/consultores_Canal_Interno.xlsx')
corretores_interno["CPF"] = pd.to_numeric(corretores_interno["CPF"], errors='coerce')

corr_interno = list(corretores_interno.CPF.unique())

corretores_disponiveis = corretores_ativos[corretores_ativos["cpf"].isin(corr_interno)]

print(len(corretores_disponiveis), "corretores disponíveis (ativos e internos)")
corr_ativo_interno = list(corretores_disponiveis.cpf.unique())


# In[3]:


# LIMITAÇÃO DE LEADS DIARIO

ano = datetime.today().year
mes = datetime.today().month
dia = datetime.today().day

data = str(ano)+str(mes)+str(dia)
data
anomes = str(ano)+str(mes)

path_fair_motor = 'C:/Users/lumini10/Desktop/motor_cognitivo_leads/enviados/base_fair_motor'
        
base_fair_motor = pd.read_csv(path_fair_motor+'/dados_empilhados_motor.csv')

frame = base_fair_motor

n_leads = len(frame)

corretor_full = frame.groupby("CPF_CORRETOR").count().reset_index()[["CPF_CORRETOR","ID_LEAD"]]
#corretor_full = corretor_full[corretor_full["ID_LEAD"]>4

#corr_full = list(corretor_full.CPF_CORRETOR.unique())

corretor_full = frame.groupby("CPF_CORRETOR").count().reset_index()[["CPF_CORRETOR","ID_LEAD"]]

print(n_leads-1, "leads distribuidos")


corretor_full.sort_values("ID_LEAD", ascending = False)
corretor_full = corretor_full[corretor_full["CPF_CORRETOR"]>0] 
corretor_full = corretor_full.sort_values("ID_LEAD", ascending = False)

corretor_full = corretor_full[corretor_full["CPF_CORRETOR"].isin(corr_ativo_interno)]

import matplotlib.pyplot as plt
import seaborn as sns


sns.set(style="white", rc={"lines.linewidth": 3})
fig= plt.subplots(figsize=(20,5))

sns.barplot(x=corretor_full["CPF_CORRETOR"],
        y=corretor_full["ID_LEAD"], 
        color='grey',
        order=corretor_full.sort_values('ID_LEAD', ascending = False).CPF_CORRETOR).set(xticklabels=[])

print((n_leads-1) - len(frame[frame["RATING_3"]==0.5]), "leads distribuidos otimizados")

print(len(frame[frame["RATING_3"]==0.5]), "leads distribuidos randômicamente")

print(len(corretor_full), "corretores ativos e internos")


# In[4]:


group_count = corretor_full.sort_values("ID_LEAD", ascending = False)
group_count.groupby("ID_LEAD").count().reset_index()[["CPF_CORRETOR","ID_LEAD"]].sort_values("ID_LEAD", ascending = False)


# In[5]:


base_leads_distribuidos = frame
base_leads_distribuidos["tipo_distribuicao"]=["randomica" if x in [0,0.5] else "otimizada" for x in base_leads_distribuidos['RATING_3']]

base_leads_distribuidos["tipo_distribuicao"].value_counts()


# In[6]:


corretor_full = frame.groupby("CPF_CORRETOR").count().reset_index()[["CPF_CORRETOR","ID_LEAD"]]
corretor_full["%LEADS"] = corretor_full["ID_LEAD"]/(corretor_full["ID_LEAD"].sum())
corretor_full


# In[7]:


path = r'C:/Users/lumini10/Desktop/motor_cognitivo_leads/enviados'

all_files = glob.glob(path + "/*"+".csv")

li = []

for filename in all_files:
    
    try:
        df = pd.read_csv(filename, index_col=None, header=0, delimiter = ";")
        li.append(df)
    except: pass

try:    
    enviados = pd.concat(li, axis=0, ignore_index=True)
    qtd_leads=len(enviados)
    
except: qtd_leads = 0


# In[8]:


qtd_leads


# In[9]:


print(str(dia)+"/"+str(mes)+"/"+str(ano))
print(len(corretores_disponiveis), "corretores disponíveis (ativos e internos)")
print(n_leads, "leads distribuidos nos últimos 30 dias")
print((n_leads) - len(frame[frame["RATING_3"].isin([0, 0.5])]), "distribuidos otimizados")
print(len(frame[frame["RATING_3"].isin([0, 0.5])]), "distribuidos randômicamente")
print(str(qtd_leads) +" leads recebidos e distribuidos na última chamada")


# In[13]:


import io

def print_to_string(*args, **kwargs):
    output = io.StringIO()
    print(*args, file=output, **kwargs)
    contents = output.getvalue()
    output.close()
    return contents

texto = print_to_string("data:",dia,"/",mes,"/", ano, "\n"
     "\n", len(corretores_disponiveis), "corretores disponíveis (ativos e internos)", "\n"
     "\n", n_leads, "leads distribuidos nos últimos 30 dias", "\n"
     "\n", (n_leads) - len(frame[frame["RATING_3"].isin([0, 0.5])]), "distribuidos otimizados","\n"
     "\n", len(frame[frame["RATING_3"].isin([0, 0.5])]), "distribuidos randômicamente","\n"
     "\n", qtd_leads, "leads recebidos e distribuidos na última chamada")


# In[14]:


texto


# In[15]:


import pymsteams

#teste
myTeamsMessage = pymsteams.connectorcard("https://luminiitsolutions.webhook.office.com/webhookb2/04c027ba-9e80-4d77-8738-0cee08904648@ef191aab-b339-4591-935f-a36f876d4edd/IncomingWebhook/6c6fb008e4ef43458af92bf0b60f2f36/de6bf6d1-8f60-4c3e-abf2-ed4ed599cafb")

myTeamsMessage.title("Acompanhamento do motor cognitivo de leads")
myTeamsMessage.color("#00ffff")
myTeamsMessage.text(texto)
myTeamsMessage.send()


# In[ ]:





# In[ ]:




