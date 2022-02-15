from flask import Flask, request, jsonify
import pandas as pd
import random
import sys
import hashlib
from datetime import datetime
import motor_predict as mc
import motor_score_predict as mc_score
import config

import logging
LOG_FILENAME = "logfileX.log"
log_format = '%(asctime)s:%(levelname)s:%(filename)s:%(message)s'
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG,format=log_format)  
    
app = Flask(__name__)
sys.path.insert(1, 'Leads_recomendacao')
#import motor_cognitivo_v0_3 as mc
@app.route('/leads', methods=['GET', 'POST'])
def add_message():
    logging.info('API chamada')

    content = request.json
    val_key = config.LEADS_VAL_KEY
    api_val = str(request).split('apikey=')[1].split("'")[0]
    if (hashlib.sha256(str(api_val).encode('utf-8')).hexdigest() == val_key):
        ano = datetime.today().year
        mes = datetime.today().month
        dia = datetime.today().day

        data = str(ano)+str(mes)+str(dia)
        #f = open("C:/Users/lumini10/Desktop/motor_cognitivo_leads/Leads_recomendacao/input_leads.json", 'w+')
        #f.write(content)
        try:
            ent_json = pd.DataFrame.from_dict(content['leads'])
            TIPO_SCORE=content['flagDistribuicaoScore']
        except TypeError:
            print('formato de entrada incorreto')
            logging.info('formato de entrada incorreto')
            ent_json = pd.DataFrame()
        k = str(random.randint(10000, 100000000))
        if ent_json.empty:
            print('Payload em branco')
            logging.info('Payload em branco')
        else:
            if TIPO_SCORE:
                ent_json.to_csv(config.path_score_recebidos+'/'+k+'_recebido_score_'+data+'.csv', index=None)
                arq = mc_score.motor(ent_json)
                df = pd.read_csv(config.path_recomendacao+'/'+arq, delimiter=';')
                df.to_csv(config.path_score_enviados+'/'+k+'_'+arq, index=None)
                logging.info('Motor score')
            else:
                ent_json.to_csv(config.path_recebidos+'/'+k+'_recebido_'+data+'.csv', index=None)
                id_lead_cod_entrada = sum(ent_json['ID_LEAD'])*len(ent_json['ID_LEAD'])
                arq = mc.motor(ent_json)
                df = pd.read_csv(config.path_recomendacao+'/'+arq, delimiter=';')
                #k = str(random.randint(10000, 100000000))
                #ent_json.to_csv('C:/Users/lumini10/Desktop/motor_cognitivo_leads/recebidos/'+k+'_'+arq, index=None)
                id_lead_cod_saida = sum(df['ID_LEAD'])*len(df['ID_LEAD'])
                if id_lead_cod_entrada == id_lead_cod_saida:
                    print('Retorno OK')
                    saida= ('Retorno OK '+ str(sum(df['ID_LEAD'])) + ' leads')
                    logging.info(saida)
                else:
                    print('Retorno não coerente com a entrada')
                    logging.info('Retorno não coerente com a entrada')
                df.to_csv(config.path_enviados+'/enviados_motor'+'/'+k+'_'+arq, index=None)
                df = df.drop(columns=['FLAG_REDISTRIBUICAO'])
            result = {}
            for index, row in df.iterrows():
                result[index] = dict(row)
            return jsonify (result)       
    
    return '[{"Status":"ACESSO NEGADO"}]'

#if __name__=='__main__':
    #app.run(host='0.0.0.0')

