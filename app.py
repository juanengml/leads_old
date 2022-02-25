from flask import Flask, request, jsonify
import pandas as pd
import random
import sys
import hashlib
from datetime import datetime
import motor_predict_new as mc
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
                output = mc.run_motor(ent_json)

                # TODO This response does not make sense
                result = {}
                for l in output:
                    out = {';'.join(list(output[l].keys())): ';'.join(
                        map(lambda x: str(x), list(output[l].values())))}
                    result[l] = out
                output = result
                return jsonify(output)

            result = {}
            for index, row in df.iterrows():
                result[index] = dict(row)
            return jsonify(result)
    
    return '[{"Status":"ACESSO NEGADO"}]'
#
# if __name__=='__main__':
#     app.run(host='0.0.0.0')


