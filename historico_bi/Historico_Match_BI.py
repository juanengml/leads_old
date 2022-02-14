import glob
import pandas as pd
from datetime import datetime
import os
import salva_historico_BI as bi
from salva_azure import salva_blob

def match():
    all_files = glob.glob('../enviados/backup' + "/*"+".csv")
    li = []

    for filename in all_files:
        try:
            df = pd.read_csv(filename, index_col=None, header=0, sep="[;,]", engine='python')
            df["data"]=datetime.fromtimestamp(os.path.getmtime(filename)).strftime('%Y%m%d')
            df.rename(columns={',ID_LEAD':'ID_LEAD'}, inplace=True)
            li.append(df)
        except: pass
 

    try:     
        enviados = pd.concat(li, axis=0, ignore_index=True)
        enviados_2 = enviados
        enviados_2['ID_LEAD'] = enviados_2[',ID_LEAD'].apply(lambda x: x.split(',')[1]).astype(float)
    except: pass

    arq_mane='historico_match.csv'
    enviados_2.to_csv(arq_mane, index=False)
    salva_blob(arq_mane)

