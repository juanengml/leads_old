import pandas as pd
import joblib
import config
import json

'''
Base que contém o capacity do corretor. Criada no notebook de treinamento do capacity
'''
capacity_corretor = pd.read_csv(config.path_3+'/'+'capacity_corretor.csv') 
df_calibracao_score = pd.read_csv(config.main_path+'/'+'df_calibrado_score.csv')

'''
As listas abaixo servem para fazer o replace de alguns dados nomes incorretos para uma lista com os nomes corrigidos.
Exemplo: replace dos nomes de cidade_1 para cidade_2
Criadas manualmente pelo cientista de dados.
'''
listas =  ['cidade_1.txt', 'cidade_2.txt', 'entidade_1.txt', 'entidade_2.txt', 
        'origem_midia_veiculo_1.txt', 'origem_midia_veiculo_2.txt', 'profissao_1.txt',
        'profissao_2.txt']
Listas = []
for lista in listas:
    with open(config.path_3+'/'+lista, 'r') as filehandle:
        Listas.append(json.load(filehandle)) 
        
'''
As listas abaixo são listas dos rótulos mais comuns nas variáveis. São usadas para selecionar os rótulos da categoria para
treinamento.
Criadas no notebook de treino.
'''
profissao_lista_representativa = joblib.load(config.path_3+'/'+'profissao_lista_representativa.sav')
MIDIA_VEICULO_lista_representativa = joblib.load(config.path_3+'/'+'MIDIA_VEICULO_lista_representativa.sav')

'''
Artefatos de treinamento do motor de leads. Craidos no notebook de treino.
'''       

features = joblib.load(config.path_3+'/'+'features.sav')
features_cluster = joblib.load(config.path_3+'/'+'features_cluster.sav')
categoria_treino = joblib.load(config.path_3+'/'+'categoria_treino.sav')

#carrega o modelo de preprocessamento
preprocessor = joblib.load(config.path_3+'/'+'preprocessor_leads.joblib.dat')
# carrega o modelo utilizado para predição
xgboost_loaded_model = joblib.load(config.path_3+'/'+'xgboost_leads_2.joblib.dat')
        #preprocessamento para clusterização
#preprocessor_cluster = joblib.load(config.path_3+'/'+'preprocessor_leads_cluster.joblib.dat')
#lista de modelos por cluster

    
with open(config.path_3+'/'+'preprocessor_leads_cluster.joblib.dat', 'rb') as f:
    preprocessor_cluster = joblib.load(f) 
        


kmeans_model = joblib.load(config.path_3+'/'+'kmeans_cluster.joblib.dat')

###base de ddd das praças. Enviada pela Qualicorp
ddd = pd.read_excel(config.path_3+'/'+'ddd.xlsx')
#idh dos municípios. Base pública
idhm = pd.read_csv(config.path_3+'/'+'IDH2010.csv', delimiter=';')

'''
Core do motor de recomendação. Tabela que asscocia uma nota de cada corretor para cada cluster de lead
Criada no notebook de treino
'''
matriz = pd.read_csv(config.path_3+'/'+'matriz_recomendacao.csv')

#base de corretores ativos
corretores = pd.read_csv(config.path_3+'/'+'corretores_ativos.csv')

#enviados_count_corretor_redistribuicao = joblib.load(config.path_enviados+'/enviados_count_corretor_redistribuicao.csv')

#base que contém os leads enviados nos últimos 30 dias. Usada como histórico para o programa fair de distribuição.
#criada na execução dda api motor_predict.py
base_fair_motor = pd.read_csv(config.path_fair_motor+'/dados_empilhados_motor.csv')

#histórico com os lotes de leads enviados e recebidos
#criadas na execução dda api motor_predict.py
historico_recebidos = pd.read_csv(config.path_recebidos_historico+'/historico_recebidos.csv')
historico_enviados = pd.read_csv(config.path_enviados_historico+'/historico_enviados.csv')

'''
Usada para o script de retreino. Varifica se há novas origens a serem retreinadas.
criada na execucação da api motor_predict.py
'''
lista_novas_origens = joblib.load(config.path_3+'/'+'lista_novas_origens.joblib.dat')
try:
    enviados_count_corretor_redistribuicao = pd.read_csv(config.path_3+'/enviados_count_corretor_redistribuicao.csv')
except FileNotFoundError:
    enviados_count_corretor_redistribuicao = pd.DataFrame({'ID_LEAD':[], 'CPF_CORRETOR':[], 'data_referencia':[]})

base_teste = pd.read_csv(config.path_3+'/'+'base_teste.csv')
