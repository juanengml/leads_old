import os
main_path = os.path.join(os.path.dirname(__file__))
#SALVA O LOG DO MOTOR
path_2 = main_path + '/log' 

#CAMINHO ONDE ESTA OS DADOS DO MOTOR
path_3 = main_path + '/'+'arquivos_treino' 
HOMOLOGACAO = False
PLANTAO = True
#path com os lotes de leads recebidos e enviados
if HOMOLOGACAO:
    #SALVA O LOG DO MOTOR
    path_2 = main_path +'/HOMOLOGACAO'+ '/log' 
    path_recebidos = main_path +'/HOMOLOGACAO' + '/' + 'recebidos'
    path_enviados = main_path +'/HOMOLOGACAO' + '/' + 'enviados'
    path_score_enviados = main_path +'/HOMOLOGACAO' + '/' + 'Score_enviados'
    path_score_recebidos = main_path +'/HOMOLOGACAO' + '/' + 'Score_recebidos'
    path_recebidos_historico = path_recebidos + '/'+'historico_producao'
    path_enviados_historico = path_enviados +'/'+ 'historico_producao'
    #CAMINHO ONDE ESTA OS RESULTADOS DO MOTOR
    path_recomendacao = main_path+'/HOMOLOGACAO' + '/' + 'Leads_recomendacao'
    #caminho para a localização da base fair do motor
    path_fair_motor = path_enviados+'/'+'base_fair_motor'

    URL_PLANTAO = 'http://hmwinap03:8082/focus-leads-integration/buscar-plantao-lumini'
    URL_CORRETOR = "http://hmwinap03:8082/focus-leads-integration/corretor"
    URL_BEFAIR = "http://hmwinap03:8082/focus-leads-integration/set-befair"
    
    API_KEY_HEAD = 'X-Gravitee-Api-Key'
    API_KEY = 'da716f45-d4b0-42b1-acb5-50822e8b4870'
    HEADERS = {API_KEY_HEAD:API_KEY}
    '''
    URL_PLANTAO = 'https://qualitech.qualicorp.com.br/focus-leads-integration/buscar-plantao-lumini'
    URL_CORRETOR = "https://qualitech.qualicorp.com.br/focus-leads-integration/corretor"
    URL_BEFAIR = "https://qualitech.qualicorp.com.br/focus-leads-integration/set-befair" 
   
    #chave de acesso às APIS
    API_KEY_HEAD = 'X-Gravitee-Api-Key'
    API_KEY = '2e31146a-f2d5-42b4-a228-341b31db7652'
    HEADERS = {API_KEY_HEAD:API_KEY}
    '''
else:  
    path_2 = main_path + '/log' 
    path_recebidos = main_path +'/' + 'recebidos'
    path_score_recebidos = main_path +'/' + 'Score_recebidos'
    path_enviados = main_path +'/' + 'enviados'
    path_score_enviados = main_path +'/' + 'Score_enviados'

    path_recebidos_historico = path_recebidos + '/'+'historico_producao'
    path_enviados_historico = path_enviados +'/'+ 'historico_producao'
    #CAMINHO ONDE ESTA OS RESULTADOS DO MOTOR
    path_recomendacao = main_path +'/' + 'Leads_recomendacao'
    #caminho para a localização da base fair do motor
    path_fair_motor = path_enviados+'/'+'base_fair_motor'
    
    URL_PLANTAO = 'https://qualitech.qualicorp.com.br/focus-leads-integration/buscar-plantao-lumini'
    URL_CORRETOR = "https://qualitech.qualicorp.com.br/focus-leads-integration/corretor"
    URL_BEFAIR = "https://qualitech.qualicorp.com.br/focus-leads-integration/set-befair" 
    
    #chave de acesso às APIS
    API_KEY_HEAD = 'X-Gravitee-Api-Key'
    API_KEY = '2e31146a-f2d5-42b4-a228-341b31db7652'
    HEADERS = {API_KEY_HEAD:API_KEY}


#URL para conexão com a API de leads e corretores
URL_LEADS = 'https://qualitech.qualicorp.com.br/focus-leads-integration/leads'

#chave de acesso ao bucket da S3
AWS_S3_ACCESS = 'AKIAU7LGGRXFZMYPUTPV'
AWS_S3_SECRET = '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'
BUCKET_QUALI = 'motorleads'

AWS_S3_ACCESS_TREINO = 'AKIAVDDAGVT7SZ2RSGEK'
AWS_S3_SECRET_TREINO = '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'
BUCKET_QUALI_TREINO =  'qualicorp-bi-lumini'

LEADS_VAL_KEY  = '8732cf0cb37ebff86a8d9c4bdcc9d17b217eccf63580072dfb4d4d17408e802a'
#chave de acesso ao Teams
URL_TEAMS = "https://luminiitsolutions.webhook.office.com/webhookb2/04c027ba-9e80-4d77-8738-0cee08904648@ef191aab-b339-4591-935f-a36f876d4edd/IncomingWebhook/6c6fb008e4ef43458af92bf0b60f2f36/de6bf6d1-8f60-4c3e-abf2-ed4ed599cafb"
