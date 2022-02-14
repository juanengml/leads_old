import salva_historico_BI as bi
import Historico_Match_BI as mbi
import Historico_Score_BI as sbi


def processa():

        #api = bi.DriveAPI()
        #api.delete_file()
        
        match = mbi.match()
        score = sbi.score()
        
        api.FileUpload("historico_match.csv")
        api.FileUpload("historico_score.csv")
 
if __name__ == '__main__':
    processa()
    print("Ok processado")
    