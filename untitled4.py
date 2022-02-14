import salva_historico_BI as bi
import Historico_Match_BI as mbi
import Historico_Score_BI as sbi

def processa():

        deletar = bi.DriveAPI()

        deletar.delete_file()

        match = mbi.match()

        score = sbi.score()

 

if __name__ == '__main__':

    processa()

    print("Ok processado")
    