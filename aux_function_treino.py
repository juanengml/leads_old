import imblearn
import pandas as pd
from sklearn.model_selection import StratifiedShuffleSplit
import xgboost
import numpy as np

from skopt import gp_minimize
from skopt.space import Categorical, Integer, Real
from skopt.utils import use_named_args
from sklearn.model_selection import cross_val_score
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.cluster import KMeans
from sklearn.metrics import calinski_harabasz_score
from sklearn.base import BaseEstimator, TransformerMixin

def teste_base_balanceada(X_, y_, xvar, proporcao=1):
    accuracy=[]
    #o índice modifica os random_states
    for i in range(0,100,10):
        RM = imblearn.under_sampling.RandomUnderSampler(sampling_strategy=proporcao, random_state=i)
        RMS = RM.fit_resample(X=X_, y=y_)

        base_final_imb = pd.concat([pd.DataFrame(RMS[0]),pd.DataFrame(RMS[1])],axis=1)
        X = base_final_imb[xvar].values
        y = base_final_imb.FLGVENDASAUDE.values
        sss = StratifiedShuffleSplit(n_splits=1, train_size=0.7, random_state=i)
        sss.get_n_splits(X_, y_)
        for train_index, test_index in sss.split(X, y):
            X_train_imb, X_test_imb = X[train_index], X[test_index]
            y_train_imb, y_test_imb = y[train_index], y[test_index]

        xg_balanced = xgboost.XGBClassifier(scale_pos_weight= 1/proporcao, max_depth=4, 
                                            verbosity = 0, random_state=i)
        xg_balanced.fit(X_train_imb, y_train_imb)

        y_xgb_prob = xg_balanced.predict_proba(X_test_imb)
        cm = pd.crosstab(y_test_imb, y_xgb_prob[:,-1]>0.5, rownames=['Real'], colnames=['Predito'])

        TP = cm.reset_index().iloc[1,2]
        TN = cm.reset_index().iloc[0,1]
        FP = cm.reset_index().iloc[0,2]
        FN = cm.reset_index().iloc[1,1]
        ModelAccuracy = (TP + TN) / float(TP + TN + FP + FN)
        print(f'Acurácia = {ModelAccuracy}')
        accuracy.append(ModelAccuracy)
    print(f'Acurácia média de {round(np.mean(accuracy),2)}')
    return np.mean(accuracy)

def otimizador_xgboost(model, X, y, metrica='roc_auc', n_chamadas=50):
    space  = [Integer(1, 5, name='max_depth'),
              Real(10**-5, 10**0, "log-uniform", name='learning_rate'),
              Integer(1, 13, name='max_features'),
              Integer(2, 100, name='min_samples_split'),
              Integer(1, 100, name='min_samples_leaf')]

    # definindo a função para ser otimizada
    @use_named_args(space)
    def evaluate_model(**params):
        model.set_params(**params)
        # calculando o cross-validation com 5 folds
        return 1-np.mean(cross_val_score(model, X, y, cv=3, n_jobs=-1,
                                        scoring="roc_auc"))
    # aplicando a otimização via processo gaussiano

    # this decorator allows your objective function to receive a the parameters as
    # keyword arguments. This is particularly convenient when you want to set scikit-learn
    # estimator para meters
    @use_named_args(space)
    def objective(**params):
        reg.set_params(**params)

        return -np.mean(cross_val_score(reg, X, y, cv=5, n_jobs=-1,
                                        scoring="neg_mean_absolute_error"))
    result = gp_minimize(evaluate_model, space, verbose=True, n_calls= 50)
    # resumindo os resultados:
    print('Best AUC_ROC: %.5f' % (1.0 - result.fun))
    best_params = {'max_depth': result.x[0], 'learning_rate': result.x[1], 'max_features': result.x[2], 'min_samples_split':result.x[3], 
           'min_samples_leaf': result.x[4]}
    print(best_params)
    return best_params

class Praca_transformer(BaseEstimator, TransformerMixin):
    def __init__(self, feature_name):
        self.feature_name = feature_name
        
    def fit(self, X, y=None):
        return self
        
    def transform(self, X, y=None):
        X_ = X.copy()
        dict_praca = {'gsp':1, 'spi_campinas':2, 'spi': 3, 'sorocaba':4, 'spl':5,'vale do paraiba':6, 'rj':7,
           'petropolis':8, 'mg':9, 'es': 10, 
            'ba':11, 'se':12, 'al':13,'pe':14, 'rn':15, 'ce':16, 'ma':17, 'pi':18, 'pb':19,
            'rs':30, 'pr':31, 'sc':32,
            'df':20, 'go':21, 'mt':22, 'ms':23, 
            'am':40, 'ro':41, 'ap':42, 'to':43, 'ac':44, 'rr':45, 'pa':46,
           'demais':99}
        X_[self.feature_name] = X_[self.feature_name].replace(dict_praca)
        return X_
    
    
def best_n_cluster(n_cluster_lista, X):
    score_ch_lista = []
    kmeans_model_lista = []
    for n_cluster in n_cluster_lista:
        kmeans_model = KMeans(n_clusters = n_cluster, random_state = 12, n_jobs=-1).fit(X)
        kmeans_label = kmeans_model.labels_
        score_ch = calinski_harabasz_score(X, kmeans_label)
        score_ch_lista.append(score_ch)
        kmeans_model_lista.append(kmeans_model)
    best_model = kmeans_model_lista[np.argmax(score_ch_lista)]
    best_cluster = n_cluster_lista[np.argmax(score_ch_lista)]
    print(f'O melhor número de cluster é {best_cluster}.')
    return best_model  

