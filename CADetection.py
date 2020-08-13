import numpy as np
import pandas as pd
from scipy.sparse import csc_matrix
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import h5py
from SendToFireBase import sendToFB
import pickle as cPickle

#from sklearn import model_selection, feature_selection, utils, ensemble, linear_model, metrics

print("Import data")

X = pd.read_hdf('data_window_botnet3.h5', key='data')
X.reset_index(drop=True, inplace=True)

X2 = pd.read_hdf('data_window3_botnet3.h5', key='data')
X2.reset_index(drop=True, inplace=True)

X = X.join(X2)

X.drop('window_id', axis=1, inplace=True)




print("X.columns.values")
print(X.columns.values)
print(X['counts'])
#print(labels)
#print(np.where(labels == 'flow=From-Botne')[0][0])
try:
    with open('CyberAttackModel', 'rb') as f:
        clf = cPickle.load(f)

    y_pred_test = clf.predict(X)
    for i in y_pred_test:
        print(i)
        if i==False:
            print("there is an attack")
            sendToFB('1')
            break
            
    print(y_pred_test) 
except:
    print("error in traffic ")
