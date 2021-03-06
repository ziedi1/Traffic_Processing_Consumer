import pandas as pd
import numpy as np
import datetime
import h5py

from scipy.stats import mode



def normalize_column(dt, column):
    mean = dt[column].mean()
    std = dt[column].std()
    print(mean, std)

    dt[column] = (dt[column]-mean) / std
    
def RU(df):
    print(df)
    print(df.shape[0])
    if df.shape[0] == 1:
        return 1.0
    else:
        proba = df.value_counts()/df.shape[0]
        h = proba*np.log10(proba)
        return -h.sum()/np.log10(df.shape[0])
    
def preprocessing2(data):
    window_width = 120 # seconds
    window_stride = 60 # seconds
    print("Preprocessing")

    print(data)
    data['StartTime']=data['StartTime'].astype(object)
    data['Dur']=data['Dur'].astype(np.float64)
    data['Proto']=data['Proto'].astype(object)
    data['SrcAddr']=data['SrcAddr'].astype(object)
    data['Sport']=data['Sport'].astype(np.int64)
    data['Dir']=data['Dir'].astype(object)
    data['DstAddr']=data['DstAddr'].astype(object)
    data['Dport']=data['Dport'].astype(np.int64)
    data['State']=data['State'].astype(object)
    data['sTos']=data['sTos'].astype(np.int64)
    data['dTos']=data['dTos'].astype(np.int64)
    data['TotPkts']=data['TotPkts'].astype(np.int64)
    data['TotBytes']=data['TotBytes'].astype(np.int64)
    data['SrcBytes']=data['SrcBytes'].astype(np.int64)
    print(data)


    data['StartTime'] = pd.to_datetime(data['StartTime']).astype(np.int64)*1e-9
    datetime_start =data['StartTime'].min()

    data['Window_lower'] = (data['StartTime']-datetime_start-window_width)/window_stride+1
    data['Window_lower'].clip(lower=0, inplace=True)
    data['Window_upper_excl'] = (data['StartTime']-datetime_start)/window_stride+1
    data = data.astype({"Window_lower": int, "Window_upper_excl": int})
    data.drop('StartTime', axis=1, inplace=True)


    X = pd.DataFrame()
    nb_windows =data['Window_upper_excl'].max()
    print(nb_windows)

    for i in range(0, nb_windows):
        gb = data.loc[(data['Window_lower'] <= i) & (data['Window_upper_excl'] > i)].groupby('SrcAddr')
        X = X.append(gb.agg({'Sport':[RU], 
                            'DstAddr':[RU], 
                            'Dport':[RU]}).reset_index())
        print(X.shape)
        
    del(data)

    X.columns = ["_".join(x) if isinstance(x, tuple) else x for x in X.columns.ravel()]
    #print(X.columns.values)

    # std can be Nan if only one element
    X.fillna(-1, inplace=True)

    #print(X.columns.values)
    columns_to_normalize = list(X.columns.values)

    print(columns_to_normalize)
    columns_to_normalize.remove('SrcAddr_')
    print("++++++++++++++++++++++++++++++++2222222222222222222222222222222222222+++++++++++++++++++++++++++++++++++++++++++++++++++++")

    normalize_column(X, columns_to_normalize)
    print("++++++++++++++++++++++++++++++++5555555555555555555555555555+++++++++++++++++++++++++++++++++++++++++++++++++++++")

    with pd.option_context('display.max_rows', 10, 'display.max_columns', 22):
        print(X.shape)
        print(X)
        print(X.dtypes)
        
    #with pd.option_context('display.max_rows', 10, 'display.max_columns', 20):
    #    print(X.loc[X['Label'] != 0])

    X.drop('SrcAddr_', axis=1).to_hdf('data_window3_botnet3.h5', key="data", mode="w")
    np.save("data_window_botnet3_id3.npy", X['SrcAddr_'])
