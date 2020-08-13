from kafka import KafkaConsumer
import logging
import time
import pandas as pd
from RealTimePreprocessing2 import preprocessing2
from RealTimePreprocessing1 import preprocessing1
import os
import threading


def preProcessing1_thread(name,data):

    logging.info("Thread preprocessing %s: starting", name)
    preprocessing1(data)                                                                            
    logging.info("Thread preprocessing %s: finishing", name)
    
def preProcessing2_thread(name,data):

    logging.info("Thread preprocessing %s: starting", name)
    preprocessing2(data) 
    logging.info("Thread preprocessing %s: finishing", name)
    


if __name__ == "__main__":
    

    format = "%(asctime)s: %(message)s"

    logging.basicConfig(format=format, level=logging.INFO,datefmt="%H:%M:%S")
    consumer = KafkaConsumer ('traffic',bootstrap_servers = ['localhost:9092'])
    
    data = pd.DataFrame(columns=['StartTime','Dur','Proto','SrcAddr','Sport','Dir','DstAddr','Dport','State','sTos','dTos','TotPkts','TotBytes','SrcBytes'])
    
    print ("Consuming messages from the given topic")
    start = time.time()
    for message in consumer:
        end = time.time()
        if(end - start)>100.0:
            print("end time")
            logging.info("Main    : before creating thread")
            #x = threading.Thread(target=preProcessing1_thread, args=(1,data,))
            logging.info("Main    : before running thread")
            #x.start()
            
            logging.info("Main    : before creating thread")
            y = threading.Thread(target=preProcessing2_thread, args=(1,data,))
            logging.info("Main    : before running thread")
            y.start()
            
            start=time.time()
            data = pd.DataFrame(columns=['StartTime','Dur','Proto','SrcAddr','Sport','Dir','DstAddr','Dport','State','sTos','dTos','TotPkts','TotBytes','SrcBytes'])
        print(message)
        messageSplit=str(message.value.decode("utf-8")).split(',')
        test=1
        for s in messageSplit:
            if s=='':
                test=0
                break
            
        if test==1:
            data = data.append(pd.Series(messageSplit,  index=['StartTime','Dur','Proto','SrcAddr','Sport','Dir','DstAddr','Dport','State','sTos','dTos','TotPkts','TotBytes','SrcBytes']),ignore_index=True)
    
    
    

    

    logging.info("Main    : wait for the thread to finish")

    # x.join()

    logging.info("Main    : all done")
"""consumer = KafkaConsumer ('traffic',bootstrap_servers = ['localhost:9092'])
print ("Consuming messages from the given topic")
start = time.time()
data = pd.DataFrame(columns=['StartTime','Dur','Proto','SrcAddr','Sport','Dir','DstAddr','Dport','State','sTos','dTos','TotPkts','TotBytes','SrcBytes'])
for message in consumer:
    end = time.time()
    if(end - start)>300.0:
        print("end time")
        start=time.time()
        preprocessing2(data)
        data = pd.DataFrame(columns=['StartTime','Dur','Proto','SrcAddr','Sport','Dir','DstAddr','Dport','State','sTos','dTos','TotPkts','TotBytes','SrcBytes'])
        
    messageSplit=str(message.value.decode("utf-8")).split(',')
    test=1
    for s in messageSplit:
        if s=='':
            test=0
            break
            
    if test==1:
        data = data.append(pd.Series(messageSplit, index=['StartTime','Dur','Proto','SrcAddr','Sport','Dir','DstAddr','Dport','State','sTos','dTos','TotPkts','TotBytes','SrcBytes']),ignore_index=True)
    
    print(data)
"""
