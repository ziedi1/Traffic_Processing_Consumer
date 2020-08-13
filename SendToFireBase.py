from firebase import firebase
import sys

def sendToFB(status) :
    name="attack"
 
    fb = firebase.FirebaseApplication('https://csdetection.firebaseio.com/', None)

    result = fb.put('/csdetection/Detection/', name ,status)
    print(result)
